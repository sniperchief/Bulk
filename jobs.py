"""Job lifecycle: create, process items, recover, pause, resume and retry.

Delivery is at-least-once. A process can stop after the model has responded but before
the result is saved, in which case the item is sent to the model again after recovery.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from . import discovery, executor, results
from .background import BackgroundWorkers
from .errors import ItemProcessingError, JobBlocked, JobNotFound, ShuttingDown
from .store import Store
from .worker import WorkerLock, owner_is_alive, sweep_stale_locks

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 30
SHUTDOWN_GRACE_SECONDS = 5.0


@dataclass(frozen=True)
class CreatedJob:
    job_id: str
    total_items: int
    skipped: list[tuple[str, str]]


@dataclass(frozen=True)
class ProcessedItem:
    item_key: str
    ok: bool
    error: Optional[str] = None
    output_path: Optional[Path] = None
    result: Optional[executor.ItemResult] = None


@dataclass(frozen=True)
class StartResult:
    started: bool  # False when workers were already running or nothing was pending
    pending: int
    workers: int


class JobManager:
    def __init__(
        self,
        store: Store,
        data_dir: Path,
        llm: Any,
        sleep: Callable[[float], None] = time.sleep,
        draining: Callable[[], bool] = lambda: False,
    ) -> None:
        self.store = store
        self.data_dir = data_dir
        self._llm = llm
        self._sleep = sleep
        self.worker = WorkerLock(data_dir / "workers")
        self.background = BackgroundWorkers(self, draining)
        self.recover()
        sweep_stale_locks(self.worker.directory, self.worker.id)

    def close(self, grace: float = SHUTDOWN_GRACE_SECONDS) -> None:
        """Stop workers, give back any claim they could not finish, then release resources."""
        self.background.stop(grace)
        try:
            self._release_own_claims(None)
        finally:
            self.store.close()
            self.worker.release()

    def create_job(
        self,
        instruction: str,
        source: Path,
        max_tokens: Optional[int] = None,
        max_retries: int = 2,
        concurrency: int = 4,
    ) -> CreatedJob:
        found = discovery.discover(source)
        job_id = self.store.create_job(
            instruction,
            str(source),
            [
                (item.key, item.content_hash, executor.estimate_tokens(instruction, item))
                for item in found.items
            ],
            max_tokens=max_tokens,
            max_retries=max_retries,
            concurrency=concurrency,
            skipped=found.skipped,
        )
        return CreatedJob(job_id, len(found.items), found.skipped)

    def get_job(self, job_id: str) -> sqlite3.Row:
        job = self.store.get_job(job_id)
        if job is None:
            raise JobNotFound(f"no job with id {job_id}")
        return job

    # ── Background processing ───────────────────────────────────────────────

    def start(self, job_id: str, max_concurrency: int) -> StartResult:
        """Process a job in the background. Raises ``JobBlocked`` if it is paused."""
        job = self.get_job(job_id)
        self.recover(job_id)
        if job["status"] == "paused":
            raise JobBlocked(f"Job {job_id} is paused. Continue with: /bulk resume {job_id}")
        pending = sum(item["status"] == "pending" for item in self.store.get_items(job_id))
        if not pending:
            return StartResult(False, 0, self.background.worker_count(job_id))
        limit = min(job["concurrency"], max_concurrency)
        started = self.background.start(job_id, limit)
        return StartResult(started, pending, limit)

    def note_worker_error(self, job_id: str, exc: Exception) -> None:
        """Called when a worker thread stops on an unexpected error, so the job shows why."""
        logger.warning("Bulk worker for job %s stopped: %s: %s", job_id, type(exc).__name__, exc)
        error = executor.describe_error(exc)
        self._quietly(lambda: self.store.record_event(job_id, "worker_error", error))

    def release_orphans(self, job_id: str) -> None:
        """Requeue claims held by this process after all of a job's workers have left."""
        self._quietly(lambda: self._release_own_claims(job_id))

    def _release_own_claims(self, job_id: Optional[str]) -> None:
        for claim in self.store.running_claims(job_id):
            if claim["claimed_by"] == self.worker.id:
                self.store.release_item(claim["id"], self.worker.id)

    @staticmethod
    def _quietly(action: Callable[[], Any]) -> None:
        """Bookkeeping that must not raise from a worker thread, e.g. after the store closed."""
        try:
            action()
        except Exception:
            logger.debug("Bulk bookkeeping failed", exc_info=True)

    def claims_held_by_live_process(self, job_id: str) -> bool:
        directory = self.worker.directory
        return any(owner_is_alive(directory, owner) for owner in self.store.claim_owners(job_id))

    # ── Recovery and job control ────────────────────────────────────────────

    def recover(self, job_id: Optional[str] = None) -> int:
        """Requeue items claimed by processes that have stopped."""
        recovered = 0
        for claim in self.store.running_claims(job_id):
            owner = claim["claimed_by"]
            if owner == self.worker.id or owner_is_alive(self.worker.directory, owner):
                continue
            if self.store.recover_item(claim["id"], owner):
                recovered += 1
                if owner and owner.isalnum():
                    results.discard_staged(self.data_dir, claim["job_id"], claim["item_index"], owner)
        return recovered

    def pause(self, job_id: str) -> bool:
        self.get_job(job_id)
        return self.store.pause_job(job_id)

    def resume(self, job_id: str, max_tokens: Optional[int] = None) -> int:
        """Resume a paused job. Returns how many interrupted items were recovered."""
        self.get_job(job_id)
        recovered = self.recover(job_id)
        self.store.resume_job(job_id, max_tokens)
        return recovered

    def retry_failed(self, job_id: str) -> int:
        self.get_job(job_id)
        return self.store.retry_failed(job_id)

    # ── Processing ──────────────────────────────────────────────────────────

    def process_next(self, job_id: str) -> Optional[ProcessedItem]:
        """Process the next pending item, or return ``None`` if there is none."""
        job = self.get_job(job_id)
        self.recover(job_id)
        item = self.store.claim_next(job_id, self.worker.id)
        if item is None:
            return None
        try:
            return self._process(job, item)
        except ShuttingDown as exc:
            self._quietly(lambda: self.store.release_item(item["id"], self.worker.id))
            return ProcessedItem(item["item_key"], ok=False, error=str(exc))
        except Exception as exc:
            logger.warning("Bulk item %s failed: %s: %s", item["item_key"], type(exc).__name__, exc)
            error = executor.describe_error(exc)
            self._record_failure(item["id"], error)
            return ProcessedItem(item["item_key"], ok=False, error=error)
        except BaseException:  # Ctrl+C or cancellation: this attempt did not finish
            self.store.release_item(item["id"], self.worker.id)
            raise

    def _record_failure(self, item_id: int, error: str) -> None:
        """Mark an item failed. If that cannot be saved, try not to leave it claimed."""
        try:
            self.store.fail_item(item_id, self.worker.id, error)
        except Exception:
            logger.warning("Bulk could not record a failed item; releasing it", exc_info=True)
            self._quietly(lambda: self.store.release_item(item_id, self.worker.id))

    def _process(self, job: sqlite3.Row, row: sqlite3.Row) -> ProcessedItem:
        item = discovery.load_item(Path(job["source"]), row["item_key"])
        if item.content_hash != row["content_hash"]:
            raise ItemProcessingError("source content changed since the job was created")
        outcome = self._call_model(job, row, item)
        worker = self.worker.id
        staged = results.stage_output(self.data_dir, job["id"], row["item_index"], worker, outcome.text)
        relative = results.output_relative(job["id"], row["item_index"])
        final = self.data_dir / relative
        try:
            self.store.complete_item(
                row["id"],
                worker,
                relative,
                outcome.tokens_in,
                outcome.tokens_out,
                outcome.cost_usd,
                publish=lambda: os.replace(staged, final),
            )
        except Exception:
            staged.unlink(missing_ok=True)
            raise
        return ProcessedItem(row["item_key"], ok=True, output_path=final, result=outcome)

    def _call_model(self, job: sqlite3.Row, row: sqlite3.Row, item: discovery.Item) -> executor.ItemResult:
        max_attempts = job["max_retries"] + 1
        attempt = 1
        while True:
            try:
                return executor.run_item(self._llm, job["instruction"], item)
            except Exception as exc:
                if attempt >= max_attempts or not executor.is_transient(exc):
                    raise
                logger.warning("Bulk item %s attempt %d failed: %s: %s", item.key, attempt, type(exc).__name__, exc)
                error = executor.describe_error(exc)
                self.store.record_failed_attempt(row["id"], self.worker.id, error, attempt, max_attempts)
                self._sleep(min(2**attempt, MAX_BACKOFF_SECONDS))
                if self.background.stopping():
                    raise ShuttingDown("stopped between retries") from exc
                attempt += 1
