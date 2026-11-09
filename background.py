"""Plugin-owned daemon threads that work through a job's pending items.

Each running job has at most one ``_Run`` per process, holding a bounded number of
threads. Threads only ever take work through the atomic claim in the store.

Threads are used instead of ``ctx.spawn_task`` because Hermes runs synchronous command
handlers on a pool thread with no running event loop, and because the model call blocks.
They are daemons and exist only as long as the process: the gateway ends with
``os._exit`` and does not call ``on_unload``, so unfinished claims are recovered by the
next process through the file-lock check in ``worker.py``.

Each thread runs in its own copy of the context captured from the command that started
the job, which carries the active Hermes profile into ``ctx.llm`` and storage.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from .errors import JobBlocked, JobStateError

logger = logging.getLogger(__name__)


@dataclass
class _Run:
    job_id: str
    limit: int
    context: contextvars.Context
    active: int = 0
    again: bool = False  # someone asked for work while the threads may be leaving


class BackgroundWorkers:
    def __init__(self, manager: Any, draining: Callable[[], bool] = lambda: False) -> None:
        self._manager = manager
        self._draining = draining
        self._lock = threading.Lock()
        self._runs: dict[str, _Run] = {}
        self._threads: set[threading.Thread] = set()
        self._stopping = threading.Event()

    def start(self, job_id: str, limit: int) -> bool:
        """Start workers for a job, or top up an existing run. True if a new run began."""
        with self._lock:
            if self._stopping.is_set():
                raise JobStateError("Bulk is shutting down")
            run = self._runs.get(job_id)
            created = run is None
            if run is None:
                run = _Run(job_id, limit, contextvars.copy_context())
                self._runs[job_id] = run
            else:
                run.limit = limit
                run.again = True
            self._spawn(run)
            if not run.active:
                del self._runs[job_id]
                raise JobStateError("could not start a worker thread")
            return created

    def is_active(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._runs

    def worker_count(self, job_id: str) -> int:
        with self._lock:
            run = self._runs.get(job_id)
            return run.active if run else 0

    def stopping(self) -> bool:
        return self._stopping.is_set() or self._draining()

    def stop(self, grace: float) -> None:
        """Stop claiming new items and wait up to ``grace`` seconds for threads to leave."""
        self._stopping.set()
        with self._lock:
            threads = list(self._threads)
        deadline = time.monotonic() + grace
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))

    # ── Worker threads ──────────────────────────────────────────────────────

    def _spawn(self, run: _Run) -> None:
        """Bring a run up to its limit. The caller holds ``_lock``."""
        while run.active < run.limit:
            thread = threading.Thread(
                target=run.context.copy().run,
                args=(self._work, run),
                name=f"bulk-{run.job_id}",
                daemon=True,
            )
            run.active += 1
            self._threads.add(thread)
            try:
                thread.start()
            except RuntimeError:
                logger.warning("Bulk could not start a worker thread for job %s", run.job_id)
                run.active -= 1
                self._threads.discard(thread)
                return

    def _work(self, run: _Run) -> None:
        try:
            while not self.stopping():
                if self._manager.process_next(run.job_id) is None:
                    return
        except JobBlocked:
            return  # paused or at the token limit; in-flight items on other threads finish
        except Exception as exc:
            self._manager.note_worker_error(run.job_id, exc)
        finally:
            self._finish(run)

    def _finish(self, run: _Run) -> None:
        with self._lock:
            self._threads.discard(threading.current_thread())
            run.active -= 1
            if run.active:
                return
            # No thread of this run is left, so any claim this process still holds for
            # the job is orphaned. Done under the lock so a new run cannot claim first.
            self._manager.release_orphans(run.job_id)
            if run.again and not self.stopping():
                run.again = False
                self._spawn(run)
            if not run.active:
                del self._runs[run.job_id]
