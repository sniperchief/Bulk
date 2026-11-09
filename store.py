"""SQLite persistence for jobs, items and events.

The connection is shared between threads (Hermes runs synchronous slash command
handlers in an executor), so every statement and transaction holds ``_lock``. Every
state transition is one ``BEGIN IMMEDIATE`` transaction, which also serializes
workers in different processes.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterable, Iterator, Optional

from .errors import JobBlocked, JobStateError, StoreError

_V1 = (
    """
    CREATE TABLE jobs (
        id              TEXT PRIMARY KEY,
        instruction     TEXT NOT NULL,
        source          TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'paused',
                                          'completed', 'completed_with_errors')),
        total_items     INTEGER NOT NULL DEFAULT 0 CHECK (total_items >= 0),
        completed_items INTEGER NOT NULL DEFAULT 0 CHECK (completed_items >= 0),
        failed_items    INTEGER NOT NULL DEFAULT 0 CHECK (failed_items >= 0),
        budget_usd      REAL CHECK (budget_usd IS NULL OR budget_usd >= 0),
        spent_usd       REAL NOT NULL DEFAULT 0 CHECK (spent_usd >= 0),
        concurrency     INTEGER NOT NULL DEFAULT 4 CHECK (concurrency >= 1),
        created_at      TEXT NOT NULL,
        updated_at      TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE items (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id       TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        item_index   INTEGER NOT NULL CHECK (item_index >= 0),
        item_key     TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        status       TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'completed', 'failed')),
        attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        last_error   TEXT,
        tokens_in    INTEGER NOT NULL DEFAULT 0 CHECK (tokens_in >= 0),
        tokens_out   INTEGER NOT NULL DEFAULT 0 CHECK (tokens_out >= 0),
        output_path  TEXT,
        updated_at   TEXT NOT NULL,
        UNIQUE (job_id, item_index),
        UNIQUE (job_id, item_key)
    )
    """,
    """
    CREATE TABLE events (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        message    TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX idx_jobs_created_at ON jobs (created_at DESC)",
    "CREATE INDEX idx_items_job_status ON items (job_id, status, item_index)",
    "CREATE INDEX idx_events_job ON events (job_id, id)",
)

_V2 = (
    "ALTER TABLE jobs ADD COLUMN max_tokens INTEGER CHECK (max_tokens IS NULL OR max_tokens >= 1)",
    "ALTER TABLE jobs ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 2 CHECK (max_retries >= 0)",
    "ALTER TABLE items ADD COLUMN estimated_tokens INTEGER NOT NULL DEFAULT 0 CHECK (estimated_tokens >= 0)",
    "ALTER TABLE items ADD COLUMN reserved_tokens INTEGER NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0)",
    "ALTER TABLE items ADD COLUMN claimed_by TEXT",
)

_MIGRATIONS = (_V1, _V2)
SCHEMA_VERSION = len(_MIGRATIONS)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Store:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._lock = threading.RLock()
        conn.row_factory = sqlite3.Row
        conn.isolation_level = None  # transactions are explicit, see _tx()
        conn.execute("PRAGMA foreign_keys = ON")
        self._migrate()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    def _migrate(self) -> None:
        with self._tx() as c:
            version = c.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise StoreError(
                    f"database schema v{version} is newer than this version of Bulk "
                    f"(v{SCHEMA_VERSION})"
                )
            for migration in _MIGRATIONS[version:]:
                for statement in migration:
                    c.execute(statement)
            if version < SCHEMA_VERSION:
                c.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ── Job writes ──────────────────────────────────────────────────────────

    def create_job(
        self,
        instruction: str,
        source: str,
        items: Iterable[tuple[str, str, int]],
        max_tokens: Optional[int] = None,
        max_retries: int = 2,
        concurrency: int = 4,
        skipped: Iterable[tuple[str, str]] = (),
    ) -> str:
        """Create a job and its items atomically. ``items`` is ``(key, hash, estimate)``."""
        job_id = uuid.uuid4().hex[:12]
        now = _now()
        item_rows = [
            (job_id, index, key, content_hash, estimate, now)
            for index, (key, content_hash, estimate) in enumerate(items)
        ]
        with self._tx() as c:
            c.execute(
                "INSERT INTO jobs (id, instruction, source, total_items, max_tokens, max_retries,"
                " concurrency, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (job_id, instruction, source, len(item_rows), max_tokens, max_retries, concurrency, now, now),
            )
            c.executemany(
                "INSERT INTO items (job_id, item_index, item_key, content_hash, estimated_tokens,"
                " updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                item_rows,
            )
            self._event(c, job_id, "job_created", f"{len(item_rows)} {'item' if len(item_rows) == 1 else 'items'} from {source}")
            for key, reason in skipped:
                self._event(c, job_id, "item_skipped", f"{key}: {reason}")
        return job_id

    def pause_job(self, job_id: str) -> bool:
        """Stop new claims. Returns False if the job was already paused."""
        with self._tx() as c:
            status = self._job(c, job_id)["status"]
            if status == "paused":
                return False
            if status in ("completed", "completed_with_errors"):
                raise JobStateError(f"job {job_id} has already finished ({status})")
            c.execute(
                "UPDATE jobs SET status = 'paused', updated_at = ? WHERE id = ?", (_now(), job_id)
            )
            self._event(c, job_id, "job_paused", "paused by user")
            return True

    def resume_job(self, job_id: str, max_tokens: Optional[int] = None) -> None:
        with self._tx() as c:
            job = self._job(c, job_id)
            if job["status"] != "paused":
                raise JobStateError(f"job {job_id} is not paused ({job['status']})")
            limit = job["max_tokens"] if max_tokens is None else max_tokens
            c.execute(
                "UPDATE jobs SET status = 'pending', max_tokens = ?, updated_at = ? WHERE id = ?",
                (limit, _now(), job_id),
            )
            self._sync_job(c, job_id)
            detail = f"token limit now {limit}" if max_tokens is not None else "resumed by user"
            self._event(c, job_id, "job_resumed", detail)

    def retry_failed(self, job_id: str) -> int:
        """Return failed items to ``pending``. Completed items are never touched."""
        with self._tx() as c:
            self._job(c, job_id)
            keys = [
                row["item_key"]
                for row in c.execute(
                    "SELECT item_key FROM items WHERE job_id = ? AND status = 'failed'"
                    " ORDER BY item_index",
                    (job_id,),
                )
            ]
            if not keys:
                return 0
            c.execute(
                "UPDATE items SET status = 'pending', updated_at = ?"
                " WHERE job_id = ? AND status = 'failed'",
                (_now(), job_id),
            )
            self._sync_job(c, job_id)
            for key in keys:
                self._event(c, job_id, "item_retry_requested", key)
            return len(keys)

    # ── Item transitions ────────────────────────────────────────────────────

    def claim_next(self, job_id: str, worker: str) -> Optional[sqlite3.Row]:
        """Atomically claim the lowest-indexed pending item for ``worker``.

        Returns ``None`` when no item is pending and raises ``JobBlocked`` when the job
        is paused or the next item does not fit in the token limit.
        """
        blocked: Optional[str] = None
        with self._tx() as c:
            job = self._job(c, job_id)
            row = c.execute(
                "SELECT id, item_key, estimated_tokens FROM items WHERE job_id = ?"
                " AND status = 'pending' ORDER BY item_index LIMIT 1",
                (job_id,),
            ).fetchone()
            if job["status"] == "paused":
                blocked = f"Job {job_id} is paused. Continue with: /bulk resume {job_id}"
            elif row is None:
                return None
            else:
                blocked = self._pause_if_over_limit(c, job, row)
            if blocked is None:
                c.execute(
                    "UPDATE items SET status = 'running', claimed_by = ?, attempts = attempts + 1,"
                    " reserved_tokens = estimated_tokens, last_error = NULL, updated_at = ?"
                    " WHERE id = ?",
                    (worker, _now(), row["id"]),
                )
                self._sync_job(c, job_id)
                self._event(c, job_id, "item_started", row["item_key"])
                return c.execute("SELECT * FROM items WHERE id = ?", (row["id"],)).fetchone()
        raise JobBlocked(blocked)

    def _pause_if_over_limit(
        self, c: sqlite3.Connection, job: sqlite3.Row, item: sqlite3.Row
    ) -> Optional[str]:
        limit = job["max_tokens"]
        if limit is None:
            return None
        usage = self._token_usage(c, job["id"])
        committed = usage["actual"] + usage["unreported"] + usage["reserved"]
        needed = item["estimated_tokens"]
        if committed + needed <= limit:
            return None
        message = (
            f"Token limit reached: {committed} of {limit} tokens are used or reserved, and "
            f"{item['item_key']} is estimated to need {needed}."
        )
        self._pause_for_limit(c, job["id"], message)
        return (
            f"{message} Job {job['id']} is paused. Raise the limit with: "
            f"/bulk resume {job['id']} --max-tokens <n>"
        )

    def _pause_for_limit(self, c: sqlite3.Connection, job_id: str, message: str) -> None:
        c.execute("UPDATE jobs SET status = 'paused', updated_at = ? WHERE id = ?", (_now(), job_id))
        self._event(c, job_id, "token_limit_reached", message)

    def record_failed_attempt(
        self, item_id: int, worker: str, error: str, attempt: int, max_attempts: int
    ) -> None:
        """Log a transient failure and count the attempt that follows it."""
        with self._tx() as c:
            item = self._claimed_item(c, item_id, worker)
            c.execute(
                "UPDATE items SET attempts = attempts + 1, last_error = ?, updated_at = ? WHERE id = ?",
                (error, _now(), item_id),
            )
            self._event(
                c,
                item["job_id"],
                "attempt_failed",
                f"{item['item_key']} attempt {attempt}/{max_attempts}: {error}",
            )

    def complete_item(
        self,
        item_id: int,
        worker: str,
        output_path: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: Optional[float],
        publish: Optional[Callable[[], None]] = None,
    ) -> None:
        """Finish an item. ``publish`` runs only once the claim has been verified."""
        with self._tx() as c:
            item = self._claimed_item(c, item_id, worker)
            if publish is not None:
                publish()
            c.execute(
                "UPDATE items SET status = 'completed', claimed_by = NULL, reserved_tokens = 0,"
                " output_path = ?, tokens_in = ?, tokens_out = ?, last_error = NULL, updated_at = ?"
                " WHERE id = ?",
                (output_path, tokens_in, tokens_out, _now(), item_id),
            )
            job_id = item["job_id"]
            if cost_usd is not None:
                c.execute(
                    "UPDATE jobs SET spent_usd = spent_usd + ? WHERE id = ?", (cost_usd, job_id)
                )
            self._sync_job(c, job_id)
            self._event(c, job_id, "item_completed", item["item_key"])
            job = self._job(c, job_id)
            usage = self._token_usage(c, job_id)
            used = usage["actual"] + usage["unreported"]
            if job["max_tokens"] is not None and used >= job["max_tokens"] and job["status"] in (
                "pending",
                "running",
            ):
                self._pause_for_limit(c, job_id, f"{used} of {job['max_tokens']} tokens used")

    def fail_item(self, item_id: int, worker: str, error: str) -> None:
        with self._tx() as c:
            item = self._claimed_item(c, item_id, worker)
            c.execute(
                "UPDATE items SET status = 'failed', claimed_by = NULL, reserved_tokens = 0,"
                " last_error = ?, updated_at = ? WHERE id = ?",
                (error, _now(), item_id),
            )
            self._sync_job(c, item["job_id"])
            self._event(
                c,
                item["job_id"],
                "item_failed",
                f"{item['item_key']} after {item['attempts']} attempts: {error}",
            )

    def release_item(self, item_id: int, worker: str) -> None:
        """Return an item this process stopped working on (for example Ctrl+C)."""
        self._requeue(item_id, worker, "item_interrupted", "stopped while processing")

    def recover_item(self, item_id: int, owner: Optional[str]) -> bool:
        """Requeue an item whose owner has stopped. False if someone else got there first."""
        return self._requeue(
            item_id,
            owner,
            "item_recovered",
            "returned to pending after its process stopped; the interrupted attempt may "
            "already have reached the model",
        )

    def record_event(self, job_id: str, event_type: str, message: str) -> None:
        with self._tx() as c:
            self._event(c, job_id, event_type, message)

    def _requeue(self, item_id: int, owner: Optional[str], event: str, message: str) -> bool:
        with self._tx() as c:
            item = c.execute(
                "SELECT job_id, item_key FROM items WHERE id = ? AND status = 'running'"
                " AND claimed_by IS ?",
                (item_id, owner),
            ).fetchone()
            if item is None:
                return False
            c.execute(
                "UPDATE items SET status = 'pending', claimed_by = NULL, reserved_tokens = 0,"
                " updated_at = ? WHERE id = ?",
                (_now(), item_id),
            )
            self._sync_job(c, item["job_id"])
            self._event(c, item["job_id"], event, f"{item['item_key']}: {message}")
            return True

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _job(c: sqlite3.Connection, job_id: str) -> sqlite3.Row:
        job = c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if job is None:
            raise JobStateError(f"no job with id {job_id}")
        return job

    @staticmethod
    def _claimed_item(c: sqlite3.Connection, item_id: int, worker: str) -> sqlite3.Row:
        item = c.execute(
            "SELECT job_id, item_key, attempts FROM items WHERE id = ? AND status = 'running'"
            " AND claimed_by = ?",
            (item_id, worker),
        ).fetchone()
        if item is None:
            raise StoreError(f"item {item_id} is not claimed by this worker")
        return item

    @staticmethod
    def _event(c: sqlite3.Connection, job_id: str, event_type: str, message: str) -> None:
        c.execute(
            "INSERT INTO events (job_id, event_type, message, created_at) VALUES (?, ?, ?, ?)",
            (job_id, event_type, message, _now()),
        )

    @staticmethod
    def _token_usage(c: sqlite3.Connection, job_id: str) -> sqlite3.Row:
        return c.execute(
            "SELECT COALESCE(SUM(tokens_in + tokens_out), 0) AS actual,"
            " COALESCE(SUM(CASE WHEN status = 'completed' AND tokens_in + tokens_out = 0"
            " THEN estimated_tokens ELSE 0 END), 0) AS unreported,"
            " COALESCE(SUM(reserved_tokens), 0) AS reserved,"
            " COALESCE(SUM(estimated_tokens), 0) AS estimated_total"
            " FROM items WHERE job_id = ?",
            (job_id,),
        ).fetchone()

    @staticmethod
    def _sync_job(c: sqlite3.Connection, job_id: str) -> None:
        current = c.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()["status"]
        counts = {
            row["status"]: row["n"]
            for row in c.execute(
                "SELECT status, COUNT(*) AS n FROM items WHERE job_id = ? GROUP BY status",
                (job_id,),
            )
        }
        if not counts.get("running") and not counts.get("pending"):
            status = "completed_with_errors" if counts.get("failed") else "completed"
        elif current == "paused":
            status = "paused"
        elif counts.get("running"):
            status = "running"
        else:
            status = "pending"
        c.execute(
            "UPDATE jobs SET status = ?, completed_items = ?, failed_items = ?, updated_at = ?"
            " WHERE id = ?",
            (status, counts.get("completed", 0), counts.get("failed", 0), _now(), job_id),
        )

    # ── Reads ───────────────────────────────────────────────────────────────

    def _all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def get_job(self, job_id: str) -> Optional[sqlite3.Row]:
        rows = self._all("SELECT * FROM jobs WHERE id = ?", (job_id,))
        return rows[0] if rows else None

    def list_jobs(self, limit: int = 10) -> list[sqlite3.Row]:
        return self._all("SELECT * FROM jobs ORDER BY created_at DESC, id LIMIT ?", (limit,))

    def get_items(self, job_id: str) -> list[sqlite3.Row]:
        return self._all("SELECT * FROM items WHERE job_id = ? ORDER BY item_index", (job_id,))

    def get_events(self, job_id: str, limit: int = 5) -> list[sqlite3.Row]:
        """The most recent ``limit`` events, oldest first."""
        rows = self._all(
            "SELECT * FROM events WHERE job_id = ? ORDER BY id DESC LIMIT ?", (job_id, limit)
        )
        return list(reversed(rows))

    def running_claims(self, job_id: Optional[str] = None) -> list[sqlite3.Row]:
        columns = "SELECT id, job_id, item_index, item_key, claimed_by FROM items WHERE status = 'running'"
        if job_id is None:
            return self._all(columns)
        return self._all(columns + " AND job_id = ?", (job_id,))

    def claim_owners(self, job_id: str) -> list[Optional[str]]:
        return [row["claimed_by"] for row in self.running_claims(job_id)]

    def token_usage(self, job_id: str) -> sqlite3.Row:
        with self._lock:
            return self._token_usage(self._conn, job_id)
