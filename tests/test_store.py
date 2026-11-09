from __future__ import annotations

import sqlite3
import threading

from bulk_plugin import store as store_module
from bulk_plugin.errors import JobBlocked, StoreError
from bulk_plugin.store import Store
from tests import TempDirTestCase, open_test_db

ITEMS = [("a.txt", "hash-a", 100), ("b.txt", "hash-b", 100)]
WORKER = "worker1"


class StoreTests(TempDirTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.path = self.tmp / "data.db"
        self.store = Store(open_test_db(self.path))
        self.addCleanup(self.store.close)

    def columns(self, table: str) -> list[str]:
        return [row["name"] for row in self.store._all(f"PRAGMA table_info({table})")]

    def test_schema_has_specified_columns(self) -> None:
        self.assertEqual(self.columns("jobs"), [
            "id", "instruction", "source", "status", "total_items", "completed_items", "failed_items",
            "budget_usd", "spent_usd", "concurrency", "created_at", "updated_at",
            "max_tokens", "max_retries"])
        self.assertEqual(self.columns("items"), [
            "id", "job_id", "item_index", "item_key", "content_hash", "status", "attempts", "last_error",
            "tokens_in", "tokens_out", "output_path", "updated_at",
            "estimated_tokens", "reserved_tokens", "claimed_by"])
        self.assertEqual(self.columns("events"), ["id", "job_id", "event_type", "message", "created_at"])

    def test_indexes_and_foreign_keys(self) -> None:
        names = {row["name"] for row in self.store._all("SELECT name FROM sqlite_master WHERE type='index'")}
        self.assertLessEqual({"idx_jobs_created_at", "idx_items_job_status", "idx_events_job"}, names)
        self.assertEqual(self.store._all("PRAGMA foreign_keys")[0][0], 1)
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store._tx() as c:
                c.execute("INSERT INTO events (job_id, event_type, message, created_at)"
                          " VALUES ('missing', 'x', 'x', 'now')")

    def test_delete_job_cascades(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        with self.store._tx() as c:
            c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self.assertEqual(self.store.get_items(job_id), [])
        self.assertEqual(self.store.get_events(job_id), [])

    def test_create_job_inserts_items_in_order(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS, max_tokens=500, max_retries=1, concurrency=2)
        job = self.store.get_job(job_id)
        self.assertEqual((job["status"], job["total_items"], job["max_tokens"], job["max_retries"], job["concurrency"]),
                         ("pending", 2, 500, 1, 2))
        self.assertEqual([(i["item_index"], i["item_key"], i["status"], i["estimated_tokens"])
                          for i in self.store.get_items(job_id)],
                         [(0, "a.txt", "pending", 100), (1, "b.txt", "pending", 100)])

    def test_create_job_is_atomic(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.create_job("do", "src", [("same", "h1", 1), ("same", "h2", 1)])
        self.assertEqual(self.store.list_jobs(), [])
        self.assertEqual(self.store._all("SELECT * FROM items"), [])

    def test_claim_is_exclusive_and_counts_attempts(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        first, second = self.store.claim_next(job_id, WORKER), self.store.claim_next(job_id, WORKER)
        self.assertEqual((first["item_key"], first["attempts"], first["status"], first["claimed_by"]),
                         ("a.txt", 1, "running", WORKER))
        self.assertEqual(second["item_key"], "b.txt")
        self.assertIsNone(self.store.claim_next(job_id, WORKER))
        self.assertEqual(self.store.get_job(job_id)["status"], "running")

    def test_finishing_a_non_running_item_is_rejected(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        pending_id = self.store.get_items(job_id)[0]["id"]
        with self.assertRaises(StoreError):
            self.store.complete_item(pending_id, WORKER, "x", 1, 1, None)

    def test_only_the_claiming_worker_can_finish_an_item(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        item = self.store.claim_next(job_id, WORKER)
        published = []
        with self.assertRaises(StoreError):
            self.store.complete_item(item["id"], "other", "x", 1, 1, None, publish=lambda: published.append(1))
        with self.assertRaises(StoreError):
            self.store.fail_item(item["id"], "other", "boom")
        self.assertEqual(published, [])
        self.assertEqual(self.store.get_items(job_id)[0]["status"], "running")

    def test_completed_item_cannot_be_completed_again(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        item = self.store.claim_next(job_id, WORKER)
        self.store.complete_item(item["id"], WORKER, "first.md", 1, 2, None)
        published = []
        with self.assertRaises(StoreError):
            self.store.complete_item(item["id"], WORKER, "second.md", 9, 9, None, publish=lambda: published.append(1))
        self.assertEqual(published, [])
        row = self.store.get_items(job_id)[0]
        self.assertEqual((row["output_path"], row["tokens_in"]), ("first.md", 1))

    def test_failed_publish_rolls_back_the_transition(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        item = self.store.claim_next(job_id, WORKER)

        def fail():
            raise OSError("disk full")

        with self.assertRaises(OSError):
            self.store.complete_item(item["id"], WORKER, "x.md", 1, 1, None, publish=fail)
        row = self.store.get_items(job_id)[0]
        self.assertEqual((row["status"], row["output_path"]), ("running", None))
        self.assertEqual(self.store.get_job(job_id)["completed_items"], 0)

    def test_job_counters_and_terminal_status(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        first = self.store.claim_next(job_id, WORKER)
        self.store.complete_item(first["id"], WORKER, "out", 3, 4, 0.25)
        job = self.store.get_job(job_id)
        self.assertEqual((job["status"], job["completed_items"], job["spent_usd"]), ("pending", 1, 0.25))
        second = self.store.claim_next(job_id, WORKER)
        self.store.fail_item(second["id"], WORKER, "boom")
        job = self.store.get_job(job_id)
        self.assertEqual((job["status"], job["failed_items"]), ("completed_with_errors", 1))

    def test_data_survives_close_and_reopen(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS)
        claimed = self.store.claim_next(job_id, WORKER)
        self.store.complete_item(claimed["id"], WORKER, "out.md", 5, 6, None)
        self.store.close()
        reopened = Store(open_test_db(self.path))
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.get_job(job_id)["completed_items"], 1)
        items = reopened.get_items(job_id)
        self.assertEqual((items[0]["status"], items[0]["output_path"], items[0]["tokens_in"]),
                         ("completed", "out.md", 5))
        self.assertEqual(items[1]["status"], "pending")
        self.assertEqual(reopened.claim_next(job_id, WORKER)["item_key"], "b.txt")

    def test_newer_schema_is_refused(self) -> None:
        self.store._conn.execute(f"PRAGMA user_version = {store_module.SCHEMA_VERSION + 1}")
        with self.assertRaises(StoreError):
            Store(open_test_db(self.path))

    def test_two_workers_never_claim_the_same_item(self) -> None:
        job_id = self.store.create_job("do", "src", [(f"item-{n}", f"h{n}", 1) for n in range(40)])
        stores = [Store(open_test_db(self.path)) for _ in range(2)]
        for store in stores:
            self.addCleanup(store.close)
        barrier = threading.Barrier(2)
        claimed: list[list[str]] = [[], []]
        errors: list[BaseException] = []

        def work(index: int) -> None:
            try:
                barrier.wait()
                while (item := stores[index].claim_next(job_id, f"worker{index}")) is not None:
                    claimed[index].append(item["item_key"])
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(index,)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        everything = claimed[0] + claimed[1]
        self.assertEqual(len(everything), 40)
        self.assertEqual(len(set(everything)), 40)
        attempts = {item["attempts"] for item in self.store.get_items(job_id)}
        self.assertEqual(attempts, {1})

    def test_reservation_blocks_a_second_claim_and_is_released(self) -> None:
        job_id = self.store.create_job("do", "src", ITEMS, max_tokens=150)
        first = self.store.claim_next(job_id, WORKER)
        self.assertEqual(self.store.token_usage(job_id)["reserved"], 100)
        with self.assertRaisesRegex(JobBlocked, "Token limit reached"):
            self.store.claim_next(job_id, "worker2")
        self.assertEqual(self.store.get_job(job_id)["status"], "paused")
        self.store.complete_item(first["id"], WORKER, "out", 20, 10, None)
        usage = self.store.token_usage(job_id)
        self.assertEqual((usage["reserved"], usage["actual"]), (0, 30))


class MigrationTests(TempDirTestCase):
    def test_version_1_database_is_upgraded_in_place(self) -> None:
        path = self.tmp / "data.db"
        conn = open_test_db(path)
        for statement in store_module._V1:
            conn.execute(statement)
        conn.execute("PRAGMA user_version = 1")
        conn.execute("INSERT INTO jobs (id, instruction, source, status, total_items, completed_items,"
                     " budget_usd, created_at, updated_at) VALUES ('j1', 'do', 'src', 'running', 2, 1, 1.5, 'n', 'n')")
        conn.execute("INSERT INTO items (job_id, item_index, item_key, content_hash, status, tokens_in,"
                     " tokens_out, output_path, updated_at) VALUES ('j1', 0, 'a', 'h', 'completed', 5, 6, 'o.md', 'n')")
        conn.execute("INSERT INTO items (job_id, item_index, item_key, content_hash, status, updated_at)"
                     " VALUES ('j1', 1, 'b', 'h', 'running', 'n')")
        conn.commit()
        conn.close()

        store = Store(open_test_db(path))
        self.addCleanup(store.close)
        self.assertEqual(store._all("PRAGMA user_version")[0][0], store_module.SCHEMA_VERSION)
        job = store.get_job("j1")
        self.assertEqual((job["completed_items"], job["budget_usd"], job["max_tokens"], job["max_retries"]),
                         (1, 1.5, None, 2))
        items = store.get_items("j1")
        self.assertEqual((items[0]["status"], items[0]["tokens_in"], items[0]["output_path"]),
                         ("completed", 5, "o.md"))
        self.assertEqual((items[1]["status"], items[1]["claimed_by"]), ("running", None))
        self.assertTrue(store.recover_item(items[1]["id"], None))
        self.assertEqual(store.get_items("j1")[1]["status"], "pending")
