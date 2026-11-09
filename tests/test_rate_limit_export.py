"""Rate-limit waiting and combined export, with a stub in place of the model."""

from __future__ import annotations

import csv
import unittest

from bulk_plugin import executor
from bulk_plugin.jobs import MAX_RATE_LIMIT_WAITS, RateLimitGate
from tests import JobTestCase, StubResult


class FakeTime:
    def __init__(self) -> None:
        self.now = 1000.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


class RateLimited(Exception):
    status_code = 429


class HeaderResponse:
    def __init__(self, value) -> None:
        self.headers = {"retry-after": value}


class GateTests(unittest.TestCase):
    def test_wait_doubles_only_after_a_wait_has_ended(self) -> None:
        time = FakeTime()
        gate = RateLimitGate(time)
        self.assertEqual(gate.hit(), 5)
        self.assertEqual(gate.hit(), 5)  # another worker, during the same wait
        time.now += 5
        self.assertEqual(gate.hit(), 10)
        time.now += 10
        self.assertEqual(gate.hit(), 20)

    def test_wait_is_capped(self) -> None:
        time = FakeTime()
        gate = RateLimitGate(time)
        for _ in range(10):
            wait = gate.hit()
            time.now += wait
        self.assertEqual(wait, 120)

    def test_retry_after_is_used_when_longer(self) -> None:
        time = FakeTime()
        gate = RateLimitGate(time)
        self.assertEqual(gate.hit(42), 42)
        time.now += 42
        self.assertEqual(gate.hit(1), 10)

    def test_retry_after_cannot_exceed_the_cap(self) -> None:
        self.assertEqual(RateLimitGate(FakeTime()).hit(9999), 120)

    def test_success_resets_the_wait_once_it_has_ended(self) -> None:
        time = FakeTime()
        gate = RateLimitGate(time)
        gate.hit()
        gate.success()  # a call already in flight must not cancel the back-off
        time.now += 5
        self.assertEqual(gate.hit(), 10)
        time.now += 10
        gate.success()
        self.assertEqual(gate.hit(), 5)


class ClassificationTests(unittest.TestCase):
    def test_rate_limit_detection(self) -> None:
        class RateLimitError(Exception):
            pass

        self.assertTrue(executor.is_rate_limited(RateLimited()))
        self.assertTrue(executor.is_rate_limited(RateLimitError()))
        self.assertFalse(executor.is_rate_limited(TimeoutError()))

    def test_retry_after_parsing(self) -> None:
        def error(value) -> Exception:
            exc = Exception()
            exc.response = HeaderResponse(value)
            return exc

        self.assertEqual(executor.retry_after_seconds(error("30")), 30)
        for value in ("Wed, 21 Oct 2026 07:28:00 GMT", "-5", "0", "inf", None):
            self.assertIsNone(executor.retry_after_seconds(error(value)), value)
        self.assertIsNone(executor.retry_after_seconds(Exception()))


class RateLimitJobTests(JobTestCase):
    def use_fake_time(self) -> FakeTime:
        time = FakeTime()
        manager = self.runtime.manager()
        manager.rate_limit = RateLimitGate(time)
        manager._sleep = time.sleep
        return time

    def limited_then_ok(self, failures: int):
        remaining = [failures]

        def respond(_messages):
            if remaining[0] > 0:
                remaining[0] -= 1
                raise RateLimited("slow down")
            return StubResult("finally")

        return respond

    def test_item_waits_and_succeeds_without_using_its_retries(self) -> None:
        job_id = self.create_job("--retries 1")
        time = self.use_fake_time()
        self.llm.respond = self.limited_then_ok(4)
        self.assertIn("Completed q1.txt", self.run_one(job_id))
        self.assertEqual(time.waits, [5, 10, 20, 40])
        self.assertEqual(self.items(job_id)[0]["status"], "completed")
        message = [e["message"] for e in self.store.get_events(job_id, limit=100) if e["event_type"] == "attempt_failed"][0]
        self.assertEqual(message, "q1.txt attempt 1/8: RateLimited (HTTP 429); waiting 5s")

    def test_item_fails_after_the_wait_limit(self) -> None:
        job_id = self.create_job()
        self.use_fake_time()
        self.llm.respond = self.limited_then_ok(10_000)
        output = self.run_one(job_id)
        self.assertIn("Failed q1.txt", output)
        first = self.items(job_id)[0]
        self.assertEqual(first["status"], "failed")
        self.assertEqual(self.event_types(job_id).count("attempt_failed"), MAX_RATE_LIMIT_WAITS + 2)

    def test_retries_zero_turns_waiting_off(self) -> None:
        job_id = self.create_job("--retries 0")
        time = self.use_fake_time()
        self.llm.respond = self.limited_then_ok(1)
        self.assertIn("Failed q1.txt", self.run_one(job_id))
        self.assertEqual(time.waits, [])
        self.assertEqual(len(self.llm.calls), 1)

    def test_next_item_waits_before_it_is_claimed(self) -> None:
        job_id = self.create_job()
        manager = self.runtime.manager()
        time = self.use_fake_time()
        manager.rate_limit.hit()
        claimed_during_wait = []
        original = time.sleep

        def sleep(seconds: float) -> None:
            claimed_during_wait.append(sum(i["status"] == "running" for i in self.items(job_id)))
            original(seconds)

        manager._sleep = sleep
        self.run_one(job_id)
        self.assertEqual(claimed_during_wait, [0])

    def test_shutdown_during_a_wait_gives_the_item_back(self) -> None:
        job_id = self.create_job()
        manager = self.runtime.manager()
        self.use_fake_time()
        self.llm.respond = self.limited_then_ok(5)
        manager._sleep = lambda _seconds: manager.background._stopping.set()
        self.run_one(job_id)
        self.assertEqual(len(self.llm.calls), 1)
        first = self.items(job_id)[0]
        self.assertEqual((first["status"], first["claimed_by"]), ("pending", None))

    def test_background_job_recovers_from_a_burst_of_rate_limits(self) -> None:
        job_id = self.create_job("--concurrency 2")
        self.llm.respond = self.limited_then_ok(3)
        self.commands.handle(f"run {job_id}")
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_status_says_when_bulk_is_waiting(self) -> None:
        job_id = self.create_job("--concurrency 1")
        manager = self.runtime.manager()
        seen: list[str] = []

        def respond(_messages):
            manager.rate_limit.hit(60)
            seen.append(self.commands.handle(f"status {job_id}"))
            manager.rate_limit._until = 0.0
            return StubResult("ok")

        self.llm.respond = respond
        self.commands.handle(f"run {job_id}")
        self.wait_until_idle(job_id)
        self.assertIn("rate limited by the provider, waiting 60s", seen[0])


class ExportTests(JobTestCase):
    def finish(self, job_id: str) -> None:
        self.commands.handle(f"run {job_id}")
        self.wait_until_idle(job_id)

    def result_path(self, job_id: str, fmt: str):
        return self.data_dir / "jobs" / job_id / f"results.{fmt}"

    def test_markdown_export_combines_results_in_order(self) -> None:
        job_id = self.create_job()
        self.llm.respond = lambda m: StubResult(f"summary of {m[1]['content'].split('Item (')[1][:6]}")
        self.finish(job_id)
        calls = len(self.llm.calls)
        output = self.commands.handle(f"export {job_id}")
        self.assertIn("Exported 2 of 2 results to", output)
        text = self.result_path(job_id, "md").read_text(encoding="utf-8")
        self.assertLess(text.index("## q1.txt"), text.index("## q2.txt"))
        self.assertIn("summary of q1.txt", text)
        self.assertIn("Summarize this", text)
        self.assertEqual(len(self.llm.calls), calls)

    def test_csv_export_can_be_read_back_with_commas_and_newlines(self) -> None:
        job_id = self.create_job()
        self.llm.respond = lambda _m: StubResult('line one, with "quotes"\nline two')
        self.finish(job_id)
        self.assertIn("Exported 2 of 2", self.commands.handle(f"export {job_id} --format csv"))
        with open(self.result_path(job_id, "csv"), encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual([row["item"] for row in rows], ["q1.txt", "q2.txt"])
        self.assertEqual(rows[0]["result"], 'line one, with "quotes"\nline two')
        self.assertEqual(rows[0]["status"], "completed")

    def test_csv_neutralises_spreadsheet_formulas(self) -> None:
        job_id = self.create_job()
        self.llm.respond = lambda _m: StubResult('=HYPERLINK("http://evil","click")')
        self.finish(job_id)
        self.commands.handle(f"export {job_id} --format csv")
        with open(self.result_path(job_id, "csv"), encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertTrue(rows[0]["result"].startswith("'="))

    def test_failed_and_unfinished_items_are_listed_and_reported(self) -> None:
        job_id = self.create_job("--retries 0")

        def refuse(_messages):
            raise ValueError("refused")

        self.llm.respond = refuse
        self.run_one(job_id)
        output = self.commands.handle(f"export {job_id}")
        self.assertIn("Exported 0 of 2 results", output)
        self.assertIn("1 failed item listed", output)
        self.assertIn("1 item not finished", output)
        text = self.result_path(job_id, "md").read_text(encoding="utf-8")
        self.assertIn("_Failed: ValueError", text)
        self.assertIn("_Not finished (pending)._", text)

    def test_export_replaces_an_earlier_export_and_leaves_results_alone(self) -> None:
        job_id = self.create_job()
        self.finish(job_id)
        output_file = self.data_dir / self.items(job_id)[0]["output_path"]
        before = output_file.read_bytes()
        self.commands.handle(f"export {job_id}")
        self.commands.handle(f"export {job_id}")
        self.assertEqual(output_file.read_bytes(), before)
        leftovers = [p.name for p in self.result_path(job_id, "md").parent.iterdir() if p.suffix == ".tmp"]
        self.assertEqual(leftovers, [])

    def test_missing_result_file_is_reported(self) -> None:
        job_id = self.create_job()
        self.finish(job_id)
        (self.data_dir / self.items(job_id)[0]["output_path"]).unlink()
        output = self.commands.handle(f"export {job_id}")
        self.assertIn("Exported 1 of 2", output)
        self.assertIn("1 completed item had no result file", output)
        self.assertIn("_The result file is missing._", self.result_path(job_id, "md").read_text(encoding="utf-8"))

    def test_bad_arguments_are_explained(self) -> None:
        job_id = self.create_job()
        self.assertIn("no job with id nope", self.commands.handle("export nope"))
        self.assertIn("must be md or csv", self.commands.handle(f"export {job_id} --format pdf"))
        self.assertIn("usage: /bulk export", self.commands.handle(f"export {job_id} extra"))

    def test_status_points_to_export_once_something_is_done(self) -> None:
        job_id = self.create_job()
        self.assertNotIn("/bulk export", self.commands.handle(f"status {job_id}"))
        self.finish(job_id)
        self.assertIn(f"/bulk export {job_id}", self.commands.handle(f"status {job_id}"))
