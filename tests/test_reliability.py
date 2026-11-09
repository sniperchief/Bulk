"""Recovery, retries, pause/resume and token limits, with a stub in place of the model.

The interruption tests use real separate processes that end without any cleanup.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from bulk_plugin import executor
from bulk_plugin.commands import BulkCommands
from bulk_plugin.errors import EmptyModelResponse
from bulk_plugin.worker import WorkerLock, owner_is_alive
from tests import JobTestCase, StubResult, StubUsage, TempDirTestCase

WORKER_SCRIPT = str(Path(__file__).resolve().parent / "_worker_process.py")


def transient_then(result: StubResult, failures: int = 1):
    remaining = [failures]

    def respond(_messages):
        if remaining[0] > 0:
            remaining[0] -= 1
            raise TimeoutError("provider timed out")
        return result

    return respond


class HttpError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class RecoveryTests(JobTestCase):
    def start_worker(self, job_id: str, mode: str) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, WORKER_SCRIPT, str(self.data_dir), job_id, mode],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

    def fresh_commands(self) -> BulkCommands:
        self.runtime.close()
        self.runtime = self.make_runtime(self.llm)
        self.addCleanup(self.runtime.close)
        return BulkCommands(self.runtime)

    def test_process_killed_during_model_call_is_recovered_on_startup(self) -> None:
        job_id = self.create_job()
        worker = self.start_worker(job_id, "crash-in-model")
        stdout, stderr = worker.communicate(timeout=60)
        self.assertEqual(worker.returncode, 9, stderr)
        self.assertIn("in-model", stdout)
        self.assertEqual(self.items(job_id)[0]["status"], "running")

        commands = self.fresh_commands()
        first = self.items(job_id)[0]
        self.assertEqual((first["status"], first["claimed_by"], first["reserved_tokens"], first["attempts"]),
                         ("pending", None, 0, 1))
        self.assertEqual(self.job(job_id)["status"], "pending")
        self.assertIn("item_recovered", self.event_types(job_id))
        self.assertIn("may already have reached the model", commands.handle(f"status {job_id}"))

        self.assertIn("Completed q1.txt", self.run_one(job_id))
        self.assertEqual(self.items(job_id)[0]["attempts"], 2)

    def test_item_owned_by_a_live_process_is_not_taken_over(self) -> None:
        job_id = self.create_job()
        worker = self.start_worker(job_id, "hold")
        self.addCleanup(worker.kill)
        self.assertEqual(worker.stdout.readline().strip(), "claimed")

        self.assertEqual(self.runtime.manager().recover(job_id), 0)
        self.assertIn("Completed q2.txt", self.run_one(job_id))
        self.assertEqual(self.items(job_id)[0]["status"], "running")
        self.assertEqual(len(self.llm.calls), 1)

        worker.communicate(timeout=60)
        self.assertEqual(self.runtime.manager().recover(job_id), 1)
        self.assertEqual(self.items(job_id)[0]["status"], "pending")

    def test_run_recovers_an_item_from_a_process_that_died_after_startup(self) -> None:
        job_id = self.create_job()
        self.runtime.manager()  # this process has already done its startup recovery
        worker = self.start_worker(job_id, "crash-in-model")
        worker.communicate(timeout=60)
        self.assertIn("Completed q1.txt", self.run_one(job_id))

    def test_resume_recovers_interrupted_items(self) -> None:
        job_id = self.create_job()
        worker = self.start_worker(job_id, "crash-in-model")
        worker.communicate(timeout=60)
        self.commands.handle(f"pause {job_id}")
        output = self.commands.handle(f"resume {job_id}")
        self.assertIn("Recovered 1 interrupted item.", output)
        self.wait_until_idle(job_id)
        self.assertEqual(self.items(job_id)[0]["attempts"], 2)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_recovery_keeps_a_paused_job_paused(self) -> None:
        job_id = self.create_job()
        worker = self.start_worker(job_id, "crash-in-model")
        worker.communicate(timeout=60)
        self.commands.handle(f"pause {job_id}")
        self.fresh_commands()
        self.assertEqual(self.items(job_id)[0]["status"], "pending")
        self.assertEqual(self.job(job_id)["status"], "paused")

    def test_claim_with_no_owner_from_phase_1_is_recovered(self) -> None:
        job_id = self.create_job()
        with self.store._tx() as c:
            c.execute("UPDATE items SET status = 'running', claimed_by = NULL WHERE job_id = ? AND item_index = 0",
                      (job_id,))
        self.assertEqual(self.runtime.manager().recover(job_id), 1)

    def test_leftover_staged_output_is_removed_on_recovery(self) -> None:
        job_id = self.create_job()
        dead = WorkerLock(self.data_dir / "workers")
        dead_id = dead.id
        item = self.store.claim_next(job_id, dead_id)
        dead.release()
        staged = self.data_dir / "jobs" / job_id / "outputs" / f"{item['item_index']:06d}.{dead_id}.tmp"
        staged.parent.mkdir(parents=True)
        staged.write_text("partial", encoding="utf-8")
        self.runtime.manager().recover(job_id)
        self.assertFalse(staged.exists())

    def test_ctrl_c_during_the_model_call_releases_the_item(self) -> None:
        job_id = self.create_job()

        def interrupt(_messages):
            raise KeyboardInterrupt

        self.llm.respond = interrupt
        with self.assertRaises(KeyboardInterrupt):
            self.run_one(job_id)
        first = self.items(job_id)[0]
        self.assertEqual((first["status"], first["claimed_by"], first["reserved_tokens"]), ("pending", None, 0))
        self.assertIn("item_interrupted", self.event_types(job_id))


class WorkerLockTests(TempDirTestCase):
    def test_lock_reports_owner_state(self) -> None:
        directory = self.tmp / "workers"
        lock = WorkerLock(directory)
        self.assertTrue(owner_is_alive(directory, lock.id))
        lock.release()
        self.assertFalse(owner_is_alive(directory, lock.id))

    def test_unknown_or_missing_owner_is_not_alive(self) -> None:
        for worker_id in (None, "", "../evil", "abc123"):
            self.assertFalse(owner_is_alive(self.tmp, worker_id))


class RetryTests(JobTestCase):
    def test_transient_failure_succeeds_on_retry(self) -> None:
        job_id = self.create_job()
        self.llm.respond = transient_then(StubResult("recovered output"))
        output = self.run_one(job_id)
        self.assertIn("Completed q1.txt", output)
        self.assertEqual(len(self.llm.calls), 2)
        self.assertEqual(self.sleeps, [2])
        first = self.items(job_id)[0]
        self.assertEqual((first["status"], first["attempts"], first["last_error"]), ("completed", 2, None))
        events = self.store.get_events(job_id, limit=1000)
        failure = next(event for event in events if event["event_type"] == "attempt_failed")
        self.assertEqual(failure["message"], "q1.txt attempt 1/3: TimeoutError (details are in the Hermes log)")

    def test_retries_exhausted_marks_item_failed(self) -> None:
        job_id = self.create_job()
        self.llm.respond = transient_then(StubResult("never"), failures=99)
        output = self.run_one(job_id)
        self.assertIn("Failed q1.txt", output)
        self.assertEqual(len(self.llm.calls), 3)
        self.assertEqual(self.sleeps, [2, 4])
        first = self.items(job_id)[0]
        self.assertEqual((first["status"], first["attempts"], first["claimed_by"], first["reserved_tokens"]),
                         ("failed", 3, None, 0))
        self.assertEqual(self.event_types(job_id).count("attempt_failed"), 2)
        self.assertEqual(self.event_types(job_id).count("item_failed"), 1)
        self.assertEqual(self.job(job_id)["failed_items"], 1)

    def test_retry_limit_is_configurable_per_job(self) -> None:
        job_id = self.create_job("--retries 0")
        self.llm.respond = transient_then(StubResult("never"), failures=99)
        self.run_one(job_id)
        self.assertEqual(len(self.llm.calls), 1)

    def test_default_retry_limit_comes_from_plugin_setting(self) -> None:
        runtime = self.make_runtime(self.llm, settings={"max_retries": 5})
        self.addCleanup(runtime.close)
        self.assertEqual(runtime.default_retries(), 5)
        for bad in ("many", -1, None):
            other = self.make_runtime(self.llm, settings={"max_retries": bad})
            self.addCleanup(other.close)
            self.assertEqual(other.default_retries(), 2)

    def test_permanent_error_is_not_retried(self) -> None:
        job_id = self.create_job()

        def refuse(_messages):
            raise HttpError(401)

        self.llm.respond = refuse
        self.run_one(job_id)
        self.assertEqual(len(self.llm.calls), 1)
        self.assertEqual(self.items(job_id)[0]["status"], "failed")

    def test_stored_error_never_contains_the_providers_message(self) -> None:
        class ProviderError(Exception):
            status_code = 401

        secret = "Incorrect API key provided: sk-live-abc123"
        self.assertEqual(executor.describe_error(ProviderError(secret)), "ProviderError (HTTP 401)")
        self.assertNotIn("sk-live", executor.describe_error(ValueError(secret)))
        self.assertNotIn("sk-live", executor.describe_error(TimeoutError(secret)))
        self.assertNotIn("sk-live", executor.describe_error(ConnectionError(secret)))
        self.assertEqual(executor.describe_error(EmptyModelResponse("no text")), "EmptyModelResponse: no text")
        self.assertEqual(executor.describe_error(FileNotFoundError(2, "No such file", secret)),
                         "FileNotFoundError: No such file")

    def test_transient_classification(self) -> None:
        class RateLimitError(Exception):
            pass

        for error in (TimeoutError(), ConnectionResetError(), HttpError(429), HttpError(503),
                      RateLimitError(), EmptyModelResponse("x")):
            self.assertTrue(executor.is_transient(error), repr(error))
        for error in (HttpError(400), HttpError(401), RuntimeError("no provider"), ValueError(), PermissionError()):
            self.assertFalse(executor.is_transient(error), repr(error))

    def test_manual_retry_processes_failed_item_again(self) -> None:
        job_id = self.create_job("--retries 0")
        self.llm.respond = transient_then(StubResult("never"), failures=1)
        self.run_one(job_id)
        self.run_one(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed_with_errors")
        second_file = self.data_dir / self.items(job_id)[1]["output_path"]
        before = second_file.read_bytes()

        self.llm.respond = lambda _m: StubResult("second chance")
        self.assertIn("Returned 1 failed item to pending", self.commands.handle(f"retry {job_id}"))
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")
        first, second = self.items(job_id)
        self.assertEqual((first["attempts"], first["last_error"]), (2, None))
        self.assertEqual((self.data_dir / first["output_path"]).read_text(encoding="utf-8"), "second chance")
        self.assertEqual(second["attempts"], 1)
        self.assertEqual(second_file.read_bytes(), before)
        self.assertIn("item_retry_requested", self.event_types(job_id))

    def test_manual_retry_with_nothing_failed(self) -> None:
        job_id = self.create_job()
        self.assertIn("no failed items", self.commands.handle(f"retry {job_id}"))


class PauseResumeTests(JobTestCase):
    def test_pause_stops_new_claims(self) -> None:
        job_id = self.create_job()
        self.assertIn("Paused job", self.commands.handle(f"pause {job_id}"))
        output = self.run_one(job_id)
        self.assertIn("is paused", output)
        self.assertEqual(self.llm.calls, [])
        self.assertEqual(self.items(job_id)[0]["status"], "pending")
        self.assertIn("already paused", self.commands.handle(f"pause {job_id}"))

    def test_item_in_progress_finishes_and_job_stays_paused(self) -> None:
        job_id = self.create_job()

        def pause_during_call(_messages):
            self.store.pause_job(job_id)
            return StubResult("finished anyway")

        self.llm.respond = pause_during_call
        self.assertIn("Completed q1.txt", self.run_one(job_id))
        self.assertEqual(self.items(job_id)[0]["status"], "completed")
        self.assertEqual(self.job(job_id)["status"], "paused")

    def test_resume_continues_without_touching_completed_items(self) -> None:
        job_id = self.create_job()
        self.llm.respond = lambda _m: StubResult("first result", usage=StubUsage(50, 5))
        self.run_one(job_id)
        first_before = dict(self.items(job_id)[0])
        output_file = self.data_dir / first_before["output_path"]
        file_before = output_file.read_bytes()
        calls_before = len(self.llm.calls)

        self.commands.handle(f"pause {job_id}")
        self.llm.respond = lambda _m: StubResult("second result")
        self.assertIn("Resumed job", self.commands.handle(f"resume {job_id}"))
        self.wait_until_idle(job_id)
        self.assertIn("has no pending items", self.run_one(job_id))

        self.assertEqual(len(self.llm.calls), calls_before + 1)
        self.assertEqual(dict(self.items(job_id)[0]), first_before)
        self.assertEqual(output_file.read_bytes(), file_before)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_job_finishing_while_paused_becomes_terminal(self) -> None:
        job_id = self.create_job()
        self.run_one(job_id)

        def pause_during_call(_messages):
            self.store.pause_job(job_id)
            return StubResult("last")

        self.llm.respond = pause_during_call
        self.run_one(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_invalid_transitions_are_explained(self) -> None:
        job_id = self.create_job()
        self.assertIn("is not paused", self.commands.handle(f"resume {job_id}"))
        self.run_one(job_id)
        self.run_one(job_id)
        self.assertIn("already finished", self.commands.handle(f"pause {job_id}"))

    def test_pause_state_survives_restart(self) -> None:
        job_id = self.create_job()
        self.commands.handle(f"pause {job_id}")
        self.runtime.close()
        reopened = self.make_runtime(self.llm)
        self.addCleanup(reopened.close)
        self.assertIn("is paused", BulkCommands(reopened).handle(f"run {job_id}"))


class TokenLimitTests(JobTestCase):
    def test_item_that_does_not_fit_pauses_without_a_model_call(self) -> None:
        job_id = self.create_job("--max-tokens 10")
        output = self.run_one(job_id)
        self.assertIn("Token limit reached", output)
        self.assertIn(f"/bulk resume {job_id} --max-tokens", output)
        self.assertEqual(self.llm.calls, [])
        self.assertEqual(self.job(job_id)["status"], "paused")
        self.assertIn("token_limit_reached", self.event_types(job_id))

    def test_actual_usage_is_tracked_and_reservation_is_released(self) -> None:
        job_id = self.create_job("--max-tokens 100000")
        seen: list[int] = []

        def respond(_messages):
            seen.append(self.store.token_usage(job_id)["reserved"])
            return StubResult("ok", usage=StubUsage(120, 30))

        self.llm.respond = respond
        self.run_one(job_id)
        self.assertEqual(seen, [self.items(job_id)[0]["estimated_tokens"]])
        usage = self.store.token_usage(job_id)
        self.assertEqual((usage["actual"], usage["reserved"]), (150, 0))

    def test_job_pauses_when_actual_usage_reaches_the_limit(self) -> None:
        estimate = max(item["estimated_tokens"] for item in self.items(self.create_job()))
        job_id = self.create_job(f"--max-tokens {estimate}")
        self.llm.respond = lambda _m: StubResult("big", usage=StubUsage(estimate, 10))
        self.assertIn("Completed q1.txt", self.run_one(job_id))
        self.assertEqual(self.job(job_id)["status"], "paused")
        self.assertIn("is paused", self.run_one(job_id))
        self.assertEqual(len(self.llm.calls), 1)

    def test_resume_can_raise_the_limit(self) -> None:
        job_id = self.create_job("--max-tokens 10")
        self.run_one(job_id)
        self.commands.handle(f"resume {job_id} --max-tokens 100000")
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["max_tokens"], 100000)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_resume_without_raising_the_limit_pauses_again(self) -> None:
        job_id = self.create_job("--max-tokens 10")
        self.run_one(job_id)
        self.commands.handle(f"resume {job_id}")
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "paused")
        self.assertEqual(self.event_types(job_id).count("token_limit_reached"), 2)
        self.assertEqual(self.llm.calls, [])

    def test_missing_usage_counts_the_estimate_against_the_limit(self) -> None:
        job_id = self.create_job("--max-tokens 100000")
        self.llm.respond = lambda _m: StubResult("ok", usage=StubUsage(0, 0))
        self.run_one(job_id)
        estimate = self.items(job_id)[0]["estimated_tokens"]
        usage = self.store.token_usage(job_id)
        self.assertEqual((usage["actual"], usage["unreported"]), (0, estimate))
        self.assertIn(f"Token limit: {estimate} of 100000", self.commands.handle(f"status {job_id}"))

    def test_status_separates_actual_reserved_and_estimated(self) -> None:
        job_id = self.create_job("--max-tokens 100000")
        captured: list[str] = []

        def respond(_messages):
            captured.append(self.commands._job_text(self.runtime.manager(), job_id))
            return StubResult("ok", usage=StubUsage(40, 2))

        self.llm.respond = respond
        self.run_one(job_id)
        self.assertIn("Reserved tokens (estimated, in progress):", captured[0])
        output = self.commands.handle(f"status {job_id}")
        self.assertIn("Actual tokens (reported by Hermes): 40 in, 2 out", output)
        self.assertIn("not a billing limit", output)
        self.assertNotIn("Reserved tokens", output)

    def test_estimate_scales_with_content_and_never_truncates(self) -> None:
        small = executor.Item("a", "x" * 300)
        large = executor.Item("a", "x" * 300_000)
        self.assertGreater(executor.estimate_tokens("do", large), executor.estimate_tokens("do", small) * 100)
