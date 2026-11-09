"""Background processing with a stub in place of the model.

The gateway-exit test uses a real separate process that ends with ``os._exit``. These
tests are not evidence that concurrent real model calls work; see README.md.
"""

from __future__ import annotations

import contextvars
import os
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

from bulk_plugin.commands import BulkCommands
from bulk_plugin.runtime import gateway_is_stopping
from bulk_plugin.worker import WorkerLock, sweep_stale_locks
from tests import JobTestCase, StubResult, StubUsage, TempDirTestCase

WORKER_SCRIPT = str(Path(__file__).resolve().parent / "_worker_process.py")
WAIT = 30


class Gate:
    """Holds stub model calls until released, and records how many overlap."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.inside = 0
        self.peak = 0
        self.entered = threading.Semaphore(0)

    def __call__(self, _messages) -> StubResult:
        with self.lock:
            self.inside += 1
            self.peak = max(self.peak, self.inside)
        self.entered.release()
        released = self.release.wait(WAIT)
        with self.lock:
            self.inside -= 1
        if not released:
            raise AssertionError("gate was never released")
        return StubResult("gated output")

    def wait_for_calls(self, test: JobTestCase, count: int) -> None:
        for _ in range(count):
            test.assertTrue(self.entered.acquire(timeout=WAIT), "expected model call did not start")


def item_key(messages) -> str:
    return messages[1]["content"].split("Item (", 1)[1].split("):", 1)[0]


class BackgroundTestCase(JobTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.addCleanup(self.open_gates)
        self.gates: list[Gate] = []

    def open_gates(self) -> None:
        for gate in self.gates:
            gate.release.set()

    def gate(self) -> Gate:
        gate = Gate()
        self.gates.append(gate)
        self.llm.respond = gate
        return gate

    def create_many(self, count: int, options: str = "") -> str:
        source = self.tmp / "work" / "many"
        source.mkdir(exist_ok=True)
        for child in source.iterdir():
            child.unlink()
        for index in range(count):
            (source / f"item{index:03d}.txt").write_text(f"content of item {index}", encoding="utf-8")
        output = self.commands.handle(f'"Summarize this" --over many {options}')
        self.assertIn(f"with {count} item", output)
        return output.split()[2]

    def run_job(self, job_id: str) -> str:
        return self.commands.handle(f"run {job_id}")

    def status(self, job_id: str) -> str:
        return self.commands.handle(f"status {job_id}")

    def outputs(self, job_id: str) -> dict[str, bytes]:
        return {
            item["item_key"]: (self.data_dir / item["output_path"]).read_bytes()
            for item in self.items(job_id)
            if item["status"] == "completed"
        }


class RunTests(BackgroundTestCase):
    def test_one_command_processes_every_item(self) -> None:
        job_id = self.create_many(25)
        self.assertIn("in the background", self.run_job(job_id))
        self.wait_until_idle(job_id)
        self.assertEqual(len(self.llm.calls), 25)
        job = self.job(job_id)
        self.assertEqual((job["status"], job["completed_items"]), ("completed", 25))
        self.assertEqual(len(self.outputs(job_id)), 25)
        self.assertIn("State: completed", self.status(job_id))

    def test_command_returns_while_the_model_is_still_working(self) -> None:
        job_id = self.create_many(3, "--concurrency 1")
        gate = self.gate()
        started = time.monotonic()
        output = self.run_job(job_id)
        self.assertLess(time.monotonic() - started, 5)
        self.assertIn("in the background", output)
        gate.wait_for_calls(self, 1)
        self.assertFalse(gate.release.is_set())
        self.assertIn("State: processing", self.status(job_id))
        self.assertIn("1 running", self.status(job_id))
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["completed_items"], 3)

    def test_second_run_does_not_add_workers(self) -> None:
        job_id = self.create_many(10, "--concurrency 2")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 2)
        self.assertIn("already being processed", self.run_job(job_id))
        self.assertEqual(self.runtime.manager().background.worker_count(job_id), 2)
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(gate.peak, 2)
        self.assertEqual(len(self.llm.calls), 10)

    def test_run_on_a_finished_job_makes_no_model_calls(self) -> None:
        job_id = self.create_many(2)
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        self.assertIn("has no pending items", self.run_job(job_id))
        self.assertEqual(len(self.llm.calls), 2)

    def test_workers_inherit_the_callers_context(self) -> None:
        profile = contextvars.ContextVar("profile", default="none")
        job_id = self.create_many(6, "--concurrency 3")
        seen: list[str] = []

        def respond(_messages):
            seen.append(profile.get())
            return StubResult("ok")

        self.llm.respond = respond
        token = profile.set("work-profile")
        try:
            self.run_job(job_id)
        finally:
            profile.reset(token)
        self.wait_until_idle(job_id)
        self.assertEqual(seen, ["work-profile"] * 6)


class ConcurrencyTests(BackgroundTestCase):
    def test_limit_is_reached_and_never_exceeded(self) -> None:
        job_id = self.create_many(12, "--concurrency 3")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 3)
        time.sleep(0.05)  # a fourth worker, if one existed, would enter here
        self.assertEqual(gate.inside, 3)
        self.assertEqual(len([i for i in self.items(job_id) if i["status"] == "running"]), 3)
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(gate.peak, 3)

    def test_default_concurrency_is_four(self) -> None:
        job_id = self.create_many(8)
        gate = self.gate()
        self.assertIn("up to 4 items at once", self.run_job(job_id))
        gate.wait_for_calls(self, 4)
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(gate.peak, 4)

    def test_upper_bound_is_configurable_and_enforced(self) -> None:
        self.assertIn("at most 16", self.commands.handle('"x" --over reports --concurrency 17'))
        runtime = self.make_runtime(self.llm, settings={"max_concurrency": 2})
        self.addCleanup(runtime.close)
        commands = BulkCommands(runtime)
        self.assertIn("at most 2", commands.handle('"x" --over reports --concurrency 3'))
        for bad in ("many", 0, None):
            other = self.make_runtime(self.llm, settings={"max_concurrency": bad})
            self.addCleanup(other.close)
            self.assertEqual(other.max_concurrency(), 16)

    def test_lowered_bound_applies_to_an_existing_job(self) -> None:
        job_id = self.create_many(6, "--concurrency 4")
        self.runtime.close()
        self.runtime = self.make_runtime(self.llm, settings={"max_concurrency": 2})
        self.addCleanup(self.runtime.close)
        gate = self.gate()
        self.assertIn("up to 2 items at once", BulkCommands(self.runtime).handle(f"run {job_id}"))
        gate.wait_for_calls(self, 2)
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(gate.peak, 2)

    def test_no_item_is_processed_twice(self) -> None:
        job_id = self.create_many(60, "--concurrency 8")
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        keys = [item_key(call["messages"]) for call in self.llm.calls]
        self.assertEqual(len(keys), 60)
        self.assertEqual(len(set(keys)), 60)
        self.assertTrue(all(item["attempts"] == 1 for item in self.items(job_id)))

    def test_two_managers_on_one_database_never_share_an_item(self) -> None:
        job_id = self.create_many(40, "--concurrency 4")
        other = self.make_runtime(self.llm)
        self.addCleanup(other.close)
        self.run_job(job_id)
        BulkCommands(other).handle(f"run {job_id}")
        self.wait_until_idle(job_id)
        self.wait_until_idle(job_id, other)
        keys = [item_key(call["messages"]) for call in self.llm.calls]
        self.assertEqual(sorted(keys), sorted(set(keys)))
        self.assertEqual(len(keys), 40)


class PauseResumeTests(BackgroundTestCase):
    def test_pause_stops_new_claims_and_in_flight_items_finish(self) -> None:
        job_id = self.create_many(10, "--concurrency 2")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 2)
        self.assertIn("Paused job", self.commands.handle(f"pause {job_id}"))
        self.assertIn("paused (2 in progress may still finish)", self.status(job_id))
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(len(self.llm.calls), 2)
        job = self.job(job_id)
        self.assertEqual((job["status"], job["completed_items"]), ("paused", 2))
        self.assertIn("State: paused", self.status(job_id))

    def test_run_on_a_paused_job_starts_nothing(self) -> None:
        job_id = self.create_many(3)
        self.commands.handle(f"pause {job_id}")
        self.assertIn("is paused", self.run_job(job_id))
        self.assertFalse(self.runtime.manager().background.is_active(job_id))
        self.assertEqual(self.llm.calls, [])

    def test_resume_finishes_the_job_and_leaves_results_alone(self) -> None:
        job_id = self.create_many(10, "--concurrency 2")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 2)
        self.commands.handle(f"pause {job_id}")
        gate.release.set()
        self.wait_until_idle(job_id)
        before = self.outputs(job_id)
        rows_before = {item["item_key"]: dict(item) for item in self.items(job_id) if item["status"] == "completed"}
        self.assertEqual(len(before), 2)

        self.llm.respond = lambda _m: StubResult("later output")
        self.assertIn("in the background", self.commands.handle(f"resume {job_id}"))
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")
        self.assertEqual(len(self.llm.calls), 10)
        after = self.outputs(job_id)
        for key, content in before.items():
            self.assertEqual(after[key], content)
        items = {item["item_key"]: dict(item) for item in self.items(job_id)}
        for key, row in rows_before.items():
            self.assertEqual(items[key], row)

    def test_resume_while_workers_are_still_leaving(self) -> None:
        job_id = self.create_many(12, "--concurrency 3")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 3)
        self.commands.handle(f"pause {job_id}")
        self.commands.handle(f"resume {job_id}")
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")
        self.assertEqual(len(self.llm.calls), 12)
        self.assertLessEqual(gate.peak, 3)

    def test_retry_processes_failed_items_in_the_background(self) -> None:
        job_id = self.create_many(4, "--retries 0")

        def refuse(_messages):
            raise ValueError("refused")

        self.llm.respond = refuse
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        self.assertIn("State: failed (every item failed)", self.status(job_id))
        self.llm.respond = lambda _m: StubResult("second chance")
        self.assertIn("Returned 4 failed items", self.commands.handle(f"retry {job_id}"))
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_retry_on_a_paused_job_waits_for_resume(self) -> None:
        job_id = self.create_many(2, "--retries 0")
        self.llm.respond = lambda _m: (_ for _ in ()).throw(ValueError("refused"))
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        calls = len(self.llm.calls)
        self.store.retry_failed(job_id)
        self.commands.handle(f"pause {job_id}")
        self.assertIn("is paused", self.run_job(job_id))
        self.assertEqual(len(self.llm.calls), calls)


class TokenLimitTests(BackgroundTestCase):
    def test_limit_pauses_a_running_job_and_resume_continues(self) -> None:
        estimate_job = self.create_many(10)
        estimate = self.items(estimate_job)[0]["estimated_tokens"]
        job_id = self.create_many(10, f"--max-tokens {estimate * 3} --concurrency 2")
        self.llm.respond = lambda _m: StubResult("ok", usage=StubUsage(estimate, 0))
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        job = self.job(job_id)
        self.assertEqual(job["status"], "paused")
        self.assertEqual(job["completed_items"], 3)
        self.assertEqual(len(self.llm.calls), 3)
        self.assertIn("State: paused", self.status(job_id))
        self.assertEqual(self.store.token_usage(job_id)["reserved"], 0)
        self.assertIn("token_limit_reached", self.event_types(job_id))

        before = self.outputs(job_id)
        self.commands.handle(f"resume {job_id} --max-tokens {estimate * 100}")
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")
        after = self.outputs(job_id)
        self.assertEqual({key: after[key] for key in before}, before)

    def test_concurrent_reservations_never_exceed_the_limit(self) -> None:
        estimate_job = self.create_many(10)
        estimate = self.items(estimate_job)[0]["estimated_tokens"]
        job_id = self.create_many(10, f"--max-tokens {estimate * 2} --concurrency 5")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 2)
        time.sleep(0.05)
        self.assertEqual(gate.inside, 2)
        self.assertEqual(self.job(job_id)["status"], "paused")
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertEqual(len(self.llm.calls), 2)


class WorkerFailureTests(BackgroundTestCase):
    def test_failed_items_do_not_stop_the_others(self) -> None:
        job_id = self.create_many(6, "--retries 0 --concurrency 2")

        def respond(messages):
            if item_key(messages) == "item002.txt":
                raise ValueError("bad item")
            return StubResult("ok")

        self.llm.respond = respond
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        job = self.job(job_id)
        self.assertEqual((job["status"], job["completed_items"], job["failed_items"]),
                         ("completed_with_errors", 5, 1))
        self.assertIn("State: completed with errors", self.status(job_id))

    def test_unexpected_worker_error_is_reported_and_job_can_continue(self) -> None:
        job_id = self.create_many(4, "--concurrency 1")
        manager = self.runtime.manager()
        original = manager.process_next
        manager.process_next = lambda _job: (_ for _ in ()).throw(RuntimeError("worker broke"))
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        self.assertIn("worker_error", self.event_types(job_id))
        self.assertIn("worker_error: RuntimeError", self.status(job_id))
        self.assertIn("not started", self.status(job_id))

        manager.process_next = original
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")

    def test_item_is_released_when_its_failure_cannot_be_saved(self) -> None:
        job_id = self.create_many(1, "--retries 0 --concurrency 1")
        store = self.store
        original = store.fail_item
        store.fail_item = lambda *_a: (_ for _ in ()).throw(RuntimeError("disk trouble"))

        def refuse(_messages):
            raise ValueError("refused")

        self.llm.respond = refuse
        job = self.job(job_id)
        self.assertEqual(job["status"], "pending")
        self.run_one(job_id)
        store.fail_item = original
        item = self.items(job_id)[0]
        self.assertEqual((item["status"], item["claimed_by"]), ("pending", None))
        self.assertIn("awaiting recovery", self.status(job_id))

    def test_claim_abandoned_by_a_worker_is_released_when_the_run_ends(self) -> None:
        job_id = self.create_many(3, "--concurrency 1")
        manager = self.runtime.manager()
        original = manager.process_next
        first = [True]

        def abandon(job):
            if first[0]:
                first[0] = False
                manager.store.claim_next(job, manager.worker.id)
                raise RuntimeError("thread died holding a claim")
            return original(job)

        manager.process_next = abandon
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        running = [item for item in self.items(job_id) if item["status"] == "running"]
        self.assertEqual(running, [])
        self.assertIn("item_interrupted", self.event_types(job_id))
        self.assertIn("awaiting recovery", self.status(job_id))


class ShutdownTests(BackgroundTestCase):
    def start_process(self, job_id: str, mode: str) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, WORKER_SCRIPT, str(self.data_dir), job_id, mode],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )

    def reopen(self) -> BulkCommands:
        self.runtime.close()
        self.runtime = self.make_runtime(self.llm)
        self.addCleanup(self.runtime.close)
        self.commands = BulkCommands(self.runtime)
        return self.commands

    def test_gateway_exit_mid_item_then_restart_continues(self) -> None:
        job_id = self.create_job("--concurrency 1")
        process = self.start_process(job_id, "gateway-exit")
        stdout, stderr = process.communicate(timeout=60)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertIn("in-model", stdout)
        first, second = self.items(job_id)
        self.assertEqual((first["status"], second["status"]), ("completed", "running"))
        output_before = (self.data_dir / first["output_path"]).read_bytes()
        first_before = dict(first)

        commands = self.reopen()
        self.assertEqual(self.items(job_id)[1]["status"], "pending")
        self.assertIn("item_recovered", self.event_types(job_id))
        self.assertIn("awaiting recovery", commands.handle(f"status {job_id}"))

        self.llm.respond = lambda _m: StubResult("finished after restart")
        self.assertIn("in the background", commands.handle(f"run {job_id}"))
        self.wait_until_idle(job_id)
        first, second = self.items(job_id)
        self.assertEqual(self.job(job_id)["status"], "completed")
        self.assertEqual(len(self.llm.calls), 1)
        self.assertEqual(dict(first), first_before)
        self.assertEqual((self.data_dir / first["output_path"]).read_bytes(), output_before)
        self.assertEqual(second["attempts"], 2)
        self.assertEqual((self.data_dir / second["output_path"]).read_text(encoding="utf-8"),
                         "finished after restart")

    def test_unload_stops_new_claims_and_saves_finished_work(self) -> None:
        job_id = self.create_many(10, "--concurrency 2")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 2)
        manager = self.runtime.manager()
        threading.Timer(0.05, gate.release.set).start()
        manager.close(grace=WAIT)
        self.runtime._managers.clear()
        self.assertEqual(len(self.llm.calls), 2)

        commands = self.reopen()
        self.assertEqual(self.job(job_id)["completed_items"], 2)
        self.assertEqual(len([i for i in self.items(job_id) if i["status"] == "running"]), 0)
        self.assertIn("awaiting recovery", commands.handle(f"status {job_id}"))

    def test_unload_gives_back_a_claim_that_outlives_the_grace_period(self) -> None:
        job_id = self.create_many(3, "--concurrency 1")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 1)
        manager = self.runtime.manager()
        manager.close(grace=0.01)
        self.runtime._managers.clear()
        gate.release.set()  # the late thread must fail quietly against the closed store

        self.reopen()
        first = self.items(job_id)[0]
        self.assertEqual((first["status"], first["claimed_by"], first["output_path"]), ("pending", None, None))
        self.assertIn("item_interrupted", self.event_types(job_id))
        outputs = self.data_dir / "jobs" / job_id / "outputs"
        for _ in range(200):  # the abandoned thread removes its staged file as it leaves
            if not outputs.exists() or not list(outputs.iterdir()):
                break
            time.sleep(0.01)
        self.assertFalse(outputs.exists() and list(outputs.iterdir()))

    def test_start_is_refused_after_shutdown_began(self) -> None:
        job_id = self.create_many(2)
        manager = self.runtime.manager()
        manager.background.stop(0)
        self.assertIn("shutting down", self.run_job(job_id))
        self.assertEqual(self.llm.calls, [])

    def test_draining_gateway_stops_new_claims(self) -> None:
        job_id = self.create_many(10, "--concurrency 1")
        manager = self.runtime.manager()
        draining = [False]
        manager.background._draining = lambda: draining[0]

        def drain_after_first(_messages):
            draining[0] = True
            return StubResult("in flight")

        self.llm.respond = drain_after_first
        self.run_job(job_id)
        self.wait_until_idle(job_id)
        self.assertEqual(len(self.llm.calls), 1)
        self.assertEqual(self.job(job_id)["completed_items"], 1)
        self.assertIn("awaiting recovery", self.status(job_id))

    def test_retry_backoff_gives_up_the_claim_during_shutdown(self) -> None:
        job_id = self.create_many(1, "--retries 3 --concurrency 1")
        manager = self.runtime.manager()
        manager._sleep = lambda _seconds: manager.background._stopping.set()

        def time_out(_messages):
            raise TimeoutError("slow provider")

        self.llm.respond = time_out
        self.run_one(job_id)
        self.assertEqual(len(self.llm.calls), 1)
        item = self.items(job_id)[0]
        self.assertEqual((item["status"], item["claimed_by"]), ("pending", None))
        self.assertIn("item_interrupted", self.event_types(job_id))


class StatusTests(BackgroundTestCase):
    def test_wording_is_singular_for_one(self) -> None:
        job_id = self.create_many(1, "--concurrency 1")
        output = self.run_job(job_id)
        self.assertIn("up to 1 item at once", output)
        self.assertNotIn("1 items", output)
        self.assertIn("Concurrency: up to 1 item at once", self.status(job_id))

    def test_status_shows_the_concurrency_that_will_apply(self) -> None:
        job_id = self.create_many(1, "--concurrency 4")
        self.assertIn("Concurrency: up to 4 items at once", self.status(job_id))
        self.runtime.close()
        self.runtime = self.make_runtime(self.llm, settings={"max_concurrency": 2})
        self.addCleanup(self.runtime.close)
        output = BulkCommands(self.runtime).handle(f"status {job_id}")
        self.assertIn("Concurrency: up to 2 items at once (the job asked for 4; limited by max_concurrency)", output)

    def test_new_job_reports_not_started_and_counts(self) -> None:
        job_id = self.create_many(3)
        output = self.status(job_id)
        self.assertIn("State: not started", output)
        self.assertIn("0/3 completed, 0 failed, 3 pending, 0 running", output)
        total = sum(item["estimated_tokens"] for item in self.items(job_id))
        self.assertIn(f"Estimated tokens for the whole job: {total}", output)

    def test_status_shows_a_claim_held_by_another_live_process(self) -> None:
        job_id = self.create_job()
        worker = subprocess.Popen(
            [sys.executable, WORKER_SCRIPT, str(self.data_dir), job_id, "hold"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(worker.kill)
        self.assertEqual(worker.stdout.readline().strip(), "claimed")
        self.assertIn("processing in another Hermes process", self.status(job_id))
        worker.communicate(timeout=60)

    def test_job_list_marks_active_jobs(self) -> None:
        job_id = self.create_many(1, "--concurrency 1")
        gate = self.gate()
        self.run_job(job_id)
        gate.wait_for_calls(self, 1)
        self.assertIn("[processing]", self.commands.handle("status"))
        gate.release.set()
        self.wait_until_idle(job_id)
        self.assertNotIn("[processing]", self.commands.handle("status"))


class GatewayStatusTests(unittest.TestCase):
    def test_detects_this_process_draining(self) -> None:
        for state, expected in (("draining", True), ("stopped", True), ("running", False)):
            status = {"pid": os.getpid(), "gateway_state": state}
            self.assertEqual(gateway_is_stopping(lambda: status), expected, state)

    def test_another_process_draining_is_ignored(self) -> None:
        status = {"pid": os.getpid() + 1, "gateway_state": "draining"}
        self.assertFalse(gateway_is_stopping(lambda: status))

    def test_missing_or_unexpected_status_means_not_stopping(self) -> None:
        for status in (None, [1, 2], {}):
            self.assertFalse(gateway_is_stopping(lambda: status), status)

    def test_unavailable_status_means_not_stopping(self) -> None:
        def unavailable():
            raise ImportError("no gateway")

        self.assertFalse(gateway_is_stopping(unavailable))


class StaleLockTests(TempDirTestCase):
    def age(self, path: Path, seconds: float = 3600) -> None:
        old = time.time() - seconds
        os.utime(path, (old, old))

    def test_old_unlocked_files_are_removed(self) -> None:
        directory = self.tmp / "workers"
        mine = WorkerLock(directory)
        self.addCleanup(mine.release)
        stale = directory / ("a" * 32 + ".lock")
        stale.touch()
        self.age(stale)
        self.assertEqual(sweep_stale_locks(directory, mine.id), 1)
        self.assertFalse(stale.exists())

    def test_live_and_recent_locks_are_kept(self) -> None:
        directory = self.tmp / "workers"
        mine = WorkerLock(directory)
        self.addCleanup(mine.release)
        live = WorkerLock(directory)
        self.addCleanup(live.release)
        live_path = directory / f"{live.id}.lock"
        self.age(live_path)
        self.age(directory / f"{mine.id}.lock")
        young = directory / ("b" * 32 + ".lock")
        young.touch()
        self.assertEqual(sweep_stale_locks(directory, mine.id), 0)
        self.assertTrue(live_path.exists())
        self.assertTrue(young.exists())
        self.assertTrue((directory / f"{mine.id}.lock").exists())

    def test_unusual_names_are_ignored(self) -> None:
        directory = self.tmp / "workers"
        directory.mkdir()
        odd = directory / "not-a-worker.lock"
        odd.touch()
        self.age(odd)
        self.assertEqual(sweep_stale_locks(directory, "x"), 0)
        self.assertTrue(odd.exists())
