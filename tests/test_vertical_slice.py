"""The /bulk flow end to end with a stub in place of the model.

These tests cover Bulk's own logic around the call. They are not evidence that a real
model call works.
"""

from __future__ import annotations

from bulk_plugin.commands import BulkCommands
from bulk_plugin.runtime import Runtime
from tests import JobTestCase, StubCtx, StubResult, StubUsage


class SliceTests(JobTestCase):
    def test_create_registers_items_without_calling_the_model(self) -> None:
        job_id = self.create_job("--max-tokens 5000 --concurrency 3")
        job = self.job(job_id)
        self.assertEqual((job["instruction"], job["source"]), ("Summarize this", str(self.reports.resolve())))
        self.assertEqual((job["max_tokens"], job["concurrency"]), (5000, 3))
        self.assertTrue(all(len(item["content_hash"]) == 64 for item in self.items(job_id)))
        self.assertTrue(all(item["estimated_tokens"] > 0 for item in self.items(job_id)))
        self.assertEqual(self.llm.calls, [])

    def test_one_item_step_saves_its_result(self) -> None:
        job_id = self.create_job()
        self.llm.respond = lambda _m: StubResult("the summary", usage=StubUsage(100, 20, 0.5))
        output = self.run_one(job_id)
        self.assertIn("Completed q1.txt", output)
        self.assertEqual(len(self.llm.calls), 1)
        first, second = self.items(job_id)
        self.assertEqual((first["status"], first["tokens_in"], first["tokens_out"], first["attempts"]),
                         ("completed", 100, 20, 1))
        self.assertEqual((self.data_dir / first["output_path"]).read_text(encoding="utf-8"), "the summary")
        self.assertEqual(second["status"], "pending")
        job = self.job(job_id)
        self.assertEqual((job["status"], job["completed_items"], job["spent_usd"]), ("pending", 1, 0.5))

    def test_model_call_is_isolated_and_uses_no_overrides(self) -> None:
        job_id = self.create_job()
        self.run_one(job_id)
        call = self.llm.calls[0]
        text = "\n".join(message["content"] for message in call["messages"])
        self.assertEqual(len(call["messages"]), 2)
        self.assertIn("Q1 revenue", text)
        self.assertNotIn("Q2 revenue", text)
        self.assertIn("Summarize this", text)
        self.assertEqual((call["purpose"], call["provider"], call["model"]), ("bulk:item", None, None))

    def test_full_item_content_reaches_the_model(self) -> None:
        body = "y" * 500_000 + "TAIL"
        (self.reports / "q1.txt").write_text(body, encoding="utf-8")
        job_id = self.create_job()
        self.run_one(job_id)
        self.assertIn(body, self.llm.calls[0]["messages"][1]["content"])

    def test_status_after_all_items_complete(self) -> None:
        job_id = self.create_job()
        self.run_one(job_id)
        self.run_one(job_id)
        output = self.commands.handle(f"status {job_id}")
        self.assertIn(f"Job {job_id}: completed", output)
        self.assertIn("2/2 completed", output)
        self.assertNotIn("Reported cost", output)
        self.assertIn("has no pending items", self.run_one(job_id))

    def test_model_exception_marks_item_failed_and_job_continues(self) -> None:
        job_id = self.create_job()

        def explode(_messages):
            raise ValueError("provider refused with sk-secret")

        self.llm.respond = explode
        output = self.run_one(job_id)
        self.assertIn("Failed q1.txt", output)
        self.assertNotIn("sk-secret", output)
        self.assertNotIn("sk-secret", self.commands.handle(f"status {job_id}"))
        first, second = self.items(job_id)
        self.assertEqual((first["status"], first["attempts"]), ("failed", 1))
        self.assertEqual(first["last_error"], "ValueError (details are in the Hermes log)")
        self.assertIsNone(first["output_path"])
        self.assertEqual(second["status"], "pending")
        self.assertEqual(self.job(job_id)["failed_items"], 1)

    def test_empty_model_response_is_a_failure(self) -> None:
        job_id = self.create_job()
        self.llm.respond = lambda _m: StubResult("   ")
        self.assertIn("empty response", self.run_one(job_id))
        self.assertEqual(self.items(job_id)[0]["status"], "failed")

    def test_source_changed_after_creation_is_a_failure_without_model_call(self) -> None:
        job_id = self.create_job()
        (self.reports / "q1.txt").write_text("edited", encoding="utf-8")
        self.assertIn("source content changed", self.run_one(job_id))
        self.assertEqual(self.llm.calls, [])

    def test_unwritable_result_does_not_mark_item_completed(self) -> None:
        job_id = self.create_job()
        (self.data_dir / "jobs").write_text("a file blocks the directory", encoding="utf-8")
        self.assertIn("Failed q1.txt", self.run_one(job_id))
        self.assertEqual(self.items(job_id)[0]["status"], "failed")
        self.assertIsNone(self.items(job_id)[0]["output_path"])

    def test_state_survives_plugin_close_and_reopen(self) -> None:
        job_id = self.create_job()
        self.run_one(job_id)
        self.runtime.close()
        reopened = self.make_runtime(self.llm)
        self.addCleanup(reopened.close)
        commands = BulkCommands(reopened)
        self.assertIn("1/2 completed", commands.handle(f"status {job_id}"))
        self.assertIn("Completed q2.txt", self.run_one(job_id))
        self.assertIn("2/2 completed", commands.handle(f"status {job_id}"))

    def test_bad_input_returns_text_instead_of_raising(self) -> None:
        cases = {
            "": "Usage:",
            "nonsense": "unrecognized arguments",
            '--over reports': "instruction is required",
            '"x" --over': "needs a value",
            '"x" --over reports --budget 5': "replaced by --max-tokens",
            '"x" --over reports --max-tokens abc': "expects a number",
            '"x" --over reports --max-tokens 0': "must be at least",
            '"x" --over reports --retries -1': "must be at least",
            '"x" --over reports --concurrency 0': "must be at least",
            '"x" --over reports --wat 1': "unknown option",
            '"unterminated --over reports': "could not parse",
            '"x" --over does-not-exist': "does not exist",
            "status nope": "no job with id nope",
            "run nope": "no job with id nope",
            "pause nope": "no job with id nope",
            "resume nope": "no job with id nope",
            "retry nope": "no job with id nope",
            "resume x --max-tokens": "usage: /bulk resume",
            "pause": "unrecognized arguments",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertIn(expected, self.commands.handle(raw))
        self.assertEqual(self.commands.handle("status"), "No Bulk jobs yet.")

    def test_windows_paths_keep_their_backslashes(self) -> None:
        output = self.commands.handle(f'"Summarize" --over "{self.reports}"')
        self.assertIn("with 2 items", output)
        output = self.commands.handle(f'"Summarize" --over {self.reports}')
        self.assertIn("with 2 items", output)

    def test_unexpected_exception_is_reported_not_raised(self) -> None:
        def broken():
            raise RuntimeError("no storage")

        runtime = Runtime(StubCtx(), data_dir=broken, open_db=broken, cwd=broken)
        self.assertIn("internal error (RuntimeError)", BulkCommands(runtime).handle("status"))
