"""The ``/bulk`` slash command: argument parsing and text output."""

from __future__ import annotations

import logging
import shlex
from pathlib import Path
from typing import Any, Optional

from .errors import BulkError, JobBlocked, UsageError
from .jobs import EXPORT_FORMATS, JobManager

logger = logging.getLogger(__name__)

ARGS_HINT = (
    '"<instruction>" --over <path> | run <job_id> | status [job_id] | '
    "pause <job_id> | resume <job_id> | retry <job_id> | export <job_id>"
)

USAGE = """Usage:

  /bulk "<instruction>" --over <path> [--max-tokens <n>] [--retries <n>] [--concurrency <n>]
      Create a job from a directory, .csv, .json, .jsonl or text file.
  /bulk run <job_id>
      Process every pending item in the background and return straight away.
  /bulk status [job_id]
      Show one job, or the most recent jobs.
  /bulk pause <job_id>
      Stop new items from being claimed. Items in progress may still finish.
  /bulk resume <job_id> [--max-tokens <n>]
      Continue a paused job, optionally with a new token limit.
  /bulk retry <job_id>
      Return failed items to pending and process them.
  /bulk export <job_id> [--format md|csv]
      Write all of a job's results into one file. Makes no model calls.

--max-tokens is enforced by Bulk from reported usage and estimates. It is not a
provider billing limit. --retries is the number of automatic retries per item.
--concurrency is how many items run at once (default 4). When the provider rate
limits Bulk, all workers wait and try again, and --retries 0 turns this off.

Background work runs inside the Hermes gateway and stops if the gateway stops. Run
/bulk run <job_id> again after a restart to continue."""


def _tokenize(raw: str) -> list[str]:
    lexer = shlex.shlex(raw, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    lexer.escape = ""  # keep backslashes so Windows paths survive
    try:
        return list(lexer)
    except ValueError as exc:
        raise UsageError(f"could not parse arguments: {exc}") from exc


def _number(flag: str, value: str, kind: type, minimum: float) -> Any:
    try:
        number = kind(value)
    except ValueError as exc:
        raise UsageError(f"{flag} expects a number, got {value!r}") from exc
    if not number >= minimum:  # also rejects NaN
        raise UsageError(f"{flag} must be at least {minimum}")
    return number


def _count(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"


class BulkCommands:
    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def handle(self, raw_args: str, **_context: Any) -> str:
        try:
            return self._dispatch(_tokenize(raw_args or ""))
        except BulkError as exc:
            return f"Bulk: {exc}"
        except Exception as exc:  # handler exceptions would propagate into the session
            logger.exception("Bulk command failed")
            return f"Bulk: internal error ({type(exc).__name__}). See the Hermes log."

    def _dispatch(self, tokens: list[str]) -> str:
        if not tokens or tokens[0] == "help":
            return USAGE
        if "--over" in tokens:
            return self._create(tokens)
        command, arguments = tokens[0], tokens[1:]
        if command == "status" and len(arguments) <= 1:
            return self._status(arguments[0] if arguments else None)
        if command == "resume" and arguments:
            return self._resume(arguments)
        if command == "export" and arguments:
            return self._export(arguments)
        if command in ("run", "pause", "retry") and len(arguments) == 1:
            handler = {"run": self._run, "pause": self._pause, "retry": self._retry}[command]
            return handler(arguments[0])
        raise UsageError(f"unrecognized arguments.\n\n{USAGE}")

    def _create(self, tokens: list[str]) -> str:
        instruction: Optional[str] = None
        options: dict[str, str] = {}
        position = 0
        while position < len(tokens):
            token = tokens[position]
            if token == "--budget":
                raise UsageError("--budget was replaced by --max-tokens")
            if token in ("--over", "--max-tokens", "--retries", "--concurrency"):
                if position + 1 >= len(tokens):
                    raise UsageError(f"{token} needs a value")
                options[token] = tokens[position + 1]
                position += 2
                continue
            if token.startswith("--"):
                raise UsageError(f"unknown option {token}")
            if instruction is not None:
                raise UsageError("put the instruction in double quotes")
            instruction = token
            position += 1
        if not instruction or not instruction.strip():
            raise UsageError(f"an instruction is required.\n\n{USAGE}")

        max_tokens = options.get("--max-tokens")
        retries = options.get("--retries")
        concurrency = options.get("--concurrency")
        cap = self._runtime.max_concurrency()
        if concurrency is not None and _number("--concurrency", concurrency, int, 1) > cap:
            raise UsageError(f"--concurrency must be at most {cap} (the max_concurrency plugin setting)")
        created = self._manager().create_job(
            instruction,
            self._resolve(options["--over"]),
            max_tokens=_number("--max-tokens", max_tokens, int, 1) if max_tokens is not None else None,
            max_retries=(
                _number("--retries", retries, int, 0)
                if retries is not None
                else self._runtime.default_retries()
            ),
            concurrency=_number("--concurrency", concurrency, int, 1) if concurrency is not None else 4,
        )
        lines = [f"Created job {created.job_id} with {_count(created.total_items, 'item')}."]
        if created.skipped:
            skipped = len(created.skipped)
            lines.append(f"Skipped {skipped} {'entry' if skipped == 1 else 'entries'}:")
            lines.extend(f"  {key}: {reason}" for key, reason in created.skipped[:5])
            if len(created.skipped) > 5:
                lines.append(f"  ... and {len(created.skipped) - 5} more (recorded in the job's events)")
        lines.append(f"Start it with: /bulk run {created.job_id}")
        return "\n".join(lines)

    def _start(self, manager: JobManager, job_id: str) -> str:
        """Start background processing and describe the outcome in one line."""
        try:
            outcome = manager.start(job_id, self._runtime.max_concurrency())
        except JobBlocked as exc:
            return str(exc)
        if outcome.started:
            return (
                f"Processing job {job_id} in the background, up to {_count(outcome.workers, 'item')} at once. "
                f"Check progress with: /bulk status {job_id}"
            )
        if outcome.pending:
            return f"Job {job_id} is already being processed."
        if outcome.workers:
            return f"Job {job_id} has no pending items; items in progress are finishing."
        return f"Job {job_id} has no pending items."

    def _run(self, job_id: str) -> str:
        manager = self._manager()
        first = self._start(manager, job_id)
        return f"{first}\n\n{self._job_text(manager, job_id)}"

    def _pause(self, job_id: str) -> str:
        manager = self._manager()
        first = (
            f"Paused job {job_id}. No new items will be claimed; items in progress may still finish."
            if manager.pause(job_id)
            else f"Job {job_id} is already paused."
        )
        return f"{first}\n\n{self._job_text(manager, job_id)}"

    def _resume(self, arguments: list[str]) -> str:
        job_id = arguments[0]
        max_tokens: Optional[int] = None
        if len(arguments) == 3 and arguments[1] == "--max-tokens":
            max_tokens = _number("--max-tokens", arguments[2], int, 1)
        elif len(arguments) != 1:
            raise UsageError("usage: /bulk resume <job_id> [--max-tokens <n>]")
        manager = self._manager()
        recovered = manager.resume(job_id, max_tokens)
        first = f"Resumed job {job_id}.\n{self._start(manager, job_id)}"
        if recovered:
            first += f"\nRecovered {_count(recovered, 'interrupted item')}."
        return f"{first}\n\n{self._job_text(manager, job_id)}"

    def _retry(self, job_id: str) -> str:
        manager = self._manager()
        count = manager.retry_failed(job_id)
        if count:
            first = f"Returned {_count(count, 'failed item')} to pending.\n{self._start(manager, job_id)}"
        else:
            first = f"Job {job_id} has no failed items."
        return f"{first}\n\n{self._job_text(manager, job_id)}"

    def _export(self, arguments: list[str]) -> str:
        job_id = arguments[0]
        fmt = "md"
        if len(arguments) == 3 and arguments[1] == "--format":
            fmt = arguments[2].lower()
            if fmt not in EXPORT_FORMATS:
                raise UsageError("--format must be md or csv")
        elif len(arguments) != 1:
            raise UsageError("usage: /bulk export <job_id> [--format md|csv]")
        exported = self._manager().export(job_id, fmt)
        total = exported.included + exported.failed + exported.unfinished + exported.missing
        lines = [f"Exported {exported.included} of {_count(total, 'result')} to {exported.path}"]
        if exported.failed:
            lines.append(f"{_count(exported.failed, 'failed item')} listed without a result. Reprocess with: /bulk retry {job_id}")
        if exported.unfinished:
            lines.append(f"{_count(exported.unfinished, 'item')} not finished yet. Export again when the job is done.")
        if exported.missing:
            lines.append(f"{_count(exported.missing, 'completed item')} had no result file.")
        return "\n".join(lines)

    def _status(self, job_id: Optional[str]) -> str:
        manager = self._manager()
        if job_id is not None:
            return self._job_text(manager, job_id)
        jobs = manager.store.list_jobs()
        if not jobs:
            return "No Bulk jobs yet."
        return "\n".join(
            f"{job['id']}  {job['status']:<21}  {job['completed_items']}/{job['total_items']} done, "
            f"{job['failed_items']} failed  "
            f"{'[processing]  ' if manager.background.is_active(job['id']) else ''}{job['instruction']}"
            for job in jobs
        )

    @staticmethod
    def _state(manager: JobManager, job: Any, items: list) -> str:
        """What is actually happening to the job, which the stored status alone cannot say."""
        job_id = job["id"]
        running = sum(item["status"] == "running" for item in items)
        if job["status"] == "completed":
            return "completed"
        if job["status"] == "completed_with_errors":
            return "completed with errors" if job["completed_items"] else "failed (every item failed)"
        if job["status"] == "paused":
            return f"paused ({running} in progress may still finish)" if running else "paused"
        if manager.background.is_active(job_id):
            waiting = manager.rate_limit.remaining()
            return f"processing (rate limited by the provider, waiting {waiting:.0f}s)" if waiting else "processing"
        if running and manager.claims_held_by_live_process(job_id):
            return "processing in another Hermes process"
        if running or any(item["attempts"] for item in items):
            return f"awaiting recovery (no worker is running). Continue with: /bulk run {job_id}"
        return f"not started. Start with: /bulk run {job_id}"

    def _concurrency_text(self, job: Any) -> str:
        cap = self._runtime.max_concurrency()
        text = f"Concurrency: up to {_count(min(job['concurrency'], cap), 'item')} at once"
        if job["concurrency"] > cap:
            text += f" (the job asked for {job['concurrency']}; limited by max_concurrency)"
        return text

    def _job_text(self, manager: JobManager, job_id: str) -> str:
        job = manager.get_job(job_id)
        items = manager.store.get_items(job_id)
        usage = manager.store.token_usage(job_id)
        pending = sum(item["status"] == "pending" for item in items)
        running = sum(item["status"] == "running" for item in items)
        tokens_in = sum(item["tokens_in"] for item in items)
        tokens_out = sum(item["tokens_out"] for item in items)
        lines = [
            f"Job {job['id']}: {job['status']}",
            f"State: {self._state(manager, job, items)}",
            f"Instruction: {job['instruction']}",
            f"Source: {job['source']}",
            f"Items: {job['completed_items']}/{job['total_items']} completed, "
            f"{job['failed_items']} failed, {pending} pending, {running} running",
            f"Actual tokens (reported by Hermes): {tokens_in} in, {tokens_out} out",
            f"Estimated tokens for the whole job: {usage['estimated_total']}",
        ]
        if usage["unreported"]:
            lines.append(f"Estimated tokens (no usage reported): {usage['unreported']}")
        if usage["reserved"]:
            lines.append(f"Reserved tokens (estimated, in progress): {usage['reserved']}")
        if job["max_tokens"] is not None:
            used = usage["actual"] + usage["unreported"]
            lines.append(f"Token limit: {used} of {job['max_tokens']} used (enforced by Bulk, not a billing limit)")
        if job["spent_usd"]:
            lines.append(f"Reported cost: ${job['spent_usd']:.4f}")
        lines.append(self._concurrency_text(job))
        lines.append(f"Automatic retries per item: {job['max_retries']}")
        if job["completed_items"]:
            lines.append(f"Combine results into one file with: /bulk export {job_id}")
        lines.append("Recent events:")
        lines.extend(
            f"  {event['created_at']} {event['event_type']}: {event['message']}"
            for event in manager.store.get_events(job_id)
        )
        return "\n".join(lines)

    def _manager(self) -> JobManager:
        return self._runtime.manager()

    def _resolve(self, value: str) -> Path:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = self._runtime.cwd() / path
        return path.resolve()
