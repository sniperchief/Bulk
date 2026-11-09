"""Test support. Run from the repository root:

    python -m unittest discover -s tests -t . -v

These tests use the standard library only and do not need Hermes. ``StubLlm`` is a
test double: passing tests here do NOT show that a real model call works. The live
check is the manual procedure in README.md.
"""

from __future__ import annotations

import importlib.util
import shutil
import sqlite3
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_plugin_package():
    """Import the plugin the way Hermes does: from its ``__init__.py`` as a package."""
    name = "bulk_plugin"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


load_plugin_package()

from bulk_plugin.commands import BulkCommands  # noqa: E402
from bulk_plugin.errors import JobBlocked  # noqa: E402
from bulk_plugin.runtime import Runtime  # noqa: E402


@dataclass
class StubUsage:
    input_tokens: int = 11
    output_tokens: int = 7
    cost_usd: Optional[float] = None


@dataclass
class StubResult:
    text: str
    provider: str = "stub-provider"
    model: str = "stub-model"
    usage: StubUsage = field(default_factory=StubUsage)


class StubLlm:
    """Mirrors the signature of ``PluginLlm.complete``. Not a model."""

    def __init__(self, respond: Callable[[list[dict]], Any] = lambda _messages: StubResult("stub output")):
        self.respond = respond
        self.calls: list[dict] = []

    def complete(self, messages, *, provider=None, model=None, temperature=None, max_tokens=None,
                 timeout=None, agent_id=None, profile=None, purpose=None, task=None):
        self.calls.append({"messages": messages, "purpose": purpose, "provider": provider, "model": model})
        return self.respond(messages)


class StubCtx:
    def __init__(self, llm: Optional[StubLlm] = None, settings: Optional[dict] = None) -> None:
        self.llm = llm or StubLlm()
        self.settings = settings or {}
        self.commands: dict[str, dict] = {}
        self.unload_callbacks: list = []

    def register_command(self, name, handler, description="", args_hint="", argument_mode=None):
        self.commands[name] = {"handler": handler, "description": description, "args_hint": args_hint}

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)

    def get_config(self, key, default=None):
        return self.settings.get(key, default)


def open_test_db(path: Path) -> sqlite3.Connection:
    """Same connection settings as ``plugins.plugin_storage.plugin_db``."""
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class TempDirTestCase(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(directory.cleanup)
        self.tmp = Path(directory.name)


class JobTestCase(TempDirTestCase):
    """A Bulk runtime on real SQLite and files, with a stub in place of the model."""

    def setUp(self) -> None:
        super().setUp()
        self.reports = self.tmp / "work" / "reports"
        shutil.copytree(FIXTURES / "reports", self.reports)
        self.data_dir = self.tmp / "plugin-data"
        self.data_dir.mkdir()
        self.llm = StubLlm()
        self.sleeps: list[float] = []
        self.runtime = self.make_runtime(self.llm)
        self.addCleanup(self.runtime.close)
        self.commands = BulkCommands(self.runtime)

    def make_runtime(self, llm: StubLlm, settings: Optional[dict] = None) -> Runtime:
        return Runtime(
            StubCtx(llm, settings),
            data_dir=lambda: self.data_dir,
            open_db=lambda: open_test_db(self.data_dir / "data.db"),
            cwd=lambda: self.tmp / "work",
            sleep=self.sleeps.append,
        )

    def create_job(self, options: str = "") -> str:
        output = self.commands.handle(f'"Summarize this" --over reports {options}')
        self.assertIn("with 2 items", output)
        return output.split()[2]

    @property
    def store(self):
        return self.runtime.manager().store

    def job(self, job_id: str):
        return self.store.get_job(job_id)

    def items(self, job_id: str):
        return self.store.get_items(job_id)

    def event_types(self, job_id: str) -> list[str]:
        return [event["event_type"] for event in self.store.get_events(job_id, limit=1000)]

    def run_one(self, job_id: str, runtime: Optional[Runtime] = None) -> str:
        """Process one item on this thread, for tests of item-level behaviour."""
        try:
            processed = (runtime or self.runtime).manager().process_next(job_id)
        except JobBlocked as exc:
            return str(exc)
        if processed is None:
            return f"Job {job_id} has no pending items."
        return f"Completed {processed.item_key}" if processed.ok else f"Failed {processed.item_key}: {processed.error}"

    def wait_until_idle(self, job_id: str, runtime: Optional[Runtime] = None, timeout: float = 30) -> None:
        background = (runtime or self.runtime).manager().background
        deadline = time.monotonic() + timeout
        while background.is_active(job_id):
            if time.monotonic() > deadline:
                self.fail(f"workers for job {job_id} did not stop within {timeout}s")
            time.sleep(0.005)
