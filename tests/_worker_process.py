"""A real separate process that claims an item, for interruption tests.

usage: _worker_process.py <data_dir> <job_id> <crash-in-model|hold|gateway-exit>
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests import StubLlm, StubResult, open_test_db  # noqa: E402
from bulk_plugin.jobs import JobManager  # noqa: E402
from bulk_plugin.store import Store  # noqa: E402


def die_during_model_call(_messages):
    print("in-model", flush=True)
    os._exit(9)  # no cleanup runs, as when a process is killed


def main() -> None:
    data_dir, job_id, mode = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
    llm = StubLlm(die_during_model_call)
    manager = JobManager(Store(open_test_db(data_dir / "data.db")), data_dir, llm)
    if mode == "crash-in-model":
        manager.process_next(job_id)
        return
    if mode == "gateway-exit":
        # Background workers finish one item, then the process ends mid-call the way
        # the Hermes gateway does: os._exit, with no unload callback.
        calls = []

        def complete_first_then_exit(_messages):
            calls.append(1)
            if len(calls) == 1:
                return StubResult("saved before the exit")
            print("in-model", flush=True)
            os._exit(0)

        llm.respond = complete_first_then_exit
        manager.start(job_id, 1)
        time.sleep(60)  # the worker thread ends the process
        os._exit(3)
    manager.store.claim_next(job_id, manager.worker.id)
    print("claimed", flush=True)
    sys.stdin.read()  # stay alive until the parent closes stdin
    os._exit(0)


if __name__ == "__main__":
    main()
