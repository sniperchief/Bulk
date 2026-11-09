"""Write per-item output files under the plugin's data directory.

Output is staged to a worker-specific temporary file and only moved to its final path
inside the transaction that verifies the item is still claimed by that worker, so a
completed result cannot be overwritten by a later attempt.
"""

from __future__ import annotations

import os
from pathlib import Path


def output_relative(job_id: str, item_index: int) -> str:
    return f"jobs/{job_id}/outputs/{item_index:06d}.md"


def _temp_path(data_dir: Path, job_id: str, item_index: int, worker: str) -> Path:
    return data_dir / "jobs" / job_id / "outputs" / f"{item_index:06d}.{worker}.tmp"


def stage_output(data_dir: Path, job_id: str, item_index: int, worker: str, text: str) -> Path:
    temporary = _temp_path(data_dir, job_id, item_index, worker)
    temporary.parent.mkdir(parents=True, exist_ok=True)
    with open(temporary, "w", encoding="utf-8", newline="") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    return temporary


def discard_staged(data_dir: Path, job_id: str, item_index: int, worker: str) -> None:
    _temp_path(data_dir, job_id, item_index, worker).unlink(missing_ok=True)
