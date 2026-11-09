"""Write per-item output files under the plugin's data directory.

Output is staged to a worker-specific temporary file and only moved to its final path
inside the transaction that verifies the item is still claimed by that worker, so a
completed result cannot be overwritten by a later attempt.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, TextIO


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


# ── Combined export ─────────────────────────────────────────────────────────

_SPREADSHEET_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


@dataclass(frozen=True)
class Export:
    path: Path
    included: int
    failed: int
    unfinished: int
    missing: int


def _read_result(data_dir: Path, item) -> Optional[str]:
    if item["status"] != "completed" or not item["output_path"]:
        return None
    try:
        return (data_dir / item["output_path"]).read_text(encoding="utf-8")
    except OSError:
        return None


def _csv_cell(text: str) -> str:
    """Stop a spreadsheet from running model output or a file name as a formula."""
    return f"'{text}" if text.startswith(_SPREADSHEET_FORMULA_PREFIXES) else text


def _write_csv(handle: TextIO, rows: Iterable[tuple]) -> None:
    writer = csv.writer(handle)
    writer.writerow(["item", "status", "result", "error"])
    for item, result in rows:
        writer.writerow([
            _csv_cell(item["item_key"]),
            item["status"],
            _csv_cell(result) if result is not None else "",
            _csv_cell(item["last_error"] or ""),
        ])


def _write_markdown(handle: TextIO, job, rows: Iterable[tuple]) -> None:
    handle.write(f"# Bulk results for job {job['id']}\n\nInstruction: {job['instruction']}\n")
    for item, result in rows:
        handle.write(f"\n## {item['item_key']}\n\n")
        if result is not None:
            handle.write(f"{result.rstrip()}\n")
        elif item["status"] == "completed":
            handle.write("_The result file is missing._\n")
        elif item["status"] == "failed":
            handle.write(f"_Failed: {item['last_error'] or 'unknown error'}_\n")
        else:
            handle.write(f"_Not finished ({item['status']})._\n")


def write_export(data_dir: Path, job, items: list, fmt: str) -> Export:
    """Write every item's result into one file, replacing any earlier export of that format."""
    rows = [(item, _read_result(data_dir, item)) for item in items]
    final = data_dir / "jobs" / job["id"] / f"results.{fmt}"
    temporary = final.with_name(f"{final.name}.{os.getpid()}.tmp")
    final.parent.mkdir(parents=True, exist_ok=True)
    try:
        # utf-8-sig lets Excel recognise the encoding of a CSV file.
        encoding = "utf-8-sig" if fmt == "csv" else "utf-8"
        with open(temporary, "w", encoding=encoding, newline="" if fmt == "csv" else None) as handle:
            if fmt == "csv":
                _write_csv(handle, rows)
            else:
                _write_markdown(handle, job, rows)
        os.replace(temporary, final)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return Export(
        path=final,
        included=sum(result is not None for _item, result in rows),
        failed=sum(item["status"] == "failed" for item in items),
        unfinished=sum(item["status"] in ("pending", "running") for item in items),
        missing=sum(item["status"] == "completed" and result is None for item, result in rows),
    )
