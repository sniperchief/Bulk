"""Turn a source path into independent items.

A directory yields one item per text file. A ``.csv`` file yields one item per data
row, ``.json`` one per element of a top-level array, ``.jsonl`` one per line. Any other
file is a single item. Content is never truncated, and anything left out of a
directory is returned in ``Discovery.skipped`` with a reason.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from .errors import DiscoveryError, ItemProcessingError


@dataclass(frozen=True)
class Item:
    key: str
    content: str

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content.encode("utf-8")).hexdigest()


@dataclass
class Discovery:
    items: list[Item] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)


def discover(source: Path) -> Discovery:
    if not source.exists():
        raise DiscoveryError(f"source does not exist: {source}")
    result = _discover_directory(source) if source.is_dir() else _discover_file(source)
    if not result.items:
        detail = "; ".join(f"{key}: {reason}" for key, reason in result.skipped)
        raise DiscoveryError(f"no processable items in {source}" + (f" ({detail})" if detail else ""))
    return result


def load_item(source: Path, key: str) -> Item:
    """Re-read one previously discovered item from its source."""
    if source.is_dir():
        return Item(key, _read_directory_member(source, key))
    for item in _discover_file(source).items:
        if item.key == key:
            return item
    raise ItemProcessingError(f"item {key} no longer exists in {source}")


def _read_text(path: Path) -> str:
    try:
        return path.read_bytes().decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DiscoveryError(f"{path} is not UTF-8 text") from exc
    except OSError as exc:
        raise DiscoveryError(f"cannot read {path}: {exc}") from exc


def _discover_directory(root: Path) -> Discovery:
    result = Discovery()
    for current, dirnames, filenames in os.walk(root):
        current_path = Path(current)
        for name in list(dirnames):
            path = current_path / name
            if name.startswith("."):
                result.skipped.append((_key(path, root), "hidden directory"))
                dirnames.remove(name)
            elif path.is_symlink():
                result.skipped.append((_key(path, root), "symbolic link"))
                dirnames.remove(name)
        for name in filenames:
            path = current_path / name
            key = _key(path, root)
            if name.startswith("."):
                result.skipped.append((key, "hidden file"))
            elif path.is_symlink():
                result.skipped.append((key, "symbolic link"))
            else:
                try:
                    content = _read_text(path)
                except DiscoveryError as exc:
                    result.skipped.append((key, str(exc)))
                    continue
                if content.strip():
                    result.items.append(Item(key, content))
                else:
                    result.skipped.append((key, "empty file"))
    result.items.sort(key=lambda item: item.key)
    result.skipped.sort()
    return result


def _key(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _read_directory_member(root: Path, key: str) -> str:
    path = (root / PurePosixPath(key)).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ItemProcessingError(f"item {key} is outside {root}")
    try:
        return _read_text(path)
    except DiscoveryError as exc:
        raise ItemProcessingError(str(exc)) from exc


def _discover_file(path: Path) -> Discovery:
    text = _read_text(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return Discovery(items=_csv_items(text, path))
    if suffix == ".json":
        return Discovery(items=_json_items(text, path))
    if suffix == ".jsonl":
        return Discovery(items=_jsonl_items(text, path))
    if not text.strip():
        return Discovery(skipped=[(path.name, "empty file")])
    return Discovery(items=[Item(path.name, text)])


def _render(record: object) -> str:
    return json.dumps(record, ensure_ascii=False, indent=2)


def _csv_items(text: str, path: Path) -> list[Item]:
    reader = csv.DictReader(io.StringIO(text, newline=""), restkey="_extra_columns")
    try:
        return [Item(f"row:{number}", _render(row)) for number, row in enumerate(reader, 1)]
    except csv.Error as exc:
        raise DiscoveryError(f"invalid CSV in {path}: {exc}") from exc


def _json_items(text: str, path: Path) -> list[Item]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DiscoveryError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, list):
        raise DiscoveryError(f"{path} must contain a top-level JSON array of records")
    return [Item(f"record:{number}", _render(record)) for number, record in enumerate(data, 1)]


def _jsonl_items(text: str, path: Path) -> list[Item]:
    items = []
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            items.append(Item(f"line:{number}", _render(json.loads(line))))
        except json.JSONDecodeError as exc:
            raise DiscoveryError(f"invalid JSON on line {number} of {path}: {exc}") from exc
    return items
