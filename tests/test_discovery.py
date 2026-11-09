from __future__ import annotations

import json

from bulk_plugin.discovery import discover, load_item
from bulk_plugin.errors import DiscoveryError
from tests import FIXTURES, TempDirTestCase


class DiscoveryTests(TempDirTestCase):
    def test_directory_items_are_sorted(self) -> None:
        found = discover(FIXTURES / "reports")
        self.assertEqual([item.key for item in found.items], ["q1.txt", "q2.txt"])
        self.assertEqual(found.skipped, [])

    def test_csv_rows(self) -> None:
        found = discover(FIXTURES / "records.csv")
        self.assertEqual([item.key for item in found.items], ["row:1", "row:2", "row:3"])
        self.assertEqual(json.loads(found.items[0].content)["customer"], "Acme")

    def test_json_records(self) -> None:
        found = discover(FIXTURES / "records.json")
        self.assertEqual([item.key for item in found.items], ["record:1", "record:2"])
        self.assertEqual(json.loads(found.items[1].content)["id"], 2)

    def test_jsonl_lines_keep_source_line_numbers(self) -> None:
        path = self.tmp / "data.jsonl"
        path.write_text('{"a": 1}\n\n{"a": 2}\n', encoding="utf-8")
        self.assertEqual([item.key for item in discover(path).items], ["line:1", "line:3"])

    def test_large_content_is_not_truncated(self) -> None:
        body = "x" * 2_000_000 + "END"
        (self.tmp / "big.txt").write_text(body, encoding="utf-8")
        self.assertEqual(discover(self.tmp).items[0].content, body)

    def test_excluded_entries_are_reported(self) -> None:
        (self.tmp / "good.txt").write_text("hello", encoding="utf-8")
        (self.tmp / "binary.dat").write_bytes(b"\xff\xfe\x00\x80")
        (self.tmp / "empty.txt").write_text("  \n", encoding="utf-8")
        (self.tmp / ".secret").write_text("hidden", encoding="utf-8")
        found = discover(self.tmp)
        self.assertEqual([item.key for item in found.items], ["good.txt"])
        reasons = dict(found.skipped)
        self.assertEqual(set(reasons), {"binary.dat", "empty.txt", ".secret"})
        self.assertIn("not UTF-8", reasons["binary.dat"])

    def test_load_item_matches_discovery(self) -> None:
        for source in (FIXTURES / "reports", FIXTURES / "records.csv", FIXTURES / "records.json"):
            for item in discover(source).items:
                self.assertEqual(load_item(source, item.key), item)

    def test_errors_are_explicit(self) -> None:
        bad_json = self.tmp / "object.json"
        bad_json.write_text('{"not": "an array"}', encoding="utf-8")
        for source, message in (
            (self.tmp / "missing", "does not exist"),
            (bad_json, "top-level JSON array"),
        ):
            with self.assertRaisesRegex(DiscoveryError, message):
                discover(source)

    def test_directory_with_nothing_processable_is_an_error(self) -> None:
        (self.tmp / "empty.txt").write_text("", encoding="utf-8")
        with self.assertRaisesRegex(DiscoveryError, "no processable items"):
            discover(self.tmp)
