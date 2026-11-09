"""Registration against a recording context, like the `hermes plugins validate` probe."""

from __future__ import annotations

import re
import subprocess
import sys
import unittest

from tests import ROOT, StubCtx, load_plugin_package


class PluginLoadTests(unittest.TestCase):
    def test_register_adds_bulk_command_and_unload_hook(self) -> None:
        ctx = StubCtx()
        load_plugin_package().register(ctx)
        self.assertEqual(list(ctx.commands), ["bulk"])
        self.assertTrue(callable(ctx.commands["bulk"]["handler"]))
        self.assertTrue(ctx.commands["bulk"]["description"])
        self.assertTrue(ctx.commands["bulk"]["args_hint"])
        self.assertEqual(len(ctx.unload_callbacks), 1)

    def test_import_needs_no_hermes_modules(self) -> None:
        script = (
            "import importlib.util, sys\n"
            f"root = {str(ROOT)!r}\n"
            "spec = importlib.util.spec_from_file_location('probe', root + '/__init__.py',"
            " submodule_search_locations=[root])\n"
            "module = importlib.util.module_from_spec(spec)\n"
            "sys.modules['probe'] = module\n"
            "spec.loader.exec_module(module)\n"
            "bad = [m for m in sys.modules if m.split('.')[0] in"
            " ('agent', 'hermes_cli', 'plugins', 'hermes_constants')]\n"
            "assert not bad, bad\n"
        )
        completed = subprocess.run([sys.executable, "-I", "-c", script], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_manifest_required_fields(self) -> None:
        manifest = (ROOT / "plugin.yaml").read_text(encoding="utf-8")
        self.assertRegex(manifest, r"(?m)^name: bulk$")
        self.assertRegex(manifest, r"(?m)^version: \d+\.\d+\.\d+$")
        self.assertRegex(manifest, r"(?m)^description: .+$")

    def test_no_direct_credential_access(self) -> None:
        pattern = re.compile(r"os\.environ|getenv|auth\.json|\.env\b|api_key", re.IGNORECASE)
        for path in ROOT.glob("*.py"):
            self.assertIsNone(pattern.search(path.read_text(encoding="utf-8")), path.name)
