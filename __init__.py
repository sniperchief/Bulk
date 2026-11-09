"""Bulk: a Hermes plugin that applies one instruction to many independent items.

This module only wires the plugin into Hermes. ``hermes plugins validate`` imports
it in a bare interpreter, so no Hermes module may be imported at module level.
"""

from __future__ import annotations

from .commands import ARGS_HINT, BulkCommands
from .runtime import Runtime


def register(ctx) -> None:
    runtime = Runtime(ctx)
    commands = BulkCommands(runtime)
    ctx.register_command(
        "bulk",
        handler=commands.handle,
        description="Apply one instruction to many files, CSV rows or JSON records",
        args_hint=ARGS_HINT,
    )
    ctx.on_unload(runtime.close)
