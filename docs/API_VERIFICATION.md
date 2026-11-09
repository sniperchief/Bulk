# Hermes API verification

Checked on 2026-11-08 by reading the official docs and the source of `NousResearch/hermes-agent` at commit `3e82260` (`__release_date__` 2026.9.24).

Runtime results on Hermes v0.23.0 (2026.9.24, commit `2bd378e`):

- `hermes plugins validate` passes with no warnings. An earlier version imported `agent.redact.redact_sensitive_text`, which is not on the v0.23.0 validator's public list (`hermes_cli/plugin_validate_private_imports.py`), and no `ctx.*` method does redaction. Bulk now stores no provider error text, so it needs no redaction call.
- The real plugin loader registers `/bulk`, and a job and its items were written through the real `plugin_db`.
- A real `ctx.llm.complete` call succeeded in an interactive Hermes session with `nous/upstage/solar-mini4:free` (188 tokens in, 60 out), and its output was saved. `usage.cost_usd` came back empty, matching difference 1 below.
- Slash commands run inside the gateway process, which Hermes auto-starts and which outlives the CLI. Restarting the CLI does not reload the plugin or restart that process; run `hermes gateway stop` first.
- Phase 2 on the same install: with the gateway force-stopped while an item was running, the next gateway returned the item to `pending` with an `item_recovered` event and left the completed item untouched. Pause, resume, and a `--max-tokens` pause before any model call were also confirmed.
- Phase 3 on the same install, 20 files through `nous/upstage/solar-mini4:free`: one `/bulk run` completed the whole job at `--concurrency 3` with a peak of exactly 3 items running and no item started twice. A pause at 11/20 stopped new claims and resume finished the job without rerunning those 11. `hermes gateway stop` mid-job ended the gateway while `note06.txt` was in progress; the next gateway logged `item_recovered` for it, finished the job at 20/20 and left the 5 earlier items at one attempt.
- Final verification on v0.4.0, 100 files at `--concurrency 8`: during a 1.0-second `hermes gateway stop`, 2 in-flight items finished and were saved and 0 new items were claimed. After the restart, and before any resume, `/bulk status` showed `State: awaiting recovery` with 6 interrupted items recovered. `/bulk run` continued the job without rerunning the 19 completed items.
- Real rate limiting from the free model (HTTP 429, starting after about 50 calls) exercised automatic retries: in one run 47 retries were recorded, 6 items then succeeded and 18 failed after 3 attempts. `/bulk retry` returned 22 such failed items in a later run and the job finished at 100/100.
- A `--max-tokens 2000` job at `--concurrency 3` paused at 6/20 with 1,637 tokens used or reserved, because the next item was estimated at 420. Resuming with a higher limit finished it at 20/20.
- v0.5.0 rate-limit handling, the same 100 files at `--concurrency 8`: 24 real HTTP 429 responses, with all workers waiting together (5 seconds, doubling), finished at 100/100 with 0 failed, where the earlier version lost 15 to 22 items. `/bulk export` then wrote 100 of 100 results to one Markdown file and one CSV file.
- Not exercised for real: timeouts, connection errors and 5xx responses (unit tests with a stub model), the 10-minute idle exit of an auto-started gateway (read from source), and `plugins.isolation: host`.

On v0.23.0 `PluginContext` is defined in `hermes_cli/plugins.py`; the separate `plugins_context.py` exists only in newer source. `ctx.get_config(key, default)`, used for `max_retries`, is present on both. `call_llm` raises `TimeoutError` for timeouts and `RuntimeError` for configuration and provider-envelope errors, so Bulk retries only the former.

## Gateway lifecycle (Phase 3, read from the v0.23.0 source at `2bd378e`)

| Assumption | Finding | Source |
|---|---|---|
| Sync handlers have an event loop | No. They run on a gateway pool thread (loop-free), so `ctx.spawn_task`, which needs a running loop, cannot be used from one. Bulk uses daemon threads. | `gateway/run_inbound_hooks.py`, `tui_gateway/methods_tools.py` |
| `on_unload` runs at gateway shutdown | No. It runs on targeted unload and forced rediscovery (`discover_plugins(force=True)`, for example plugin reload). No call site exists in the shutdown path. | `hermes_cli/plugins_ledger.py`, `gateway/run_plugin_rewire.py`, `gateway/run_shutdown.py` |
| Threads outlive the gateway | No. Every graceful exit ends in `os._exit`, which also skips `atexit`. A worker dies wherever it is, with no chance to clean up. | `gateway/run.py` (`_exit_after_graceful_shutdown`) |
| A plugin is told about shutdown | No hook or context method exists. The gateway publishes `gateway_state` (`draining`, then `stopped`), readable with `gateway.status.read_runtime_status()`, a module on the validator's public list. Bulk reads it before each claim, matching on `pid` and treating anything unexpected as "not stopping". | `gateway/run_shutdown.py`, `gateway/status.py` |
| A gateway stays up while a plugin works | Not if it was auto-started. `--idle-exit` gateways stop after `gateway.unmanaged_idle_exit_seconds` (default 600) with no clients, and the checks do not count plugin threads. | `gateway/run_idle_exit.py` |
| Profile context reaches threads | Only if carried over. `get_hermes_home()` reads a `ContextVar`, so a bare thread would fall back to the launch home. Each worker runs in a copy of the context from the `/bulk run` call. | `hermes_constants.py` |
| Plugins run in the gateway process | Yes by default. With `plugins.isolation: host` they run in a per-profile host process; Bulk has not been tested in that mode. | `hermes_cli/plugins_loader.py` |

## Verified signatures

| Capability | API | Source |
|---|---|---|
| Entry point | `register(ctx)` in `__init__.py`, beside `plugin.yaml` | docs; `hermes_cli/plugins_loader.py` |
| Required manifest fields | `name`, `version`, `description` | `hermes_cli/plugin_validate.py` |
| Slash command | `ctx.register_command(name, handler, description="", args_hint="", argument_mode=None)`; handler `fn(raw_args: str, **kw) -> str \| None`, sync or async | `hermes_cli/plugins_context.py` |
| Tool | `ctx.register_tool(name, toolset, schema, handler, check_fn=None, requires_env=None, is_async=False, description="", emoji="", override=False)` | `plugins_context.py` |
| Model call | `ctx.llm.complete(messages, *, provider, model, temperature, max_tokens, timeout, agent_id, profile, purpose, task)`, and `await ctx.llm.acomplete(...)` | `agent/plugin_llm.py` |
| Result | `.text`, `.provider`, `.model`, `.agent_id`, `.audit`, `.usage.input_tokens`, `.usage.output_tokens`, `.usage.cost_usd` | `plugin_llm.py` |
| SQLite | `plugins.plugin_storage.plugin_db(name, filename="data.db")`: WAL, foreign keys on, `check_same_thread=False`, caller owns transactions | `plugins/plugin_storage.py` |
| Data directory | `plugins.plugin_storage.plugin_data_dir(name)`, which follows the active profile | same |
| Async background task | `ctx.spawn_task(coro, *, name=None)`, needs a running loop, cancelled on unload | `plugins_context.py` |
| Cleanup | `ctx.on_unload(callback)` | `plugins_context.py` |
| Session messages | `ctx.post_message(text)`, `ctx.inject_message(content, role="user", *, session_key=None, origin=None)`, `ctx.emit_to_session(session_id, event, payload=None)` | `plugins_context.py`, `hermes_cli/plugin_rpc.py` |
| Validation | `hermes plugins validate <path> [--install-deps]`, `hermes plugins doctor [path-or-id]` | docs; `hermes_cli/subcommands/plugins.py` |

## Differences and limits that affect the design

1. **`usage.cost_usd` is never populated.** The docs say it appears when the provider returns it, but `_extract_usage` in `agent/plugin_llm.py` does not set it. At this commit `spent_usd` stays 0 and a dollar budget cannot be enforced from it. Phase 2 needs another cost source, or a token-based budget.
2. **No host-managed worker or concurrency API exists.** The options are `ctx.spawn_task` or a plugin-owned thread, and Bulk has to bound concurrency itself. Bulk uses plugin-owned threads, for the reason given under Gateway lifecycle.
3. **Plugin-started threads have no Hermes profile scope.** The note on `emit_to_session` says so directly. `plugin_data_dir` and `ctx.llm` read the ambient profile, so Phase 2 workers must capture and restore context (the docs use `contextvars.copy_context().run`).
4. **`ctx.post_message` needs a session bound to the call.** From a detached worker it returns `ok=False`. Completion notices need a stored `session_key` with `inject_message`, or `emit_to_session`. Gateway injection may also require the `allow_gateway_injection` setting. Check this in Phase 2.
5. **The validator imports the plugin without Hermes modules.** Module-level Hermes imports would fail, so Bulk imports them inside functions.
6. **Windows path.** The docs say `~/.hermes`, but on Windows the code defaults to `%LOCALAPPDATA%\hermes`.
7. **`on_unload`, `spawn_task`, `inject_message` and `post_message` are absent from the plugin guide page** (`developer-guide/plugins/index.md`). They are public `PluginContext` methods in the source.

## Not verified

- The guide at `/docs/guides/build-a-hermes-plugin` returned an empty page and has no matching file in the repository's `website/docs/guides/`, so it was not read.
- `requires_hermes: ">=0.23.0"` is the oldest release Bulk has been tested on. Earlier releases were not tested.
