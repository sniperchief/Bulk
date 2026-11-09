# Bulk

Bulk is a Hermes Agent plugin that applies one instruction to many independent items: files in a directory, CSV rows, or JSON records. Job state lives in SQLite, outside the conversation.

`/bulk run` processes a whole job in the background with bounded concurrency. Bulk retries transient model failures, recovers interrupted items, and supports pause, resume and token limits. Each item's result is saved as its own file; Bulk does not merge results into one document.

Bulk requires Hermes Agent v0.23.0 or later and has been tested on v0.23.0. It has no Python dependencies beyond the standard library.

## Commands

```text
/bulk "<instruction>" --over <path> [--max-tokens <n>] [--retries <n>] [--concurrency <n>]
/bulk run <job_id>
/bulk status [job_id]
/bulk pause <job_id>
/bulk resume <job_id> [--max-tokens <n>]
/bulk retry <job_id>
```

`/bulk resume` and `/bulk retry` also start background processing, unless the job is paused.

## Background processing

`/bulk run` returns straight away and items are processed on plugin-owned threads inside the Hermes process. Use `/bulk status <job_id>` to follow progress. Its `State:` line reads `processing`, `paused`, `completed`, `completed with errors`, `failed`, `awaiting recovery` or `not started`.

- **Concurrency.** `--concurrency` sets how many items run at once (default 4). The `max_concurrency` plugin setting caps it (default 16). A job never has more than that many workers in one process, and each item is claimed atomically, so two workers cannot hold the same item.
- **Background work stops when the gateway stops.** Nothing runs outside Hermes. After a restart, `/bulk status` shows `awaiting recovery`; run `/bulk run <job_id>` to continue. Jobs do not restart by themselves, so a restart never spends tokens without you asking.
- **Keep the gateway running.** A gateway that a Hermes client started by itself exits after 10 minutes with no clients attached, and Bulk's workers do not count as activity. For long jobs, keep a Hermes session open, run `hermes gateway run`, or set `gateway.unmanaged_idle_exit_seconds: 0` in `config.yaml`.
- **Rate limits.** Higher concurrency makes provider rate limits more likely, and the default retries wait only a few seconds. On a rate-limited model, use a lower `--concurrency` or more retries (`--retries 6` waits about 90 seconds in total per item). Items that run out of retries are marked failed, and `/bulk retry` returns them to the queue.

## Reliability

- **Recovery.** Items left `running` by a process that has stopped return to `pending` the next time Bulk starts, and on `/bulk run` and `/bulk resume`. Items owned by a process that is still running are left alone. Lock files left by ended processes are removed the next time Bulk starts.
- **At-least-once delivery.** A process can stop after the model responds but before the result is saved, so a recovered item may be sent to the model, and billed, again. A completed item is never reprocessed.
- **Retries.** Timeouts, connection errors, HTTP 408/409/425/429/5xx and empty responses are retried automatically, 2 times by default, waiting 2, 4, 8 seconds and so on (at most 30). Set the default with the `max_retries` plugin setting or per job with `--retries`. Each failed attempt is recorded in the job's events. `/bulk retry` returns failed items to `pending` and processes them.
- **Pause.** `/bulk pause` stops new items from being claimed. Items already in progress may finish.
- **Token limits.** `--max-tokens` is enforced by Bulk, not by your provider, and it is **not a billing limit**. Before an item starts, Bulk reserves an estimate (about one token per 3 characters of input, plus an output allowance) and pauses the job if it would not fit. Actual usage replaces the reservation when Hermes reports it. Usage from failed or interrupted attempts is not reported, so real usage can exceed the limit.

| Source | Items |
|---|---|
| Directory | One per UTF-8 text file, recursively |
| `.csv` | One per data row |
| `.json` | One per element of a top-level array |
| `.jsonl` | One per non-blank line |
| Any other file | The whole file as one item |

Item content is never truncated. Hidden entries, symbolic links, empty files and non-UTF-8 files in a directory are left out, and each one is listed in the job's events with the reason.

## Install

Requires Hermes Agent with a working model provider (`hermes setup`).

```text
hermes plugins install sniperchief/Bulk
hermes plugins enable bulk
hermes plugins list
```

To install from a local checkout, copy the folder to `<hermes home>/plugins/bulk` and enable it the same way. The Hermes home defaults to `%LOCALAPPDATA%\hermes` on Windows and `~/.hermes` elsewhere.

## Configuration

Plugin settings are declared in `plugin.yaml` under `config_schema`.

| Setting | Default | Meaning |
|---|---|---|
| `max_retries` | 2 | Automatic retries per item after a transient failure. Override per job with `--retries`. |
| `max_concurrency` | 16 | The most items one job may process at once. `--concurrency` cannot exceed it. |

Slash commands run inside the Hermes gateway, which keeps running after the CLI exits. After installing a new version, exit Hermes and run `hermes gateway stop` so the new code loads.

## Test

The unit tests need only Python 3.11+. They replace the model with a stub, so they do not show that a real model call works.

```powershell
python -m unittest discover -s tests -t . -v
```

For the live check, start `hermes` and run these, using the job id that the first command prints:

```text
/bulk "Summarize this report in 3 bullet points" --over C:\path\to\Bulk\tests\fixtures\reports
/bulk run <job_id>
/bulk status <job_id>
```

`/bulk run` should return immediately. Repeat `/bulk status` until it shows 2/2 completed; the summaries are saved under `<hermes home>/plugin-data/bulk/jobs/<job_id>/outputs/`.

## Data and disclosures

- Jobs are stored in `<hermes home>/plugin-data/bulk/data.db`, and results in `<hermes home>/plugin-data/bulk/jobs/<job_id>/outputs/`.
- **Reads outside the plugin's data:** Bulk reads the paths you pass to `--over`, including paths outside its data directory.
- Each processed item is sent to your configured model provider through `ctx.llm`, and that usage is billed to your provider account.
- Each running Hermes process keeps a small lock file in `<hermes home>/plugin-data/bulk/workers/` so that recovery can tell whether a claim's owner is still alive.
- **Network:** Bulk makes no network calls of its own. Model calls go through Hermes to the provider you configured.
- **Background work:** while a job runs, Bulk keeps threads running inside the Hermes process until the job finishes, pauses or Hermes stops. It starts no separate processes and runs no shell commands.
- **Credentials and telemetry:** Bulk never reads or stores provider credentials and sends no telemetry.
- **Errors:** job records and `/bulk status` keep only the error type, the HTTP status and Bulk's own messages, because a provider's error text can echo credentials. The full message goes to the Hermes log, which Hermes redacts.
- **Results are not encrypted.** Output files and the job database contain whatever the model returned and your instruction text. Treat them like the source data.
- **Delivery is at-least-once.** An item interrupted by a crash or shutdown may be sent to the model, and billed, more than once. Completed items are never reprocessed.

## Layout

| File | Responsibility |
|---|---|
| `__init__.py` | Plugin registration |
| `runtime.py` | Access to Hermes services |
| `commands.py` | `/bulk` parsing and output |
| `jobs.py` | Job lifecycle |
| `background.py` | Worker threads for running jobs |
| `store.py` | SQLite schema, migrations and transactions |
| `worker.py` | Process liveness for item claims |
| `discovery.py` | Finding and reloading items |
| `executor.py` | The `ctx.llm` call |
| `results.py` | Saving output files |
| `tests/` | Unit tests and a small dataset |

## License

MIT. See [LICENSE](LICENSE).
