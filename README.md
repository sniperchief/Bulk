# Bulk

**Apply one instruction to hundreds of files, CSV rows or JSON records, from a single Hermes Agent command.**

Bulk is a native plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent). You give it an instruction and a source. It processes each item with its own separate model call, in the background, and keeps track of the job outside your conversation. If Hermes stops halfway through, the job picks up where it left off.

```text
/bulk "Summarize this report in 3 bullet points" --over ./reports
/bulk run <job_id>
/bulk status <job_id>
/bulk export <job_id>
```

## Contents

- [Why Bulk](#why-bulk)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Supported sources](#supported-sources)
- [Command reference](#command-reference)
- [Results](#results)
- [Configuration](#configuration)
- [Reliability](#reliability)
- [How it works](#how-it-works)
- [Data, privacy and security](#data-privacy-and-security)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [License](#license)

## Why Bulk

Pasting hundreds of items into a conversation does not work well. The context fills up, items influence each other, and a crash at item 180 leaves you guessing what finished.

Bulk treats the work as a batch job instead:

- Each item is sent to the model **on its own**, with only your instruction, so items cannot affect each other.
- Progress is stored in **SQLite**, not in the conversation.
- `/bulk run` **returns immediately**, and you can keep using Hermes while the job runs.
- A crash, restart or rate limit does not cost you the work that has already finished.

## Features

- **Files, CSV, JSON and JSONL** sources. Content is never truncated.
- **Background processing** with a configurable number of items running at once.
- **Crash and restart recovery**, with interrupted items returned to the queue.
- **Automatic retries** for timeouts, connection errors and server errors.
- **Rate-limit handling**: all workers wait together, then continue.
- **Pause and resume** at any point.
- **Token limits** that pause a job before it goes over.
- **Manual retry** of failed items.
- **Combined export** to a single Markdown or CSV file.
- **No API keys and no dependencies.** Bulk uses your existing Hermes model setup and the Python standard library.

## Requirements

- Hermes Agent **v0.23.0 or later**, with a working model provider (`hermes setup`). Bulk has been tested on v0.23.0.
- Python 3.11 or later, only if you want to run the unit tests.

## Installation

```text
hermes plugins install sniperchief/Bulk
hermes plugins enable bulk
hermes plugins list
```

To install from a local checkout, copy the folder to `<hermes home>/plugins/bulk` and enable it the same way. The Hermes home defaults to `%LOCALAPPDATA%\hermes` on Windows and `~/.hermes` elsewhere.

Slash commands run inside the Hermes gateway, which keeps running after the CLI exits. **After installing or updating Bulk, exit Hermes and run `hermes gateway stop`** so the new code loads.

## Quick start

**1. Create a job.** This finds the items and makes no model calls.

```text
/bulk "Summarize this report in 3 bullet points" --over ./reports --concurrency 3
```

```text
Created job 29b74c4db08b with 20 items.
Start it with: /bulk run 29b74c4db08b
```

**2. Start it.** The command returns straight away.

```text
/bulk run 29b74c4db08b
```

**3. Check progress** whenever you like.

```text
/bulk status 29b74c4db08b
```

```text
Job 29b74c4db08b: running
State: processing
Items: 12/20 completed, 0 failed, 5 pending, 3 running
Actual tokens (reported by Hermes): 2016 in, 334 out
Estimated tokens for the whole job: 8383
Concurrency: up to 3 items at once
```

**4. Combine the results** into one file.

```text
/bulk export 29b74c4db08b
```

## Supported sources

| Source | One item per | Item name |
|---|---|---|
| Directory, searched recursively | UTF-8 text file | Relative path, such as `2026/q1.txt` |
| `.csv` file with a header row | Data row | `row:1`, `row:2`, ... |
| `.json` file with a top-level array | Array element | `record:1`, `record:2`, ... |
| `.jsonl` file | Non-blank line | `line:1`, `line:2`, ... |
| Any other file | The whole file | The file name |

Hidden entries, symbolic links, empty files and non-UTF-8 files in a directory are left out. Each one is recorded in the job's events with the reason.

Bulk checks each item's content when it is processed. If a source changed after the job was created, that item fails rather than being processed against content you did not submit.

## Command reference

| Command | What it does |
|---|---|
| `/bulk "<instruction>" --over <path> [options]` | Creates a job. |
| `/bulk run <job_id>` | Processes all pending items in the background. |
| `/bulk status [job_id]` | Shows one job, or the most recent jobs. |
| `/bulk pause <job_id>` | Stops new items from starting. Items in progress may finish. |
| `/bulk resume <job_id> [--max-tokens <n>]` | Continues a paused job, optionally with a new limit. |
| `/bulk retry <job_id>` | Returns failed items to the queue and processes them. |
| `/bulk export <job_id> [--format md\|csv]` | Writes all results to one file. Makes no model calls. |

`/bulk resume` and `/bulk retry` start processing straight away, unless the job is paused.

**Options when creating a job**

| Option | Default | Meaning |
|---|---|---|
| `--over <path>` | Required | The directory or file to process. |
| `--concurrency <n>` | 4 | How many items run at once. Cannot exceed `max_concurrency`. |
| `--retries <n>` | 2 | Automatic retries per item. `0` turns off retries and rate-limit waits. |
| `--max-tokens <n>` | None | Pauses the job before it goes over this many tokens. |

**States shown by `/bulk status`**

| State | Meaning |
|---|---|
| `not started` | The job exists and has not been run. |
| `processing` | Workers are running. It also says when Bulk is waiting on a rate limit. |
| `paused` | No new items will start. |
| `awaiting recovery` | Work is unfinished and no worker is running. Continue with `/bulk run`. |
| `completed` | Every item finished. |
| `completed with errors` | Some items failed. |
| `failed` | Every item failed. |

## Results

Files are stored under `<hermes home>/plugin-data/bulk/jobs/<job_id>/`:

```text
outputs/000000.md    one file per item, numbered in item order
outputs/000001.md
results.md           written by /bulk export
results.csv          written by /bulk export --format csv
```

You can export at any time, including while a job is running. Failed and unfinished items are listed without a result, and running the export again replaces the earlier file. CSV files have the columns `item`, `status`, `result` and `error`, and are readable by Excel.

## Configuration

Add these to `config.yaml`, or use the Plugins settings in Hermes Desktop:

```yaml
plugins:
  entries:
    bulk:
      settings:
        max_retries: 2
        max_concurrency: 16
```

| Setting | Default | Meaning |
|---|---|---|
| `max_retries` | 2 | Default automatic retries per item. `--retries` overrides it for one job. |
| `max_concurrency` | 16 | The most items one job may process at once. Lowering it also applies to existing jobs. |

## Reliability

**Recovery.** Items left running by a process that has stopped return to the queue the next time Bulk starts. Items owned by a process that is still alive are left alone. Jobs never restart by themselves, so a restart cannot spend tokens without you asking: `/bulk status` shows `awaiting recovery`, and `/bulk run` continues the job.

**At-least-once delivery.** Bulk does not promise exactly-once processing. A process can stop after the model has responded but before the result is saved, so an interrupted item may be sent to the model, and billed, again. An item that has completed is never reprocessed, and its result is never overwritten.

**Automatic retries.** Timeouts, connection errors, empty responses and HTTP 408, 409, 425 and 5xx responses are retried with waits of 2, 4, 8 seconds and so on, up to 30 seconds. Each failed attempt is recorded in the job's events.

**Rate limits.** When the provider answers HTTP 429, every worker waits together. Bulk uses the provider's `Retry-After` value when it sends one. Otherwise it waits 5 seconds, doubling up to 2 minutes. These waits do not use up `--retries`. An item fails only after 8 waits, about 9 minutes in total, and `/bulk retry` can return it to the queue.

**Token limits.** `--max-tokens` is enforced by Bulk from estimates and reported usage. **It is not a billing limit.** Before an item starts, Bulk reserves an estimate of about one token per 3 characters of input plus an output allowance, and pauses the job if the item would not fit. Actual usage replaces the estimate when Hermes reports it. Usage from failed or interrupted attempts is not reported, so real usage can exceed the limit.

**Keeping a long job running.** Background work lives inside the Hermes gateway and stops when the gateway stops. A gateway that a Hermes client started automatically exits after 10 minutes with no clients attached, and Bulk's workers do not count as activity. For long jobs, do one of the following:

- Keep a Hermes session open.
- Run `hermes gateway run`.
- Set `gateway.unmanaged_idle_exit_seconds: 0` in `config.yaml`.

## How it works

```text
/bulk "<instruction>" --over <path>
   discovers the items and records them in SQLite

/bulk run <job_id>
   starts up to <concurrency> worker threads, then returns

each worker, until no items are left:
   1. atomically claim the next pending item
   2. confirm its source content has not changed
   3. call the model with the instruction and that one item
   4. save the result and mark the item complete in one transaction
```

| File | Responsibility |
|---|---|
| `__init__.py` | Plugin registration |
| `runtime.py` | Access to Hermes services |
| `commands.py` | `/bulk` parsing and output |
| `jobs.py` | Job lifecycle, retries and rate-limit waits |
| `background.py` | Worker threads for running jobs |
| `store.py` | SQLite schema, migrations and transactions |
| `worker.py` | Process liveness for item claims |
| `discovery.py` | Finding and reloading items |
| `executor.py` | The `ctx.llm` call and error classification |
| `results.py` | Result files and combined export |
| `tests/` | Unit tests and a small dataset |

Each claim is a single `BEGIN IMMEDIATE` transaction, so two workers cannot hold the same item. Each process holds an operating-system file lock for as long as it runs, which lets a new process tell a stopped worker from a live one. Hermes API findings are recorded in [docs/API_VERIFICATION.md](docs/API_VERIFICATION.md).

## Data, privacy and security

- **Model provider:** each processed item is sent to the provider you configured in Hermes, through `ctx.llm`, and that usage is billed to your provider account.
- **Network:** Bulk makes no network calls of its own.
- **Files read:** Bulk reads the paths you pass to `--over`, including paths outside its data directory. It never changes them.
- **Files written:** only under `<hermes home>/plugin-data/bulk/`, which holds the `data.db` job database, the `jobs/` results and the `workers/` lock files.
- **Background work:** while a job runs, Bulk keeps threads running inside the Hermes process. It starts no separate processes and runs no shell commands.
- **Credentials:** Bulk never reads or stores provider credentials.
- **Telemetry:** none.
- **Errors:** job records and `/bulk status` keep only the error type, the HTTP status and Bulk's own messages, because a provider's error text can echo credentials. The full message goes to the Hermes log, which Hermes redacts.
- **Prompt injection:** the model is told to treat each item as data, but no such instruction is a guarantee. Review results from untrusted sources.
- **CSV exports:** cells that start with `=`, `+`, `-` or `@` get a leading `'` so a spreadsheet does not run them as formulas.
- **Storage is not encrypted.** Result files and the job database contain your instruction and the model's output. Treat them like the source data.

## Limitations

- Each item is one model call that returns text. Bulk cannot browse, run commands or use tools.
- Bulk does not notify you when a job finishes. Use `/bulk status`.
- Background work runs only while the Hermes gateway is running.
- Delivery is at-least-once, and token limits are approximate.
- Bulk has been tested on Hermes v0.23.0 on Windows. `plugins.isolation: host` mode has not been tested.
- Against a real provider, recovery, pause and resume, token limits, HTTP 429 handling and manual retry have been tested. Timeouts, connection errors and 5xx responses are covered by unit tests with a stub model.

## Troubleshooting

| Problem | Cause and fix |
|---|---|
| A new option or command is not recognized after an update | The gateway still has the old code. Exit Hermes and run `hermes gateway stop`. |
| `State: awaiting recovery` | Hermes stopped during the job, or a worker hit an unexpected error. Run `/bulk run <job_id>` to continue. |
| Items fail with `RateLimitError (HTTP 429)` | The provider limit outlasted Bulk's waits. Wait a few minutes, run `/bulk retry <job_id>`, and use a lower `--concurrency` for large jobs. |
| `source content changed since the job was created` | The file changed after the job was created. Restore it and run `/bulk retry <job_id>`, or create a new job. |
| The job paused by itself | It reached its token limit. Continue with `/bulk resume <job_id> --max-tokens <n>`. |
| An error says the details are in the Hermes log | Look in `<hermes home>/logs/gateway.log`. Bulk keeps provider error text out of job records on purpose. |

## Development

The unit tests use only the standard library and need neither Hermes nor a model provider:

```text
python -m unittest discover -s tests -t . -v
```

They replace the model with a stub, so they do not show that a real model call works. Hermes's validator is the catalog admission check:

```text
hermes plugins validate <path to this repository> --install-deps
```

For a live check in a Hermes session, using the small dataset in this repository:

```text
/bulk "Summarize this report in 3 bullet points" --over <path to this repository>/tests/fixtures/reports
/bulk run <job_id>
/bulk status <job_id>
```

`/bulk run` should return immediately, and the status should reach 2/2 completed.

Hermes modules must be imported inside functions, because `hermes plugins validate` imports the plugin in a bare interpreter. Use only documented Hermes plugin surfaces.

## License

[MIT](LICENSE) © 2026 Sniperchief
