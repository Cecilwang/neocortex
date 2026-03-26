# Neocortex Reference

This reference is self-contained for the installable `neocortex` skill.

## Entry

Default command shape:

```bash
uv run python -m neocortex ...
```

If `neocortex` is already available in the current Python environment, the skill wrapper may also run:

```bash
python -m neocortex ...
```

Top-level shared options always go before the domain command:

```bash
uv run python -m neocortex --env-file .env.local --log-level DEBUG ...
```

## Domain Guide

- `market-data-provider`
  - Default runtime read path
  - Use for profiles, bars, trading dates, fundamentals, disclosures, and macro
- `indicator`
  - Use for technical indicators
- `connector`
  - Use only for single-source debugging
- `db`
  - Use only for direct SQLite inspection and troubleshooting
- `sync`
  - Use for explicit sync and backfill
- `agent`
  - Use for single-agent request and prompt rendering
- `feishu`
  - Use for bot transport operations, not general market-data reads

## Preferred Calling Patterns

- Company profile: `market-data-provider profile`
- Historical bars: `market-data-provider bars`
- Indicators: `indicator <name>`
- Source-level debugging: `connector ...`
- Bottom-table inspection: `db query`

## Structured Output

Prefer JSON when supported:

- `db query --format json`
- `indicator <name> --format json`
- `agent render --format json`
- `market-data-provider profile`
- `market-data-provider fundamentals`
- `market-data-provider disclosures`
- `market-data-provider macro`
- `sync securities`
- `sync bars`
- `sync trading-dates`

Commands that currently return table text instead of JSON include:

- `market-data-provider bars`
- `market-data-provider securities`
- most `connector` list and price-bar commands

## Failure Semantics

- usage/help errors return command help text
- `db query` only allows single read-only SQL
- write SQL, DDL, multi-statement SQL, and invalid table names are rejected
- options belong on leaf commands only
- CN market date defaults are market-aware rather than blindly using today
- `sync bars` requires exactly one target mode:
  - `--symbol` / `--name`
  - `--ticker`
  - `--all-securities`
- `sync bars --all-securities` is async only in Feishu transport; local CLI stays sync

## Copyable Examples

Profile:

```bash
uv run python -m neocortex market-data-provider profile --name 中芯国际
```

Bars:

```bash
uv run python -m neocortex market-data-provider bars \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20
```

Indicator JSON:

```bash
uv run python -m neocortex indicator macd \
  --name 中芯国际 \
  --start-date 2026-03-01 \
  --end-date 2026-03-20 \
  --format json
```

Connector debug:

```bash
uv run python -m neocortex connector efinance profile --name 中芯国际
```

DB query JSON:

```bash
uv run python -m neocortex db query \
  --table daily_price_bars \
  --limit 20 \
  --format json
```

Agent render:

```bash
uv run python -m neocortex agent render \
  --role technical \
  --name 中芯国际 \
  --as-of-date 2026-03-20 \
  --format json
```

Feishu cleanup:

```bash
uv run python -m neocortex feishu cleanup --older-than-days 3
```
