---
name: neocortex
description: Use when an agent needs to query market data, inspect the local SQLite market DB, compute indicators, debug connector or provider behavior, render single-agent prompts, or manage the Feishu transport through the local neocortex command-line interface. Requires a local neocortex repo checkout and uv.
---

# Neocortex

Use this skill when you need to drive `neocortex` through its local CLI instead of reimplementing business logic.

This skill assumes:

- either a Python environment where `neocortex` is already importable
- or a local `neocortex` repository checkout plus `uv`

It is written to stay compatible with multiple skill hosts. The installable artifact is this directory itself.

## Workflow

1. Locate the local `neocortex` repo.
2. Choose the narrowest CLI domain that matches the task.
3. Prefer structured output when the command supports it.
4. Use provider-level commands by default; only drop to connector or DB when debugging.
5. Read `references/cli.md` when you need command selection help, failure semantics, or copyable examples.

## Repo Resolution

Use `scripts/run_neocortex.py` to execute commands.

Resolution order is fixed:

1. If the current Python environment already has `neocortex`, run `python -m neocortex`.
2. Otherwise, if the current working directory is the `neocortex` repo root, use it.
3. Otherwise pass `--repo <path>`.
4. If neither is true, fail fast and ask for a local `neocortex` repo path.

The wrapper does not clone repos, install dependencies, or scan arbitrary directories.

## Command Selection

- Use `market-data-provider` by default for reading data.
- Use `indicator` for technical indicator calculation.
- Use `connector` only for single-source debugging.
- Use `db query` only for SQLite inspection and troubleshooting.
- Use `agent render` for single-agent prompt/request debugging.
- Use `feishu` only for transport management.

## Output Guidance

- Prefer JSON when available.
- If the command only supports table text, consume stdout as presentation text rather than trying to parse it as structured data.

## Trigger Examples

- “查中芯国际最近行情”
- “看看本地 daily_price_bars 表里有什么”
- “算一下 MACD，最好给 JSON”
- “调试一下 efinance profile 为什么和 provider 不一致”
- “渲染 technical agent 的 prompt”
- “清理 Feishu bot DB 的旧 job”

## Resources

- Read `references/cli.md` for domain selection, failure semantics, and ready-to-run command templates.
- Use `scripts/run_neocortex.py` instead of rebuilding the shell command yourself.
