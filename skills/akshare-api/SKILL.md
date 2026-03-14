---
name: akshare-api
description: AkShare data access workflow and API lookup guidance. Use this skill when Codex needs to fetch financial, macro, stock, fund, futures, bond, or alternative data with AkShare, choose the right AkShare function from the official documentation, understand the returned schema, or implement and debug AkShare-based data collection in Python.
---

# AkShare API

## Overview

Use this skill to work with AkShare in Python. Prefer the official documentation at `https://akshare.akfamily.xyz/index.html` to locate the right function, confirm parameters, and inspect the expected return shape before writing code.

## Workflow

1. Identify the data domain first: stock, index, fund, futures, options, bond, macro, currency, commodity, news, or another dataset family.
2. Open the official AkShare docs and navigate to the matching section before guessing function names.
3. Read the target function page carefully: function name, parameters, examples, and returned columns.
4. Implement the smallest call that proves the API works.
5. Normalize column names, dtypes, and dates only after confirming the raw return shape.
6. Add defensive handling for empty data, schema drift, rate limits, and upstream failures.

## Rules

1. Always prefer the official AkShare docs over memory when choosing an API.
2. Verify the exact function name and parameters before coding.
3. Keep the raw AkShare fetch step separate from downstream cleaning or business logic.
4. Inspect the returned DataFrame columns before assuming schema.
5. Add logging around request scope, function choice, row counts, and failure paths.
6. Raise clear exceptions when a required dataset is unavailable or returns unexpected columns.
7. Keep AkShare-specific code isolated behind small helpers so upstream API changes are easier to contain.

## Reference Map

- Read [references/official-docs.md](references/official-docs.md) for the official documentation entrypoints and lookup strategy.

## Finish Check

1. The chosen AkShare function matches the target dataset.
2. Parameters were verified against the official docs.
3. The returned schema was inspected instead of assumed.
4. Fetch logic is separated from cleaning and downstream logic.
5. Logging and error handling make upstream failures diagnosable.
