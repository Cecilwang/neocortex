# Official Docs

Primary source:

- `https://akshare.akfamily.xyz/index.html`

Use the official documentation as the source of truth for:

- Available dataset families.
- Exact AkShare function names.
- Required and optional parameters.
- Example calls.
- Returned DataFrame columns and sample rows.

## Lookup Strategy

1. Start at the documentation index.
2. Find the closest data domain page.
3. Search within that page for the business object you need.
4. Match on function name, parameter list, and example output.
5. Only then write or modify code.

## Common Tasks

- If the user asks for A-share, HK, US, index, ETF, fund, macro, or futures data, first navigate to that domain rather than keyword-guessing globally.
- If multiple AkShare functions look similar, prefer the one whose examples and returned columns match the target use case most closely.
- If a function is undocumented, unstable, or the schema differs from examples, treat the live return shape as authoritative and add explicit guards.

## Implementation Pattern

Use a thin wrapper around AkShare:

1. Validate business inputs.
2. Call one AkShare function.
3. Check for empty data and required columns.
4. Normalize types and names if needed.
5. Return a stable internal schema to the rest of the project.
