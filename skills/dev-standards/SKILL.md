---
name: dev-standards
description: General software engineering standards for design, implementation, refactoring, and maintenance across languages. Use this skill when the user asks to design, implement, or refactor code in any language.
---

# Dev Standards

## Overview

Apply these rules as the default engineering policy for non-language-specific work. Use this skill to drive design decisions, refactoring scope, naming, modularity, configuration, cleanup, and documentation maintenance.

If the task is in Python, pair this skill with `python-dev-standards` for Python-specific implementation rules.

## Core Workflow

1. Clarify uncertainty first. If a requirement, invariant, or upstream or downstream behavior is unclear and the ambiguity is material, ask before coding.
2. Analyze from first principles. Identify the real problem, constraints, invariants, and reusable pattern behind the request instead of patching the symptom.
3. Trace the call chain before editing. Inspect upstream inputs, downstream consumers, configuration paths, error handling, and tests before changing interfaces or behavior.
4. Design the clean target state. Do not preserve transitional structures for compatibility if they make the code worse.
5. Implement the smallest complete solution. Avoid over-abstraction, wrapper layers, and speculative extensibility.
6. Refactor aggressively when a file or function becomes bloated, duplicated, or hard to reason about.
7. Update `DESIGN.md` on every change so the architecture, modules, interfaces, interaction flow, and key logic remain synchronized with the code.
8. Run project-appropriate validation before finishing. Treat failing checks as unresolved work.

## Non-Negotiable Rules

- Keep functions and files small enough to understand locally. Split oversized logic instead of stacking conditionals.
- Eliminate duplicate logic. Move stable shared logic into `common`.
- Avoid hardcoded values. Promote them to named constants, configuration, or derived values.
- Put user-behavior-facing parameters in configuration, not inline literals.
- Log key decision points, external boundaries, retries, and failures so the runtime path is reconstructable.
- Raise exceptions immediately when invariants fail. Catch them only where recovery, retry, translation, or user-facing handling is valid.
- Add or update tests for every changed behavior.
- Keep comments sparse, necessary, and behavior-oriented.
- Remove dead code during edits. Do not leave fallback branches, unused helpers, or commented-out legacy logic behind.

## Naming and Structure

- Keep variable and function names short but meaningful.
- Do not repeat information already obvious from the file, module, class, or scope.
- Use abbreviations only when they remain readable and domain-correct.
- Prefer names that expose the role of the value, not its type.
- Separate modules to reduce coupling, not to create artificial layers.
- Avoid wrapper functions that only rename or pass through arguments unless they enforce a real boundary.

## Validation Checklist

- Read [references/philosophy.md](references/philosophy.md) when reasoning about tradeoffs, simplification, and refactoring direction.
- Read [references/workflow.md](references/workflow.md) when planning changes, validating a call chain, or deciding the order of work.
- Read [references/general.md](references/general.md) for cross-language engineering rules.
- Read [references/maintenance.md](references/maintenance.md) when updating `DESIGN.md`, cleaning dead code, or deciding whether to keep compatibility layers.

Before finishing, verify all of the following:

1. The solution addresses the root problem rather than the local symptom.
2. The final code has no unnecessary compatibility shims or transitional branches.
3. Shared logic has been deduplicated into the appropriate common location.
4. Logging, exceptions, and tests follow the rules above.
5. Naming, config usage, and comments follow the rules above.
6. `DESIGN.md` reflects the current code accurately.
