# General Engineering Rules

## Simplicity

- Keep functions focused on one job.
- Keep files compact enough to scan quickly.
- Split modules when cohesion drops or responsibilities diverge.
- Avoid over-design, speculative extension points, and redundant wrappers.

## Naming

- Use concise names with clear meaning.
- Do not restate surrounding context in identifiers.
- Favor domain semantics over generic placeholders.

## Reuse

- Never duplicate stable logic.
- Extract common code into `common` when the abstraction is mature and shared by multiple modules.
- Do not create `common` utilities for one caller.

## Configuration

- Never hardcode values that may change by environment, user behavior, or deployment.
- Put user-facing behavioral parameters into configuration.
- Keep configuration names explicit and discoverable.

## Comments

- Write only necessary comments.
- Explain intent, invariants, or non-obvious behavior.
- Do not narrate obvious code line by line.
