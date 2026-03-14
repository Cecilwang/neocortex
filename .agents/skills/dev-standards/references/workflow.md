# Development Workflow

## Change Sequence

1. Clarify uncertain requirements if the ambiguity can change the design.
2. Inspect the full call chain: entrypoints, config, dependencies, consumers, side effects, and tests.
3. Define the target design before editing.
4. Implement directly toward the target state.
5. Refactor adjacent duplication or dead code while the context is loaded.
6. Update `DESIGN.md`.
7. Run project-appropriate validation.

## Call-Chain Analysis

Before changing code, inspect:

- Who calls the code.
- What contracts the code assumes.
- What data shape enters and leaves.
- Which config values affect behavior.
- What logs, exceptions, and retries exist.
- Which downstream modules depend on current semantics.

Do not change a function in isolation when the real contract is enforced elsewhere.

## Refactoring Policy

- Prefer aggressive cleanup when the desired design is clear.
- Do not preserve obsolete interfaces "just in case".
- Do not leave intermediate code paths behind after a migration.
- Remove dead code in the same change unless it is explicitly out of scope.
