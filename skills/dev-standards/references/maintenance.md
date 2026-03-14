# Maintenance and Documentation

## DESIGN.md

Maintain `DESIGN.md` on every code change. Keep it aligned with the current code, not historical intent.

Ensure it covers:

- Overall architecture.
- Module definitions and ownership.
- Interface definitions.
- Module interaction flow.
- Key logic and important control paths.

## Code Cleanup

- Remove dead code immediately when discovered in the edited area.
- Delete unused imports, constants, branches, and helpers.
- Remove old compatibility paths after the new design lands.

## Compatibility Policy

- Favor clean architecture over backward compatibility during internal refactors unless the task explicitly requires compatibility.
- Do not keep intermediate states in the codebase.
- Finish the migration within the same change whenever practical.

## Review Lens

When reviewing or editing, ask:

1. Is the design simpler after the change?
2. Can each module be understood with less context than before?
3. Did the change reduce or increase coupling?
4. Are config and comments aligned with the new behavior?
5. Does `DESIGN.md` describe the code that actually exists?
