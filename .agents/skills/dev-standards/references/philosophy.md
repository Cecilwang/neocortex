# Design Philosophy

## Principles

- Start from first principles. Identify the invariant, not just the failing surface behavior.
- Apply Occam's razor. If two designs solve the same problem, choose the one with fewer moving parts.
- Prefer one clean end state over multi-step compatibility scaffolding.
- Generalize only after finding the shared essence across concrete cases.
- Optimize for local reasoning. A reader should understand a module or function without scanning the whole repository.

## Design Heuristics

- Remove layers that do not add a real boundary, policy, or transformation.
- Refactor when complexity clusters in one file, function, or control path.
- Reduce coupling by narrowing interfaces and moving ownership to the most natural module.
- Reject redundant abstractions, duplicate adapters, and pass-through APIs.
- Promote repeated patterns into `common` only when the abstraction is stable and truly shared.

## Decision Rule

When choosing between alternatives, prefer the option that:

1. Solves the root cause.
2. Reduces total concepts in the system.
3. Improves readability of the steady state.
4. Minimizes cross-module knowledge.
5. Leaves fewer exceptional branches to maintain.
