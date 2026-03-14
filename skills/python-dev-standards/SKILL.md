---
name: python-dev-standards
description: Python-specific implementation standards. Use this skill when the user asks to build, modify, review, or refactor Python code.
---

# Python Dev Standards

## Overview

Apply these rules for Python-specific implementation details. Keep this skill narrow and pair it with `dev-standards` for cross-language engineering.

## Rules

1. Use `dev-standards` alongside this skill.
2. Use `uv` to manage Python projects, environments, and dependencies.
3. Add dependencies only when needed. Prefer the standard library or existing project code first.
4. Add type hints to every function definition.
5. Use `logging` for runtime output. Do not leave committed `print` debugging behind.
6. Always run static checks before finishing.

## Finish Check

1. The project is managed with `uv`.
2. Dependencies were added only if necessary.
3. Function definitions include type hints.
4. Logging uses the `logging` module rather than committed `print` debugging.
5. Static checks cover the changed behavior.
