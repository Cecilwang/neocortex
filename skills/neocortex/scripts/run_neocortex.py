#!/usr/bin/env python3
"""Run the local neocortex CLI from a skill wrapper."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import subprocess
import sys


def _looks_like_repo_root(path: Path) -> bool:
    return (
        path.is_dir()
        and (path / "pyproject.toml").is_file()
        and (path / "src" / "neocortex").is_dir()
    )


def _resolve_repo_root(explicit_repo: str | None) -> Path:
    cwd = Path.cwd()
    if _looks_like_repo_root(cwd):
        return cwd
    if explicit_repo is not None:
        repo_root = Path(explicit_repo).expanduser().resolve()
        if _looks_like_repo_root(repo_root):
            return repo_root
        raise SystemExit(
            f"Invalid neocortex repo path: {repo_root}. "
            "Expected a repo root with pyproject.toml and src/neocortex/."
        )
    raise SystemExit(
        "Could not resolve a local neocortex repo. "
        "Run this script from the neocortex repo root or pass --repo <path>."
    )


def _has_installed_neocortex() -> bool:
    return importlib.util.find_spec("neocortex") is not None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the local neocortex CLI via uv.",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help="Path to the local neocortex repo root.",
    )
    parser.add_argument(
        "argv",
        nargs=argparse.REMAINDER,
        help="Arguments to pass to `python -m neocortex`.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    argv = list(args.argv)
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        parser.error("missing neocortex CLI arguments")

    if (
        args.repo is None
        and not _looks_like_repo_root(Path.cwd())
        and _has_installed_neocortex()
    ):
        command = [sys.executable, "-m", "neocortex", *argv]
        completed = subprocess.run(command)
        return completed.returncode

    repo_root = _resolve_repo_root(args.repo)
    command = ["uv", "run", "python", "-m", "neocortex", *argv]
    completed = subprocess.run(command, cwd=repo_root)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
