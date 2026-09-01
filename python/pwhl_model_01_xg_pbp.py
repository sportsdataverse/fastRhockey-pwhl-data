"""Stage 01 — PWHL xG-enriched shots (scored by the sdv-py xG proxy).

Thin numbered entry over ``pwhl_model_publish xg``; args forward verbatim (injects the ``xg`` subcommand).
Publisher refuses a season with no committed pbp; model fit/gates live in sdv-py.
Usage::

    python -m pwhl_model_01_xg_pbp --seasons 2026 --dry-run
    scripts/pwhl_models.sh 01
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from pwhl_model_publish.cli import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main(["xg", *argv])


if __name__ == "__main__":
    raise SystemExit(main())
