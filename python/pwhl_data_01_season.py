"""Stage 01 — PWHL season dataset compile (parquet + rds + csv per family).

Thin numbered entry over ``pwhl_data_build.season``; args forward verbatim.
The library package owns the logic; this file makes the repo lifecycle
enumerable: season -> publish (models: pwhl_model_01_xg_pbp). Single home:
models/manifest.yaml.

Usage::

    python -m pwhl_data_01_season -s 2026 [--raw-root URL|path] [--out pwhl]
    scripts/pwhl_data.sh 01
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from pwhl_data_build.season import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
