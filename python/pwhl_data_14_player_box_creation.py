"""Stage 14 — pwhl `player_box` season dataset (parse + compile ONE family).

Thin numbered entry over ``pwhl_data_build.season`` restricted to the ``player_box``
family (``--families player_box`` injected; remaining args forward verbatim).
Single home for the family list: the config DATASETS registry — these
stages are generated from it. Publish is the final numbered stage.

Usage::

    python -m pwhl_data_14_player_box_creation -s 2026
    scripts/pwhl_data.sh 14
"""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    from pwhl_data_build.season import main as _main

    argv = list(argv) if argv is not None else sys.argv[1:]
    return _main([*argv, "--families", "player_box"])


if __name__ == "__main__":
    raise SystemExit(main())
