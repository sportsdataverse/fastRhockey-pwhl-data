"""Season compile — accumulate per-game blocks into season-level frames.

Port of the season loop in ``pwhl_data_creation.R``: stream each game's
``final.json`` through :func:`pwhl_data_build.extract.extract_all` and bind the
rows per dataset key.
"""

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from pwhl_data_build.config import ALL_KEYS
from pwhl_data_build.extract import extract_all


def _frame(rows: list[dict]) -> pl.DataFrame | None:
    """Build a frame from heterogeneous row dicts (union of keys, nulls for gaps).

    ``infer_schema_length=None`` scans every row so a field that only appears in
    later games still gets a column; ``strict=False`` tolerates mixed types
    (HockeyTech occasionally flips int/str) instead of raising mid-season.
    """
    if not rows:
        return None
    return pl.DataFrame(rows, infer_schema_length=None, strict=False)


def build_season(games: Iterable[dict]) -> dict[str, pl.DataFrame]:
    """Compile one season's datasets from an iterable of parsed game JSONs.

    Returns only the non-empty datasets, keyed by dataset key.
    """
    acc: dict[str, list[dict]] = {k: [] for k in ALL_KEYS}
    for game_json in games:
        for key, rows in extract_all(game_json).items():
            if rows:
                acc[key].extend(rows)

    out: dict[str, pl.DataFrame] = {}
    for key, rows in acc.items():
        df = _frame(rows)
        if df is not None and df.height > 0:
            out[key] = df
    return out
