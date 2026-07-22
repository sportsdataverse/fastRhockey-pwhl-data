"""Season driver — enumerate a season's games, compile, write the release formats.

Mirrors ``pwhl_data_creation.R``: read the season schedule to get its game ids,
stream each ``pwhl/json/final/{game_id}.json``, compile, then write
parquet + rds + csv per dataset.

``raw_root`` is either a local ``fastRhockey-pwhl-raw`` checkout or the
``raw.githubusercontent.com`` base URL (R's ``RAW_BASE``).

Season is the **end year** (2026 == 2025-26).
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import polars as pl

from pwhl_data_build.build import build_season
from pwhl_data_build.config import OUTPUTS
from pwhl_data_build.io import write_dataset

log = logging.getLogger(__name__)

RAW_BASE = "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-pwhl-raw/main"


def _is_url(root: str | Path) -> bool:
    return str(root).startswith(("http://", "https://"))


def _read_bytes(root: str | Path, rel: str) -> bytes | None:
    """Fetch ``rel`` under ``root`` from disk or over HTTP; None when missing."""
    if _is_url(root):
        try:
            with urllib.request.urlopen(
                f"{str(root).rstrip('/')}/{rel}", timeout=60
            ) as r:
                return r.read()
        except Exception:
            return None
    p = Path(root) / rel
    return p.read_bytes() if p.exists() else None


def season_game_ids(season_year: int, raw_root: str | Path) -> list[str]:
    """Game ids for ``season_year`` from the raw schedule parquet.

    Read via pyarrow: the R/arrow-written parquet carries key-value metadata that
    is not valid UTF-8, which polars rejects outright.
    """
    import io as _io

    import pyarrow.parquet as pq

    rel = f"pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"
    raw = _read_bytes(raw_root, rel)
    if raw is None:
        return []
    table = pq.read_table(_io.BytesIO(raw))
    if "game_id" not in table.column_names:
        return []
    return [str(g) for g in table.column("game_id").to_pylist() if g is not None]


def iter_final(raw_root: str | Path, game_ids: list[str]) -> Iterator[dict]:
    """Yield each game's parsed ``final.json`` (skipping any that are missing)."""
    for gid in game_ids:
        raw = _read_bytes(raw_root, f"pwhl/json/final/{gid}.json")
        if raw is None:
            continue
        try:
            yield json.loads(raw)
        except json.JSONDecodeError:
            log.warning("game %s: unparseable final.json, skipping", gid)


def write_datasets(
    season: dict[str, pl.DataFrame], out_dir: str | Path, season_year: int
) -> dict[str, int]:
    """Write parquet + rds + csv for every compiled dataset; return row counts."""
    out = Path(out_dir)
    written: dict[str, int] = {}
    for key, df in season.items():
        prefix, _tag = OUTPUTS[key]
        write_dataset(df, out, key, prefix, season_year)
        written[key] = df.height
    return written


def compile_season(season_year: int, raw_root: str | Path) -> dict[str, pl.DataFrame]:
    """Enumerate + compile one season's datasets."""
    gids = season_game_ids(season_year, raw_root)
    log.info("season %s: %d games in schedule", season_year, len(gids))
    return build_season(iter_final(raw_root, gids))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Compile PWHL season datasets from raw JSON."
    )
    ap.add_argument(
        "-s",
        "--season",
        type=int,
        required=True,
        help="season END year (2026 = 2025-26)",
    )
    ap.add_argument(
        "--raw-root", default=RAW_BASE, help="pwhl-raw checkout path or base URL"
    )
    ap.add_argument("--out", default="pwhl", help="output dir (default: pwhl)")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    season = compile_season(args.season, args.raw_root)
    written = write_datasets(season, args.out, args.season)
    for key, n in sorted(written.items()):
        log.info("%s: %d rows", key, n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
