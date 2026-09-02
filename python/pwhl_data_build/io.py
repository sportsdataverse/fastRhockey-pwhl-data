"""Dataset IO — write the three released formats for one dataset/season.

Every released dataset ships **parquet + rds + csv**:

* ``parquet`` — canonical cross-engine format (the parity bar).
* ``rds``     — what ``fastRhockey::load_pwhl_*()`` reads. Written natively via
  :func:`sportsdataverse._rds.write_rds`; no R round-trip.
* ``csv``     — plain text (never ``.csv.gz``).

All three are **release artifacts**: they ship to the ``pwhl_*`` tags on
``sportsdataverse-data`` and are not committed to this repo.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sportsdataverse._rds import write_rds

from pwhl_data_build.config import TYPES

# The stamp ``fastRhockey:::make_fastRhockey_data()`` applies (fastRhockey/R/utils.R:364).
#
# R's own producer writes a bare ``saveRDS(df, compress = "xz")``, so the released files
# carry none of this — and ``rbindlist_with_attrs()`` (utils.R:400) reads
# fastRhockey_timestamp / fastRhockey_type off the last season's file to re-attach after
# the bind, so every load_pwhl_*() call currently gets NULL for both and prints a blank
# header. Stamping here populates them for the first time.
RDS_CLASS: tuple[str, ...] = (
    "fastRhockey_data",
    "tbl_df",
    "tbl",
    "data.table",
    "data.frame",
)


def flatten_nested(df: pl.DataFrame) -> pl.DataFrame:
    """JSON-encode struct/list columns so every serializer shares one schema.

    Mirrors R's ``.flatten_struct_cols`` (arrow can't write nested data.frame
    columns). Applied to all three formats so parquet/rds/csv agree.
    """
    nested = [
        c
        for c, dt in zip(df.columns, df.dtypes)
        if isinstance(dt, (pl.Struct, pl.List))
    ]
    if not nested:
        return df
    return df.with_columns(
        [
            pl.col(c)
            .map_elements(
                lambda v: None if v is None else json.dumps(v, default=str),
                return_dtype=pl.String,
            )
            .alias(c)
            for c in nested
        ]
    )


def write_dataset(
    df: pl.DataFrame,
    out_dir: Path,
    key: str,
    prefix: str,
    season_year: int,
    data_type: str | None = None,
    timestamp: datetime | None = None,
) -> dict[str, Path]:
    """Write ``{out_dir}/{key}/{parquet,rds,csv}/{prefix}_{season}.*``.

    ``data_type`` is stamped as the rds's ``fastRhockey_type`` (R's
    ``make_fastRhockey_data(df, type, timestamp)``); it defaults to the key's
    registry description. ``timestamp`` defaults to now (R passes ``Sys.time()``)
    and is injectable so tests can assert a fixed value.
    """
    flat = flatten_nested(df)
    stem = f"{prefix}_{season_year}"
    paths: dict[str, Path] = {}
    for sub in ("parquet", "rds", "csv"):
        (out_dir / key / sub).mkdir(parents=True, exist_ok=True)

    paths["parquet"] = out_dir / key / "parquet" / f"{stem}.parquet"
    paths["rds"] = out_dir / key / "rds" / f"{stem}.rds"
    paths["csv"] = out_dir / key / "csv" / f"{stem}.csv"

    flat.write_parquet(paths["parquet"], compression="gzip")
    write_rds(
        flat,
        paths["rds"],
        cls=RDS_CLASS,
        attributes={
            "fastRhockey_timestamp": timestamp or datetime.now(timezone.utc),
            "fastRhockey_type": data_type or TYPES.get(key, key),
        },
    )
    flat.write_csv(paths["csv"])
    return paths
