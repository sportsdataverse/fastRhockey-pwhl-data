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
from pathlib import Path

import polars as pl

from sportsdataverse._rds import write_rds

# R writes these with a plain ``saveRDS(df, compress = "xz")``
# (R/pwhl_data_creation.R:177) — no custom S3 stamp — so mirror a plain
# data.frame rather than a ``*_data`` class vector.
RDS_CLASS: tuple[str, ...] = ("data.frame",)


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
    df: pl.DataFrame, out_dir: Path, key: str, prefix: str, season_year: int
) -> dict[str, Path]:
    """Write ``{out_dir}/{key}/{parquet,rds,csv}/{prefix}_{season}.*``."""
    flat = flatten_nested(df)
    stem = f"{prefix}_{season_year}"
    paths: dict[str, Path] = {}
    for sub in ("parquet", "rds", "csv"):
        (out_dir / key / sub).mkdir(parents=True, exist_ok=True)

    paths["parquet"] = out_dir / key / "parquet" / f"{stem}.parquet"
    paths["rds"] = out_dir / key / "rds" / f"{stem}.rds"
    paths["csv"] = out_dir / key / "csv" / f"{stem}.csv"

    flat.write_parquet(paths["parquet"], compression="gzip")
    write_rds(flat, paths["rds"], cls=RDS_CLASS)
    flat.write_csv(paths["csv"])
    return paths
