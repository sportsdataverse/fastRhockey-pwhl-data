"""PWHL season-dataset compiler — python port of ``R/pwhl_data_creation.R``.

Reads per-game HockeyTech JSON from ``fastRhockey-pwhl-raw`` and compiles
season-level datasets, writing parquet + rds + csv and publishing all three to
the ``pwhl_*`` release tags on ``sportsdataverse-data``.
"""

from pwhl_data_build.config import ALL_KEYS, DATASETS, OUTPUTS

__all__ = ["ALL_KEYS", "DATASETS", "OUTPUTS"]
