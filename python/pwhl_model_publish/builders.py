"""Build the `pwhl_xg_pbp` dataset parquet for the sportsdataverse-data release.

Thin orchestration over the ``sportsdataverse.pwhl`` compute surface, mirroring
``cfbfastR-cfb-data/python/cfb_model_publish/builders.py``:

* :func:`build_xg` -> one ``pwhl_xg_pbp_{season}.parquet`` per season, plus a
  single ``pwhl_xg_pbp_card.json`` provenance sidecar.

The xG model itself (coordinate logistic + per-strength Platt calibration)
lives in ``sportsdataverse.pwhl.pwhl_xg_proxy``; this module reads the
repo-committed play-by-play parquet, fits ONE model on the pooled requested
seasons (PWHL samples are small -- per-season fits would be noisier and score
seasons inconsistently), scores each season with it, and writes frames to disk.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

# PWHL's inaugural season.
MIN_SEASON = 2024

# The committed pbp tree of this repo -- the builder runs from a checkout
# (locally or in Actions), so the input needs no network at all.
DEFAULT_PBP_DIR = Path(__file__).resolve().parents[2] / "pwhl" / "pbp" / "parquet"


def _read_pbp(season: int, pbp_dir: Path) -> pl.DataFrame:
    path = pbp_dir / f"play_by_play_{season}.parquet"
    if not path.exists():
        raise ValueError(
            f"pwhl_xg_pbp: no committed pbp for season {season} ({path}) -- "
            "run the R processor first or drop the season"
        )
    return pl.read_parquet(path)


def build_xg(
    seasons: list[int],
    out_dir,
    *,
    pbp_dir=None,
    fit=None,
    score=None,
) -> list[dict]:
    """Build per-season xG-enriched shots and write ``pwhl_xg_pbp_{season}.parquet``.

    Args:
        seasons: Seasons to build (one parquet per season). The xG model is
            fit ONCE on the pooled pbp of all of them, so every season is
            scored by the same model.
        out_dir: Output directory (created if absent).
        pbp_dir: Directory holding ``play_by_play_{season}.parquet`` (default:
            this repo's committed ``pwhl/pbp/parquet`` tree).
        fit: Injectable ``fit_pwhl_coord_xg``-shaped callable, for hermetic
            tests. Defaults to ``sportsdataverse.pwhl.pwhl_xg_proxy.fit_pwhl_coord_xg``.
        score: Injectable ``pwhl_shot_xg``-shaped callable (``score(pbp,
            model=...)``), for hermetic tests. Defaults to
            ``sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg``.

    Returns:
        List of ``{"season": int, "rows": int, "path": str}`` dicts, one per
        season, in input order.

    Raises:
        ValueError: If a season is below :data:`MIN_SEASON`, its committed pbp
            parquet is missing, or it yields zero shot rows (publishing that
            would ship a silently-empty tag).
    """
    if fit is None or score is None:
        from sportsdataverse.pwhl.pwhl_xg_proxy import fit_pwhl_coord_xg, pwhl_shot_xg

        fit = fit or fit_pwhl_coord_xg
        score = score or pwhl_shot_xg

    too_old = [s for s in seasons if s < MIN_SEASON]
    if too_old:
        raise ValueError(
            f"pwhl_xg_pbp: seasons {too_old} predate the {MIN_SEASON} floor "
            "(PWHL's inaugural season)"
        )

    pbp_dir = Path(pbp_dir) if pbp_dir else DEFAULT_PBP_DIR
    frames = {s: _read_pbp(s, pbp_dir) for s in seasons}
    model = fit(pl.concat(list(frames.values()), how="diagonal_relaxed"))

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    for season in seasons:
        df = score(frames[season], model=model)
        if df.height == 0:
            raise ValueError(
                f"pwhl_xg_pbp: season {season} produced 0 shot rows -- "
                "refusing to publish an empty tag"
            )
        path = out_dir / f"pwhl_xg_pbp_{season}.parquet"
        df.write_parquet(path)
        results.append({"season": season, "rows": df.height, "path": str(path)})
        print(f"xg: season={season} rows={df.height} -> {path}")
    return results


def write_xg_card(results: list[dict], out_dir) -> Path:
    """Write the ``pwhl_xg_pbp`` model card next to the season parquet.

    Carries the T5 gate anchors and the honest caveats, so a consumer can tell
    what this tag's xG values were validated against.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    card = {
        "tag": "pwhl_xg_pbp",
        "grain": "one row per on-net shot per season",
        "source": (
            "sdv-py sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg() over this "
            "repo's committed play-by-play parquet"
        ),
        "seasons": [r["season"] for r in results],
        "rows_by_season": {str(r["season"]): r["rows"] for r in results},
        "model": {
            "method": "coords (distance/angle logistic + strength/clock/shot-type features)",
            "calibration": "per-strength Platt (EV/PP/SH), shooter-relative buckets",
            "fit": (
                "re-fit at publish time on the pooled requested seasons; every "
                "season is scored by the same model, and xg values shift "
                "slightly as new seasons extend the pool (in-sample scoring -- "
                "a descriptive dataset, not a held-out backtest)"
            ),
            "gate_anchors_t5": {
                "note": "LOSO-validated in sdv-py's T5 oracle suite",
                "prod_auc": 0.697,
            },
        },
        "notes": [
            "Only on-net shots exist in HockeyTech pbp (event == 'shot'; goal"
            " is the outcome flag) -- no missed/blocked shots, so xG totals sit"
            " below Corsi-based models by construction.",
            "Coordinates are rink-feet with nets at x = +/-89 (fastRhockey"
            " convention); coverage is ~100% on shot rows, and the rare"
            " null-geometry row falls back to the league goal rate.",
        ],
    }
    path = out_dir / "pwhl_xg_pbp_card.json"
    path.write_text(json.dumps(card, indent=2) + "\n", encoding="utf-8")
    print(f"card: {path}")
    return path
