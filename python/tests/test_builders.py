"""Hermetic tests for the pwhl_xg_pbp builder.

The sdv-py fit/score seams are stubbed, so these assert *orchestration* --
pooled single-fit, season ordering, the empty-frame refusal, the card sidecar,
and per-file upload -- not the xG math (gated in sdv-py's own T5 oracle suite).
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from pwhl_model_publish.artifacts import upload_artifacts
from pwhl_model_publish.builders import MIN_SEASON, build_xg, write_xg_card
from pwhl_model_publish.cli import _seasons, main


def _write_pbp(pbp_dir, season: int) -> None:
    pl.DataFrame(
        {
            "game_id": [season * 10 + 1] * 3,
            "event": ["shot", "faceoff", "shot"],
            "goal": [False, None, True],
            "x_coord": [70.0, None, 80.0],
            "y_coord": [5.0, None, -3.0],
        }
    ).write_parquet(pbp_dir / f"play_by_play_{season}.parquet")


def _fake_fit(pbp: pl.DataFrame):
    return {"pooled_rows": pbp.height}


def _fake_score(pbp: pl.DataFrame, *, model) -> pl.DataFrame:
    shots = pbp.filter(pl.col("event") == "shot")
    return shots.with_columns(pl.lit(0.1).alias("xg"))


def test_build_xg_fits_once_on_the_pool_and_writes_per_season(tmp_path):
    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir()
    for s in (2024, 2025):
        _write_pbp(pbp_dir, s)

    fits: list = []

    def fit(pbp):
        fits.append(pbp.height)
        return _fake_fit(pbp)

    results = build_xg(
        [2024, 2025], tmp_path / "out", pbp_dir=pbp_dir, fit=fit, score=_fake_score
    )

    # ONE fit over the pooled seasons (3 rows x 2 seasons), not one per season
    assert fits == [6]
    assert [r["season"] for r in results] == [2024, 2025]
    assert [r["rows"] for r in results] == [2, 2]
    for s in (2024, 2025):
        path = tmp_path / "out" / f"pwhl_xg_pbp_{s}.parquet"
        assert path.exists()
        assert pl.read_parquet(path)["xg"].to_list() == [0.1, 0.1]


def test_build_xg_refuses_an_empty_season(tmp_path):
    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir()
    _write_pbp(pbp_dir, 2025)

    with pytest.raises(ValueError, match="0 shot rows"):
        build_xg(
            [2025],
            tmp_path / "out",
            pbp_dir=pbp_dir,
            fit=_fake_fit,
            score=lambda pbp, *, model: pbp.filter(pl.col("event") == "nope"),
        )

    assert not (tmp_path / "out" / "pwhl_xg_pbp_2025.parquet").exists()


def test_build_xg_rejects_seasons_below_the_floor(tmp_path):
    with pytest.raises(ValueError, match=str(MIN_SEASON)):
        build_xg([MIN_SEASON - 1], tmp_path, fit=_fake_fit, score=_fake_score)


def test_build_xg_rejects_a_missing_pbp_parquet(tmp_path):
    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir()

    with pytest.raises(ValueError, match="no committed pbp"):
        build_xg(
            [2025], tmp_path / "out", pbp_dir=pbp_dir, fit=_fake_fit, score=_fake_score
        )


def test_card_carries_seasons_and_gate_anchor(tmp_path):
    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir()
    _write_pbp(pbp_dir, 2025)
    results = build_xg(
        [2025], tmp_path / "out", pbp_dir=pbp_dir, fit=_fake_fit, score=_fake_score
    )

    path = write_xg_card(results, tmp_path / "out")
    card = json.loads(path.read_text(encoding="utf-8"))

    assert card["tag"] == "pwhl_xg_pbp"
    assert card["seasons"] == [2025]
    assert card["rows_by_season"] == {"2025": 2}
    assert card["model"]["gate_anchors_t5"]["prod_auc"] == pytest.approx(0.697)


def test_upload_pattern_selects_parquet_and_card(tmp_path):
    (tmp_path / "pwhl_xg_pbp_2025.parquet").write_bytes(b"x")
    (tmp_path / "pwhl_xg_pbp_card.json").write_text("{}")
    (tmp_path / "unrelated.txt").write_text("no")

    calls: list = []
    res = upload_artifacts(
        tmp_path,
        "pwhl_xg_pbp",
        "sportsdataverse/sportsdataverse-data",
        pattern="pwhl_xg_pbp_*.*",
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )

    names = sorted(p.rsplit("\\", 1)[-1].rsplit("/", 1)[-1] for p in res["files"])
    assert names == ["pwhl_xg_pbp_2025.parquet", "pwhl_xg_pbp_card.json"]
    assert res["uploaded"] == 2
    assert all("--clobber" in c for c in calls)


def test_seasons_parses_range_and_single():
    assert _seasons("2025") == [2025]
    assert _seasons("2024:2026") == [2024, 2025, 2026]


def test_cli_build_only_writes_files_and_skips_upload(tmp_path, monkeypatch):
    import pwhl_model_publish.cli as cli

    pbp_dir = tmp_path / "pbp"
    pbp_dir.mkdir()
    _write_pbp(pbp_dir, 2025)

    monkeypatch.setattr(
        cli,
        "build_xg",
        lambda seasons, out, **kw: build_xg(
            seasons, out, pbp_dir=pbp_dir, fit=_fake_fit, score=_fake_score
        ),
    )
    monkeypatch.setattr(
        cli,
        "upload_artifacts",
        lambda *a, **k: pytest.fail("--build-only must not upload"),
    )

    rc = main(
        ["xg", "--seasons", "2025", "--out", str(tmp_path / "out"), "--build-only"]
    )

    assert rc == 0
    assert (tmp_path / "out" / "pwhl_xg_pbp_2025.parquet").exists()
    assert (tmp_path / "out" / "pwhl_xg_pbp_card.json").exists()
