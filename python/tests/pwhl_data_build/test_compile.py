"""Contract tests for the PWHL python compiler (offline, fixture-driven)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pwhl_data_build.build import build_season
from pwhl_data_build.config import ALL_KEYS, OUTPUTS
from pwhl_data_build.extract import extract_all
from pwhl_data_build.io import flatten_nested, write_dataset


def _game(gid: int = 1) -> dict:
    return {
        "pbp": [{"game_id": gid, "event": "shot", "period_of_game": 1}],
        "skaters": [
            {"game_id": gid, "player_id": 7, "starting": "1"},  # string (skater)
            {"game_id": gid, "player_id": 8, "starting": "0"},
        ],
        "goalies": [{"game_id": gid, "player_id": 30, "starting": 1}],  # int (goalie)
        "team_box": [{"game_id": gid, "team_id": 1}, {"game_id": gid, "team_id": 2}],
        "game_info": [{"game_id": gid, "game_date": "2024-01-13"}],
    }


def test_extract_pulls_configured_blocks() -> None:
    out = extract_all(_game())
    assert len(out["pbp"]) == 1
    assert len(out["skater_box"]) == 2
    assert len(out["goalie_box"]) == 1
    assert len(out["team_box"]) == 2
    # absent blocks come back empty, not missing
    assert out["shootout"] == []
    assert set(out) == set(ALL_KEYS)


def test_starting_coerced_to_int_across_skaters_and_goalies() -> None:
    """Skaters send "1"/"0" (str), goalies send 1/0 (int) — must unify or player_box breaks."""
    out = extract_all(_game())
    assert [r["starting"] for r in out["skater_box"]] == [1, 0]
    assert [r["starting"] for r in out["goalie_box"]] == [1]


def test_player_box_is_skaters_plus_goalies_tagged() -> None:
    out = extract_all(_game())
    pb = out["player_box"]
    assert len(pb) == 3  # 2 skaters + 1 goalie
    assert [r["player_type"] for r in pb] == ["skater", "skater", "goalie"]


def test_extract_handles_none_and_odd_shapes() -> None:
    assert extract_all(None)["pbp"] == []
    # a single row serialized as a bare dict still yields one row
    assert len(extract_all({"game_info": {"game_id": 5}})["game_info"]) == 1


def test_build_season_binds_across_games() -> None:
    season = build_season([_game(1), _game(2)])
    assert season["team_box"].height == 4  # 2 games x 2 teams
    assert season["player_box"].height == 6
    assert "shootout" not in season  # empty datasets are dropped


def test_build_season_unions_columns_across_schema_drift() -> None:
    a = {"game_info": [{"game_id": 1}]}
    b = {"game_info": [{"game_id": 2, "attendance": 2417}]}
    df = build_season([a, b])["game_info"]
    assert df.height == 2
    assert "attendance" in df.columns  # later-appearing field still gets a column


def test_write_dataset_emits_all_three_formats(tmp_path: Path) -> None:
    df = pl.DataFrame({"game_id": [1, 2], "team_id": [1, 2]})
    paths = write_dataset(df, tmp_path, "team_box", "team_box", 2024)

    assert paths["parquet"].exists() and paths["rds"].exists() and paths["csv"].exists()
    assert paths["csv"].name == "team_box_2024.csv"  # plain csv, never .csv.gz
    assert not (tmp_path / "team_box" / "csv" / "team_box_2024.csv.gz").exists()
    assert pl.read_parquet(paths["parquet"]).shape == (2, 2)
    assert paths["csv"].read_text().splitlines()[0] == "game_id,team_id"
    assert paths["rds"].stat().st_size > 0


def test_flatten_nested_json_encodes_struct_columns() -> None:
    df = pl.DataFrame({"game_id": [1], "detail": [{"a": 1, "b": 2}]})
    flat = flatten_nested(df)
    assert flat["detail"].dtype == pl.String
    assert "a" in flat["detail"].item()


@pytest.mark.parametrize("key", ALL_KEYS)
def test_every_key_has_a_prefix_and_release_tag(key: str) -> None:
    prefix, tag = OUTPUTS[key]
    assert prefix and tag.startswith("pwhl_")
