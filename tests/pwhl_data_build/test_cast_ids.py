"""cast_ids_int64: release ids are Int64 whether the final JSON held "1" or 1; bad values raise."""

import math

import polars as pl
import pytest
from pwhl_data_build.io import cast_ids_int64


def test_text_float_int_ids_all_become_int64_with_nulls_kept():
    df = pl.DataFrame(
        {
            "game_id": [" 7 ", "", None],
            "team_id": [1.0, math.nan, None],
            "player_id": [3, None, 5],
            "name": ["a", "b", "c"],
        }
    )
    out = cast_ids_int64(df)
    assert all(out.schema[c] == pl.Int64 for c in ("game_id", "team_id", "player_id"))
    assert out["game_id"].to_list() == [7, None, None]
    assert out["team_id"].to_list() == [1, None, None]
    assert out["player_id"].to_list() == [3, None, 5]
    assert out["name"].to_list() == ["a", "b", "c"]


@pytest.mark.parametrize("bad", [pl.Series("team_id", ["12a"]), pl.Series("team_id", [1.5])])
def test_non_integer_ids_raise(bad):
    with pytest.raises(ValueError, match="team_id"):
        cast_ids_int64(bad.to_frame())
