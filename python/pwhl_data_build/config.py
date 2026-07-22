"""Dataset registry — Python port of ``pwhl_data_creation.R``'s ``DATASETS`` tribble.

Each entry maps a per-game ``final.json`` block to a season-level dataset and its
``sportsdataverse-data`` release tag. ``player_box`` is *derived* (skater_box +
goalie_box, tagged with ``player_type``), not a direct block.

Season is the **end year** everywhere (2026 == the 2025-26 season), matching
``fastRhockey::most_recent_pwhl_season()``.
"""

from __future__ import annotations

# (key, json_field, file_prefix, release_tag)
DATASETS: list[tuple[str, str, str, str]] = [
    ("pbp", "pbp", "play_by_play", "pwhl_pbp"),
    ("shifts", "shifts", "shifts", "pwhl_shifts"),
    ("skater_box", "skaters", "skater_box", "pwhl_skater_boxscores"),
    ("goalie_box", "goalies", "goalie_box", "pwhl_goalie_boxscores"),
    ("team_box", "team_box", "team_box", "pwhl_team_boxscores"),
    ("game_info", "game_info", "game_info", "pwhl_game_info"),
    ("game_rosters", "game_rosters", "game_rosters", "pwhl_game_rosters"),
    ("scoring_summary", "scoring_summary", "scoring_summary", "pwhl_scoring_summary"),
    ("penalty_summary", "penalty_summary", "penalty_summary", "pwhl_penalty_summary"),
    ("three_stars", "three_stars", "three_stars", "pwhl_three_stars"),
    ("officials", "officials", "officials", "pwhl_officials"),
    ("shots_by_period", "shots_by_period", "shots_by_period", "pwhl_shots_by_period"),
    ("shootout", "shootout_summary", "shootout_summary", "pwhl_shootout"),
]

# Derived: bind of skater_box + goalie_box with a player_type column.
PLAYER_BOX: tuple[str, str, str] = ("player_box", "player_box", "pwhl_player_boxscores")

# Every key that gets compiled + published (direct blocks + the derived one).
ALL_KEYS: list[str] = [key for key, *_ in DATASETS] + [PLAYER_BOX[0]]

# key -> (file_prefix, release_tag), including the derived player_box.
OUTPUTS: dict[str, tuple[str, str]] = {
    key: (prefix, tag) for key, _field, prefix, tag in DATASETS
} | {PLAYER_BOX[0]: (PLAYER_BOX[1], PLAYER_BOX[2])}
