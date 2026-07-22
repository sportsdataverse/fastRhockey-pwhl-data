"""Per-game extraction — Python port of ``pwhl_data_creation.R``'s ``.extract_all``.

Reads one parsed ``final.json`` and returns ``{dataset_key: list[row-dict]}``.

The HockeyTech feed already ships each block as a flat list of row objects that
carry their own ``game_id``, so this is a straight field pull — no id-attachment
step (unlike the NHL port). Two behaviours are carried over verbatim from R:

* ``starting`` is serialized as a **string** for skaters (``"1"``/``"0"``) but an
  **integer** for goalies. Coerced to int so ``skater_box`` + ``goalie_box`` bind
  into ``player_box`` instead of tripping a type error.
* ``player_box`` is derived by binding the two boxscores with a ``player_type``
  tag (``"skater"`` / ``"goalie"``).
"""

from __future__ import annotations

from typing import Any

from pwhl_data_build.config import DATASETS, PLAYER_BOX


def _rows(val: Any) -> list[dict]:
    """Normalize one JSON block to a list of row dicts (empty when absent/odd-shaped)."""
    if val is None:
        return []
    if isinstance(val, dict):
        # a single row object serialized as a bare dict
        return [val]
    if isinstance(val, list):
        return [r for r in val if isinstance(r, dict)]
    return []


def _coerce_starting(rows: list[dict]) -> list[dict]:
    """Force ``starting`` to int — skaters send "1"/"0", goalies send 1/0."""
    for r in rows:
        if "starting" in r:
            try:
                r["starting"] = int(r["starting"])
            except (TypeError, ValueError):
                r["starting"] = None
    return rows


def extract_all(game_json: dict | None) -> dict[str, list[dict]]:
    """Pull every configured dataset out of one parsed game JSON in a single pass."""
    out: dict[str, list[dict]] = {key: [] for key, *_ in DATASETS}
    out[PLAYER_BOX[0]] = []
    if not isinstance(game_json, dict):
        return out

    for key, field, _prefix, _tag, _desc in DATASETS:
        out[key] = _rows(game_json.get(field))

    out["skater_box"] = _coerce_starting(out["skater_box"])
    out["goalie_box"] = _coerce_starting(out["goalie_box"])

    # player_box = skater_box + goalie_box, tagged by player_type (R: mutate + bind_rows)
    player_box: list[dict] = []
    for key, ptype in (("skater_box", "skater"), ("goalie_box", "goalie")):
        for r in out[key]:
            player_box.append({**r, "player_type": ptype})
    out[PLAYER_BOX[0]] = player_box
    return out
