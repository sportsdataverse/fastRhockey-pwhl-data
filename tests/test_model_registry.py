"""models/REGISTRY.md carries the pwhl_xg_pbp row (Track C guard)."""

from pathlib import Path

REGISTRY = Path(__file__).resolve().parents[1] / "models" / "REGISTRY.md"


def _rows() -> list[str]:
    text = REGISTRY.read_text(encoding="utf-8")
    return [ln for ln in text.splitlines() if ln.startswith("|") and "---" not in ln]


def test_registry_exists():
    assert REGISTRY.is_file(), "models/REGISTRY.md is missing"


def test_xg_row_present_with_card():
    row = next((r for r in _rows() if "`pwhl_xg_pbp`" in r), None)
    assert row, "no registry row for pwhl_xg_pbp"
    assert "pwhl_xg_pbp_card.json" in row, "row must name the card sidecar"
    assert "pwhl_shot_xg" in row, "row must name the sdv-py scoring function"
