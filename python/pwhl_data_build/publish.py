"""Publish season datasets to the ``pwhl_*`` releases on ``sportsdataverse-data``.

Python port of ``pwhl_data_creation.R``'s ``.upload_to_release`` — a thin
``gh release upload --clobber`` wrapper. All three released formats
(parquet + rds + csv) ship to the tag; the release is the distribution channel,
so rds/csv are not committed to this repo.

Release tags must pre-exist (R behaviour: warn + skip rather than auto-create),
so a typo in a tag never silently creates a stray release.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from pwhl_data_build.config import OUTPUTS

log = logging.getLogger(__name__)

_REPO = "sportsdataverse/sportsdataverse-data"
_RELEASE_EXTS: tuple[str, ...] = ("parquet", "rds", "csv")


def _run(args: list[str], timeout: int = 600) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["gh", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def release_exists(tag: str, repo: str = _REPO) -> bool:
    return _run(["release", "view", tag, "--repo", repo]).returncode == 0


def publish_file(path: Path, tag: str, *, repo: str = _REPO) -> None:
    """Upload one artifact to a release tag (clobbering any prior asset)."""
    res = _run(["release", "upload", tag, str(path), "--repo", repo, "--clobber"])
    if res.returncode != 0:
        raise RuntimeError(
            f"gh release upload failed for {path.name} -> {tag}: {res.stderr.strip()}"
        )


def publish_season(
    out_dir: str | Path, season_year: int, *, repo: str = _REPO, dry_run: bool = False
) -> list[tuple[str, str]]:
    """Upload every present ``{prefix}_{season}.{parquet,rds,csv}`` to its tag."""
    out = Path(out_dir)
    done: list[tuple[str, str]] = []
    checked: dict[str, bool] = {}
    for key, (prefix, tag) in OUTPUTS.items():
        for ext in _RELEASE_EXTS:
            path = out / key / ext / f"{prefix}_{season_year}.{ext}"
            if not path.exists():
                continue
            if dry_run:
                print(f"[dry-run] {path.name} -> {repo}@{tag}")
                done.append((tag, path.name))
                continue
            # existence-checked once per tag per run (R caches this too)
            if tag not in checked:
                checked[tag] = release_exists(tag, repo)
            if not checked[tag]:
                log.warning(
                    "release tag %s missing on %s — skipping %s", tag, repo, path.name
                )
                continue
            publish_file(path, tag, repo=repo)
            done.append((tag, path.name))
    return done
