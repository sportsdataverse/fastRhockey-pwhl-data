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

from sportsdataverse.release import upload_release_sidecars

from pwhl_data_build.config import OUTPUTS

log = logging.getLogger(__name__)

_REPO = "sportsdataverse/sportsdataverse-data"
_RELEASE_EXTS: tuple[str, ...] = ("parquet", "rds", "csv")

#: Release sidecar metadata. Every published tag carries package_function.txt/.json
#: naming the loader a consumer reads it through -- the half of R's
#: sportsdataverse_save() this port dropped. fastRhockey names every one of these
#: loaders after its tag, and the derivation was checked against the
#: package_function.json already published to all 14 tags, so re-stamping from
#: Python does not change what a consumer sees.
PKG_FUNCTION: dict[str, str] = {tag: f"fastRhockey::load_{tag}()" for _prefix, tag in OUTPUTS.values()}


def _run(args: list[str], timeout: int = 600) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["gh", *args], capture_output=True, text=True, timeout=timeout, check=False
    )


def release_exists(tag: str, repo: str = _REPO) -> bool:
    return _run(["release", "view", tag, "--repo", repo]).returncode == 0


def publish_file(path: Path, tag: str, *, repo: str = _REPO) -> None:
    """Upload one artifact to a release tag (clobbering any prior asset)."""
    _upload(["release", "upload", tag, str(path), "--repo", repo, "--clobber"], path.name, tag)


def _upload(args: list[str], what: str, tag: str) -> None:
    """One ``gh release upload`` -- the chokepoint the sidecar stamp shares."""
    res = _run(args)
    if res.returncode != 0:
        raise RuntimeError(f"gh release upload failed for {what} -> {tag}: {res.stderr.strip()}")


def publish_season(
    out_dir: str | Path, season_year: int, *, repo: str = _REPO, dry_run: bool = False
) -> list[tuple[str, str]]:
    """Upload every present ``{prefix}_{season}.{parquet,rds,csv}`` to its tag."""
    out = Path(out_dir)
    done: list[tuple[str, str]] = []
    checked: dict[str, bool] = {}
    for key, (prefix, tag) in OUTPUTS.items():
        uploaded = 0
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
            uploaded += 1
            done.append((tag, path.name))
        # stamp LAST so the timestamp describes a finished upload, and only when
        # something actually uploaded -- a stamp on a no-op run would claim data
        # moved when it did not
        if uploaded:
            upload_release_sidecars(
                tag,
                runner=lambda args, _tag=tag: _upload(args, Path(args[3]).name, _tag),
                pkg_function=PKG_FUNCTION.get(tag),
                repo=repo,
            )
    return done
