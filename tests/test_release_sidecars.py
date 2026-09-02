"""The release sidecars R's sportsdataverse_save() attaches to every tag.

publish.py is a port of that upload and dropped this half, which left the pwhl_*
tags carrying a timestamp.json frozen at the last R run while the data kept
moving -- a consumer reading it to decide whether to re-download got a confident
wrong answer.
"""

import json
from pathlib import Path

from pwhl_data_build import publish
from pwhl_data_build.config import OUTPUTS

SIDECAR_NAMES = [
    "timestamp.txt",
    "timestamp.json",
    "package_function.txt",
    "package_function.json",
]


#: release metadata sidecars -- asserted separately, not a data asset
SIDECARS = ("timestamp.", "package_function.")

def _stage(tmp_path, key, prefix, exts=("parquet", "rds")):
    for ext in exts:
        path = tmp_path / key / ext / f"{prefix}_2025.{ext}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")


def test_every_published_tag_names_a_loader():
    missing = sorted({tag for _p, tag in OUTPUTS.values()} - set(publish.PKG_FUNCTION))
    assert missing == [], f"tags with no PKG_FUNCTION entry: {missing}"
    assert publish.PKG_FUNCTION["pwhl_pbp"] == "fastRhockey::load_pwhl_pbp()"


def test_publish_season_stamps_each_tag_last(tmp_path, monkeypatch):
    key, (prefix, tag) = next(iter(OUTPUTS.items()))
    _stage(tmp_path, key, prefix)

    calls: list[list[str]] = []

    class _Ok:
        returncode = 0
        stderr = ""

    def _fake_run(args, timeout=600):
        calls.append(args)
        return _Ok()

    monkeypatch.setattr(publish, "_run", _fake_run)
    publish.publish_season(tmp_path, 2025)

    names = [Path(c[3]).name for c in calls if c[:2] == ["release", "upload"]]
    assert names == [f"{prefix}_2025.parquet", f"{prefix}_2025.rds", *SIDECAR_NAMES]
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"]
        and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert all(c[2] == tag and c[-1] == "--clobber" for c in uploads)


def test_no_files_means_no_stamp(tmp_path, monkeypatch):
    """A season with nothing on disk must not move any timestamp."""
    calls: list[list[str]] = []

    class _Ok:
        returncode = 0
        stderr = ""

    monkeypatch.setattr(publish, "_run", lambda args, timeout=600: (calls.append(args), _Ok())[1])
    publish.publish_season(tmp_path, 2025)

    assert not [
        c
        for c in calls
        if c[:2] == ["release", "upload"]
        and not Path(c[3]).name.startswith(SIDECARS)
    ]


def test_stamped_sidecars_carry_the_loader_and_a_timestamp(tmp_path, monkeypatch):
    key, (prefix, tag) = next(iter(OUTPUTS.items()))
    _stage(tmp_path, key, prefix, exts=("parquet",))
    seen: dict[str, str] = {}

    class _Ok:
        returncode = 0
        stderr = ""

    def _fake_run(args, timeout=600):
        if args[:2] == ["release", "upload"]:
            path = Path(args[3])
            # read inside the runner: the temp dir is cleaned up behind the upload
            if path.name.startswith(("timestamp.", "package_function.")):
                seen[path.name] = path.read_text()
        return _Ok()

    monkeypatch.setattr(publish, "_run", _fake_run)
    publish.publish_season(tmp_path, 2025)

    assert seen["package_function.txt"].strip() == publish.PKG_FUNCTION[tag]
    assert json.loads(seen["package_function.json"])["package_function"] == publish.PKG_FUNCTION[tag]
    assert json.loads(seen["timestamp.json"])["last_updated"].strip()
