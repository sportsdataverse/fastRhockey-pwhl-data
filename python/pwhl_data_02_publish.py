"""Stage 02 — publish compiled season assets to the pwhl_* release tags.

Numbered CLI over ``pwhl_data_build.publish.publish_season``, previously
reachable only as inline ``python -c`` snippets in the workflow and the
droplet processor — un-enumerable and un-runnable by hand. Uploads are
idempotent (``--clobber`` under the hood), so a partial season still ships
the datasets that built. Single home: models/manifest.yaml.

Usage::

    python -m pwhl_data_02_publish -s 2026 [--out-dir pwhl] [--dry-run]
    scripts/pwhl_data.sh 02
"""

from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    from pwhl_data_build.publish import publish_season

    ap = argparse.ArgumentParser(prog="python -m pwhl_data_02_publish")
    ap.add_argument("-s", "--start", type=int, required=True, help="start season end-year")
    ap.add_argument("-e", "--end", type=int, help="end season end-year (default: --start)")
    ap.add_argument("--out-dir", default="pwhl", help="compiled tree (pwhl_data_01_season --out)")
    ap.add_argument("--repo", default="sportsdataverse/sportsdataverse-data")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    end = a.end if a.end is not None else a.start
    for season in range(a.start, end + 1):
        done = publish_season(a.out_dir, season, repo=a.repo, dry_run=a.dry_run)
        print(f"{season}: {len(done)} assets uploaded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
