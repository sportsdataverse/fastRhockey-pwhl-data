from __future__ import annotations

import argparse

from .artifacts import upload_artifacts
from .builders import build_xg, write_xg_card


def _seasons(spec: str) -> list[int]:
    """Parse ``2024:2026`` or ``2025`` into a season list."""
    if ":" in spec:
        start, end = spec.split(":", 1)
        return list(range(int(start), int(end) + 1))
    return [int(spec)]


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="pwhl_model_publish")
    sub = ap.add_subparsers(dest="cmd", required=True)

    x = sub.add_parser("xg", help="build + publish xG-enriched shots")
    x.add_argument(
        "--seasons",
        required=True,
        help="a season (2025) or an inclusive range (2024:2026)",
    )
    x.add_argument("--out", default="out/pwhl_xg_pbp")
    x.add_argument("--tag", default="pwhl_xg_pbp")
    x.add_argument("--repo", default="sportsdataverse/sportsdataverse-data")
    x.add_argument(
        "--pbp-dir",
        default=None,
        help="directory of play_by_play_{season}.parquet (default: this repo's committed tree)",
    )
    x.add_argument("--dry-run", action="store_true")
    x.add_argument(
        "--build-only",
        action="store_true",
        help="write parquet + card, skip the upload",
    )
    return ap


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "xg":
        results = build_xg(_seasons(args.seasons), args.out, pbp_dir=args.pbp_dir)
        write_xg_card(results, args.out)
        total = sum(r["rows"] for r in results)
        if args.build_only:
            print(
                f"xg: built seasons={len(results)} rows={total} -> {args.out} (build-only)"
            )
            return 0
        res = upload_artifacts(
            args.out,
            args.tag,
            args.repo,
            pattern="pwhl_xg_pbp_*.*",
            dry_run=args.dry_run,
        )
        created = " (created release)" if res.get("created_release") else ""
        print(
            f"publish: seasons={len(results)} rows={total} uploaded={res['uploaded']} "
            f"-> {args.repo}:{res['tag']}"
            + created
            + (" (dry-run)" if args.dry_run else "")
        )
    return 0
