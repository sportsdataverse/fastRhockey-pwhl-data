# CLAUDE.md — fastRhockey-pwhl-data Development Guide

## Repo Overview

`fastRhockey-pwhl-data` is the R-side parser/compiler stage of the PWHL
pipeline. It reads cached HockeyTech raw JSON from
`sportsdataverse/fastRhockey-pwhl-raw`, compiles per-season datasets
(play-by-play, skater/goalie/team boxscores, schedules, rosters, scoring
and penalty summaries, three stars, officials, shots-by-period, shootout
summaries, master indexes), persists them under `pwhl/` as RDS (xz) +
Parquet (gzip), commits them back to this repo, and uploads the same
artifacts to the corresponding `pwhl_*` GitHub releases on
`sportsdataverse/sportsdataverse-data`.

The `fastRhockey::load_pwhl_*()` family in the `fastRhockey` R package
reads from those releases — this repo is what populates them.

The package name declared in `DESCRIPTION` is **`fastRhockey.pwhl.data`**
(version 1.0.0, MIT-licensed), but in practice this repo is run as a
script project: `R/pwhl_data_creation.R` is the entry point, driven by
`scripts/daily_pwhl_R_processor.sh`, and there are no exported package
functions to test.

## Pipeline Position

```
HockeyTech API --[python+R scrape]--> fastRhockey-pwhl-raw
                                            | push trigger (daily_pwhl_data)
                                            v
                                       fastRhockey-pwhl-data [HERE]
                                            | piggyback release upload
                                            v
                                       sportsdataverse-data (pwhl_* release tags)
                                            | load_pwhl_*()
                                            v
                                       fastRhockey R package
```

The push trigger lives in `fastRhockey-pwhl-raw/.github/workflows/` and
fires `repository_dispatch` event-type `daily_pwhl_data` against this
repo. `.github/workflows/daily_pwhl.yml` is the receiver — it accepts the
dispatch, parses `start_year`/`end_year` from the upstream commit
message, then runs `scripts/daily_pwhl_R_processor.sh`.

## Season Convention

`-s` / `-e` refer to the **end year** of the season, matching
`fastRhockey::most_recent_pwhl_season()`:

- `-s 2026` -> the 2025-26 season
- `-s 2024 -e 2026` -> 2023-24 through 2025-26

All compiled artifacts are named with the end year:
`play_by_play_2026.rds`, `pwhl_schedule_2026.parquet`, etc.

## Build & Development Commands

```sh
# Full daily flow for one or more seasons (CI entry point)
bash scripts/daily_pwhl_R_processor.sh -s 2026 -e 2026

# Or call the R script directly when iterating
Rscript R/pwhl_data_creation.R -s 2026           # single season 2025-26
Rscript R/pwhl_data_creation.R -s 2024 -e 2026   # range

# One-time release bootstrap (creates the pwhl_* release tags
# on sportsdataverse-data with an init asset)
Rscript R/0000_create_fastRhockey_releases_init.R

# One-time push of existing pwhl/ artifacts to all release tags
Rscript R/0001_push_existing_release_data.R
```

The shell wrapper handles the git-pull / git-add / git-commit /
git-push loop around `Rscript R/pwhl_data_creation.R`. It commits with
the message `"PWHL Data Updated (Start: $i End: $i)"` per season — this
format is intentional and parsed downstream; do not change it without
coordinating with `fastRhockey` loaders or any consuming triggers.

## Repo Layout

```
R/
  pwhl_data_creation.R               # Main compiler: raw JSON -> per-season datasets -> upload
  0000_create_fastRhockey_releases_init.R  # One-time: create pwhl_* release tags
  0001_push_existing_release_data.R        # One-time: backfill existing local artifacts -> releases
scripts/
  daily_pwhl_R_processor.sh          # CI entry point (git-loop wrapper around R script)
pwhl/                                # Committed compiled output (consumed by fastRhockey loaders)
  pbp/{rds,parquet}/                 # play_by_play_{season}.{rds,parquet}
  skater_box/{rds,parquet}/
  goalie_box/{rds,parquet}/          # (compiled from raw goalies field)
  player_box/{rds,parquet}/          # skater_box + goalie_box bound with player_type tag
  team_box/{rds,parquet}/
  game_summary/{rds,parquet}/
  rosters/{rds,parquet}/
  schedules/{rds,parquet}/
  pwhl_schedule_master.{rds,parquet} # Combined schedule across all seasons
  pwhl_games_in_data_repo.{rds,parquet}  # Index of games with PBP compiled
logs/                                # Per-season run logs (fastRhockey_pwhl_data_logfile_{year}.log)
.github/workflows/
  daily_pwhl.yml                     # Cron + repository_dispatch + workflow_dispatch entry
DESCRIPTION                          # Pkg metadata (fastRhockey.pwhl.data, v1.0.0, MIT)
README.md                            # Public-facing structure + data-loading instructions
```

## Compile-Spec Table

`R/pwhl_data_creation.R` drives every dataset off a single `DATASETS`
`tibble::tribble` near the top of the file. Each row defines one
season-level artifact compiled from per-game final JSON, mapping
`json_field` -> `file_prefix` -> `release_tag`. Adding a new dataset
needs:

1. One row in the `DATASETS` tribble (key, json_field, file_prefix,
   release_tag, description).
2. A matching subdirectory under `pwhl/` (auto-created by
   `.save_dataset()`).
3. A `pwhl_<release_tag>` release tag on
   `sportsdataverse/sportsdataverse-data` (run the init script or
   `gh release create pwhl_<tag> -R sportsdataverse/sportsdataverse-data --notes 'init'`).
4. A matching `load_pwhl_<key>()` wrapper on the `fastRhockey` side
   (one catalog row in the fastRhockey package's `R/pwhl_loaders.R`).

Current compiled datasets (`DATASETS` keys): `pbp`, `skater_box`,
`goalie_box`, `team_box`, `game_info`, `game_rosters`,
`scoring_summary`, `penalty_summary`, `three_stars`, `officials`,
`shots_by_period`, `shootout`. `player_box` is derived in `.extract_all()`
by binding `skater_box` + `goalie_box` with a `player_type` tag.

## Upload Layer

`.upload_to_release()` calls
`sportsdataversedata::sportsdataverse_save()` (a thin wrapper around
`piggyback::pb_upload`) with `purrr::insistently()` for retries. Each
release tag is existence-checked once per run via `gh release view` and
cached in `.release_cache`, so we don't shell out per file.

If a release tag does not exist on `sportsdataverse-data`, the upload is
skipped with a `cli_alert_warning` — fix by creating the tag, do not
silently rename the row in `DATASETS`.

Tokens: prefers `GITHUB_PAT` env var; falls back to `gh auth token`
when the env var is unset. CI sets `GITHUB_PAT=${{ secrets.SDV_GH_TOKEN }}`
in the workflow.

## Daily Workflow

`.github/workflows/daily_pwhl.yml` is the in-repo cron entry point.

- **Cadence**: `0 9 UTC` on the months PWHL runs (Nov-Dec, Jan-Mar
  regular season; Apr-May playoffs). The 4-hour offset behind the
  raw-side scrape (which runs earlier) gives the upstream commit + push
  + `daily_pwhl_data` dispatch time to land.
- **Triggers**: cron, `repository_dispatch` (event-type
  `daily_pwhl_data`), and `workflow_dispatch` (with optional
  `start_year`/`end_year` inputs).
- **`daily_pwhl_data` payload parsing**: when dispatched from the raw
  repo, `start_year`/`end_year` are extracted from the upstream commit
  message via `grep -o -E '[0-9]+' | head -1` / `tail -1`. This relies
  on the raw repo's commit-message format containing the years —
  changing that format on the raw side requires updating this parser.
- **Defaults**: if no inputs are supplied (manual dispatch with empty
  inputs, or cron), `START_YEAR`/`END_YEAR` both default to
  `fastRhockey::most_recent_pwhl_season()`.
- **Execution**: `bash scripts/daily_pwhl_R_processor.sh -s $START_YEAR -e $END_YEAR`.
- **Dependency install**: `r-lib/actions/setup-r-dependencies@v2`
  pulls `sportsdataverse/fastRhockey`,
  `sportsdataverse/sportsdataverse-data`, and `ropensci/piggyback` from
  GitHub (declared as `Remotes:` in `DESCRIPTION`).

## Cross-Repo References

- Upstream raw cache: <https://github.com/sportsdataverse/fastRhockey-pwhl-raw>
- Downstream R package (consumer of releases): <https://github.com/sportsdataverse/fastRhockey>
- Release host: <https://github.com/sportsdataverse/sportsdataverse-data>
- Sister NHL pipeline: <https://github.com/sportsdataverse/fastRhockey-nhl-data>
- SDV shared conventions: <https://github.com/sportsdataverse/fastRhockey/blob/main/CLAUDE.md>

## Project-Specific Gotchas

- **Commit message format is load-bearing**. The shell wrapper writes
  `"PWHL Data Updated (Start: $i End: $i)"` per season. The raw repo's
  dispatch parser (in `daily_pwhl.yml`) extracts years from the
  upstream commit; flipping the format breaks the season-range
  inference.
- **End-year convention everywhere.** `-s 2026` means 2025-26, not 2026
  start. Datasets, filenames, release assets, and the
  `most_recent_pwhl_season()` helper all use the end year. Don't pass
  start years.
- **Raw JSON shape drift** is handled at the HockeyTech parsing
  boundary inside the `fastRhockey` package (`R/pwhl_*.R`). If a field
  vanishes from a compiled dataset, fix the parser upstream — this
  repo's `.extract_all()` simply pulls already-shaped fields out of
  per-game JSON.
- **Release tags must pre-exist.** `.upload_to_release()` short-circuits
  with a warning rather than auto-creating tags. Use
  `R/0000_create_fastRhockey_releases_init.R` (or `gh release create`)
  once per new tag.
- **`pwhl/` is committed.** Keep it intentional — every push retriggers
  consumers. Don't reorganize the `pwhl/` tree without aligning the
  `fastRhockey::load_pwhl_*()` wrappers and the `DATASETS` `file_prefix`
  values that drive the release filenames.
- **Parquet + RDS are both written.** Loaders pick whichever the user
  asks for; never drop one without coordinating with `fastRhockey`.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(compile): add shootout dataset to DATASETS tribble
fix(upload): guard release_exists cache against gh rate-limit
chore(ci): bump r-lib/actions/setup-r-dependencies to v2.x
ci(daily): tighten the in-season cron windows
```

The **only exception** is the automated daily push, which keeps the
hard-coded subject `"PWHL Data Updated (Start: <year> End: <year>)"` —
this string is parsed downstream and must not be changed.

For manual commits, prefer scoped subjects
(`feat(compile): ...`, `ci(daily): ...`). Use `type!:` or a
`BREAKING CHANGE:` footer for breaking changes. Split unrelated work
into separate commits for reviewability.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
