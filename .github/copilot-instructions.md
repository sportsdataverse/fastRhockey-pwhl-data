# fastRhockey-pwhl-data Copilot Instructions

## Project Context

This repo is the R-side compiler stage of the PWHL pipeline. It reads
cached HockeyTech raw JSON from `sportsdataverse/fastRhockey-pwhl-raw`,
compiles per-season datasets (play-by-play, skater/goalie/team
boxscores, schedules, rosters, scoring/penalty summaries, three stars,
officials, shots-by-period, shootout) under `pwhl/` as RDS (xz) +
Parquet (gzip), commits them, and uploads matching artifacts to the
`pwhl_*` release tags on `sportsdataverse/sportsdataverse-data`.

The `fastRhockey::load_pwhl_*()` family reads from those releases —
this repo is what populates them. The `DESCRIPTION` declares the
package `fastRhockey.pwhl.data`, but the repo runs as a script
project, not a CRAN-style package.

Pipeline: `HockeyTech -> fastRhockey-pwhl-raw -> fastRhockey-pwhl-data [HERE] -> sportsdataverse-data -> fastRhockey R package`.

## Repository Workflow

- Branch from `main`; `main` is the default branch.
- CI entry point: `scripts/daily_pwhl_R_processor.sh -s <START> -e <END>`.
- Underlying R script: `Rscript R/pwhl_data_creation.R -s <START> -e <END>`.
- `-s` / `-e` are **end years** (e.g., `2026` = 2025-26), matching `fastRhockey::most_recent_pwhl_season()`.
- HockeyTech JSON-parsing bugs belong in the `fastRhockey` package (`R/pwhl_*.R`), not here. This repo only reshapes fields that are already extracted upstream.
- Don't reorganize the `pwhl/` output tree without aligning the catalog row in `fastRhockey/R/pwhl_loaders.R` and the `file_prefix` values in the `DATASETS` tribble of `R/pwhl_data_creation.R`.

## Build & Development Commands

```sh
# Full daily flow for one or more seasons (CI entry point)
bash scripts/daily_pwhl_R_processor.sh -s 2026 -e 2026

# Or run the R script directly when iterating
Rscript R/pwhl_data_creation.R -s 2026           # single season 2025-26
Rscript R/pwhl_data_creation.R -s 2024 -e 2026   # range 2023-24 through 2025-26

# One-time release init (creates pwhl_* release tags on sportsdataverse-data)
Rscript R/0000_create_fastRhockey_releases_init.R

# One-time backfill of existing local pwhl/ artifacts to releases
Rscript R/0001_push_existing_release_data.R
```

Output paths the compiler writes under:

- `pwhl/pbp/{rds,parquet}/play_by_play_{end_year}.{ext}`
- `pwhl/skater_box/{rds,parquet}/skater_box_{end_year}.{ext}`
- `pwhl/goalie_box/{rds,parquet}/goalie_box_{end_year}.{ext}`
- `pwhl/player_box/{rds,parquet}/player_box_{end_year}.{ext}` (skater + goalie bind with `player_type` tag)
- `pwhl/team_box/{rds,parquet}/team_box_{end_year}.{ext}`
- `pwhl/game_summary/{rds,parquet}/game_info_{end_year}.{ext}`
- `pwhl/rosters/{rds,parquet}/`
- `pwhl/schedules/{rds,parquet}/`
- `pwhl/pwhl_schedule_master.{rds,parquet}` — combined schedule
- `pwhl/pwhl_games_in_data_repo.{rds,parquet}` — index of games with PBP compiled

## Code Style

- Follow the parent SDK conventions: `snake_case`, 2-space indent, tidyverse style.
- Use `cli::cli_alert_info()` / `cli::cli_alert_warning()` / `cli::cli_alert_danger()` for messaging.
- Use `glue::glue()` for string interpolation and filename construction.
- Use `purrr::insistently()` + `rate_backoff()` for any upload retry.
- Drive dataset additions through the `DATASETS` `tibble::tribble` at the top of `R/pwhl_data_creation.R` — one row, one matching `pwhl/<dir>` subdirectory, one matching release tag.
- Initialize return values before `tryCatch` blocks (`out <- list()` etc.) so error handlers don't leave them unbound.
- Use `dplyr::any_of()` when dropping or renaming columns that upstream may shed without warning.

## Daily Workflow

`.github/workflows/daily_pwhl.yml` is the in-repo cron entry point.

- Cron `0 9 UTC` on PWHL months: Nov-Dec, Jan-Mar (regular season), Apr-May (playoffs). The offset behind the raw-side scrape gives the upstream `daily_pwhl_data` `repository_dispatch` time to land.
- Triggers: cron, `repository_dispatch` (event-type `daily_pwhl_data`), and `workflow_dispatch` with optional `start_year`/`end_year`.
- When dispatched from the raw repo, the workflow parses `start_year`/`end_year` from the upstream commit message with `grep -o -E '[0-9]+' | head -1` / `tail -1`. The raw-side commit message format (years embedded) is load-bearing.
- If `START_YEAR`/`END_YEAR` are empty, both default to `fastRhockey::most_recent_pwhl_season()`.
- Execution: `bash scripts/daily_pwhl_R_processor.sh -s $START_YEAR -e $END_YEAR`, which loops per-season, calls `Rscript R/pwhl_data_creation.R`, then `git add` + commit (`"PWHL Data Updated (Start: <year> End: <year>)"`) + push.
- Token: workflow exports `GITHUB_PAT=${{ secrets.SDV_GH_TOKEN }}` for the `piggyback` upload step.

## Cross-Repo References

- Upstream raw cache: <https://github.com/sportsdataverse/fastRhockey-pwhl-raw>
- Downstream R package: <https://github.com/sportsdataverse/fastRhockey>
- Release host: <https://github.com/sportsdataverse/sportsdataverse-data>
- Shared conventions: <https://github.com/sportsdataverse/fastRhockey/blob/main/CLAUDE.md>

## Conventional Commits

Use: `type(scope): description`. Common types: `feat`, `fix`, `chore`, `ci`, `docs`, `refactor`. Common scopes: `compile`, `upload`, `loader`, `ci`, `daily`. Use `type!:` or a `BREAKING CHANGE:` footer for breaking changes.

The **only exception** is the automated daily push, which uses the
hard-coded subject `"PWHL Data Updated (Start: <year> End: <year>)"`.
That format is parsed downstream and must not be changed.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
