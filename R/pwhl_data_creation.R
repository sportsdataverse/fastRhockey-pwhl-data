## Compile PWHL season datasets from fastRhockey-pwhl-raw repo
##
## NOTE ON SEASON CONVENTION:
##   -s / -e refer to the *end year* of the season (e.g., 2026 = 2025-26).
##   This matches `fastRhockey::most_recent_pwhl_season()`. All compiled
##   datasets are named using the end year: play_by_play_{end_year}.rds,
##   pwhl_schedule_{end_year}.rds, etc.
##
## Usage:
##   Rscript R/pwhl_data_creation.R -s 2026           (single season: 2025-26)
##   Rscript R/pwhl_data_creation.R -s 2024 -e 2026   (range: 2023-24 through 2025-26)
##
## Reads from: sportsdataverse/fastRhockey-pwhl-raw (schedules + final game JSON)
## Produces:   PBP, skater_box, goalie_box, player_box, team_box, game_info,
##             game_rosters, scoring_summary, penalty_summary, three_stars,
##             officials, shots_by_period, rosters, schedules, master files
## Uploads to: sportsdataverse/sportsdataverse-data (GitHub releases)

suppressPackageStartupMessages(library(fastRhockey))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(glue))
suppressPackageStartupMessages(library(purrr))
suppressPackageStartupMessages(library(furrr))
suppressPackageStartupMessages(library(future))
suppressPackageStartupMessages(library(jsonlite))
suppressPackageStartupMessages(library(arrow))
suppressPackageStartupMessages(library(optparse))
suppressPackageStartupMessages(library(cli))

cli::cli_alert_info("=== PWHL Data Creation started ===")

option_list <- list(
  optparse::make_option(
    c("-s", "--start_year"),
    action = "store",
    default = fastRhockey::most_recent_pwhl_season(),
    type = "integer",
    help = "Start season's end year to process, e.g. 2026 for 2025-26 [default: most recent]"
  ),
  optparse::make_option(
    c("-e", "--end_year"),
    action = "store",
    default = NA_integer_,
    type = "integer",
    help = "End season's end year to process [default: same as start_year]"
  )
)

opt <- optparse::parse_args(optparse::OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)

if (is.na(opt$end_year)) opt$end_year <- opt$start_year
years_vec <- opt$start_year:opt$end_year
cli::cli_alert_info("Processing seasons: {paste(years_vec, collapse=', ')}")

RAW_BASE <- "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-pwhl-raw/main"


# ═══════════════════════════════════════════════════════════════════════
# Compile-spec table
#
# Each row defines one season-level dataset compiled from the per-game
# final JSON. Order matches the export sequence; new datasets only need
# a row added here plus a sub-dir under `pwhl/`.
# ═══════════════════════════════════════════════════════════════════════

DATASETS <- tibble::tribble(
  ~key,              ~json_field,         ~file_prefix,       ~release_tag,             ~description,
  "pbp",             "pbp",               "play_by_play",     "pwhl_pbp",               "PWHL play-by-play data",
  "skater_box",      "skaters",           "skater_box",       "pwhl_skater_boxscores",  "PWHL skater boxscores",
  "goalie_box",      "goalies",           "goalie_box",       "pwhl_goalie_boxscores",  "PWHL goalie boxscores",
  "team_box",        "team_box",          "team_box",         "pwhl_team_boxscores",    "PWHL team boxscores",
  "game_info",       "game_info",         "game_info",        "pwhl_game_info",         "PWHL game info",
  "game_rosters",    "game_rosters",      "game_rosters",     "pwhl_game_rosters",      "PWHL per-game rosters",
  "scoring_summary", "scoring_summary",   "scoring_summary",  "pwhl_scoring_summary",   "PWHL scoring summary",
  "penalty_summary", "penalty_summary",   "penalty_summary",  "pwhl_penalty_summary",   "PWHL penalty summary",
  "three_stars",     "three_stars",       "three_stars",      "pwhl_three_stars",       "PWHL three stars",
  "officials",       "officials",         "officials",        "pwhl_officials",         "PWHL on-ice officials",
  "shots_by_period", "shots_by_period",   "shots_by_period",  "pwhl_shots_by_period",   "PWHL shots by period",
  "shootout",        "shootout_summary",  "shootout_summary", "pwhl_shootout",          "PWHL shootout summary"
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

.rds_from_url <- function(url) {
  tryCatch(
    {
      con <- url(url)
      on.exit(close(con))
      readRDS(con)
    },
    error = function(e) {
      cli::cli_alert_danger("Failed to read RDS from {url}: {conditionMessage(e)}")
      NULL
    }
  )
}

.json_from_url <- function(url) {
  tryCatch(
    {
      jsonlite::fromJSON(url, simplifyVector = TRUE, flatten = TRUE)
    },
    error = function(e) NULL
  )
}

# Pull all configured datasets out of one parsed game JSON in a single pass.
# Returns a named list mapping dataset key -> data.frame (possibly NULL).
.extract_all <- function(game_json) {
  out <- setNames(
    vector("list", nrow(DATASETS) + 1),
    c(DATASETS$key, "player_box")
  )
  if (is.null(game_json)) return(out)

  for (i in seq_len(nrow(DATASETS))) {
    key   <- DATASETS$key[i]
    field <- DATASETS$json_field[i]
    val   <- game_json[[field]]

    df <- tryCatch(
      {
        if (is.data.frame(val) && nrow(val) > 0) {
          val
        } else if (is.list(val) && length(val) > 0 && !is.data.frame(val)) {
          dplyr::bind_rows(val)
        } else {
          NULL
        }
      },
      error = function(e) NULL
    )
    if (!is.null(df) && nrow(df) > 0) out[[key]] <- df
  }

  # Player box = bind of skater_box + goalie_box, with a player_type tag
  parts <- list()
  if (!is.null(out[["skater_box"]])) {
    parts[[1]] <- dplyr::mutate(out[["skater_box"]], player_type = "skater")
  }
  if (!is.null(out[["goalie_box"]])) {
    parts[[2]] <- dplyr::mutate(out[["goalie_box"]], player_type = "goalie")
  }
  parts <- purrr::compact(parts)
  if (length(parts) > 0) {
    out[["player_box"]] <- tryCatch(dplyr::bind_rows(parts),
      error = function(e) parts[[1]])
  }

  out
}

.save_dataset <- function(df, dir_base, name, season) {
  rds_dir <- file.path(dir_base, "rds")
  parquet_dir <- file.path(dir_base, "parquet")
  for (d in c(rds_dir, parquet_dir)) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }
  saveRDS(df, file.path(rds_dir, glue("{name}_{season}.rds")), compress = "xz")
  # arrow::write_parquet can't write nested data.frame columns ("structs").
  # PWHL JSON is parsed with flatten = TRUE so this is rarely needed, but
  # defensive flattening here matches the NHL data-creation behavior and
  # protects against any HockeyTech field that comes through nested.
  arrow::write_parquet(.flatten_struct_cols(df),
    file.path(parquet_dir, glue("{name}_{season}.parquet")),
    compression = "gzip"
  )
}

# Recursively unfold structures that arrow cannot write to parquet:
#   1. Nested data.frame columns (jsonlite produces them when a JSON
#      field is itself an object); jsonlite::flatten() unfolds one
#      level per call so we loop up to 5 times.
#   2. List columns where each row holds a per-row named list (e.g.
#      HockeyTech name objects with locale fallbacks). jsonlite::flatten
#      does NOT touch these because the column type is `list`, not
#      `data.frame`; extract a usable scalar with vapply -- prefer a
#      `$default` slot, then any length-1 atomic, else a JSON string.
.flatten_struct_cols <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0) return(df)

  for (iter in seq_len(5L)) {
    if (!any(vapply(df, is.data.frame, logical(1)))) break
    df <- jsonlite::flatten(df)
  }

  for (col in names(df)) {
    x <- df[[col]]
    if (is.list(x) && !is.data.frame(x)) {
      df[[col]] <- vapply(x, function(elem) {
        if (is.null(elem) || length(elem) == 0) {
          NA_character_
        } else if (is.list(elem) && !is.null(elem$default)) {
          as.character(elem$default)[[1]]
        } else if (is.atomic(elem) && length(elem) == 1L) {
          as.character(elem)
        } else {
          tryCatch(
            jsonlite::toJSON(elem, auto_unbox = TRUE, na = "null"),
            error = function(e) NA_character_
          )
        }
      }, character(1))
    }
  }

  df
}

# Cache release-existence checks so we hit `gh` once per tag, not once per file.
.release_cache <- new.env(parent = emptyenv())
.release_exists <- function(release_tag,
                            repo = "sportsdataverse/sportsdataverse-data") {
  key <- paste0(repo, "@", release_tag)
  if (exists(key, envir = .release_cache, inherits = FALSE)) {
    return(get(key, envir = .release_cache, inherits = FALSE))
  }
  ok <- tryCatch(
    {
      pat <- Sys.getenv("GITHUB_PAT", unset = "")
      gh_env <- if (nzchar(pat)) paste0("GH_TOKEN=", pat) else character(0)
      res <- suppressWarnings(system2(
        "gh",
        c("release", "view", release_tag, "-R", repo, "--json", "tagName"),
        stdout = TRUE, stderr = TRUE, env = gh_env
      ))
      st <- attr(res, "status")
      (is.null(st) || st == 0) &&
        length(res) > 0 &&
        !any(grepl("release not found", res, ignore.case = TRUE))
    },
    error = function(e) FALSE
  )
  assign(key, ok, envir = .release_cache)
  ok
}

.upload_to_release <- function(df, file_name, release_tag, description) {
  if (!.release_exists(release_tag)) {
    cli::cli_alert_warning(
      "Release tag {.val {release_tag}} does not exist on sportsdataverse-data; skipping upload of {.val {file_name}}. Create the release once with `gh release create {release_tag} -R sportsdataverse/sportsdataverse-data --notes 'init'` and re-run."
    )
    return(invisible(NULL))
  }
  retry_rate <- purrr::rate_backoff(pause_base = 1, pause_min = 60, max_times = 10)
  tryCatch(
    purrr::insistently(
      sportsdataversedata::sportsdataverse_save,
      rate = retry_rate, quiet = FALSE
    )(
      data_frame = df,
      file_name = file_name,
      sportsdataverse_type = description,
      release_tag = release_tag,
      pkg_function = glue("fastRhockey::load_pwhl_{gsub('pwhl_', '', release_tag)}()"),
      file_types = c("rds", "csv", "parquet"),
      .token = Sys.getenv("GITHUB_PAT",
                          unset = system("gh auth token", intern = TRUE))
    ),
    error = function(e) {
      cli::cli_alert_warning("Failed to upload {file_name} to {release_tag}: {conditionMessage(e)}")
    }
  )
}


# ═══════════════════════════════════════════════════════════════════════
# Main loop: per season
# ═══════════════════════════════════════════════════════════════════════

invisible(purrr::map(years_vec, function(season_year) {
  cli::cli_h1("Processing {season_year} PWHL season")


  # ──────────────────────────────────────────────────────────────────────
  # STEP 1: Fetch schedule from pwhl-raw repo
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Downloading {season_year} schedule from pwhl-raw",
    msg_done = "Downloaded {season_year} schedule"
  )

  sched <- .rds_from_url(glue("{RAW_BASE}/pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))

  if (is.null(sched)) {
    cli::cli_alert_danger("Could not fetch schedule for {season_year}. Skipping.")
    return(NULL)
  }

  for (d in c("pwhl/schedules/rds", "pwhl/schedules/parquet")) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }
  saveRDS(sched, glue("pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))
  arrow::write_parquet(sched, glue("pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"),
    compression = "gzip"
  )

  season_json_games <- sched |> dplyr::filter(.data$game_json == TRUE)
  season_game_list  <- season_json_games$game_id
  season_game_urls  <- season_json_games$game_json_url

  cli::cli_alert_info("{length(season_game_list)} games with final JSON in raw repo")

  if (length(season_game_list) == 0) {
    cli::cli_alert_warning("No games with JSON. Skipping.")
    return(NULL)
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 2: Single-pass extraction of every per-game dataset
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Reading {length(season_game_urls)} game JSONs and extracting datasets",
    msg_done = "Extracted per-game datasets"
  )

  future::plan(future::multisession, workers = 6)

  json_from_url <- .json_from_url
  extract_all   <- .extract_all
  datasets_spec <- DATASETS

  per_game <- furrr::future_map(
    season_game_urls,
    function(url) {
      tryCatch(extract_all(json_from_url(url)),
        error = function(e) NULL)
    },
    .options = furrr::furrr_options(
      seed = TRUE,
      globals = list(
        json_from_url = json_from_url,
        extract_all   = extract_all,
        DATASETS      = datasets_spec
      ),
      packages = c("jsonlite", "dplyr", "purrr", "tibble")
    )
  )

  # Pivot list-of-named-lists into named-list-of-frames, one per dataset key.
  all_keys <- c(DATASETS$key, "player_box")

  compiled <- purrr::map(all_keys, function(key) {
    parts <- purrr::map(per_game, ~ .x[[key]])
    parts <- purrr::compact(parts)
    if (length(parts) == 0) return(NULL)
    tryCatch(
      dplyr::bind_rows(parts) |> dplyr::distinct(),
      error = function(e) {
        cli::cli_alert_warning("bind_rows failed for {key}: {conditionMessage(e)}")
        parts[[1]]
      }
    )
  })
  names(compiled) <- all_keys


  # ──────────────────────────────────────────────────────────────────────
  # STEP 3: Save + upload each dataset from the spec table
  # ──────────────────────────────────────────────────────────────────────

  for (i in seq_len(nrow(DATASETS))) {
    key  <- DATASETS$key[i]
    pref <- DATASETS$file_prefix[i]
    rtag <- DATASETS$release_tag[i]
    desc <- DATASETS$description[i]
    df   <- compiled[[key]]

    if (is.null(df) || nrow(df) == 0) {
      cli::cli_alert_info("{key}: 0 rows -> skipped")
      next
    }

    cli::cli_alert_info("{key}: {nrow(df)} rows")
    # Per-dataset tryCatch: a single dataset's save/upload failure
    # (e.g. arrow refusing a struct column, transient release upload
    # 5xx) should not abort the rest of the season's compile. Flatten
    # once up front so .save_dataset, .upload_to_release, and any
    # tibble construction inside sportsdataverse_save all see scalar
    # dot-prefixed columns rather than nested data.frame / list cols.
    tryCatch(
      {
        df_flat <- .flatten_struct_cols(df)
        .save_dataset(df_flat, file.path("pwhl", key), pref, season_year)
        .upload_to_release(df_flat, glue("{pref}_{season_year}"), rtag, desc)
      },
      error = function(e) {
        cli::cli_alert_warning(
          "{key}: save/upload failed -- {conditionMessage(e)}"
        )
      }
    )
  }

  # Player box (combined skater + goalie)
  player_box <- compiled[["player_box"]]
  if (!is.null(player_box) && nrow(player_box) > 0) {
    .save_dataset(player_box, "pwhl/player_box", "player_box", season_year)
    .upload_to_release(player_box, glue("player_box_{season_year}"),
      "pwhl_player_boxscores", "PWHL player boxscores")
    cli::cli_alert_info("player_box: {nrow(player_box)} rows")
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 4: Compile season rosters (unique players from game_rosters)
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Compiling {season_year} season rosters",
    msg_done = "Compiled {season_year} season rosters"
  )

  season_rosters <- compiled[["game_rosters"]]
  if (!is.null(season_rosters) && nrow(season_rosters) > 0) {
    rosters_unique <- season_rosters |>
      dplyr::select(-dplyr::any_of(c("game_id", "starting", "status"))) |>
      dplyr::distinct()
    rosters_unique$season <- season_year
    .save_dataset(rosters_unique, "pwhl/rosters", "rosters", season_year)
    .upload_to_release(rosters_unique, glue("rosters_{season_year}"),
      "pwhl_rosters", "PWHL rosters")
    cli::cli_alert_info("rosters: {nrow(rosters_unique)} unique entries")
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 5: Update schedule with data availability flags
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Updating {season_year} schedule flags",
    msg_done = "Updated {season_year} schedule flags"
  )

  ids_with <- function(key) {
    df <- compiled[[key]]
    if (is.null(df) || !"game_id" %in% names(df)) integer(0)
    else as.integer(unique(df$game_id))
  }

  final_sched <- sched |>
    dplyr::mutate(
      PBP             = as.integer(.data$game_id) %in% ids_with("pbp"),
      player_box      = as.integer(.data$game_id) %in% ids_with("player_box"),
      skater_box      = as.integer(.data$game_id) %in% ids_with("skater_box"),
      goalie_box      = as.integer(.data$game_id) %in% ids_with("goalie_box"),
      team_box        = as.integer(.data$game_id) %in% ids_with("team_box"),
      game_info       = as.integer(.data$game_id) %in% ids_with("game_info"),
      game_rosters    = as.integer(.data$game_id) %in% ids_with("game_rosters"),
      scoring_summary = as.integer(.data$game_id) %in% ids_with("scoring_summary"),
      penalty_summary = as.integer(.data$game_id) %in% ids_with("penalty_summary"),
      three_stars     = as.integer(.data$game_id) %in% ids_with("three_stars"),
      officials       = as.integer(.data$game_id) %in% ids_with("officials"),
      shots_by_period = as.integer(.data$game_id) %in% ids_with("shots_by_period"),
      shootout        = as.integer(.data$game_id) %in% ids_with("shootout")
    ) |>
    dplyr::distinct() |>
    dplyr::arrange(dplyr::desc(.data$game_date))

  saveRDS(final_sched, glue("pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))
  arrow::write_parquet(final_sched,
    glue("pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"),
    compression = "gzip"
  )

  .upload_to_release(
    final_sched, glue("pwhl_schedule_{season_year}"),
    "pwhl_schedules", "PWHL schedule"
  )

  cli::cli_alert_success("Done with {season_year}")

  rm(compiled, per_game, final_sched, sched)
  gc()
  NULL
}))


# ═══════════════════════════════════════════════════════════════════════
# Build cross-season master files
# ═══════════════════════════════════════════════════════════════════════

cli::cli_progress_step(
  msg = "Building master schedule + pwhl_games_in_data_repo",
  msg_done = "Master files built!"
)

sched_files <- list.files("pwhl/schedules/rds", pattern = "\\.rds$", full.names = TRUE)
sched_all <- purrr::map(sched_files, readRDS) |>
  dplyr::bind_rows() |>
  dplyr::arrange(dplyr::desc(.data$game_date))

saveRDS(sched_all, "pwhl/pwhl_schedule_master.rds", compress = "xz")
arrow::write_parquet(sched_all, "pwhl/pwhl_schedule_master.parquet", compression = "gzip")

games_in_repo <- sched_all |>
  dplyr::filter(.data$PBP == TRUE) |>
  dplyr::arrange(dplyr::desc(.data$game_date))

if (!dir.exists("pwhl")) dir.create("pwhl")
saveRDS(games_in_repo, "pwhl/pwhl_games_in_data_repo.rds", compress = "xz")
arrow::write_parquet(games_in_repo, "pwhl/pwhl_games_in_data_repo.parquet", compression = "gzip")

.upload_to_release(sched_all, "pwhl_schedule_master", "pwhl_schedules", "PWHL schedules")
.upload_to_release(
  games_in_repo, "pwhl_games_in_data_repo",
  "pwhl_schedules", "PWHL games available in fastRhockey data repo"
)

cli::cli_alert_success("{nrow(sched_all)} total schedule rows, {nrow(games_in_repo)} with PBP")

cli::cli_alert_info("=== PWHL Data Creation complete ===")
cli::cli_h1("All done!")
