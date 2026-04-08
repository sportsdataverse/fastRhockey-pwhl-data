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
## Produces:   PBP, player_box, rosters, game_summary, schedules, master files
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

# ── Logging ──────────────────────────────────────────────────────────────
LOG_FILE <- "logs/fastRhockey_pwhl_data_logfile.log"
if (!dir.exists("logs")) dir.create("logs", recursive = TRUE)
logging <- function(msg, level = "INFO") {
  entry <- paste0(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), level, ": ", msg)
  cat(entry, "\n", file = LOG_FILE, append = TRUE)
}
logging("=== PWHL Data Creation started ===")

option_list <- list(
  optparse::make_option(
    c("-s", "--start_year"),
    action = "store",
    default = fastRhockey::most_recent_pwhl_season(),
    type = "integer",
    help = "Start year of the seasons to process [default: current season]"
  ),
  optparse::make_option(
    c("-e", "--end_year"),
    action = "store",
    default = NA_integer_,
    type = "integer",
    help = "End year of the seasons to process [default: same as start_year]"
  )
)

opt <- optparse::parse_args(optparse::OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)

if (is.na(opt$end_year)) opt$end_year <- opt$start_year
years_vec <- opt$start_year:opt$end_year
logging(glue("Processing seasons: {paste(years_vec, collapse=', ')}"))

RAW_BASE <- "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-pwhl-raw/main"


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
      logging(glue("Failed to read RDS from {url}: {conditionMessage(e)}"), "ERROR")
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

.save_dataset <- function(df, dir_base, name, season) {
  rds_dir <- file.path(dir_base, "rds")
  parquet_dir <- file.path(dir_base, "parquet")
  for (d in c(rds_dir, parquet_dir)) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }
  saveRDS(df, file.path(rds_dir, glue("{name}_{season}.rds")), compress = "xz")
  arrow::write_parquet(df, file.path(parquet_dir, glue("{name}_{season}.parquet")),
    compression = "gzip"
  )
}

.upload_to_release <- function(df, file_name, release_tag, description) {
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
      logging(glue("Failed to upload {file_name} to {release_tag}: {conditionMessage(e)}"), "WARN")
    }
  )
}


# ═══════════════════════════════════════════════════════════════════════
# Main loop: per season
# ═══════════════════════════════════════════════════════════════════════

all_games <- purrr::map(years_vec, function(season_year) {
  cli::cli_h1("Processing {season_year} PWHL season")
  logging(glue("=== {season_year} PWHL season ==="))


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
    logging(glue("Could not fetch schedule for {season_year}"), "ERROR")
    return(NULL)
  }

  for (d in c("pwhl/schedules/rds", "pwhl/schedules/parquet")) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }
  saveRDS(sched, glue("pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))
  arrow::write_parquet(sched, glue("pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"),
    compression = "gzip"
  )

  season_json_games <- sched %>% dplyr::filter(.data$game_json == TRUE)
  season_game_list <- season_json_games$game_id
  season_game_urls <- season_json_games$game_json_url

  logging(glue("{length(season_game_list)} games with final JSON in raw repo"))
  cli::cli_alert_info("{length(season_game_list)} games with final JSON in raw repo")

  if (length(season_game_list) == 0) {
    cli::cli_alert_warning("No games with JSON. Skipping.")
    logging("No games with JSON, skipping season", "WARN")
    return(NULL)
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 2: Compile play-by-play
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Compiling {season_year} PBP ({length(season_game_list)} games)",
    msg_done = "Compiled {season_year} PBP!"
  )

  future::plan(future::multisession, workers = 4)

  season_pbp <- furrr::future_map_dfr(
    season_game_urls,
    function(url) {
      tryCatch(
        {
          game_json <- .json_from_url(url)
          if (is.null(game_json)) {
            return(NULL)
          }
          pbp <- game_json$pbp
          if (is.data.frame(pbp) && nrow(pbp) > 0) {
            return(pbp)
          }
          NULL
        },
        error = function(e) NULL
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  season_pbp <- dplyr::distinct(season_pbp)
  logging(glue("{nrow(season_pbp)} PBP events compiled"))
  cli::cli_alert_info("{nrow(season_pbp)} PBP events")

  if (nrow(season_pbp) > 0) {
    pbp_name <- glue("play_by_play_{season_year}")

    for (sub in c("pwhl/pbp/rds", "pwhl/pbp/parquet")) {
      if (!dir.exists(sub)) dir.create(sub, recursive = TRUE)
    }
    season_pbp |> saveRDS(glue("pwhl/pbp/rds/{pbp_name}.rds"), compress = "xz")
    season_pbp |> arrow::write_parquet(glue("pwhl/pbp/parquet/{pbp_name}.parquet"), compression = "gzip")

    logging(glue("Uploading {pbp_name} to sportsdataverse-data releases"))
    .upload_to_release(season_pbp, pbp_name, "pwhl_pbp", "PWHL play-by-play data")
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 3: Compile player boxscores (skaters + goalies)
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Compiling {season_year} player boxscores",
    msg_done = "Compiled {season_year} player boxscores!"
  )

  season_skaters <- furrr::future_map_dfr(
    season_game_urls,
    function(url) {
      tryCatch(
        {
          game_json <- .json_from_url(url)
          if (is.null(game_json)) return(NULL)
          sk <- game_json$skaters
          if (is.data.frame(sk) && nrow(sk) > 0) return(sk)
          NULL
        },
        error = function(e) NULL
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  season_goalies <- furrr::future_map_dfr(
    season_game_urls,
    function(url) {
      tryCatch(
        {
          game_json <- .json_from_url(url)
          if (is.null(game_json)) return(NULL)
          gl <- game_json$goalies
          if (is.data.frame(gl) && nrow(gl) > 0) return(gl)
          NULL
        },
        error = function(e) NULL
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  season_player_box <- dplyr::bind_rows(
    if (nrow(season_skaters) > 0) dplyr::mutate(season_skaters, player_type = "skater") else NULL,
    if (nrow(season_goalies) > 0) dplyr::mutate(season_goalies, player_type = "goalie") else NULL
  )

  if (nrow(season_player_box) > 0) {
    .save_dataset(season_player_box, "pwhl/player_box", "player_box", season_year)
    logging(glue("{nrow(season_skaters)} skater + {nrow(season_goalies)} goalie rows"))
    cli::cli_alert_info("{nrow(season_skaters)} skater + {nrow(season_goalies)} goalie rows")
    .upload_to_release(
      season_player_box, glue("player_box_{season_year}"),
      "pwhl_player_boxscores", "PWHL player boxscores"
    )
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 4: Compile rosters
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Compiling {season_year} rosters",
    msg_done = "Compiled {season_year} rosters!"
  )

  # Fetch rosters from fastRhockey directly (not from raw JSON)
  teams <- tryCatch(
    fastRhockey::pwhl_teams(),
    error = function(e) data.frame()
  )

  season_rosters <- data.frame()
  if (nrow(teams) > 0) {
    for (tm in teams$team_label) {
      roster <- tryCatch(
        fastRhockey::pwhl_team_roster(team = tm, season = season_year),
        error = function(e) data.frame()
      )
      if (nrow(roster) > 0) {
        season_rosters <- dplyr::bind_rows(season_rosters, roster)
      }
    }
  }

  if (nrow(season_rosters) > 0) {
    season_rosters <- season_rosters %>%
      dplyr::distinct() %>%
      dplyr::mutate(season = season_year)
    .save_dataset(season_rosters, "pwhl/rosters", "rosters", season_year)
    logging(glue("{nrow(season_rosters)} unique roster entries"))
    cli::cli_alert_info("{nrow(season_rosters)} unique roster entries")
    .upload_to_release(season_rosters, glue("rosters_{season_year}"),
                       "pwhl_rosters", "PWHL rosters")
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 5: Compile game summaries
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Compiling {season_year} game summaries",
    msg_done = "Compiled {season_year} game summaries!"
  )

  season_game_summaries <- furrr::future_map_dfr(
    season_game_urls,
    function(url) {
      tryCatch(
        {
          game_json <- .json_from_url(url)
          if (is.null(game_json)) return(NULL)
          gs <- game_json$game_summary
          if (is.list(gs) && !is.null(gs$details)) {
            return(gs$details)
          }
          NULL
        },
        error = function(e) NULL
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  if (nrow(season_game_summaries) > 0) {
    .save_dataset(season_game_summaries, "pwhl/game_summary", "game_summary", season_year)
    logging(glue("{nrow(season_game_summaries)} game summary rows"))
    cli::cli_alert_info("{nrow(season_game_summaries)} game summary rows")
  }


  # ──────────────────────────────────────────────────────────────────────
  # STEP 6: Update schedule with data availability flags
  # ──────────────────────────────────────────────────────────────────────

  cli::cli_progress_step(
    msg = "Updating {season_year} schedule flags",
    msg_done = "Updated {season_year} schedule flags"
  )

  pbp_ids <- if (nrow(season_pbp) > 0) unique(season_pbp$game_id) else integer(0)
  player_ids <- if (nrow(season_player_box) > 0) unique(season_player_box$game_id) else integer(0)

  final_sched <- sched %>%
    dplyr::mutate(
      PBP        = as.integer(.data$game_id) %in% as.integer(pbp_ids),
      player_box = as.integer(.data$game_id) %in% as.integer(player_ids)
    ) %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::desc(.data$game_date))

  saveRDS(final_sched, glue("pwhl/schedules/rds/pwhl_schedule_{season_year}.rds"))
  arrow::write_parquet(final_sched,
    glue("pwhl/schedules/parquet/pwhl_schedule_{season_year}.parquet"),
    compression = "gzip"
  )

  # Upload the single-season schedule (with data availability flags) to release
  .upload_to_release(
    final_sched, glue("pwhl_schedule_{season_year}"),
    "pwhl_schedules", "PWHL schedule"
  )

  cli::cli_alert_success("Done with {season_year}")
  logging(glue("Completed {season_year}: {nrow(season_pbp)} PBP, {nrow(season_player_box)} player_box"))

  rm(
    season_pbp, season_skaters, season_goalies,
    season_player_box, season_rosters,
    season_game_summaries, final_sched, sched
  )
  gc()

  return(NULL)
}) # end purrr::map


# ═══════════════════════════════════════════════════════════════════════
# Build cross-season master files
# ═══════════════════════════════════════════════════════════════════════

cli::cli_progress_step(
  msg = "Building master schedule + pwhl_games_in_data_repo",
  msg_done = "Master files built!"
)

sched_files <- list.files("pwhl/schedules/rds", pattern = "\\.rds$", full.names = TRUE)
sched_all <- purrr::map_dfr(sched_files, readRDS) %>%
  dplyr::arrange(dplyr::desc(.data$game_date))

saveRDS(sched_all, "pwhl/pwhl_schedule_master.rds", compress = "xz")
arrow::write_parquet(sched_all, "pwhl/pwhl_schedule_master.parquet", compression = "gzip")

games_in_repo <- sched_all %>%
  dplyr::filter(.data$PBP == TRUE) %>%
  dplyr::arrange(dplyr::desc(.data$game_date))

if (!dir.exists("pwhl")) dir.create("pwhl")
saveRDS(games_in_repo, "pwhl/pwhl_games_in_data_repo.rds", compress = "xz")
arrow::write_parquet(games_in_repo, "pwhl/pwhl_games_in_data_repo.parquet", compression = "gzip")

# Upload schedules and games index to release
.upload_to_release(sched_all, "pwhl_schedule_master", "pwhl_schedules", "PWHL schedules")
.upload_to_release(
  games_in_repo, "pwhl_games_in_data_repo",
  "pwhl_schedules", "PWHL games available in fastRhockey data repo"
)

logging(glue("Master: {nrow(sched_all)} schedule rows, {nrow(games_in_repo)} with PBP"))
cli::cli_alert_success("{nrow(sched_all)} total schedule rows, {nrow(games_in_repo)} with PBP")

logging("=== PWHL Data Creation complete ===")
cli::cli_h1("All done!")
