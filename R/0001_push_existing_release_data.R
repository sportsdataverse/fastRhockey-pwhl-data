suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(glue))
suppressPackageStartupMessages(library(purrr))
suppressPackageStartupMessages(library(arrow))
suppressPackageStartupMessages(library(stringr))


# --- Push existing schedule data ---
sched_list <- list.files(path = "pwhl/schedules/rds/")
sched_g <- purrr::map(sched_list, function(x) {
  sched <- readRDS(paste0("pwhl/schedules/rds/", x))

  sched <- sched %>%
    fastRhockey:::make_fastRhockey_data("PWHL Schedule from fastRhockey data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = sched,
    file_name = glue::glue("pwhl_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "pwhl_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(sched_g)

# --- Push existing PBP data ---
pbp_list <- list.files(path = "pwhl/pbp/rds/")
pbp_g <- purrr::map(pbp_list, function(x) {
  pbp <- readRDS(paste0("pwhl/pbp/rds/", x))

  pbp <- pbp %>%
    fastRhockey:::make_fastRhockey_data("PWHL Play-by-Play from fastRhockey data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = pbp,
    file_name = glue::glue("pwhl_play_by_play_{y}"),
    sportsdataverse_type = "Play-by-Play data",
    release_tag = "pwhl_pbp",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(pbp_g)

# --- Push existing player box data ---
player_box_list <- list.files(path = "pwhl/player_box/rds/")
player_box_g <- purrr::map(player_box_list, function(x) {
  player_box <- readRDS(paste0("pwhl/player_box/rds/", x))
  player_box <- player_box %>%
    fastRhockey:::make_fastRhockey_data("PWHL Player Boxscores from fastRhockey data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = player_box,
    file_name = glue::glue("pwhl_player_box_{y}"),
    sportsdataverse_type = "Player Boxscores data",
    release_tag = "pwhl_player_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(player_box_g)

# --- Push existing roster data ---
roster_list <- list.files(path = "pwhl/rosters/rds/")
roster_g <- purrr::map(roster_list, function(x) {
  rosters <- readRDS(paste0("pwhl/rosters/rds/", x))
  rosters <- rosters %>%
    fastRhockey:::make_fastRhockey_data("PWHL Rosters from fastRhockey data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = rosters,
    file_name = glue::glue("pwhl_rosters_{y}"),
    sportsdataverse_type = "Rosters data",
    release_tag = "pwhl_rosters",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(roster_g)
