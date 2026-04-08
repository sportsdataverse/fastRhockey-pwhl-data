#--- PWHL Data Release Initialization -----
# Run once to create the release tags on sportsdataverse-data

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "pwhl_schedules",
  name = "pwhl_schedules",
  body = "PWHL Schedules Data (from HockeyTech API)",
  .token = Sys.getenv("GITHUB_PAT", unset = system("gh auth token", intern = TRUE))
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "pwhl_player_boxscores",
  name = "pwhl_player_boxscores",
  body = "PWHL Player Boxscores Data (from HockeyTech API)",
  .token = Sys.getenv("GITHUB_PAT", unset = system("gh auth token", intern = TRUE))
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "pwhl_pbp",
  name = "pwhl_pbp",
  body = "PWHL Play-by-Play Data (from HockeyTech API)",
  .token = Sys.getenv("GITHUB_PAT", unset = system("gh auth token", intern = TRUE))
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "pwhl_rosters",
  name = "pwhl_rosters",
  body = "PWHL Rosters Data (from HockeyTech API)",
  .token = Sys.getenv("GITHUB_PAT", unset = system("gh auth token", intern = TRUE))
)
