# Model registry

One row per model dataset this repo publishes (Track C step 1). Compute-on-
demand: the xG proxy model lives in sdv-py (`sportsdataverse.pwhl.pwhl_xg_proxy`
/ `pwhl_shot_xg()`); this repo scores the committed play-by-play with it and
publishes the result, with a `pwhl_xg_pbp_card.json` provenance sidecar as the
per-run metadata authority. `tests/test_model_registry.py` keeps this table in
lockstep.

| model | artifact(s) | release tag | training data | fitting script | gates at publish | last publish | cadence |
|---|---|---|---|---|---|---|---|
| PWHL xG-enriched shots | `pwhl_xg_pbp_{season}.parquet` per season + `pwhl_xg_pbp_card.json` | `pwhl_xg_pbp` | this repo's committed pbp (strength_state via the gameshifts backfill — T5/R4) scored by sdv-py `pwhl_shot_xg()` | `pwhl_model_publish/builders.py::build_xg` | model gates live with the proxy in sdv-py; publisher refuses a season with no committed pbp (fail-fast, never scores an empty season); card records per-run provenance | see `pwhl_xg_pbp` release | manual/dispatch — **no cron; scheduled refresh NOT wired, Track C follow-up** |

Note: the xG proxy's fit and validation are registered where the model lives
(sdv-py). This registry covers what this repo owns — the scoring + publish
surface. Next-step candidates recorded in `dev/t5_xg_reevaluation/`:
EV/PP/SH-split xG.
