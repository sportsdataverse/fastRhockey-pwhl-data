# PWHL xG-enriched shots — model documentation

`pwhl_xg_pbp_{season}.parquet` per season on the `pwhl_xg_pbp` tag, plus a
`pwhl_xg_pbp_card.json` provenance sidecar per publish (the per-run metadata
authority).

## Model

The xG proxy model itself lives in sdv-py
(`sportsdataverse.pwhl.pwhl_xg_proxy` / `pwhl_shot_xg()`) — its fit and gates
are registered where it is trained. This repo owns the SCORING + publish
surface: the committed play-by-play (with `strength_state` derived via the
gameshifts backfill, T5/R4) is scored per season and published.

## Operability

Stage `python/pwhl_model_01_xg_pbp.py` (wraps `pwhl_model_publish xg`); daily
in-season cron Nov-May (`pwhl_xg_cron.yml`) + dispatch. The publisher REFUSES
a season with no committed pbp — it fail-fasts rather than scoring an empty
season. Single home: `models/manifest.yaml`.

## Data & methodology

Shots come from this repo's committed play-by-play, whose `strength_state`
exists because of the gameshifts backfill (T5/R4) — strength context is a
first-class xG input, and the backfill is what made it available league-wide.
The proxy model (fit, features, and gates registered in sdv-py) maps shot
location/type/strength to a goal probability; this repo scores every season's
committed shots with `pwhl_shot_xg()` and publishes the enriched frames.

## Limitations

The league is young (three seasons, ~18k shots), so calibration is evaluated
in-sample on the published scores rather than on a held-out era; the proxy
carries no pre-shot passing data. EV/PP/SH-split models are the catalogued
next step (`dev/t5_xg_reevaluation/`).

## Evaluation on the published releases (2026-09-01)

18,031 scored shots across seasons 2024-2026: log-loss **0.2639**, Monte-Carlo rank AUC **0.707**, goal rate 0.0824. In-sample calibration of the published scores; the proxy's own fit and gates live in sdv-py.

Card: [`pwhl_xg_eval_card.json`](pwhl_xg_eval_card.json)

## Figures

![xG calibration](figures/pwhl_xg_calibration.png)

## Avenues for improvement & open issues

- **EV/PP/SH-split models** — catalogued in `dev/t5_xg_reevaluation/`; the
  strength_state column exists league-wide precisely to enable this.
- **Calibration by season** — three seasons now exist; a per-season curve
  would show whether the proxy drifts as the league's shot mix matures.
- **Known issue:** evaluation is in-sample on published scores — a
  season-held-out refit is the honest next gate once a fourth season lands.
