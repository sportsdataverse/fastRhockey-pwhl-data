# PWHL Expected Goals — coordinate xG


The PWHL coordinate expected-goals model estimates the probability that
an on-net shot becomes a goal, from pre-shot information only: shot
geometry (distance and angle to the net, derived from the HockeyTech
rink coordinates), shot type, rebound state, and the pre-shot movement
context (what the previous event was, how long ago, and how far away).
It is a **logistic regression fit at call time** in sdv-py
(`sportsdataverse.pwhl.pwhl_xg_proxy.fit_pwhl_coord_xg`) — there is no
bundled artifact; the model is a deterministic function of the
play-by-play it is handed, which is what makes this document fully
reproducible from the data committed in this repository. A per-strength
(EV/PP/SH) Platt recalibration is applied on top of the base logit when
the frame carries strength columns. An alternative “shot-quality tier”
proxy exists for API stability but is not the production scorer; its two
goal-named tiers tautologically encode the outcome, a limitation the
module documents openly.

This document is the model’s reproducible writeup: every number, figure
and table below is computed at render time from the committed
play-by-play parquet in `pwhl/pbp/parquet/`.

## Training data

<div id="rxfvkmlulh" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#rxfvkmlulh table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#rxfvkmlulh thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#rxfvkmlulh p { margin: 0; padding: 0; }
 #rxfvkmlulh .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #rxfvkmlulh .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #rxfvkmlulh .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #rxfvkmlulh .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #rxfvkmlulh .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #rxfvkmlulh .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rxfvkmlulh .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #rxfvkmlulh .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #rxfvkmlulh .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #rxfvkmlulh .gt_column_spanner_outer:first-child { padding-left: 0; }
 #rxfvkmlulh .gt_column_spanner_outer:last-child { padding-right: 0; }
 #rxfvkmlulh .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #rxfvkmlulh .gt_spanner_row { border-bottom-style: hidden; }
 #rxfvkmlulh .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #rxfvkmlulh .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #rxfvkmlulh .gt_from_md> :first-child { margin-top: 0; }
 #rxfvkmlulh .gt_from_md> :last-child { margin-bottom: 0; }
 #rxfvkmlulh .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #rxfvkmlulh .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #rxfvkmlulh .gt_indent_1 { text-indent: 5px; }
 #rxfvkmlulh .gt_indent_2 { text-indent: calc(5px * 2); }
 #rxfvkmlulh .gt_indent_3 { text-indent: calc(5px * 3); }
 #rxfvkmlulh .gt_indent_4 { text-indent: calc(5px * 4); }
 #rxfvkmlulh .gt_indent_5 { text-indent: calc(5px * 5); }
 #rxfvkmlulh .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #rxfvkmlulh .gt_row_group_first td { border-top-width: 2px; }
 #rxfvkmlulh .gt_row_group_first th { border-top-width: 2px; }
 #rxfvkmlulh .gt_striped { color: #333333; background-color: #F4F4F4; }
 #rxfvkmlulh .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rxfvkmlulh .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #rxfvkmlulh .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #rxfvkmlulh .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #rxfvkmlulh .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #rxfvkmlulh .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #rxfvkmlulh .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #rxfvkmlulh .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rxfvkmlulh .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #rxfvkmlulh .gt_left { text-align: left; }
 #rxfvkmlulh .gt_center { text-align: center; }
 #rxfvkmlulh .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #rxfvkmlulh .gt_font_normal { font-weight: normal; }
 #rxfvkmlulh .gt_font_bold { font-weight: bold; }
 #rxfvkmlulh .gt_font_italic { font-style: italic; }
 #rxfvkmlulh .gt_super { font-size: 65%; }
 #rxfvkmlulh .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rxfvkmlulh .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #rxfvkmlulh .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #rxfvkmlulh .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #rxfvkmlulh .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #rxfvkmlulh .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| PWHL committed play-by-play, by season |  |  |  |  |  |  |
|----|----|----|----|----|----|----|
| on-net shots (event == 'shot'); computed at render time |  |  |  |  |  |  |
| season | games | pbp_events | shots | goals | goal_rate | coord_coverage |
| 2024 | 85 | 10,456 | 4,922 | 385 | 7.8% | 100.0% |
| 2025 | 102 | 12,520 | 5,671 | 499 | 8.8% | 100.0% |
| 2026 | 133 | 23,838 | 7,438 | 601 | 8.1% | 100.0% |

&#10;</div>

The corpus is every season of PWHL play-by-play this repository has
compiled (the league launched in 2024), with shot coordinates at
effectively full coverage — the reason a coordinate xG is viable here at
all. `strength_state` exists league-wide because of the game-shifts
backfill (T5/R4), and enters the model through the `is_pp` / `is_sh`
strength one-hots and the per-strength recalibrator rather than as a raw
feature.

One data defect is recomputed at every render so it cannot silently go
stale. From 2025-26 the feed carries `shot_distance` / `shot_angle`
columns; earlier seasons do not. `pwhl_xg_proxy._shot_geometry()` reuses
those columns whenever they are present, so in a **pooled** multi-season
frame every earlier-season shot keeps a null distance, which
`_build_xg_features` then fills with 0.0 — the production publisher
(`pwhl_model_publish.builders.build_xg`) fits once on exactly such a
pooled frame. This document drops the feed geometry and recomputes it
from the coordinates (lossless; the two agree to 0.0 ft on 2025-26), so
every number below is on real geometry. The count goes to zero when
sdv-py’s `_shot_geometry` recomputes null geometry:

<div id="pewpkdnkxb" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#pewpkdnkxb table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#pewpkdnkxb thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#pewpkdnkxb p { margin: 0; padding: 0; }
 #pewpkdnkxb .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #pewpkdnkxb .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #pewpkdnkxb .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #pewpkdnkxb .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #pewpkdnkxb .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #pewpkdnkxb .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pewpkdnkxb .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #pewpkdnkxb .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #pewpkdnkxb .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #pewpkdnkxb .gt_column_spanner_outer:first-child { padding-left: 0; }
 #pewpkdnkxb .gt_column_spanner_outer:last-child { padding-right: 0; }
 #pewpkdnkxb .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #pewpkdnkxb .gt_spanner_row { border-bottom-style: hidden; }
 #pewpkdnkxb .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #pewpkdnkxb .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #pewpkdnkxb .gt_from_md> :first-child { margin-top: 0; }
 #pewpkdnkxb .gt_from_md> :last-child { margin-bottom: 0; }
 #pewpkdnkxb .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #pewpkdnkxb .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #pewpkdnkxb .gt_indent_1 { text-indent: 5px; }
 #pewpkdnkxb .gt_indent_2 { text-indent: calc(5px * 2); }
 #pewpkdnkxb .gt_indent_3 { text-indent: calc(5px * 3); }
 #pewpkdnkxb .gt_indent_4 { text-indent: calc(5px * 4); }
 #pewpkdnkxb .gt_indent_5 { text-indent: calc(5px * 5); }
 #pewpkdnkxb .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #pewpkdnkxb .gt_row_group_first td { border-top-width: 2px; }
 #pewpkdnkxb .gt_row_group_first th { border-top-width: 2px; }
 #pewpkdnkxb .gt_striped { color: #333333; background-color: #F4F4F4; }
 #pewpkdnkxb .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pewpkdnkxb .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #pewpkdnkxb .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #pewpkdnkxb .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pewpkdnkxb .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #pewpkdnkxb .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #pewpkdnkxb .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #pewpkdnkxb .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pewpkdnkxb .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #pewpkdnkxb .gt_left { text-align: left; }
 #pewpkdnkxb .gt_center { text-align: center; }
 #pewpkdnkxb .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #pewpkdnkxb .gt_font_normal { font-weight: normal; }
 #pewpkdnkxb .gt_font_bold { font-weight: bold; }
 #pewpkdnkxb .gt_font_italic { font-style: italic; }
 #pewpkdnkxb .gt_super { font-size: 65%; }
 #pewpkdnkxb .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pewpkdnkxb .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #pewpkdnkxb .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pewpkdnkxb .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #pewpkdnkxb .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #pewpkdnkxb .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Pooled-frame geometry contamination — recomputed at render time |  |  |
|----|----|----|
| sdv-py pwhl_xg_proxy.\_shot_geometry reuses feed geometry columns even where they are null |  |  |
| pooled on-net shots with coordinates | null shot_distance after \_shot_geometry (would train at 0 ft) | share |
| 18,031 | 10,593 | 58.7% |

&#10;</div>

## Exploratory data analysis

<img src="pwhl_xg_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Goal rate falls steeply with shot distance; the model’s dominant feature." />

<img src="pwhl_xg_files/figure-commonmark/cell-6-output-1.png"
width="420" height="300"
alt="Shot locations (offensive half, attacking to the right); goals in orange." />

<div id="xvmwcjrxgq" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#xvmwcjrxgq table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#xvmwcjrxgq thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#xvmwcjrxgq p { margin: 0; padding: 0; }
 #xvmwcjrxgq .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #xvmwcjrxgq .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #xvmwcjrxgq .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #xvmwcjrxgq .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #xvmwcjrxgq .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xvmwcjrxgq .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xvmwcjrxgq .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #xvmwcjrxgq .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #xvmwcjrxgq .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #xvmwcjrxgq .gt_column_spanner_outer:first-child { padding-left: 0; }
 #xvmwcjrxgq .gt_column_spanner_outer:last-child { padding-right: 0; }
 #xvmwcjrxgq .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #xvmwcjrxgq .gt_spanner_row { border-bottom-style: hidden; }
 #xvmwcjrxgq .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #xvmwcjrxgq .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #xvmwcjrxgq .gt_from_md> :first-child { margin-top: 0; }
 #xvmwcjrxgq .gt_from_md> :last-child { margin-bottom: 0; }
 #xvmwcjrxgq .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #xvmwcjrxgq .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #xvmwcjrxgq .gt_indent_1 { text-indent: 5px; }
 #xvmwcjrxgq .gt_indent_2 { text-indent: calc(5px * 2); }
 #xvmwcjrxgq .gt_indent_3 { text-indent: calc(5px * 3); }
 #xvmwcjrxgq .gt_indent_4 { text-indent: calc(5px * 4); }
 #xvmwcjrxgq .gt_indent_5 { text-indent: calc(5px * 5); }
 #xvmwcjrxgq .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #xvmwcjrxgq .gt_row_group_first td { border-top-width: 2px; }
 #xvmwcjrxgq .gt_row_group_first th { border-top-width: 2px; }
 #xvmwcjrxgq .gt_striped { color: #333333; background-color: #F4F4F4; }
 #xvmwcjrxgq .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xvmwcjrxgq .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xvmwcjrxgq .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #xvmwcjrxgq .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #xvmwcjrxgq .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #xvmwcjrxgq .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #xvmwcjrxgq .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #xvmwcjrxgq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xvmwcjrxgq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xvmwcjrxgq .gt_left { text-align: left; }
 #xvmwcjrxgq .gt_center { text-align: center; }
 #xvmwcjrxgq .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #xvmwcjrxgq .gt_font_normal { font-weight: normal; }
 #xvmwcjrxgq .gt_font_bold { font-weight: bold; }
 #xvmwcjrxgq .gt_font_italic { font-style: italic; }
 #xvmwcjrxgq .gt_super { font-size: 65%; }
 #xvmwcjrxgq .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xvmwcjrxgq .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #xvmwcjrxgq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #xvmwcjrxgq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #xvmwcjrxgq .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #xvmwcjrxgq .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Shot mix and conversion by strength state |        |           |
|-------------------------------------------|--------|-----------|
| strength_state                            | shots  | goal_rate |
| <na>                                      | 10,640 | 8.5%      |
| 5v5                                       | 4,817  | 8.1%      |
| 5v4                                       | 759    | 4.7%      |
| 4v5                                       | 692    | 4.8%      |
| 5v3                                       | 133    | 8.3%      |
| 4v4                                       | 131    | 6.1%      |
| 5v6                                       | 117    | 7.7%      |
| 6v5                                       | 114    | 8.8%      |

&#10;</div>

The distance curve is the core of the model: conversion drops from over
20% at the crease to low single digits beyond ~45 ft, and it is smooth
enough that a logistic in distance and angle captures most of the
signal. The strength-state table shows why strength enters the model:
power-play shots convert at a visibly higher rate than even-strength
shots from a similar mix of locations.

## Model, features & importance

<div id="grxaghtdvd" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#grxaghtdvd table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#grxaghtdvd thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#grxaghtdvd p { margin: 0; padding: 0; }
 #grxaghtdvd .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #grxaghtdvd .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #grxaghtdvd .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #grxaghtdvd .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #grxaghtdvd .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #grxaghtdvd .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #grxaghtdvd .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #grxaghtdvd .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #grxaghtdvd .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #grxaghtdvd .gt_column_spanner_outer:first-child { padding-left: 0; }
 #grxaghtdvd .gt_column_spanner_outer:last-child { padding-right: 0; }
 #grxaghtdvd .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #grxaghtdvd .gt_spanner_row { border-bottom-style: hidden; }
 #grxaghtdvd .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #grxaghtdvd .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #grxaghtdvd .gt_from_md> :first-child { margin-top: 0; }
 #grxaghtdvd .gt_from_md> :last-child { margin-bottom: 0; }
 #grxaghtdvd .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #grxaghtdvd .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #grxaghtdvd .gt_indent_1 { text-indent: 5px; }
 #grxaghtdvd .gt_indent_2 { text-indent: calc(5px * 2); }
 #grxaghtdvd .gt_indent_3 { text-indent: calc(5px * 3); }
 #grxaghtdvd .gt_indent_4 { text-indent: calc(5px * 4); }
 #grxaghtdvd .gt_indent_5 { text-indent: calc(5px * 5); }
 #grxaghtdvd .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #grxaghtdvd .gt_row_group_first td { border-top-width: 2px; }
 #grxaghtdvd .gt_row_group_first th { border-top-width: 2px; }
 #grxaghtdvd .gt_striped { color: #333333; background-color: #F4F4F4; }
 #grxaghtdvd .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #grxaghtdvd .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #grxaghtdvd .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #grxaghtdvd .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #grxaghtdvd .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #grxaghtdvd .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #grxaghtdvd .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #grxaghtdvd .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #grxaghtdvd .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #grxaghtdvd .gt_left { text-align: left; }
 #grxaghtdvd .gt_center { text-align: center; }
 #grxaghtdvd .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #grxaghtdvd .gt_font_normal { font-weight: normal; }
 #grxaghtdvd .gt_font_bold { font-weight: bold; }
 #grxaghtdvd .gt_font_italic { font-style: italic; }
 #grxaghtdvd .gt_super { font-size: 65%; }
 #grxaghtdvd .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #grxaghtdvd .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #grxaghtdvd .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #grxaghtdvd .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #grxaghtdvd .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #grxaghtdvd .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Logistic coefficients and standardized importance |  |  |
|----|----|----|
| importance = \|coef\| × sd(feature), full-corpus fit |  |  |
| feature | coefficient | std_importance |
| shot_distance | −0.0360 | 0.6884 |
| shot_angle | −0.0124 | 0.2543 |
| rebound | 1.0138 | 0.2408 |
| last_shot | −0.3573 | 0.1704 |
| is_wrist | −0.2650 | 0.1316 |
| is_backhand | −0.5181 | 0.1250 |
| is_pp | −0.4348 | 0.1226 |
| last_faceoff | −0.2151 | 0.1075 |
| last_hit | −0.3939 | 0.1031 |
| empty_net_for | 0.7441 | 0.0651 |
| is_slap | 0.2658 | 0.0556 |
| distance_from_last | 0.0011 | 0.0492 |
| last_blocked | −0.1542 | 0.0376 |
| is_home | 0.0742 | 0.0371 |
| last_x | 0.0005 | 0.0312 |
| last_y | 0.0014 | 0.0305 |
| is_sh | 0.1386 | 0.0282 |
| time_since_last | 0.0005 | 0.0182 |
| last_penalty | 0.2157 | 0.0120 |
| is_tip | −0.0574 | 0.0112 |
| is_snap | 0.0011 | 0.0003 |

&#10;</div>

<img src="pwhl_xg_files/figure-commonmark/cell-9-output-1.png"
width="420" height="300"
alt="Standardized feature importance (|coef|·sd)." />

## SHAP-style attribution

For a linear-logit model the exact Shapley attribution of each feature
on the log-odds scale is `coef × (x − E[x])` — no approximation needed.
The distribution of per-shot attributions shows how each feature moves
individual shots, not just the average:

<img src="pwhl_xg_files/figure-commonmark/cell-10-output-1.png"
width="420" height="300"
alt="Per-shot log-odds attributions (linear SHAP), top 8 features by importance; 2,000-shot sample." />

Distance dominates — its attributions span roughly ±1 log-odds — with
rebound state the strongest binary: a rebound shot gains about one
log-odds (odds ×2.7) over a non-rebound shot from the same location.

## Evaluation

The honest test for a young league is a **season holdout**: fit on all
seasons before the newest, score the newest, and compare against a
constant-rate baseline.

<div id="igsnzvnufd" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#igsnzvnufd table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#igsnzvnufd thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#igsnzvnufd p { margin: 0; padding: 0; }
 #igsnzvnufd .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #igsnzvnufd .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #igsnzvnufd .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #igsnzvnufd .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #igsnzvnufd .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #igsnzvnufd .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #igsnzvnufd .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #igsnzvnufd .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #igsnzvnufd .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #igsnzvnufd .gt_column_spanner_outer:first-child { padding-left: 0; }
 #igsnzvnufd .gt_column_spanner_outer:last-child { padding-right: 0; }
 #igsnzvnufd .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #igsnzvnufd .gt_spanner_row { border-bottom-style: hidden; }
 #igsnzvnufd .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #igsnzvnufd .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #igsnzvnufd .gt_from_md> :first-child { margin-top: 0; }
 #igsnzvnufd .gt_from_md> :last-child { margin-bottom: 0; }
 #igsnzvnufd .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #igsnzvnufd .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #igsnzvnufd .gt_indent_1 { text-indent: 5px; }
 #igsnzvnufd .gt_indent_2 { text-indent: calc(5px * 2); }
 #igsnzvnufd .gt_indent_3 { text-indent: calc(5px * 3); }
 #igsnzvnufd .gt_indent_4 { text-indent: calc(5px * 4); }
 #igsnzvnufd .gt_indent_5 { text-indent: calc(5px * 5); }
 #igsnzvnufd .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #igsnzvnufd .gt_row_group_first td { border-top-width: 2px; }
 #igsnzvnufd .gt_row_group_first th { border-top-width: 2px; }
 #igsnzvnufd .gt_striped { color: #333333; background-color: #F4F4F4; }
 #igsnzvnufd .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #igsnzvnufd .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #igsnzvnufd .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #igsnzvnufd .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #igsnzvnufd .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #igsnzvnufd .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #igsnzvnufd .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #igsnzvnufd .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #igsnzvnufd .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #igsnzvnufd .gt_left { text-align: left; }
 #igsnzvnufd .gt_center { text-align: center; }
 #igsnzvnufd .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #igsnzvnufd .gt_font_normal { font-weight: normal; }
 #igsnzvnufd .gt_font_bold { font-weight: bold; }
 #igsnzvnufd .gt_font_italic { font-style: italic; }
 #igsnzvnufd .gt_super { font-size: 65%; }
 #igsnzvnufd .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #igsnzvnufd .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #igsnzvnufd .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #igsnzvnufd .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #igsnzvnufd .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #igsnzvnufd .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Season-holdout evaluation — fit 2024–2025, scored on 2026 |         |          |
|-----------------------------------------------------------|---------|----------|
| 7,438 held-out shots, goal rate 0.081                     |         |          |
| model                                                     | logloss | rank_AUC |
| coordinate xG                                             | 0.2627  | 0.7033   |
| constant baseline (train goal rate)                       | 0.2808  | 0.5000   |

&#10;</div>

<img src="pwhl_xg_files/figure-commonmark/cell-12-output-1.png"
width="420" height="300"
alt="Holdout calibration by xG decile — points on the diagonal are perfectly calibrated." />

The model beats the constant baseline on the held-out season and its
decile calibration tracks the diagonal. The published per-season assets
on the `pwhl_xg_pbp` release tag are scored by the same recipe with the
full corpus available at publish time; their in-sample evaluation lives
in [`pwhl_xg_eval_card.json`](pwhl_xg_eval_card.json).

## Per-season calibration

Three seasons now exist — enough to ask whether calibration holds season
by season as the league matures, not only on the newest one. Each season
is scored **leave-one-season-out**: the model is re-fit on the other
seasons and predicts the held-out one, so no season is scored by a model
that saw it. The per-season decile curves and the binomial calibration
z-score (`z = (goals − Σᵢ xGᵢ) / sqrt(Σᵢ xGᵢ(1 − xGᵢ))` — the variance
is the sum of the per-shot Bernoulli variances, not `ΣxG` times a single
`(1 − xG)`; a calibrated season has \|z\| ≲ 2 whatever its shot count)
are computed at render time and extend automatically as seasons land.

<div id="asnapcgkwk" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#asnapcgkwk table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#asnapcgkwk thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#asnapcgkwk p { margin: 0; padding: 0; }
 #asnapcgkwk .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #asnapcgkwk .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #asnapcgkwk .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #asnapcgkwk .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #asnapcgkwk .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #asnapcgkwk .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #asnapcgkwk .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #asnapcgkwk .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #asnapcgkwk .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #asnapcgkwk .gt_column_spanner_outer:first-child { padding-left: 0; }
 #asnapcgkwk .gt_column_spanner_outer:last-child { padding-right: 0; }
 #asnapcgkwk .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #asnapcgkwk .gt_spanner_row { border-bottom-style: hidden; }
 #asnapcgkwk .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #asnapcgkwk .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #asnapcgkwk .gt_from_md> :first-child { margin-top: 0; }
 #asnapcgkwk .gt_from_md> :last-child { margin-bottom: 0; }
 #asnapcgkwk .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #asnapcgkwk .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #asnapcgkwk .gt_indent_1 { text-indent: 5px; }
 #asnapcgkwk .gt_indent_2 { text-indent: calc(5px * 2); }
 #asnapcgkwk .gt_indent_3 { text-indent: calc(5px * 3); }
 #asnapcgkwk .gt_indent_4 { text-indent: calc(5px * 4); }
 #asnapcgkwk .gt_indent_5 { text-indent: calc(5px * 5); }
 #asnapcgkwk .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #asnapcgkwk .gt_row_group_first td { border-top-width: 2px; }
 #asnapcgkwk .gt_row_group_first th { border-top-width: 2px; }
 #asnapcgkwk .gt_striped { color: #333333; background-color: #F4F4F4; }
 #asnapcgkwk .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #asnapcgkwk .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #asnapcgkwk .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #asnapcgkwk .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #asnapcgkwk .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #asnapcgkwk .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #asnapcgkwk .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #asnapcgkwk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #asnapcgkwk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #asnapcgkwk .gt_left { text-align: left; }
 #asnapcgkwk .gt_center { text-align: center; }
 #asnapcgkwk .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #asnapcgkwk .gt_font_normal { font-weight: normal; }
 #asnapcgkwk .gt_font_bold { font-weight: bold; }
 #asnapcgkwk .gt_font_italic { font-style: italic; }
 #asnapcgkwk .gt_super { font-size: 65%; }
 #asnapcgkwk .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #asnapcgkwk .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #asnapcgkwk .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #asnapcgkwk .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #asnapcgkwk .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #asnapcgkwk .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Per-season calibration — leave-one-season-out |  |  |  |  |  |  |  |
|----|----|----|----|----|----|----|----|
| each season scored by a model fit on the other committed seasons |  |  |  |  |  |  |  |
| season | shots | goals | xG | goals_per_xG | z | logloss | rank_AUC |
| 2024 | 4,922 | 385 | 429.2 | 0.897 | −2.282 | 0.261 | 0.676 |
| 2025 | 5,671 | 499 | 455.0 | 1.097 | 2.199 | 0.281 | 0.688 |
| 2026 | 7,438 | 601 | 711.7 | 0.844 | −4.473 | 0.263 | 0.703 |

&#10;</div>

<img src="pwhl_xg_files/figure-commonmark/cell-14-output-1.png"
width="420" height="300"
alt="Per-season calibration curves by xG decile (leave-one-season-out); the dashed line is perfect calibration." />

Read the table with the z column first: a season outside \|z\| ≈ 2 is
one the other seasons’ shot-quality mix does not price correctly, and
the decile curve for that season shows *where* (crease vs perimeter) the
miss sits. Two data facts shape these rows. First, `strength_state`
coverage differs sharply by season (the 2024 and 2025 parquets carry no
skater/strength columns at all, 2026 is ~99% covered — see the strength
table above), so the `is_pp` / `is_sh` features and the per-strength
recalibrator are effectively active only for 2026 shots, and a model fit
without 2026 has no strength features at all. Second, the frames are
normalized at load (see the setup cell): the feed’s geometry columns are
dropped and recomputed from coordinates, and the skater columns exist
(null) in every season. Without the latter, `predict()` on a lone
2024/2025 frame zero-fills the whole strength block *including*
`is_home` and returns ~25% lower xG for the same rows; without the
former, a pooled fit trains 58% of its shots at distance 0 (the
contamination table above).

## Results — players and teams

<div id="pocxpjqopf" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#pocxpjqopf table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#pocxpjqopf thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#pocxpjqopf p { margin: 0; padding: 0; }
 #pocxpjqopf .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #pocxpjqopf .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #pocxpjqopf .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #pocxpjqopf .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #pocxpjqopf .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #pocxpjqopf .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pocxpjqopf .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #pocxpjqopf .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #pocxpjqopf .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #pocxpjqopf .gt_column_spanner_outer:first-child { padding-left: 0; }
 #pocxpjqopf .gt_column_spanner_outer:last-child { padding-right: 0; }
 #pocxpjqopf .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #pocxpjqopf .gt_spanner_row { border-bottom-style: hidden; }
 #pocxpjqopf .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #pocxpjqopf .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #pocxpjqopf .gt_from_md> :first-child { margin-top: 0; }
 #pocxpjqopf .gt_from_md> :last-child { margin-bottom: 0; }
 #pocxpjqopf .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #pocxpjqopf .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #pocxpjqopf .gt_indent_1 { text-indent: 5px; }
 #pocxpjqopf .gt_indent_2 { text-indent: calc(5px * 2); }
 #pocxpjqopf .gt_indent_3 { text-indent: calc(5px * 3); }
 #pocxpjqopf .gt_indent_4 { text-indent: calc(5px * 4); }
 #pocxpjqopf .gt_indent_5 { text-indent: calc(5px * 5); }
 #pocxpjqopf .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #pocxpjqopf .gt_row_group_first td { border-top-width: 2px; }
 #pocxpjqopf .gt_row_group_first th { border-top-width: 2px; }
 #pocxpjqopf .gt_striped { color: #333333; background-color: #F4F4F4; }
 #pocxpjqopf .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pocxpjqopf .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #pocxpjqopf .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #pocxpjqopf .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #pocxpjqopf .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #pocxpjqopf .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #pocxpjqopf .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #pocxpjqopf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pocxpjqopf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #pocxpjqopf .gt_left { text-align: left; }
 #pocxpjqopf .gt_center { text-align: center; }
 #pocxpjqopf .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #pocxpjqopf .gt_font_normal { font-weight: normal; }
 #pocxpjqopf .gt_font_bold { font-weight: bold; }
 #pocxpjqopf .gt_font_italic { font-style: italic; }
 #pocxpjqopf .gt_super { font-size: 65%; }
 #pocxpjqopf .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pocxpjqopf .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #pocxpjqopf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #pocxpjqopf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #pocxpjqopf .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #pocxpjqopf .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 15 skaters by expected goals — 2026 season |  |  |  |  |  |  |
|----|----|----|----|----|----|----|
| xG summed over on-net shots; GAX = goals above expected |  |  |  |  |  |  |
|  | Player | Team | Shots | xG | G | GAX |
| <img src="https://assets.leaguestat.com/pwhl/240x240/72.jpg"
height="42" /> | Rebecca Leslie | OTT | 93 | 9.11 | 14 | 4.89 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/32.jpg"
height="42" /> | Laura Stacey | MTL | 113 | 9.09 | 9 | −0.09 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/25.jpg"
height="42" /> | Grace Zumwinkle | MIN | 76 | 7.75 | 11 | 3.25 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/61.jpg"
height="42" /> | Hayley Scamurra | MTL | 70 | 7.59 | 9 | 1.41 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/36.jpg"
height="42" /> | Jessie Eldridge | BOS | 83 | 7.45 | 13 | 5.55 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/15.jpg"
height="42" /> | Alina Müller | BOS | 67 | 7.30 | 4 | −3.30 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/205.jpg"
height="42" /> | Sarah Fillier | NY | 73 | 6.27 | 8 | 1.73 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/21.jpg"
height="42" /> | Taylor Heise | MIN | 90 | 6.19 | 12 | 5.81 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/53.jpg"
height="42" /> | Emily Clark | OTT | 63 | 6.13 | 3 | −3.13 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/34.jpg"
height="42" /> | Alex Carpenter | SEA | 62 | 5.88 | 7 | 1.12 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/42.jpg"
height="42" /> | Abby Roque | MTL | 71 | 5.83 | 9 | 3.17 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/58.jpg"
height="42" /> | Brianne Jenner | OTT | 66 | 5.74 | 9 | 3.26 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/23.jpg"
height="42" /> | Kelly Pannek | MIN | 54 | 5.57 | 14 | 8.43 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/4.jpg"
height="42" /> | Shiann Darkangelo | MTL | 54 | 5.53 | 4 | −1.53 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/75.jpg"
height="42" /> | Sarah Nurse | VAN | 66 | 5.53 | 8 | 2.47 |

&#10;</div>

<div id="kycuihcnko" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#kycuihcnko table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#kycuihcnko thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#kycuihcnko p { margin: 0; padding: 0; }
 #kycuihcnko .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #kycuihcnko .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #kycuihcnko .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #kycuihcnko .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #kycuihcnko .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #kycuihcnko .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #kycuihcnko .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #kycuihcnko .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #kycuihcnko .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #kycuihcnko .gt_column_spanner_outer:first-child { padding-left: 0; }
 #kycuihcnko .gt_column_spanner_outer:last-child { padding-right: 0; }
 #kycuihcnko .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #kycuihcnko .gt_spanner_row { border-bottom-style: hidden; }
 #kycuihcnko .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #kycuihcnko .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #kycuihcnko .gt_from_md> :first-child { margin-top: 0; }
 #kycuihcnko .gt_from_md> :last-child { margin-bottom: 0; }
 #kycuihcnko .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #kycuihcnko .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #kycuihcnko .gt_indent_1 { text-indent: 5px; }
 #kycuihcnko .gt_indent_2 { text-indent: calc(5px * 2); }
 #kycuihcnko .gt_indent_3 { text-indent: calc(5px * 3); }
 #kycuihcnko .gt_indent_4 { text-indent: calc(5px * 4); }
 #kycuihcnko .gt_indent_5 { text-indent: calc(5px * 5); }
 #kycuihcnko .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #kycuihcnko .gt_row_group_first td { border-top-width: 2px; }
 #kycuihcnko .gt_row_group_first th { border-top-width: 2px; }
 #kycuihcnko .gt_striped { color: #333333; background-color: #F4F4F4; }
 #kycuihcnko .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #kycuihcnko .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #kycuihcnko .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #kycuihcnko .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #kycuihcnko .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #kycuihcnko .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #kycuihcnko .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #kycuihcnko .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #kycuihcnko .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #kycuihcnko .gt_left { text-align: left; }
 #kycuihcnko .gt_center { text-align: center; }
 #kycuihcnko .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #kycuihcnko .gt_font_normal { font-weight: normal; }
 #kycuihcnko .gt_font_bold { font-weight: bold; }
 #kycuihcnko .gt_font_italic { font-style: italic; }
 #kycuihcnko .gt_super { font-size: 65%; }
 #kycuihcnko .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #kycuihcnko .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #kycuihcnko .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #kycuihcnko .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #kycuihcnko .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #kycuihcnko .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Team shot generation — 2026 season |       |        |           |             |
|------------------------------------|-------|--------|-----------|-------------|
| team                               | games | xG_for | goals_for | xG_per_game |
| New York Sirens                    | 21    | 49.67  | 43        | 2.37        |
| Boston Fleet                       | 26    | 60.12  | 58        | 2.31        |
| Montréal Victoire                  | 32    | 73.89  | 81        | 2.31        |
| Minnesota Frost                    | 27    | 61.50  | 77        | 2.28        |
| Seattle Torrent                    | 23    | 50.23  | 47        | 2.18        |
| Ottawa Charge                      | 29    | 60.48  | 66        | 2.09        |
| Toronto Sceptres                   | 21    | 42.46  | 33        | 2.02        |
| Vancouver Goldeneyes               | 21    | 41.54  | 53        | 1.98        |

&#10;</div>

Positive GAX identifies finishers beating their shot quality; sustained
negative GAX at high xG identifies volume shooters running cold. Because
the league is three seasons old these player samples are small — GAX
here is a descriptive ledger, not a talent estimate.

## Provenance & reproducibility

- **Trained on:** every committed PWHL season in this repository
  (`pwhl/pbp/parquet/play_by_play_{2024..}.parquet`); the holdout
  evaluation fits on all seasons except the newest and scores the
  newest.
- **Model spec:** `sportsdataverse.pwhl.pwhl_xg_proxy.fit_pwhl_coord_xg`
  (logistic regression + per-strength Platt recalibration); no bundled
  artifact — deterministic re-fit from data.
- **Rebuild this document:** `scripts/render_model_docs.sh` (Quarto →
  GFM; uses this repo’s `.venv` via `QUARTO_PYTHON`; deps in the `docs`
  group: `uv sync --group docs`).
- **Production surface:** stage `python/pwhl_model_01_xg_pbp.py`
  publishes xG-enriched per-season parquet to the `pwhl_xg_pbp` release
  tag (`pwhl_xg_cron.yml`); per-publish metadata in the card sidecars.
  Single home: `models/manifest.yaml`; registry row:
  `models/REGISTRY.md`.

## Avenues for improvement & open issues

- **EV/PP/SH-split models** — catalogued in `dev/t5_xg_reevaluation/`;
  the league-wide `strength_state` column exists precisely to enable
  this.
- **Resolved (2026-09-01, PR \#5):** *Per-season calibration drift* —
  the leave-one-season-out table and decile curves above recompute
  per-season calibration (goals/ΣxG, binomial z, AUC) at every render
  and extend automatically as seasons land.
- **Pre-shot passing data does not exist publicly** — royal-road
  one-timers are under-rated by construction; honest gains come from
  better rebound and rush definitions, not more features.
- **Known issue:** the published-release evaluation is in-sample; this
  document’s season-holdout is the honest gate, and it should be re-run
  as each new season lands (the render does this automatically).
- **Known issue (measured 2026-09-01):** *pooled-fit geometry
  contamination* — `pwhl_xg_proxy._shot_geometry` reuses the feed’s
  `shot_distance`/`shot_angle` when the columns exist, so in a pooled
  frame the pre-2025-26 seasons (which lack them) keep null geometry
  that `_build_xg_features` fills with 0.0: 10,593 of 18,031 pooled
  shots (58%) train at distance 0. The production publisher fits on
  exactly that pooled frame, so the published `pwhl_xg_pbp` assets are
  affected until sdv-py recomputes null geometry (the table in *Training
  data* re-measures it every render). This document works around it by
  dropping the feed geometry at load.
- **Known issue (measured 2026-09-01):** *lone early-season scoring* — a
  2024 or 2025 frame scored on its own lacks the skater columns, so
  `predict()` zero-fills `is_home` together with the strength block
  (~25% lower xG for the same rows). The load step here adds the columns
  as null; the durable fix is for `_build_xg_features` not to tie
  `is_home` to the skater columns.
