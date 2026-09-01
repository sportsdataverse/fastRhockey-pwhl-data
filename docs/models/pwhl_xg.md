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

<div id="geoprfoclq" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#geoprfoclq table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#geoprfoclq thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#geoprfoclq p { margin: 0; padding: 0; }
 #geoprfoclq .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #geoprfoclq .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #geoprfoclq .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #geoprfoclq .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #geoprfoclq .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #geoprfoclq .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #geoprfoclq .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #geoprfoclq .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #geoprfoclq .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #geoprfoclq .gt_column_spanner_outer:first-child { padding-left: 0; }
 #geoprfoclq .gt_column_spanner_outer:last-child { padding-right: 0; }
 #geoprfoclq .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #geoprfoclq .gt_spanner_row { border-bottom-style: hidden; }
 #geoprfoclq .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #geoprfoclq .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #geoprfoclq .gt_from_md> :first-child { margin-top: 0; }
 #geoprfoclq .gt_from_md> :last-child { margin-bottom: 0; }
 #geoprfoclq .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #geoprfoclq .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #geoprfoclq .gt_indent_1 { text-indent: 5px; }
 #geoprfoclq .gt_indent_2 { text-indent: calc(5px * 2); }
 #geoprfoclq .gt_indent_3 { text-indent: calc(5px * 3); }
 #geoprfoclq .gt_indent_4 { text-indent: calc(5px * 4); }
 #geoprfoclq .gt_indent_5 { text-indent: calc(5px * 5); }
 #geoprfoclq .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #geoprfoclq .gt_row_group_first td { border-top-width: 2px; }
 #geoprfoclq .gt_row_group_first th { border-top-width: 2px; }
 #geoprfoclq .gt_striped { color: #333333; background-color: #F4F4F4; }
 #geoprfoclq .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #geoprfoclq .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #geoprfoclq .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #geoprfoclq .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #geoprfoclq .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #geoprfoclq .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #geoprfoclq .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #geoprfoclq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #geoprfoclq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #geoprfoclq .gt_left { text-align: left; }
 #geoprfoclq .gt_center { text-align: center; }
 #geoprfoclq .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #geoprfoclq .gt_font_normal { font-weight: normal; }
 #geoprfoclq .gt_font_bold { font-weight: bold; }
 #geoprfoclq .gt_font_italic { font-style: italic; }
 #geoprfoclq .gt_super { font-size: 65%; }
 #geoprfoclq .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #geoprfoclq .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #geoprfoclq .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #geoprfoclq .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #geoprfoclq .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #geoprfoclq .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

## Exploratory data analysis

<img src="pwhl_xg_files/figure-commonmark/cell-4-output-1.png"
width="420" height="300"
alt="Goal rate falls steeply with shot distance; the model’s dominant feature." />

<img src="pwhl_xg_files/figure-commonmark/cell-5-output-1.png"
width="420" height="300"
alt="Shot locations (offensive half, attacking to the right); goals in orange." />

<div id="ulxukkickr" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#ulxukkickr table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#ulxukkickr thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#ulxukkickr p { margin: 0; padding: 0; }
 #ulxukkickr .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #ulxukkickr .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #ulxukkickr .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #ulxukkickr .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #ulxukkickr .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ulxukkickr .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ulxukkickr .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #ulxukkickr .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #ulxukkickr .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #ulxukkickr .gt_column_spanner_outer:first-child { padding-left: 0; }
 #ulxukkickr .gt_column_spanner_outer:last-child { padding-right: 0; }
 #ulxukkickr .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #ulxukkickr .gt_spanner_row { border-bottom-style: hidden; }
 #ulxukkickr .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #ulxukkickr .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #ulxukkickr .gt_from_md> :first-child { margin-top: 0; }
 #ulxukkickr .gt_from_md> :last-child { margin-bottom: 0; }
 #ulxukkickr .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #ulxukkickr .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #ulxukkickr .gt_indent_1 { text-indent: 5px; }
 #ulxukkickr .gt_indent_2 { text-indent: calc(5px * 2); }
 #ulxukkickr .gt_indent_3 { text-indent: calc(5px * 3); }
 #ulxukkickr .gt_indent_4 { text-indent: calc(5px * 4); }
 #ulxukkickr .gt_indent_5 { text-indent: calc(5px * 5); }
 #ulxukkickr .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #ulxukkickr .gt_row_group_first td { border-top-width: 2px; }
 #ulxukkickr .gt_row_group_first th { border-top-width: 2px; }
 #ulxukkickr .gt_striped { color: #333333; background-color: #F4F4F4; }
 #ulxukkickr .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ulxukkickr .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ulxukkickr .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #ulxukkickr .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #ulxukkickr .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #ulxukkickr .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #ulxukkickr .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #ulxukkickr .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ulxukkickr .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ulxukkickr .gt_left { text-align: left; }
 #ulxukkickr .gt_center { text-align: center; }
 #ulxukkickr .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #ulxukkickr .gt_font_normal { font-weight: normal; }
 #ulxukkickr .gt_font_bold { font-weight: bold; }
 #ulxukkickr .gt_font_italic { font-style: italic; }
 #ulxukkickr .gt_super { font-size: 65%; }
 #ulxukkickr .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ulxukkickr .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #ulxukkickr .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #ulxukkickr .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #ulxukkickr .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #ulxukkickr .gt_asterisk { font-size: 100%; vertical-align: 0; }
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

<div id="nrzfjyitro" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#nrzfjyitro table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#nrzfjyitro thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#nrzfjyitro p { margin: 0; padding: 0; }
 #nrzfjyitro .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #nrzfjyitro .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #nrzfjyitro .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #nrzfjyitro .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #nrzfjyitro .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nrzfjyitro .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nrzfjyitro .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #nrzfjyitro .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #nrzfjyitro .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #nrzfjyitro .gt_column_spanner_outer:first-child { padding-left: 0; }
 #nrzfjyitro .gt_column_spanner_outer:last-child { padding-right: 0; }
 #nrzfjyitro .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #nrzfjyitro .gt_spanner_row { border-bottom-style: hidden; }
 #nrzfjyitro .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #nrzfjyitro .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #nrzfjyitro .gt_from_md> :first-child { margin-top: 0; }
 #nrzfjyitro .gt_from_md> :last-child { margin-bottom: 0; }
 #nrzfjyitro .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #nrzfjyitro .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #nrzfjyitro .gt_indent_1 { text-indent: 5px; }
 #nrzfjyitro .gt_indent_2 { text-indent: calc(5px * 2); }
 #nrzfjyitro .gt_indent_3 { text-indent: calc(5px * 3); }
 #nrzfjyitro .gt_indent_4 { text-indent: calc(5px * 4); }
 #nrzfjyitro .gt_indent_5 { text-indent: calc(5px * 5); }
 #nrzfjyitro .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #nrzfjyitro .gt_row_group_first td { border-top-width: 2px; }
 #nrzfjyitro .gt_row_group_first th { border-top-width: 2px; }
 #nrzfjyitro .gt_striped { color: #333333; background-color: #F4F4F4; }
 #nrzfjyitro .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nrzfjyitro .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nrzfjyitro .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #nrzfjyitro .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #nrzfjyitro .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #nrzfjyitro .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #nrzfjyitro .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #nrzfjyitro .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nrzfjyitro .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nrzfjyitro .gt_left { text-align: left; }
 #nrzfjyitro .gt_center { text-align: center; }
 #nrzfjyitro .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #nrzfjyitro .gt_font_normal { font-weight: normal; }
 #nrzfjyitro .gt_font_bold { font-weight: bold; }
 #nrzfjyitro .gt_font_italic { font-style: italic; }
 #nrzfjyitro .gt_super { font-size: 65%; }
 #nrzfjyitro .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nrzfjyitro .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #nrzfjyitro .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #nrzfjyitro .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #nrzfjyitro .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #nrzfjyitro .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Logistic coefficients and standardized importance |  |  |
|----|----|----|
| importance = \|coef\| × sd(feature), full-corpus fit |  |  |
| feature | coefficient | std_importance |
| shot_distance | −0.0165 | 0.3349 |
| rebound | 1.3185 | 0.3131 |
| last_shot | −0.6279 | 0.2994 |
| last_faceoff | −0.5420 | 0.2710 |
| is_wrist | −0.2975 | 0.1478 |
| last_hit | −0.4485 | 0.1174 |
| is_tip | 0.4252 | 0.0830 |
| distance_from_last | 0.0016 | 0.0728 |
| is_sh | 0.3522 | 0.0716 |
| empty_net_for | 0.7335 | 0.0642 |
| is_backhand | −0.2192 | 0.0529 |
| is_pp | −0.1793 | 0.0505 |
| last_blocked | −0.2017 | 0.0492 |
| shot_angle | 0.0023 | 0.0460 |
| is_home | 0.0827 | 0.0414 |
| last_y | 0.0014 | 0.0304 |
| last_x | 0.0005 | 0.0272 |
| is_slap | −0.0714 | 0.0149 |
| is_snap | 0.0353 | 0.0106 |
| time_since_last | −0.0003 | 0.0105 |
| last_penalty | −0.0075 | 0.0004 |

&#10;</div>

<img src="pwhl_xg_files/figure-commonmark/cell-8-output-1.png"
width="420" height="300"
alt="Standardized feature importance (|coef|·sd)." />

## SHAP-style attribution

For a linear-logit model the exact Shapley attribution of each feature
on the log-odds scale is `coef × (x − E[x])` — no approximation needed.
The distribution of per-shot attributions shows how each feature moves
individual shots, not just the average:

<img src="pwhl_xg_files/figure-commonmark/cell-9-output-1.png"
width="420" height="300"
alt="Per-shot log-odds attributions (linear SHAP), top 8 features by importance; 2,000-shot sample." />

Distance dominates — its attributions span roughly ±1 log-odds — with
rebound state the strongest binary: a rebound shot gains about one
log-odds (odds ×2.7) over a non-rebound shot from the same location.

## Evaluation

The honest test for a young league is a **season holdout**: fit on all
seasons before the newest, score the newest, and compare against a
constant-rate baseline.

<div id="wnorzwwtti" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#wnorzwwtti table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#wnorzwwtti thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#wnorzwwtti p { margin: 0; padding: 0; }
 #wnorzwwtti .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #wnorzwwtti .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #wnorzwwtti .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #wnorzwwtti .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #wnorzwwtti .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wnorzwwtti .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wnorzwwtti .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #wnorzwwtti .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #wnorzwwtti .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #wnorzwwtti .gt_column_spanner_outer:first-child { padding-left: 0; }
 #wnorzwwtti .gt_column_spanner_outer:last-child { padding-right: 0; }
 #wnorzwwtti .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #wnorzwwtti .gt_spanner_row { border-bottom-style: hidden; }
 #wnorzwwtti .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #wnorzwwtti .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #wnorzwwtti .gt_from_md> :first-child { margin-top: 0; }
 #wnorzwwtti .gt_from_md> :last-child { margin-bottom: 0; }
 #wnorzwwtti .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #wnorzwwtti .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #wnorzwwtti .gt_indent_1 { text-indent: 5px; }
 #wnorzwwtti .gt_indent_2 { text-indent: calc(5px * 2); }
 #wnorzwwtti .gt_indent_3 { text-indent: calc(5px * 3); }
 #wnorzwwtti .gt_indent_4 { text-indent: calc(5px * 4); }
 #wnorzwwtti .gt_indent_5 { text-indent: calc(5px * 5); }
 #wnorzwwtti .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #wnorzwwtti .gt_row_group_first td { border-top-width: 2px; }
 #wnorzwwtti .gt_row_group_first th { border-top-width: 2px; }
 #wnorzwwtti .gt_striped { color: #333333; background-color: #F4F4F4; }
 #wnorzwwtti .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wnorzwwtti .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wnorzwwtti .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #wnorzwwtti .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #wnorzwwtti .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #wnorzwwtti .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #wnorzwwtti .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #wnorzwwtti .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wnorzwwtti .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wnorzwwtti .gt_left { text-align: left; }
 #wnorzwwtti .gt_center { text-align: center; }
 #wnorzwwtti .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #wnorzwwtti .gt_font_normal { font-weight: normal; }
 #wnorzwwtti .gt_font_bold { font-weight: bold; }
 #wnorzwwtti .gt_font_italic { font-style: italic; }
 #wnorzwwtti .gt_super { font-size: 65%; }
 #wnorzwwtti .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wnorzwwtti .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #wnorzwwtti .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #wnorzwwtti .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #wnorzwwtti .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #wnorzwwtti .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Season-holdout evaluation — fit 2024–2025, scored on 2026 |         |          |
|-----------------------------------------------------------|---------|----------|
| 7,438 held-out shots, goal rate 0.081                     |         |          |
| model                                                     | logloss | rank_AUC |
| coordinate xG                                             | 0.2628  | 0.7032   |
| constant baseline (train goal rate)                       | 0.2808  | 0.5000   |

&#10;</div>

<img src="pwhl_xg_files/figure-commonmark/cell-11-output-1.png"
width="420" height="300"
alt="Holdout calibration by xG decile — points on the diagonal are perfectly calibrated." />

The model beats the constant baseline on the held-out season and its
decile calibration tracks the diagonal. The published per-season assets
on the `pwhl_xg_pbp` release tag are scored by the same recipe with the
full corpus available at publish time; their in-sample evaluation lives
in [`pwhl_xg_eval_card.json`](pwhl_xg_eval_card.json).

## Results — players and teams

<div id="qliexdkepb" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#qliexdkepb table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#qliexdkepb thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#qliexdkepb p { margin: 0; padding: 0; }
 #qliexdkepb .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #qliexdkepb .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #qliexdkepb .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #qliexdkepb .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #qliexdkepb .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qliexdkepb .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qliexdkepb .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #qliexdkepb .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #qliexdkepb .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #qliexdkepb .gt_column_spanner_outer:first-child { padding-left: 0; }
 #qliexdkepb .gt_column_spanner_outer:last-child { padding-right: 0; }
 #qliexdkepb .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #qliexdkepb .gt_spanner_row { border-bottom-style: hidden; }
 #qliexdkepb .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #qliexdkepb .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #qliexdkepb .gt_from_md> :first-child { margin-top: 0; }
 #qliexdkepb .gt_from_md> :last-child { margin-bottom: 0; }
 #qliexdkepb .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #qliexdkepb .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #qliexdkepb .gt_indent_1 { text-indent: 5px; }
 #qliexdkepb .gt_indent_2 { text-indent: calc(5px * 2); }
 #qliexdkepb .gt_indent_3 { text-indent: calc(5px * 3); }
 #qliexdkepb .gt_indent_4 { text-indent: calc(5px * 4); }
 #qliexdkepb .gt_indent_5 { text-indent: calc(5px * 5); }
 #qliexdkepb .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #qliexdkepb .gt_row_group_first td { border-top-width: 2px; }
 #qliexdkepb .gt_row_group_first th { border-top-width: 2px; }
 #qliexdkepb .gt_striped { color: #333333; background-color: #F4F4F4; }
 #qliexdkepb .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qliexdkepb .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qliexdkepb .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #qliexdkepb .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #qliexdkepb .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #qliexdkepb .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #qliexdkepb .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #qliexdkepb .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qliexdkepb .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qliexdkepb .gt_left { text-align: left; }
 #qliexdkepb .gt_center { text-align: center; }
 #qliexdkepb .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #qliexdkepb .gt_font_normal { font-weight: normal; }
 #qliexdkepb .gt_font_bold { font-weight: bold; }
 #qliexdkepb .gt_font_italic { font-style: italic; }
 #qliexdkepb .gt_super { font-size: 65%; }
 #qliexdkepb .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qliexdkepb .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #qliexdkepb .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #qliexdkepb .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #qliexdkepb .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #qliexdkepb .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Top 15 skaters by expected goals — 2026 season |  |  |  |  |  |  |
|----|----|----|----|----|----|----|
| xG summed over on-net shots; GAX = goals above expected |  |  |  |  |  |  |
|  | Player | Team | Shots | xG | G | GAX |
| <img src="https://assets.leaguestat.com/pwhl/240x240/32.jpg"
height="42" /> | Laura Stacey | MTL | 113 | 8.00 | 9 | 1.00 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/72.jpg"
height="42" /> | Rebecca Leslie | OTT | 93 | 7.17 | 14 | 6.83 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/36.jpg"
height="42" /> | Jessie Eldridge | BOS | 83 | 7.17 | 13 | 5.83 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/61.jpg"
height="42" /> | Hayley Scamurra | MTL | 70 | 7.10 | 9 | 1.90 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/25.jpg"
height="42" /> | Grace Zumwinkle | MIN | 76 | 6.95 | 11 | 4.05 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/15.jpg"
height="42" /> | Alina Müller | BOS | 67 | 5.91 | 4 | −1.91 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/21.jpg"
height="42" /> | Taylor Heise | MIN | 90 | 5.54 | 12 | 6.46 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/42.jpg"
height="42" /> | Abby Roque | MTL | 71 | 5.42 | 9 | 3.58 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/34.jpg"
height="42" /> | Alex Carpenter | SEA | 62 | 5.35 | 7 | 1.65 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/23.jpg"
height="42" /> | Kelly Pannek | MIN | 54 | 5.25 | 14 | 8.75 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/58.jpg"
height="42" /> | Brianne Jenner | OTT | 66 | 5.13 | 9 | 3.87 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/205.jpg"
height="42" /> | Sarah Fillier | NY | 73 | 5.05 | 8 | 2.95 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/53.jpg"
height="42" /> | Emily Clark | OTT | 63 | 4.99 | 3 | −1.99 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/75.jpg"
height="42" /> | Sarah Nurse | VAN | 66 | 4.97 | 8 | 3.03 |
| <img src="https://assets.leaguestat.com/pwhl/240x240/20.jpg"
height="42" /> | Kendall Coyne Schofield | MIN | 56 | 4.73 | 6 | 1.27 |

&#10;</div>

<div id="cjauyixmmf" style="padding-left:0px;padding-right:0px;padding-top:10px;padding-bottom:10px;overflow-x:auto;overflow-y:auto;width:auto;height:auto;">
<style>
#cjauyixmmf table {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
        }
&#10;#cjauyixmmf thead, tbody, tfoot, tr, td, th { border-style: none; }
 tr { background-color: transparent; }
#cjauyixmmf p { margin: 0; padding: 0; }
 #cjauyixmmf .gt_table { display: table; border-collapse: collapse; line-height: normal; margin-left: auto; margin-right: auto; color: #333333; font-size: 16px; font-weight: normal; font-style: normal; background-color: #FFFFFF; width: auto; border-top-style: solid; border-top-width: 2px; border-top-color: #A8A8A8; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #A8A8A8; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; }
 #cjauyixmmf .gt_caption { padding-top: 4px; padding-bottom: 4px; }
 #cjauyixmmf .gt_title { color: #333333; font-size: 125%; font-weight: initial; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; border-bottom-color: #FFFFFF; border-bottom-width: 0; }
 #cjauyixmmf .gt_subtitle { color: #333333; font-size: 85%; font-weight: initial; padding-top: 3px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; border-top-color: #FFFFFF; border-top-width: 0; }
 #cjauyixmmf .gt_heading { background-color: #FFFFFF; text-align: center; border-bottom-color: #FFFFFF; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #cjauyixmmf .gt_bottom_border { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #cjauyixmmf .gt_col_headings { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; }
 #cjauyixmmf .gt_col_heading { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; padding-left: 5px; padding-right: 5px; overflow-x: hidden; }
 #cjauyixmmf .gt_column_spanner_outer { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: normal; text-transform: inherit; padding-top: 0; padding-bottom: 0; padding-left: 4px; padding-right: 4px; }
 #cjauyixmmf .gt_column_spanner_outer:first-child { padding-left: 0; }
 #cjauyixmmf .gt_column_spanner_outer:last-child { padding-right: 0; }
 #cjauyixmmf .gt_column_spanner { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: bottom; padding-top: 5px; padding-bottom: 5px; overflow-x: hidden; display: inline-block; width: 100%; }
 #cjauyixmmf .gt_spanner_row { border-bottom-style: hidden; }
 #cjauyixmmf .gt_group_heading { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; text-align: left; }
 #cjauyixmmf .gt_empty_group_heading { padding: 0.5px; color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; vertical-align: middle; }
 #cjauyixmmf .gt_from_md> :first-child { margin-top: 0; }
 #cjauyixmmf .gt_from_md> :last-child { margin-bottom: 0; }
 #cjauyixmmf .gt_row { padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; margin: 10px; border-top-style: solid; border-top-width: 1px; border-top-color: #D3D3D3; border-left-style: none; border-left-width: 1px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 1px; border-right-color: #D3D3D3; vertical-align: middle; overflow-x: hidden; }
 #cjauyixmmf .gt_stub { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; }
 #cjauyixmmf .gt_indent_1 { text-indent: 5px; }
 #cjauyixmmf .gt_indent_2 { text-indent: calc(5px * 2); }
 #cjauyixmmf .gt_indent_3 { text-indent: calc(5px * 3); }
 #cjauyixmmf .gt_indent_4 { text-indent: calc(5px * 4); }
 #cjauyixmmf .gt_indent_5 { text-indent: calc(5px * 5); }
 #cjauyixmmf .gt_stub_row_group { color: #333333; background-color: #FFFFFF; font-size: 100%; font-weight: initial; text-transform: inherit; border-right-style: solid; border-right-width: 2px; border-right-color: #D3D3D3; padding-left: 5px; padding-right: 5px; vertical-align: top; }
 #cjauyixmmf .gt_row_group_first td { border-top-width: 2px; }
 #cjauyixmmf .gt_row_group_first th { border-top-width: 2px; }
 #cjauyixmmf .gt_striped { color: #333333; background-color: #F4F4F4; }
 #cjauyixmmf .gt_table_body { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #cjauyixmmf .gt_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #cjauyixmmf .gt_first_summary_row { border-top-style: solid; border-top-width: 2px; border-top-color: #D3D3D3; }
 #cjauyixmmf .gt_last_summary_row_top { border-bottom-style: solid; border-bottom-width: 2px; border-bottom-color: #D3D3D3; }
 #cjauyixmmf .gt_grand_summary_row { color: #333333; background-color: #FFFFFF; text-transform: inherit; padding-top: 8px; padding-bottom: 8px; padding-left: 5px; padding-right: 5px; }
 #cjauyixmmf .gt_first_grand_summary_row_bottom { border-top-style: double; border-top-width: 6px; border-top-color: #D3D3D3; }
 #cjauyixmmf .gt_last_grand_summary_row_top { border-bottom-style: double; border-bottom-width: 6px; border-bottom-color: #D3D3D3; }
 #cjauyixmmf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #cjauyixmmf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #cjauyixmmf .gt_left { text-align: left; }
 #cjauyixmmf .gt_center { text-align: center; }
 #cjauyixmmf .gt_right { text-align: right; font-variant-numeric: tabular-nums; }
 #cjauyixmmf .gt_font_normal { font-weight: normal; }
 #cjauyixmmf .gt_font_bold { font-weight: bold; }
 #cjauyixmmf .gt_font_italic { font-style: italic; }
 #cjauyixmmf .gt_super { font-size: 65%; }
 #cjauyixmmf .gt_footnotes { color: font-color(#FFFFFF); background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #cjauyixmmf .gt_footnote { margin: 0px; font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; }
 #cjauyixmmf .gt_sourcenotes { color: #333333; background-color: #FFFFFF; border-bottom-style: none; border-bottom-width: 2px; border-bottom-color: #D3D3D3; border-left-style: none; border-left-width: 2px; border-left-color: #D3D3D3; border-right-style: none; border-right-width: 2px; border-right-color: #D3D3D3; }
 #cjauyixmmf .gt_sourcenote { font-size: 90%; padding-top: 4px; padding-bottom: 4px; padding-left: 5px; padding-right: 5px; text-align: left; }
 #cjauyixmmf .gt_footnote_marks { font-size: 75%; vertical-align: 0.4em; position: initial; }
 #cjauyixmmf .gt_asterisk { font-size: 100%; vertical-align: 0; }
 &#10;</style>

| Team shot generation — 2026 season |       |        |           |             |
|------------------------------------|-------|--------|-----------|-------------|
| team                               | games | xG_for | goals_for | xG_per_game |
| Montréal Victoire                  | 32    | 66.75  | 81        | 2.09        |
| Minnesota Frost                    | 27    | 55.54  | 77        | 2.06        |
| New York Sirens                    | 21    | 42.59  | 43        | 2.03        |
| Boston Fleet                       | 26    | 52.05  | 58        | 2.00        |
| Seattle Torrent                    | 23    | 44.58  | 47        | 1.94        |
| Toronto Sceptres                   | 21    | 38.07  | 33        | 1.81        |
| Vancouver Goldeneyes               | 21    | 37.01  | 53        | 1.76        |
| Ottawa Charge                      | 29    | 50.72  | 66        | 1.75        |

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
- **Per-season calibration drift** — three seasons now exist; a
  per-season curve would show whether the shot-quality mix matures as
  the league does.
- **Pre-shot passing data does not exist publicly** — royal-road
  one-timers are under-rated by construction; honest gains come from
  better rebound and rush definitions, not more features.
- **Known issue:** the published-release evaluation is in-sample; this
  document’s season-holdout is the honest gate, and it should be re-run
  as each new season lands (the render does this automatically).
