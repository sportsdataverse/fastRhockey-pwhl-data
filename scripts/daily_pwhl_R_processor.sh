#!/bin/bash
# Process PWHL datasets from fastRhockey-pwhl-raw repo
# Usage: bash scripts/daily_pwhl_R_processor.sh -s 2025 -e 2025

while getopts s:e: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
    esac
done

if [ -z "$START_YEAR" ] || [ -z "$END_YEAR" ]; then
    echo "Usage: $0 -s <start_year> -e <end_year>"
    exit 1
fi

for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    echo "=== Processing PWHL data for season $i ==="
    git pull >> /dev/null
    git config --local user.email "action@github.com"
    git config --local user.name "Github Action"
    Rscript R/pwhl_data_creation.R -s $i -e $i
    git pull >> /dev/null
    git add pwhl/* >> /dev/null
    git add logs/* >> /dev/null
    git pull >> /dev/null
    git add . >> /dev/null
    git commit -m "PWHL Data Updated (Start: $i End: $i)" || echo "No changes to commit"
    git pull >> /dev/null
    git push >> /dev/null
done
