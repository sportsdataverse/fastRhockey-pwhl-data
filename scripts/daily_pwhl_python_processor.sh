#!/bin/bash
# Compile PWHL datasets with the Python compiler (python/pwhl_data_build).
#
# Drop-in replacement for daily_pwhl_R_processor.sh: same -s/-e contract, same
# per-season commit subject, same exit-code propagation, so sdv-orch's
# `data.build` stage can call either one.
#
#   bash scripts/daily_pwhl_python_processor.sh -s 2026 -e 2026
#
# Reads the raw finals from the sibling fastRhockey-pwhl-raw checkout (the
# `raw.scrape` stage runs first and self-commits there), writes parquet into
# pwhl/, and uploads parquet + rds + csv to the pwhl_* releases. rds/csv are
# gitignored -- the releases are their distribution channel.

set -uo pipefail

while getopts s:e: flag; do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        *) echo "Usage: $0 -s <start_year> -e <end_year>"; exit 1;;
    esac
done

if [ -z "${START_YEAR:-}" ] || [ -z "${END_YEAR:-}" ]; then
    echo "Usage: $0 -s <start_year> -e <end_year>"
    exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOS_ROOT="${SDV_REPOS:-/mnt/sdv_repos}"
RAW_ROOT="${PWHL_RAW_ROOT:-${REPOS_ROOT}/fastRhockey-pwhl-raw}"
OUT_DIR="${REPO_DIR}/pwhl"

# Call the project venv's interpreter by absolute path rather than going through
# `uv run`. sdv-orch invokes this from a systemd unit, whose PATH is the systemd
# default (/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin) and does
# NOT include /root/.local/bin where uv lives -- so `uv` exits 127 there while
# working fine in an interactive shell. The venv interpreter needs no PATH lookup
# and no cwd games (pwhl_data_build is installed into it).
PYBIN="${PWHL_PYBIN:-${REPO_DIR}/python/.venv/bin/python}"

# Fail before touching git if the upstream checkout isn't where we expect. A
# missing raw root would otherwise compile zero games and "succeed", quietly
# publishing nothing.
if [ ! -d "${RAW_ROOT}/pwhl/json/final" ]; then
    echo "::error ::raw finals not found at ${RAW_ROOT}/pwhl/json/final"
    exit 1
fi

if [ ! -x "${PYBIN}" ]; then
    echo "::error ::python venv not found at ${PYBIN} -- run 'uv sync' in ${REPO_DIR}/python"
    exit 1
fi

cd "${REPO_DIR}" || exit 1
mkdir -p logs

ANY_FAILED=0
for i in $(seq "${START_YEAR}" "${END_YEAR}"); do
    LOGFILE="logs/fastRhockey_pwhl_data_logfile_${i}.log"
    TMPLOG=$(mktemp "/tmp/fastRhockey_pwhl_data_logfile_${i}.XXXXXX.log")
    echo "=== Processing PWHL data for season $i (Python) ==="

    # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
    # don't trip over their own log output being written to a tracked file.
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"

        "${PYBIN}" -m pwhl_data_build.season \
            -s "$i" --raw-root "${RAW_ROOT}" --out "${OUT_DIR}"
        echo "COMPILE_RC=$?" > "/tmp/_pwhl_compile_rc_${i}"

        # Publish only what compiled. Uploading is idempotent (--clobber), so a
        # partial season still ships the datasets that built.
        "${PYBIN}" -c "
from pwhl_data_build.publish import publish_season
print(len(publish_season('${OUT_DIR}', ${i})), 'assets uploaded')
"

        git pull >> /dev/null
        git add pwhl >> /dev/null
        git commit -m "PWHL Data Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$TMPLOG"

    COMPILE_RC=$(sed 's/COMPILE_RC=//' "/tmp/_pwhl_compile_rc_${i}" 2>/dev/null)
    rm -f "/tmp/_pwhl_compile_rc_${i}"

    cp "$TMPLOG" "$LOGFILE"
    git stash -u --quiet 2>/dev/null || true
    git pull --rebase >> /dev/null || true
    git stash pop --quiet 2>/dev/null || true
    git add "$LOGFILE"
    git commit -m "PWHL Data log update (Start: $i End: $i)" >> /dev/null || echo "No log changes to commit"
    git push >> /dev/null
    rm -f "$TMPLOG"

    # Surface a failed compile rather than masking it with a successful push;
    # finish the remaining seasons first.
    if [ "${COMPILE_RC:-0}" != "0" ]; then
        echo "::error ::pwhl_data_build.season for season $i exited with code ${COMPILE_RC}"
        ANY_FAILED=1
    fi
done

if [ "${ANY_FAILED}" != "0" ]; then
    echo "::error ::At least one season's compile exited non-zero. See per-season logs."
    exit 1
fi
