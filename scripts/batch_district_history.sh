#!/bin/bash
# Batch process district history for all states.
# Downloads + parses from Ballotpedia, then populates the DB.
#
# Usage:
#   bash scripts/batch_district_history.sh           # all states
#   bash scripts/batch_district_history.sh AR CA FL   # specific states
#   bash scripts/batch_district_history.sh --skip-done # skip already-populated states

set -euo pipefail
cd "$(dirname "$0")/.."

LOGFILE="/tmp/district_history/batch_log.txt"
mkdir -p /tmp/district_history

STATES=(
    AL AK AZ AR CA CO CT DE FL GA
    HI ID IL IN IA KS KY LA ME MD
    MA MI MN MS MO MT NE NV NH NJ
    NM NY NC ND OH OK OR PA RI SC
    SD TN TX UT VT VA WA WV WI WY
)

SKIP_DONE=false

# Parse arguments
if [[ "${1:-}" == "--skip-done" ]]; then
    SKIP_DONE=true
    shift
fi

# If specific states provided, use those instead
if [[ $# -gt 0 ]]; then
    STATES=("$@")
fi

echo "═══════════════════════════════════════════════" | tee -a "$LOGFILE"
echo "BATCH DISTRICT HISTORY — $(date)" | tee -a "$LOGFILE"
echo "States: ${STATES[*]}" | tee -a "$LOGFILE"
echo "═══════════════════════════════════════════════" | tee -a "$LOGFILE"

TOTAL_ELECTIONS=0
TOTAL_CANDIDACIES=0
TOTAL_CANDIDATES=0
FAILED_STATES=()

for STATE in "${STATES[@]}"; do
    STATE=$(echo "$STATE" | tr '[:lower:]' '[:upper:]')

    # Check if already done
    if [[ "$SKIP_DONE" == true ]] && [[ -f "/tmp/district_history/${STATE}.done" ]]; then
        echo "[$STATE] Already done, skipping" | tee -a "$LOGFILE"
        continue
    fi

    echo "" | tee -a "$LOGFILE"
    echo "━━━ $STATE ━━━ $(date +%H:%M:%S)" | tee -a "$LOGFILE"

    # Step 1: Download + Parse
    echo "  Downloading..." | tee -a "$LOGFILE"
    if ! python3 scripts/download_district_history.py --state "$STATE" 2>&1 | tee -a "$LOGFILE" | grep -E "^(═|OUTPUT|Districts|Elections|Candidate|  By year|    20|  Parsed|  Total|Error|WARNING)" ; then
        echo "  [$STATE] Download FAILED" | tee -a "$LOGFILE"
        FAILED_STATES+=("$STATE")
        continue
    fi

    # Step 2: Populate DB
    echo "  Populating..." | tee -a "$LOGFILE"
    OUTPUT=$(python3 scripts/populate_district_history.py --state "$STATE" 2>&1)
    echo "$OUTPUT" >> "$LOGFILE"

    # Extract stats
    INSERTED=$(echo "$OUTPUT" | grep "Elections inserted:" | awk '{print $NF}')
    CANDS=$(echo "$OUTPUT" | grep "Candidacies inserted:" | awk '{print $NF}')
    CREATED=$(echo "$OUTPUT" | grep "Candidates created:" | awk '{print $NF}')

    echo "  [$STATE] Elections: ${INSERTED:-0}, Candidacies: ${CANDS:-0}, New candidates: ${CREATED:-0}" | tee -a "$LOGFILE"

    TOTAL_ELECTIONS=$((TOTAL_ELECTIONS + ${INSERTED:-0}))
    TOTAL_CANDIDACIES=$((TOTAL_CANDIDACIES + ${CANDS:-0}))
    TOTAL_CANDIDATES=$((TOTAL_CANDIDATES + ${CREATED:-0}))

    # Mark as done
    touch "/tmp/district_history/${STATE}.done"
done

echo "" | tee -a "$LOGFILE"
echo "═══════════════════════════════════════════════" | tee -a "$LOGFILE"
echo "BATCH COMPLETE — $(date)" | tee -a "$LOGFILE"
echo "Total elections: $TOTAL_ELECTIONS" | tee -a "$LOGFILE"
echo "Total candidacies: $TOTAL_CANDIDACIES" | tee -a "$LOGFILE"
echo "Total new candidates: $TOTAL_CANDIDATES" | tee -a "$LOGFILE"
if [[ ${#FAILED_STATES[@]} -gt 0 ]]; then
    echo "Failed states: ${FAILED_STATES[*]}" | tee -a "$LOGFILE"
fi
echo "═══════════════════════════════════════════════" | tee -a "$LOGFILE"
