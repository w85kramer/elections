#!/usr/bin/env python3
"""
Populate forecasts table with 2026 gubernatorial race ratings from
Cook Political Report and Sabato's Crystal Ball.

Also sets elections.forecast_rating to a consensus rating.

Rating scale (our system):
  Solid (>99%), Very Likely (90-99%), Likely (75-90%),
  Lean (60-75%), Tilt (50.1-60%), Toss-up (50%)

Usage:
  python3 scripts/populate_forecasts_gov.py --dry-run
  python3 scripts/populate_forecasts_gov.py
"""

import argparse
import requests

SUPABASE_URL = "https://api.supabase.com/v1/projects/pikcvwulzfxgwfcfssxc/database/query"
TOKEN = "sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# Numeric scale for averaging
SCALE = {
    "Solid": 6,
    "Very Likely": 5,
    "Likely": 4,
    "Lean": 3,
    "Tilt": 2,
    "Toss-up": 1,
}
REVERSE_SCALE = {v: k for k, v in SCALE.items()}

# ── Cook Political Report ratings (direct from cookpolitical.com, Feb 2026) ──
# Cook uses: Solid, Likely, Lean, Toss Up (same labels as our scale)
COOK = {
    "AL": "Solid R",    "AK": "Solid R",    "AZ": "Toss-up",
    "AR": "Solid R",    "CA": "Solid D",    "CO": "Solid D",
    "CT": "Solid D",    "FL": "Solid R",    "GA": "Toss-up",
    "HI": "Solid D",    "ID": "Solid R",    "IL": "Solid D",
    "IA": "Lean R",     "KS": "Lean R",     "ME": "Likely D",
    "MD": "Solid D",    "MA": "Solid D",    "MI": "Toss-up",
    "MN": "Likely D",   "NE": "Solid R",    "NV": "Toss-up",
    "NH": "Likely R",   "NM": "Likely D",   "NY": "Solid D",
    "OH": "Likely R",   "OK": "Solid R",    "OR": "Solid D",
    "PA": "Likely D",   "RI": "Solid D",    "SC": "Solid R",
    "SD": "Solid R",    "TN": "Solid R",    "TX": "Solid R",
    "VT": "Solid R",    "WI": "Toss-up",   "WY": "Solid R",
}

# ── Sabato's Crystal Ball ratings (user-transcribed, Feb 2026) ──
CRYSTAL_BALL = {
    "AL": "Solid R",    "AK": "Likely R",   "AZ": "Toss-up",
    "AR": "Solid R",    "CA": "Solid D",    "CO": "Solid D",
    "CT": "Solid D",    "FL": "Solid R",    "GA": "Lean R",
    "HI": "Solid D",    "ID": "Solid R",    "IL": "Solid D",
    "IA": "Lean R",     "KS": "Lean R",     "ME": "Lean D",
    "MD": "Solid D",    "MA": "Solid D",    "MI": "Toss-up",
    "MN": "Likely D",   "NE": "Solid R",    "NV": "Lean R",
    "NH": "Likely R",   "NM": "Likely D",   "NY": "Likely D",
    "OH": "Likely R",   "OK": "Solid R",    "OR": "Likely D",
    "PA": "Likely D",   "RI": "Likely D",   "SC": "Solid R",
    "SD": "Solid R",    "TN": "Solid R",    "TX": "Solid R",
    "VT": "Likely R",   "WI": "Toss-up",   "WY": "Solid R",
}

DATE_COOK = "2026-02-09"
DATE_CB = "2026-02-09"


def run_query(sql):
    resp = requests.post(SUPABASE_URL, headers=HEADERS, json={"query": sql})
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "message" in data:
        raise RuntimeError(data["message"])
    return data


def parse_rating(rating_str):
    """Parse 'Solid R' → ('Solid', 'R'), 'Toss-up' → ('Toss-up', None)"""
    if rating_str == "Toss-up":
        return "Toss-up", None
    parts = rating_str.rsplit(" ", 1)
    return parts[0], parts[1]


def consensus_rating(cook_str, cb_str):
    """Average two ratings to produce a consensus."""
    cook_label, cook_party = parse_rating(cook_str)
    cb_label, cb_party = parse_rating(cb_str)

    cook_num = SCALE[cook_label]
    cb_num = SCALE[cb_label]
    avg = (cook_num + cb_num) / 2

    # Round to nearest integer on scale
    consensus_num = round(avg)
    consensus_label = REVERSE_SCALE[consensus_num]

    # Determine party direction
    if cook_party and cb_party:
        # Both agree on party
        party = cook_party
    elif cook_party:
        party = cook_party
    elif cb_party:
        party = cb_party
    else:
        party = None

    # If consensus rounds to Toss-up level but one source has a lean, use Tilt
    if consensus_num == 1 and (cook_num > 1 or cb_num > 1):
        # One says Toss-up, other says something higher — Tilt
        consensus_label = "Tilt"
        consensus_num = 2
        # Party comes from the source that's not Toss-up
        if cook_party:
            party = cook_party
        elif cb_party:
            party = cb_party

    if consensus_label == "Toss-up":
        return "Toss-up"
    return f"{consensus_label} {party}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Verify both sources have the same 36 states
    assert set(COOK.keys()) == set(CRYSTAL_BALL.keys()), "State mismatch between sources"
    states = sorted(COOK.keys())
    print(f"Processing {len(states)} gubernatorial races")

    # Check for existing forecasts
    existing = run_query(
        "SELECT COUNT(*) as cnt FROM forecasts f "
        "JOIN elections e ON f.election_id = e.id "
        "JOIN seats s ON e.seat_id = s.id "
        "WHERE s.office_type = 'Governor';"
    )
    existing_count = existing[0]["cnt"]
    if existing_count > 0:
        print(f"WARNING: {existing_count} governor forecasts already exist!")
        if not args.dry_run:
            print("Use --dry-run first or delete existing forecasts before re-running.")
            return

    # Get election IDs for Governor generals
    rows = run_query(
        "SELECT e.id as election_id, st.abbreviation "
        "FROM elections e "
        "JOIN seats s ON e.seat_id = s.id "
        "JOIN districts d ON s.district_id = d.id "
        "JOIN states st ON d.state_id = st.id "
        "WHERE s.office_type = 'Governor' "
        "AND e.election_type = 'General' "
        "ORDER BY st.abbreviation;"
    )
    election_map = {r["abbreviation"]: r["election_id"] for r in rows}
    print(f"Found {len(election_map)} governor general elections in DB")

    # Verify all states match
    missing = set(states) - set(election_map.keys())
    if missing:
        print(f"ERROR: Missing elections for: {missing}")
        return

    # Build forecast inserts and consensus updates
    forecast_values = []
    consensus_updates = []
    disagreements = []

    for abbr in states:
        eid = election_map[abbr]
        cook_rating = COOK[abbr]
        cb_rating = CRYSTAL_BALL[abbr]
        cons = consensus_rating(cook_rating, cb_rating)

        # Forecast rows
        cook_escaped = cook_rating.replace("'", "''")
        cb_escaped = cb_rating.replace("'", "''")
        cons_escaped = cons.replace("'", "''")

        forecast_values.append(
            f"({eid}, 'Cook Political Report', '{cook_escaped}', '{DATE_COOK}')"
        )
        forecast_values.append(
            f"({eid}, 'Sabato''s Crystal Ball', '{cb_escaped}', '{DATE_CB}')"
        )

        # Consensus update
        consensus_updates.append(
            f"UPDATE elections SET forecast_rating = '{cons_escaped}' WHERE id = {eid};"
        )

        if cook_rating != cb_rating:
            disagreements.append((abbr, cook_rating, cb_rating, cons))

    # Print summary
    print(f"\nForecasts to insert: {len(forecast_values)} rows ({len(states)} states × 2 sources)")
    print(f"Consensus updates: {len(consensus_updates)}")

    if disagreements:
        print(f"\nDisagreements ({len(disagreements)} states):")
        print(f"  {'State':<6} {'Cook':<12} {'Crystal Ball':<14} {'Consensus':<14}")
        for abbr, cook, cb, cons in disagreements:
            print(f"  {abbr:<6} {cook:<12} {cb:<14} {cons:<14}")

    # Print consensus summary
    consensus_counts = {}
    for abbr in states:
        cons = consensus_rating(COOK[abbr], CRYSTAL_BALL[abbr])
        consensus_counts[cons] = consensus_counts.get(cons, 0) + 1
    print(f"\nConsensus distribution:")
    for rating in sorted(consensus_counts.keys(), key=lambda r: (
        -SCALE.get(r.rsplit(" ", 1)[0] if r != "Toss-up" else "Toss-up", 0),
        r
    )):
        print(f"  {rating}: {consensus_counts[rating]}")

    if args.dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    # Insert forecasts
    insert_sql = (
        "INSERT INTO forecasts (election_id, source, rating, date_of_forecast) VALUES "
        + ", ".join(forecast_values) + ";"
    )
    run_query(insert_sql)
    print(f"\nInserted {len(forecast_values)} forecast rows")

    # Update consensus ratings on elections
    update_sql = " ".join(consensus_updates)
    run_query(update_sql)
    print(f"Updated {len(consensus_updates)} election forecast_rating fields")

    # Update forecast_source
    eid_list = ", ".join(str(election_map[a]) for a in states)
    run_query(
        f"UPDATE elections SET forecast_source = 'Cook/Crystal Ball consensus' "
        f"WHERE id IN ({eid_list});"
    )
    print("Updated forecast_source on all governor elections")

    # Verify
    verify = run_query(
        "SELECT COUNT(*) as cnt FROM forecasts f "
        "JOIN elections e ON f.election_id = e.id "
        "JOIN seats s ON e.seat_id = s.id "
        "WHERE s.office_type = 'Governor';"
    )
    print(f"\nVerification: {verify[0]['cnt']} governor forecasts in DB")

    rated = run_query(
        "SELECT forecast_rating, COUNT(*) as cnt "
        "FROM elections e JOIN seats s ON e.seat_id = s.id "
        "WHERE s.office_type = 'Governor' AND e.election_type = 'General' "
        "AND forecast_rating IS NOT NULL "
        "GROUP BY forecast_rating ORDER BY forecast_rating;"
    )
    print("Elections with forecast_rating set:")
    for r in rated:
        print(f"  {r['forecast_rating']}: {r['cnt']}")


if __name__ == "__main__":
    main()
