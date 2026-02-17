#!/usr/bin/env python3
"""
Populate chamber_control table with current governance status for all 99
state legislative chambers.

Derives control_status from seat_terms/seats caucus data. Presiding officers
are populated separately (Phase 3).

Usage:
  python3 scripts/populate_chamber_control.py --dry-run
  python3 scripts/populate_chamber_control.py
"""

import argparse
import requests
import time
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

SUPABASE_URL = "https://api.supabase.com/v1/projects/pikcvwulzfxgwfcfssxc/database/query"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# Special-case control overrides (state abbr, chamber) -> (control_status, notes, coalition_desc)
SPECIAL_CASES = {
    ("AK", "House"): (
        "Coalition",
        "Bipartisan majority coalition controls chamber despite R plurality",
        "14D + 5I + 2R bipartisan coalition",
    ),
    ("AK", "Senate"): (
        "Coalition",
        "Bipartisan majority coalition controls chamber",
        "9D + 8R bipartisan coalition",
    ),
    ("MN", "House"): (
        "Power_Sharing",
        "67-67 D/R tie; bipartisan power-sharing agreement",
        None,
    ),
    ("NE", "Legislature"): (
        "R",
        "Officially nonpartisan unicameral legislature; R majority controls",
        None,
    ),
    ("MI", "Senate"): (
        "D",
        "D has 19 of 38 seats (1 vacant); D controls via plurality",
        None,
    ),
    ("PA", "House"): (
        "D",
        "D has 100 of 203 seats (5 vacant); D controls via election-night majority",
        None,
    ),
}

def run_query(sql, retries=5):
    for attempt in range(1, retries + 1):
        resp = requests.post(SUPABASE_URL, headers=HEADERS, json={"query": sql})
        if resp.status_code == 429:
            wait = 5 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "message" in data:
            raise RuntimeError(data["message"])
        return data
    raise RuntimeError("Max retries exceeded")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Check for existing data
    existing = run_query("SELECT COUNT(*) as cnt FROM chamber_control;")
    if existing[0]["cnt"] > 0:
        print(f"WARNING: {existing[0]['cnt']} chamber_control rows already exist!")
        if not args.dry_run:
            print("Delete existing rows first or use --dry-run.")
            return

    # Get state IDs
    states = run_query(
        "SELECT id, abbreviation, state_name FROM states ORDER BY abbreviation;"
    )
    state_map = {s["abbreviation"]: s for s in states}
    print(f"Found {len(state_map)} states")

    # Get seat counts by state, chamber, caucus
    seat_data = run_query("""
        SELECT st.abbreviation, d.chamber,
               s.current_holder_caucus as caucus,
               COUNT(*) as cnt
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE d.office_level = 'Legislative'
        GROUP BY st.abbreviation, d.chamber, s.current_holder_caucus
        ORDER BY st.abbreviation, d.chamber;
    """)

    # Aggregate into chamber-level counts
    chambers = {}
    for r in seat_data:
        key = (r["abbreviation"], r["chamber"])
        if key not in chambers:
            chambers[key] = {"d": 0, "r": 0, "other": 0, "vacant": 0, "total": 0}
        c = r["caucus"]
        cnt = r["cnt"]
        chambers[key]["total"] += cnt
        if c == "D":
            chambers[key]["d"] += cnt
        elif c == "R":
            chambers[key]["r"] += cnt
        elif c is None:
            chambers[key]["vacant"] += cnt
        else:
            chambers[key]["other"] += cnt

    print(f"Found {len(chambers)} legislative chambers")

    # Build INSERT values
    values = []
    control_summary = {}

    for (abbr, chamber), counts in sorted(chambers.items()):
        state_id = state_map[abbr]["id"]
        total = counts["total"]
        d = counts["d"]
        r = counts["r"]
        other = counts["other"]
        vacant = counts["vacant"]
        majority = total // 2 + 1

        # Determine control status
        if (abbr, chamber) in SPECIAL_CASES:
            control_status, notes, coalition_desc = SPECIAL_CASES[(abbr, chamber)]
        else:
            coalition_desc = None
            notes = None
            if d >= majority:
                control_status = "D"
            elif r >= majority:
                control_status = "R"
            else:
                # Should not happen — all special cases covered above
                control_status = "Tied"
                notes = f"Unexpected: D={d}, R={r}, Other={other}, Vacant={vacant}"
                print(f"  WARNING: Unexpected non-majority for {abbr} {chamber}")

        control_summary[control_status] = control_summary.get(control_status, 0) + 1

        # Escape strings
        notes_sql = f"'{notes}'" if notes else "NULL"
        notes_sql = notes_sql.replace("'", "''").replace("''", "'", 1)  # fix double-escape on first quote
        if notes:
            notes_sql = f"'{notes.replace(chr(39), chr(39)+chr(39))}'"
        else:
            notes_sql = "NULL"
        coalition_sql = f"'{coalition_desc}'" if coalition_desc else "NULL"

        values.append(
            f"({state_id}, '{chamber}', '2025-01-01', '{control_status}', "
            f"{d}, {r}, {other}, {vacant}, {total}, {majority}, "
            f"NULL, NULL, NULL, "  # presiding officer fields — Phase 3
            f"{coalition_sql}, {notes_sql})"
        )

    # Print summary
    print(f"\nControl distribution:")
    for status in sorted(control_summary.keys()):
        print(f"  {status}: {control_summary[status]}")
    print(f"  Total: {sum(control_summary.values())}")

    # Print special cases
    print(f"\nSpecial cases:")
    for (abbr, chamber), (status, notes, cdesc) in sorted(SPECIAL_CASES.items()):
        counts = chambers.get((abbr, chamber), {})
        print(f"  {abbr} {chamber}: {status} — {notes}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would insert {len(values)} chamber_control rows.")
        return

    # Insert all rows
    insert_sql = (
        "INSERT INTO chamber_control "
        "(state_id, chamber, effective_date, control_status, "
        "d_seats, r_seats, other_seats, vacant_seats, total_seats, majority_threshold, "
        "presiding_officer, presiding_officer_title, presiding_officer_party, "
        "coalition_desc, notes) VALUES "
        + ", ".join(values) + ";"
    )
    run_query(insert_sql)
    print(f"\nInserted {len(values)} chamber_control rows")

    # Verify
    verify = run_query(
        "SELECT control_status, COUNT(*) as cnt "
        "FROM chamber_control GROUP BY control_status ORDER BY control_status;"
    )
    print("\nVerification — control_status distribution:")
    for r in verify:
        print(f"  {r['control_status']}: {r['cnt']}")

    total = run_query("SELECT COUNT(*) as cnt FROM chamber_control;")
    print(f"\nTotal rows: {total[0]['cnt']}")

if __name__ == "__main__":
    main()
