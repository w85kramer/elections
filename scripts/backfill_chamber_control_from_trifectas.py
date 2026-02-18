#!/usr/bin/env python3
"""
Backfill historical chamber_control records (1992-2020) from trifectas.legislature_status.

Logic:
  - legislature_status = 'Republican' → both chambers are R
  - legislature_status = 'Democrat'   → both chambers are D
  - legislature_status = 'Split'      → needs research (which chamber is which?)
  - legislature_status = NULL (NE)    → NE is nonpartisan unicameral

For unified (D/R) states, we insert chamber_control rows for both chambers.
For split states, we output a research report.

No seat counts are available from this source — only control_status.
Effective dates use Jan 1 of each year (same convention as existing data).

Skips years that already have chamber_control data (2021, 2023, 2025).

Usage:
    python3 scripts/backfill_chamber_control_from_trifectas.py [--dry-run]
"""

import sys
import time
import requests

sys.path.insert(0, __import__('os').path.join(
    __import__('os').path.dirname(__import__('os').path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

DRY_RUN = '--dry-run' in sys.argv

HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'Content-Type': 'application/json'
}


def run_sql(query, attempt=1):
    resp = requests.post(API_URL, headers=HEADERS, json={'query': query})
    if resp.status_code == 429 and attempt < 5:
        wait = 5 * attempt
        print(f'  Rate limited, waiting {wait}s...')
        time.sleep(wait)
        return run_sql(query, attempt + 1)
    if resp.status_code not in (200, 201):
        print(f"  SQL error ({resp.status_code}): {resp.text[:300]}")
        return None
    return resp.json()


def esc(val):
    """Escape a string for SQL."""
    if val is None:
        return 'NULL'
    return "'" + str(val).replace("'", "''") + "'"


# Chamber names per state (from existing 2025 chamber_control records)
CHAMBER_MAP = {
    'CA': ('Senate', 'Assembly'),
    'MD': ('Senate', 'House of Delegates'),
    'NE': ('Legislature',),  # unicameral
    'NJ': ('Senate', 'Assembly'),
    'NV': ('Senate', 'Assembly'),
    'NY': ('Senate', 'Assembly'),
    'VA': ('Senate', 'House of Delegates'),
    'WI': ('Senate', 'Assembly'),
    'WV': ('Senate', 'House of Delegates'),
}
DEFAULT_CHAMBERS = ('Senate', 'House')


def get_chambers(state_abbr):
    """Return tuple of chamber names for a state."""
    return CHAMBER_MAP.get(state_abbr, DEFAULT_CHAMBERS)


def main():
    # Step 1: Get state abbreviation → id mapping
    print("Fetching state IDs...")
    states = run_sql("SELECT id, abbreviation FROM states ORDER BY abbreviation")
    state_map = {r['abbreviation']: r['id'] for r in states}
    print(f"  {len(state_map)} states")

    # Step 2: Get existing chamber_control dates to skip
    print("Checking existing chamber_control dates...")
    existing = run_sql("""
        SELECT DISTINCT effective_date FROM chamber_control ORDER BY effective_date
    """)
    existing_dates = set(r['effective_date'] for r in existing)
    print(f"  Existing dates: {sorted(existing_dates)}")

    # Step 3: Get all trifecta records with legislature_status
    print("Fetching trifecta legislature_status data...")
    trifectas = run_sql("""
        SELECT s.abbreviation as state, t.year, t.legislature_status
        FROM trifectas t
        JOIN states s ON s.id = t.state_id
        WHERE t.year >= 1992 AND t.year <= 2020
        ORDER BY t.year, s.abbreviation
    """)
    print(f"  {len(trifectas)} trifecta rows (1992-2020)")

    # Step 4: Build chamber_control inserts and research list
    inserts = []
    research_needed = []
    skipped_existing = 0
    skipped_null = 0

    for row in trifectas:
        state = row['state']
        year = row['year']
        leg_status = row['legislature_status']
        effective_date = f"{year}-01-01"

        # Skip years we already have
        if effective_date in existing_dates:
            skipped_existing += 1
            continue

        state_id = state_map.get(state)
        if not state_id:
            print(f"  WARNING: no state_id for {state}")
            continue

        # NE is nonpartisan unicameral — legislature_status is NULL
        if leg_status is None:
            if state == 'NE':
                chambers = get_chambers(state)
                for chamber in chambers:
                    inserts.append(
                        f"({state_id}, {esc(chamber)}, '{effective_date}', 'Nonpartisan', "
                        f"0, 0, 0, 0, 0, 0)"
                    )
            else:
                skipped_null += 1
            continue

        chambers = get_chambers(state)

        if leg_status == 'Republican':
            for chamber in chambers:
                inserts.append(
                    f"({state_id}, {esc(chamber)}, '{effective_date}', 'R', "
                    f"0, 0, 0, 0, 0, 0)"
                )
        elif leg_status == 'Democrat':
            for chamber in chambers:
                inserts.append(
                    f"({state_id}, {esc(chamber)}, '{effective_date}', 'D', "
                    f"0, 0, 0, 0, 0, 0)"
                )
        elif leg_status == 'Split':
            research_needed.append({
                'state': state,
                'year': year,
            })
        else:
            print(f"  WARNING: unknown legislature_status '{leg_status}' for {state} {year}")

    print(f"\n--- Summary ---")
    print(f"  Unified inserts ready: {len(inserts)} chamber_control rows")
    print(f"  Split states needing research: {len(research_needed)} state-years")
    print(f"  Skipped (already in DB): {skipped_existing}")
    print(f"  Skipped (null, non-NE): {skipped_null}")

    # Print research report
    if research_needed:
        print(f"\n--- Split Legislature Research Needed ---")
        print(f"  These state-years have split legislatures. We need to determine")
        print(f"  which chamber was D and which was R.\n")

        # Group by year for readability
        by_year = {}
        for r in research_needed:
            by_year.setdefault(r['year'], []).append(r['state'])
        for year in sorted(by_year.keys()):
            states_list = ', '.join(sorted(by_year[year]))
            print(f"  {year} ({len(by_year[year])} states): {states_list}")

        print(f"\n  Total: {len(research_needed)} state-years across {len(by_year)} years")

    # Step 5: Insert
    if not inserts:
        print("\nNothing to insert.")
        return

    if DRY_RUN:
        print(f"\n[DRY RUN] Would insert {len(inserts)} rows. Exiting.")
        return

    print(f"\nInserting {len(inserts)} chamber_control rows...")
    inserted = 0
    batch_size = 50
    for i in range(0, len(inserts), batch_size):
        batch = inserts[i:i + batch_size]
        sql = f"""INSERT INTO chamber_control
            (state_id, chamber, effective_date, control_status,
             d_seats, r_seats, other_seats, vacant_seats, total_seats, majority_threshold)
        VALUES {', '.join(batch)}
        ON CONFLICT (state_id, chamber, effective_date) DO UPDATE SET
            control_status = EXCLUDED.control_status"""

        result = run_sql(sql)
        if result is not None:
            inserted += len(batch)
            print(f"  Batch {i // batch_size + 1}: {len(batch)} rows")
        else:
            print(f"  FAILED batch {i // batch_size + 1}")
        time.sleep(0.5)

    print(f"\nDone! Inserted {inserted} chamber_control rows.")

    # Verification
    result = run_sql("""
        SELECT effective_date, COUNT(*) as chambers,
            SUM(CASE WHEN control_status = 'D' THEN 1 ELSE 0 END) as d_chambers,
            SUM(CASE WHEN control_status = 'R' THEN 1 ELSE 0 END) as r_chambers,
            SUM(CASE WHEN control_status NOT IN ('D','R') THEN 1 ELSE 0 END) as other
        FROM chamber_control
        GROUP BY effective_date
        ORDER BY effective_date
    """)
    if result:
        print("\nAll chamber_control snapshots:")
        for r in result:
            date = r['effective_date']
            print(f"  {date}: {r['chambers']} chambers — "
                  f"D:{r['d_chambers']} R:{r['r_chambers']} Other:{r['other']}")


if __name__ == '__main__':
    main()
