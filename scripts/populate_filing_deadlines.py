"""
Populate elections.filing_deadline for all 2026 regular elections.

Sets the candidate filing deadline date on every 2026 General, Primary_D,
Primary_R, Primary, and Primary_Nonpartisan election. Does NOT touch special
elections.

Usage:
    python3 scripts/populate_filing_deadlines.py
    python3 scripts/populate_filing_deadlines.py --dry-run
    python3 scripts/populate_filing_deadlines.py --state TX
"""
import sys
import time
import argparse

import httpx

TOKEN = 'sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7'
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'


def run_sql(query, exit_on_error=True, retries=5):
    for attempt in range(retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'\n  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None


# Filing deadline for each state (candidate filing deadline for state-level races)
STATE_DEADLINES = {
    'AL': '2026-01-23',
    'AK': '2026-06-01',
    'AZ': '2026-03-23',
    'AR': '2025-11-12',
    'CA': '2026-03-06',
    'CO': '2026-03-18',
    'CT': '2026-06-09',
    'DE': '2026-07-14',
    'FL': '2026-06-12',
    'GA': '2026-03-06',
    'HI': '2026-06-02',
    'ID': '2026-02-27',
    'IL': '2025-11-03',
    'IN': '2026-02-06',
    'IA': '2026-03-13',
    'KS': '2026-06-01',
    'KY': '2026-01-09',
    'LA': '2026-02-13',
    'ME': '2026-03-16',
    'MD': '2026-02-24',
    'MA': '2026-06-02',
    'MI': '2026-04-21',
    'MN': '2026-06-02',
    'MS': '2025-12-26',
    'MO': '2026-03-31',
    'MT': '2026-03-04',
    'NE': '2026-03-02',
    'NV': '2026-03-13',
    'NH': '2026-06-12',
    'NJ': '2026-03-23',
    'NM': '2026-02-03',
    'NY': '2026-04-06',
    'NC': '2025-12-19',
    'ND': '2026-04-06',
    'OH': '2026-02-04',
    'OK': '2026-04-03',
    'OR': '2026-03-10',
    'PA': '2026-03-10',
    'RI': '2026-06-24',
    'SC': '2026-03-30',
    'SD': '2026-03-31',
    'TN': '2026-03-10',
    'TX': '2025-12-08',
    'UT': '2026-01-08',
    'VT': '2026-05-28',
    'VA': '2026-04-02',
    'WA': '2026-05-08',
    'WV': '2026-01-31',
    'WI': '2026-06-01',
    'WY': '2026-05-29',
}


def main():
    parser = argparse.ArgumentParser(description='Populate filing deadlines on 2026 elections')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without writing')
    parser.add_argument('--state', type=str, help='Only process a single state (2-letter abbreviation)')
    args = parser.parse_args()

    states_to_process = STATE_DEADLINES
    if args.state:
        abbr = args.state.upper()
        if abbr not in STATE_DEADLINES:
            print(f"ERROR: Unknown state '{abbr}'")
            sys.exit(1)
        states_to_process = {abbr: STATE_DEADLINES[abbr]}

    print(f"{'DRY RUN: ' if args.dry_run else ''}Populating filing deadlines for {len(states_to_process)} states")
    print()

    total_updated = 0

    for abbr in sorted(states_to_process):
        deadline = states_to_process[abbr]

        if args.dry_run:
            # Count how many elections would be updated
            count_sql = f"""
                SELECT COUNT(*) as cnt
                FROM elections e
                JOIN seats s ON e.seat_id = s.id
                JOIN districts d ON s.district_id = d.id
                JOIN states st ON d.state_id = st.id
                WHERE e.election_year = 2026
                  AND e.election_type IN ('General','Primary_D','Primary_R','Primary','Primary_Nonpartisan')
                  AND st.abbreviation = '{abbr}'
            """
            rows = run_sql(count_sql)
            count = rows[0]['cnt'] if rows else 0
            print(f"  {abbr}: would set filing_deadline = {deadline} on {count} elections")
            total_updated += count
        else:
            update_sql = f"""
                UPDATE elections e
                SET filing_deadline = '{deadline}'
                FROM seats s
                JOIN districts d ON s.district_id = d.id
                JOIN states st ON d.state_id = st.id
                WHERE e.seat_id = s.id
                  AND e.election_year = 2026
                  AND e.election_type IN ('General','Primary_D','Primary_R','Primary','Primary_Nonpartisan')
                  AND st.abbreviation = '{abbr}'
            """
            result = run_sql(update_sql)
            # Result is empty list on successful UPDATE
            print(f"  {abbr}: set filing_deadline = {deadline}")
            total_updated += 1

    print(f"\n{'Would update' if args.dry_run else 'Updated'} {total_updated} {'elections' if args.dry_run else 'states'}")

    # Verification
    if not args.dry_run:
        print("\n--- Verification ---")

        # Count elections with filing_deadline set
        rows = run_sql("""
            SELECT COUNT(*) as cnt FROM elections WHERE filing_deadline IS NOT NULL
        """)
        print(f"Elections with filing_deadline: {rows[0]['cnt']}")

        # By state
        rows = run_sql("""
            SELECT st.abbreviation, e.filing_deadline, COUNT(*) as cnt
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            WHERE e.election_year = 2026 AND e.filing_deadline IS NOT NULL
            GROUP BY st.abbreviation, e.filing_deadline
            ORDER BY e.filing_deadline, st.abbreviation
        """)
        print(f"\nFiling deadlines by state ({len(rows)} state entries):")
        for r in rows:
            print(f"  {r['abbreviation']}: {r['filing_deadline']} ({r['cnt']} elections)")

        # States already past deadline
        rows = run_sql("""
            SELECT st.abbreviation, e.filing_deadline
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            JOIN districts d ON s.district_id = d.id
            JOIN states st ON d.state_id = st.id
            WHERE e.election_year = 2026 AND e.filing_deadline < CURRENT_DATE
            GROUP BY st.abbreviation, e.filing_deadline
            ORDER BY e.filing_deadline
        """)
        print(f"\nStates with filing already closed ({len(rows)}):")
        for r in rows:
            print(f"  {r['abbreviation']}: {r['filing_deadline']}")

        # Confirm specials not touched
        rows = run_sql("""
            SELECT COUNT(*) as cnt FROM elections
            WHERE election_year = 2026 AND election_type LIKE 'Special%' AND filing_deadline IS NOT NULL
        """)
        print(f"\nSpecial elections with filing_deadline (should be 0): {rows[0]['cnt']}")


if __name__ == '__main__':
    main()
