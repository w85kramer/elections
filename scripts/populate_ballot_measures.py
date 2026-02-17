"""
Populate ballot_measures table from parsed JSON data.

Reads /tmp/ballot_measures_parsed.json (from download_ballot_measures.py) and
/tmp/ballot_measures_descriptions.json (AI-generated descriptions), then inserts
into the ballot_measures table via Supabase Management API.

Usage:
    python3 scripts/populate_ballot_measures.py --dry-run
    python3 scripts/populate_ballot_measures.py
    python3 scripts/populate_ballot_measures.py --year 2024
"""
import sys
import re
import json
import argparse
from datetime import datetime

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

BATCH_SIZE = 50

# Manual result overrides for measures with missing BP result icons
RESULT_OVERRIDES = {
    'https://ballotpedia.org/California_Proposition_15,_Tax_on_Commercial_and_Industrial_Properties_for_Education_and_Local_Government_Funding_Initiative_(2020)': 'Failed',
    'https://ballotpedia.org/Colorado_Proposition_113,_National_Popular_Vote_Interstate_Compact_Referendum_(2020)': 'Passed',
}

def run_sql(query, exit_on_error=True):
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': query},
        timeout=120
    )
    if resp.status_code != 201:
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    return resp.json()

def esc(s):
    if s is None:
        return None
    return str(s).replace("'", "''")

def parse_date(date_str):
    """Convert 'November 5, 2024' to '2024-11-05'."""
    try:
        dt = datetime.strptime(date_str, '%B %d, %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        try:
            dt = datetime.strptime(date_str, '%B %d,%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            print(f'  WARNING: Could not parse date: {date_str}')
            return None

def sql_val(v):
    """Convert a Python value to SQL literal."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (int, float)):
        return str(v)
    return f"'{esc(v)}'"

def main():
    parser = argparse.ArgumentParser(description='Populate ballot_measures table')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be inserted without modifying DB')
    parser.add_argument('--year', type=int, choices=list(range(2020, 2027)),
                        help='Only process a single year')
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN MODE — no database changes will be made.\n")

    # Load parsed data
    print("Loading parsed data...")
    with open('/tmp/ballot_measures_parsed.json') as f:
        all_measures = json.load(f)
    print(f"  Loaded {len(all_measures)} measures from parsed JSON")

    # Load AI descriptions (optional)
    descriptions = {}
    try:
        with open('/tmp/ballot_measures_descriptions.json') as f:
            descriptions = json.load(f)
        print(f"  Loaded {len(descriptions)} AI descriptions")
    except FileNotFoundError:
        print("  No AI descriptions file found — using BP descriptions as fallback")

    # Filter by year if specified
    if args.year:
        all_measures = [m for m in all_measures if m['year'] == args.year]
        print(f"  Filtered to {len(all_measures)} measures for {args.year}")

    # Load state map
    print("Loading state map...")
    states = run_sql("SELECT id, state_name FROM states ORDER BY state_name")
    state_map = {s['state_name']: s['id'] for s in states}
    print(f"  {len(state_map)} states loaded")

    # Check for existing data (idempotency)
    print("Checking existing ballot measures...")
    existing = run_sql("""
        SELECT election_year, COUNT(*) as cnt
        FROM ballot_measures GROUP BY election_year ORDER BY election_year
    """)
    existing_years = {r['election_year']: r['cnt'] for r in existing}
    print(f"  Existing: {existing_years}")

    # Group measures by year
    by_year = {}
    for m in all_measures:
        by_year.setdefault(m['year'], []).append(m)

    total_inserted = 0
    total_skipped = 0

    for year in sorted(by_year.keys()):
        measures = by_year[year]

        if year in existing_years and existing_years[year] > 0:
            print(f"\n  Year {year}: already has {existing_years[year]} measures — skipping")
            total_skipped += existing_years[year]
            continue

        print(f"\n{'=' * 60}")
        print(f"INSERTING: {year} ({len(measures)} measures)")
        print(f"{'=' * 60}")

        values = []
        for m in measures:
            state_id = state_map.get(m['state'])
            if not state_id:
                print(f"  WARNING: Unknown state '{m['state']}' — skipping")
                continue

            election_date = parse_date(m['election_date'])

            # Manual overrides for measures with missing BP result icons
            bp_result = m['result']
            if bp_result is None:
                bp_result = RESULT_OVERRIDES.get(m.get('measure_url'))

            # Determine status and result
            if bp_result == 'Passed':
                status = 'Passed'
                result = 'Passed'
            elif bp_result == 'Failed':
                status = 'Failed'
                result = 'Failed'
            else:
                status = 'On Ballot'
                result = 'Pending'

            # Get AI description, fall back to BP description
            desc_key = m['measure_url']
            description = descriptions.get(desc_key, m.get('bp_description'))

            # Compute yes_percentage
            yes_pct = None
            if m.get('yes_pct') is not None:
                yes_pct = m['yes_pct']

            vals = (
                f"({state_id}, "
                f"{sql_val(election_date)}, "
                f"{year}, "
                f"{sql_val(m.get('measure_type'))}, "
                f"{sql_val(m['measure_number'])}, "
                f"{sql_val(m['short_title'])}, "
                f"{sql_val(description)}, "
                f"{sql_val(m.get('subject_category'))}, "
                f"{sql_val(m.get('sponsor_type'))}, "
                f"NULL, "  # placed_by
                f"NULL, "  # signature_threshold
                f"NULL, "  # signatures_submitted
                f"NULL, "  # qualified_date
                f"{sql_val(status)}, "
                f"{sql_val(m.get('votes_yes'))}, "
                f"{sql_val(m.get('votes_no'))}, "
                f"{sql_val(yes_pct)}, "
                f"{sql_val(result)}, "
                f"NULL, "  # passage_threshold
                f"NULL, "  # fiscal_impact_estimate
                f"NULL, "  # key_supporters
                f"NULL, "  # key_opponents
                f"NULL, "  # forecast_rating
                f"NULL)"   # notes
            )
            values.append(vals)

        print(f"  Prepared {len(values)} INSERT values")

        if args.dry_run:
            # Show samples
            for v in values[:3]:
                print(f"  SAMPLE: {v[:200]}...")
            total_inserted += len(values)
            continue

        # Batch INSERT
        year_inserted = 0
        for batch_start in range(0, len(values), BATCH_SIZE):
            batch = values[batch_start:batch_start + BATCH_SIZE]
            sql = (
                "INSERT INTO ballot_measures "
                "(state_id, election_date, election_year, measure_type, "
                "measure_number, short_title, description, subject_category, "
                "sponsor_type, placed_by, signature_threshold, signatures_submitted, "
                "qualified_date, status, votes_yes, votes_no, yes_percentage, "
                "result, passage_threshold, fiscal_impact_estimate, "
                "key_supporters, key_opponents, forecast_rating, notes) VALUES\n"
                + ",\n".join(batch)
                + "\nRETURNING id;"
            )
            result = run_sql(sql, exit_on_error=False)
            if result is None:
                print(f"  ERROR: Batch {batch_start}-{batch_start+len(batch)} failed!")
                sys.exit(1)
            year_inserted += len(result)
            print(f"  Batch: inserted {len(result)} (total: {year_inserted})")

        print(f"  {year}: Inserted {year_inserted} measures")
        total_inserted += year_inserted

    # Verify
    print(f"\n{'=' * 60}")
    print(f"VERIFICATION")
    print(f"{'=' * 60}")

    if not args.dry_run:
        verify = run_sql("""
            SELECT election_year, COUNT(*) as cnt,
                   SUM(CASE WHEN result='Passed' THEN 1 ELSE 0 END) as passed,
                   SUM(CASE WHEN result='Failed' THEN 1 ELSE 0 END) as failed,
                   SUM(CASE WHEN result='Pending' THEN 1 ELSE 0 END) as pending
            FROM ballot_measures GROUP BY election_year ORDER BY election_year
        """)
        for r in verify:
            print(f"  {r['election_year']}: {r['cnt']} total "
                  f"(passed: {r['passed']}, failed: {r['failed']}, pending: {r['pending']})")

        # By state sample
        by_state = run_sql("""
            SELECT s.abbreviation, bm.election_year, COUNT(*) as cnt
            FROM ballot_measures bm
            JOIN states s ON bm.state_id = s.id
            GROUP BY s.abbreviation, bm.election_year
            ORDER BY s.abbreviation, bm.election_year
        """)
        print(f"\n  Measures by state:")
        for r in by_state:
            print(f"    {r['abbreviation']} {r['election_year']}: {r['cnt']}")

        # Spot checks
        spots = run_sql("""
            SELECT s.abbreviation, bm.election_year, bm.measure_number,
                   bm.measure_type, bm.result, bm.votes_yes, bm.status
            FROM ballot_measures bm
            JOIN states s ON bm.state_id = s.id
            ORDER BY RANDOM() LIMIT 8
        """)
        print(f"\n  Spot checks:")
        for r in spots:
            print(f"    {r['abbreviation']} {r['election_year']}: {r['measure_number']} "
                  f"[{r['measure_type']}] — {r['result']} "
                  f"(votes_yes: {r['votes_yes']}, status: {r['status']})")

    print(f"\n  Total inserted: {total_inserted}")
    if total_skipped:
        print(f"  Total skipped (existing): {total_skipped}")
    print("\nDone!")

if __name__ == '__main__':
    main()
