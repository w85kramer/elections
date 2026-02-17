"""
Import StateNavigate legislative forecast CSV files into the forecasts table.

Reads CSV files with columns: district, id, incumbent_name, inc_party, pres_24,
leading_party, proj_margin, caucus_margin, win_pct

Converts win_pct probability to our categorical rating scale, matches districts
to 2026 General elections, and inserts forecast rows.

Usage:
    python3 scripts/populate_forecasts_leg.py "map examples/forecast_26_lower.csv" "map examples/forecast_26_upper.csv"
    python3 scripts/populate_forecasts_leg.py --dry-run "map examples/forecast_26_lower.csv"
    python3 scripts/populate_forecasts_leg.py --state WV "map examples/forecast_26_lower.csv"
"""

import argparse
import csv
import sys
import time

import httpx

SUPABASE_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'

SOURCE = 'StateNavigate'
FORECAST_DATE = '2026-02-10'  # SN WV forecast release date

# Rating scale: win_pct probability → categorical label
# These thresholds match our existing governor forecast scale
def win_pct_to_rating(win_pct, leading_party):
    """Convert win probability (0-1) to categorical rating like 'Solid R'."""
    pct = float(win_pct)
    if pct >= 0.99:
        label = 'Solid'
    elif pct >= 0.90:
        label = 'Very Likely'
    elif pct >= 0.75:
        label = 'Likely'
    elif pct >= 0.60:
        label = 'Lean'
    elif pct > 0.50:
        label = 'Tilt'
    else:
        return 'Toss-up'
    return f'{label} {leading_party}'

# Map SN id prefixes to our chamber names
# sldl = state legislative district lower, sldu = upper
CHAMBER_MAP = {
    'sldl': {
        'WV': 'House of Delegates',
        'VA': 'House of Delegates',
        'MD': 'House of Delegates',
        'CA': 'Assembly', 'NV': 'Assembly', 'NY': 'Assembly',
        'WI': 'Assembly', 'NJ': 'Assembly',
        'NE': 'Legislature',
    },
    'sldu': {},  # Almost all states use 'Senate'
}
# Default: sldl→'House', sldu→'Senate'

def get_chamber(id_prefix, state_abbr):
    """Get our DB chamber name from SN id prefix and state."""
    if id_prefix == 'sldl':
        return CHAMBER_MAP['sldl'].get(state_abbr, 'House')
    else:
        return CHAMBER_MAP['sldu'].get(state_abbr, 'Senate')

def parse_sn_id(sn_id):
    """Parse SN id like 'sldl:5' or 'sldu:3A' → (prefix, district_num, seat_designator)."""
    prefix, rest = sn_id.split(':')
    # Senate may have seat designator: '3A' or '3B'
    seat_designator = None
    district_num = rest
    if prefix == 'sldu' and rest and rest[-1].isalpha():
        seat_designator = rest[-1]
        district_num = rest[:-1]
    return prefix, district_num, seat_designator

def run_sql(query, label='', retries=5):
    for attempt in range(retries):
        resp = httpx.post(
            SUPABASE_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited on {label}, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR on {label}: {resp.status_code} - {resp.text[:500]}')
        return None

def esc(s):
    """Escape single quotes for SQL."""
    if s is None:
        return 'NULL'
    return str(s).replace("'", "''")

def main():
    parser = argparse.ArgumentParser(description='Import StateNavigate legislative forecasts')
    parser.add_argument('files', nargs='+', help='CSV file path(s)')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without writing')
    parser.add_argument('--state', type=str, help='Override state detection (2-letter abbreviation)')
    parser.add_argument('--date', type=str, default=FORECAST_DATE, help='Forecast date (YYYY-MM-DD)')
    args = parser.parse_args()

    # Parse all CSV files
    all_rows = []
    for filepath in args.files:
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            print(f"Read {len(rows)} rows from {filepath}")
            all_rows.extend(rows)

    if not all_rows:
        print("No data found in CSV files")
        sys.exit(1)

    # Detect state from first row's id (sldl:1 or sldu:1A)
    # For now we need the state from --state flag or detect from incumbent data
    # SN CSVs don't include state abbreviation, so we need it from the user or context
    if args.state:
        state_abbr = args.state.upper()
    else:
        # Try to detect from file path
        # e.g., 'map examples/forecast_26_lower.csv' doesn't tell us the state
        # For now, we need the --state flag or we try to detect from the DB
        print("WARNING: No --state specified. Attempting to detect from incumbent names...")
        # Query a few incumbent names to find the state
        sample_names = [r['incumbent_name'] for r in all_rows[:5] if r['incumbent_name'] != 'OPEN']
        if sample_names:
            name = esc(sample_names[0])
            detect = run_sql(f"""
                SELECT DISTINCT sta.abbreviation
                FROM seats s
                JOIN districts d ON s.district_id = d.id
                JOIN states sta ON d.state_id = sta.id
                WHERE s.current_holder LIKE '%{name}%'
            """, 'detect state')
            if detect and len(detect) == 1:
                state_abbr = detect[0]['abbreviation']
                print(f"  Detected state: {state_abbr}")
            else:
                print("ERROR: Could not auto-detect state. Use --state flag.")
                sys.exit(1)
        else:
            print("ERROR: No incumbent names to detect state. Use --state flag.")
            sys.exit(1)

    # Categorize rows by chamber
    lower_rows = []
    upper_rows = []
    skipped = 0

    for row in all_rows:
        prefix, district_num, seat_designator = parse_sn_id(row['id'])

        # Skip Senate B seats (not up in 2026 for WV)
        if prefix == 'sldu' and seat_designator == 'B':
            skipped += 1
            continue

        chamber = get_chamber(prefix, state_abbr)
        row['_chamber'] = chamber
        row['_district_num'] = district_num
        row['_seat_designator'] = seat_designator

        if prefix == 'sldl':
            lower_rows.append(row)
        else:
            upper_rows.append(row)

    print(f"\nState: {state_abbr}")
    print(f"Lower chamber rows: {len(lower_rows)}")
    print(f"Upper chamber rows: {len(upper_rows)}")
    print(f"Skipped (not up in 2026): {skipped}")

    all_forecast_rows = lower_rows + upper_rows

    # Get election IDs for matching
    # We need: state + chamber + district_number + election_type='General' + election_year=2026
    chambers = set(r['_chamber'] for r in all_forecast_rows)
    chamber_list = ', '.join(f"'{c}'" for c in chambers)

    elections = run_sql(f"""
        SELECT e.id as election_id, d.district_number, d.chamber,
               s.seat_designator, s.id as seat_id
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = '{state_abbr}'
          AND d.chamber IN ({chamber_list})
          AND e.election_type = 'General'
          AND e.election_year = 2026
        ORDER BY d.chamber, d.district_number
    """, 'get elections')

    if not elections:
        print("ERROR: No matching elections found")
        sys.exit(1)

    # Build lookup: (chamber, district_number, seat_designator) → election_id
    election_map = {}
    for e in elections:
        key = (e['chamber'], e['district_number'], e.get('seat_designator'))
        election_map[key] = e['election_id']

    print(f"Found {len(election_map)} matching 2026 General elections in DB")

    # Check for existing SN forecasts
    existing = run_sql(f"""
        SELECT COUNT(*) as cnt FROM forecasts
        WHERE source = '{SOURCE}'
          AND election_id IN (
              SELECT e.id FROM elections e
              JOIN seats s ON e.seat_id = s.id
              JOIN districts d ON s.district_id = d.id
              JOIN states st ON d.state_id = st.id
              WHERE st.abbreviation = '{state_abbr}'
                AND e.election_type = 'General'
                AND e.election_year = 2026
          )
    """, 'check existing')
    if existing and existing[0]['cnt'] > 0:
        print(f"WARNING: {existing[0]['cnt']} StateNavigate forecasts already exist for {state_abbr}!")
        if not args.dry_run:
            print("Delete existing forecasts first or use --dry-run.")
            return

    # Match and build inserts
    forecast_values = []
    election_updates = []
    matched = 0
    unmatched = []
    open_seats = 0
    competitive = []  # Toss-up, Tilt, Lean races

    for row in all_forecast_rows:
        chamber = row['_chamber']
        district_num = row['_district_num']
        seat_designator = row['_seat_designator']

        # Try matching with seat_designator first, then without
        key = (chamber, district_num, seat_designator)
        election_id = election_map.get(key)
        if election_id is None and seat_designator:
            # Try without designator (some states don't use it)
            key_no_desig = (chamber, district_num, None)
            election_id = election_map.get(key_no_desig)

        if election_id is None:
            unmatched.append(f"{chamber} {district_num}{seat_designator or ''}")
            continue

        matched += 1
        rating = win_pct_to_rating(row['win_pct'], row['leading_party'])
        win_pct = float(row['win_pct'])
        proj_margin = float(row['proj_margin'])
        is_open = row['incumbent_name'] == 'OPEN'
        if is_open:
            open_seats += 1

        # Track competitive races
        if win_pct < 0.90:
            competitive.append({
                'chamber': chamber,
                'district': district_num,
                'designator': seat_designator or '',
                'incumbent': row['incumbent_name'],
                'inc_party': row['inc_party'],
                'leading': row['leading_party'],
                'rating': rating,
                'win_pct': win_pct,
                'proj_margin': proj_margin,
            })

        # Build notes with numeric detail
        notes_parts = [f"win_pct={row['win_pct']}"]
        notes_parts.append(f"proj_margin={row['proj_margin']}")
        if is_open:
            notes_parts.append("OPEN SEAT")
        else:
            notes_parts.append(f"inc={row['incumbent_name']} ({row['inc_party']})")
        notes = '; '.join(notes_parts)

        forecast_values.append(
            f"({election_id}, '{SOURCE}', '{esc(rating)}', '{args.date}', '{esc(notes)}')"
        )
        election_updates.append(
            f"UPDATE elections SET forecast_rating = '{esc(rating)}', "
            f"forecast_source = '{SOURCE}' WHERE id = {election_id};"
        )

    # Print summary
    print(f"\n{'DRY RUN: ' if args.dry_run else ''}=== Summary ===")
    print(f"Matched: {matched}")
    print(f"Unmatched: {len(unmatched)}")
    if unmatched:
        print(f"  Unmatched districts: {', '.join(unmatched[:10])}")
    print(f"Open seats: {open_seats}")
    print(f"Forecasts to insert: {len(forecast_values)}")

    # Rating distribution
    from collections import Counter
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
    ratings = Counter()
    for row in all_forecast_rows:
        key = (row['_chamber'], row['_district_num'], row['_seat_designator'])
        eid = election_map.get(key)
        if eid is None and row['_seat_designator']:
            eid = election_map.get((row['_chamber'], row['_district_num'], None))
        if eid:
            ratings[win_pct_to_rating(row['win_pct'], row['leading_party'])] += 1

    print(f"\nRating distribution:")
    for rating in sorted(ratings.keys()):
        print(f"  {rating}: {ratings[rating]}")

    # Competitive races
    if competitive:
        competitive.sort(key=lambda x: x['win_pct'])
        print(f"\nCompetitive races (< Very Likely, {len(competitive)} total):")
        for c in competitive[:20]:
            inc_label = 'OPEN' if c['incumbent'] == 'OPEN' else f"{c['incumbent']} ({c['inc_party']})"
            print(f"  {c['chamber']} {c['district']}{c['designator']}: "
                  f"{c['rating']} — {inc_label} — win_pct={c['win_pct']:.4f}")
        if len(competitive) > 20:
            print(f"  ... and {len(competitive) - 20} more")

    if args.dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    # Insert forecasts in batch
    if forecast_values:
        # Batch into groups of 50 to avoid query size limits
        batch_size = 50
        total_inserted = 0
        for i in range(0, len(forecast_values), batch_size):
            batch = forecast_values[i:i + batch_size]
            insert_sql = (
                "INSERT INTO forecasts (election_id, source, rating, date_of_forecast, notes) VALUES "
                + ", ".join(batch) + ";"
            )
            result = run_sql(insert_sql, f'insert batch {i // batch_size + 1}')
            if result is not None:
                total_inserted += len(batch)
            time.sleep(2)
        print(f"\nInserted {total_inserted} forecast rows")

    # Update elections with forecast ratings
    if election_updates:
        batch_size = 50
        total_updated = 0
        for i in range(0, len(election_updates), batch_size):
            batch = election_updates[i:i + batch_size]
            update_sql = " ".join(batch)
            result = run_sql(update_sql, f'update batch {i // batch_size + 1}')
            if result is not None:
                total_updated += len(batch)
            time.sleep(2)
        print(f"Updated {total_updated} elections with forecast_rating")

    # Verification
    print("\n--- Verification ---")
    verify = run_sql(f"""
        SELECT COUNT(*) as cnt FROM forecasts
        WHERE source = '{SOURCE}'
          AND election_id IN (
              SELECT e.id FROM elections e
              JOIN seats s ON e.seat_id = s.id
              JOIN districts d ON s.district_id = d.id
              JOIN states st ON d.state_id = st.id
              WHERE st.abbreviation = '{state_abbr}'
                AND e.election_type = 'General'
                AND e.election_year = 2026
          )
    """, 'verify count')
    if verify:
        print(f"StateNavigate forecasts for {state_abbr}: {verify[0]['cnt']}")

    rated = run_sql(f"""
        SELECT e.forecast_rating, COUNT(*) as cnt
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = '{state_abbr}'
          AND e.election_type = 'General'
          AND e.election_year = 2026
          AND e.forecast_rating IS NOT NULL
        GROUP BY e.forecast_rating
        ORDER BY e.forecast_rating
    """, 'verify ratings')
    if rated:
        print("Election forecast_rating distribution:")
        for r in rated:
            print(f"  {r['forecast_rating']}: {r['cnt']}")

if __name__ == '__main__':
    main()
