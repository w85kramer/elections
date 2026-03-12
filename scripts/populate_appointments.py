#!/usr/bin/env python3
"""
Populate legislative appointment seat_terms from Ballotpedia vacancy research.

Reads appointment data from /tmp/appointment_research.md (markdown tables),
then for each appointment:
1. Finds the seat by state/chamber/district
2. Finds the departing legislator's seat_term and fixes end_date/end_reason
3. Creates candidate record for appointee if needed
4. Creates appointed seat_term for the appointee

Usage:
    python3 scripts/populate_appointments.py --dry-run              # preview all
    python3 scripts/populate_appointments.py --dry-run --state AZ   # preview one state
    python3 scripts/populate_appointments.py --state AZ             # run one state
    python3 scripts/populate_appointments.py                        # run all states
"""

import json
import os
import re
import sys
import time
import requests
from pathlib import Path

# Load env
env_path = Path(__file__).parent.parent / '.env'
env = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

SUPABASE_TOKEN = env['SUPABASE_MANAGEMENT_TOKEN']
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'

DRY_RUN = '--dry-run' in sys.argv
STATE_FILTER = None
for i, arg in enumerate(sys.argv):
    if arg == '--state' and i + 1 < len(sys.argv):
        STATE_FILTER = sys.argv[i + 1].upper()


def run_sql(query, label=""):
    if DRY_RUN:
        return None
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {SUPABASE_TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"    ERROR ({r.status_code}): {r.text[:300]}")
        return None
    print(f"    Failed after 5 retries")
    return None


def run_sql_read(query, label=""):
    """Always executes, even in dry-run mode (for lookups)."""
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {SUPABASE_TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            time.sleep(wait)
            continue
        print(f"    ERROR ({r.status_code}): {r.text[:200]}")
        return None
    return None


def esc(s):
    return s.replace("'", "''")


# Map departure reasons from research to DB end_reason values
END_REASON_MAP = {
    'resignation': 'resigned',
    'death': 'died',
    'removed': 'removed',
    'elevation': 'appointed_elsewhere',
    'appointed to another office': 'appointed_elsewhere',
    'elected to another office': 'appointed_elsewhere',  # close enough
    'left at term end': 'term_expired',
}

# Chamber name mapping
CHAMBER_MAP = {
    'House': 'House',
    'Senate': 'Senate',
    'Assembly': 'Assembly',  # NJ, NV
    'Legislature': 'Legislature',  # NE
    'House of Delegates': 'House of Delegates',  # WV
}

# States that use Assembly instead of House
ASSEMBLY_STATES = {'NJ', 'NV'}
# States that use House of Delegates
HOD_STATES = {'WV'}


def parse_research_file(filepath):
    """Parse the markdown research file into structured data."""
    with open(filepath) as f:
        text = f.read()

    states = {}
    current_state = None

    # Find each state section
    state_pattern = re.compile(r'### ([A-Z ]+) \(([A-Z]{2})\) -- (\d+) appointments?')

    lines = text.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i]

        # Check for state header
        m = state_pattern.match(line)
        if m:
            current_state = m.group(2)
            states[current_state] = []
            i += 1
            continue

        # Check for table rows (skip header and separator)
        if current_state and line.startswith('|') and not line.startswith('|---') and not line.startswith('| Year'):
            parts = [p.strip() for p in line.split('|')]
            parts = [p for p in parts if p]  # remove empty

            if len(parts) >= 9:
                year = parts[0]
                chamber = parts[1]
                district = parts[2]
                departed = parts[3]
                party = parts[4]
                reason = parts[5]
                departure_date = parts[6]
                appointed_raw = parts[7]
                appt_date = parts[8]

                # Parse appointee name and party
                appt_match = re.match(r'(.+?)\s*\(([A-Za-z]+)\)\s*$', appointed_raw)
                if appt_match:
                    appointee = appt_match.group(1).strip()
                    appt_party = appt_match.group(2)
                else:
                    appointee = appointed_raw
                    appt_party = party

                states[current_state].append({
                    'year': int(year),
                    'chamber': chamber,
                    'district': district,
                    'departed': departed,
                    'departed_party': party,
                    'reason': reason.lower(),
                    'departure_date': departure_date,
                    'appointee': appointee,
                    'appointee_party': appt_party,
                    'appointment_date': appt_date,
                })

        # Check for end of state section
        if line.startswith('---') and current_state:
            current_state = None

        i += 1

    return states


def get_db_chamber(state, research_chamber):
    """Map research chamber name to DB chamber name."""
    if state in ASSEMBLY_STATES and research_chamber == 'Assembly':
        return 'Assembly'
    if state == 'WV':
        if research_chamber == 'House':
            return 'House of Delegates'
    if state == 'NE':
        return 'Legislature'
    return research_chamber


def normalize_district(district):
    """Normalize district identifier for DB matching."""
    d = district.strip()
    # Remove position info for WA (e.g., "40" from "40-Pos 1")
    # Actually WA districts are just numbers, position is seat_designator
    # Keep the base district number
    d = re.sub(r'-Pos \d+', '', d)
    d = re.sub(r'\s+', ' ', d).strip()
    return d


def compute_end_date(appointment):
    """Compute the end date for an appointee's term."""
    year = appointment['year']
    chamber = appointment['chamber']

    # The term ends at the next regular election's swearing-in
    # House/Assembly: 2-year terms, Senate: varies by state
    # For simplicity, end at next Jan 1 after the next general election
    # Most appointees serve until the next swearing-in

    # For House/Assembly (2-year terms): end at next odd-year Jan 1
    # For Senate: depends on state's cycle

    # Actually, the end date depends on when the next election for that seat occurs.
    # For simplicity, we'll set it to the next January 1 after an even-year election.
    # Many of these will be overwritten by actual elected terms if the appointee wins.

    # If the year is even, the term likely ends Jan 1 of the next odd year
    # If the year is odd, the term ends Jan 1 of the next even+1 year (next cycle)

    # Simplified: end at the next odd-year January 1
    if year % 2 == 0:
        return f"{year + 1}-01-01"
    else:
        return f"{year + 2}-01-01"


def process_state(state_abbr, appointments, seat_cache, candidate_cache):
    """Process all appointments for a single state."""
    print(f"\n  === {state_abbr}: {len(appointments)} appointments ===")

    stats = {'departures_fixed': 0, 'terms_created': 0, 'candidates_created': 0, 'skipped': 0, 'errors': 0}

    for appt in appointments:
        db_chamber = get_db_chamber(state_abbr, appt['chamber'])
        district = normalize_district(appt['district'])

        # Find seat
        seat_key = (state_abbr, db_chamber, district)
        if seat_key not in seat_cache:
            stats['skipped'] += 1
            continue

        seat_id = seat_cache[seat_key]

        # Map departure reason
        end_reason = END_REASON_MAP.get(appt['reason'], 'resigned')

        # === Fix departing legislator's seat_term ===
        departed_name = appt['departed']
        dep_date = appt['departure_date']

        if dep_date and dep_date != 'XX' and not DRY_RUN:
            # Find the most recent seat_term for this person on this seat
            # that hasn't already been fixed (end_reason still = term_expired)
            fix_sql = f"""
            UPDATE seat_terms SET end_date = '{dep_date}', end_reason = '{end_reason}'
            WHERE id = (
                SELECT st.id FROM seat_terms st
                JOIN candidates c ON st.candidate_id = c.id
                WHERE st.seat_id = {seat_id}
                AND c.full_name = '{esc(departed_name)}'
                AND st.end_reason = 'term_expired'
                AND st.end_date >= '{dep_date}'
                ORDER BY st.start_date DESC
                LIMIT 1
            )
            """
            result = run_sql(fix_sql, f"Fix {departed_name} departure")
            if result is not None:
                stats['departures_fixed'] += 1

        # === Create/find appointee candidate ===
        appointee_name = appt['appointee']
        if appointee_name not in candidate_cache:
            # Try to find in DB
            found = run_sql_read(f"SELECT id FROM candidates WHERE full_name = '{esc(appointee_name)}' LIMIT 1")
            if found and len(found) > 0:
                candidate_cache[appointee_name] = found[0]['id']
            elif not DRY_RUN:
                # Parse name
                parts = appointee_name.split()
                if len(parts) == 1:
                    first, last = parts[0], ''
                else:
                    # Handle suffixes
                    suffixes = {'Jr', 'Jr.', 'Sr', 'Sr.', 'II', 'III', 'IV'}
                    non_suffix = [p for p in parts if p.rstrip(',') not in suffixes]
                    first = non_suffix[0] if non_suffix else parts[0]
                    last = ' '.join(non_suffix[1:]) if len(non_suffix) > 1 else (parts[-1] if len(parts) > 1 else '')

                create_result = run_sql(
                    f"INSERT INTO candidates (full_name, first_name, last_name) VALUES ('{esc(appointee_name)}', '{esc(first)}', '{esc(last)}') RETURNING id",
                    f"Create candidate {appointee_name}"
                )
                if create_result and len(create_result) > 0:
                    candidate_cache[appointee_name] = create_result[0]['id']
                    stats['candidates_created'] += 1
                else:
                    stats['errors'] += 1
                    continue
            else:
                # dry run
                stats['candidates_created'] += 1

        candidate_id = candidate_cache.get(appointee_name)

        # === Create appointed seat_term ===
        appt_date = appt['appointment_date']
        if not appt_date or 'XX' in appt_date:
            stats['skipped'] += 1
            continue

        party = appt['appointee_party']
        if party == 'Fwd':
            party = 'I'  # Forward Party → Independent for DB
        elif len(party) > 1:
            party = party[0]

        # Determine term end date
        # Check if there's already a term for this person on this seat starting later
        # If so, use its start_date as our end_date
        if candidate_id and not DRY_RUN:
            existing = run_sql_read(
                f"SELECT start_date FROM seat_terms WHERE seat_id = {seat_id} AND candidate_id = {candidate_id} AND start_date > '{appt_date}' ORDER BY start_date LIMIT 1"
            )
            if existing and len(existing) > 0:
                # They have a later elected term — our appointed term ends when that starts
                end_date = existing[0]['start_date']
            else:
                # Check if someone else has a term starting after the appointment
                next_term = run_sql_read(
                    f"SELECT start_date FROM seat_terms WHERE seat_id = {seat_id} AND start_date > '{appt_date}' ORDER BY start_date LIMIT 1"
                )
                if next_term and len(next_term) > 0:
                    end_date = next_term[0]['start_date']
                else:
                    # Current holder — end_date is NULL
                    end_date = None

            # Check if an appointed term already exists
            dup_check = run_sql_read(
                f"SELECT id FROM seat_terms WHERE seat_id = {seat_id} AND candidate_id = {candidate_id} AND start_reason = 'appointed' AND start_date = '{appt_date}'"
            )
            if dup_check and len(dup_check) > 0:
                # Already exists
                continue

            # Also check if this person already has a term that just needs start_reason fixed
            fix_check = run_sql_read(
                f"SELECT id, start_date, start_reason FROM seat_terms WHERE seat_id = {seat_id} AND candidate_id = {candidate_id} AND start_reason = 'elected' ORDER BY start_date"
            )
            if fix_check:
                for fc in fix_check:
                    # If their "elected" term start_date is close to the appointment date, just fix it
                    fc_start = fc['start_date']
                    # Compare: if the existing start is within ~6 months of the appointment, update it
                    fc_year = int(fc_start[:4])
                    appt_year = int(appt_date[:4])
                    if abs(fc_year - appt_year) <= 1:
                        # Check more carefully — if the appointed date is before the "elected" start
                        if appt_date < fc_start:
                            run_sql(
                                f"UPDATE seat_terms SET start_date = '{appt_date}', start_reason = 'appointed' WHERE id = {fc['id']}",
                                f"Fix {appointee_name} start_reason"
                            )
                            stats['terms_created'] += 1
                            break
                else:
                    # Create new term
                    end_sql = f"'{end_date}'" if end_date else 'NULL'
                    end_reason_sql = "'term_expired'" if end_date else 'NULL'
                    sql = f"""INSERT INTO seat_terms (seat_id, candidate_id, start_date, end_date, start_reason, end_reason, party)
                    VALUES ({seat_id}, {candidate_id}, '{appt_date}', {end_sql}, 'appointed', {end_reason_sql}, '{party}')"""
                    result = run_sql(sql, f"Create term for {appointee_name}")
                    if result is not None:
                        stats['terms_created'] += 1
                    else:
                        stats['errors'] += 1
            else:
                # No existing term — create new
                end_sql = f"'{end_date}'" if end_date else 'NULL'
                end_reason_sql = "'term_expired'" if end_date else 'NULL'
                sql = f"""INSERT INTO seat_terms (seat_id, candidate_id, start_date, end_date, start_reason, end_reason, party)
                VALUES ({seat_id}, {candidate_id}, '{appt_date}', {end_sql}, 'appointed', {end_reason_sql}, '{party}')"""
                result = run_sql(sql, f"Create term for {appointee_name}")
                if result is not None:
                    stats['terms_created'] += 1
                else:
                    stats['errors'] += 1
        else:
            stats['terms_created'] += 1  # count for dry run

        time.sleep(0.3)  # Rate limiting

    print(f"    Departures fixed: {stats['departures_fixed']}")
    print(f"    New candidates: {stats['candidates_created']}")
    print(f"    Terms created/fixed: {stats['terms_created']}")
    print(f"    Skipped: {stats['skipped']}")
    if stats['errors']:
        print(f"    Errors: {stats['errors']}")

    return stats


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}Legislative Appointment Seat Terms")
    print("=" * 60)

    # Parse research file
    research_path = '/tmp/appointment_research.md'
    if not os.path.exists(research_path):
        print(f"ERROR: {research_path} not found")
        return

    states_data = parse_research_file(research_path)
    total = sum(len(v) for v in states_data.values())
    print(f"Parsed {total} appointments across {len(states_data)} states")

    if STATE_FILTER:
        if STATE_FILTER not in states_data:
            print(f"ERROR: State {STATE_FILTER} not found in research data")
            print(f"  Available: {', '.join(sorted(states_data.keys()))}")
            return
        states_data = {STATE_FILTER: states_data[STATE_FILTER]}
        print(f"  Filtered to {STATE_FILTER}: {len(states_data[STATE_FILTER])} appointments")

    # Build seat cache: (state_abbr, chamber, district_number) -> seat_id
    print("\nBuilding seat cache...")
    states_list = list(states_data.keys())
    states_in = ", ".join(f"'{s}'" for s in states_list)

    seats = run_sql_read(f"""
        SELECT s.id as seat_id, st.abbreviation as state, d.district_number, d.chamber
        FROM seats s
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation IN ({states_in})
        AND d.office_level = 'Legislative'
        AND d.redistricting_cycle = '2022'
    """)

    if not seats:
        print("ERROR: Could not load seats")
        return

    seat_cache = {}
    for s in seats:
        key = (s['state'], s['chamber'], s['district_number'])
        seat_cache[key] = s['seat_id']

    print(f"  Loaded {len(seat_cache)} seats")

    # Candidate name -> id cache
    candidate_cache = {}

    # Process each state
    totals = {'departures_fixed': 0, 'terms_created': 0, 'candidates_created': 0, 'skipped': 0, 'errors': 0}

    for state_abbr in sorted(states_data.keys()):
        appointments = states_data[state_abbr]
        stats = process_state(state_abbr, appointments, seat_cache, candidate_cache)
        for k in totals:
            totals[k] += stats[k]
        time.sleep(1)

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"  States processed: {len(states_data)}")
    print(f"  Total departures fixed: {totals['departures_fixed']}")
    print(f"  Total candidates created: {totals['candidates_created']}")
    print(f"  Total terms created/fixed: {totals['terms_created']}")
    print(f"  Total skipped: {totals['skipped']}")
    if totals['errors']:
        print(f"  Total errors: {totals['errors']}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
