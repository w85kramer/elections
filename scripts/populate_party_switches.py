"""
Populate party switch records into the database.

Reads /tmp/party_switches.json (from download_party_switches.py), matches names
to candidates in the database, and inserts records into the party_switches table.

Optionally updates seat_terms and seats cache for current officeholders.

Usage:
    python3 scripts/populate_party_switches.py --dry-run
    python3 scripts/populate_party_switches.py
    python3 scripts/populate_party_switches.py --update-current          # also update seat_terms + seats
    python3 scripts/populate_party_switches.py --update-current --dry-run
    python3 scripts/populate_party_switches.py --year 2024               # filter to one year
"""
import sys
import os
import json
import time
import argparse
import unicodedata
from collections import Counter

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

INPUT_PATH = '/tmp/party_switches.json'

BP_SOURCE_URL = 'https://ballotpedia.org/State_legislators_who_have_switched_political_party_affiliation'

# Chambers in our DB that count as "House" on BP
HOUSE_CHAMBERS = {'House', 'Assembly', 'House of Delegates', 'Legislature'}


def run_sql(query, exit_on_error=True, max_retries=5):
    for attempt in range(max_retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None
    print(f'SQL ERROR: Max retries exceeded')
    if exit_on_error:
        sys.exit(1)
    return None


def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")


def strip_accents(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def normalize_name(name):
    """Normalize a name for matching: lowercase, strip accents, remove suffixes."""
    name = strip_accents(name).lower().strip()
    # Remove common suffixes
    for suffix in [' jr.', ' jr', ' sr.', ' sr', ' ii', ' iii', ' iv', ' v']:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    return name


def get_last_name(full_name):
    """Extract last name from full name, handling suffixes."""
    parts = full_name.strip().split()
    if not parts:
        return ''
    # Skip suffixes at end
    suffixes = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v'}
    while len(parts) > 1 and parts[-1].lower().rstrip('.') in suffixes:
        parts.pop()
    return parts[-1] if parts else full_name


def get_first_name(full_name):
    """Extract first name from full name."""
    parts = full_name.strip().split()
    return parts[0] if parts else full_name


def load_seat_terms():
    """
    Load all legislative seat_terms joined with candidates, seats, districts, states.
    Returns list of dicts and an index by normalized last name.
    """
    print('Loading seat_terms from database...')
    query = """
    SELECT
        st.id as seat_term_id,
        st.seat_id,
        st.candidate_id,
        st.party,
        st.caucus,
        st.start_date,
        st.end_date,
        c.full_name,
        c.first_name,
        c.last_name,
        s.seat_label,
        s.current_holder,
        d.chamber,
        d.state_id,
        states.abbreviation as state_abbrev,
        states.state_name,
        EXTRACT(YEAR FROM st.start_date)::int as start_year,
        EXTRACT(YEAR FROM st.end_date)::int as end_year
    FROM seat_terms st
    JOIN candidates c ON st.candidate_id = c.id
    JOIN seats s ON st.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states ON d.state_id = states.id
    WHERE d.office_level = 'Legislative'
    ORDER BY st.id
    """
    rows = run_sql(query)
    print(f'  Loaded {len(rows)} seat_terms')

    # Build index by normalized last name
    by_last_name = {}
    for row in rows:
        ln = normalize_name(get_last_name(row['full_name']))
        by_last_name.setdefault(ln, []).append(row)

    return rows, by_last_name


def match_candidate(record, by_last_name):
    """
    Try to match a BP party switch record to a candidate in our DB.

    Returns (best_match_row, score) or (None, 0).
    """
    bp_name = record['name']
    bp_chamber = record['chamber']  # 'Senate' or 'House of Representatives'
    bp_from = record['from_party']
    bp_year = record['year']

    bp_last = normalize_name(get_last_name(bp_name))
    bp_first = normalize_name(get_first_name(bp_name))

    candidates_with_last = by_last_name.get(bp_last, [])
    if not candidates_with_last:
        return None, 0

    bp_to = record['to_party']

    best_match = None
    best_score = 0

    for row in candidates_with_last:
        score = 0

        # Chamber match
        # NE is unicameral ('Legislature' in DB) but BP lists under 'Senate'
        db_chamber = row['chamber']
        if bp_chamber == 'Senate':
            if db_chamber not in ('Senate', 'Legislature'):
                continue  # Hard filter
        else:
            # BP "House of Representatives" matches anything non-Senate in our DB
            if db_chamber == 'Senate':
                continue  # Hard filter

        score += 1  # Chamber matched

        # First name match (3+ char prefix)
        db_first = normalize_name(row['first_name'] or '')
        min_len = min(len(bp_first), len(db_first), 3)
        if min_len >= 3 and bp_first[:min_len] == db_first[:min_len]:
            score += 3
        elif min_len >= 2 and bp_first[:min_len] == db_first[:min_len]:
            score += 1
        else:
            continue  # Hard filter — first name must have some match

        # Time overlap: person held office during switch year
        start_year = row['start_year']
        end_year = row['end_year']
        is_current = row['end_date'] is None

        if start_year is not None:
            if start_year <= bp_year:
                if end_year is None or end_year >= bp_year - 1:
                    score += 3  # Strong temporal match
                elif end_year >= bp_year - 2:
                    score += 1  # Weak temporal match
                else:
                    score -= 2  # Ended too long ago
            else:
                score -= 1  # Started after switch year
        else:
            # No start_date — use is_current as proxy
            if is_current:
                score += 2  # Likely still serving, reasonable match

        # Party match: check BOTH old_party and new_party against DB party
        # The DB may have either the pre-switch or post-switch party
        db_party = row['party'] or ''
        if db_party == bp_from:
            score += 2  # DB has pre-switch party
        elif db_party == bp_to:
            score += 2  # DB already updated to post-switch party
        # No penalty for mismatch — party data can be messy

        # Is current (end_date IS NULL) — bonus for recent switches
        if is_current:
            score += 1

        if score > best_score:
            best_score = score
            best_match = row

    return best_match, best_score


def main():
    parser = argparse.ArgumentParser(description='Populate party switch records')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--update-current', action='store_true',
                        help='Also update seat_terms and seats cache for current officeholders')
    parser.add_argument('--year', type=int, help='Filter to a specific switch year')
    args = parser.parse_args()

    # Load input
    with open(INPUT_PATH) as f:
        records = json.load(f)
    print(f'Loaded {len(records)} party switch records from {INPUT_PATH}')

    if args.year:
        records = [r for r in records if r['year'] == args.year]
        print(f'Filtered to {len(records)} records for year {args.year}')

    if not records:
        print('No records to process.')
        return

    # Load DB data
    all_terms, by_last_name = load_seat_terms()

    # Match each record
    matched = []
    unmatched = []
    low_confidence = []

    MIN_SCORE = 4  # Minimum score to consider a match reliable

    for record in records:
        match, score = match_candidate(record, by_last_name)
        if match and score >= MIN_SCORE:
            matched.append({
                'record': record,
                'match': match,
                'score': score,
            })
        elif match and score >= 3:
            low_confidence.append({
                'record': record,
                'match': match,
                'score': score,
            })
        else:
            unmatched.append(record)

    print(f'\n{"=" * 60}')
    print(f'MATCHING RESULTS')
    print(f'{"=" * 60}')
    print(f'  Matched (score >= {MIN_SCORE}): {len(matched)}')
    print(f'  Low confidence (score 3-{MIN_SCORE-1}): {len(low_confidence)}')
    print(f'  Unmatched: {len(unmatched)}')

    # Show unmatched
    if unmatched:
        print(f'\nUNMATCHED records ({len(unmatched)}):')
        for r in sorted(unmatched, key=lambda x: (-x['year'], x['name'])):
            print(f'  {r["year"]} {r["chamber"][:6]:6s} {r["from_party"]}->{r["to_party"]}  {r["name"]}')

    # Show low confidence
    if low_confidence:
        print(f'\nLOW CONFIDENCE matches ({len(low_confidence)}):')
        for item in sorted(low_confidence, key=lambda x: (-x['record']['year'], x['record']['name'])):
            r = item['record']
            m = item['match']
            print(f'  {r["year"]} {r["name"]:30s} -> {m["full_name"]:30s} '
                  f'({m["state_abbrev"]} {m["chamber"]}) score={item["score"]}')

    # Insert matched records
    if not matched:
        print('\nNo matches to insert.')
        return

    print(f'\n{"=" * 60}')
    print(f'INSERTING {len(matched)} records')
    print(f'{"=" * 60}')

    inserted = 0
    skipped = 0
    errors = 0

    for item in sorted(matched, key=lambda x: (-x['record']['year'], x['record']['name'])):
        r = item['record']
        m = item['match']
        is_current = m['end_date'] is None

        print(f'  {r["year"]} {r["name"]:30s} -> {m["full_name"]:30s} '
              f'({m["state_abbrev"]} {m["chamber"]}) '
              f'{"CURRENT" if is_current else "former"} score={item["score"]}')

        if args.dry_run:
            inserted += 1
            continue

        # Build INSERT
        query = f"""
        INSERT INTO party_switches (
            candidate_id, seat_id, state_id, chamber,
            old_party, new_party, old_caucus, new_caucus,
            switch_year, source_url, bp_profile_url, is_current
        ) VALUES (
            {m['candidate_id']},
            {m['seat_id']},
            {m['state_id']},
            '{esc(m['chamber'])}',
            '{esc(r['from_party'])}',
            '{esc(r['to_party'])}',
            '{esc(r['from_party'])}',
            '{esc(r['to_party'])}',
            {r['year']},
            '{esc(BP_SOURCE_URL)}',
            '{esc(r.get('bp_profile_url', ''))}',
            {str(is_current).upper()}
        )
        ON CONFLICT (candidate_id, switch_year, old_party, new_party) DO NOTHING
        """

        result = run_sql(query, exit_on_error=False)
        if result is not None:
            inserted += 1
        else:
            errors += 1
            print(f'    ERROR inserting record')

        time.sleep(0.5)

    print(f'\n{"=" * 60}')
    print(f'INSERT SUMMARY')
    print(f'{"=" * 60}')
    print(f'  Inserted: {inserted}')
    print(f'  Skipped (conflict): {skipped}')
    print(f'  Errors: {errors}')

    if args.dry_run:
        print(f'\n  *** DRY RUN — no changes made ***')

    # Update current officeholders if requested
    if args.update_current and not args.dry_run:
        update_current_officeholders(matched, dry_run=False)
    elif args.update_current and args.dry_run:
        update_current_officeholders(matched, dry_run=True)


def update_current_officeholders(matched, dry_run=False):
    """
    For matched records where is_current=True, update seat_terms and seats cache
    to reflect the new party/caucus.
    """
    current_switches = [item for item in matched if item['match']['end_date'] is None]

    if not current_switches:
        print('\nNo current officeholders with party switches.')
        return

    print(f'\n{"=" * 60}')
    print(f'UPDATING CURRENT OFFICEHOLDERS ({len(current_switches)})')
    print(f'{"=" * 60}')

    updated = 0
    affected_states = set()

    for item in current_switches:
        r = item['record']
        m = item['match']
        new_party = r['to_party']
        new_caucus = r['to_party']  # caucus follows party for switches

        print(f'  {m["full_name"]} ({m["state_abbrev"]} {m["chamber"]}): '
              f'{r["from_party"]} -> {new_party}')

        if dry_run:
            print(f'    [DRY RUN] Would update seat_term #{m["seat_term_id"]} and seat #{m["seat_id"]}')
            updated += 1
            affected_states.add(m['state_abbrev'])
            continue

        # Update seat_term in-place
        run_sql(
            f"UPDATE seat_terms SET "
            f"party = '{esc(new_party)}', "
            f"caucus = '{esc(new_caucus)}' "
            f"WHERE id = {m['seat_term_id']}"
        )
        time.sleep(0.5)

        # Update seats cache
        run_sql(
            f"UPDATE seats SET "
            f"current_holder_party = '{esc(new_party)}', "
            f"current_holder_caucus = '{esc(new_caucus)}' "
            f"WHERE id = {m['seat_id']}"
        )
        time.sleep(0.5)

        updated += 1
        affected_states.add(m['state_abbrev'])
        print(f'    Updated seat_term #{m["seat_term_id"]} and seat #{m["seat_id"]}')

    print(f'\nUpdated: {updated} officeholders')
    print(f'Affected states: {", ".join(sorted(affected_states))}')
    print(f'NOTE: Chamber control may need recalculation for: {", ".join(sorted(affected_states))}')

    if dry_run:
        print(f'\n  *** DRY RUN — no changes made ***')


if __name__ == '__main__':
    main()
