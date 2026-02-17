"""
Populate seat gap corrections into the database.

Reads researched gap details from /tmp/seat_gap_details.json and applies:
- create_seat_term: Create candidate + seat_term for filled vacancies, update seats cache
- close_seat_term: Close existing seat_term, clear seats cache
- replace_holder: Close old term + create new candidate/term + update cache
- update_name: Fix candidate name + seats cache (same person, different name)
- update_holder: Close old term + create new term (stale OpenStates data fix)

Usage:
    python3 scripts/populate_seat_gaps.py --dry-run
    python3 scripts/populate_seat_gaps.py
    python3 scripts/populate_seat_gaps.py --state VA
"""
import sys
import json
import time
import argparse
import unicodedata
from datetime import datetime
from collections import Counter

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

INPUT_PATH = '/tmp/seat_gap_details.json'

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

# Valid CHECK constraint values
VALID_START_REASONS = {'appointed', 'elected', 'succeeded'}
VALID_END_REASONS = {'appointed_elsewhere', 'died', 'lost_election', 'removed', 'resigned', 'term_expired'}

# Map research labels to valid DB values
START_REASON_MAP = {
    'special_election': 'elected',
    'appointed': 'appointed',
    'elected': 'elected',
}

END_REASON_MAP = {
    'data_correction': 'resigned',  # stale data — use 'resigned' as default
    'appointed_elsewhere': 'appointed_elsewhere',
    'resigned': 'resigned',
    'removed': 'removed',
    'term_expired': 'term_expired',
    'died': 'died',
}

def normalize_reason(reason, valid_set, default, reason_map):
    """Map a reason string to a valid DB value."""
    if not reason:
        return default
    mapped = reason_map.get(reason, reason)
    if mapped in valid_set:
        return mapped
    print(f'  WARNING: Unknown reason {reason!r}, using {default!r}')
    return default

def parse_date(s):
    """Parse date string like 'January 14, 2026' or '2013' to 'YYYY-MM-DD'."""
    if not s:
        return None
    s = s.strip()
    # Try full date format first
    try:
        dt = datetime.strptime(s, '%B %d, %Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass
    # Try year-only format
    try:
        year = int(s)
        return f'{year}-01-01'
    except ValueError:
        pass
    print(f'  WARNING: Could not parse date: {s!r}')
    return None

def split_name(full_name):
    """Split full name into (first, last) for candidates table."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], parts[0]
    # Remove suffixes for splitting purposes
    clean_parts = [p for p in parts if p.rstrip('.') not in ('Jr', 'Sr', 'II', 'III', 'IV', 'V')]
    if not clean_parts:
        clean_parts = parts
    first = clean_parts[0]
    last = clean_parts[-1]
    return first, last

def find_or_create_candidate(name, party, dry_run):
    """Find existing candidate by name or create new one. Returns candidate_id."""
    # Search by exact name
    result = run_sql(
        f"SELECT id FROM candidates WHERE full_name = '{esc(name)}'"
    )
    if result:
        return result[0]['id'], False

    # Search with accent stripping
    stripped = strip_accents(name)
    if stripped != name:
        result = run_sql(
            f"SELECT id FROM candidates WHERE full_name = '{esc(stripped)}'"
        )
        if result:
            return result[0]['id'], False

    if dry_run:
        print(f'    [DRY RUN] Would create candidate: {name} ({party})')
        return None, True

    # Create new candidate
    first, last = split_name(name)
    result = run_sql(
        f"INSERT INTO candidates (full_name, first_name, last_name) "
        f"VALUES ('{esc(name)}', '{esc(first)}', '{esc(last)}') "
        f"RETURNING id"
    )
    cand_id = result[0]['id']
    print(f'    Created candidate #{cand_id}: {name} ({party})')
    time.sleep(1)
    return cand_id, True

def check_seat_term_open(seat_term_id):
    """Check if a seat_term is still open (end_date IS NULL)."""
    result = run_sql(
        f"SELECT id, end_date FROM seat_terms WHERE id = {seat_term_id}"
    )
    if not result:
        return False
    return result[0]['end_date'] is None

def check_existing_open_term(seat_id):
    """Check if seat already has an open seat_term."""
    result = run_sql(
        f"SELECT st.id, c.full_name FROM seat_terms st "
        f"JOIN candidates c ON st.candidate_id = c.id "
        f"WHERE st.seat_id = {seat_id} AND st.end_date IS NULL"
    )
    if result:
        return result[0]
    return None

# ══════════════════════════════════════════════════════════════════════
# ACTION HANDLERS
# ══════════════════════════════════════════════════════════════════════

def process_create_seat_term(item, dry_run):
    """Create candidate + seat_term for a filled vacancy, update seats cache."""
    seat_id = item['seat_id']
    name = item['new_holder']
    party = item['new_holder_party']
    start_date = parse_date(item.get('assumed_office'))
    start_reason = normalize_reason(
        item.get('start_reason', 'elected'),
        VALID_START_REASONS, 'elected', START_REASON_MAP
    )
    label = item['seat_label']

    print(f'  {label}: CREATE seat_term for {name} ({party})')
    print(f'    Start: {start_date}, reason: {start_reason}')

    # Safety: check if seat already has an open term
    existing = check_existing_open_term(seat_id)
    if existing:
        print(f'    SKIP: Seat already has open term #{existing["id"]} for {existing["full_name"]}')
        return 'skipped'

    if dry_run:
        print(f'    [DRY RUN] Would create candidate + seat_term + update cache')
        return 'would_create'

    cand_id, created = find_or_create_candidate(name, party, dry_run=False)
    time.sleep(1)

    # Create seat_term
    start_sql = f"'{start_date}'" if start_date else 'NULL'
    run_sql(
        f"INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, start_reason, caucus) "
        f"VALUES ({seat_id}, {cand_id}, '{esc(party)}', {start_sql}, '{esc(start_reason)}', '{esc(party)}')"
    )
    print(f'    Created seat_term for candidate #{cand_id}')
    time.sleep(1)

    # Update seats cache
    run_sql(
        f"UPDATE seats SET "
        f"current_holder = '{esc(name)}', "
        f"current_holder_party = '{esc(party)}', "
        f"current_holder_caucus = '{esc(party)}' "
        f"WHERE id = {seat_id}"
    )
    print(f'    Updated seats cache')
    return 'created'

def process_close_seat_term(item, dry_run):
    """Close existing seat_term with end_date/end_reason, clear seats cache."""
    seat_id = item['seat_id']
    seat_term_id = item['seat_term_id']
    former_holder = item['former_holder']
    end_reason = normalize_reason(
        item.get('end_reason') or 'resigned',
        VALID_END_REASONS, 'resigned', END_REASON_MAP
    )
    label = item['seat_label']

    # Use a reasonable end_date: either from end_reason context or default
    if end_reason == 'term_expired':
        end_date = '2025-01-01'
    else:
        end_date = '2025-12-01'

    print(f'  {label}: CLOSE seat_term #{seat_term_id} for {former_holder}')
    print(f'    End: {end_date}, reason: {end_reason}')
    if item.get('has_special'):
        print(f'    Note: Special election exists for this seat')

    # Safety: check if term is already closed
    if not check_seat_term_open(seat_term_id):
        print(f'    SKIP: Seat_term #{seat_term_id} is already closed')
        return 'skipped'

    if dry_run:
        print(f'    [DRY RUN] Would close seat_term + clear cache')
        return 'would_close'

    # Close seat_term
    run_sql(
        f"UPDATE seat_terms SET "
        f"end_date = '{end_date}', "
        f"end_reason = '{esc(end_reason)}' "
        f"WHERE id = {seat_term_id}"
    )
    print(f'    Closed seat_term #{seat_term_id}')
    time.sleep(1)

    # Clear seats cache
    run_sql(
        f"UPDATE seats SET "
        f"current_holder = NULL, "
        f"current_holder_party = NULL, "
        f"current_holder_caucus = NULL "
        f"WHERE id = {seat_id}"
    )
    print(f'    Cleared seats cache')
    return 'closed'

def process_replace_holder(item, dry_run):
    """Close old term, create new candidate/term, update seats cache."""
    seat_id = item['seat_id']
    seat_term_id = item['seat_term_id']
    old_name = item['db_holder']
    new_name = item['bp_name']
    label = item['seat_label']
    start_date = parse_date(item.get('assumed_office'))
    start_reason = normalize_reason(
        item.get('start_reason', 'elected'),
        VALID_START_REASONS, 'elected', START_REASON_MAP
    )
    end_reason = normalize_reason(
        item.get('end_reason', 'resigned'),
        VALID_END_REASONS, 'resigned', END_REASON_MAP
    )

    # Determine new holder's party from existing data or context
    # We don't have party in the gap data for replacements, so query from DB
    party_result = run_sql(
        f"SELECT current_holder_party FROM seats WHERE id = {seat_id}"
    )
    old_party = party_result[0]['current_holder_party'] if party_result else None

    print(f'  {label}: REPLACE {old_name} → {new_name}')
    print(f'    Close old: end_reason={end_reason}')
    print(f'    Start new: {start_date}, reason={start_reason}')

    # Safety: check if old term is already closed
    term_open = check_seat_term_open(seat_term_id)
    if not term_open:
        print(f'    WARNING: Seat_term #{seat_term_id} already closed, will still create new term')

    if dry_run:
        print(f'    [DRY RUN] Would close old term, create new candidate/term, update cache')
        return 'would_replace'

    # Close old seat_term (use new holder's assumed_office as end_date)
    if term_open:
        end_date = start_date or '2025-12-01'
        run_sql(
            f"UPDATE seat_terms SET "
            f"end_date = '{end_date}', "
            f"end_reason = '{esc(end_reason)}' "
            f"WHERE id = {seat_term_id}"
        )
        print(f'    Closed seat_term #{seat_term_id}')
        time.sleep(1)

    # We need to figure out the new holder's party
    # For now, try to find it from the new candidate or use old_party
    new_party = old_party  # default: assume same party
    # Check if new holder already exists as candidate
    check = run_sql(f"SELECT id FROM candidates WHERE full_name = '{esc(new_name)}'")
    if check:
        cand_id = check[0]['id']
        # Try to get party from their most recent seat_term
        party_check = run_sql(
            f"SELECT party FROM seat_terms WHERE candidate_id = {cand_id} "
            f"AND party IS NOT NULL ORDER BY id DESC LIMIT 1"
        )
        if party_check and party_check[0]['party']:
            new_party = party_check[0]['party']
        created = False
    else:
        # Create new candidate
        first, last = split_name(new_name)
        result = run_sql(
            f"INSERT INTO candidates (full_name, first_name, last_name) "
            f"VALUES ('{esc(new_name)}', '{esc(first)}', '{esc(last)}') "
            f"RETURNING id"
        )
        cand_id = result[0]['id']
        created = True
        print(f'    Created candidate #{cand_id}: {new_name} ({new_party})')
    time.sleep(1)

    # Create new seat_term
    start_sql = f"'{start_date}'" if start_date else 'NULL'
    party_sql = f"'{esc(new_party)}'" if new_party else 'NULL'
    run_sql(
        f"INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, start_reason, caucus) "
        f"VALUES ({seat_id}, {cand_id}, {party_sql}, {start_sql}, '{esc(start_reason)}', {party_sql})"
    )
    print(f'    Created seat_term for candidate #{cand_id}')
    time.sleep(1)

    # Update seats cache
    run_sql(
        f"UPDATE seats SET "
        f"current_holder = '{esc(new_name)}', "
        f"current_holder_party = {party_sql}, "
        f"current_holder_caucus = {party_sql} "
        f"WHERE id = {seat_id}"
    )
    print(f'    Updated seats cache')
    return 'replaced'

def process_update_name(item, dry_run):
    """Update candidate name + seats cache for same person (nickname/married name)."""
    seat_id = item['seat_id']
    seat_term_id = item['seat_term_id']
    bp_name = item['bp_name']
    db_name = item['db_holder']
    classification = item.get('classification', 'unknown')
    label = item['seat_label']

    print(f'  {label}: UPDATE NAME ({classification})')
    print(f'    DB: {db_name} → BP: {bp_name}')

    if dry_run:
        print(f'    [DRY RUN] Would update candidate name + seats cache')
        return 'would_update'

    # Get candidate_id from seat_term
    result = run_sql(
        f"SELECT candidate_id FROM seat_terms WHERE id = {seat_term_id}"
    )
    if not result:
        print(f'    ERROR: Seat_term #{seat_term_id} not found')
        return 'error'
    cand_id = result[0]['candidate_id']
    time.sleep(1)

    # Update candidate name
    first, last = split_name(bp_name)
    run_sql(
        f"UPDATE candidates SET "
        f"full_name = '{esc(bp_name)}', "
        f"first_name = '{esc(first)}', "
        f"last_name = '{esc(last)}' "
        f"WHERE id = {cand_id}"
    )
    print(f'    Updated candidate #{cand_id} name')
    time.sleep(1)

    # Update seats cache
    run_sql(
        f"UPDATE seats SET current_holder = '{esc(bp_name)}' WHERE id = {seat_id}"
    )
    print(f'    Updated seats cache')
    return 'updated'

def process_update_holder(item, dry_run):
    """Close old term, create new term for stale OpenStates data fix."""
    seat_id = item['seat_id']
    seat_term_id = item['seat_term_id']
    old_name = item['db_holder']
    new_name = item['bp_name']
    label = item['seat_label']
    start_date = parse_date(item.get('assumed_office'))

    # Query seat for current party info
    party_result = run_sql(
        f"SELECT current_holder_party FROM seats WHERE id = {seat_id}"
    )
    old_party = party_result[0]['current_holder_party'] if party_result else None

    print(f'  {label}: UPDATE HOLDER (stale data)')
    print(f'    DB: {old_name} → BP: {new_name}')
    print(f'    Assumed office: {start_date}')

    # Safety: check if old term is already closed
    term_open = check_seat_term_open(seat_term_id)

    if dry_run:
        print(f'    [DRY RUN] Would close old term #{seat_term_id} (open={term_open}), create new term')
        return 'would_update'

    # Close old seat_term (these holders were never actually there — OpenStates was stale)
    if term_open:
        end_date = start_date or '2025-01-01'
        run_sql(
            f"UPDATE seat_terms SET "
            f"end_date = '{end_date}', "
            f"end_reason = 'resigned' "
            f"WHERE id = {seat_term_id}"
        )
        print(f'    Closed stale seat_term #{seat_term_id}')
        time.sleep(1)

    # Find or create new candidate
    new_party = old_party
    check = run_sql(f"SELECT id FROM candidates WHERE full_name = '{esc(new_name)}'")
    if check:
        cand_id = check[0]['id']
        # Try to get party from their most recent seat_term
        party_check = run_sql(
            f"SELECT party FROM seat_terms WHERE candidate_id = {cand_id} "
            f"AND party IS NOT NULL ORDER BY id DESC LIMIT 1"
        )
        if party_check and party_check[0]['party']:
            new_party = party_check[0]['party']
        print(f'    Found existing candidate #{cand_id}: {new_name}')
    else:
        first, last = split_name(new_name)
        result = run_sql(
            f"INSERT INTO candidates (full_name, first_name, last_name) "
            f"VALUES ('{esc(new_name)}', '{esc(first)}', '{esc(last)}') "
            f"RETURNING id"
        )
        cand_id = result[0]['id']
        print(f'    Created candidate #{cand_id}: {new_name} ({new_party})')
    time.sleep(1)

    # Create new seat_term
    start_sql = f"'{start_date}'" if start_date else 'NULL'
    party_sql = f"'{esc(new_party)}'" if new_party else 'NULL'
    run_sql(
        f"INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, start_reason, caucus) "
        f"VALUES ({seat_id}, {cand_id}, {party_sql}, {start_sql}, 'elected', {party_sql})"
    )
    print(f'    Created seat_term for candidate #{cand_id}')
    time.sleep(1)

    # Update seats cache
    run_sql(
        f"UPDATE seats SET "
        f"current_holder = '{esc(new_name)}', "
        f"current_holder_party = {party_sql}, "
        f"current_holder_caucus = {party_sql} "
        f"WHERE id = {seat_id}"
    )
    print(f'    Updated seats cache')
    return 'updated'

# ══════════════════════════════════════════════════════════════════════
# FAMILY CLASSIFICATION OVERRIDE
# ══════════════════════════════════════════════════════════════════════

# Items classified as "family" in update_name are actually different people.
# Re-classify them as replace_holder at runtime.
FAMILY_OVERRIDES = {
    ('ME', 'House', '146'),  # Walter Runte Jr. replaced Gerry Runte
}

# Items to skip — known BP member page lags or incorrect research data
SKIP_ITEMS = {
    ('OK', 'House', '35'),  # Travis won Feb 10, 2026 special; BP member page not updated yet
}

# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Populate seat gap corrections')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--state', type=str, help='Process only this state (e.g., VA)')
    args = parser.parse_args()

    with open(INPUT_PATH) as f:
        items = json.load(f)

    print(f'Loaded {len(items)} gap items from {INPUT_PATH}')

    # Filter by state if requested
    if args.state:
        items = [i for i in items if i['state'] == args.state.upper()]
        print(f'Filtered to {len(items)} items for state {args.state.upper()}')

    if not items:
        print('No items to process.')
        return

    # Skip known-bad items
    before = len(items)
    items = [i for i in items if (i['state'], i['chamber'], i['district']) not in SKIP_ITEMS]
    if len(items) < before:
        print(f'Skipped {before - len(items)} items from SKIP_ITEMS list')

    # Re-classify family items as replace_holder
    for item in items:
        key = (item['state'], item['chamber'], item['district'])
        if key in FAMILY_OVERRIDES and item['action'] == 'update_name':
            print(f'  Re-classifying {item["seat_label"]} from update_name to replace_holder (family)')
            item['action'] = 'replace_holder'
            item['start_reason'] = item.get('start_reason', 'elected')
            item['end_reason'] = item.get('end_reason', 'resigned')

    # Group by action
    by_action = {}
    for item in items:
        action = item['action']
        by_action.setdefault(action, []).append(item)

    print(f'\nActions:')
    for action, group in sorted(by_action.items()):
        print(f'  {action}: {len(group)} items')

    stats = Counter()

    # Process in order: close first, then create, then replace, then update
    action_order = ['close_seat_term', 'create_seat_term', 'replace_holder', 'update_holder', 'update_name']
    handlers = {
        'create_seat_term': process_create_seat_term,
        'close_seat_term': process_close_seat_term,
        'replace_holder': process_replace_holder,
        'update_name': process_update_name,
        'update_holder': process_update_holder,
    }

    for action in action_order:
        group = by_action.get(action, [])
        if not group:
            continue

        print(f'\n{"=" * 60}')
        print(f'Processing {action} ({len(group)} items)')
        print(f'{"=" * 60}')

        handler = handlers[action]
        for item in group:
            result = handler(item, args.dry_run)
            stats[result] += 1
            if not args.dry_run:
                time.sleep(1)

    # Summary
    print(f'\n{"=" * 60}')
    print('SUMMARY')
    print(f'{"=" * 60}')
    for key, count in sorted(stats.items()):
        print(f'  {key}: {count}')
    print(f'  Total processed: {sum(stats.values())}')

    if args.dry_run:
        print(f'\n  *** DRY RUN — no changes made ***')

if __name__ == '__main__':
    main()
