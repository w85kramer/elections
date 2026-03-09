"""
Weekly ballot measure update: download fresh 2026 data from Ballotpedia,
compare with database, and apply changes (new measures, title updates,
status changes).

Usage:
    python3 scripts/update_ballot_measures.py              # dry run (default)
    python3 scripts/update_ballot_measures.py --apply       # apply changes to DB
    python3 scripts/update_ballot_measures.py --force-download  # re-download even if cached today
"""
import sys
import os
import json
import time
import argparse
import re
from datetime import datetime, date

# Add parent dir so we can import db_config and download_ballot_measures
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from db_config import TOKEN, API_URL

# State name → abbreviation
STATE_ABBREV = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY',
}


def run_sql(query, max_retries=5):
    """Execute SQL via Supabase Management API with retry."""
    for attempt in range(1, max_retries + 1):
        resp = requests.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
        )
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * attempt
            print(f'  Rate limited, waiting {wait}s...', flush=True)
            time.sleep(wait)
            continue
        print(f'  SQL ERROR {resp.status_code}: {resp.text[:200]}', flush=True)
        return []
    print('  Max retries exceeded', flush=True)
    return []


def download_2026_measures(force=False):
    """Download and parse fresh 2026 ballot measures from Ballotpedia."""
    cache_file = '/tmp/bp_2026_ballot_measures.html'

    # Check if cache is fresh (downloaded today)
    if not force and os.path.exists(cache_file):
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if mtime.date() == date.today():
            print(f'Using today\'s cached index page: {cache_file}')
            with open(cache_file, 'r', encoding='utf-8') as f:
                html_text = f.read()
        else:
            print(f'Cache is from {mtime.date()}, re-downloading...')
            html_text = None
    else:
        html_text = None

    if html_text is None:
        # Import the download function from download_ballot_measures
        from download_ballot_measures import download_page, parse_index_page

        url = 'https://ballotpedia.org/2026_ballot_measures'
        print(f'Downloading: {url}')
        html_text = download_page(url)
        if not html_text:
            print('ERROR: Could not download 2026 index page')
            sys.exit(1)

        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(html_text)
        print(f'Saved to {cache_file}')

    # Parse
    from download_ballot_measures import parse_index_page
    measures = parse_index_page(html_text, 2026)
    print(f'Parsed {len(measures)} measures from Ballotpedia')

    # Save parsed JSON
    output_file = '/tmp/ballot_measures_2026_update.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(measures, f, indent=2, ensure_ascii=False)

    return measures


def load_db_measures():
    """Load all 2026 ballot measures from the database."""
    rows = run_sql("""
        SELECT bm.id, s.abbreviation as state, bm.short_title, bm.description,
               bm.measure_type, bm.measure_number, bm.status, bm.subject_category,
               bm.sponsor_type, bm.election_date::text as election_date
        FROM ballot_measures bm
        JOIN states s ON bm.state_id = s.id
        WHERE bm.election_year = 2026
        ORDER BY s.abbreviation, bm.id
    """)
    return rows


def _norm_apos(s):
    """Normalize curly apostrophes/quotes to straight ones."""
    if not s:
        return s
    return s.replace('\u2019', "'").replace('\u2018', "'").replace('\u201c', '"').replace('\u201d', '"')


def normalize_title(title):
    """Normalize a measure title for fuzzy matching."""
    if not title:
        return ''
    t = title.lower().strip()
    # Remove state prefix patterns like "Colorado " or "CO "
    t = re.sub(r'^[a-z]+ ', '', t, count=1)
    # Remove year suffixes like "(2026)"
    t = re.sub(r'\s*\(\d{4}\)\s*$', '', t)
    # Remove common suffixes
    for suffix in ['amendment', 'initiative', 'measure', 'proposition', 'question']:
        pass  # Keep these — they're part of the title
    return t


def match_measures(bp_measures, db_measures):
    """Match BP measures to DB measures, find new/changed/removed."""
    # Index DB measures by (state, normalized_title)
    db_by_key = {}
    for row in db_measures:
        key = (row['state'], _norm_apos(row['short_title']))
        db_by_key[key] = row

    # Also index by (state, description prefix) for fuzzy matching
    db_by_state = {}
    for row in db_measures:
        db_by_state.setdefault(row['state'], []).append(row)

    new_measures = []
    updated_measures = []
    matched_db_ids = set()

    for bp in bp_measures:
        st = STATE_ABBREV.get(bp['state'], bp['state'])

        # Try exact title match first (normalized apostrophes)
        key = (st, _norm_apos(bp['short_title']))
        if key in db_by_key:
            db_row = db_by_key[key]
            matched_db_ids.add(db_row['id'])
            changes = _compare_measure(bp, db_row, st)
            if changes:
                updated_measures.append((db_row, bp, changes))
            continue

        # Try matching by description
        matched = False
        for db_row in db_by_state.get(st, []):
            if db_row['id'] in matched_db_ids:
                continue
            bp_desc = (bp.get('bp_description') or '').lower()
            db_desc = (db_row.get('description') or '').lower()
            if bp_desc and db_desc and (bp_desc in db_desc or db_desc in bp_desc):
                matched_db_ids.add(db_row['id'])
                changes = _compare_measure(bp, db_row, st)
                # Only add title change if actually different
                if db_row['short_title'] != bp['short_title']:
                    changes.append(('title', db_row['short_title'], bp['short_title']))
                if changes:
                    updated_measures.append((db_row, bp, changes))
                matched = True
                break

        if not matched:
            new_measures.append((st, bp))

    # Find DB measures not matched to any BP entry
    removed = [row for row in db_measures if row['id'] not in matched_db_ids]

    return new_measures, updated_measures, removed


def _compare_measure(bp, db_row, state):
    """Compare a BP measure with a DB row, return list of (field, old, new) changes."""
    changes = []

    # Check description
    bp_desc = (bp.get('bp_description') or '').strip()
    db_desc = (db_row.get('description') or '').strip()
    if bp_desc and db_desc and bp_desc not in db_desc:
        # BP description is the core; DB may have extra context appended
        # Only flag if they're substantially different
        if len(bp_desc) > 20 and bp_desc[:30].lower() not in db_desc.lower():
            changes.append(('description', db_desc[:60] + '...', bp_desc[:60] + '...'))

    # Check measure_type
    if bp.get('measure_type') and db_row.get('measure_type'):
        if bp['measure_type'] != db_row['measure_type']:
            changes.append(('measure_type', db_row['measure_type'], bp['measure_type']))

    # Check subject_category — ignore reorderings (same tags, different order)
    bp_subj = bp.get('subject_category') or ''
    db_subj = db_row.get('subject_category') or ''
    if bp_subj and bp_subj != db_subj:
        bp_tags = set(s.strip().lower() for s in bp_subj.split(';'))
        db_tags = set(s.strip().lower() for s in db_subj.split(';'))
        if bp_tags != db_tags:
            changes.append(('subject_category', db_subj, bp_subj))

    return changes


def apply_changes(new_measures, updated_measures, apply=False):
    """Apply changes to the database."""
    if not new_measures and not updated_measures:
        print('\nNo changes to apply.')
        return

    # Get state IDs
    state_rows = run_sql("SELECT id, abbreviation FROM states ORDER BY abbreviation")
    state_ids = {r['abbreviation']: r['id'] for r in state_rows}

    if new_measures:
        print(f'\n--- NEW MEASURES ({len(new_measures)}) ---')
        for st, bp in new_measures:
            print(f'  [{st}] {bp["short_title"]}')
            print(f'        Type: {bp.get("measure_type", "?")}')
            print(f'        Desc: {(bp.get("bp_description") or "")[:80]}')

            if apply:
                state_id = state_ids.get(st)
                if not state_id:
                    print(f'        SKIP: Unknown state {st}')
                    continue

                desc = (bp.get('bp_description') or '').replace("'", "''")
                title = bp['short_title'].replace("'", "''")
                mtype = (bp.get('measure_type') or 'Unknown').replace("'", "''")
                sponsor = (bp.get('sponsor_type') or '').replace("'", "''")
                subj = (bp.get('subject_category') or '').replace("'", "''")
                number = bp.get('measure_number', title).replace("'", "''")

                sql = f"""INSERT INTO ballot_measures
                    (state_id, election_date, election_year, measure_type, measure_number,
                     short_title, description, subject_category, sponsor_type, status)
                    VALUES ({state_id}, '2026-11-03', 2026, '{mtype}', '{number}',
                     '{title}', '{desc}', '{subj}', '{sponsor}', 'On Ballot')"""
                run_sql(sql)
                print(f'        INSERTED')
                time.sleep(1)

    if updated_measures:
        print(f'\n--- UPDATED MEASURES ({len(updated_measures)}) ---')
        for db_row, bp, changes in updated_measures:
            st = db_row['state']
            print(f'  [{st}] {db_row["short_title"][:60]} (id={db_row["id"]})')
            for field, old_val, new_val in changes:
                print(f'        {field}: "{old_val}" → "{new_val}"')

            if apply:
                set_clauses = []
                for field, old_val, new_val in changes:
                    if field == 'title':
                        escaped = new_val.replace("'", "''")
                        set_clauses.append(f"short_title = '{escaped}'")
                        set_clauses.append(f"measure_number = '{escaped}'")
                    elif field == 'description':
                        full_desc = (bp.get('bp_description') or '').replace("'", "''")
                        set_clauses.append(f"description = '{full_desc}'")
                    elif field == 'measure_type':
                        set_clauses.append(f"measure_type = '{new_val}'")
                    elif field == 'subject_category':
                        escaped = new_val.replace("'", "''")
                        set_clauses.append(f"subject_category = '{escaped}'")

                if set_clauses:
                    sql = f"UPDATE ballot_measures SET {', '.join(set_clauses)} WHERE id = {db_row['id']}"
                    run_sql(sql)
                    print(f'        UPDATED')
                    time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description='Update 2026 ballot measures from Ballotpedia')
    parser.add_argument('--apply', action='store_true', help='Apply changes to database (default: dry run)')
    parser.add_argument('--force-download', action='store_true', help='Re-download even if cached today')
    parser.add_argument('--skip-subjects', action='store_true', help='Skip subject category updates (reduces noise)')
    parser.add_argument('--include-subjects', action='store_true', help='Include subject category updates')
    args = parser.parse_args()

    mode = 'APPLY' if args.apply else 'DRY RUN'
    print(f'=== Ballot Measures Update ({mode}) ===\n')

    # 1. Download fresh BP data
    bp_measures = download_2026_measures(force=args.force_download)

    # 2. Load DB measures
    print('\nLoading database measures...')
    db_measures = load_db_measures()
    print(f'Found {len(db_measures)} measures in database')

    # 3. Compare
    print('\nComparing...')
    new_measures, updated_measures, removed = match_measures(bp_measures, db_measures)

    # Filter out subject-only updates unless --include-subjects
    if not args.include_subjects:
        filtered = []
        for db_row, bp, changes in updated_measures:
            non_subj = [c for c in changes if c[0] != 'subject_category']
            if non_subj:
                filtered.append((db_row, bp, non_subj))
        updated_measures = filtered

    # 4. Summary
    print(f'\n=== SUMMARY ===')
    print(f'  BP 2026 measures: {len(bp_measures)}')
    print(f'  DB 2026 measures: {len(db_measures)}')
    print(f'  New (in BP, not DB): {len(new_measures)}')
    print(f'  Updated (changed): {len(updated_measures)}')
    print(f'  Unmatched DB (in DB, not BP): {len(removed)}')

    if removed:
        print(f'\n--- UNMATCHED DB MEASURES ---')
        for row in removed:
            print(f'  [{row["state"]}] id={row["id"]}: {row["short_title"][:60]}')

    # 5. Apply or show
    apply_changes(new_measures, updated_measures, apply=args.apply)

    if not args.apply and (new_measures or updated_measures):
        print(f'\nRun with --apply to make these changes.')

    # 6. States to re-export
    affected_states = set()
    for st, bp in new_measures:
        affected_states.add(st)
    for db_row, bp, changes in updated_measures:
        affected_states.add(db_row['state'])
    if affected_states:
        states_str = ' '.join(f'--state {s}' for s in sorted(affected_states))
        print(f'\nRe-export affected states:')
        for st in sorted(affected_states):
            print(f'  python3 scripts/export_site_data.py --state {st}')


if __name__ == '__main__':
    main()
