#!/usr/bin/env python3
"""
Fix non-standard party codes in the candidacies table.

Many party values were ingested as truncated or full-length party names
instead of standard abbreviations. This script normalizes them.

Usage:
    python3 scripts/fix_party_codes.py              # Execute
    python3 scripts/fix_party_codes.py --dry-run    # Show SQL only
"""

import sys
import os
import time
import argparse

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL


def run_sql(query, retries=5):
    for attempt in range(retries):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        sys.exit(1)


# Mapping of bad party codes → standard abbreviation.
# Codes are matched case-sensitively except where noted.
PARTY_FIXES = {
    # Standard parties with alternate spellings
    'Dem': 'D',
    'Rep': 'R',
    'Ind': 'I',
    'Lib': 'L',
    'Gre': 'G',
    'Non': 'NP',
    'NPA': 'NP',      # No Party Affiliation (FL)

    # Constitution Party variants
    'Con': 'CP',
    'Constitution': 'CP',
    'Conservati': 'CP',  # Truncated "Conservative" in NJ — actually Constitution Party candidates

    # Reform Party variants
    'Ref': 'REF',
    'REF': 'REF',
    'Reform/Independence': 'REF',
    'IP': 'REF',       # Independence Party (MN) — Reform Party affiliate

    # Truncated state-name parties → these are actually D or R
    # but we can't auto-map without context. Map to the actual minor party name.
    # Many of these are state-specific party lines that cross-endorse D or R candidates.

    # Green Party variants
    'Gre': 'G',

    # Libertarian variants (already handled above)

    # Write-in designations → keep as 'W' (write-in marker)
    'Wri': 'W',

    # Peace & Freedom Party
    'Pea': 'PF',

    # Socialist parties
    'Soc': 'SOC',
    'Socialist ': 'SOC',  # trailing space
    'SL': 'SL',           # Socialist Labor — already short, keep
    'SW': 'SW',           # Socialist Workers — already short, keep

    # Mountain Party (WV)
    'Mou': 'MTP',
    'MP': 'MTP',

    # American Party / American Independent
    'Ame': 'AMP',

    # Prohibition Party
    'Pro': 'PRO',

    # Natural Law Party
    'Nat': 'NL',
    'NL': 'NL',

    # United Independent Party
    'Uni': 'UIP',

    # Raza Unida (TX)
    'Raz': 'RU',

    # Human Rights Party (MI)
    'Hum': 'HRP',

    # NOTA — None of These Candidates (NV ballot line, not a real party)
    'NOTA': 'NOTA',  # Keep as-is, it's a valid special code

    # IA — Independent American Party (NV)
    'IA': 'IAP',

    # Moderate Party
    'Mod': 'MOD',

    # Communist Party
    'Com': 'COM',

    # A Connecticut Party
    'A C': 'ACP',
    'A Connecticut': 'ACP',

    # Grassroots Party (MN)
    'Gra': 'GRP',

    # Earth Party? (MN)
    'Ear': 'EAR',

    # Stonewall (AZ — unknown minor party)
    'Sto': 'OTH',

    # Other — generic catch-all
    'Other': 'OTH',
}

# State-name truncations that need context-specific mapping.
# These are state-specific party lines (e.g., "Minnesota DFL" = D, "Arizona Libertarian" = L).
# We'll query each to determine the correct mapping.
STATE_TRUNCATIONS = {
    # (bad_code, state) → correct_party
    ('Min', 'MN'): 'D',    # Minnesota DFL = Democrat
    ('Ari', 'AZ'): 'L',    # Arizona Libertarian Party
    ('Mic', 'MI'): 'D',    # Michigan Democratic
    ('Wis', 'WI'): 'G',    # Wisconsin Green
    ('Iow', 'IA'): 'OTH',  # Iowa Party (minor)
    ('Ver', 'VT'): 'OTH',  # Vermont Grassroots
    ('Was', 'WA'): 'OTH',  # Washington Party
    ('Nor', 'ND'): 'I',    # HTML artifact, candidate was Independent
}

# Per-candidate fixes for state-name truncations where party varies by candidate.
CANDIDATE_SPECIFIC_FIXES = {
    # MS 2023: "Mis" = Mississippi — not a real party
    ('Tate Reeves', 'MS', 'Mis'): 'R',
    ('Brandon Presley', 'MS', 'Mis'): 'D',
    # CA: "Cal" = California Democratic/Republican party line
    ('William F. Knowland', 'CA', 'Cal'): 'R',
    ('Pat Brown', 'CA', 'Cal'): 'D',
    ('Jesse M. Unruh', 'CA', 'Cal'): 'D',
    ('Kate Brown', 'CA', 'Cal'): 'D',
    ('Phil Angelides', 'CA', 'Cal'): 'D',
    # WV: "Wes" = West Virginia party line
    ('Patrick Morrisey', 'WV', 'Wes'): 'R',
    ('Steve Williams', 'WV', 'Wes'): 'D',
}


def main():
    parser = argparse.ArgumentParser(description='Fix non-standard party codes')
    parser.add_argument('--dry-run', action='store_true', help='Show SQL without executing')
    args = parser.parse_args()

    # Step 1: Query current state of bad party codes
    print('Querying non-standard party codes...')
    q = """
        SELECT cy.id, cy.party, c.full_name, st.abbreviation as state,
               e.election_year, e.election_type, se.office_type
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE cy.party NOT IN ('D','R','I','L','G','NP','AIP','CP','WF','C','F','MOD','PF','W','VET')
          AND cy.party IS NOT NULL
          AND cy.party != ''
        ORDER BY st.abbreviation, e.election_year DESC
    """

    if args.dry_run:
        print('  Would query for bad party codes...')
        # Show the planned mappings
        print(f'\n  Direct mappings ({len(PARTY_FIXES)}):')
        for old, new in sorted(PARTY_FIXES.items()):
            print(f'    {old!r:25s} → {new}')
        print(f'\n  State-specific mappings ({len(STATE_TRUNCATIONS)}):')
        for (old, st), new in sorted(STATE_TRUNCATIONS.items()):
            print(f'    {old!r} in {st:5s} → {new}')
        return

    rows = run_sql(q)
    print(f'  Found {len(rows)} candidacies with non-standard party codes.')

    # Step 2: Build UPDATE statements
    updates = []  # (candidacy_id, old_party, new_party, candidate_name, state)
    unresolved = []

    for r in rows:
        cid = r['id']
        old = r['party']
        state = r['state']
        name = r['full_name']

        # Check candidate-specific mapping first
        new = CANDIDATE_SPECIFIC_FIXES.get((name, state, old))
        if new is None:
            # Check state-specific mapping
            new = STATE_TRUNCATIONS.get((old, state))
        if new is None:
            # Check direct mapping
            new = PARTY_FIXES.get(old)
        if new is None:
            # Try trimmed version
            new = PARTY_FIXES.get(old.strip())

        if new and new != old:
            updates.append((cid, old, new, name, state))
        elif new is None:
            unresolved.append((cid, old, name, state, r['election_year']))

    print(f'  Resolved: {len(updates)} updates')
    if unresolved:
        print(f'  Unresolved: {len(unresolved)} candidacies:')
        for cid, old, name, state, year in unresolved:
            print(f'    {state} {year}: {name} party={old!r}')

    if not updates:
        print('  Nothing to update.')
        return

    # Step 3: Execute updates in batches
    # Group by old→new mapping for efficient batch updates
    from collections import defaultdict
    batches = defaultdict(list)  # (old, new) → [ids]
    for cid, old, new, name, state in updates:
        batches[(old, new)].append(cid)

    print(f'\n  Executing {len(batches)} batch updates...')
    total_updated = 0
    for (old, new), ids in sorted(batches.items()):
        id_list = ','.join(str(i) for i in ids)
        sql = f"UPDATE candidacies SET party = '{new}' WHERE id IN ({id_list})"
        print(f'    {old!r:25s} → {new:5s} ({len(ids)} rows)')
        run_sql(sql)
        total_updated += len(ids)

    # Step 4: Also fix the HTML artifact in ND candidate name
    print('\n  Fixing HTML artifacts in candidate names...')
    html_fix = run_sql("""
        SELECT id, full_name FROM candidates
        WHERE full_name LIKE '%&lt;%' OR full_name LIKE '%<br%' OR full_name LIKE '%&amp;%'
        LIMIT 20
    """)
    if html_fix:
        for r in html_fix:
            print(f'    Found: id={r["id"]} name={r["full_name"]!r}')
            # Clean up common HTML entities
            clean = r['full_name'].replace('&lt;br', '').replace('&lt;', '').replace('&gt;', '').replace('&amp;', '&').strip()
            if clean != r['full_name']:
                sql = f"UPDATE candidates SET full_name = '{clean.replace(chr(39), chr(39)+chr(39))}' WHERE id = {r['id']}"
                print(f'      → {clean!r}')
                run_sql(sql)
    else:
        print('    None found.')

    # Step 5: Fix empty-string parties → NULL
    print('\n  Setting empty-string parties to NULL...')
    empty_fix = run_sql("UPDATE candidacies SET party = NULL WHERE party = '' RETURNING id")
    print(f'    Fixed {len(empty_fix)} rows.')

    print(f'\n  Done. Updated {total_updated} party codes + {len(empty_fix)} empty→NULL.')


if __name__ == '__main__':
    main()
