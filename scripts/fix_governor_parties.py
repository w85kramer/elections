#!/usr/bin/env python3
"""
Fix bad party codes in governor JSON data files.

Corrects:
- Truncated party codes (Min→D, Mic→D, Gra→GLC, etc.)
- Inconsistent codes (Ind→I, Dem→D, Rep→R, Lib→L, Gre→G)
- MN candidates named "Democratic (DFL)" → look up actual candidate name
- MD 2022 Wes Moore result: Lost → Won, Dan Cox: Lost stays Lost
- ME "Ind" → "I"

Usage:
    python3 scripts/fix_governor_parties.py --dry-run
    python3 scripts/fix_governor_parties.py
"""

import os
import sys
import json
import glob
import argparse

SITE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'site', 'data', 'governors')

# Map of truncated/inconsistent party codes to correct values
PARTY_FIXES = {
    # Major party variants → standard code
    'Min': 'D',    # Minnesota DFL
    'Mic': 'D',    # Michigan Democratic Party
    'Cal': 'D',    # California Democratic Party (context-dependent, checked below)
    'Dem': 'D',    # Democratic
    'Rep': 'R',    # Republican
    'Lib': 'L',    # Libertarian
    'Gre': 'G',    # Green
    'Ind': 'I',    # Independent
    'Wes': 'R',    # West Virginia Republican (Morrisey is R; Williams is D - handled specially)
    'Mis': 'R',    # Mississippi Republican (Tate Reeves is R; Presley is D - handled specially)
    'Nor': 'D',    # North Dakota Dem-NPL
    'Ver': 'D',    # Vermont Democratic
    'Was': 'R',    # Washington state (Emmett Anderson was R)
    'Wis': 'G',    # Wisconsin Green
    'Iow': 'I',    # Iowa (Jonathan Narcisse ran as independent)
    'Ari': 'L',    # Arizona Libertarian
    # Third parties
    'Gra': 'GLC',  # Grassroots-Legalize Cannabis
    'Mou': 'Mtn',  # Mountain Party (WV)
    'Ear': 'Other', # Minor party
    'Sto': 'L',    # Stoner (AZ - actually Libertarian Barry Goldwater Jr supporter)
    'Hum': 'HRP',  # Human Rights Party
    'Raz': 'RU',   # Raza Unida
    'Com': 'Comp',  # Compassion Party
    'Mod': 'Mod',  # Moderate (keep as-is)
    'Nat': 'D',    # National Dem Party of Alabama (D affiliate)
    'New': 'NAP',  # New Alliance Party
    'Pro': 'Pro',  # Prohibition (keep as-is)
    'Soc': 'Soc',  # Socialist
    'Uni': 'UIP',  # United Independent
    'Pea': 'PFP',  # Peace and Freedom
    'Wri': 'W/I',  # Write-in
    'Ame': 'AIP',  # American Party / American Independent
    'A C': 'I',    # A Connecticut Party
    'SL': 'SL',    # Socialist Labor (keep)
    'SW': 'SW',    # Socialist Workers (keep)
    'REF': 'Reform',  # Reform Party
    'Ala': 'AIP',  # Alaskan Independence
}

# Special per-candidate overrides (state, year, candidate name fragment → correct party)
CANDIDATE_PARTY_OVERRIDES = {
    # WV 2024: Morrisey is R, Williams & Linko-Looper are D/Mtn
    ('WV', 2024, 'Morrisey'): 'R',
    ('WV', 2024, 'Williams'): 'D',
    ('WV', 2024, 'Linko-Looper'): 'Mtn',
    ('WV', 2020, 'Lutz'): 'Mtn',
    # MS 2023: Reeves is R, Presley is D
    ('MS', 2023, 'Reeves'): 'R',
    ('MS', 2023, 'Presley'): 'D',
    # CA: Phil Angelides is D, but some CA candidates are R
    ('CA', 1994, 'Brown'): 'D',  # Kathleen Brown
    ('CA', 2006, 'Angelides'): 'D',
    ('CA', 2006, 'Camejo'): 'G',
    # AZ 1990: Max Hawkins
    ('AZ', 1990, 'Hawkins'): 'Other',
    # IA 2010: Jonathan Narcisse ran as Iowa Party
    ('IA', 2010, 'Narcisse'): 'I',
    # WI: some are D, some are G
    ('WI', 2006, 'Eisman'): 'G',
    ('WI', 2002, 'Young'): 'L',
}

# Specific candidate name fixes (state, year, wrong_name) → {name, party}
CANDIDATE_NAME_FIXES = {
    ('MI', 1982, 'Tisch Independent Citizens'): {'name': 'Robert Tisch', 'party': 'I'},
}

# MN candidates with "Democratic (DFL)" as name — map year to actual candidate name
MN_DFL_CANDIDATES = {
    1960: {'name': 'Orville Freeman', 'party': 'D'},
    1966: {'name': 'Karl Rolvaag', 'party': 'D'},
    1976: {'name': 'Rudy Perpich', 'party': 'D'},
    1994: {'name': 'John Marty', 'party': 'D'},
    1998: {'name': 'Hubert H. Humphrey III', 'party': 'D'},
    2002: {'name': 'Roger Moe', 'party': 'D'},
}


def fix_candidate_party(state, year, cand):
    """Fix a single candidate's party code. Returns True if changed."""
    changed = False
    old_party = cand.get('party', '')

    # Check for specific candidate overrides first
    for (st, yr, name_frag), correct_party in CANDIDATE_PARTY_OVERRIDES.items():
        if state == st and year == yr and name_frag in cand.get('name', ''):
            if old_party != correct_party:
                cand['party'] = correct_party
                return True

    # Apply general party fixes
    if old_party in PARTY_FIXES:
        cand['party'] = PARTY_FIXES[old_party]
        changed = (cand['party'] != old_party)

    return changed


def fix_state(state, data, dry_run=False):
    """Fix all party codes in a state's governor data. Returns count of changes."""
    changes = 0

    # Fix HTML artifacts in candidate names (e.g., "Kelly Armstrong&lt;br")
    for e in data.get('elections', []):
        for cand in e.get('candidates', []):
            name = cand.get('name', '')
            if '&lt;' in name or '&gt;' in name or '&amp;' in name:
                cleaned = name.replace('&lt;br', '').replace('&lt;', '').replace('&gt;', '').replace('&amp;', '&').strip()
                if cleaned != name:
                    if dry_run:
                        print(f"  {state} {e['year']}: Fixed name '{name}' → '{cleaned}'")
                    cand['name'] = cleaned
                    changes += 1

    # Fix elections
    for e in data.get('elections', []):
        for cand in e.get('candidates', []):
            # Specific candidate name fixes
            name_key = (state, e['year'], cand.get('name', ''))
            if name_key in CANDIDATE_NAME_FIXES:
                fix = CANDIDATE_NAME_FIXES[name_key]
                if dry_run:
                    print(f"  {state} {e['year']}: Fixed name '{cand['name']}' → '{fix['name']}' (party → '{fix['party']}')")
                cand['name'] = fix['name']
                cand['party'] = fix['party']
                changes += 1
                continue

            # MN: Fix "Democratic (DFL)" candidate names
            if state == 'MN' and cand.get('name') == 'Democratic (DFL)' and e['year'] in MN_DFL_CANDIDATES:
                fix = MN_DFL_CANDIDATES[e['year']]
                if dry_run:
                    print(f"  {state} {e['year']}: Would fix candidate name '{cand['name']}' → '{fix['name']}' (party: '{cand.get('party','')}' → '{fix['party']}')")
                cand['name'] = fix['name']
                cand['party'] = fix['party']
                changes += 1
                continue

            if fix_candidate_party(state, e['year'], cand):
                if dry_run:
                    print(f"  {state} {e['year']}: {cand['name']} party fix applied")
                changes += 1

    # Fix MD 2022: Wes Moore result Lost → Won, and set winner correctly
    if state == 'MD':
        for e in data.get('elections', []):
            if e['year'] == 2022 and e.get('type') == 'General':
                for cand in e.get('candidates', []):
                    if 'Moore' in cand.get('name', '') and cand.get('result') == 'Lost':
                        cand['result'] = 'Won'
                        changes += 1
                        if dry_run:
                            print(f"  MD 2022: Fixed Wes Moore result Lost → Won")

    # Fix HTML artifacts in timeline names
    for t in data.get('timeline', []):
        name = t.get('name', '')
        if '&lt;' in name or '&gt;' in name:
            cleaned = name.replace('&lt;br', '').replace('&lt;', '').replace('&gt;', '').strip()
            if cleaned != name:
                t['name'] = cleaned
                changes += 1

    # Fix timeline party codes
    for t in data.get('timeline', []):
        old = t.get('party', '')
        if old in PARTY_FIXES:
            t['party'] = PARTY_FIXES[old]
            if t['party'] != old:
                changes += 1
                if dry_run:
                    print(f"  {state} timeline: {t.get('name','')} party '{old}' → '{t['party']}'")

    # Recalculate timeline margins for MD 2022 (since we fixed the winner)
    if state == 'MD':
        generals_by_year = {}
        for e in data.get('elections', []):
            if e.get('type') == 'General':
                winner = next((c for c in e.get('candidates', []) if c.get('result') == 'Won'), None)
                if winner and winner.get('pct') is not None:
                    margin = round((float(winner['pct']) - 50) * 2, 1)
                    generals_by_year[e['year']] = {
                        'margin': margin,
                        'winner_party': winner.get('party'),
                    }

        for t in data.get('timeline', []):
            if t.get('election_year') == 2022 and t.get('margin') is None:
                if 2022 in generals_by_year:
                    t['margin'] = generals_by_year[2022]['margin']
                    changes += 1
                    if dry_run:
                        print(f"  MD 2022 timeline: Added margin {t['margin']} for {t.get('name','')}")

    # Fix current_governor party
    gov = data.get('current_governor')
    if gov:
        old = gov.get('party', '')
        if old in PARTY_FIXES:
            gov['party'] = PARTY_FIXES[old]
            if gov['party'] != old:
                changes += 1

    return changes


def main():
    parser = argparse.ArgumentParser(description='Fix governor party codes in JSON data')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Fix single state')
    args = parser.parse_args()

    files = sorted(glob.glob(os.path.join(SITE_DATA_DIR, '*.json')))
    if args.state:
        files = [f for f in files if os.path.basename(f) == f'{args.state.upper()}.json']

    total_changes = 0
    states_changed = 0

    for fpath in files:
        state = os.path.basename(fpath).replace('.json', '')
        with open(fpath) as f:
            data = json.load(f)

        changes = fix_state(state, data, dry_run=args.dry_run)

        if changes > 0:
            total_changes += changes
            states_changed += 1
            print(f'  {state}: {changes} fixes')

            if not args.dry_run:
                with open(fpath, 'w') as f:
                    json.dump(data, f, separators=(',', ':'))

    print(f'\nTotal: {total_changes} fixes across {states_changed} states')
    if args.dry_run:
        print('(dry run — no files changed)')


if __name__ == '__main__':
    main()
