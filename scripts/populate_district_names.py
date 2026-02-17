"""
Populate district_name for all 6,857 districts.

Sets display names like 'HD-1', 'SD-3', 'AD-80', '1st Barnstable' based on
chamber type and state-specific conventions.

Naming conventions:
  - House         → HD-{n}
  - Senate        → SD-{n}
  - Assembly      → AD-{n}
  - House of Del. → HD-{n}
  - Legislature   → LD-{n}
  - MA House/Sen  → canonical named districts (e.g. '1st Barnstable')
  - NH House      → keep as-is (e.g. 'Belknap-1')
  - VT all        → keep as-is (e.g. 'Addison-1')
  - Statewide     → 'Statewide'

Usage:
    python3 scripts/populate_district_names.py
    python3 scripts/populate_district_names.py --dry-run
"""
import sys
import time
import argparse

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

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
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None

def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")

# ══════════════════════════════════════════════════════════════════
# MA CANONICAL DISTRICT LISTS (from populate_seat_terms_ma.py)
# Sorted alphabetically → district_number = index+1
# ══════════════════════════════════════════════════════════════════

MA_HOUSE_DISTRICTS_BP = [
    "1st Barnstable", "1st Berkshire", "1st Bristol", "1st Essex", "1st Franklin",
    "1st Hampden", "1st Hampshire", "1st Middlesex", "1st Norfolk", "1st Plymouth",
    "1st Suffolk", "1st Worcester",
    "2nd Barnstable", "2nd Berkshire", "2nd Bristol", "2nd Essex", "2nd Franklin",
    "2nd Hampden", "2nd Hampshire", "2nd Middlesex", "2nd Norfolk", "2nd Plymouth",
    "2nd Suffolk", "2nd Worcester",
    "3rd Barnstable", "3rd Berkshire", "3rd Bristol", "3rd Essex",
    "3rd Hampden", "3rd Hampshire", "3rd Middlesex", "3rd Norfolk", "3rd Plymouth",
    "3rd Suffolk", "3rd Worcester",
    "4th Barnstable", "4th Bristol", "4th Essex", "4th Hampden",
    "4th Middlesex", "4th Norfolk", "4th Plymouth", "4th Suffolk", "4th Worcester",
    "5th Barnstable", "5th Bristol", "5th Essex", "5th Hampden",
    "5th Middlesex", "5th Norfolk", "5th Plymouth", "5th Suffolk", "5th Worcester",
    "6th Bristol", "6th Essex", "6th Hampden", "6th Middlesex",
    "6th Norfolk", "6th Plymouth", "6th Suffolk", "6th Worcester",
    "7th Bristol", "7th Essex", "7th Hampden", "7th Middlesex",
    "7th Norfolk", "7th Plymouth", "7th Suffolk", "7th Worcester",
    "8th Bristol", "8th Essex", "8th Hampden", "8th Middlesex",
    "8th Norfolk", "8th Plymouth", "8th Suffolk", "8th Worcester",
    "9th Bristol", "9th Essex", "9th Hampden", "9th Middlesex",
    "9th Norfolk", "9th Plymouth", "9th Suffolk", "9th Worcester",
    "10th Bristol", "10th Essex", "10th Hampden", "10th Middlesex",
    "10th Norfolk", "10th Plymouth", "10th Suffolk", "10th Worcester",
    "11th Bristol", "11th Essex", "11th Hampden", "11th Middlesex",
    "11th Norfolk", "11th Plymouth", "11th Suffolk", "11th Worcester",
    "12th Bristol", "12th Essex", "12th Hampden", "12th Middlesex",
    "12th Norfolk", "12th Plymouth", "12th Suffolk", "12th Worcester",
    "13th Bristol", "13th Essex", "13th Middlesex", "13th Norfolk",
    "13th Suffolk", "13th Worcester",
    "14th Bristol", "14th Essex", "14th Middlesex", "14th Norfolk",
    "14th Suffolk", "14th Worcester",
    "15th Essex", "15th Middlesex", "15th Norfolk", "15th Suffolk", "15th Worcester",
    "16th Essex", "16th Middlesex", "16th Suffolk", "16th Worcester",
    "17th Essex", "17th Middlesex", "17th Suffolk", "17th Worcester",
    "18th Essex", "18th Middlesex", "18th Suffolk", "18th Worcester",
    "19th Middlesex", "19th Suffolk", "19th Worcester",
    "20th Middlesex", "21st Middlesex", "22nd Middlesex", "23rd Middlesex",
    "24th Middlesex", "25th Middlesex", "26th Middlesex", "27th Middlesex",
    "28th Middlesex", "29th Middlesex", "30th Middlesex", "31st Middlesex",
    "32nd Middlesex", "33rd Middlesex", "34th Middlesex", "35th Middlesex",
    "36th Middlesex", "37th Middlesex",
    "Barnstable, Dukes, and Nantucket",
]

MA_SENATE_DISTRICTS_BP = [
    "1st Bristol and Plymouth", "1st Essex", "1st Essex and Middlesex",
    "1st Middlesex", "1st Plymouth and Norfolk", "1st Suffolk", "1st Worcester",
    "2nd Bristol and Plymouth", "2nd Essex", "2nd Essex and Middlesex",
    "2nd Middlesex", "2nd Plymouth and Norfolk", "2nd Suffolk", "2nd Worcester",
    "3rd Bristol and Plymouth", "3rd Essex", "3rd Middlesex", "3rd Suffolk",
    "4th Middlesex", "5th Middlesex",
    "Berkshire, Hampden, Franklin, and Hampshire",
    "Bristol and Norfolk", "Cape and Islands",
    "Hampden", "Hampden and Hampshire", "Hampden, Hampshire, and Worcester",
    "Hampshire, Franklin, and Worcester",
    "Middlesex and Norfolk", "Middlesex and Suffolk", "Middlesex and Worcester",
    "Norfolk and Middlesex", "Norfolk and Plymouth", "Norfolk and Suffolk",
    "Norfolk, Plymouth, and Bristol", "Norfolk, Worcester, and Middlesex",
    "Plymouth and Barnstable",
    "Suffolk and Middlesex",
    "Worcester and Hampden", "Worcester and Hampshire", "Worcester and Middlesex",
]

def build_number_to_name_maps():
    """
    Sort district names alphabetically and build district_number → display_name.
    Returns (house_map, senate_map) where each is {district_number_str: bp_name}.
    """
    house_sorted = sorted(MA_HOUSE_DISTRICTS_BP)
    senate_sorted = sorted(MA_SENATE_DISTRICTS_BP)

    assert len(house_sorted) == 160, f"Expected 160 House, got {len(house_sorted)}"
    assert len(senate_sorted) == 40, f"Expected 40 Senate, got {len(senate_sorted)}"

    house_map = {str(i + 1): name for i, name in enumerate(house_sorted)}
    senate_map = {str(i + 1): name for i, name in enumerate(senate_sorted)}

    return house_map, senate_map

def main():
    parser = argparse.ArgumentParser(description='Populate district_name for all districts')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done')
    args = parser.parse_args()

    if args.dry_run:
        print('DRY RUN MODE')

    # ── Step 1: Check current state ─────────────────────────────
    print("=" * 60)
    print("STEP 1: Check current state")
    print("=" * 60)

    counts = run_sql("""
        SELECT
            COUNT(*) as total,
            COUNT(district_name) as named
        FROM districts
    """)
    print(f"  Total districts: {counts[0]['total']}")
    print(f"  Already named: {counts[0]['named']}")

    # ── Step 2: Build MA name maps ──────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2: Build MA district_number → display_name maps")
    print("=" * 60)

    ma_house_map, ma_senate_map = build_number_to_name_maps()
    print(f"  MA House: {len(ma_house_map)} mappings")
    print(f"  MA Senate: {len(ma_senate_map)} mappings")
    print(f"  Examples: district 1 → '{ma_house_map['1']}', district 160 → '{ma_house_map['160']}'")
    print(f"  Senate: district 1 → '{ma_senate_map['1']}', district 40 → '{ma_senate_map['40']}'")

    if args.dry_run:
        print("\n  DRY RUN — would execute 5 UPDATE statements")
        print("\n  1. Standard chambers (House→HD-, Senate→SD-, Assembly→AD-, HoD→HD-, Legislature→LD-)")
        print("  2. MA House: 160 individual name updates")
        print("  3. MA Senate: 40 individual name updates")
        print("  4. NH House + VT: set district_name = district_number")
        print("  5. Statewide: set district_name = 'Statewide'")
        return

    # ── Step 3: Statewide districts ─────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 3: Statewide districts → 'Statewide'")
    print("=" * 60)

    result = run_sql("""
        UPDATE districts
        SET district_name = 'Statewide'
        WHERE office_level = 'Statewide'
        RETURNING id;
    """)
    print(f"  Updated {len(result)} statewide districts")

    # ── Step 4: NH House + VT (all chambers) → keep as-is ──────
    print("\n" + "=" * 60)
    print("STEP 4: NH House + VT → district_name = district_number")
    print("=" * 60)

    result = run_sql("""
        UPDATE districts
        SET district_name = district_number
        WHERE id IN (
            SELECT d.id FROM districts d
            JOIN states s ON d.state_id = s.id
            WHERE d.office_level = 'Legislative'
              AND (
                  (s.abbreviation = 'NH' AND d.chamber = 'House')
                  OR s.abbreviation = 'VT'
              )
        )
        RETURNING id;
    """)
    print(f"  Updated {len(result)} districts (NH House + VT)")

    # ── Step 5: Standard chambers ───────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 5: Standard chambers (bulk prefix update)")
    print("=" * 60)

    result = run_sql("""
        UPDATE districts
        SET district_name = CASE chamber
            WHEN 'House' THEN 'HD-' || district_number
            WHEN 'Senate' THEN 'SD-' || district_number
            WHEN 'Assembly' THEN 'AD-' || district_number
            WHEN 'House of Delegates' THEN 'HD-' || district_number
            WHEN 'Legislature' THEN 'LD-' || district_number
        END
        WHERE office_level = 'Legislative'
          AND district_name IS NULL
          AND state_id NOT IN (
              SELECT id FROM states WHERE abbreviation = 'MA'
          )
        RETURNING id;
    """)
    print(f"  Updated {len(result)} standard districts")

    # ── Step 6: MA House ────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 6: MA House → canonical names")
    print("=" * 60)

    # Build CASE statement for MA House
    case_parts = []
    for num, name in ma_house_map.items():
        case_parts.append(f"WHEN '{num}' THEN '{esc(name)}'")
    case_sql = " ".join(case_parts)

    result = run_sql(f"""
        UPDATE districts
        SET district_name = CASE district_number {case_sql} END
        WHERE id IN (
            SELECT d.id FROM districts d
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'MA' AND d.chamber = 'House'
        )
        RETURNING id;
    """)
    print(f"  Updated {len(result)} MA House districts")

    # ── Step 7: MA Senate ───────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 7: MA Senate → canonical names")
    print("=" * 60)

    case_parts = []
    for num, name in ma_senate_map.items():
        case_parts.append(f"WHEN '{num}' THEN '{esc(name)}'")
    case_sql = " ".join(case_parts)

    result = run_sql(f"""
        UPDATE districts
        SET district_name = CASE district_number {case_sql} END
        WHERE id IN (
            SELECT d.id FROM districts d
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = 'MA' AND d.chamber = 'Senate'
        )
        RETURNING id;
    """)
    print(f"  Updated {len(result)} MA Senate districts")

    # ── Verification ────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Overall counts
    counts = run_sql("""
        SELECT
            COUNT(*) as total,
            COUNT(district_name) as named,
            COUNT(*) - COUNT(district_name) as unnamed
        FROM districts
    """)
    c = counts[0]
    print(f"  Total districts: {c['total']}")
    print(f"  Named: {c['named']}")
    print(f"  Unnamed: {c['unnamed']}")

    if c['unnamed'] > 0:
        unnamed = run_sql("""
            SELECT s.abbreviation, d.chamber, d.district_number, d.office_level
            FROM districts d
            JOIN states s ON d.state_id = s.id
            WHERE d.district_name IS NULL
            LIMIT 10
        """)
        print("  Unnamed examples:")
        for r in unnamed:
            print(f"    {r['abbreviation']} {r['chamber']} {r['district_number']} ({r['office_level']})")

    # Spot checks
    print("\n  Spot checks:")
    spots = run_sql("""
        SELECT s.abbreviation, d.chamber, d.district_number, d.district_name
        FROM districts d
        JOIN states s ON d.state_id = s.id
        WHERE d.office_level = 'Legislative'
        ORDER BY RANDOM()
        LIMIT 15
    """)
    for r in spots:
        print(f"    {r['abbreviation']} {r['chamber']} #{r['district_number']} → {r['district_name']}")

    # Key verification states
    print("\n  Key state checks:")
    for st, ch, dn in [
        ('MI', 'Senate', '1'), ('MI', 'House', '110'),
        ('MA', 'House', '1'), ('MA', 'Senate', '23'),
        ('NH', 'House', 'Belknap-1'), ('NH', 'Senate', '1'),
        ('VT', 'House', 'Addison-1'), ('VT', 'Senate', 'Addison'),
        ('NE', 'Legislature', '1'), ('CA', 'Assembly', '1'),
        ('MD', 'House of Delegates', '1A'),
    ]:
        rows = run_sql(f"""
            SELECT d.district_name
            FROM districts d
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation = '{st}' AND d.chamber = '{ch}'
              AND d.district_number = '{dn}'
        """)
        name = rows[0]['district_name'] if rows else 'NOT FOUND'
        print(f"    {st} {ch} #{dn} → {name}")

    print("\nDone!")

if __name__ == '__main__':
    main()
