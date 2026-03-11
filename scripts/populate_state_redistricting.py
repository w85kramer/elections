#!/usr/bin/env python3
"""
Populate state_redistricting table with redistricting cycle data.

Derives effective years from existing districts.redistricting_cycle values,
then adds known mid-decade court-ordered redraws for state legislative maps.

Usage:
    python3 scripts/populate_state_redistricting.py              # Execute
    python3 scripts/populate_state_redistricting.py --dry-run    # Show SQL only
"""

import sys
import os
import json
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


def main():
    parser = argparse.ArgumentParser(description='Populate state_redistricting table')
    parser.add_argument('--dry-run', action='store_true', help='Show SQL without executing')
    args = parser.parse_args()

    # Step 1: Get all state+chamber combos and their redistricting cycles from districts table
    print('Querying existing redistricting cycles from districts table...')
    q = """
        SELECT DISTINCT
            s.id as state_id,
            s.abbreviation,
            d.chamber,
            d.redistricting_cycle
        FROM states s
        JOIN districts d ON d.state_id = s.id
        WHERE d.office_level = 'Legislative'
          AND d.redistricting_cycle IS NOT NULL
          AND d.redistricting_cycle != 'permanent'
        ORDER BY s.abbreviation, d.chamber, d.redistricting_cycle
    """

    if args.dry_run:
        print(f'  Would run query:\n{q}')

    rows = run_sql(q) if not args.dry_run else []

    # Build set of (state_id, abbreviation, chamber, effective_year) from existing data
    # Each redistricting_cycle value IS the effective year (first election under new maps)
    cycle_entries = set()
    state_chambers = {}  # (state_id, abbr) -> set of chambers

    for r in rows:
        state_id = r['state_id']
        abbr = r['abbreviation']
        chamber = r['chamber']
        cycle = r['redistricting_cycle']

        state_chambers.setdefault((state_id, abbr), set()).add(chamber)

        try:
            effective_year = int(cycle)
            cycle_entries.add((state_id, abbr, chamber, effective_year))
        except (ValueError, TypeError):
            continue

    # Step 2: For each state+chamber, ensure we have both the 2010-census and 2020-census cycles.
    # The districts table mostly has '2022' (current) and '2012' (old-era) entries.
    # But some states may only have current-era districts in the DB.
    # Standard post-2010 census: 2012 for most, 2011 for VA/NJ (odd-year elections)
    # Standard post-2020 census: 2022 for most, 2021 for VA/NJ
    for (state_id, abbr), chambers in state_chambers.items():
        for chamber in chambers:
            existing_years = {y for (sid, a, c, y) in cycle_entries
                             if sid == state_id and c == chamber}

            # Determine standard post-2010 year
            if abbr in ('VA', 'NJ'):
                post_2010 = 2011
                post_2020 = 2021
            else:
                post_2010 = 2012
                post_2020 = 2022

            # Add post-2010 if not already present (and no other year in 2011-2013 range)
            has_2010_era = any(2011 <= y <= 2013 for y in existing_years)
            if not has_2010_era:
                cycle_entries.add((state_id, abbr, chamber, post_2010))

            # Add post-2020 if not already present (and no other year in 2021-2023 range)
            has_2020_era = any(2021 <= y <= 2023 for y in existing_years)
            if not has_2020_era:
                cycle_entries.add((state_id, abbr, chamber, post_2020))

    # Step 3: Determine census_year and is_mid_decade for each entry
    insert_rows = []
    for (state_id, abbr, chamber, ey) in sorted(cycle_entries):
        if 2011 <= ey <= 2013:
            census_year = 2010
            is_mid_decade = False
        elif 2021 <= ey <= 2023:
            census_year = 2020
            is_mid_decade = False
        else:
            # Could be mid-decade or older — mark as mid-decade if between census cycles
            census_year = None
            is_mid_decade = False  # will be overridden for known mid-decade below

        insert_rows.append((state_id, abbr, chamber, ey, census_year, is_mid_decade, None))

    # Step 4: Add known mid-decade court-ordered redraws (state legislative maps only)
    mid_decade_redraws = [
        # NC House + Senate → effective 2018 (Covington v. NC, racial gerrymander)
        ('NC', 'House', 2018, None, True, 'Covington v. NC — 28 districts redrawn (racial gerrymander)'),
        ('NC', 'Senate', 2018, None, True, 'Covington v. NC — racial gerrymander'),
        # VA House of Delegates → effective 2019 (Bethune-Hill, racial gerrymander)
        ('VA', 'House of Delegates', 2019, None, True, 'Bethune-Hill v. VA Board of Elections — 11 districts redrawn'),
        # AL House + Senate → effective 2018 (federal court, racial gerrymander)
        ('AL', 'House', 2018, None, True, 'Federal court — 12 districts redrawn (racial gerrymander)'),
        ('AL', 'Senate', 2018, None, True, 'Federal court — racial gerrymander'),
        # MS House + Senate → effective 2025 (federal court VRA order, new majority-Black districts)
        ('MS', 'House', 2025, None, True, 'Federal court VRA order — new majority-Black districts required'),
        ('MS', 'Senate', 2025, None, True, 'Federal court VRA order — 2 new majority-Black Senate districts required'),
    ]

    # Look up state_ids for mid-decade entries
    state_id_lookup = {abbr: sid for (sid, abbr), _ in state_chambers.items()}

    for (abbr, chamber, ey, census_year, is_mid, notes) in mid_decade_redraws:
        sid = state_id_lookup.get(abbr)
        if sid is None:
            print(f'  WARNING: No state_id for {abbr}, skipping mid-decade entry')
            continue
        # Check if already exists
        exists = any(r[0] == sid and r[2] == chamber and r[3] == ey for r in insert_rows)
        if not exists:
            insert_rows.append((sid, abbr, chamber, ey, census_year, is_mid, notes))
        else:
            # Update existing entry to mark as mid-decade
            insert_rows = [
                (sid2, a, c, y, cy2, True if (sid2 == sid and c == chamber and y == ey) else mid2,
                 notes if (sid2 == sid and c == chamber and y == ey) else n)
                for (sid2, a, c, y, cy2, mid2, n) in insert_rows
            ]

    # Sort for clean output
    insert_rows.sort(key=lambda r: (r[1], r[2], r[3]))  # by abbr, chamber, year

    # Step 5: Build and execute INSERT
    print(f'\nPrepared {len(insert_rows)} rows for state_redistricting table.')

    values_parts = []
    for (state_id, abbr, chamber, ey, census_year, is_mid, notes) in insert_rows:
        cy_sql = str(census_year) if census_year else 'NULL'
        notes_sql = f"'{notes}'" if notes else 'NULL'
        # Escape single quotes in notes
        if notes:
            notes_sql = f"'{notes.replace(chr(39), chr(39)+chr(39))}'"
        values_parts.append(
            f"({state_id}, '{chamber}', {ey}, {cy_sql}, {str(is_mid).upper()}, {notes_sql})"
        )

    joiner = ',\n            '
    values_sql = joiner.join(values_parts)
    sql = f"""
        INSERT INTO state_redistricting (state_id, chamber, effective_year, census_year, is_mid_decade, notes)
        VALUES
            {values_sql}
        ON CONFLICT (state_id, chamber, effective_year) DO UPDATE SET
            census_year = EXCLUDED.census_year,
            is_mid_decade = EXCLUDED.is_mid_decade,
            notes = EXCLUDED.notes;
    """

    if args.dry_run:
        print(f'\n  SQL ({len(values_parts)} rows):')
        # Show first and last few
        lines = sql.strip().split('\n')
        for line in lines[:10]:
            print(f'    {line}')
        if len(lines) > 20:
            print(f'    ... ({len(lines) - 20} more lines) ...')
            for line in lines[-10:]:
                print(f'    {line}')
        else:
            for line in lines[10:]:
                print(f'    {line}')
        return

    print('  Inserting rows...')
    result = run_sql(sql)
    print(f'  Done. Inserted/updated {len(insert_rows)} rows.')

    # Verify
    verify = run_sql("SELECT COUNT(*) as cnt FROM state_redistricting")
    print(f'  Verification: {verify[0]["cnt"]} total rows in state_redistricting.')


if __name__ == '__main__':
    main()
