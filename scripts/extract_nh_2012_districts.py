#!/usr/bin/env python3
"""
Extract NH House 2012-cycle district list from 2020 SoS Excel files.

Parses all 10 county files to get district numbers, seat counts, and floterial status.
Then cross-references against the current DB (2022-cycle) districts.

Usage:
    python3 scripts/extract_nh_2012_districts.py
    python3 scripts/extract_nh_2012_districts.py --json  # Output JSON for create_nh_old_districts.py
"""
import os
import sys
import json
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

# Reuse the SoS parser
from scripts.parse_nh_sos import (
    find_county_file, parse_county_file, run_sql, NH_COUNTIES, TMP_DIR
)


def extract_old_districts(year=2020):
    """Parse SoS files and extract the district list with seat counts."""
    districts = {}  # district_number -> {num_seats, is_floterial}

    for county in NH_COUNTIES:
        filepath = find_county_file(county, year)
        if not filepath:
            print(f'  WARNING: No file for {county}')
            continue

        parsed = parse_county_file(filepath, county)
        county_title = county.title()

        for d in parsed:
            dist_num = f"{county_title}-{d['sos_district_num']}"
            fl_suffix = ' FL' if d['is_floterial'] else ''
            districts[dist_num] = {
                'district_number': dist_num,
                'num_seats': d['num_seats'],
                'is_floterial': d['is_floterial'],
            }

    return districts


def load_current_districts():
    """Load current (2022-cycle) NH House districts from DB."""
    result = run_sql("""
        SELECT d.district_number, d.num_seats, d.is_floterial, d.id as district_id
        FROM districts d
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH'
          AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
        ORDER BY d.district_number
    """)
    if not result:
        return {}
    return {r['district_number']: r for r in result}


def main():
    parser = argparse.ArgumentParser(description='Extract NH 2012-cycle district list')
    parser.add_argument('--json', action='store_true', help='Output JSON for downstream scripts')
    args = parser.parse_args()

    print('Extracting 2012-cycle districts from 2020 SoS files...\n')
    old_districts = extract_old_districts(2020)
    print(f'Found {len(old_districts)} districts in SoS files\n')

    print('Loading current (2022-cycle) districts from DB...')
    current_districts = load_current_districts()
    print(f'Found {len(current_districts)} current districts\n')

    # Classify each old district
    same_seats = []
    diff_seats = []
    old_only = []

    for dist_num in sorted(old_districts.keys()):
        old = old_districts[dist_num]
        cur = current_districts.get(dist_num)

        if cur is None:
            old_only.append(old)
        elif old['num_seats'] == cur['num_seats'] and old['is_floterial'] == cur['is_floterial']:
            same_seats.append({**old, 'current_seats': cur['num_seats']})
        else:
            diff_seats.append({
                **old,
                'current_seats': cur['num_seats'],
                'current_floterial': cur['is_floterial'],
            })

    # Districts in current map but not in old map
    current_only = []
    for dist_num in sorted(current_districts.keys()):
        if dist_num not in old_districts:
            current_only.append(current_districts[dist_num])

    if args.json:
        output = {
            'old_districts': list(old_districts.values()),
            'same_seats': same_seats,
            'diff_seats': diff_seats,
            'old_only': old_only,
            'current_only': current_only,
        }
        json_path = '/tmp/nh_2012_districts.json'
        with open(json_path, 'w') as f:
            json.dump(output, f, indent=2)
        print(f'Written to {json_path}')
        return

    # Summary
    old_total_seats = sum(d['num_seats'] for d in old_districts.values())
    print(f'=== Summary ===')
    print(f'Total old-era districts: {len(old_districts)}')
    print(f'Total old-era seats: {old_total_seats}')
    print(f'Same seat count: {len(same_seats)}')
    print(f'Different seat count: {len(diff_seats)}')
    print(f'Old-only (not in 2022 map): {len(old_only)}')
    print(f'Current-only (not in 2012 map): {len(current_only)}')

    if diff_seats:
        print(f'\n=== Seat Count Changes ===')
        for d in diff_seats:
            fl_old = ' FL' if d['is_floterial'] else ''
            fl_cur = ' FL' if d.get('current_floterial') else ''
            print(f"  {d['district_number']}: {d['num_seats']} seats{fl_old} → {d['current_seats']} seats{fl_cur}")

    if old_only:
        print(f'\n=== Old-Only Districts (not in 2022 map) ===')
        for d in old_only:
            fl = ' FL' if d['is_floterial'] else ''
            print(f"  {d['district_number']}: {d['num_seats']} seats{fl}")

    if current_only:
        print(f'\n=== Current-Only Districts (not in 2012 map) ===')
        for d in current_only:
            fl = ' FL' if d.get('is_floterial') else ''
            print(f"  {d['district_number']}: {d['num_seats']} seats{fl}")


if __name__ == '__main__':
    main()
