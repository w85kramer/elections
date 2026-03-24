"""
Audit NH House district seat counts by comparing DB values against
the official NH SoS Excel files (2024 general election).

The SoS files have "District No. N (seats)" headers with the correct
seat counts from the 2022 redistricting.

Only checks redistricting_cycle='2022' districts.
"""
import os
import re
import sys
import glob

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

try:
    import xlrd
except ImportError:
    xlrd = None

try:
    import openpyxl
except ImportError:
    openpyxl = None

TMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'tmp')

NH_COUNTIES = [
    'belknap', 'carroll', 'cheshire', 'coos', 'grafton',
    'hillsborough', 'merrimack', 'rockingham', 'strafford', 'sullivan',
]


def read_excel(filepath):
    if filepath.endswith('.xls'):
        wb = xlrd.open_workbook(filepath)
        ws = wb.sheet_by_index(0)
        rows = []
        for r in range(ws.nrows):
            row = [ws.cell_value(r, c) for c in range(ws.ncols)]
            rows.append(row)
        return rows
    else:
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active
        return [list(row) for row in ws.iter_rows(values_only=True)]


def extract_seat_counts(filepath, county_name):
    """Extract district -> seat count from an SoS Excel file."""
    rows = read_excel(filepath)
    districts = {}
    for row in rows:
        col0 = str(row[0]).strip() if row[0] is not None else ''
        if 'RECOUNT' in col0.upper():
            continue
        m = re.match(
            r'District(?:\s+No\.?)?\s*(\d+)\s*\((\d+)\)\s*(FL|F)?',
            col0, re.IGNORECASE
        )
        if m:
            dist_num = int(m.group(1))
            num_seats = int(m.group(2))
            is_fl = bool(m.group(3))
            dist_name = f"{county_name.title()}-{dist_num}"
            if is_fl:
                dist_name += ' FL'
            districts[dist_name] = num_seats
    return districts


def main():
    # Parse all 2024 SoS files to get correct seat counts
    sos_seats = {}  # district_name -> num_seats
    sos_flotorials = {}  # floterial districts

    for county in NH_COUNTIES:
        pattern = os.path.join(TMP_DIR, f'2024-ge-house-{county}*')
        matches = glob.glob(pattern)
        if not matches:
            print(f"WARNING: No 2024 file for {county}")
            continue
        filepath = matches[0]
        districts = extract_seat_counts(filepath, county)
        for dist_name, seats in districts.items():
            if dist_name.endswith(' FL'):
                sos_flotorials[dist_name] = seats
            else:
                sos_seats[dist_name] = seats

    sos_total = sum(sos_seats.values())
    print(f"SoS 2024 data: {len(sos_seats)} regular districts, "
          f"{len(sos_flotorials)} floterial districts, "
          f"{sos_total} total seats (regular only)")
    print()

    # Now compare against DB
    from db_config import TOKEN, PROJECT_REF, API_URL
    import httpx

    def run_sql(query):
        resp = httpx.post(
            API_URL,
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=60
        )
        if resp.status_code != 201:
            print(f'SQL ERROR: {resp.text[:300]}')
            return []
        return resp.json()

    db_districts = run_sql("""
        SELECT d.id, d.district_name, d.num_seats, d.is_floterial,
               (SELECT COUNT(*) FROM seats s WHERE s.district_id = d.id) as seat_records
        FROM districts d
        JOIN states st ON d.state_id = st.id
        WHERE st.abbreviation = 'NH'
          AND d.chamber = 'House'
          AND d.redistricting_cycle = '2022'
        ORDER BY d.district_name
    """)

    db_total = sum(d['num_seats'] for d in db_districts)
    print(f"DB (2022 cycle): {len(db_districts)} districts, {db_total} total seats")
    print()

    # Build DB lookup (non-floterial only for comparison)
    db_lookup = {}
    db_flotorials = {}
    for d in db_districts:
        if d['is_floterial']:
            db_flotorials[d['district_name']] = d
        else:
            db_lookup[d['district_name']] = d

    # Compare
    mismatches = []
    missing_from_db = []
    extra_in_db = []

    for dist_name, sos_count in sorted(sos_seats.items()):
        if dist_name in db_lookup:
            db_count = db_lookup[dist_name]['num_seats']
            if db_count != sos_count:
                mismatches.append({
                    'district': dist_name,
                    'db_id': db_lookup[dist_name]['id'],
                    'db_seats': db_count,
                    'db_records': db_lookup[dist_name]['seat_records'],
                    'sos_seats': sos_count,
                    'diff': db_count - sos_count,
                })
        else:
            missing_from_db.append((dist_name, sos_count))

    for dist_name, d in sorted(db_lookup.items()):
        if dist_name not in sos_seats:
            extra_in_db.append((dist_name, d['num_seats'], d['id']))

    # Report
    if mismatches:
        print(f"=== SEAT COUNT MISMATCHES ({len(mismatches)}) ===")
        total_diff = 0
        for m in mismatches:
            sign = '+' if m['diff'] > 0 else ''
            print(f"  {m['district']}: DB={m['db_seats']} (records={m['db_records']}), "
                  f"SoS={m['sos_seats']} ({sign}{m['diff']})")
            total_diff += m['diff']
        print(f"  Net difference: {'+' if total_diff > 0 else ''}{total_diff}")
        print()

    if missing_from_db:
        print(f"=== MISSING FROM DB ({len(missing_from_db)}) ===")
        for name, seats in missing_from_db:
            print(f"  {name}: {seats} seats")
        print()

    if extra_in_db:
        print(f"=== EXTRA IN DB (not in SoS) ({len(extra_in_db)}) ===")
        for name, seats, did in extra_in_db:
            print(f"  {name}: {seats} seats (district_id={did})")
        print()

    if not mismatches and not missing_from_db and not extra_in_db:
        print("All district seat counts match!")
    else:
        # Generate fix SQL
        print("=== FIX SQL ===")
        for m in mismatches:
            print(f"-- {m['district']}: {m['db_seats']} → {m['sos_seats']}")
            print(f"UPDATE districts SET num_seats = {m['sos_seats']} "
                  f"WHERE id = {m['db_id']};")
            if m['db_records'] < m['sos_seats']:
                needed = m['sos_seats'] - m['db_records']
                print(f"-- Need to INSERT {needed} seat records")
            elif m['db_records'] > m['sos_seats']:
                excess = m['db_records'] - m['sos_seats']
                print(f"-- Need to DELETE {excess} excess seat records (check for linked data first!)")
        for name, seats in missing_from_db:
            print(f"-- {name}: needs to be CREATED with {seats} seats")
        for name, seats, did in extra_in_db:
            print(f"-- {name}: may need to be DELETED (district_id={did})")


if __name__ == '__main__':
    main()
