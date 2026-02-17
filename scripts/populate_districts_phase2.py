"""
Phase 2: Insert complex multi-member districts for MD, NH, VT.
Run AFTER populate_districts.py (Phase 1).
"""
import httpx
import sys
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

def run_sql(query):
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': query},
        timeout=60
    )
    if resp.status_code != 201:
        print(f'ERROR: {resp.status_code} - {resp.text[:500]}')
        sys.exit(1)
    return resp.json()

def get_state_id(abbr):
    result = run_sql(f"SELECT id FROM states WHERE abbreviation = '{abbr}'")
    return result[0]['id']

def insert_districts(state_id, chamber, districts_data):
    """Insert a list of (district_label, num_seats) tuples."""
    values = []
    for dist_label, num_seats in districts_data:
        label_escaped = dist_label.replace("'", "''")
        values.append(
            f"({state_id}, 'Legislative', '{chamber}', '{label_escaped}', {num_seats})"
        )
    sql = (
        "INSERT INTO districts (state_id, office_level, chamber, district_number, num_seats) "
        "VALUES " + ",\n".join(values) + "\nRETURNING id;"
    )
    return run_sql(sql)

# ══════════════════════════════════════════════════════════════════════
# MARYLAND House of Delegates — 71 delegate districts, 141 seats
# Source: Maryland State Archives (msa.maryland.gov)
# ══════════════════════════════════════════════════════════════════════
MD_DELEGATE_DISTRICTS = [
    ('1A', 1), ('1B', 1), ('1C', 1),
    ('2A', 2), ('2B', 1),
    ('3', 3),
    ('4', 3),
    ('5', 3),
    ('6', 3),
    ('7A', 2), ('7B', 1),
    ('8', 3),
    ('9A', 2), ('9B', 1),
    ('10', 3),
    ('11A', 1), ('11B', 2),
    ('12A', 2), ('12B', 1),
    ('13', 3),
    ('14', 3),
    ('15', 3),
    ('16', 3),
    ('17', 3),
    ('18', 3),
    ('19', 3),
    ('20', 3),
    ('21', 3),
    ('22', 3),
    ('23', 3),
    ('24', 3),
    ('25', 3),
    ('26', 3),
    ('27A', 1), ('27B', 1), ('27C', 1),
    ('28', 3),
    ('29A', 1), ('29B', 1), ('29C', 1),
    ('30A', 2), ('30B', 1),
    ('31', 3),
    ('32', 3),
    ('33A', 1), ('33B', 1), ('33C', 1),
    ('34A', 2), ('34B', 1),
    ('35A', 2), ('35B', 1),
    ('36', 3),
    ('37A', 1), ('37B', 2),
    ('38A', 1), ('38B', 1), ('38C', 1),
    ('39', 3),
    ('40', 3),
    ('41', 3),
    ('42A', 1), ('42B', 1), ('42C', 1),
    ('43A', 2), ('43B', 1),
    ('44A', 1), ('44B', 2),
    ('45', 3),
    ('46', 3),
    ('47A', 2), ('47B', 1),
]

# ══════════════════════════════════════════════════════════════════════
# NEW HAMPSHIRE House — 200 districts, 400 seats
# Source: HB 50 Amendment 2021-2274h (adopted redistricting plan)
# ══════════════════════════════════════════════════════════════════════
NH_HOUSE_DISTRICTS = [
    # I. Belknap County (8 districts, 18 seats)
    ('Belknap-1', 1), ('Belknap-2', 2), ('Belknap-3', 1), ('Belknap-4', 1),
    ('Belknap-5', 4), ('Belknap-6', 4), ('Belknap-7', 3), ('Belknap-8', 2),
    # II. Carroll County (8 districts, 15 seats)
    ('Carroll-1', 3), ('Carroll-2', 2), ('Carroll-3', 2), ('Carroll-4', 2),
    ('Carroll-5', 1), ('Carroll-6', 2), ('Carroll-7', 1), ('Carroll-8', 2),
    # III. Cheshire County (18 districts, 22 seats)
    ('Cheshire-1', 1), ('Cheshire-2', 1), ('Cheshire-3', 1), ('Cheshire-4', 1),
    ('Cheshire-5', 1), ('Cheshire-6', 2), ('Cheshire-7', 1), ('Cheshire-8', 1),
    ('Cheshire-9', 1), ('Cheshire-10', 2), ('Cheshire-11', 1), ('Cheshire-12', 1),
    ('Cheshire-13', 1), ('Cheshire-14', 1), ('Cheshire-15', 2), ('Cheshire-16', 1),
    ('Cheshire-17', 1), ('Cheshire-18', 2),
    # IV. Coos County (6 districts, 9 seats)
    ('Coos-1', 2), ('Coos-2', 1), ('Coos-3', 1), ('Coos-4', 1),
    ('Coos-5', 3), ('Coos-6', 1),
    # V. Grafton County (18 districts, 26 seats)
    ('Grafton-1', 3), ('Grafton-2', 1), ('Grafton-3', 1), ('Grafton-4', 1),
    ('Grafton-5', 2), ('Grafton-6', 1), ('Grafton-7', 1), ('Grafton-8', 3),
    ('Grafton-9', 1), ('Grafton-10', 1), ('Grafton-11', 1), ('Grafton-12', 4),
    ('Grafton-13', 1), ('Grafton-14', 1), ('Grafton-15', 1), ('Grafton-16', 1),
    ('Grafton-17', 1), ('Grafton-18', 1),
    # VI. Hillsborough County (45 districts, 123 seats)
    ('Hillsborough-1', 4), ('Hillsborough-2', 7), ('Hillsborough-3', 3),
    ('Hillsborough-4', 3), ('Hillsborough-5', 3), ('Hillsborough-6', 3),
    ('Hillsborough-7', 3), ('Hillsborough-8', 3), ('Hillsborough-9', 3),
    ('Hillsborough-10', 3), ('Hillsborough-11', 3), ('Hillsborough-12', 8),
    ('Hillsborough-13', 6), ('Hillsborough-14', 2), ('Hillsborough-15', 2),
    ('Hillsborough-16', 2), ('Hillsborough-17', 2), ('Hillsborough-18', 2),
    ('Hillsborough-19', 2), ('Hillsborough-20', 2), ('Hillsborough-21', 2),
    ('Hillsborough-22', 2), ('Hillsborough-23', 2), ('Hillsborough-24', 2),
    ('Hillsborough-25', 2), ('Hillsborough-26', 2), ('Hillsborough-27', 4),
    ('Hillsborough-28', 1), ('Hillsborough-29', 5), ('Hillsborough-30', 3),
    ('Hillsborough-31', 1), ('Hillsborough-32', 2), ('Hillsborough-33', 2),
    ('Hillsborough-34', 3), ('Hillsborough-35', 2), ('Hillsborough-36', 4),
    ('Hillsborough-37', 4), ('Hillsborough-38', 2), ('Hillsborough-39', 2),
    ('Hillsborough-40', 2), ('Hillsborough-41', 2), ('Hillsborough-42', 1),
    ('Hillsborough-43', 2), ('Hillsborough-44', 1), ('Hillsborough-45', 2),
    # VII. Merrimack County (30 districts, 45 seats)
    ('Merrimack-1', 1), ('Merrimack-2', 1), ('Merrimack-3', 2), ('Merrimack-4', 2),
    ('Merrimack-5', 2), ('Merrimack-6', 1), ('Merrimack-7', 2), ('Merrimack-8', 3),
    ('Merrimack-9', 4), ('Merrimack-10', 4), ('Merrimack-11', 1), ('Merrimack-12', 2),
    ('Merrimack-13', 2), ('Merrimack-14', 1), ('Merrimack-15', 1), ('Merrimack-16', 1),
    ('Merrimack-17', 1), ('Merrimack-18', 1), ('Merrimack-19', 1), ('Merrimack-20', 1),
    ('Merrimack-21', 1), ('Merrimack-22', 1), ('Merrimack-23', 1), ('Merrimack-24', 1),
    ('Merrimack-25', 1), ('Merrimack-26', 1), ('Merrimack-27', 2), ('Merrimack-28', 1),
    ('Merrimack-29', 1), ('Merrimack-30', 1),
    # VIII. Rockingham County (38 districts, 91 seats)
    ('Rockingham-1', 2), ('Rockingham-2', 3), ('Rockingham-3', 4), ('Rockingham-4', 1),
    ('Rockingham-5', 1), ('Rockingham-6', 1), ('Rockingham-7', 4), ('Rockingham-8', 2),
    ('Rockingham-9', 1), ('Rockingham-10', 1), ('Rockingham-11', 2), ('Rockingham-12', 2),
    ('Rockingham-13', 3), ('Rockingham-14', 3), ('Rockingham-15', 2), ('Rockingham-16', 1),
    ('Rockingham-17', 5), ('Rockingham-18', 2), ('Rockingham-19', 10), ('Rockingham-20', 9),
    ('Rockingham-21', 4), ('Rockingham-22', 7), ('Rockingham-23', 1), ('Rockingham-24', 1),
    ('Rockingham-25', 1), ('Rockingham-26', 1), ('Rockingham-27', 2), ('Rockingham-28', 3),
    ('Rockingham-29', 2), ('Rockingham-30', 1), ('Rockingham-31', 1), ('Rockingham-32', 1),
    ('Rockingham-33', 1), ('Rockingham-34', 1), ('Rockingham-35', 1), ('Rockingham-36', 1),
    ('Rockingham-37', 1), ('Rockingham-38', 2),
    # IX. Strafford County (21 districts, 38 seats)
    ('Strafford-1', 2), ('Strafford-2', 3), ('Strafford-3', 1), ('Strafford-4', 3),
    ('Strafford-5', 1), ('Strafford-6', 1), ('Strafford-7', 1), ('Strafford-8', 1),
    ('Strafford-9', 1), ('Strafford-10', 4), ('Strafford-11', 3), ('Strafford-12', 4),
    ('Strafford-13', 1), ('Strafford-14', 1), ('Strafford-15', 1), ('Strafford-16', 1),
    ('Strafford-17', 1), ('Strafford-18', 1), ('Strafford-19', 3), ('Strafford-20', 1),
    ('Strafford-21', 3),
    # X. Sullivan County (8 districts, 13 seats)
    ('Sullivan-1', 1), ('Sullivan-2', 1), ('Sullivan-3', 3), ('Sullivan-4', 1),
    ('Sullivan-5', 1), ('Sullivan-6', 3), ('Sullivan-7', 1), ('Sullivan-8', 2),
]

# ══════════════════════════════════════════════════════════════════════
# VERMONT House — 109 districts (68 single + 41 two-member), 150 seats
# Source: VT Legislature 2025-26 House District List (official PDF)
# ══════════════════════════════════════════════════════════════════════
VT_HOUSE_DISTRICTS = [
    # Addison County (6 districts, 9 seats)
    ('Addison-1', 2), ('Addison-2', 1), ('Addison-3', 2),
    ('Addison-4', 2), ('Addison-5', 1), ('Addison-Rutland', 1),
    # Bennington County (6 districts, 9 seats)
    ('Bennington-1', 1), ('Bennington-2', 2), ('Bennington-3', 1),
    ('Bennington-4', 2), ('Bennington-5', 2), ('Bennington-Rutland', 1),
    # Caledonia County (5 districts, 7 seats)
    ('Caledonia-1', 1), ('Caledonia-2', 1), ('Caledonia-3', 2),
    ('Caledonia-Essex', 2), ('Caledonia-Washington', 1),
    # Chittenden County (26 districts, 39 seats)
    ('Chittenden-1', 1), ('Chittenden-2', 2), ('Chittenden-3', 2),
    ('Chittenden-4', 1), ('Chittenden-5', 1), ('Chittenden-6', 1),
    ('Chittenden-7', 1), ('Chittenden-8', 1), ('Chittenden-9', 1),
    ('Chittenden-10', 1), ('Chittenden-11', 1), ('Chittenden-12', 1),
    ('Chittenden-13', 2), ('Chittenden-14', 2), ('Chittenden-15', 2),
    ('Chittenden-16', 2), ('Chittenden-17', 1), ('Chittenden-18', 2),
    ('Chittenden-19', 2), ('Chittenden-20', 2), ('Chittenden-21', 2),
    ('Chittenden-22', 2), ('Chittenden-23', 2), ('Chittenden-24', 1),
    ('Chittenden-25', 1), ('Chittenden-Franklin', 2),
    # Essex County (2 districts, 2 seats)
    ('Essex-Caledonia', 1), ('Essex-Orleans', 1),
    # Franklin County (8 districts, 11 seats)
    ('Franklin-1', 2), ('Franklin-2', 1), ('Franklin-3', 1),
    ('Franklin-4', 2), ('Franklin-5', 2), ('Franklin-6', 1),
    ('Franklin-7', 1), ('Franklin-8', 1),
    # Grand Isle County (1 district, 2 seats)
    ('Grand Isle-Chittenden', 2),
    # Lamoille County (4 districts, 6 seats)
    ('Lamoille-1', 1), ('Lamoille-2', 2), ('Lamoille-3', 1),
    ('Lamoille-Washington', 2),
    # Orange County (5 districts, 6 seats)
    ('Orange-1', 1), ('Orange-2', 1), ('Orange-3', 1),
    ('Orange-Caledonia', 1), ('Orange-Washington-Addison', 2),
    # Orleans County (5 districts, 6 seats)
    ('Orleans-1', 1), ('Orleans-2', 1), ('Orleans-3', 1),
    ('Orleans-4', 1), ('Orleans-Lamoille', 2),
    # Rutland County (13 districts, 14 seats)
    ('Rutland-1', 1), ('Rutland-2', 2), ('Rutland-3', 1),
    ('Rutland-4', 1), ('Rutland-5', 1), ('Rutland-6', 1),
    ('Rutland-7', 1), ('Rutland-8', 1), ('Rutland-9', 1),
    ('Rutland-10', 1), ('Rutland-11', 1), ('Rutland-Bennington', 1),
    ('Rutland-Windsor', 1),
    # Washington County (8 districts, 14 seats)
    ('Washington-1', 2), ('Washington-2', 2), ('Washington-3', 2),
    ('Washington-4', 2), ('Washington-5', 1), ('Washington-6', 1),
    ('Washington-Chittenden', 2), ('Washington-Orange', 2),
    # Windham County (10 districts, 11 seats)
    ('Windham-1', 1), ('Windham-2', 1), ('Windham-3', 2),
    ('Windham-4', 1), ('Windham-5', 1), ('Windham-6', 1),
    ('Windham-7', 1), ('Windham-8', 1), ('Windham-9', 1),
    ('Windham-Windsor-Bennington', 1),
    # Windsor County (10 districts, 14 seats)
    ('Windsor-1', 2), ('Windsor-2', 1), ('Windsor-3', 2),
    ('Windsor-4', 1), ('Windsor-5', 1), ('Windsor-6', 2),
    ('Windsor-Addison', 1), ('Windsor-Orange-1', 1),
    ('Windsor-Orange-2', 2), ('Windsor-Windham', 1),
]

# ══════════════════════════════════════════════════════════════════════
# VALIDATE & INSERT
# ══════════════════════════════════════════════════════════════════════

def validate_and_insert(abbr, chamber, data, expected_districts, expected_seats):
    """Validate counts and insert districts."""
    actual_districts = len(data)
    actual_seats = sum(n for _, n in data)
    print(f"\n{abbr} {chamber}:")
    print(f"  Districts: {actual_districts} (expected {expected_districts})")
    print(f"  Seats: {actual_seats} (expected {expected_seats})")

    if actual_seats != expected_seats:
        print(f"  ERROR: Seat count mismatch!")
        return False

    state_id = get_state_id(abbr)

    # Check for existing districts in this chamber
    existing = run_sql(
        f"SELECT COUNT(*) as cnt FROM districts d "
        f"JOIN states s ON d.state_id = s.id "
        f"WHERE s.abbreviation = '{abbr}' AND d.chamber = '{chamber}'"
    )
    if existing[0]['cnt'] > 0:
        print(f"  WARNING: {existing[0]['cnt']} districts already exist — skipping")
        return True

    result = insert_districts(state_id, chamber, data)
    print(f"  Inserted {len(result)} districts")
    return True

# ── Maryland House of Delegates ───────────────────────────────────────
validate_and_insert('MD', 'House of Delegates', MD_DELEGATE_DISTRICTS, 71, 141)

# ── New Hampshire House ───────────────────────────────────────────────
if NH_HOUSE_DISTRICTS:
    validate_and_insert('NH', 'House', NH_HOUSE_DISTRICTS, 200, 400)
else:
    print("\nNH House: SKIPPED (data not yet populated)")

# ── Vermont House ─────────────────────────────────────────────────────
if VT_HOUSE_DISTRICTS:
    validate_and_insert('VT', 'House', VT_HOUSE_DISTRICTS, 109, 150)
else:
    print("\nVT House: SKIPPED (data not yet populated)")

# ── Final verification ────────────────────────────────────────────────
totals = run_sql("""
    SELECT COUNT(*) as total_districts, SUM(num_seats) as total_seats
    FROM districts
""")
print(f"\nGrand total in database: {totals[0]['total_districts']} districts, {totals[0]['total_seats']} seats")
