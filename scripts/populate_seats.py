"""
Populate the seats table with one row per seat across all legislative districts.

For multi-member districts, creates multiple seat records (Seat A, Seat B, etc.).
Sets office_type, seat_label, term_length, election_class, and next_regular_election_year
based on per-state election schedules researched from Ballotpedia.

Total expected: 7,385 seats across 6,806 districts in 50 states.
"""
import httpx
import sys

TOKEN = 'sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7'
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'


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


# ── Chamber → office_type mapping ────────────────────────────────────
OFFICE_TYPE = {
    'Senate': 'State Senate',
    'House': 'State House',
    'Assembly': 'State House',
    'House of Delegates': 'State House',
    'Legislature': 'State Legislature',
}

# ── Chamber display names for seat labels ────────────────────────────
LABEL_CHAMBER = {
    'Senate': 'Senate',
    'House': 'House',
    'Assembly': 'Assembly',
    'House of Delegates': 'HoD',
    'Legislature': 'Legislature',
}

# ══════════════════════════════════════════════════════════════════════
# ELECTION SCHEDULE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

# A. 2-year term senates — all seats every cycle, no election_class
#    next_regular_election_year derived from cycle
TWO_YEAR_SENATES = {'AZ', 'CT', 'GA', 'ID', 'MA', 'ME', 'NC', 'NH', 'NY', 'RI', 'SD', 'VT'}

# B. 4-year NON-STAGGERED senates — all seats same year, no election_class
NONSTAGGERED_SENATE_NEXT = {
    'AL': 2026, 'KS': 2028, 'MD': 2026, 'MI': 2026, 'MN': 2026,
    'NM': 2028, 'SC': 2028,
    # Odd-year states
    'LA': 2027, 'MS': 2027, 'NJ': 2027, 'VA': 2027,
}

# C. 4-year STAGGERED senates — odd districts in 2026
ODD_IN_2026_SENATES = {'IA', 'ND', 'OH', 'TN', 'WI', 'WY'}

# C. 4-year STAGGERED senates — even districts in 2026
EVEN_IN_2026_SENATES = {'CA', 'FL', 'KY', 'MO', 'NE', 'OK', 'PA'}

# C. 4-year STAGGERED senates — explicit district lists (from Ballotpedia)
EXPLICIT_2026_SENATE = {
    'AK': {1, 3, 5, 7, 9, 11, 13, 15, 17, 19},
    'AR': {2, 7, 9, 10, 11, 13, 14, 15, 16, 21, 24, 27, 28, 30, 31, 32, 35},
    'CO': {1, 3, 4, 5, 7, 8, 9, 11, 15, 20, 22, 24, 25, 27, 30, 32, 34, 35},
    'DE': {1, 5, 7, 8, 9, 12, 13, 14, 15, 19, 20},
    'HI': {2, 5, 8, 9, 10, 11, 13, 14, 15, 17, 20, 21, 25},
    'IN': {1, 4, 6, 11, 14, 15, 17, 19, 21, 22, 23, 25, 26, 27, 29, 31, 38, 39, 41, 43, 45, 46, 47, 48, 49},
    'MT': {1, 4, 6, 8, 9, 10, 11, 12, 14, 18, 19, 22, 23, 25, 28, 29, 31, 32, 34, 41, 42, 43, 48, 49, 50},
    'NV': {2, 8, 9, 10, 12, 13, 14, 16, 17, 20, 21},
    'OR': {3, 4, 6, 7, 8, 10, 11, 13, 15, 16, 17, 19, 20, 24, 26},
    'TX': {1, 2, 3, 4, 5, 9, 11, 13, 18, 19, 21, 22, 24, 26, 28, 31},
    'UT': {1, 5, 6, 7, 9, 11, 12, 13, 14, 18, 19, 20, 21, 23, 28},
    'WA': {6, 7, 8, 13, 15, 21, 26, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 42, 43, 44, 45, 46, 47, 48},
}

# D. House/lower chamber — mostly 2-year terms, all seats every cycle
#    4-year house exceptions:
FOUR_YEAR_HOUSE_NEXT = {
    'AL': 2026, 'MD': 2026,
    'LA': 2027, 'MS': 2027,
}
# ND House: 4-year staggered, odd districts 2026
ND_HOUSE_STAGGERED = True

# Odd-year 2-year house states (next election 2027 not 2026)
ODD_YEAR_HOUSE = {'VA', 'NJ'}

# ══════════════════════════════════════════════════════════════════════
# HELPER: Determine election info for a seat
# ══════════════════════════════════════════════════════════════════════

def parse_district_int(district_number):
    """Try to parse district_number as an integer, return None if not numeric."""
    try:
        return int(district_number)
    except (ValueError, TypeError):
        return None


def get_senate_election_info(abbr, district_number):
    """
    Returns (election_class, next_regular_election_year) for a senate seat.
    election_class: None, '1', '2', or for IL: 'A', 'B', 'C'
    """
    # 2-year term senates: all up every cycle
    if abbr in TWO_YEAR_SENATES:
        return (None, 2026)

    # Non-staggered 4-year
    if abbr in NONSTAGGERED_SENATE_NEXT:
        return (None, NONSTAGGERED_SENATE_NEXT[abbr])

    # IL: 3-class system
    if abbr == 'IL':
        d = parse_district_int(district_number)
        if d is not None:
            if d % 3 == 1:
                return ('A', 2028)
            elif d % 3 == 2:
                return ('B', 2026)
            else:  # d % 3 == 0
                return ('C', 2026)
        return (None, 2026)

    # WV: within-district stagger (handled at seat level, not here)
    # This function is called per-seat for WV, see generate_seats()
    if abbr == 'WV':
        # Seat A = class 1 (2026), Seat B = class 2 (2028)
        # Handled by caller passing seat_index
        return (None, None)  # Placeholder, overridden in generate_seats

    # Odd districts in 2026
    if abbr in ODD_IN_2026_SENATES:
        d = parse_district_int(district_number)
        if d is not None:
            if d % 2 == 1:
                return ('1', 2026)
            else:
                return ('2', 2028)
        return (None, 2026)

    # Even districts in 2026
    if abbr in EVEN_IN_2026_SENATES:
        d = parse_district_int(district_number)
        if d is not None:
            if d % 2 == 0:
                return ('1', 2026)
            else:
                return ('2', 2028)
        return (None, 2026)

    # Explicit district lists
    if abbr in EXPLICIT_2026_SENATE:
        d = parse_district_int(district_number)
        if d is not None:
            if d in EXPLICIT_2026_SENATE[abbr]:
                return ('1', 2026)
            else:
                return ('2', 2028)
        return (None, 2026)

    # Fallback — shouldn't reach here for any known state
    print(f"  WARNING: No senate election config for {abbr}, defaulting to 2026")
    return (None, 2026)


def get_house_election_info(abbr, district_number):
    """
    Returns (election_class, next_regular_election_year) for a house/assembly seat.
    """
    # 4-year non-staggered house
    if abbr in FOUR_YEAR_HOUSE_NEXT:
        return (None, FOUR_YEAR_HOUSE_NEXT[abbr])

    # ND House: 4-year staggered, odd districts 2026
    if abbr == 'ND':
        d = parse_district_int(district_number)
        if d is not None:
            if d % 2 == 1:
                return ('1', 2026)
            else:
                return ('2', 2028)
        return (None, 2026)

    # Odd-year 2-year house (VA, NJ)
    if abbr in ODD_YEAR_HOUSE:
        return (None, 2027)

    # Standard 2-year house: all seats up 2026
    return (None, 2026)


# ══════════════════════════════════════════════════════════════════════
# MAIN SCRIPT
# ══════════════════════════════════════════════════════════════════════

# ── Guard: abort if seats table is non-empty ─────────────────────────
existing = run_sql("SELECT COUNT(*) as cnt FROM seats")
if existing[0]['cnt'] > 0:
    print(f"WARNING: seats table already has {existing[0]['cnt']} rows!")
    print("Aborting to prevent duplicates.")
    sys.exit(1)

# ── Load all districts joined with states ────────────────────────────
print("Loading districts and states...")
districts_data = run_sql("""
    SELECT d.id as district_id, d.chamber, d.district_number, d.num_seats,
           s.abbreviation, s.senate_term_years, s.house_term_years
    FROM districts d
    JOIN states s ON d.state_id = s.id
    ORDER BY s.abbreviation, d.chamber, d.district_number
""")
print(f"Loaded {len(districts_data)} districts")

# ── Generate seat records ────────────────────────────────────────────
DESIGNATORS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

seats = []

for dist in districts_data:
    district_id = dist['district_id']
    chamber = dist['chamber']
    district_number = dist['district_number']
    num_seats = dist['num_seats']
    abbr = dist['abbreviation']
    senate_term = dist['senate_term_years']
    house_term = dist['house_term_years']

    office_type = OFFICE_TYPE[chamber]
    label_chamber = LABEL_CHAMBER[chamber]

    # Term length
    if chamber == 'Senate':
        term_length = senate_term
    elif chamber == 'Legislature':
        term_length = senate_term  # NE unicameral uses senate_term_years
    else:
        term_length = house_term

    for seat_idx in range(num_seats):
        # Seat designator (A, B, C...) for multi-member; NULL for single
        if num_seats > 1:
            designator = DESIGNATORS[seat_idx]
        else:
            designator = None

        # Seat label
        if num_seats > 1:
            seat_label = f"{abbr} {label_chamber} {district_number} Seat {designator}"
        else:
            seat_label = f"{abbr} {label_chamber} {district_number}"

        # Election info
        if chamber == 'Senate':
            if abbr == 'WV':
                # WV: within-district stagger
                # Seat A (idx 0) = class 1, next 2026
                # Seat B (idx 1) = class 2, next 2028
                if seat_idx == 0:
                    election_class = '1'
                    next_election = 2026
                else:
                    election_class = '2'
                    next_election = 2028
            else:
                election_class, next_election = get_senate_election_info(abbr, district_number)
        elif chamber == 'Legislature':
            # NE unicameral — uses senate election schedule
            election_class, next_election = get_senate_election_info(abbr, district_number)
        else:
            # House / Assembly / House of Delegates
            election_class, next_election = get_house_election_info(abbr, district_number)

        seats.append((
            district_id,
            'Legislative',
            office_type,
            seat_label,
            designator,
            term_length,
            election_class,
            next_election,
        ))

print(f"Generated {len(seats)} seat records")

if len(seats) != 7385:
    print(f"WARNING: Expected 7,385 seats, got {len(seats)}")
    # Don't abort — the count may be slightly different; verify after insert

# ── Batch insert ─────────────────────────────────────────────────────
BATCH_SIZE = 300
total_inserted = 0

for batch_start in range(0, len(seats), BATCH_SIZE):
    batch = seats[batch_start:batch_start + BATCH_SIZE]
    values = []
    for (district_id, office_level, office_type, seat_label, designator,
         term_length, election_class, next_election) in batch:
        # Escape single quotes in seat_label
        label_escaped = seat_label.replace("'", "''")
        desig_sql = f"'{designator}'" if designator else 'NULL'
        eclass_sql = f"'{election_class}'" if election_class else 'NULL'
        next_sql = str(next_election) if next_election else 'NULL'
        values.append(
            f"({district_id}, '{office_level}', '{office_type}', "
            f"'{label_escaped}', {desig_sql}, {term_length}, "
            f"{eclass_sql}, {next_sql})"
        )

    sql = (
        "INSERT INTO seats (district_id, office_level, office_type, seat_label, "
        "seat_designator, term_length_years, election_class, next_regular_election_year) "
        "VALUES " + ",\n".join(values) + "\nRETURNING id;"
    )

    result = run_sql(sql)
    total_inserted += len(result)
    batch_num = batch_start // BATCH_SIZE + 1
    total_batches = (len(seats) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  Batch {batch_num}/{total_batches}: +{len(result)} (total: {total_inserted})")

print(f"\nInserted {total_inserted} seats")

# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# 1. Total count
total = run_sql("SELECT COUNT(*) as cnt FROM seats")
print(f"\n1. Total seats: {total[0]['cnt']} (expected 7,385)")

# 2. Per-state seat counts vs district sums
print("\n2. Per-state seat counts (mismatches only):")
state_check = run_sql("""
    WITH expected AS (
        SELECT s.abbreviation, SUM(d.num_seats) as expected_seats
        FROM districts d JOIN states s ON d.state_id = s.id
        GROUP BY s.abbreviation
    ), actual AS (
        SELECT s.abbreviation, COUNT(*) as actual_seats
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        GROUP BY s.abbreviation
    )
    SELECT e.abbreviation, e.expected_seats, COALESCE(a.actual_seats, 0) as actual_seats
    FROM expected e LEFT JOIN actual a ON e.abbreviation = a.abbreviation
    ORDER BY e.abbreviation
""")
mismatches = 0
for row in state_check:
    if row['expected_seats'] != row['actual_seats']:
        print(f"  MISMATCH {row['abbreviation']}: expected {row['expected_seats']}, got {row['actual_seats']}")
        mismatches += 1
if mismatches == 0:
    print("  All 50 states match!")

# 3. Election class distribution for staggered states
print("\n3. Election class distribution (staggered senates):")
class_check = run_sql("""
    SELECT s.abbreviation, se.election_class, COUNT(*) as cnt
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE se.election_class IS NOT NULL
    GROUP BY s.abbreviation, se.election_class
    ORDER BY s.abbreviation, se.election_class
""")
current_state = None
for row in class_check:
    if row['abbreviation'] != current_state:
        current_state = row['abbreviation']
        print(f"  {current_state}:", end='')
    print(f"  class {row['election_class']}={row['cnt']}", end='')
print()

# 4. NULL checks
print("\n4. NULL checks:")
null_check = run_sql("""
    SELECT
        SUM(CASE WHEN office_type IS NULL THEN 1 ELSE 0 END) as null_type,
        SUM(CASE WHEN seat_label IS NULL THEN 1 ELSE 0 END) as null_label,
        SUM(CASE WHEN term_length_years IS NULL THEN 1 ELSE 0 END) as null_term,
        SUM(CASE WHEN next_regular_election_year IS NULL THEN 1 ELSE 0 END) as null_next
    FROM seats
""")
nc = null_check[0]
print(f"  Null office_type: {nc['null_type']}")
print(f"  Null seat_label: {nc['null_label']}")
print(f"  Null term_length: {nc['null_term']}")
print(f"  Null next_election: {nc['null_next']}")

# 5. Spot checks
print("\n5. Spot checks:")
spots = [
    ("OH Senate odd (2026)", "s.abbreviation='OH' AND d.chamber='Senate' AND d.district_number='1'"),
    ("OH Senate even (2028)", "s.abbreviation='OH' AND d.chamber='Senate' AND d.district_number='2'"),
    ("CA Senate even (2026)", "s.abbreviation='CA' AND d.chamber='Senate' AND d.district_number='2'"),
    ("CA Senate odd (2028)", "s.abbreviation='CA' AND d.chamber='Senate' AND d.district_number='1'"),
    ("AL Senate (all 2026)", "s.abbreviation='AL' AND d.chamber='Senate' AND d.district_number='1'"),
    ("VA HoD (2027)", "s.abbreviation='VA' AND d.chamber='House of Delegates' AND d.district_number='1'"),
    ("NE Legislature (even=2026)", "s.abbreviation='NE' AND d.chamber='Legislature' AND d.district_number='2'"),
    ("WV Senate d1 Seat A", "s.abbreviation='WV' AND d.chamber='Senate' AND d.district_number='1' AND se.seat_designator='A'"),
    ("WV Senate d1 Seat B", "s.abbreviation='WV' AND d.chamber='Senate' AND d.district_number='1' AND se.seat_designator='B'"),
    ("IL Senate d2 (class B, 2026)", "s.abbreviation='IL' AND d.chamber='Senate' AND d.district_number='2'"),
    ("IL Senate d1 (class A, 2028)", "s.abbreviation='IL' AND d.chamber='Senate' AND d.district_number='1'"),
    ("ND House odd (2026)", "s.abbreviation='ND' AND d.chamber='House' AND d.district_number='1' AND se.seat_designator='A'"),
    ("ND House even (2028)", "s.abbreviation='ND' AND d.chamber='House' AND d.district_number='2' AND se.seat_designator='A'"),
]
for label, where in spots:
    result = run_sql(f"""
        SELECT se.seat_label, se.election_class, se.next_regular_election_year, se.term_length_years
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE {where}
        LIMIT 1
    """)
    if result:
        r = result[0]
        print(f"  {label}: {r['seat_label']} class={r['election_class']} next={r['next_regular_election_year']} term={r['term_length_years']}yr")
    else:
        print(f"  {label}: NOT FOUND")

# 6. Next election year distribution
print("\n6. Next election year distribution:")
year_dist = run_sql("""
    SELECT next_regular_election_year, COUNT(*) as cnt
    FROM seats
    GROUP BY next_regular_election_year
    ORDER BY next_regular_election_year
""")
for row in year_dist:
    print(f"  {row['next_regular_election_year']}: {row['cnt']} seats")

print("\nDone!")
