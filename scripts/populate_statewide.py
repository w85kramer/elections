"""
Populate statewide districts and seats for all 50 states.

Creates:
- 50 statewide districts (one per state)
- ~277 statewide seats (Governor, Lt. Gov., AG, SoS, Treasurer, Auditor,
  Controller, Superintendent, Insurance Commissioner, Agriculture Commissioner,
  Labor Commissioner)

Each state gets exactly one statewide district with num_seats = count of
elected statewide offices. All statewide offices share the governor's
election cycle (term length and next election year).

Data sources: Ballotpedia statewide office pages, verified Feb 2026.
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

# ══════════════════════════════════════════════════════════════════════
# STATEWIDE OFFICE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

ALL_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
}

# Governor: all 50 states
GOVERNOR = ALL_STATES

# Lt. Governor: 44 states elected
# No Lt. Gov. office: ME, NH, OR, WY
# Senate President serves (not separately elected): TN, WV
LT_GOVERNOR = ALL_STATES - {'ME', 'NH', 'OR', 'WY', 'TN', 'WV'}

# Attorney General: 43 states elected
# Appointed: AK, HI, ME, NH, NJ, TN, WY
ATTORNEY_GENERAL = ALL_STATES - {'AK', 'HI', 'ME', 'NH', 'NJ', 'TN', 'WY'}

# Secretary of State: 35 states elected
# No SoS office: AK, HI, UT
# Appointed: DE, FL, MD, ME, NH, NJ, NY, OK, PA, TN, TX, VA
SECRETARY_OF_STATE = ALL_STATES - {
    'AK', 'HI', 'UT',  # no office
    'DE', 'FL', 'MD', 'ME', 'NH', 'NJ', 'NY', 'OK', 'PA', 'TN', 'TX', 'VA',  # appointed
}

# Treasurer: 36 states elected
# No office: NY, TX (Comptroller handles)
# Appointed: AK, GA, HI, ME, MD, MI, MN, MT, NJ, VA (governor), NH, TN (legislature)
TREASURER = ALL_STATES - {
    'NY', 'TX',  # no office
    'AK', 'GA', 'HI', 'ME', 'MD', 'MI', 'MN', 'MT', 'NJ', 'VA',  # governor-appointed
    'NH', 'TN',  # legislature-appointed
}

# Auditor: 24 states elected
AUDITOR = {
    'AL', 'AR', 'DE', 'IN', 'IA', 'KY', 'MA', 'MN', 'MO', 'MS',
    'MT', 'NE', 'NM', 'NC', 'ND', 'OH', 'OK', 'PA', 'SD', 'UT',
    'VT', 'WA', 'WV', 'WY',
}

# Controller/Comptroller: 6 states elected
# (CA, IL, MD, NY, SC, TX per plan; research says more but we use the
#  well-established 6 from plan)
CONTROLLER = {'CA', 'IL', 'MD', 'NY', 'SC', 'TX'}

# Superintendent of Public Instruction: 12 states elected
SUPERINTENDENT = {
    'AZ', 'CA', 'GA', 'ID', 'MT', 'NC', 'ND', 'OK', 'OR', 'SC', 'WA', 'WI',
}

# Insurance Commissioner: 11 states elected
INSURANCE_COMMISSIONER = {
    'CA', 'DE', 'GA', 'KS', 'LA', 'MS', 'MT', 'NC', 'ND', 'OK', 'WA',
}

# Agriculture Commissioner: 12 states elected
AGRICULTURE_COMMISSIONER = {
    'AL', 'FL', 'GA', 'IA', 'KY', 'LA', 'MS', 'NC', 'ND', 'SC', 'TX', 'WV',
}

# Labor Commissioner: 4 states elected
LABOR_COMMISSIONER = {'GA', 'NC', 'OK', 'OR'}

# Map of office_type (matching schema CHECK constraint) -> set of states
OFFICES = {
    'Governor': GOVERNOR,
    'Lt. Governor': LT_GOVERNOR,
    'Attorney General': ATTORNEY_GENERAL,
    'Secretary of State': SECRETARY_OF_STATE,
    'Treasurer': TREASURER,
    'Auditor': AUDITOR,
    'Controller': CONTROLLER,
    'Superintendent of Public Instruction': SUPERINTENDENT,
    'Insurance Commissioner': INSURANCE_COMMISSIONER,
    'Agriculture Commissioner': AGRICULTURE_COMMISSIONER,
    'Labor Commissioner': LABOR_COMMISSIONER,
}

def build_state_offices():
    """Return dict: abbreviation -> list of office_type strings."""
    result = {}
    for abbr in sorted(ALL_STATES):
        offices = []
        for office_type, state_set in OFFICES.items():
            if abbr in state_set:
                offices.append(office_type)
        result[abbr] = offices
    return result

# ══════════════════════════════════════════════════════════════════════
# MAIN SCRIPT
# ══════════════════════════════════════════════════════════════════════

# Print expected counts
print("Expected office counts:")
total_expected = 0
for office_type, state_set in OFFICES.items():
    print(f"  {office_type}: {len(state_set)}")
    total_expected += len(state_set)
print(f"  TOTAL: {total_expected}")

# ── Idempotency guard ──────────────────────────────────────────────
existing = run_sql("SELECT COUNT(*) as cnt FROM districts WHERE office_level = 'Statewide'")
if existing[0]['cnt'] > 0:
    print(f"\nWARNING: districts table already has {existing[0]['cnt']} statewide districts!")
    print("Aborting to prevent duplicates.")
    sys.exit(1)

# ── Load states from DB ────────────────────────────────────────────
print("\nLoading states...")
states_data = run_sql("""
    SELECT id, abbreviation, gov_term_years, next_gov_election_year
    FROM states
    ORDER BY abbreviation
""")
print(f"Loaded {len(states_data)} states")

if len(states_data) != 50:
    print(f"ERROR: Expected 50 states, got {len(states_data)}")
    sys.exit(1)

# Build lookup: abbreviation -> state row
states_by_abbr = {s['abbreviation']: s for s in states_data}

# Build per-state office lists
state_offices = build_state_offices()

# ══════════════════════════════════════════════════════════════════════
# PHASE 1: Insert 50 statewide districts
# ══════════════════════════════════════════════════════════════════════
print("\nPhase 1: Inserting statewide districts...")

district_values = []
for abbr in sorted(ALL_STATES):
    state = states_by_abbr[abbr]
    num_seats = len(state_offices[abbr])
    district_values.append(
        f"({state['id']}, 'Statewide', 'Statewide', 'Statewide', {num_seats})"
    )

sql = (
    "INSERT INTO districts (state_id, office_level, chamber, district_number, num_seats) "
    "VALUES " + ",\n".join(district_values) + "\n"
    "RETURNING id, state_id;"
)
result = run_sql(sql)
print(f"Inserted {len(result)} statewide districts")

if len(result) != 50:
    print(f"ERROR: Expected 50 districts, got {len(result)}")
    sys.exit(1)

# ── Build district ID lookup ───────────────────────────────────────
# Map state_id -> district_id for the newly inserted statewide districts
state_id_to_district_id = {row['state_id']: row['id'] for row in result}

# Also build abbr -> district_id
abbr_to_district_id = {}
for abbr, state in states_by_abbr.items():
    abbr_to_district_id[abbr] = state_id_to_district_id[state['id']]

# ══════════════════════════════════════════════════════════════════════
# PHASE 2: Insert statewide seats
# ══════════════════════════════════════════════════════════════════════
print("\nPhase 2: Inserting statewide seats...")

seats = []
for abbr in sorted(ALL_STATES):
    state = states_by_abbr[abbr]
    district_id = abbr_to_district_id[abbr]
    term_length = state['gov_term_years']
    next_election = state['next_gov_election_year']

    for office_type in state_offices[abbr]:
        seat_label = f"{abbr} {office_type}"
        seats.append((
            district_id,
            office_type,
            seat_label,
            term_length,
            next_election,
        ))

print(f"Generated {len(seats)} seat records (expected {total_expected})")

if len(seats) != total_expected:
    print(f"WARNING: Count mismatch! Generated {len(seats)}, expected {total_expected}")

# ── Batch insert ───────────────────────────────────────────────────
BATCH_SIZE = 300
total_inserted = 0

for batch_start in range(0, len(seats), BATCH_SIZE):
    batch = seats[batch_start:batch_start + BATCH_SIZE]
    values = []
    for (district_id, office_type, seat_label, term_length, next_election) in batch:
        label_escaped = seat_label.replace("'", "''")
        values.append(
            f"({district_id}, 'Statewide', '{office_type}', "
            f"'{label_escaped}', NULL, {term_length}, NULL, {next_election})"
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

print(f"\nInserted {total_inserted} statewide seats")

# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# 1. Total statewide districts and seats
d_count = run_sql("SELECT COUNT(*) as cnt FROM districts WHERE office_level = 'Statewide'")
s_count = run_sql("SELECT COUNT(*) as cnt FROM seats WHERE office_level = 'Statewide'")
print(f"\n1. Statewide districts: {d_count[0]['cnt']} (expected 50)")
print(f"   Statewide seats: {s_count[0]['cnt']} (expected {total_expected})")

# 2. Every state has exactly 1 statewide district
print("\n2. States with statewide district count != 1:")
state_dist_check = run_sql("""
    SELECT s.abbreviation, COUNT(*) as cnt
    FROM districts d JOIN states s ON d.state_id = s.id
    WHERE d.office_level = 'Statewide'
    GROUP BY s.abbreviation
    HAVING COUNT(*) != 1
""")
if not state_dist_check:
    print("   All 50 states have exactly 1 statewide district!")
else:
    for row in state_dist_check:
        print(f"   PROBLEM: {row['abbreviation']} has {row['cnt']} statewide districts")

# 3. Governor seat exists for all 50 states
print("\n3. Governor seat count:")
gov_count = run_sql("SELECT COUNT(*) as cnt FROM seats WHERE office_type = 'Governor'")
print(f"   {gov_count[0]['cnt']} (expected 50)")

# 4. Lt. Governor: ME, NH, OR, WY should NOT have one
print("\n4. Lt. Governor spot checks (should NOT exist):")
for abbr in ['ME', 'NH', 'OR', 'WY', 'TN', 'WV']:
    check = run_sql(f"""
        SELECT COUNT(*) as cnt FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{abbr}' AND se.office_type = 'Lt. Governor'
    """)
    status = "NONE (correct)" if check[0]['cnt'] == 0 else f"FOUND {check[0]['cnt']} (WRONG!)"
    print(f"   {abbr}: {status}")

# 5. AG: AK, HI, ME, NH, NJ, TN, WY should NOT have one
print("\n5. Attorney General spot checks (should NOT exist):")
for abbr in ['AK', 'HI', 'ME', 'NH', 'NJ', 'TN', 'WY']:
    check = run_sql(f"""
        SELECT COUNT(*) as cnt FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{abbr}' AND se.office_type = 'Attorney General'
    """)
    status = "NONE (correct)" if check[0]['cnt'] == 0 else f"FOUND {check[0]['cnt']} (WRONG!)"
    print(f"   {abbr}: {status}")

# 6. NH and VT have term_length_years = 2
print("\n6. NH and VT term length check (should be 2):")
for abbr in ['NH', 'VT']:
    check = run_sql(f"""
        SELECT DISTINCT se.term_length_years FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{abbr}' AND se.office_level = 'Statewide'
    """)
    terms = [str(r['term_length_years']) for r in check]
    print(f"   {abbr}: term_length_years = {', '.join(terms)}")

# 7. Office type distribution
print("\n7. Seat count by office type:")
type_dist = run_sql("""
    SELECT office_type, COUNT(*) as cnt
    FROM seats
    WHERE office_level = 'Statewide'
    GROUP BY office_type
    ORDER BY cnt DESC, office_type
""")
for row in type_dist:
    expected = len(OFFICES.get(row['office_type'], set()))
    match = "OK" if row['cnt'] == expected else f"MISMATCH (expected {expected})"
    print(f"   {row['office_type']}: {row['cnt']} {match}")

# 8. NULL checks on statewide seats
print("\n8. NULL checks (statewide seats):")
null_check = run_sql("""
    SELECT
        SUM(CASE WHEN office_type IS NULL THEN 1 ELSE 0 END) as null_type,
        SUM(CASE WHEN seat_label IS NULL THEN 1 ELSE 0 END) as null_label,
        SUM(CASE WHEN term_length_years IS NULL THEN 1 ELSE 0 END) as null_term,
        SUM(CASE WHEN next_regular_election_year IS NULL THEN 1 ELSE 0 END) as null_next
    FROM seats
    WHERE office_level = 'Statewide'
""")
nc = null_check[0]
print(f"   Null office_type: {nc['null_type']}")
print(f"   Null seat_label: {nc['null_label']}")
print(f"   Null term_length: {nc['null_term']}")
print(f"   Null next_election: {nc['null_next']}")

# 9. Overall DB totals (legislative + statewide)
print("\n9. Overall database totals:")
overall = run_sql("""
    SELECT
        (SELECT COUNT(*) FROM districts) as total_districts,
        (SELECT COUNT(*) FROM districts WHERE office_level = 'Legislative') as leg_districts,
        (SELECT COUNT(*) FROM districts WHERE office_level = 'Statewide') as sw_districts,
        (SELECT COUNT(*) FROM seats) as total_seats,
        (SELECT COUNT(*) FROM seats WHERE office_level = 'Legislative') as leg_seats,
        (SELECT COUNT(*) FROM seats WHERE office_level = 'Statewide') as sw_seats
""")
o = overall[0]
print(f"   Districts: {o['total_districts']} total ({o['leg_districts']} legislative + {o['sw_districts']} statewide)")
print(f"   Seats: {o['total_seats']} total ({o['leg_seats']} legislative + {o['sw_seats']} statewide)")

print("\nDone!")
