"""
Populate appointed, ex-officio, and N/A statewide seats for all 50 states.

Adds the selection_method column to the seats table, backfills all existing
seats as 'Elected', then inserts ~273 new seats:
  - 3 missing elected Controller seats (CT, ID, NV)
  - 229 appointed seats
  - 2 ex-officio seats (TN/WV Lt. Governor = Senate President)
  - 39 not-applicable seats (office doesn't exist in that state)

After this script, every state has exactly 11 statewide seats (one per
office type), and all 7,936+ seats have a non-null selection_method.

Data sources: Ballotpedia statewide office pages, verified Feb 2026.
"""
import httpx
import sys

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

ALL_STATES = sorted([
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
])
ALL_STATES_SET = set(ALL_STATES)

# ══════════════════════════════════════════════════════════════════════
# OFFICE CATEGORIZATION: elected, appointed, ex_officio, not_applicable
# ══════════════════════════════════════════════════════════════════════

# For each office, define which states have it elected (already in DB
# from populate_statewide.py), and categorize the rest.

# --- Governor: all 50 elected, none other ---
GOV_ELECTED = ALL_STATES_SET

# --- Lt. Governor ---
LT_GOV_ELECTED = ALL_STATES_SET - {'ME', 'NH', 'OR', 'WY', 'TN', 'WV'}  # 44
LT_GOV_EX_OFFICIO = {'TN', 'WV'}  # Senate President serves
LT_GOV_NA = {'ME', 'NH', 'OR', 'WY'}  # no such office

# --- Attorney General ---
AG_ELECTED = ALL_STATES_SET - {'AK', 'HI', 'ME', 'NH', 'NJ', 'TN', 'WY'}  # 43
AG_APPOINTED = {
    'AK': 'Appointed by Governor',
    'HI': 'Appointed by Governor',
    'NH': 'Appointed by Governor',
    'NJ': 'Appointed by Governor',
    'WY': 'Appointed by Governor',
    'ME': 'Appointed by Legislature',
    'TN': 'Appointed by Supreme Court',
}

# --- Secretary of State ---
SOS_ELECTED = ALL_STATES_SET - {
    'AK', 'HI', 'UT',
    'DE', 'FL', 'MD', 'ME', 'NH', 'NJ', 'NY', 'OK', 'PA', 'TN', 'TX', 'VA',
}  # 35
SOS_APPOINTED = {
    'DE': 'Appointed by Governor',
    'FL': 'Appointed by Governor',
    'MD': 'Appointed by Governor',
    'NJ': 'Appointed by Governor',
    'NY': 'Appointed by Governor',
    'OK': 'Appointed by Governor',
    'PA': 'Appointed by Governor',
    'TX': 'Appointed by Governor',
    'VA': 'Appointed by Governor',
    'ME': 'Appointed by Legislature',
    'NH': 'Appointed by Legislature',
    'TN': 'Appointed by Legislature',
}
SOS_NA = {'AK', 'HI', 'UT'}  # no such office

# --- Treasurer ---
TREAS_ELECTED = ALL_STATES_SET - {
    'NY', 'TX',
    'AK', 'GA', 'HI', 'ME', 'MD', 'MI', 'MN', 'MT', 'NJ', 'VA',
    'NH', 'TN',
}  # 36
TREAS_APPOINTED = {
    'AK': 'Appointed by Governor',
    'HI': 'Appointed by Governor',
    'MD': 'Appointed by Governor',
    'MI': 'Appointed by Governor',
    'MN': 'Appointed by Governor',
    'MT': 'Appointed by Governor',
    'NJ': 'Appointed by Governor',
    'VA': 'Appointed by Governor',
    'GA': 'Appointed by State Depository Board',
    'ME': 'Appointed by Legislature',
    'NH': 'Appointed by Legislature',
    'TN': 'Appointed by Legislature',
}
TREAS_NA = {'NY', 'TX'}  # no such office (Comptroller handles)

# --- Auditor ---
AUDITOR_ELECTED = {
    'AL', 'AR', 'DE', 'IN', 'IA', 'KY', 'MA', 'MN', 'MO', 'MS',
    'MT', 'NE', 'NM', 'NC', 'ND', 'OH', 'OK', 'PA', 'SD', 'UT',
    'VT', 'WA', 'WV', 'WY',
}  # 24
# All non-elected states have appointed auditors (typically by legislature)
AUDITOR_APPOINTED = {}
for st in sorted(ALL_STATES_SET - AUDITOR_ELECTED):
    if st in ('CA', 'NJ'):
        AUDITOR_APPOINTED[st] = 'Appointed by Governor'
    elif st == 'OR':
        AUDITOR_APPOINTED[st] = 'Under Secretary of State'
    else:
        AUDITOR_APPOINTED[st] = 'Appointed by Legislature'

# --- Controller/Comptroller ---
# Original 6 elected: CA, IL, MD, NY, SC, TX
# Plan says add CT, ID, NV as missing elected = 9 total elected
CONTROLLER_ELECTED = {'CA', 'IL', 'MD', 'NY', 'SC', 'TX', 'CT', 'ID', 'NV'}  # 9
CONTROLLER_APPOINTED = {
    'AK': 'Appointed by Governor',
    'CO': 'Appointed by Governor',
    'ME': 'Appointed by Governor',
    'MA': 'Appointed by Governor',
    'NH': 'Appointed by Governor',
    'NJ': 'Appointed by Governor',
    'NM': 'Appointed by Governor',
    'NC': 'Appointed by Governor',
    'VA': 'Appointed by Governor',
    'TN': 'Appointed by Legislature',
    'AL': 'State Finance Director',
}  # 11
CONTROLLER_NA = ALL_STATES_SET - CONTROLLER_ELECTED - set(CONTROLLER_APPOINTED.keys())  # 30

# --- Superintendent of Public Instruction ---
SUPT_ELECTED = {
    'AZ', 'CA', 'GA', 'ID', 'MT', 'NC', 'ND', 'OK', 'OR', 'SC', 'WA', 'WI',
}  # 12
SUPT_APPOINTED = {}
# States where superintendent is appointed by Governor
_supt_gov = {
    'AK', 'CO', 'CT', 'DE', 'FL', 'HI', 'IL', 'IN', 'IA', 'KY',
    'LA', 'MD', 'ME', 'MI', 'MS', 'NV',
}
# States where superintendent is appointed by State Board of Education
_supt_board = {
    'AL', 'AR', 'KS', 'MA', 'MN', 'MO', 'NE', 'NH', 'NJ', 'NM',
    'OH', 'PA', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WV', 'WY',
}
# Special cases
_supt_regents = {'NY', 'RI'}
# Remaining: MT, ND already elected; WA, WI already elected
_supt_other = {'GA', 'NC', 'OK', 'OR'}  # already elected
for st in sorted(ALL_STATES_SET - SUPT_ELECTED):
    if st in _supt_gov:
        SUPT_APPOINTED[st] = 'Appointed by Governor'
    elif st in _supt_board:
        SUPT_APPOINTED[st] = 'Appointed by State Board of Education'
    elif st in _supt_regents:
        SUPT_APPOINTED[st] = 'Appointed by Board of Regents'
    else:
        SUPT_APPOINTED[st] = 'Appointed by Governor'
# Verify: elected + appointed = 50
assert len(SUPT_ELECTED) + len(SUPT_APPOINTED) == 50, \
    f"Superintendent: {len(SUPT_ELECTED)} + {len(SUPT_APPOINTED)} != 50"

# --- Insurance Commissioner ---
INS_ELECTED = {
    'CA', 'DE', 'GA', 'KS', 'LA', 'MS', 'MT', 'NC', 'ND', 'OK', 'WA',
}  # 11
INS_APPOINTED = {}
for st in sorted(ALL_STATES_SET - INS_ELECTED):
    if st == 'VA':
        INS_APPOINTED[st] = 'State Corporation Commission'
    elif st == 'NM':
        INS_APPOINTED[st] = 'Insurance Nominating Committee'
    else:
        INS_APPOINTED[st] = 'Appointed by Governor'

# --- Agriculture Commissioner ---
AG_COMM_ELECTED = {
    'AL', 'FL', 'GA', 'IA', 'KY', 'LA', 'MS', 'NC', 'ND', 'SC', 'TX', 'WV',
}  # 12
AG_COMM_APPOINTED = {}
for st in sorted(ALL_STATES_SET - AG_COMM_ELECTED):
    AG_COMM_APPOINTED[st] = 'Appointed by Governor'

# --- Labor Commissioner ---
LABOR_ELECTED = {'GA', 'NC', 'OK', 'OR'}  # 4
LABOR_APPOINTED = {}
for st in sorted(ALL_STATES_SET - LABOR_ELECTED):
    LABOR_APPOINTED[st] = 'Appointed by Governor'

# ══════════════════════════════════════════════════════════════════════
# MASTER OFFICE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════
# Each entry: (office_type, elected_set, appointed_dict, ex_officio_dict, na_set)

OFFICE_CONFIG = [
    ('Governor', GOV_ELECTED, {}, {}, set()),
    ('Lt. Governor', LT_GOV_ELECTED, {},
     {st: 'Senate President serves as Lt. Governor' for st in LT_GOV_EX_OFFICIO},
     LT_GOV_NA),
    ('Attorney General', AG_ELECTED, AG_APPOINTED, {}, set()),
    ('Secretary of State', SOS_ELECTED, SOS_APPOINTED, {}, SOS_NA),
    ('Treasurer', TREAS_ELECTED, TREAS_APPOINTED, {}, TREAS_NA),
    ('Auditor', AUDITOR_ELECTED, AUDITOR_APPOINTED, {}, set()),
    ('Controller', CONTROLLER_ELECTED, CONTROLLER_APPOINTED, {},
     CONTROLLER_NA),
    ('Superintendent of Public Instruction', SUPT_ELECTED, SUPT_APPOINTED,
     {}, set()),
    ('Insurance Commissioner', INS_ELECTED, INS_APPOINTED, {}, set()),
    ('Agriculture Commissioner', AG_COMM_ELECTED, AG_COMM_APPOINTED,
     {}, set()),
    ('Labor Commissioner', LABOR_ELECTED, LABOR_APPOINTED, {}, set()),
]

# Validate: every office covers all 50 states
print("Office coverage validation:")
total_new = 0
for office_type, elected, appointed, ex_officio, na in OFFICE_CONFIG:
    coverage = len(elected) + len(appointed) + len(ex_officio) + len(na)
    # New seats to insert (appointed + ex_officio + NA + missing elected)
    new_count = len(appointed) + len(ex_officio) + len(na)
    total_new += new_count
    status = "OK" if coverage == 50 else f"MISMATCH ({coverage})"
    print(f"  {office_type}: {len(elected)}E + {len(appointed)}A + "
          f"{len(ex_officio)}X + {len(na)}N = {coverage} {status}")
    if coverage != 50:
        all_covered = elected | set(appointed.keys()) | set(ex_officio.keys()) | na
        missing = ALL_STATES_SET - all_covered
        extra = all_covered - ALL_STATES_SET
        if missing:
            print(f"    MISSING: {sorted(missing)}")
        if extra:
            print(f"    EXTRA: {sorted(extra)}")
        sys.exit(1)

# 3 missing elected Controllers (CT, ID, NV) counted separately
MISSING_ELECTED_CONTROLLERS = {'CT', 'ID', 'NV'}
# These are in CONTROLLER_ELECTED but NOT yet in the DB
# They'll be inserted with selection_method = 'Elected'

print(f"\nTotal new seats to insert: {total_new} (appointed/ex_officio/NA)")
print(f"Plus 3 missing elected Controllers: CT, ID, NV")
print(f"Grand total new: {total_new + 3}")

# ══════════════════════════════════════════════════════════════════════
# IDEMPOTENCY GUARD
# ══════════════════════════════════════════════════════════════════════
print("\nChecking idempotency...")
check = run_sql("""
    SELECT COUNT(*) as cnt FROM information_schema.columns
    WHERE table_name = 'seats' AND column_name = 'selection_method'
""")
col_exists = check[0]['cnt'] > 0

if col_exists:
    appointed_check = run_sql(
        "SELECT COUNT(*) as cnt FROM seats WHERE selection_method = 'Appointed'"
    )
    if appointed_check[0]['cnt'] > 0:
        print(f"Already have {appointed_check[0]['cnt']} appointed seats. Aborting.")
        sys.exit(1)
    print("selection_method column exists but no appointed seats yet. Continuing.")
else:
    print("selection_method column does not exist yet. Will create.")

# ══════════════════════════════════════════════════════════════════════
# STEP A: Add selection_method column
# ══════════════════════════════════════════════════════════════════════
if not col_exists:
    print("\nStep A: Adding selection_method column...")
    run_sql("""
        ALTER TABLE seats ADD COLUMN selection_method TEXT CHECK (
            selection_method IN ('Elected', 'Appointed', 'Ex_Officio', 'Not_Applicable')
        )
    """)
    print("  Column added.")
else:
    print("\nStep A: selection_method column already exists. Skipping.")

# ══════════════════════════════════════════════════════════════════════
# STEP B: Backfill existing seats as 'Elected'
# ══════════════════════════════════════════════════════════════════════
print("\nStep B: Backfilling existing seats as 'Elected'...")
backfill_check = run_sql(
    "SELECT COUNT(*) as cnt FROM seats WHERE selection_method IS NULL"
)
null_count = backfill_check[0]['cnt']
if null_count > 0:
    run_sql("UPDATE seats SET selection_method = 'Elected' WHERE selection_method IS NULL")
    print(f"  Updated {null_count} seats to 'Elected'.")
else:
    print("  No NULL selection_method values. Skipping.")

# ══════════════════════════════════════════════════════════════════════
# STEP C: Load statewide district IDs
# ══════════════════════════════════════════════════════════════════════
print("\nStep C: Loading statewide district IDs...")
districts = run_sql("""
    SELECT d.id as district_id, s.abbreviation
    FROM districts d
    JOIN states s ON d.state_id = s.id
    WHERE d.office_level = 'Statewide'
    ORDER BY s.abbreviation
""")
print(f"  Found {len(districts)} statewide districts.")

if len(districts) != 50:
    print(f"  ERROR: Expected 50, got {len(districts)}")
    sys.exit(1)

abbr_to_district = {row['abbreviation']: row['district_id'] for row in districts}

# Also load states for term info (needed for missing elected controllers)
states_data = run_sql("""
    SELECT s.id, s.abbreviation, s.gov_term_years, s.next_gov_election_year
    FROM states s ORDER BY s.abbreviation
""")
states_by_abbr = {s['abbreviation']: s for s in states_data}

# ══════════════════════════════════════════════════════════════════════
# STEP D: Insert 3 missing elected Controller seats (CT, ID, NV)
# ══════════════════════════════════════════════════════════════════════
print("\nStep D: Inserting 3 missing elected Controller seats...")

# Verify they don't already exist
existing_ctrl = run_sql("""
    SELECT st.abbreviation
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE se.office_type = 'Controller'
""")
existing_ctrl_abbrs = {r['abbreviation'] for r in existing_ctrl}
missing_ctrl = MISSING_ELECTED_CONTROLLERS - existing_ctrl_abbrs

if missing_ctrl:
    ctrl_values = []
    for abbr in sorted(missing_ctrl):
        district_id = abbr_to_district[abbr]
        state = states_by_abbr[abbr]
        term_length = state['gov_term_years']
        next_election = state['next_gov_election_year']
        seat_label = f"{abbr} Controller"
        ctrl_values.append(
            f"({district_id}, 'Statewide', 'Controller', '{seat_label}', "
            f"NULL, {term_length}, NULL, {next_election}, NULL, NULL, NULL, "
            f"'Elected', NULL)"
        )
    sql = (
        "INSERT INTO seats (district_id, office_level, office_type, seat_label, "
        "seat_designator, term_length_years, election_class, "
        "next_regular_election_year, current_holder, current_holder_party, "
        "current_holder_caucus, selection_method, notes) "
        "VALUES " + ",\n".join(ctrl_values) + "\nRETURNING id;"
    )
    result = run_sql(sql)
    print(f"  Inserted {len(result)} missing elected Controller seats: {sorted(missing_ctrl)}")
else:
    print("  All 3 Controller seats already exist. Skipping.")

# ══════════════════════════════════════════════════════════════════════
# STEP E: Insert appointed / ex_officio / NA seats
# ══════════════════════════════════════════════════════════════════════
print("\nStep E: Inserting appointed/ex_officio/NA seats...")

# Check which office_type + state combos already exist
existing_combos = run_sql("""
    SELECT se.office_type, st.abbreviation
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE se.office_level = 'Statewide'
""")
existing_set = {(r['office_type'], r['abbreviation']) for r in existing_combos}
print(f"  {len(existing_set)} existing statewide seat combos found.")

new_seats = []
for office_type, elected, appointed, ex_officio, na in OFFICE_CONFIG:
    # Appointed seats
    for abbr, note in appointed.items():
        if (office_type, abbr) in existing_set:
            continue
        district_id = abbr_to_district[abbr]
        seat_label = f"{abbr} {office_type}"
        label_esc = seat_label.replace("'", "''")
        note_esc = note.replace("'", "''")
        new_seats.append(
            f"({district_id}, 'Statewide', '{office_type}', '{label_esc}', "
            f"NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Appointed', '{note_esc}')"
        )

    # Ex-officio seats
    for abbr, note in ex_officio.items():
        if (office_type, abbr) in existing_set:
            continue
        district_id = abbr_to_district[abbr]
        seat_label = f"{abbr} {office_type}"
        label_esc = seat_label.replace("'", "''")
        note_esc = note.replace("'", "''")
        new_seats.append(
            f"({district_id}, 'Statewide', '{office_type}', '{label_esc}', "
            f"NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Ex_Officio', '{note_esc}')"
        )

    # N/A seats
    for abbr in sorted(na):
        if (office_type, abbr) in existing_set:
            continue
        district_id = abbr_to_district[abbr]
        seat_label = f"{abbr} {office_type}"
        label_esc = seat_label.replace("'", "''")
        new_seats.append(
            f"({district_id}, 'Statewide', '{office_type}', '{label_esc}', "
            f"NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Not_Applicable', "
            f"'Office does not exist')"
        )

print(f"  {len(new_seats)} new seats to insert.")

# Batch insert
BATCH_SIZE = 200
total_inserted = 0
for batch_start in range(0, len(new_seats), BATCH_SIZE):
    batch = new_seats[batch_start:batch_start + BATCH_SIZE]
    sql = (
        "INSERT INTO seats (district_id, office_level, office_type, seat_label, "
        "seat_designator, term_length_years, election_class, "
        "next_regular_election_year, current_holder, current_holder_party, "
        "current_holder_caucus, selection_method, notes) "
        "VALUES " + ",\n".join(batch) + "\nRETURNING id;"
    )
    result = run_sql(sql)
    total_inserted += len(result)
    batch_num = batch_start // BATCH_SIZE + 1
    total_batches = (len(new_seats) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  Batch {batch_num}/{total_batches}: +{len(result)} (total: {total_inserted})")

print(f"  Inserted {total_inserted} new seats.")

# ══════════════════════════════════════════════════════════════════════
# STEP F: Update statewide district num_seats = 11
# ══════════════════════════════════════════════════════════════════════
print("\nStep F: Updating statewide district num_seats to 11...")
run_sql("UPDATE districts SET num_seats = 11 WHERE office_level = 'Statewide'")
print("  Done.")

# ══════════════════════════════════════════════════════════════════════
# VERIFICATION
# ══════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# 1. Total statewide seats = 550
sw_count = run_sql(
    "SELECT COUNT(*) as cnt FROM seats WHERE office_level = 'Statewide'"
)
print(f"\n1. Total statewide seats: {sw_count[0]['cnt']} (expected 550)")

# 2. selection_method distribution
method_dist = run_sql("""
    SELECT selection_method, COUNT(*) as cnt
    FROM seats
    WHERE office_level = 'Statewide'
    GROUP BY selection_method
    ORDER BY cnt DESC
""")
print("\n2. Statewide selection_method distribution:")
for row in method_dist:
    print(f"   {row['selection_method']}: {row['cnt']}")

# 3. Every state has exactly 11 statewide seats
print("\n3. States with statewide seat count != 11:")
seat_check = run_sql("""
    SELECT st.abbreviation, COUNT(*) as cnt
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE se.office_level = 'Statewide'
    GROUP BY st.abbreviation
    HAVING COUNT(*) != 11
    ORDER BY st.abbreviation
""")
if not seat_check:
    print("   All 50 states have exactly 11 statewide seats!")
else:
    for row in seat_check:
        print(f"   PROBLEM: {row['abbreviation']} has {row['cnt']} statewide seats")

# 4. All legislative seats are Elected
leg_check = run_sql("""
    SELECT selection_method, COUNT(*) as cnt
    FROM seats
    WHERE office_level = 'Legislative'
    GROUP BY selection_method
""")
print("\n4. Legislative seat selection_method distribution:")
for row in leg_check:
    print(f"   {row['selection_method']}: {row['cnt']}")

# 5. No NULLs in selection_method
null_check = run_sql(
    "SELECT COUNT(*) as cnt FROM seats WHERE selection_method IS NULL"
)
print(f"\n5. Seats with NULL selection_method: {null_check[0]['cnt']} (expected 0)")

# 6. Appointed/NA seats have NULL election year
appt_elec = run_sql("""
    SELECT COUNT(*) as cnt FROM seats
    WHERE selection_method IN ('Appointed', 'Ex_Officio', 'Not_Applicable')
    AND next_regular_election_year IS NOT NULL
""")
print(f"\n6. Appointed/NA/ExOfficio seats with non-NULL election year: "
      f"{appt_elec[0]['cnt']} (expected 0)")

# 7. Elected seats still have non-NULL election year (statewide only)
elected_null = run_sql("""
    SELECT COUNT(*) as cnt FROM seats
    WHERE selection_method = 'Elected'
    AND office_level = 'Statewide'
    AND next_regular_election_year IS NULL
""")
print(f"\n7. Elected statewide seats with NULL election year: "
      f"{elected_null[0]['cnt']} (expected 0)")

# 8. Governor: 50 Elected, 0 other
gov_check = run_sql("""
    SELECT selection_method, COUNT(*) as cnt
    FROM seats WHERE office_type = 'Governor'
    GROUP BY selection_method
""")
print("\n8. Governor selection_method distribution:")
for row in gov_check:
    print(f"   {row['selection_method']}: {row['cnt']}")

# 9. num_seats = 11 for all statewide districts
dist_check = run_sql("""
    SELECT COUNT(*) as cnt FROM districts
    WHERE office_level = 'Statewide' AND num_seats != 11
""")
print(f"\n9. Statewide districts with num_seats != 11: "
      f"{dist_check[0]['cnt']} (expected 0)")

# 10. Overall DB totals
overall = run_sql("""
    SELECT
        (SELECT COUNT(*) FROM seats) as total_seats,
        (SELECT COUNT(*) FROM seats WHERE office_level = 'Legislative') as leg_seats,
        (SELECT COUNT(*) FROM seats WHERE office_level = 'Statewide') as sw_seats,
        (SELECT COUNT(*) FROM seats WHERE selection_method = 'Elected') as elected,
        (SELECT COUNT(*) FROM seats WHERE selection_method = 'Appointed') as appointed,
        (SELECT COUNT(*) FROM seats WHERE selection_method = 'Ex_Officio') as ex_officio,
        (SELECT COUNT(*) FROM seats WHERE selection_method = 'Not_Applicable') as na
""")
o = overall[0]
print(f"\n10. Overall database totals:")
print(f"    Total seats: {o['total_seats']}")
print(f"    Legislative: {o['leg_seats']}")
print(f"    Statewide: {o['sw_seats']}")
print(f"    Elected: {o['elected']}")
print(f"    Appointed: {o['appointed']}")
print(f"    Ex_Officio: {o['ex_officio']}")
print(f"    Not_Applicable: {o['na']}")

# 11. Office type breakdown
print("\n11. Full office type × selection_method grid:")
grid = run_sql("""
    SELECT office_type, selection_method, COUNT(*) as cnt
    FROM seats
    WHERE office_level = 'Statewide'
    GROUP BY office_type, selection_method
    ORDER BY office_type, selection_method
""")
from collections import defaultdict
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
office_grid = defaultdict(dict)
for row in grid:
    office_grid[row['office_type']][row['selection_method']] = row['cnt']

office_order = [
    'Governor', 'Lt. Governor', 'Attorney General', 'Secretary of State',
    'Treasurer', 'Auditor', 'Controller',
    'Superintendent of Public Instruction', 'Insurance Commissioner',
    'Agriculture Commissioner', 'Labor Commissioner',
]
print(f"    {'Office':<40} {'Elected':>7} {'Apptd':>7} {'ExOff':>7} {'N/A':>7} {'Total':>7}")
print(f"    {'-'*40} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7}")
grand_total = 0
for ot in office_order:
    e = office_grid.get(ot, {}).get('Elected', 0)
    a = office_grid.get(ot, {}).get('Appointed', 0)
    x = office_grid.get(ot, {}).get('Ex_Officio', 0)
    n = office_grid.get(ot, {}).get('Not_Applicable', 0)
    t = e + a + x + n
    grand_total += t
    print(f"    {ot:<40} {e:>7} {a:>7} {x:>7} {n:>7} {t:>7}")
print(f"    {'TOTAL':<40} {'':>7} {'':>7} {'':>7} {'':>7} {grand_total:>7}")

print("\nDone!")
