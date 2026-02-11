"""
Populate seat_terms for elected statewide officeholders.

Inserts ~278 candidates, ~278 seat_terms, and updates ~278 seats cache columns
(current_holder, current_holder_party, current_holder_caucus).

Requires: elections/data/statewide_officeholders.py

Skips 2 vacant seats (AZ Lt. Gov, OR Superintendent).
"""
import sys
import os

# Add project root to path for data imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import httpx
from data.statewide_officeholders import OFFICEHOLDERS

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


def esc(s):
    """Escape single quotes for SQL."""
    return s.replace("'", "''")


# ══════════════════════════════════════════════════════════════════
# STEP 1: Precondition checks
# ══════════════════════════════════════════════════════════════════
print("Step 1: Precondition checks...")

cand_count = run_sql("SELECT COUNT(*) as cnt FROM candidates")
if cand_count[0]['cnt'] > 0:
    print(f"  WARNING: candidates table has {cand_count[0]['cnt']} rows!")
    print("  Aborting to prevent duplicates.")
    sys.exit(1)
print(f"  candidates table: {cand_count[0]['cnt']} rows (empty, good)")

st_count = run_sql("SELECT COUNT(*) as cnt FROM seat_terms")
if st_count[0]['cnt'] > 0:
    print(f"  WARNING: seat_terms table has {st_count[0]['cnt']} rows!")
    print("  Aborting to prevent duplicates.")
    sys.exit(1)
print(f"  seat_terms table: {st_count[0]['cnt']} rows (empty, good)")

elected_sw = run_sql("""
    SELECT COUNT(*) as cnt FROM seats
    WHERE office_level = 'Statewide' AND selection_method = 'Elected'
""")
print(f"  elected statewide seats: {elected_sw[0]['cnt']} (expected 280)")
if elected_sw[0]['cnt'] != 280:
    print("  ERROR: Expected 280 elected statewide seats!")
    sys.exit(1)

print(f"  Officeholders to insert: {len(OFFICEHOLDERS)}")


# ══════════════════════════════════════════════════════════════════
# STEP 2: Look up seat IDs — build (state, office_type) → seat_id map
# ══════════════════════════════════════════════════════════════════
print("\nStep 2: Looking up seat IDs...")

seat_rows = run_sql("""
    SELECT se.id as seat_id, s.abbreviation as state, se.office_type
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE se.office_level = 'Statewide' AND se.selection_method = 'Elected'
    ORDER BY s.abbreviation, se.office_type
""")

seat_map = {}
for row in seat_rows:
    key = (row['state'], row['office_type'])
    seat_map[key] = row['seat_id']

print(f"  Loaded {len(seat_map)} seat mappings")

# Verify all officeholders have matching seats
missing = []
for o in OFFICEHOLDERS:
    key = (o['state'], o['office_type'])
    if key not in seat_map:
        missing.append(key)
if missing:
    print(f"  ERROR: {len(missing)} officeholders have no matching seat:")
    for m in missing[:10]:
        print(f"    {m}")
    sys.exit(1)
print("  All officeholders matched to seats!")


# ══════════════════════════════════════════════════════════════════
# STEP 3: Insert candidates (batch)
# ══════════════════════════════════════════════════════════════════
print("\nStep 3: Inserting candidates...")

# Build VALUES for all candidates
cand_values = []
for o in OFFICEHOLDERS:
    cand_values.append(
        f"('{esc(o['name'])}', '{esc(o['first_name'])}', '{esc(o['last_name'])}')"
    )

sql = (
    "INSERT INTO candidates (full_name, first_name, last_name) VALUES\n"
    + ",\n".join(cand_values)
    + "\nRETURNING id, full_name;"
)
cand_result = run_sql(sql)
print(f"  Inserted {len(cand_result)} candidates")

if len(cand_result) != len(OFFICEHOLDERS):
    print(f"  ERROR: Expected {len(OFFICEHOLDERS)}, got {len(cand_result)}")
    sys.exit(1)

# Build name → candidate_id mapping
# Since we inserted in order and RETURNING preserves order, zip with OFFICEHOLDERS
# BUT some names might appear twice (e.g., James Brown = MT Auditor & MT Insurance Commissioner)
# So we need to map by index position, not by name
cand_ids = [row['id'] for row in cand_result]
print(f"  Candidate IDs range: {min(cand_ids)} to {max(cand_ids)}")


# ══════════════════════════════════════════════════════════════════
# STEP 4: Insert seat_terms (batch)
# ══════════════════════════════════════════════════════════════════
print("\nStep 4: Inserting seat_terms...")

st_values = []
for i, o in enumerate(OFFICEHOLDERS):
    seat_id = seat_map[(o['state'], o['office_type'])]
    candidate_id = cand_ids[i]
    party = o['party']
    caucus = o['caucus']
    start_date = o['start_date']
    start_reason = o['start_reason']

    st_values.append(
        f"({seat_id}, {candidate_id}, '{party}', '{start_date}', NULL, "
        f"'{start_reason}', '{caucus}', NULL)"
    )

sql = (
    "INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, "
    "start_reason, caucus, election_id) VALUES\n"
    + ",\n".join(st_values)
    + "\nRETURNING id;"
)
st_result = run_sql(sql)
print(f"  Inserted {len(st_result)} seat_terms")

if len(st_result) != len(OFFICEHOLDERS):
    print(f"  ERROR: Expected {len(OFFICEHOLDERS)}, got {len(st_result)}")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════
# STEP 5: Update seats cache columns
# ══════════════════════════════════════════════════════════════════
print("\nStep 5: Updating seats cache columns...")

sql = """
UPDATE seats
SET current_holder = st.full_name,
    current_holder_party = st2.party,
    current_holder_caucus = st2.caucus
FROM seat_terms st2
JOIN candidates st ON st2.candidate_id = st.id
WHERE seats.id = st2.seat_id
  AND st2.end_date IS NULL
  AND seats.office_level = 'Statewide'
  AND seats.selection_method = 'Elected';
"""
run_sql(sql)

# Verify the update
updated = run_sql("""
    SELECT COUNT(*) as cnt FROM seats
    WHERE office_level = 'Statewide'
      AND selection_method = 'Elected'
      AND current_holder IS NOT NULL
""")
print(f"  Seats with current_holder populated: {updated[0]['cnt']}")


# ══════════════════════════════════════════════════════════════════
# STEP 6: Verification
# ══════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# 1. Total counts
print("\n1. Record counts:")
counts = run_sql("""
    SELECT
        (SELECT COUNT(*) FROM candidates) as candidates,
        (SELECT COUNT(*) FROM seat_terms) as seat_terms,
        (SELECT COUNT(*) FROM seat_terms WHERE end_date IS NULL) as current_terms,
        (SELECT COUNT(*) FROM seats WHERE office_level = 'Statewide'
         AND selection_method = 'Elected' AND current_holder IS NOT NULL) as seats_updated
""")
c = counts[0]
print(f"   candidates: {c['candidates']} (expected {len(OFFICEHOLDERS)})")
print(f"   seat_terms: {c['seat_terms']} (expected {len(OFFICEHOLDERS)})")
print(f"   current terms (end_date IS NULL): {c['current_terms']} (expected {len(OFFICEHOLDERS)})")
print(f"   seats with holder populated: {c['seats_updated']} (expected {len(OFFICEHOLDERS)})")

# 2. Party distribution
print("\n2. Party distribution in seat_terms:")
party_dist = run_sql("""
    SELECT party, COUNT(*) as cnt
    FROM seat_terms
    GROUP BY party
    ORDER BY cnt DESC
""")
for row in party_dist:
    print(f"   {row['party']}: {row['cnt']}")

# 3. By office type
print("\n3. Seat_terms by office type:")
office_dist = run_sql("""
    SELECT se.office_type, COUNT(*) as cnt
    FROM seat_terms st
    JOIN seats se ON st.seat_id = se.id
    GROUP BY se.office_type
    ORDER BY cnt DESC
""")
for row in office_dist:
    print(f"   {row['office_type']}: {row['cnt']}")

# 4. Spot checks
print("\n4. Spot checks:")
spot_checks = [
    ('AL', 'Governor', 'Kay Ivey', 'R'),
    ('CA', 'Governor', 'Gavin Newsom', 'D'),
    ('NY', 'Attorney General', 'Letitia James', 'D'),
    ('TX', 'Governor', 'Greg Abbott', 'R'),
    ('VA', 'Governor', 'Abigail Spanberger', 'D'),
    ('SD', 'Governor', 'Larry Rhoden', 'R'),
    ('NJ', 'Governor', 'Mikie Sherrill', 'D'),
]
for state, office, expected_name, expected_party in spot_checks:
    result = run_sql(f"""
        SELECT se.seat_label, st.party, c.full_name, st.start_reason
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        JOIN candidates c ON st.candidate_id = c.id
        WHERE s.abbreviation = '{state}'
          AND se.office_type = '{office}'
          AND st.end_date IS NULL
    """)
    if result:
        r = result[0]
        match = "OK" if r['full_name'] == expected_name and r['party'] == expected_party else "MISMATCH"
        print(f"   {state} {office}: {r['full_name']} ({r['party']}) [{r['start_reason']}] {match}")
    else:
        print(f"   {state} {office}: NOT FOUND!")

# 5. Vacant seats (should NOT have seat_terms)
print("\n5. Elected seats WITHOUT seat_terms (expect 2: AZ Lt. Gov, OR Superintendent):")
vacant = run_sql("""
    SELECT s.abbreviation, se.office_type
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    LEFT JOIN seat_terms st ON se.id = st.seat_id AND st.end_date IS NULL
    WHERE se.office_level = 'Statewide'
      AND se.selection_method = 'Elected'
      AND st.id IS NULL
    ORDER BY s.abbreviation, se.office_type
""")
for row in vacant:
    print(f"   {row['abbreviation']} {row['office_type']}")

# 6. Overall DB summary
print("\n6. Overall database summary:")
overall = run_sql("""
    SELECT
        (SELECT COUNT(*) FROM states) as states,
        (SELECT COUNT(*) FROM districts) as districts,
        (SELECT COUNT(*) FROM seats) as seats,
        (SELECT COUNT(*) FROM elections) as elections,
        (SELECT COUNT(*) FROM candidates) as candidates,
        (SELECT COUNT(*) FROM seat_terms) as seat_terms,
        (SELECT COUNT(*) FROM candidacies) as candidacies
""")
o = overall[0]
print(f"   states: {o['states']}")
print(f"   districts: {o['districts']}")
print(f"   seats: {o['seats']}")
print(f"   elections: {o['elections']}")
print(f"   candidates: {o['candidates']}")
print(f"   seat_terms: {o['seat_terms']}")
print(f"   candidacies: {o['candidacies']}")

print("\nDone!")
