"""
Populate seat_terms for appointed/ex-officio statewide officeholders.

Inserts ~69 candidates (Mumpower is both TN Auditor + TN Controller),
~70 seat_terms, and updates ~70 seats cache columns
(current_holder, current_holder_party, current_holder_caucus).

Requires: elections/data/appointed_officeholders.py

Usage:
    python3 scripts/populate_appointed_officeholders.py [--dry-run]
"""
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import httpx
from data.appointed_officeholders import OFFICEHOLDERS
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

DRY_RUN = '--dry-run' in sys.argv

def run_sql(query, retries=5):
    for attempt in range(1, retries + 1):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=60
        )
        if resp.status_code == 429:
            wait = 5 * attempt
            print(f'  Rate limited, waiting {wait}s (attempt {attempt}/{retries})...')
            time.sleep(wait)
            continue
        if resp.status_code != 201:
            print(f'ERROR: {resp.status_code} - {resp.text[:500]}')
            sys.exit(1)
        return resp.json()
    print('ERROR: Max retries exceeded (429)')
    sys.exit(1)

def esc(s):
    """Escape single quotes for SQL."""
    if s is None:
        return None
    return s.replace("'", "''")

if DRY_RUN:
    print("=== DRY RUN MODE ===\n")

# ══════════════════════════════════════════════════════════════════
# STEP 1: Look up seat IDs for appointed/ex_officio seats
# ══════════════════════════════════════════════════════════════════
print("Step 1: Looking up appointed/ex-officio seat IDs...")

seat_rows = run_sql("""
    SELECT se.id as seat_id, s.abbreviation as state, se.office_type, se.selection_method
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE se.office_level = 'Statewide'
      AND se.selection_method IN ('Appointed', 'Ex_Officio')
    ORDER BY s.abbreviation, se.office_type
""")

seat_map = {}
for row in seat_rows:
    key = (row['state'], row['office_type'])
    seat_map[key] = row['seat_id']

print(f"  Found {len(seat_map)} appointed/ex-officio seats")

# Verify all officeholders have matching seats
missing = []
for o in OFFICEHOLDERS:
    key = (o['state'], o['office_type'])
    if key not in seat_map:
        missing.append(key)
if missing:
    print(f"  ERROR: {len(missing)} officeholders have no matching seat:")
    for m in missing:
        print(f"    {m}")
    sys.exit(1)
print("  All officeholders matched to seats!")

# ══════════════════════════════════════════════════════════════════
# STEP 2: Check for existing seat_terms on these seats
# ══════════════════════════════════════════════════════════════════
print("\nStep 2: Checking for existing active seat_terms...")

seat_ids = list(seat_map.values())
existing = run_sql(f"""
    SELECT st.seat_id, c.full_name
    FROM seat_terms st
    JOIN candidates c ON st.candidate_id = c.id
    WHERE st.seat_id IN ({','.join(str(s) for s in seat_ids)})
      AND st.end_date IS NULL
""")

existing_seats = {row['seat_id']: row['full_name'] for row in existing}
if existing_seats:
    print(f"  WARNING: {len(existing_seats)} seats already have active seat_terms:")
    for sid, name in list(existing_seats.items())[:5]:
        print(f"    seat {sid}: {name}")
    if len(existing_seats) > 5:
        print(f"    ... and {len(existing_seats) - 5} more")
    print("  These will be SKIPPED.")

# Filter to only officeholders whose seats don't already have terms
to_insert = []
skipped = 0
for o in OFFICEHOLDERS:
    seat_id = seat_map[(o['state'], o['office_type'])]
    if seat_id in existing_seats:
        skipped += 1
    else:
        to_insert.append(o)

print(f"  To insert: {len(to_insert)}, Skipping: {skipped}")

if not to_insert:
    print("\nNothing to do!")
    sys.exit(0)

# ══════════════════════════════════════════════════════════════════
# STEP 3: Check for existing candidates (dedup by full_name)
# ══════════════════════════════════════════════════════════════════
print("\nStep 3: Checking for existing candidates...")

# Collect unique names from to_insert
unique_names = list(dict.fromkeys(o['name'] for o in to_insert))
print(f"  Unique names to insert: {len(unique_names)} (from {len(to_insert)} records)")

# Check which already exist
name_conditions = " OR ".join(f"full_name = '{esc(n)}'" for n in unique_names)
existing_cands = run_sql(f"SELECT id, full_name FROM candidates WHERE {name_conditions}")
existing_cand_map = {row['full_name']: row['id'] for row in existing_cands}

if existing_cand_map:
    print(f"  Found {len(existing_cand_map)} existing candidates:")
    for name, cid in existing_cand_map.items():
        print(f"    {name} (id={cid})")

new_names = [n for n in unique_names if n not in existing_cand_map]
print(f"  New candidates to create: {len(new_names)}")

# ══════════════════════════════════════════════════════════════════
# STEP 4: Insert new candidates
# ══════════════════════════════════════════════════════════════════
if new_names:
    print(f"\nStep 4: Inserting {len(new_names)} new candidates...")

    if DRY_RUN:
        print("  [DRY RUN] Would insert:")
        for n in new_names:
            print(f"    {n}")
    else:
        # Build name → officeholder data for first/last name lookup
        name_data = {}
        for o in to_insert:
            if o['name'] not in name_data:
                name_data[o['name']] = o

        cand_values = []
        for n in new_names:
            d = name_data[n]
            cand_values.append(
                f"('{esc(d['name'])}', '{esc(d['first_name'])}', '{esc(d['last_name'])}')"
            )

        sql = (
            "INSERT INTO candidates (full_name, first_name, last_name) VALUES\n"
            + ",\n".join(cand_values)
            + "\nRETURNING id, full_name;"
        )
        cand_result = run_sql(sql)
        print(f"  Inserted {len(cand_result)} candidates")

        for row in cand_result:
            existing_cand_map[row['full_name']] = row['id']
else:
    print("\nStep 4: No new candidates needed")

# Build full name → candidate_id map
cand_id_map = existing_cand_map

# ══════════════════════════════════════════════════════════════════
# STEP 5: Insert seat_terms
# ══════════════════════════════════════════════════════════════════
print(f"\nStep 5: Inserting {len(to_insert)} seat_terms...")

if DRY_RUN:
    print("  [DRY RUN] Would insert:")
    for o in to_insert:
        seat_id = seat_map[(o['state'], o['office_type'])]
        cand_id = cand_id_map.get(o['name'], '???')
        caucus = o['caucus'] or 'NULL'
        print(f"    {o['state']} {o['office_type']}: {o['name']} (seat={seat_id}, cand={cand_id}, party={o['party']}, caucus={caucus})")
else:
    st_values = []
    for o in to_insert:
        seat_id = seat_map[(o['state'], o['office_type'])]
        cand_id = cand_id_map[o['name']]
        party = o['party']
        caucus_val = f"'{o['caucus']}'" if o['caucus'] else 'NULL'
        start_date = o['start_date']
        start_reason = o['start_reason']

        st_values.append(
            f"({seat_id}, {cand_id}, '{party}', '{start_date}', NULL, "
            f"'{start_reason}', {caucus_val}, NULL)"
        )

    sql = (
        "INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date, "
        "start_reason, caucus, election_id) VALUES\n"
        + ",\n".join(st_values)
        + "\nRETURNING id;"
    )
    st_result = run_sql(sql)
    print(f"  Inserted {len(st_result)} seat_terms")

# ══════════════════════════════════════════════════════════════════
# STEP 6: Update seats cache columns
# ══════════════════════════════════════════════════════════════════
print("\nStep 6: Updating seats cache columns...")

if DRY_RUN:
    print("  [DRY RUN] Would update current_holder, current_holder_party, current_holder_caucus")
else:
    sql = """
    UPDATE seats
    SET current_holder = c.full_name,
        current_holder_party = st.party,
        current_holder_caucus = st.caucus
    FROM seat_terms st
    JOIN candidates c ON st.candidate_id = c.id
    WHERE seats.id = st.seat_id
      AND st.end_date IS NULL
      AND seats.office_level = 'Statewide'
      AND seats.selection_method IN ('Appointed', 'Ex_Officio')
      AND seats.current_holder IS NULL;
    """
    run_sql(sql)
    print("  Done")

# ══════════════════════════════════════════════════════════════════
# STEP 7: Verification
# ══════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# Count appointed/ex_officio seats with holders
filled = run_sql("""
    SELECT COUNT(*) as cnt FROM seats
    WHERE office_level = 'Statewide'
      AND selection_method IN ('Appointed', 'Ex_Officio')
      AND current_holder IS NOT NULL
""")
total_appt = run_sql("""
    SELECT COUNT(*) as cnt FROM seats
    WHERE office_level = 'Statewide'
      AND selection_method IN ('Appointed', 'Ex_Officio')
""")
print(f"\n1. Appointed/Ex-Officio seats filled: {filled[0]['cnt']} / {total_appt[0]['cnt']}")

# By office type
print("\n2. Filled by office type:")
by_office = run_sql("""
    SELECT se.office_type, se.selection_method,
           COUNT(*) as total,
           COUNT(se.current_holder) as filled
    FROM seats se
    WHERE se.office_level = 'Statewide'
      AND se.selection_method IN ('Appointed', 'Ex_Officio')
    GROUP BY se.office_type, se.selection_method
    ORDER BY se.office_type
""")
for row in by_office:
    print(f"   {row['office_type']} ({row['selection_method']}): {row['filled']}/{row['total']}")

# Spot checks
print("\n3. Spot checks:")
spots = [
    ('TN', 'Lt. Governor', 'Randy McNally'),
    ('WV', 'Lt. Governor', 'Randy E. Smith'),
    ('NH', 'Attorney General', 'John Formella'),
    ('ME', 'Secretary of State', 'Shenna Bellows'),
    ('TN', 'Auditor', 'Jason E. Mumpower'),
    ('TN', 'Controller', 'Jason E. Mumpower'),
    ('NY', 'Auditor', 'Thomas P. DiNapoli'),
]
for state, office, expected in spots:
    result = run_sql(f"""
        SELECT se.current_holder, se.current_holder_party
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state}'
          AND se.office_type = '{esc(office)}'
          AND se.selection_method IN ('Appointed', 'Ex_Officio')
    """)
    if result:
        r = result[0]
        match = "OK" if r['current_holder'] == expected else "MISMATCH"
        print(f"   {state} {office}: {r['current_holder']} ({r['current_holder_party']}) {match}")
    else:
        print(f"   {state} {office}: NO SEAT FOUND!")

# Overall counts
print("\n4. Overall database counts:")
overall = run_sql("""
    SELECT
        (SELECT COUNT(*) FROM candidates) as candidates,
        (SELECT COUNT(*) FROM seat_terms) as seat_terms,
        (SELECT COUNT(*) FROM seat_terms WHERE end_date IS NULL) as active_terms
""")
o = overall[0]
print(f"   candidates: {o['candidates']}")
print(f"   seat_terms: {o['seat_terms']}")
print(f"   active terms: {o['active_terms']}")

# Remaining unfilled appointed/ex_officio seats
unfilled = run_sql("""
    SELECT s.abbreviation, se.office_type, se.selection_method
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE se.office_level = 'Statewide'
      AND se.selection_method IN ('Appointed', 'Ex_Officio')
      AND se.current_holder IS NULL
    ORDER BY s.abbreviation, se.office_type
""")
if unfilled:
    print(f"\n5. Still unfilled appointed/ex-officio seats ({len(unfilled)}):")
    for row in unfilled:
        print(f"   {row['abbreviation']} {row['office_type']} ({row['selection_method']})")
else:
    print("\n5. All appointed/ex-officio seats filled!")

print("\nDone!")
