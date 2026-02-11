"""
Populate elections table with 2026 General and Primary election records.

Creates:
- ~6,330 General elections (one per 2026 elected seat, date = 2026-11-03)
- ~12,500 Primary elections (type varies by state rules, date = NULL)
- Total: ~18,800 election records

Primary type logic:
- Jungle primary states (CA, WA, AK, LA): 1 × 'Primary' per seat
- NE legislature (nonpartisan unicameral): 1 × 'Primary_Nonpartisan' per seat
- NE statewide (partisan): 'Primary_D' + 'Primary_R' per seat
- All other states: 'Primary_D' + 'Primary_R' per seat

Primaries are linked to their General via related_election_id.
Primary dates are left NULL (vary by state, populated later).
"""
import httpx
import sys

TOKEN = 'sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7'
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
BATCH_SIZE = 300


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
# STEP 0: Idempotency guard
# ══════════════════════════════════════════════════════════════════════
print("Step 0: Checking for existing elections...")
existing = run_sql("SELECT COUNT(*) as cnt FROM elections")
if existing[0]['cnt'] > 0:
    print(f"WARNING: elections table already has {existing[0]['cnt']} records!")
    print("Aborting to prevent duplicates.")
    sys.exit(1)
print("  Elections table is empty. Proceeding.")

# ══════════════════════════════════════════════════════════════════════
# STEP 1: Load 2026 elected seat data
# ══════════════════════════════════════════════════════════════════════
print("\nStep 1: Loading 2026 elected seats...")
seats_data = run_sql("""
    SELECT se.id AS seat_id, se.office_level, se.office_type,
           s.abbreviation, s.uses_jungle_primary
    FROM seats se
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE se.next_regular_election_year = 2026
      AND se.selection_method = 'Elected'
    ORDER BY se.id
""")
print(f"  Loaded {len(seats_data)} elected seats with next_regular_election_year = 2026")

if len(seats_data) == 0:
    print("ERROR: No seats found!")
    sys.exit(1)

# Quick summary
leg_count = sum(1 for s in seats_data if s['office_level'] == 'Legislative')
sw_count = sum(1 for s in seats_data if s['office_level'] == 'Statewide')
print(f"  Legislative: {leg_count}, Statewide: {sw_count}")

# ══════════════════════════════════════════════════════════════════════
# STEP 2: Insert General elections (batch)
# ══════════════════════════════════════════════════════════════════════
print("\nStep 2: Inserting General elections...")

# Build all general election value tuples
general_values = []
for seat in seats_data:
    general_values.append(f"({seat['seat_id']}, '2026-11-03', 2026, 'General', NULL)")

# Insert in batches, collecting seat_id -> election_id mapping
seat_to_general = {}
total_generals = 0

for batch_start in range(0, len(general_values), BATCH_SIZE):
    batch = general_values[batch_start:batch_start + BATCH_SIZE]
    sql = (
        "INSERT INTO elections (seat_id, election_date, election_year, election_type, related_election_id) "
        "VALUES " + ",\n".join(batch) + "\n"
        "RETURNING id, seat_id;"
    )
    result = run_sql(sql)
    for row in result:
        seat_to_general[row['seat_id']] = row['id']
    total_generals += len(result)
    batch_num = batch_start // BATCH_SIZE + 1
    total_batches = (len(general_values) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  Batch {batch_num}/{total_batches}: +{len(result)} (total: {total_generals})")

print(f"  Inserted {total_generals} General elections")

if total_generals != len(seats_data):
    print(f"  WARNING: Expected {len(seats_data)}, got {total_generals}")

# ══════════════════════════════════════════════════════════════════════
# STEP 3: Generate primary records
# ══════════════════════════════════════════════════════════════════════
print("\nStep 3: Generating primary election records...")

# Build list of (seat_id, election_type, related_general_id)
primary_records = []

for seat in seats_data:
    seat_id = seat['seat_id']
    abbr = seat['abbreviation']
    general_id = seat_to_general[seat_id]
    is_jungle = seat['uses_jungle_primary']
    is_ne_legislature = (abbr == 'NE' and seat['office_type'] == 'State Legislature')

    if is_jungle and not is_ne_legislature:
        # Jungle primary states: single unified primary
        # But NE legislature is nonpartisan (handled below)
        # NE statewide is partisan but uses_jungle_primary is TRUE for NE,
        # so we need to check: NE statewide should get Primary (jungle) too?
        # Actually, NE uses jungle primary for legislature (nonpartisan) but
        # statewide races are partisan. Let's handle NE specially.
        if abbr == 'NE':
            # NE statewide: partisan primaries
            primary_records.append((seat_id, 'Primary_D', general_id))
            primary_records.append((seat_id, 'Primary_R', general_id))
        else:
            # CA, WA, AK, LA: single jungle primary
            primary_records.append((seat_id, 'Primary', general_id))
    elif is_ne_legislature:
        # NE unicameral legislature: nonpartisan primary
        primary_records.append((seat_id, 'Primary_Nonpartisan', general_id))
    else:
        # Standard partisan primaries
        primary_records.append((seat_id, 'Primary_D', general_id))
        primary_records.append((seat_id, 'Primary_R', general_id))

print(f"  Generated {len(primary_records)} primary records")

# Count by type
type_counts = {}
for _, etype, _ in primary_records:
    type_counts[etype] = type_counts.get(etype, 0) + 1
for etype, cnt in sorted(type_counts.items()):
    print(f"    {etype}: {cnt}")

# ══════════════════════════════════════════════════════════════════════
# STEP 4: Insert Primary elections (batch)
# ══════════════════════════════════════════════════════════════════════
print("\nStep 4: Inserting Primary elections...")

primary_values = []
for seat_id, etype, general_id in primary_records:
    primary_values.append(f"({seat_id}, NULL, 2026, '{etype}', {general_id})")

total_primaries = 0

for batch_start in range(0, len(primary_values), BATCH_SIZE):
    batch = primary_values[batch_start:batch_start + BATCH_SIZE]
    sql = (
        "INSERT INTO elections (seat_id, election_date, election_year, election_type, related_election_id) "
        "VALUES " + ",\n".join(batch) + "\n"
        "RETURNING id;"
    )
    result = run_sql(sql)
    total_primaries += len(result)
    batch_num = batch_start // BATCH_SIZE + 1
    total_batches = (len(primary_values) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  Batch {batch_num}/{total_batches}: +{len(result)} (total: {total_primaries})")

print(f"  Inserted {total_primaries} Primary elections")

if total_primaries != len(primary_records):
    print(f"  WARNING: Expected {len(primary_records)}, got {total_primaries}")

# ══════════════════════════════════════════════════════════════════════
# STEP 5: Verification
# ══════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# 1. Total election count
total = run_sql("SELECT COUNT(*) as cnt FROM elections")
print(f"\n1. Total elections: {total[0]['cnt']} (expected ~{total_generals + total_primaries})")

# 2. Election type distribution
print("\n2. Election type distribution:")
type_dist = run_sql("""
    SELECT election_type, COUNT(*) as cnt
    FROM elections
    GROUP BY election_type
    ORDER BY election_type
""")
for row in type_dist:
    print(f"   {row['election_type']}: {row['cnt']}")

# 3. All primaries have related_election_id
print("\n3. Primaries with NULL related_election_id:")
null_related = run_sql("""
    SELECT COUNT(*) as cnt FROM elections
    WHERE election_type != 'General' AND related_election_id IS NULL
""")
print(f"   {null_related[0]['cnt']} (should be 0)")

# 4. All generals have NULL related_election_id
print("\n4. Generals with non-NULL related_election_id:")
gen_related = run_sql("""
    SELECT COUNT(*) as cnt FROM elections
    WHERE election_type = 'General' AND related_election_id IS NOT NULL
""")
print(f"   {gen_related[0]['cnt']} (should be 0)")

# 5. All generals have election_date = 2026-11-03
print("\n5. Generals without election_date = 2026-11-03:")
gen_date = run_sql("""
    SELECT COUNT(*) as cnt FROM elections
    WHERE election_type = 'General' AND (election_date IS NULL OR election_date != '2026-11-03')
""")
print(f"   {gen_date[0]['cnt']} (should be 0)")

# 6. All primaries have election_date IS NULL
print("\n6. Primaries with non-NULL election_date:")
pri_date = run_sql("""
    SELECT COUNT(*) as cnt FROM elections
    WHERE election_type != 'General' AND election_date IS NOT NULL
""")
print(f"   {pri_date[0]['cnt']} (should be 0)")

# 7. Every 2026 elected seat has exactly 1 General
print("\n7. Seats with != 1 General election:")
seat_gen = run_sql("""
    SELECT seat_id, COUNT(*) as cnt FROM elections
    WHERE election_type = 'General'
    GROUP BY seat_id
    HAVING COUNT(*) != 1
""")
print(f"   {len(seat_gen)} seats (should be 0)")

# 8. Jungle primary states vs standard states primary counts
print("\n8. Primary count per General by state type:")
jungle_check = run_sql("""
    SELECT s.abbreviation, s.uses_jungle_primary,
           COUNT(DISTINCT CASE WHEN e.election_type = 'General' THEN e.id END) as generals,
           COUNT(DISTINCT CASE WHEN e.election_type != 'General' THEN e.id END) as primaries
    FROM elections e
    JOIN seats se ON e.seat_id = se.id
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    GROUP BY s.abbreviation, s.uses_jungle_primary
    ORDER BY s.abbreviation
""")
for row in jungle_check:
    gen = row['generals']
    pri = row['primaries']
    ratio = pri / gen if gen > 0 else 0
    jungle = " (jungle)" if row['uses_jungle_primary'] else ""
    flag = ""
    if row['uses_jungle_primary'] and row['abbreviation'] != 'NE':
        flag = " OK" if abs(ratio - 1) < 0.01 else " MISMATCH"
    elif row['abbreviation'] == 'NE':
        flag = " (mixed)"
    else:
        flag = " OK" if abs(ratio - 2) < 0.01 else " MISMATCH"
    print(f"   {row['abbreviation']}{jungle}: {gen} generals, {pri} primaries (ratio {ratio:.1f}){flag}")

# 9. NE special handling check
print("\n9. NE election type breakdown:")
ne_check = run_sql("""
    SELECT e.election_type, se.office_type, COUNT(*) as cnt
    FROM elections e
    JOIN seats se ON e.seat_id = se.id
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE s.abbreviation = 'NE'
    GROUP BY e.election_type, se.office_type
    ORDER BY se.office_type, e.election_type
""")
for row in ne_check:
    print(f"   {row['office_type']} / {row['election_type']}: {row['cnt']}")

# 10. Per-state election counts
print("\n10. Per-state total election counts:")
state_counts = run_sql("""
    SELECT s.abbreviation, COUNT(*) as cnt
    FROM elections e
    JOIN seats se ON e.seat_id = se.id
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    GROUP BY s.abbreviation
    ORDER BY s.abbreviation
""")
for row in state_counts:
    print(f"   {row['abbreviation']}: {row['cnt']}")

print("\nDone!")
