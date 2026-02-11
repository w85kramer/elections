"""
Populate the districts table with state legislative districts for all 50 states.

Inserts all senate/upper-chamber districts for all 50 states, plus lower-chamber
districts for 47 states. Deferred to Phase 2 (need verified Ballotpedia data):
  - MD House of Delegates (complex sub-districts)
  - NH House (203 multi-member districts, 400 members)
  - VT House (104 districts: 58 single + 46 two-member = 150 members)
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


# ── Check for existing districts ──────────────────────────────────────
existing = run_sql("SELECT COUNT(*) as cnt FROM districts")
if existing[0]['cnt'] > 0:
    print(f"WARNING: districts table already has {existing[0]['cnt']} rows!")
    print("Aborting to prevent duplicates.")
    sys.exit(1)

# ── Load states from database ─────────────────────────────────────────
states_data = run_sql(
    "SELECT id, abbreviation, senate_seats, house_seats "
    "FROM states ORDER BY abbreviation"
)
state_map = {s['abbreviation']: s for s in states_data}
print(f"Loaded {len(state_map)} states from database")

# ── Configuration ─────────────────────────────────────────────────────

# Lower chamber name overrides (default is 'House')
LOWER_CHAMBER = {
    'CA': 'Assembly', 'NV': 'Assembly', 'NY': 'Assembly', 'WI': 'Assembly',
    'NJ': 'Assembly',
    'MD': 'House of Delegates', 'VA': 'House of Delegates', 'WV': 'House of Delegates',
}

# States where lower chamber has 2-member districts matching senate district numbers
PAIRED_2MEMBER = {'AZ', 'ID', 'NJ', 'ND', 'SD', 'WA'}

# ── Complex multi-member states ───────────────────────────────────────
# Maryland House of Delegates: 47 districts, 141 delegates total
# Sub-districts vary (3 at-large, or 1A+1B+1C, or 1A+2B) — needs Ballotpedia verification
MD_DELEGATE_DISTRICTS = []  # Phase 2 — needs verified sub-district data

# New Hampshire House: 400 members across 203 multi-member districts by county
NH_HOUSE_DISTRICTS = []  # Phase 2 — needs complete district breakdown

# Vermont Senate: 15 multi-member districts (total = 30)
# Verified from Ballotpedia research
VT_SENATE_DISTRICTS = [
    ('Addison', 2),
    ('Bennington', 2),
    ('Caledonia', 1),
    ('Chittenden-Central', 3),
    ('Chittenden-North', 1),
    ('Chittenden-Southeast', 3),
    ('Essex-Orleans', 2),
    ('Franklin', 2),
    ('Grand Isle', 1),
    ('Lamoille', 1),
    ('Orange', 1),
    ('Rutland', 3),
    ('Washington', 3),
    ('Windham', 2),
    ('Windsor', 3),
]  # Total: 30 ✓

# Vermont House: 104 districts (58 single + 46 two-member = 150)
VT_HOUSE_DISTRICTS = []  # Phase 2 — needs complete district list


# ── Build district records ────────────────────────────────────────────
# Each record: (state_id, office_level, chamber, district_number, num_seats)
districts = []

for abbr in sorted(state_map.keys()):
    state = state_map[abbr]
    sid = state['id']
    senate = state['senate_seats']
    house = state['house_seats']
    lower = LOWER_CHAMBER.get(abbr, 'House')

    # ── Nebraska: unicameral ──────────────────────────────────────────
    if abbr == 'NE':
        for i in range(1, senate + 1):
            districts.append((sid, 'Legislative', 'Legislature', str(i), 1))
        continue

    # ── West Virginia: 17×2 senate + single-member house ─────────────
    if abbr == 'WV':
        for i in range(1, 18):
            districts.append((sid, 'Legislative', 'Senate', str(i), 2))
        for i in range(1, house + 1):
            districts.append((sid, 'Legislative', lower, str(i), 1))
        continue

    # ── Maryland: standard senate + complex delegate districts ────────
    if abbr == 'MD':
        for i in range(1, 48):
            districts.append((sid, 'Legislative', 'Senate', str(i), 1))
        if MD_DELEGATE_DISTRICTS:
            for dist_label, num_seats in MD_DELEGATE_DISTRICTS:
                districts.append((sid, 'Legislative', lower, dist_label, num_seats))
        else:
            print("  SKIPPING MD House of Delegates — district data not yet verified")
        continue

    # ── New Hampshire: standard senate + complex house ────────────────
    if abbr == 'NH':
        for i in range(1, 25):
            districts.append((sid, 'Legislative', 'Senate', str(i), 1))
        if NH_HOUSE_DISTRICTS:
            for dist_label, num_seats in NH_HOUSE_DISTRICTS:
                districts.append((sid, 'Legislative', 'House', dist_label, num_seats))
        else:
            print("  SKIPPING NH House — district data not yet populated")
        continue

    # ── Vermont: complex senate + complex house ───────────────────────
    if abbr == 'VT':
        if VT_SENATE_DISTRICTS:
            for dist_label, num_seats in VT_SENATE_DISTRICTS:
                districts.append((sid, 'Legislative', 'Senate', dist_label, num_seats))
        else:
            print("  SKIPPING VT Senate — district data not yet populated")
        if VT_HOUSE_DISTRICTS:
            for dist_label, num_seats in VT_HOUSE_DISTRICTS:
                districts.append((sid, 'Legislative', 'House', dist_label, num_seats))
        else:
            print("  SKIPPING VT House — district data not yet populated")
        continue

    # ── Paired 2-member states (AZ, ID, NJ, ND, SD, WA) ─────────────
    if abbr in PAIRED_2MEMBER:
        for i in range(1, senate + 1):
            districts.append((sid, 'Legislative', 'Senate', str(i), 1))
        for i in range(1, senate + 1):
            districts.append((sid, 'Legislative', lower, str(i), 2))
        continue

    # ── Standard: single-member districts for both chambers ───────────
    for i in range(1, senate + 1):
        districts.append((sid, 'Legislative', 'Senate', str(i), 1))
    for i in range(1, house + 1):
        districts.append((sid, 'Legislative', lower, str(i), 1))

print(f"Generated {len(districts)} district records")

# ── Validate VT Senate count ──────────────────────────────────────────
if VT_SENATE_DISTRICTS:
    vt_senators = sum(n for _, n in VT_SENATE_DISTRICTS)
    print(f"VT Senate seats from districts: {vt_senators} (expected 30)")
    if vt_senators != 30:
        print(f"ERROR: VT Senate count mismatch! Got {vt_senators}, expected 30")
        sys.exit(1)

# ── Batch insert ──────────────────────────────────────────────────────
BATCH_SIZE = 300
total_inserted = 0

for batch_start in range(0, len(districts), BATCH_SIZE):
    batch = districts[batch_start:batch_start + BATCH_SIZE]
    values = []
    for sid, office_level, chamber, dist_num, num_seats in batch:
        # Escape any apostrophes in district names
        dist_num_escaped = dist_num.replace("'", "''")
        values.append(
            f"({sid}, '{office_level}', '{chamber}', '{dist_num_escaped}', {num_seats})"
        )

    sql = (
        "INSERT INTO districts (state_id, office_level, chamber, district_number, num_seats) "
        "VALUES " + ",\n".join(values) + "\nRETURNING id;"
    )

    result = run_sql(sql)
    total_inserted += len(result)
    print(f"  Batch {batch_start // BATCH_SIZE + 1}: +{len(result)} (total: {total_inserted})")

print(f"\nInserted {total_inserted} districts")

# ── Verification ──────────────────────────────────────────────────────
verification = run_sql("""
    SELECT s.abbreviation, d.chamber,
           COUNT(*) as district_count,
           SUM(d.num_seats) as total_seats
    FROM districts d
    JOIN states s ON d.state_id = s.id
    GROUP BY s.abbreviation, d.chamber
    ORDER BY s.abbreviation, d.chamber
""")

print(f"\nVerification — {len(verification)} chamber groups:")
current_state = None
for v in verification:
    if v['abbreviation'] != current_state:
        current_state = v['abbreviation']
        print(f"\n  {current_state}:")
    print(f"    {v['chamber']}: {v['district_count']} districts, {v['total_seats']} seats")

# Summary totals
totals = run_sql("""
    SELECT COUNT(*) as total_districts, SUM(num_seats) as total_seats
    FROM districts
""")
print(f"\nGrand total: {totals[0]['total_districts']} districts, {totals[0]['total_seats']} seats")
