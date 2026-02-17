"""
Update primary election dates for all 2026 primaries.

Sets election_date on all 12,354 primary elections (currently NULL)
based on each state's 2026 primary date. Uses one UPDATE per unique date
for efficiency (~19 updates total).

Primary dates sourced from NCSL and 270toWin, with Arizona updated to
Jul 21 per law signed Feb 7, 2026.
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

# Primary dates grouped by date (states sharing the same primary date)
DATE_TO_STATES = {
    '2026-03-03': ['AR', 'NC', 'TX'],
    '2026-03-17': ['IL'],
    '2026-05-05': ['IN', 'OH'],
    '2026-05-12': ['NE', 'WV'],
    '2026-05-19': ['AL', 'GA', 'ID', 'KY', 'OR', 'PA'],
    '2026-06-02': ['CA', 'IA', 'MT', 'NM', 'SD'],
    '2026-06-09': ['ME', 'NV', 'ND', 'SC'],
    '2026-06-16': ['OK'],
    '2026-06-23': ['MD', 'NY', 'UT'],
    '2026-06-30': ['CO'],
    '2026-07-21': ['AZ'],
    '2026-08-04': ['KS', 'MI', 'MO', 'WA'],
    '2026-08-06': ['TN'],
    '2026-08-08': ['HI'],
    '2026-08-11': ['CT', 'MN', 'VT', 'WI'],
    '2026-08-18': ['AK', 'FL', 'WY'],
    '2026-09-01': ['MA'],
    '2026-09-08': ['NH', 'RI'],
    '2026-09-15': ['DE'],
}

# ══════════════════════════════════════════════════════════════════════
# STEP 0: Verify preconditions
# ══════════════════════════════════════════════════════════════════════
print("Step 0: Verifying preconditions...")

result = run_sql("""
    SELECT
        COUNT(*) FILTER (WHERE election_type != 'General') AS primary_count,
        COUNT(*) FILTER (WHERE election_type != 'General' AND election_date IS NULL) AS primaries_null_date,
        COUNT(*) FILTER (WHERE election_type = 'General') AS general_count,
        COUNT(*) FILTER (WHERE election_type = 'General' AND election_date = '2026-11-03') AS generals_correct_date,
        COUNT(*) AS total
    FROM elections
""")
row = result[0]
print(f"  Total elections: {row['total']}")
print(f"  Primaries: {row['primary_count']} (NULL date: {row['primaries_null_date']})")
print(f"  Generals: {row['general_count']} (date=2026-11-03: {row['generals_correct_date']})")

if int(row['primaries_null_date']) == 0:
    print("  All primaries already have dates. Nothing to do.")
    sys.exit(0)

if int(row['primaries_null_date']) != int(row['primary_count']):
    print(f"  WARNING: Some primaries already have dates! ({int(row['primary_count']) - int(row['primaries_null_date'])} have dates)")
    print("  Proceeding anyway — will only update NULL-date primaries.")

# ══════════════════════════════════════════════════════════════════════
# STEP 1: Update primary dates (one UPDATE per unique date)
# ══════════════════════════════════════════════════════════════════════
print("\nStep 1: Updating primary election dates...")

total_updated = 0
for date, states in sorted(DATE_TO_STATES.items()):
    abbrevs = ",".join(f"'{s}'" for s in states)
    query = f"""
        UPDATE elections SET election_date = '{date}'
        WHERE election_type != 'General'
          AND election_date IS NULL
          AND seat_id IN (
            SELECT se.id FROM seats se
            JOIN districts d ON se.district_id = d.id
            JOIN states s ON d.state_id = s.id
            WHERE s.abbreviation IN ({abbrevs})
          )
        RETURNING id
    """
    rows = run_sql(query)
    count = len(rows)
    total_updated += count
    states_str = ', '.join(states)
    print(f"  {date}: {count:>4} primaries updated  ({states_str})")

print(f"\n  Total primaries updated: {total_updated}")

# ══════════════════════════════════════════════════════════════════════
# STEP 2: Verification
# ══════════════════════════════════════════════════════════════════════
print("\nStep 2: Verification...")

# Check 1: No primaries with NULL date
result = run_sql("""
    SELECT COUNT(*) AS cnt FROM elections
    WHERE election_type != 'General' AND election_date IS NULL
""")
null_count = int(result[0]['cnt'])
print(f"  Primaries with NULL date: {null_count}", "✓" if null_count == 0 else "✗ UNEXPECTED")

# Check 2: Generals unchanged
result = run_sql("""
    SELECT COUNT(*) AS cnt FROM elections
    WHERE election_type = 'General' AND election_date = '2026-11-03'
""")
gen_count = int(result[0]['cnt'])
print(f"  Generals with date 2026-11-03: {gen_count}", "✓" if gen_count == 6330 else f"✗ expected 6330")

# Check 3: Total count unchanged
result = run_sql("SELECT COUNT(*) AS cnt FROM elections")
total = int(result[0]['cnt'])
print(f"  Total elections: {total}", "✓" if total == 18684 else f"✗ expected 18684")

# Check 4: Spot checks
print("\n  Spot checks:")
spot_checks = [
    ('TX', '2026-03-03'),
    ('NH', '2026-09-08'),
    ('AZ', '2026-07-21'),
    ('CA', '2026-06-02'),
    ('DE', '2026-09-15'),
]
for state, expected_date in spot_checks:
    result = run_sql(f"""
        SELECT DISTINCT e.election_date::text
        FROM elections e
        JOIN seats se ON e.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE s.abbreviation = '{state}'
          AND e.election_type != 'General'
    """)
    actual = result[0]['election_date'] if result else 'NO DATA'
    match = "✓" if actual == expected_date else f"✗ got {actual}"
    print(f"    {state}: {actual} (expected {expected_date}) {match}")

# Check 5: Per-state summary
print("\n  Per-state primary date summary:")
result = run_sql("""
    SELECT s.abbreviation, e.election_date::text, COUNT(*) AS cnt
    FROM elections e
    JOIN seats se ON e.seat_id = se.id
    JOIN districts d ON se.district_id = d.id
    JOIN states s ON d.state_id = s.id
    WHERE e.election_type != 'General'
    GROUP BY s.abbreviation, e.election_date
    ORDER BY e.election_date, s.abbreviation
""")
for row in result:
    print(f"    {row['abbreviation']}: {row['election_date']} ({row['cnt']} primaries)")

print("\nDone!")
