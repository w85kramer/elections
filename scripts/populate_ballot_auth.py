#!/usr/bin/env python3
"""Populate ballot measure authorization data on the states table.

Adds initiative_type and referendum_type columns to the states table,
then populates with authoritative data from MultiState/NCSL.

Initiative Authorization:
  - 'both': Both statutory and constitutional initiatives (15 states)
  - 'statutory': Statutory initiatives only (5 states)
  - 'constitutional': Constitutional initiatives only (2 states)
  - NULL: Does not authorize citizen initiatives (28 states)

Referendum Authorization (legislative referendum):
  - 'both': Can refer both statutes and constitutional amendments (23 states)
  - 'amendments_only': Can refer constitutional amendments only (26 states)
  - 'statutes_only': Can refer statutes only (1 state: DE)

Sources:
  - MultiState (primary), NCSL, Ballotpedia
  - Mississippi initiative process struck down by state supreme court in 2021
"""

import json, os, time, requests

SUPABASE_URL = "https://api.supabase.com/v1/projects/pikcvwulzfxgwfcfssxc/database/query"
TOKEN = "sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7"

# ============================================================
# Authoritative state classifications
# ============================================================

# Initiative authorization: which states allow citizen-initiated measures
# From 101 page text + MultiState data
INITIATIVE_STATUTES = [
    'AK', 'AZ', 'AR', 'CA', 'CO', 'ID', 'ME', 'MA', 'MI', 'MO',
    'MT', 'NE', 'NV', 'ND', 'OH', 'OK', 'OR', 'SD', 'UT', 'WA'
]  # 20 states (WY has advisory only, not counted)

INITIATIVE_CONSTITUTIONAL = [
    'AZ', 'AR', 'CA', 'CO', 'FL', 'IL', 'MA', 'MI', 'MO', 'MT',
    'NE', 'NV', 'ND', 'OH', 'OK', 'OR', 'SD'
]  # 17 states (MS struck down in 2021, not counted)

# Derive initiative_type per state
INITIATIVE_TYPE = {}
for st in INITIATIVE_STATUTES:
    if st in INITIATIVE_CONSTITUTIONAL:
        INITIATIVE_TYPE[st] = 'both'
    else:
        INITIATIVE_TYPE[st] = 'statutory'
for st in INITIATIVE_CONSTITUTIONAL:
    if st not in INITIATIVE_STATUTES:
        INITIATIVE_TYPE[st] = 'constitutional'

# Legislative referendum authorization
# 23 states can refer both statutes and constitutional amendments
REFERENDUM_BOTH = [
    'AK', 'AR', 'CA', 'CO', 'ID', 'IA', 'KS', 'ME', 'MA', 'MI',
    'MN', 'MO', 'MT', 'NE', 'NV', 'ND', 'OH', 'OK', 'OR', 'SD',
    'UT', 'WA', 'WY'
]  # 23 states

# DE is the only state that can refer statutes but not constitutional amendments
# (legislature amends constitution without voter approval)
REFERENDUM_STATUTES_ONLY = ['DE']

# All other 26 states can only refer constitutional amendments
REFERENDUM_TYPE = {}
for st in REFERENDUM_BOTH:
    REFERENDUM_TYPE[st] = 'both'
for st in REFERENDUM_STATUTES_ONLY:
    REFERENDUM_TYPE[st] = 'statutes_only'
# All other states default to 'amendments_only'

ALL_STATES = [
    'AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
]


def run_sql(query, retries=5):
    """Execute SQL via Supabase Management API with retry."""
    for attempt in range(retries):
        resp = requests.post(
            SUPABASE_URL,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json"
            },
            json={"query": query}
        )
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code not in (200, 201):
            print(f"  ERROR {resp.status_code}: {resp.text[:200]}")
            return None
        try:
            return resp.json()
        except Exception:
            return []
    print("  Max retries exceeded")
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # Validate counts
    both = sum(1 for v in INITIATIVE_TYPE.values() if v == 'both')
    stat = sum(1 for v in INITIATIVE_TYPE.values() if v == 'statutory')
    const = sum(1 for v in INITIATIVE_TYPE.values() if v == 'constitutional')
    none_count = 50 - len(INITIATIVE_TYPE)
    print(f"Initiative: both={both}, statutory={stat}, constitutional={const}, none={none_count}")
    assert both == 15, f"Expected 15 both, got {both}"
    assert stat == 5, f"Expected 5 statutory, got {stat}"
    assert const == 2, f"Expected 2 constitutional, got {const}"
    assert none_count == 28, f"Expected 28 none, got {none_count}"

    ref_both = sum(1 for v in REFERENDUM_TYPE.values() if v == 'both')
    ref_stat = sum(1 for v in REFERENDUM_TYPE.values() if v == 'statutes_only')
    ref_amend = 50 - ref_both - ref_stat
    print(f"Referendum: both={ref_both}, statutes_only={ref_stat}, amendments_only={ref_amend}")
    assert ref_both == 23, f"Expected 23 both, got {ref_both}"
    assert ref_stat == 1, f"Expected 1 statutes_only, got {ref_stat}"
    assert ref_amend == 26, f"Expected 26 amendments_only, got {ref_amend}"

    if args.dry_run:
        print("\n[DRY RUN] Would add columns and populate data for 50 states")
        for st in ALL_STATES:
            init = INITIATIVE_TYPE.get(st)
            ref = REFERENDUM_TYPE.get(st, 'amendments_only')
            print(f"  {st}: initiative={init}, referendum={ref}")
        return

    # Step 1: Add columns if they don't exist
    print("\nAdding columns to states table...")
    sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='states' AND column_name='initiative_type') THEN
            ALTER TABLE states ADD COLUMN initiative_type TEXT
                CHECK (initiative_type IN ('both', 'statutory', 'constitutional'));
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='states' AND column_name='referendum_type') THEN
            ALTER TABLE states ADD COLUMN referendum_type TEXT
                CHECK (referendum_type IN ('both', 'amendments_only', 'statutes_only'));
        END IF;
    END $$;
    """
    result = run_sql(sql)
    if result is not None:
        print("  Columns added/verified")
    else:
        print("  ERROR adding columns")
        return

    time.sleep(2)

    # Step 2: Build and execute UPDATE statements
    print("\nPopulating ballot measure authorization data...")
    updates = []
    for st in ALL_STATES:
        init = INITIATIVE_TYPE.get(st)
        ref = REFERENDUM_TYPE.get(st, 'amendments_only')
        init_sql = f"'{init}'" if init else "NULL"
        updates.append(
            f"UPDATE states SET initiative_type = {init_sql}, "
            f"referendum_type = '{ref}' "
            f"WHERE abbreviation = '{st}'"
        )

    # Batch into groups of 10
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        sql = ";\n".join(batch) + ";"
        result = run_sql(sql)
        if result is not None:
            print(f"  Updated states {i+1}-{min(i+10, len(updates))}")
        else:
            print(f"  ERROR on batch {i+1}-{min(i+10, len(updates))}")
        time.sleep(1)

    # Step 3: Verify
    print("\nVerifying...")
    result = run_sql("""
        SELECT abbreviation, initiative_type, referendum_type
        FROM states ORDER BY abbreviation
    """)
    if result:
        for row in result:
            print(f"  {row['abbreviation']}: init={row['initiative_type']}, ref={row['referendum_type']}")

    # Step 4: Export JSON for site
    print("\nExporting ballot_auth.json...")
    export = {
        'generated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'source': 'MultiState / NCSL',
        'initiative': {
            'colors': {
                'both': '#189d91',
                'statutory': '#043858',
                'constitutional': '#88567f',
                'none': '#d9d9d9'
            },
            'legend': {
                'both': 'Both statutory & constitutional (15)',
                'statutory': 'Statutory initiatives only (5)',
                'constitutional': 'Constitutional initiatives only (2)',
                'none': 'Does not authorize initiatives (28)'
            },
            'states': {}
        },
        'referendum': {
            'colors': {
                'both': '#189d91',
                'amendments_only': '#043858',
                'statutes_only': '#88567f'
            },
            'legend': {
                'both': 'Both statutes & amendments (23)',
                'amendments_only': 'Constitutional amendments only (26)',
                'statutes_only': 'Statutes only (1)'
            },
            'states': {}
        },
        'notes': {
            'MS': 'Initiative process struck down by state supreme court in 2021',
            'MA': 'Indirect initiative process (legislature reviews first)',
            'AK': 'Indirect initiative process',
            'ME': 'Indirect initiative process',
            'WA': 'Direct and indirect initiative process',
            'FL': '60% supermajority required to pass',
            'CO': '55% supermajority required for constitutional amendments',
            'DE': 'Only state where legislature can amend constitution without voter approval',
        }
    }
    for st in ALL_STATES:
        export['initiative']['states'][st] = INITIATIVE_TYPE.get(st, 'none')
        export['referendum']['states'][st] = REFERENDUM_TYPE.get(st, 'amendments_only')

    json_path = '/home/billkramer/elections/site/data/ballot_auth.json'
    with open(json_path, 'w') as f:
        json.dump(export, f, indent=2)
    print(f"  Written: {json_path}")


if __name__ == '__main__':
    main()
