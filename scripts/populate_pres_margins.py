"""
Populate elections.pres_margin_this_cycle with 2024 presidential margins.

Reads the parsed JSON from download_pres_margins.py and sets the presidential
margin on every election in matching districts. Also populates districts.pres_2024_margin
and districts.pres_2024_winner for reference.

Usage:
    python3 scripts/populate_pres_margins.py
    python3 scripts/populate_pres_margins.py --dry-run
    python3 scripts/populate_pres_margins.py --input /tmp/pres_margins.json
"""
import sys
import json
import time
import argparse
from collections import Counter, defaultdict

import httpx

TOKEN = 'sbp_134edd259126b21a7fc11c7a13c0c8c6834d7fa7'
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
BATCH_SIZE = 400


def run_sql(query, exit_on_error=True, retries=5):
    for attempt in range(retries):
        resp = httpx.post(
            f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query},
            timeout=120
        )
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429 and attempt < retries - 1:
            wait = 10 * (attempt + 1)
            print(f'\n  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None


def esc(s):
    if s is None:
        return ''
    return str(s).replace("'", "''")


def main():
    parser = argparse.ArgumentParser(description='Populate presidential margins on elections')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without writing')
    parser.add_argument('--input', type=str, default='/tmp/pres_margins.json', help='Input JSON path')
    args = parser.parse_args()

    # Load parsed margins
    with open(args.input) as f:
        records = json.load(f)
    print(f"Loaded {len(records)} margin records from {args.input}")

    # ═══════════════════════════════════════════════════════════════
    # Step 1: Build lookup of all districts in our DB
    # ═══════════════════════════════════════════════════════════════
    print("\nLoading districts from DB...")
    rows = run_sql("""
        SELECT d.id, d.district_number, d.chamber, d.office_level, s.abbreviation as state
        FROM districts d
        JOIN states s ON d.state_id = s.id
        ORDER BY s.abbreviation, d.chamber, d.district_number
    """)

    # Build lookup: (state, chamber, district_number) → district_id
    district_lookup = {}
    for r in rows:
        key = (r['state'], r['chamber'], r['district_number'])
        district_lookup[key] = r['id']

    print(f"  {len(district_lookup)} districts loaded")

    # ═══════════════════════════════════════════════════════════════
    # Step 2: Match margin records to district IDs
    # ═══════════════════════════════════════════════════════════════
    print("\nMatching records to districts...")
    matched = []
    unmatched = []
    statewide_margins = {}  # state → margin string

    for rec in records:
        state = rec['state']
        chamber = rec['chamber']
        dist_num = rec['district_number']
        margin = rec['margin']

        if chamber == 'Statewide':
            statewide_margins[state] = margin
            continue

        key = (state, chamber, dist_num)
        district_id = district_lookup.get(key)

        if district_id:
            matched.append({
                'district_id': district_id,
                'margin': margin,
                'state': state,
                'chamber': chamber,
                'district_number': dist_num,
            })
        else:
            unmatched.append(rec)

    print(f"  Matched: {len(matched)} districts")
    print(f"  Unmatched: {len(unmatched)} districts")
    print(f"  Statewide margins: {len(statewide_margins)} states")

    if unmatched:
        # Show unmatched grouped by state
        by_state = defaultdict(list)
        for u in unmatched:
            by_state[u['state']].append(f"{u['chamber']}:{u['district_number']}")
        for state in sorted(by_state.keys()):
            items = by_state[state]
            if len(items) <= 5:
                print(f"    {state}: {', '.join(items)}")
            else:
                print(f"    {state}: {len(items)} unmatched ({', '.join(items[:3])}...)")

    if args.dry_run:
        print("\n[DRY RUN] Would update:")
        print(f"  - {len(matched)} legislative district margins (districts + elections)")
        print(f"  - {len(statewide_margins)} statewide district margins")
        # Count elections that would be updated
        district_ids = [m['district_id'] for m in matched]
        if district_ids:
            sample_ids = district_ids[:100]
            result = run_sql(f"""
                SELECT COUNT(*) as cnt FROM elections e
                JOIN seats s ON e.seat_id = s.id
                WHERE s.district_id IN ({','.join(str(x) for x in sample_ids)})
            """)
            sample_count = result[0]['cnt'] if result else 0
            est_total = sample_count * len(district_ids) / len(sample_ids)
            print(f"  - Estimated ~{int(est_total)} election rows would be updated (legislative)")
        # Count statewide elections
        if statewide_margins:
            result = run_sql("""
                SELECT COUNT(*) as cnt FROM elections e
                JOIN seats s ON e.seat_id = s.id
                JOIN districts d ON s.district_id = d.id
                WHERE d.office_level = 'Statewide'
            """)
            sw_count = result[0]['cnt'] if result else 0
            print(f"  - {sw_count} statewide election rows would be updated")

        # Show some samples
        print("\n  Sample margins:")
        for m in matched[:10]:
            print(f"    {m['state']} {m['chamber']} {m['district_number']}: {m['margin']}")
        return

    # ═══════════════════════════════════════════════════════════════
    # Step 3: Update districts table (pres_2024_margin + pres_2024_winner)
    # ═══════════════════════════════════════════════════════════════
    print("\nUpdating districts table...")
    district_updates = 0

    for i in range(0, len(matched), BATCH_SIZE):
        batch = matched[i:i+BATCH_SIZE]
        cases_margin = []
        cases_winner = []
        ids = []

        for m in batch:
            did = m['district_id']
            margin = m['margin']
            ids.append(str(did))

            # pres_2024_margin: store as signed number like "+12.3" or "-15.7"
            if margin == 'EVEN':
                cases_margin.append(f"WHEN {did} THEN '0.0'")
                cases_winner.append(f"WHEN {did} THEN NULL")
            elif margin.startswith('D+'):
                val = margin[2:]
                cases_margin.append(f"WHEN {did} THEN '+{val}'")
                cases_winner.append(f"WHEN {did} THEN 'D'")
            elif margin.startswith('R+'):
                val = margin[2:]
                cases_margin.append(f"WHEN {did} THEN '-{val}'")
                cases_winner.append(f"WHEN {did} THEN 'R'")

        sql = f"""
            UPDATE districts SET
                pres_2024_margin = CASE id {' '.join(cases_margin)} END,
                pres_2024_winner = CASE id {' '.join(cases_winner)} END
            WHERE id IN ({','.join(ids)})
        """
        run_sql(sql)
        district_updates += len(batch)
        print(f"  Districts updated: {district_updates}/{len(matched)}", end='\r')
        time.sleep(0.8)  # Rate limit

    # Also update statewide districts
    for state, margin in statewide_margins.items():
        key = (state, 'Statewide', 'Statewide')
        did = district_lookup.get(key)
        if did:
            if margin == 'EVEN':
                m_val, w_val = "'0.0'", "NULL"
            elif margin.startswith('D+'):
                m_val, w_val = f"'+{margin[2:]}'", "'D'"
            elif margin.startswith('R+'):
                m_val, w_val = f"'-{margin[2:]}'", "'R'"
            else:
                continue
            run_sql(f"UPDATE districts SET pres_2024_margin = {m_val}, pres_2024_winner = {w_val} WHERE id = {did}")
            district_updates += 1

    print(f"\n  Total districts updated: {district_updates}")

    # ═══════════════════════════════════════════════════════════════
    # Step 4: Update elections table (pres_margin_this_cycle)
    # ═══════════════════════════════════════════════════════════════
    print("\nUpdating elections table...")

    # Build district_id → margin map
    margin_by_district = {m['district_id']: m['margin'] for m in matched}
    # Add statewide
    for state, margin in statewide_margins.items():
        key = (state, 'Statewide', 'Statewide')
        did = district_lookup.get(key)
        if did:
            margin_by_district[did] = margin

    # Get all elections grouped by district
    all_district_ids = list(margin_by_district.keys())
    election_updates = 0

    for i in range(0, len(all_district_ids), BATCH_SIZE):
        batch_ids = all_district_ids[i:i+BATCH_SIZE]
        id_list = ','.join(str(x) for x in batch_ids)

        # Get election IDs for these districts
        rows = run_sql(f"""
            SELECT e.id as election_id, s.district_id
            FROM elections e
            JOIN seats s ON e.seat_id = s.id
            WHERE s.district_id IN ({id_list})
        """)

        if not rows:
            continue

        # Build update cases
        cases = []
        eids = []
        for r in rows:
            eid = r['election_id']
            did = r['district_id']
            margin = margin_by_district.get(did)
            if margin:
                cases.append(f"WHEN {eid} THEN '{esc(margin)}'")
                eids.append(str(eid))

        if not cases:
            continue

        # Update in sub-batches to avoid huge queries
        for j in range(0, len(cases), BATCH_SIZE):
            sub_cases = cases[j:j+BATCH_SIZE]
            sub_eids = eids[j:j+BATCH_SIZE]
            sql = f"""
                UPDATE elections SET
                    pres_margin_this_cycle = CASE id {' '.join(sub_cases)} END
                WHERE id IN ({','.join(sub_eids)})
            """
            run_sql(sql)
            election_updates += len(sub_cases)
            print(f"  Elections updated: {election_updates}", end='\r')
            time.sleep(0.8)  # Rate limit

    print(f"\n  Total elections updated: {election_updates}")

    # ═══════════════════════════════════════════════════════════════
    # Step 5: Verification
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("Verification:")

    # Count elections with margin populated
    result = run_sql("SELECT COUNT(*) as cnt FROM elections WHERE pres_margin_this_cycle IS NOT NULL")
    print(f"  Elections with margin: {result[0]['cnt']}")

    result = run_sql("SELECT COUNT(*) as cnt FROM elections WHERE pres_margin_this_cycle IS NULL")
    print(f"  Elections without margin: {result[0]['cnt']}")

    # Count districts with margin populated
    result = run_sql("SELECT COUNT(*) as cnt FROM districts WHERE pres_2024_margin IS NOT NULL")
    print(f"  Districts with margin: {result[0]['cnt']}")

    # Sample swing districts
    print("\n  Sample swing districts (closest margins):")
    result = run_sql("""
        SELECT s.abbreviation, d.chamber, d.district_number, d.pres_2024_margin, d.pres_2024_winner
        FROM districts d
        JOIN states s ON d.state_id = s.id
        WHERE d.pres_2024_margin IS NOT NULL
          AND d.office_level = 'Legislative'
        ORDER BY ABS(d.pres_2024_margin::numeric)
        LIMIT 10
    """)
    for r in result:
        print(f"    {r['abbreviation']} {r['chamber']} {r['district_number']}: {r['pres_2024_margin']} ({r['pres_2024_winner']})")


if __name__ == '__main__':
    main()
