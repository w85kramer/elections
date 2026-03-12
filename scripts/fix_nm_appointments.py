#!/usr/bin/env python3
"""
Fix NM legislative seat_terms to properly reflect mid-term vacancies and appointments.

Based on Ballotpedia vacancy tracking data 2019-2025.
NM fills vacancies by appointment (county commissioners nominate, governor appoints).

Updates:
1. Fix departing legislators' end_date/end_reason (from term_expired to resigned/etc.)
2. Fix existing appointees' start_reason (from elected to appointed)
3. Create new seat_terms for interim appointees not yet in DB
4. Create candidate records for missing appointees
5. Delete erroneous duplicate records

Usage:
    python3 scripts/fix_nm_appointments.py --dry-run
    python3 scripts/fix_nm_appointments.py
"""

import json
import os
import sys
import time
import requests
from pathlib import Path

# Load env
env_path = Path(__file__).parent.parent / '.env'
env = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()

SUPABASE_TOKEN = env['SUPABASE_MANAGEMENT_TOKEN']
PROJECT_REF = 'pikcvwulzfxgwfcfssxc'
API_URL = f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query'

DRY_RUN = '--dry-run' in sys.argv


def run_sql(query, label=""):
    if DRY_RUN:
        print(f"[DRY RUN] {label}")
        return None
    for attempt in range(1, 6):
        r = requests.post(API_URL,
            headers={'Authorization': f'Bearer {SUPABASE_TOKEN}', 'Content-Type': 'application/json'},
            json={'query': query})
        if r.status_code in (200, 201):
            try:
                return r.json()
            except:
                return []
        if r.status_code == 429:
            wait = 5 * attempt
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  ERROR ({r.status_code}): {r.text[:300]}")
        return None
    print(f"  Failed after 5 retries")
    return None


def esc(s):
    return s.replace("'", "''")


def main():
    print(f"{'[DRY RUN] ' if DRY_RUN else ''}NM Legislative Appointment Fixes")
    print("=" * 60)

    # ============================================================
    # STEP 1: Create missing candidate records
    # ============================================================
    print("\n--- Step 1: Create missing candidates ---")

    missing_candidates = [
        ('Viengkeo Kay Bounkeua', 'Viengkeo Kay', 'Bounkeua'),
        ('Linda Garcia Benavides', 'Linda', 'Garcia Benavides'),
        ('Dan Barrone', 'Dan', 'Barrone'),
        ('Roberto Gonzales', 'Roberto', 'Gonzales'),
        ('Cristina Parajon', 'Cristina', 'Parajon'),
    ]

    # Check which already exist
    names_sql = ", ".join(f"'{esc(n[0])}'" for n in missing_candidates)
    existing = run_sql(f"SELECT id, full_name FROM candidates WHERE full_name IN ({names_sql})", "Check existing")
    existing_names = set()
    if existing:
        existing_names = {r['full_name'] for r in existing}
        print(f"  Already exist: {existing_names}")

    for full, first, last in missing_candidates:
        if full in existing_names:
            continue
        sql = f"INSERT INTO candidates (full_name, first_name, last_name) VALUES ('{esc(full)}', '{esc(first)}', '{esc(last)}') RETURNING id"
        result = run_sql(sql, f"Create {full}")
        if result:
            print(f"  Created: {full} (id={result[0]['id']})")
        else:
            print(f"  {'Would create' if DRY_RUN else 'Failed'}: {full}")
        time.sleep(1)

    # ============================================================
    # STEP 2: Fix departure end_reasons/end_dates
    # ============================================================
    print("\n--- Step 2: Fix departure records ---")

    # (seat_term_id, new_end_date, new_end_reason, description)
    departures = [
        # HD-3: T. Ryan Lane resigned March 14, 2024
        (32588, '2024-03-14', 'resigned', 'HD-3 T. Ryan Lane'),
        # HD-6: Eliseo Alcon resigned Nov 25, 2024
        (32705, '2024-11-25', 'resigned', 'HD-6 Eliseo Alcon'),
        # HD-8: Alonzo Baldonado resigned Dec 31, 2021
        (32759, '2021-12-31', 'resigned', 'HD-8 Alonzo Baldonado'),
        # HD-12: Patricio Ruiloba resigned Aug 10, 2020
        (32511, '2020-08-10', 'resigned', 'HD-12 Patricio Ruiloba'),
        # HD-12: Brittney Barreras resigned Jan 28, 2022
        (32512, '2022-01-28', 'resigned', 'HD-12 Brittney Barreras'),
        # HD-16: Antonio Maestas appointed to SD-26, Nov 16, 2022
        (32527, '2022-11-16', 'appointed_elsewhere', 'HD-16 Antonio Maestas → SD-26'),
        # HD-17: Deborah Armstrong resigned July 15, 2022
        (32532, '2022-07-15', 'resigned', 'HD-17 Deborah Armstrong'),
        # HD-19: Sheryl Williams Stapleton resigned July 30, 2021 (indicted)
        (32541, '2021-07-30', 'resigned', 'HD-19 Sheryl Williams Stapleton (indicted)'),
        # HD-45: Jim Trujillo resigned Sept 28, 2020
        (32643, '2020-09-28', 'resigned', 'HD-45 Jim Trujillo'),
        # HD-48: Linda Trujillo resigned July 9, 2020
        (32652, '2020-07-09', 'resigned', 'HD-48 Linda Trujillo'),
        # HD-59: Greg Nibert appointed to SD-27, Jan 4, 2024
        (32698, '2024-01-04', 'appointed_elsewhere', 'HD-59 Greg Nibert → SD-27'),
        # SD-26: Jacob Candelaria resigned Oct 19, 2022
        (32798, '2022-10-19', 'resigned', 'SD-26 Jacob Candelaria'),
        # SD-27: Stuart Ingle resigned Oct 25, 2023
        (32802, '2023-10-25', 'resigned', 'SD-27 Stuart Ingle'),
        # SD-28: Howie Morales became Lt Gov, Jan 1, 2019
        (32805, '2019-01-01', 'appointed_elsewhere', 'SD-28 Howie Morales → Lt Gov'),
        # SD-42: Gay Kernan resigned Aug 1, 2023
        (32835, '2023-08-01', 'resigned', 'SD-42 Gay Kernan'),
    ]

    departure_sql_parts = []
    for st_id, end_date, end_reason, desc in departures:
        departure_sql_parts.append(
            f"UPDATE seat_terms SET end_date = '{end_date}', end_reason = '{end_reason}' WHERE id = {st_id}"
        )
        print(f"  {desc}: end_date → {end_date}, end_reason → {end_reason}")

    if departure_sql_parts:
        combined = ";\n".join(departure_sql_parts) + ";"
        run_sql(combined, "Batch departure updates")
        print(f"  Updated {len(departure_sql_parts)} departure records")
    time.sleep(2)

    # ============================================================
    # STEP 3: Fix existing appointee start_reasons
    # ============================================================
    print("\n--- Step 3: Fix existing appointee start_reasons ---")

    # (seat_term_id, new_start_date, new_start_reason, optional end_date, optional end_reason, description)
    appointee_fixes = [
        # HD-3: Bill Hall appointed April 4, 2024
        (11123, '2024-04-04', 'appointed', None, None, 'HD-3 Bill Hall'),
        # HD-6: Martha Garcia appointed Feb 26, 2025
        (11185, '2025-02-26', 'appointed', None, None, 'HD-6 Martha Garcia'),
        # HD-8: Brian Baca appointed Jan 19, 2022 (his 2023-2025 term)
        (47409, '2022-01-19', 'appointed', None, None, 'HD-8 Brian Baca'),
        # HD-16: Yanira Gurrola appointed Jan 17, 2023
        (11222, '2023-01-17', 'appointed', None, None, 'HD-16 Yanira Gurrola'),
        # HD-59: Jared Hembree appointed Jan 14, 2024, resigned Jan 11, 2025
        (32699, '2024-01-14', 'appointed', '2025-01-11', 'resigned', 'HD-59 Jared Hembree'),
        # HD-59: Mark Murphy appointed Jan 21, 2025
        (11184, '2025-01-21', 'appointed', None, None, 'HD-59 Mark Murphy'),
        # SD-26: Antonio Maestas appointed Nov 15, 2022
        (32799, '2022-11-15', 'appointed', None, None, 'SD-26 Antonio Maestas'),
        # SD-28: Siah Correa Hemphill — her 2021-2025 term was via election after being appointed
        # The appointment was Dec 29, 2020 filling remainder of Ramos's term; she was elected for 2021+
        # So her 2021-2025 elected term (32806) is actually correct, but start_date may need adjusting
        # Actually she was appointed 2020-12-29 to finish term, then elected 2021-01-01 — leave 32806 as elected
    ]

    fix_sql_parts = []
    for item in appointee_fixes:
        st_id, start_date, start_reason = item[0], item[1], item[2]
        end_date, end_reason, desc = item[3], item[4], item[5]
        sql = f"UPDATE seat_terms SET start_date = '{start_date}', start_reason = '{start_reason}'"
        if end_date:
            sql += f", end_date = '{end_date}', end_reason = '{end_reason}'"
        sql += f" WHERE id = {st_id}"
        fix_sql_parts.append(sql)
        extra = f", end → {end_date} ({end_reason})" if end_date else ""
        print(f"  {desc}: start → {start_date} (appointed){extra}")

    if fix_sql_parts:
        combined = ";\n".join(fix_sql_parts) + ";"
        run_sql(combined, "Batch appointee fixes")
        print(f"  Updated {len(fix_sql_parts)} appointee records")
    time.sleep(2)

    # ============================================================
    # STEP 4: Delete erroneous duplicate records
    # ============================================================
    print("\n--- Step 4: Delete erroneous records ---")

    # HD-6: Alcon duplicate term (2025-01-01 to 2025-01-01) — data artifact
    deletes = [32706]
    for st_id in deletes:
        run_sql(f"DELETE FROM seat_terms WHERE id = {st_id}", f"Delete seat_term {st_id}")
        print(f"  Deleted seat_term {st_id} (HD-6 Alcon duplicate)")
    time.sleep(2)

    # ============================================================
    # STEP 5: Create new seat_terms for interim appointees
    # ============================================================
    print("\n--- Step 5: Create new seat_terms for interim appointees ---")

    # Need to look up candidate IDs for each appointee
    # First get all needed candidate IDs
    needed_names = [
        'Art De La Cruz', 'Linda Garcia Benavides', 'Viengkeo Kay Bounkeua',
        'Linda Serrato', 'Tara Luján', 'Marsella Duarte', 'Greg Nibert',
        'Gabriel Ramos', 'Siah Correa Hemphill', 'Steve McCutcheon',
        'Dan Barrone', 'Roberto Gonzales', 'Cristina Parajon',
        'Shannon Pinto', 'Marian Matthews', 'Antoinette Sedillo Lopez',
    ]
    names_in = ", ".join(f"'{esc(n)}'" for n in needed_names)
    cands = run_sql(f"SELECT id, full_name FROM candidates WHERE full_name IN ({names_in})", "Get candidate IDs")

    if DRY_RUN:
        print("  [DRY RUN] Would look up candidate IDs and create terms")
        # Still print what we'd do
        new_terms_desc = [
            "HD-12: Art De La Cruz appointed 2020-09-09 → 2021-01-01",
            "HD-12: Art De La Cruz appointed 2022-02-02 → 2023-01-01",
            "HD-17: Linda Garcia Benavides appointed 2022-09-07 → 2023-01-01",
            "HD-19: Viengkeo Kay Bounkeua appointed 2021-08-24 → 2023-01-01",
            "HD-45: Linda Serrato appointed 2020-10-06 → 2021-01-01",
            "HD-48: Tara Luján appointed 2020-07-23 → 2021-01-01",
            "HD-16: Marsella Duarte appointed 2022-12-14 → 2022-12-31 (resigned)",
            "SD-27: Greg Nibert appointed 2024-01-05 → 2025-01-01",
            "SD-28: Gabriel Ramos appointed 2019-01-15 → 2020-12-06 (resigned)",
            "SD-28: Siah Correa Hemphill appointed 2020-12-29 → 2021-01-01",
            "SD-42: Steve McCutcheon appointed 2023-09-15 → 2025-01-01",
            "HD-25: Cristina Parajon appointed 2023-08-17 → 2025-01-01",
        ]
        for d in new_terms_desc:
            print(f"  Would create: {d}")
        print(f"\n  Total new seat_terms: {len(new_terms_desc)}")
        return

    if not cands:
        print("  ERROR: Could not fetch candidate IDs")
        return

    # Build candidate ID lookup (use the NM-specific IDs, preferring lower IDs for original records)
    cand_lookup = {}
    for c in cands:
        name = c['full_name']
        cid = c['id']
        # Keep the lowest ID (original record) for each name
        if name not in cand_lookup or cid < cand_lookup[name]:
            cand_lookup[name] = cid

    print(f"  Candidate IDs: {json.dumps(cand_lookup, indent=2)}")

    # Seat IDs (from earlier query)
    seat_ids = {
        ('House', '3'): 4766, ('House', '6'): 4799, ('House', '8'): 4812,
        ('House', '12'): 4747, ('House', '16'): 4751, ('House', '17'): 4752,
        ('House', '19'): 4754, ('House', '25'): 4761, ('House', '27'): 4763,
        ('House', '42'): 4780, ('House', '45'): 4783, ('House', '48'): 4786,
        ('House', '59'): 4798,
        ('Senate', '3'): 4836, ('Senate', '6'): 4852, ('Senate', '16'): 4821,
        ('Senate', '26'): 4832, ('Senate', '27'): 4833, ('Senate', '28'): 4834,
        ('Senate', '33'): 4840, ('Senate', '42'): 4850,
    }

    # (seat_key, candidate_name, start_date, end_date, start_reason, end_reason, party, notes)
    new_terms = [
        # HD-12: Art De La Cruz first appointment (Ruiloba resigned)
        (('House', '12'), 'Art De La Cruz', '2020-09-09', '2021-01-01', 'appointed', 'term_expired', 'D', 'Appointed after Ruiloba resignation'),
        # HD-12: Art De La Cruz second appointment (Barreras resigned)
        (('House', '12'), 'Art De La Cruz', '2022-02-02', '2023-01-01', 'appointed', 'term_expired', 'D', 'Re-appointed after Barreras resignation'),
        # HD-17: Linda Garcia Benavides (Armstrong resigned)
        (('House', '17'), 'Linda Garcia Benavides', '2022-09-07', '2023-01-01', 'appointed', 'term_expired', 'D', 'Appointed after Armstrong resignation'),
        # HD-19: Viengkeo Kay Bounkeua (Stapleton resigned/indicted)
        (('House', '19'), 'Viengkeo Kay Bounkeua', '2021-08-24', '2023-01-01', 'appointed', 'term_expired', 'D', 'Appointed after Stapleton resignation (indictment)'),
        # HD-45: Linda Serrato first appointment (Trujillo resigned)
        (('House', '45'), 'Linda Serrato', '2020-10-06', '2021-01-01', 'appointed', 'term_expired', 'D', 'Appointed after J. Trujillo resignation'),
        # HD-48: Tara Luján first appointment (L. Trujillo resigned)
        (('House', '48'), 'Tara Luján', '2020-07-23', '2021-01-01', 'appointed', 'term_expired', 'D', 'Appointed after L. Trujillo resignation'),
        # HD-16: Marsella Duarte (Maestas appointed elsewhere, chain vacancy)
        (('House', '16'), 'Marsella Duarte', '2022-12-14', '2022-12-31', 'appointed', 'resigned', 'D', 'Appointed after Maestas elevated to SD-26; resigned after 17 days'),
        # SD-27: Greg Nibert (Ingle resigned)
        (('Senate', '27'), 'Greg Nibert', '2024-01-05', '2025-01-01', 'appointed', 'term_expired', 'R', 'Appointed after Ingle resignation; previously HD-59 rep'),
        # SD-28: Gabriel Ramos first appointment (Morales became Lt Gov)
        (('Senate', '28'), 'Gabriel Ramos', '2019-01-15', '2020-12-06', 'appointed', 'resigned', 'D', 'Appointed after Morales became Lt Governor'),
        # SD-28: Siah Correa Hemphill appointment (Ramos resigned)
        (('Senate', '28'), 'Siah Correa Hemphill', '2020-12-29', '2021-01-01', 'appointed', 'term_expired', 'D', 'Appointed after Ramos resignation; already won Nov 2020 election'),
        # SD-42: Steve McCutcheon (Kernan resigned)
        (('Senate', '42'), 'Steve McCutcheon', '2023-09-15', '2025-01-01', 'appointed', 'term_expired', 'R', 'Appointed after Kernan resignation; lost 2024 R primary'),
        # HD-25: Cristina Parajon (Christine Trujillo resigned)
        (('House', '25'), 'Cristina Parajon', '2023-08-17', '2025-01-01', 'appointed', 'term_expired', 'D', 'Appointed after Christine Trujillo resignation'),
    ]

    created = 0
    for seat_key, cand_name, start, end, s_reason, e_reason, party, notes in new_terms:
        seat_id = seat_ids.get(seat_key)
        cand_id = cand_lookup.get(cand_name)

        if not seat_id:
            print(f"  ERROR: No seat_id for {seat_key}")
            continue
        if not cand_id:
            print(f"  ERROR: No candidate_id for {cand_name}")
            continue

        notes_sql = f"'{esc(notes)}'" if notes else 'NULL'
        sql = f"""INSERT INTO seat_terms (seat_id, candidate_id, start_date, end_date, start_reason, end_reason, party, notes)
        VALUES ({seat_id}, {cand_id}, '{start}', '{end}', '{s_reason}', '{e_reason}', '{party}', {notes_sql})"""

        result = run_sql(sql, f"Create {cand_name} term ({seat_key[0]} {seat_key[1]})")
        if result is not None:
            print(f"  Created: {seat_key[0]} {seat_key[1]}: {cand_name} ({start} → {end}, {s_reason})")
            created += 1
        else:
            print(f"  FAILED: {cand_name} ({seat_key[0]} {seat_key[1]})")
        time.sleep(1)

    # ============================================================
    # STEP 6: Also handle pre-2022 vacancies that need Senate terms
    # ============================================================
    print("\n--- Step 6: Handle additional pre-2022 cycle vacancies ---")

    # SD-3: John Pinto died May 24, 2019 → Shannon Pinto appointed July 25, 2019
    # SD-6: Carlos Cisneros died Sept 17, 2019 → Roberto Gonzales appointed Dec 20, 2019
    # HD-42: Roberto Gonzales appointed to SD-6 → Dan Barrone appointed Jan 4, 2020
    # HD-27: William Pratt died Dec 25, 2019 → Marian Matthews appointed Jan 7, 2020
    # SD-16: Cisco McSorley resigned Jan 1, 2019 → Antoinette Sedillo Lopez appointed Jan 14, 2019

    # These are older (2019-2020) and may not have seat_terms in the DB.
    # Let's check if we have terms for these people and districts
    check_sql = """
    SELECT st.id, st.start_date, st.end_date, st.start_reason, st.end_reason, c.full_name, d.district_number, d.chamber
    FROM seat_terms st
    JOIN candidates c ON st.candidate_id = c.id
    JOIN seats s ON st.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states ss ON d.state_id = ss.id
    WHERE ss.abbreviation = 'NM' AND d.office_level = 'Legislative'
    AND d.redistricting_cycle = '2022'
    AND ((d.chamber = 'Senate' AND d.district_number IN ('3', '6', '16'))
         OR (d.chamber = 'House' AND d.district_number IN ('27', '42')))
    ORDER BY d.chamber, d.district_number::int, st.start_date
    """
    pre_results = run_sql(check_sql, "Check pre-2022 vacancy districts")
    if pre_results:
        for r in pre_results:
            print(f"  {r['chamber']} {r['district_number']}: {r['full_name']} {r['start_date']}→{r['end_date']} ({r['start_reason']}/{r['end_reason']}) id={r['id']}")

    # ============================================================
    # Summary
    # ============================================================
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"  Departure records fixed: {len(departures)}")
    print(f"  Appointee start_reasons fixed: {len(appointee_fixes)}")
    print(f"  Erroneous records deleted: {len(deletes)}")
    print(f"  New seat_terms created: {created}")
    if DRY_RUN:
        print(f"\n  [DRY RUN — no changes made]")


if __name__ == '__main__':
    main()
