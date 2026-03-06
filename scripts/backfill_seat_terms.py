#!/usr/bin/env python3
"""
Backfill historical seat_terms from candidacy wins.

For each seat, walks through election history chronologically and creates
seat_term records for winners who don't already have one. Infers start/end
dates from election dates and subsequent elections.

Usage:
    python3 scripts/backfill_seat_terms.py --dry-run            # Preview changes
    python3 scripts/backfill_seat_terms.py --state VA --dry-run  # Single state preview
    python3 scripts/backfill_seat_terms.py                       # Execute all states
    python3 scripts/backfill_seat_terms.py --state VA            # Execute single state
"""

import sys
import os
import json
import time
import argparse
from collections import defaultdict

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

# States where the most common swearing-in date is NOT Jan 1.
# We still default to Jan 1 — these small offsets (Jan 8, Jan 14, etc.)
# aren't worth the complexity. The start_date is approximate anyway.
# Special elections use the election date + 1 month as a rough estimate.

MIN_EXPORT_YEAR = {
    'NE': 2014,
}


def run_sql(query, retries=5):
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
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        sys.exit(1)


def backfill_terms(dry_run=False, single_state=None):
    label = single_state or 'all states'
    print(f'Backfilling seat_terms for {label}...')

    state_filter = f"AND st.abbreviation = '{single_state}'" if single_state else ""

    # Query 1: All elections with their winners (General + Special only)
    q_winners = f"""
        SELECT
            st.abbreviation as state,
            e.id as election_id,
            e.seat_id,
            e.election_date,
            e.election_year,
            e.election_type,
            cy.candidate_id,
            c.full_name,
            cy.party,
            cy.caucus,
            d.chamber,
            d.district_number
        FROM candidacies cy
        JOIN elections e ON cy.election_id = e.id
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        JOIN candidates c ON cy.candidate_id = c.id
        WHERE cy.result = 'Won'
          AND e.election_type NOT LIKE 'Primary%%'
          AND e.election_type NOT LIKE '%%_D'
          AND e.election_type NOT LIKE '%%_R'
          AND e.election_type NOT LIKE '%%_L'
          AND e.election_type NOT LIKE '%%_Nonpartisan'
          AND e.election_type != 'Runoff'
          AND s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
          {state_filter}
        ORDER BY st.abbreviation, e.seat_id, e.election_year, e.election_date
    """

    # Query 2: Existing seat_terms (to avoid duplicates)
    q_existing = f"""
        SELECT
            st.abbreviation as state,
            stm.seat_id,
            stm.candidate_id,
            stm.start_date,
            stm.end_date,
            stm.start_reason,
            stm.end_reason,
            stm.election_id
        FROM seat_terms stm
        JOIN seats s ON stm.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        WHERE s.office_level = 'Legislative'
          AND COALESCE(d.redistricting_cycle, '2022') = '2022'
          {state_filter}
        ORDER BY st.abbreviation, stm.seat_id, stm.start_date
    """

    print('  Running 2 queries...')
    winners_data = run_sql(q_winners)
    print(f'    Winners: {len(winners_data)} rows')
    existing_data = run_sql(q_existing)
    print(f'    Existing seat_terms: {len(existing_data)} rows')

    # Index existing terms by (seat_id, candidate_id, election_id)
    # Also track date ranges per (seat_id, candidate_id) to detect overlaps
    existing_by_election = set()
    existing_ranges = defaultdict(list)  # (seat_id, cand_id) -> [(start, end), ...]
    for r in existing_data:
        existing_by_election.add((r['seat_id'], r['candidate_id'], r['election_id']))
        existing_ranges[(r['seat_id'], r['candidate_id'])].append(
            (r['start_date'], r['end_date'])
        )

    # Group winners by seat_id, in chronological order
    winners_by_seat = defaultdict(list)
    for r in winners_data:
        winners_by_seat[r['seat_id']].append(r)

    # Build new seat_terms
    new_terms = []
    skipped = 0

    for seat_id, winners in winners_by_seat.items():
        # Sort by election year/date
        winners.sort(key=lambda w: (w['election_year'], w['election_date'] or ''))

        for i, w in enumerate(winners):
            # Skip if this candidate already has a term for this seat from this election
            if (seat_id, w['candidate_id'], w['election_id']) in existing_by_election:
                skipped += 1
                continue

            state = w['state']
            min_year = MIN_EXPORT_YEAR.get(state, 0)
            if w['election_year'] < min_year:
                continue

            is_special = 'Special' in w['election_type']

            # Infer start_date
            if is_special and w['election_date']:
                start_date = w['election_date']
            elif w['election_date']:
                start_year = w['election_year'] + 1
                start_date = f'{start_year}-01-01'
            else:
                start_year = w['election_year'] + 1
                start_date = f'{start_year}-01-01'

            # Skip if an existing term already covers this start_date
            # (e.g. original current-holder term with no election_id)
            start_str = str(start_date)
            overlaps = False
            for (ex_start, ex_end) in existing_ranges.get((seat_id, w['candidate_id']), []):
                ex_s = str(ex_start) if ex_start else '0000-01-01'
                ex_e = str(ex_end) if ex_end else '9999-12-31'
                if ex_s <= start_str <= ex_e:
                    overlaps = True
                    break
            if overlaps:
                skipped += 1
                continue

            # Infer end_date from the NEXT winner of this seat
            end_date = None
            end_reason = None
            if i + 1 < len(winners):
                next_w = winners[i + 1]
                # If next winner is the same candidate, they won re-election
                # so this term ended and a new one started
                if next_w['candidate_id'] == w['candidate_id']:
                    # Same person re-elected — this term expired, next one starts
                    if 'Special' in next_w['election_type'] and next_w['election_date']:
                        end_date = next_w['election_date']
                    else:
                        end_year = next_w['election_year'] + 1
                        end_date = f'{end_year}-01-01'
                    end_reason = 'term_expired'
                else:
                    # Different person won — this person lost or didn't run
                    if 'Special' in next_w['election_type'] and next_w['election_date']:
                        end_date = next_w['election_date']
                    else:
                        end_year = next_w['election_year'] + 1
                        end_date = f'{end_year}-01-01'
                    end_reason = 'term_expired'
            else:
                # Last winner for this seat — check if they're still current
                # If they already have a current term (end_date=NULL) in existing_set,
                # we skipped them above. If we're here, they don't have a term at all.
                # They might still be the current holder (no successor yet),
                # or they lost/left and there's no recorded successor.
                # Leave end_date=NULL — will be correct for current holders.
                # For former holders where no successor won, this creates an
                # open-ended term that will need manual cleanup.
                pass

            new_terms.append({
                'seat_id': seat_id,
                'candidate_id': w['candidate_id'],
                'party': w['party'],
                'caucus': w['caucus'],
                'start_date': str(start_date),
                'end_date': str(end_date) if end_date else None,
                'start_reason': 'elected',
                'end_reason': end_reason,
                'election_id': w['election_id'],
                # For logging
                '_name': w['full_name'],
                '_state': state,
                '_chamber': w['chamber'],
                '_district': w['district_number'],
                '_year': w['election_year'],
                '_type': w['election_type'],
            })

    print(f'\n  Existing terms matched (skipped): {skipped}')
    print(f'  New terms to create: {len(new_terms)}')

    if not new_terms:
        print('  Nothing to do.')
        return

    # Show sample
    print('\n  Sample new terms:')
    for t in new_terms[:10]:
        end_str = t['end_date'] or 'present'
        print(f"    {t['_state']} {t['_chamber']} {t['_district']}: {t['_name']} ({t['party']}) "
              f"{t['start_date']} – {end_str} [from {t['_year']} {t['_type']}]")
    if len(new_terms) > 10:
        print(f'    ... and {len(new_terms) - 10} more')

    # Count by state
    by_state = defaultdict(int)
    for t in new_terms:
        by_state[t['_state']] += 1
    print('\n  Per-state counts:')
    for st in sorted(by_state):
        print(f'    {st}: {by_state[st]}')

    if dry_run:
        print('\n  DRY RUN — no changes made.')
        return

    # Insert in batches
    BATCH_SIZE = 200
    total_inserted = 0
    batches = [new_terms[i:i+BATCH_SIZE] for i in range(0, len(new_terms), BATCH_SIZE)]

    print(f'\n  Inserting {len(new_terms)} terms in {len(batches)} batches...')
    for bi, batch in enumerate(batches):
        values = []
        for t in batch:
            caucus_val = f"'{t['caucus']}'" if t['caucus'] else 'NULL'
            party_val = f"'{t['party']}'" if t['party'] else 'NULL'
            end_date_val = f"'{t['end_date']}'" if t['end_date'] else 'NULL'
            end_reason_val = f"'{t['end_reason']}'" if t['end_reason'] else 'NULL'
            election_id_val = str(t['election_id']) if t['election_id'] else 'NULL'
            values.append(
                f"({t['seat_id']}, {t['candidate_id']}, {party_val}, '{t['start_date']}', "
                f"{end_date_val}, '{t['start_reason']}', {end_reason_val}, "
                f"{caucus_val}, {election_id_val})"
            )

        sql = f"""
            INSERT INTO seat_terms (seat_id, candidate_id, party, start_date, end_date,
                                     start_reason, end_reason, caucus, election_id)
            VALUES {', '.join(values)}
            ON CONFLICT DO NOTHING
        """
        run_sql(sql)
        total_inserted += len(batch)
        print(f'    Batch {bi+1}/{len(batches)}: {total_inserted}/{len(new_terms)} inserted')

    print(f'\n  Done — {total_inserted} seat_terms created.')


def main():
    parser = argparse.ArgumentParser(description='Backfill historical seat_terms from candidacy wins')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    args = parser.parse_args()

    if args.state:
        backfill_terms(dry_run=args.dry_run, single_state=args.state.upper())
    else:
        backfill_terms(dry_run=args.dry_run)

    print('\nDone.')


if __name__ == '__main__':
    main()
