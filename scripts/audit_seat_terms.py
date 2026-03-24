#!/usr/bin/env python3
"""
Audit seat_terms integrity.

Checks for:
1. Seats with current_holder but no open seat_term (missing seat_term)
2. Open seat_terms where candidate name != current_holder (name mismatch)
3. Open seat_terms on seats with no current_holder (stale seat_term)
4. Duplicate open seat_terms on the same seat
5. Seats with current_holder but NULL caucus (missing caucus)

Usage:
    python3 scripts/audit_seat_terms.py
    python3 scripts/audit_seat_terms.py --state FL
"""
import sys
import os
import argparse
import time

import httpx
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF


def run_sql(query, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = httpx.post(
                f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
                headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120,
            )
            if resp.status_code == 201:
                return resp.json()
            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f'  Rate limited, waiting {wait}s...')
                time.sleep(wait)
                continue
            print(f'SQL ERROR ({resp.status_code}): {resp.text[:500]}')
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return None
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                httpx.ReadTimeout, httpx.WriteTimeout) as e:
            wait = 5 * (attempt + 1)
            print(f'  Connection error: {e}, retrying in {wait}s...')
            time.sleep(wait)
            continue
    return None


def main():
    parser = argparse.ArgumentParser(description='Audit seat_terms integrity')
    parser.add_argument('--state', help='Filter to a specific state')
    args = parser.parse_args()

    state_filter = f"AND s.abbreviation = '{args.state}'" if args.state else ""
    issues = 0

    # ── Check 1: current_holder with no open seat_term ────────────
    print('=' * 60)
    print('CHECK 1: Seats with current_holder but no open seat_term')
    print('=' * 60)
    rows = run_sql(f"""
        SELECT s.abbreviation as state, se.seat_label, se.current_holder
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.current_holder IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM seat_terms st WHERE st.seat_id = se.id AND st.end_date IS NULL
          )
          {state_filter}
        ORDER BY s.abbreviation, se.seat_label
    """)
    if rows:
        issues += len(rows)
        print(f'  FOUND {len(rows)} seats with current_holder but no seat_term:')
        for r in rows:
            print(f'    {r["state"]} {r["seat_label"]}: {r["current_holder"]}')
    else:
        print('  OK — all seats with holders have matching seat_terms')

    # ── Check 2: seat_term name != current_holder ─────────────────
    print('\n' + '=' * 60)
    print('CHECK 2: Open seat_terms where name != current_holder')
    print('=' * 60)
    rows = run_sql(f"""
        SELECT s.abbreviation as state, se.seat_label,
               se.current_holder, c.full_name as term_holder
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN candidates c ON st.candidate_id = c.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE st.end_date IS NULL
          AND se.current_holder IS NOT NULL
          AND se.current_holder != c.full_name
          {state_filter}
        ORDER BY s.abbreviation, se.seat_label
    """)
    if rows:
        issues += len(rows)
        print(f'  FOUND {len(rows)} name mismatches:')
        for r in rows:
            print(f'    {r["state"]} {r["seat_label"]}: holder="{r["current_holder"]}" vs term="{r["term_holder"]}"')
    else:
        print('  OK — all names match')

    # ── Check 3: open seat_terms on vacant seats ──────────────────
    print('\n' + '=' * 60)
    print('CHECK 3: Open seat_terms on seats with no current_holder')
    print('=' * 60)
    rows = run_sql(f"""
        SELECT s.abbreviation as state, se.seat_label, c.full_name, st.start_date
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN candidates c ON st.candidate_id = c.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE st.end_date IS NULL AND se.current_holder IS NULL
          {state_filter}
        ORDER BY s.abbreviation, se.seat_label
    """)
    if rows:
        issues += len(rows)
        print(f'  FOUND {len(rows)} stale seat_terms on vacant seats:')
        for r in rows:
            print(f'    {r["state"]} {r["seat_label"]}: {r["full_name"]} (started {r["start_date"]})')
    else:
        print('  OK — no stale seat_terms')

    # ── Check 4: duplicate open seat_terms ────────────────────────
    print('\n' + '=' * 60)
    print('CHECK 4: Duplicate open seat_terms (multiple holders)')
    print('=' * 60)
    rows = run_sql(f"""
        SELECT s.abbreviation as state, se.seat_label, COUNT(*) as cnt
        FROM seat_terms st
        JOIN seats se ON st.seat_id = se.id
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE st.end_date IS NULL
          {state_filter}
        GROUP BY s.abbreviation, se.seat_label
        HAVING COUNT(*) > 1
        ORDER BY s.abbreviation, se.seat_label
    """)
    if rows:
        issues += len(rows)
        print(f'  FOUND {len(rows)} seats with multiple open seat_terms:')
        for r in rows:
            print(f'    {r["state"]} {r["seat_label"]}: {r["cnt"]} open terms')
    else:
        print('  OK — no duplicates')

    # ── Check 5: current_holder with NULL caucus ──────────────────
    print('\n' + '=' * 60)
    print('CHECK 5: Seats with current_holder but NULL caucus')
    print('=' * 60)
    rows = run_sql(f"""
        SELECT s.abbreviation as state, se.seat_label, se.current_holder, se.current_holder_party
        FROM seats se
        JOIN districts d ON se.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE se.current_holder IS NOT NULL
          AND se.current_holder_caucus IS NULL
          {state_filter}
        ORDER BY s.abbreviation, se.seat_label
    """)
    if rows:
        issues += len(rows)
        print(f'  FOUND {len(rows)} seats with NULL caucus:')
        for r in rows[:20]:
            print(f'    {r["state"]} {r["seat_label"]}: {r["current_holder"]} (party={r["current_holder_party"]})')
        if len(rows) > 20:
            print(f'    ... and {len(rows) - 20} more')
    else:
        print('  OK — all holders have caucus set')

    # ── Summary ───────────────────────────────────────────────────
    print('\n' + '=' * 60)
    if issues:
        print(f'TOTAL ISSUES: {issues}')
    else:
        print('ALL CHECKS PASSED')
    print('=' * 60)


if __name__ == '__main__':
    main()
