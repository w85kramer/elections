"""
Analyze uncontested elections from candidacy data.

Identifies uncontested primaries (0-1 active non-write-in candidates), uncontested
generals, missed recruitment opportunities in competitive districts, and generates
summary statistics per state.

Only analyzes elections that have candidacy data loaded (gates with
HAVING COUNT(cy.id) > 0). For 2026, restricts --all-closed to states with closed
filing deadlines. For past years, --all-states analyzes every state with data.

Usage:
    python3 scripts/analyze_uncontested.py --state TX
    python3 scripts/analyze_uncontested.py --state TX --year 2022
    python3 scripts/analyze_uncontested.py --all-closed
    python3 scripts/analyze_uncontested.py --all-states --year 2024 --summary-only
    python3 scripts/analyze_uncontested.py --state TX --json
    python3 scripts/analyze_uncontested.py --state TX --summary-only
    python3 scripts/analyze_uncontested.py --dry-run
    python3 scripts/analyze_uncontested.py --all-closed --margin-threshold 10
"""
import sys
import os
import json
import time
import argparse

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

# Import CLOSED_FILING_STATES from populate_candidacies
from populate_candidacies import CLOSED_FILING_STATES

# All 50 state abbreviations
ALL_STATES = [
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
]

DEFAULT_MARGIN_THRESHOLD = 15
DEFAULT_YEAR = 2026

# Election types to analyze: primaries always, generals when data exists
PRIMARY_TYPES = ('Primary_D', 'Primary_R')
ALL_ELECTION_TYPES = ('Primary_D', 'Primary_R', 'General')


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
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'SQL ERROR: {resp.status_code} - {resp.text[:500]}')
        if exit_on_error:
            sys.exit(1)
        return None


# Effective partisan alignment (same as export_site_data.py)
EP = ("CASE WHEN s.current_holder_caucus = 'C' THEN s.current_holder_party "
      "ELSE COALESCE(s.current_holder_caucus, s.current_holder_party) END")


def build_election_counts_cte(state_abbr, year=2026):
    """Build the common CTE for uncontested analysis across primaries and generals."""
    type_list = "','".join(ALL_ELECTION_TYPES)
    return f"""
    WITH election_counts AS (
        SELECT
            e.id,
            st.abbreviation as state,
            e.election_type,
            d.chamber,
            d.district_number,
            s.seat_label,
            d.pres_2024_margin,
            d.pres_2024_winner,
            s.current_holder,
            e.is_open_seat,
            {EP} as holder_party,
            COUNT(cy.id) FILTER (
                WHERE cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                  AND cy.is_write_in = FALSE
            ) as active_count,
            COUNT(cy.id) as total_candidacies,
            STRING_AGG(
                CASE WHEN cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                          AND cy.is_write_in = FALSE THEN c.full_name END, ', '
            ) as candidate_names
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN candidacies cy ON cy.election_id = e.id
        LEFT JOIN candidates c ON cy.candidate_id = c.id
        WHERE e.election_year = {year}
          AND e.election_type IN ('{type_list}')
          AND s.office_level = 'Legislative'
          AND st.abbreviation = '{state_abbr}'
        GROUP BY e.id, st.abbreviation, e.election_type, d.chamber, d.district_number,
                 s.seat_label, d.pres_2024_margin, d.pres_2024_winner, s.current_holder,
                 s.current_holder_caucus, s.current_holder_party, e.is_open_seat
        HAVING COUNT(cy.id) > 0
    )"""


def query_uncontested_detail(state_abbr, year=2026):
    """Query 1: All uncontested elections (active_count <= 1) for a state."""
    cte = build_election_counts_cte(state_abbr, year)
    return f"""{cte}
    SELECT * FROM election_counts WHERE active_count <= 1
    ORDER BY election_type, chamber,
        CASE WHEN district_number SIMILAR TO '[0-9]+' THEN district_number::int ELSE 99999 END,
        district_number
    """


def query_missed_opportunities(state_abbr, threshold, year=2026):
    """Query 2: Competitive districts with 0 candidates filed for one party (primaries only)."""
    cte = build_election_counts_cte(state_abbr, year)
    return f"""{cte}
    SELECT * FROM election_counts
    WHERE active_count = 0
      AND election_type IN ('Primary_D','Primary_R')
      AND pres_2024_margin IS NOT NULL
      AND (
          (election_type = 'Primary_D' AND (
              pres_2024_winner = 'D'
              OR ABS(pres_2024_margin::numeric) <= {threshold}
          ))
          OR
          (election_type = 'Primary_R' AND (
              pres_2024_winner = 'R'
              OR ABS(pres_2024_margin::numeric) <= {threshold}
          ))
      )
    ORDER BY election_type, chamber,
        CASE WHEN district_number SIMILAR TO '[0-9]+' THEN district_number::int ELSE 99999 END,
        district_number
    """


def query_summary(state_abbr, year=2026):
    """Query 3: Summary totals per election_type and chamber."""
    cte = build_election_counts_cte(state_abbr, year)
    return f"""{cte}
    SELECT election_type, chamber,
        COUNT(*) as total_with_data,
        COUNT(*) FILTER (WHERE active_count <= 1) as uncontested,
        COUNT(*) FILTER (WHERE active_count = 0) as no_candidates
    FROM election_counts
    GROUP BY election_type, chamber
    ORDER BY election_type, chamber
    """


def format_margin(margin_str):
    """Format a pres_2024_margin string like '+12.3' or '-5.7' into 'R+12.3' or 'D+5.7'."""
    if not margin_str:
        return '—'
    try:
        val = float(margin_str)
        if val > 0:
            return f'R+{val:.1f}'
        elif val < 0:
            return f'D+{abs(val):.1f}'
        else:
            return 'EVEN'
    except (ValueError, TypeError):
        return str(margin_str)


def print_summary(summary_rows, state_abbr, year, filing_date=None):
    """Print the summary section."""
    w = 60
    print()
    print(f'{"":═<{w}}')
    print(f'  UNCONTESTED ELECTIONS — {state_abbr} {year}')
    print(f'{"":═<{w}}')
    if filing_date:
        print(f'Filing: Closed ({filing_date})')
    print()

    # Organize by election type
    by_type = {}
    for row in summary_rows:
        by_type.setdefault(row['election_type'], {})[row['chamber']] = row

    print('SUMMARY')
    type_labels = {
        'Primary_D': 'Dem primaries',
        'Primary_R': 'GOP primaries',
        'General': 'Generals',
    }
    for etype in ('Primary_D', 'Primary_R', 'General'):
        by_chamber = by_type.get(etype, {})
        if not by_chamber:
            continue
        label = type_labels.get(etype, etype)
        parts = []
        for chamber in sorted(by_chamber.keys()):
            row = by_chamber[chamber]
            total = row['total_with_data']
            unc = row['uncontested']
            pct = (unc / total * 100) if total > 0 else 0
            parts.append(f'{unc}/{total} {chamber} ({pct:.0f}%)')
        print(f'  {label:16s} {", ".join(parts)} uncontested')

    return by_type


def print_missed_opportunities(missed_rows, threshold):
    """Print the missed opportunities section."""
    if not missed_rows:
        return

    print()
    print(f'MISSED OPPORTUNITIES (no candidate, margin <={threshold}pt or favorable)')

    # Split by party
    d_missed = [r for r in missed_rows if r['election_type'] == 'Primary_D']
    r_missed = [r for r in missed_rows if r['election_type'] == 'Primary_R']

    if d_missed:
        print('  No Dem filed in competitive/favorable districts:')
        for r in d_missed:
            holder = r['current_holder'] or 'open seat'
            if r['is_open_seat']:
                holder = 'open seat'
            margin = format_margin(r['pres_2024_margin'])
            print(f'    {r["seat_label"]:25s} {margin:10s} ({holder})')

    if r_missed:
        print('  No GOP filed in competitive/favorable districts:')
        for r in r_missed:
            holder = r['current_holder'] or 'open seat'
            if r['is_open_seat']:
                holder = 'open seat'
            margin = format_margin(r['pres_2024_margin'])
            print(f'    {r["seat_label"]:25s} {margin:10s} ({holder})')


def print_detail(detail_rows):
    """Print the per-election-type uncontested detail."""
    if not detail_rows:
        return

    # Group by election_type
    by_type = {}
    for r in detail_rows:
        by_type.setdefault(r['election_type'], []).append(r)

    for etype in sorted(by_type.keys()):
        rows = by_type[etype]
        print()
        print(f'DETAIL: {etype} uncontested ({len(rows)} races)')
        for r in rows:
            margin = format_margin(r['pres_2024_margin'])
            if r['active_count'] == 0:
                candidate_str = '(no candidate filed)'
            else:
                candidate_str = r['candidate_names'] or '(unknown)'
                candidate_str += ' (unopposed)'
            print(f'    {r["seat_label"]:25s} {margin:10s} {candidate_str}')


def analyze_state(state_abbr, threshold, year=2026, summary_only=False,
                  output_json=False, dry_run=False):
    """Run full analysis for a single state."""
    # For 2026, show filing date if available
    filing_date = CLOSED_FILING_STATES.get(state_abbr) if year == 2026 else None

    q_summary = query_summary(state_abbr, year)
    q_detail = query_uncontested_detail(state_abbr, year)
    q_missed = query_missed_opportunities(state_abbr, threshold, year)

    if dry_run:
        print(f'\n--- {state_abbr} {year}: Summary query ---')
        print(q_summary.strip())
        if not summary_only:
            print(f'\n--- {state_abbr} {year}: Detail query ---')
            print(q_detail.strip())
            print(f'\n--- {state_abbr} {year}: Missed opportunities query ---')
            print(q_missed.strip())
        return None

    # Run queries
    summary_rows = run_sql(q_summary) or []
    if not summary_rows:
        print(f'  {state_abbr}: No candidacy data for {year} — skipping')
        return None

    detail_rows = [] if summary_only else (run_sql(q_detail) or [])
    missed_rows = [] if summary_only else (run_sql(q_missed) or [])

    if output_json:
        result = {
            'state': state_abbr,
            'year': year,
            'margin_threshold': threshold,
            'summary': summary_rows,
            'uncontested_detail': detail_rows,
            'missed_opportunities': missed_rows,
        }
        if filing_date:
            result['filing_deadline'] = filing_date
        return result

    # Console output
    print_summary(summary_rows, state_abbr, year, filing_date)
    if not summary_only:
        print_missed_opportunities(missed_rows, threshold)
        print_detail(detail_rows)

    print()
    return {
        'state': state_abbr,
        'year': year,
        'summary': summary_rows,
        'missed_count': len(missed_rows),
        'uncontested_count': len(detail_rows),
    }


def print_multi_state_summary(results, year):
    """Print a cross-state summary table."""
    print()
    w = 80
    print(f'{"":═<{w}}')
    print(f'  UNCONTESTED ELECTIONS — {year} — {len(results)} STATES WITH DATA')
    print(f'{"":═<{w}}')
    print()

    # Check if we have general election data
    has_generals = any(
        any(s['election_type'] == 'General' for s in r.get('summary', []))
        for r in results if r
    )

    if has_generals:
        print(f'  {"State":6s} {"D Unc":>8s} {"D Tot":>6s} {"D%":>5s}  '
              f'{"R Unc":>8s} {"R Tot":>6s} {"R%":>5s}  '
              f'{"G Unc":>8s} {"G Tot":>6s} {"G%":>5s}')
        print(f'  {"─"*6:6s} {"─"*8:>8s} {"─"*6:>6s} {"─"*5:>5s}  '
              f'{"─"*8:>8s} {"─"*6:>6s} {"─"*5:>5s}  '
              f'{"─"*8:>8s} {"─"*6:>6s} {"─"*5:>5s}')
    else:
        print(f'  {"State":6s} {"D Unc":>8s} {"D Total":>8s} {"D%":>5s}   '
              f'{"R Unc":>8s} {"R Total":>8s} {"R%":>5s}')
        print(f'  {"─"*6:6s} {"─"*8:>8s} {"─"*8:>8s} {"─"*5:>5s}   '
              f'{"─"*8:>8s} {"─"*8:>8s} {"─"*5:>5s}')

    totals = {'d_unc': 0, 'd_total': 0, 'r_unc': 0, 'r_total': 0, 'g_unc': 0, 'g_total': 0}
    for r in results:
        if not r or not r.get('summary'):
            continue
        d_unc = d_total = r_unc = r_total = g_unc = g_total = 0
        for s in r['summary']:
            if s['election_type'] == 'Primary_D':
                d_unc += s['uncontested']
                d_total += s['total_with_data']
            elif s['election_type'] == 'Primary_R':
                r_unc += s['uncontested']
                r_total += s['total_with_data']
            elif s['election_type'] == 'General':
                g_unc += s['uncontested']
                g_total += s['total_with_data']

        d_pct = (d_unc / d_total * 100) if d_total > 0 else 0
        r_pct = (r_unc / r_total * 100) if r_total > 0 else 0

        if has_generals:
            g_pct = (g_unc / g_total * 100) if g_total > 0 else 0
            print(f'  {r["state"]:6s} {d_unc:8d} {d_total:6d} {d_pct:4.0f}%  '
                  f'{r_unc:8d} {r_total:6d} {r_pct:4.0f}%  '
                  f'{g_unc:8d} {g_total:6d} {g_pct:4.0f}%')
        else:
            print(f'  {r["state"]:6s} {d_unc:8d} {d_total:8d} {d_pct:4.0f}%   '
                  f'{r_unc:8d} {r_total:8d} {r_pct:4.0f}%')

        totals['d_unc'] += d_unc
        totals['d_total'] += d_total
        totals['r_unc'] += r_unc
        totals['r_total'] += r_total
        totals['g_unc'] += g_unc
        totals['g_total'] += g_total

    d_pct = (totals['d_unc'] / totals['d_total'] * 100) if totals['d_total'] > 0 else 0
    r_pct = (totals['r_unc'] / totals['r_total'] * 100) if totals['r_total'] > 0 else 0

    if has_generals:
        g_pct = (totals['g_unc'] / totals['g_total'] * 100) if totals['g_total'] > 0 else 0
        print(f'  {"─"*6:6s} {"─"*8:>8s} {"─"*6:>6s} {"─"*5:>5s}  '
              f'{"─"*8:>8s} {"─"*6:>6s} {"─"*5:>5s}  '
              f'{"─"*8:>8s} {"─"*6:>6s} {"─"*5:>5s}')
        print(f'  {"TOTAL":6s} {totals["d_unc"]:8d} {totals["d_total"]:6d} {d_pct:4.0f}%  '
              f'{totals["r_unc"]:8d} {totals["r_total"]:6d} {r_pct:4.0f}%  '
              f'{totals["g_unc"]:8d} {totals["g_total"]:6d} {g_pct:4.0f}%')
    else:
        print(f'  {"─"*6:6s} {"─"*8:>8s} {"─"*8:>8s} {"─"*5:>5s}   '
              f'{"─"*8:>8s} {"─"*8:>8s} {"─"*5:>5s}')
        print(f'  {"TOTAL":6s} {totals["d_unc"]:8d} {totals["d_total"]:8d} {d_pct:4.0f}%   '
              f'{totals["r_unc"]:8d} {totals["r_total"]:8d} {r_pct:4.0f}%')
    print()


def main():
    parser = argparse.ArgumentParser(description='Analyze uncontested elections')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--state', type=str, help='Single state (2-letter abbreviation)')
    group.add_argument('--all-closed', action='store_true',
                       help='All states with closed filing deadlines (2026 only)')
    group.add_argument('--all-states', action='store_true',
                       help='All 50 states (useful for past years with complete data)')
    parser.add_argument('--year', type=int, default=DEFAULT_YEAR,
                        help=f'Election year to analyze (default: {DEFAULT_YEAR})')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--summary-only', action='store_true',
                        help='Summary counts only (no per-race detail)')
    parser.add_argument('--dry-run', action='store_true', help='Print queries without executing')
    parser.add_argument('--margin-threshold', type=float, default=DEFAULT_MARGIN_THRESHOLD,
                        help=f'Margin threshold for missed opportunities (default: {DEFAULT_MARGIN_THRESHOLD})')
    args = parser.parse_args()

    threshold = args.margin_threshold
    year = args.year

    if args.all_closed and year != 2026:
        print(f'Warning: --all-closed only tracks 2026 filing deadlines. '
              f'Use --all-states for year {year}.')
        return

    if args.state:
        state = args.state.upper()
        if year == 2026 and state not in CLOSED_FILING_STATES:
            print(f'Note: {state} not in CLOSED_FILING_STATES for 2026 — '
                  f'may have incomplete candidacy data')
        result = analyze_state(state, threshold, year=year,
                               summary_only=args.summary_only,
                               output_json=args.json,
                               dry_run=args.dry_run)
        if args.json and result:
            print(json.dumps(result, indent=2, default=str))

    elif args.all_closed:
        # 2026 only — use CLOSED_FILING_STATES
        if args.dry_run:
            print('DRY RUN — queries that would be executed:\n')

        results = []
        states_sorted = sorted(CLOSED_FILING_STATES.keys())
        for i, state in enumerate(states_sorted, 1):
            if not args.dry_run:
                print(f'  [{i}/{len(states_sorted)}] Analyzing {state}...')
            result = analyze_state(state, threshold, year=year,
                                   summary_only=args.summary_only,
                                   output_json=args.json,
                                   dry_run=args.dry_run)
            if result:
                results.append(result)

        if args.dry_run:
            return

        if args.json:
            print(json.dumps(results, indent=2, default=str))
        elif args.summary_only:
            print_multi_state_summary(results, year)

    else:
        # --all-states
        if args.dry_run:
            print('DRY RUN — queries that would be executed:\n')

        results = []
        for i, state in enumerate(ALL_STATES, 1):
            if not args.dry_run:
                print(f'  [{i}/{len(ALL_STATES)}] Analyzing {state}...')
            result = analyze_state(state, threshold, year=year,
                                   summary_only=args.summary_only,
                                   output_json=args.json,
                                   dry_run=args.dry_run)
            if result:
                results.append(result)

        if args.dry_run:
            return

        if args.json:
            print(json.dumps(results, indent=2, default=str))
        elif args.summary_only:
            print_multi_state_summary(results, year)


if __name__ == '__main__':
    main()
