"""
Election Briefing & Reminder System.

Queries the database and generates a tiered election briefing with:
  - Database health snapshot
  - Upcoming elections (7/14/30-day tiers)
  - Filing deadline tracking
  - Special elections
  - Monitoring task status
  - Prioritized action items

Also writes an auto-updated section to the Obsidian vault project note.

Usage:
    python3 scripts/election_briefing.py                   # Full briefing + vault write
    python3 scripts/election_briefing.py --console-only    # Console only, no vault write
    python3 scripts/election_briefing.py --dry-run         # Print queries, don't execute
    python3 scripts/election_briefing.py --mark-done seat_gap_audit   # Update check timestamp
    python3 scripts/election_briefing.py --json            # Output as JSON
"""
import sys
import os
import json
import time
import argparse
from datetime import datetime, date, timedelta

import httpx
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

# Import local data
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', 'data'))
from primary_dates_2026 import PRIMARY_DATES, RUNOFF_DATES, GENERAL_ELECTION_DATE

# Import STATE_DEADLINES from populate_filing_deadlines (same scripts/ directory)
from populate_filing_deadlines import STATE_DEADLINES

MONITORING_STATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'monitoring_state.json'
)
VAULT_NOTE_PATH = os.path.expanduser(
    '~/second-brain/02-Projects/Work/Elections/Elections Database.md'
)

TODAY = date.today()


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


def load_monitoring_state():
    if os.path.exists(MONITORING_STATE_PATH):
        with open(MONITORING_STATE_PATH) as f:
            return json.load(f)
    return {
        'format_version': 1,
        'last_briefing': None,
        'checks': {},
        'candidacy_scrape_status': {},
    }


def save_monitoring_state(state):
    os.makedirs(os.path.dirname(MONITORING_STATE_PATH), exist_ok=True)
    with open(MONITORING_STATE_PATH, 'w') as f:
        json.dump(state, f, indent=2)
        f.write('\n')


def days_until(date_str):
    """Return days from today to a date string (YYYY-MM-DD). Negative = past."""
    if not date_str:
        return None
    try:
        d = date.fromisoformat(str(date_str)[:10])
        return (d - TODAY).days
    except (ValueError, TypeError):
        return None


def format_date_short(date_str):
    """Format date string as 'Mon DD' (e.g., 'Mar 3')."""
    if not date_str:
        return '???'
    try:
        d = date.fromisoformat(str(date_str)[:10])
        return d.strftime('%b %-d')
    except (ValueError, TypeError):
        return str(date_str)[:10]


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

Q_UPCOMING_ELECTIONS = """
    SELECT e.election_date::text as election_date,
           e.election_type,
           st.abbreviation as state,
           s.office_level,
           d.chamber,
           COUNT(*) as election_count
    FROM elections e
    JOIN seats s ON e.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE e.election_date >= CURRENT_DATE
      AND e.election_date <= CURRENT_DATE + INTERVAL '30 days'
    GROUP BY e.election_date, e.election_type, st.abbreviation, s.office_level, d.chamber
    ORDER BY e.election_date, st.abbreviation, s.office_level, d.chamber
"""

Q_HEALTH = """
    SELECT
      (SELECT COUNT(*) FROM elections WHERE election_year = 2026
         AND election_type NOT LIKE 'Special%%') as regular_elections_2026,
      (SELECT COUNT(*) FROM elections WHERE election_type LIKE 'Special%%') as special_elections,
      (SELECT COUNT(*) FROM candidacies) as candidacies,
      (SELECT COUNT(*) FROM seats WHERE current_holder IS NULL
         AND office_level = 'Legislative') as vacancies,
      (SELECT COUNT(*) FROM ballot_measures) as ballot_measures_total,
      (SELECT COUNT(*) FROM ballot_measures WHERE election_year = 2026) as ballot_measures_2026,
      (SELECT COUNT(*) FROM forecasts) as forecasts,
      (SELECT COUNT(*) FROM candidates) as candidates,
      (SELECT COUNT(*) FROM seat_terms) as seat_terms
"""

Q_SEAT_CHANGES = """
    SELECT st.abbreviation as state,
           s.seat_label,
           s.office_level,
           d.chamber,
           c.full_name as holder_name,
           COALESCE(stm.caucus, stm.party) as party,
           stm.start_date::text as start_date,
           stm.start_reason,
           stm.end_date::text as end_date,
           stm.end_reason
    FROM seat_terms stm
    JOIN seats s ON stm.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    LEFT JOIN candidates c ON stm.candidate_id = c.id
    WHERE (stm.start_date >= CURRENT_DATE - INTERVAL '30 days'
           AND stm.start_reason NOT IN ('elected'))
       OR (stm.end_date >= CURRENT_DATE - INTERVAL '30 days'
           AND stm.end_reason IN ('resigned','died','removed','expelled',
                                   'recalled','appointed_to_other_office'))
    ORDER BY COALESCE(stm.end_date, stm.start_date) DESC, st.abbreviation
"""

Q_SPECIALS = """
    SELECT e.election_date::text as election_date,
           e.election_type,
           st.abbreviation as state,
           s.seat_label,
           d.chamber,
           s.office_level,
           e.result_status
    FROM elections e
    JOIN seats s ON e.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE e.election_type LIKE 'Special%%'
      AND e.election_date IS NOT NULL
      AND e.election_date >= CURRENT_DATE - INTERVAL '7 days'
      AND e.election_date <= CURRENT_DATE + INTERVAL '90 days'
    ORDER BY e.election_date, st.abbreviation, s.seat_label
"""

Q_CANDIDACY_COUNTS = """
    SELECT st.abbreviation as state, COUNT(DISTINCT cy.id) as candidacy_count
    FROM candidacies cy
    JOIN elections e ON cy.election_id = e.id
    JOIN seats s ON e.seat_id = s.id
    JOIN districts d ON s.district_id = d.id
    JOIN states st ON d.state_id = st.id
    WHERE e.election_year = 2026
    GROUP BY st.abbreviation
    ORDER BY st.abbreviation
"""

Q_UNCONTESTED_SUMMARY = """
    WITH primary_counts AS (
        SELECT e.id, st.abbreviation as state, e.election_type,
            COUNT(cy.id) FILTER (
                WHERE cy.candidate_status NOT IN ('Withdrawn_Pre_Ballot','Withdrawn_Post_Ballot','Disqualified')
                  AND cy.is_write_in = FALSE
            ) as active_count
        FROM elections e
        JOIN seats s ON e.seat_id = s.id
        JOIN districts d ON s.district_id = d.id
        JOIN states st ON d.state_id = st.id
        LEFT JOIN candidacies cy ON cy.election_id = e.id
        WHERE e.election_year = 2026
          AND e.election_type IN ('Primary_D','Primary_R')
          AND s.office_level = 'Legislative'
        GROUP BY e.id, st.abbreviation, e.election_type
        HAVING COUNT(cy.id) > 0
    )
    SELECT state, election_type,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE active_count <= 1) as uncontested,
        COUNT(*) FILTER (WHERE active_count = 0) as no_candidates
    FROM primary_counts
    GROUP BY state, election_type
    ORDER BY state, election_type
"""


# ---------------------------------------------------------------------------
# Build briefing data
# ---------------------------------------------------------------------------

def get_filing_deadlines_local():
    """Compute filing deadline status from local STATE_DEADLINES data."""
    upcoming = []  # deadline in next 30 days
    recently_closed = []  # deadline in last 30 days

    for abbr, entry in sorted(STATE_DEADLINES.items()):
        if isinstance(entry, dict):
            deadlines = [
                (entry.get('statewide'), 'Statewide'),
                (entry.get('legislative'), 'Legislative'),
            ]
        else:
            deadlines = [(entry, 'All')]

        for deadline_str, level in deadlines:
            if not deadline_str:
                continue
            days = days_until(deadline_str)
            if days is None:
                continue

            item = {
                'state': abbr,
                'deadline': deadline_str,
                'level': level,
                'days_away': days,
            }

            if 0 <= days <= 30:
                upcoming.append(item)
            elif -30 <= days < 0:
                recently_closed.append(item)

    upcoming.sort(key=lambda x: x['deadline'])
    recently_closed.sort(key=lambda x: x['deadline'])
    return upcoming, recently_closed


def build_briefing(dry_run=False):
    """Run queries and build the full briefing data structure."""

    if dry_run:
        print('DRY RUN — queries that would be executed:\n')
        for name, q in [
            ('Upcoming elections', Q_UPCOMING_ELECTIONS),
            ('Database health', Q_HEALTH),
            ('Seat changes', Q_SEAT_CHANGES),
            ('Special elections', Q_SPECIALS),
            ('Candidacy counts', Q_CANDIDACY_COUNTS),
            ('Uncontested summary', Q_UNCONTESTED_SUMMARY),
        ]:
            print(f'--- {name} ---')
            print(q.strip())
            print()
        print('(Plus local filing deadline computation from STATE_DEADLINES)')
        return None

    print('Running 6 database queries...')
    upcoming_raw = run_sql(Q_UPCOMING_ELECTIONS)
    print('  1/6 upcoming elections')
    health_raw = run_sql(Q_HEALTH)
    print('  2/6 health snapshot')
    changes_raw = run_sql(Q_SEAT_CHANGES)
    print('  3/6 seat changes')
    specials_raw = run_sql(Q_SPECIALS)
    print('  4/6 special elections')
    candidacy_counts_raw = run_sql(Q_CANDIDACY_COUNTS)
    print('  5/6 candidacy counts')
    uncontested_raw = run_sql(Q_UNCONTESTED_SUMMARY)
    print('  6/6 uncontested summary')

    health = health_raw[0] if health_raw else {}
    candidacy_by_state = {r['state']: r['candidacy_count'] for r in (candidacy_counts_raw or [])}

    # Process uncontested data: {state: {d_total, d_uncontested, r_total, r_uncontested}}
    uncontested_by_state = {}
    for r in (uncontested_raw or []):
        st = r['state']
        if st not in uncontested_by_state:
            uncontested_by_state[st] = {'d_total': 0, 'd_uncontested': 0, 'r_total': 0, 'r_uncontested': 0}
        if r['election_type'] == 'Primary_D':
            uncontested_by_state[st]['d_total'] += r['total']
            uncontested_by_state[st]['d_uncontested'] += r['uncontested']
        elif r['election_type'] == 'Primary_R':
            uncontested_by_state[st]['r_total'] += r['total']
            uncontested_by_state[st]['r_uncontested'] += r['uncontested']

    # --- Upcoming elections: group by tier ---
    elections_7d = []
    elections_14d = []
    elections_30d = []

    # Group by date + state for summary
    by_date = {}
    for r in (upcoming_raw or []):
        d = r['election_date']
        days = days_until(d)
        if days is None:
            continue

        key = (d, r['state'])
        if key not in by_date:
            by_date[key] = {
                'date': d,
                'state': r['state'],
                'types': {},
                'total': 0,
                'days_away': days,
            }
        etype = r['election_type']
        chamber = r['chamber'] or r['office_level']
        sub_key = f"{etype}|{chamber}"
        by_date[key]['types'][sub_key] = by_date[key]['types'].get(sub_key, 0) + r['election_count']
        by_date[key]['total'] += r['election_count']

    for key, item in sorted(by_date.items()):
        days = item['days_away']
        if days <= 7:
            elections_7d.append(item)
        elif days <= 14:
            elections_14d.append(item)
        else:
            elections_30d.append(item)

    # --- Primary dates from local data ---
    primary_calendar = []
    for abbr, pdate in sorted(PRIMARY_DATES.items(), key=lambda x: x[1]):
        days = days_until(pdate)
        if days is not None and 0 <= days <= 90:
            runoff = RUNOFF_DATES.get(abbr)
            primary_calendar.append({
                'state': abbr,
                'date': pdate,
                'days_away': days,
                'runoff_date': runoff,
            })

    # --- Filing deadlines (local computation) ---
    upcoming_deadlines, recently_closed_deadlines = get_filing_deadlines_local()

    # --- Monitoring state ---
    mon_state = load_monitoring_state()

    # --- Cross-reference scraping status ---
    scrape_status = mon_state.get('candidacy_scrape_status', {})
    scrape_needed = []
    for item in recently_closed_deadlines:
        abbr = item['state']
        status = scrape_status.get(abbr, {})
        if not status.get('scraped', False):
            item['candidacies'] = candidacy_by_state.get(abbr, 0)
            scrape_needed.append(item)

    # Also check deadlines older than 30 days that still need scraping
    for abbr, entry in sorted(STATE_DEADLINES.items()):
        if isinstance(entry, dict):
            # Use earliest deadline
            deadline_str = min(entry.values())
        else:
            deadline_str = entry
        days = days_until(deadline_str)
        if days is not None and days < -30:
            status = scrape_status.get(abbr, {})
            if not status.get('scraped', False):
                scrape_needed.append({
                    'state': abbr,
                    'deadline': deadline_str,
                    'level': 'All',
                    'days_away': days,
                    'candidacies': candidacy_by_state.get(abbr, 0),
                })

    # --- Build action items ---
    actions = build_action_items(
        elections_7d, elections_14d, elections_30d,
        upcoming_deadlines, scrape_needed,
        primary_calendar, specials_raw or [],
    )

    return {
        'date': TODAY.isoformat(),
        'health': health,
        'elections_7d': elections_7d,
        'elections_14d': elections_14d,
        'elections_30d': elections_30d,
        'primary_calendar': primary_calendar,
        'upcoming_deadlines': upcoming_deadlines,
        'recently_closed_deadlines': recently_closed_deadlines,
        'scrape_needed': scrape_needed,
        'seat_changes': changes_raw or [],
        'specials': specials_raw or [],
        'monitoring': mon_state,
        'candidacy_by_state': candidacy_by_state,
        'uncontested_by_state': uncontested_by_state,
        'actions': actions,
    }


def build_action_items(e7d, e14d, e30d, upcoming_dl, scrape_needed, primaries, specials):
    """Generate prioritized action items."""
    actions = []

    # URGENT: elections within 3 days
    for item in e7d:
        if item['days_away'] <= 3:
            actions.append({
                'priority': 'URGENT',
                'text': f"{item['state']} elections on {format_date_short(item['date'])} ({item['total']} races)",
            })

    # URGENT: filing deadlines within 3 days
    for item in upcoming_dl:
        if item['days_away'] <= 3:
            actions.append({
                'priority': 'URGENT',
                'text': f"{item['state']} filing closes {format_date_short(item['deadline'])} — prepare candidacy scraper",
            })

    # SOON: elections within 4-7 days
    for item in e7d:
        if 4 <= item['days_away'] <= 7:
            actions.append({
                'priority': 'SOON',
                'text': f"{item['state']} elections on {format_date_short(item['date'])} ({item['total']} races)",
            })

    # SOON: filing deadlines 4-7 days
    for item in upcoming_dl:
        if 4 <= item['days_away'] <= 7:
            actions.append({
                'priority': 'SOON',
                'text': f"{item['state']} filing closes {format_date_short(item['deadline'])}",
            })

    # SOON: primaries within 14 days
    for item in primaries:
        if item['days_away'] <= 14:
            actions.append({
                'priority': 'SOON',
                'text': f"{item['state']} primary {format_date_short(item['date'])} — research SoS results pages",
            })

    # PLAN: scraping needed
    for item in scrape_needed:
        actions.append({
            'priority': 'PLAN',
            'text': f"Scrape {item['state']} candidacies (filing closed {format_date_short(item['deadline'])})",
        })

    # PLAN: filing deadlines 8-30 days
    for item in upcoming_dl:
        if 8 <= item['days_away'] <= 30:
            actions.append({
                'priority': 'PLAN',
                'text': f"{item['state']} filing closes {format_date_short(item['deadline'])} ({item['days_away']} days)",
            })

    # PLAN: primaries 15-30 days
    for item in primaries:
        if 15 <= item['days_away'] <= 30:
            actions.append({
                'priority': 'PLAN',
                'text': f"{item['state']} primary {format_date_short(item['date'])} — prep results pipeline",
            })

    # Deduplicate by text
    seen = set()
    unique = []
    for a in actions:
        if a['text'] not in seen:
            seen.add(a['text'])
            unique.append(a)

    return unique


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def print_briefing(data):
    """Print formatted briefing to console."""
    w = 60
    print()
    print(f'{"":═<{w}}')
    print(f'  ELECTIONS BRIEFING — {data["date"]}')
    print(f'{"":═<{w}}')

    # Database Health
    h = data['health']
    print()
    print('DATABASE HEALTH')
    print(f'  Elections (2026): {h.get("regular_elections_2026", "?")} regular + {h.get("special_elections", "?")} specials')
    print(f'  Candidacies:      {h.get("candidacies", "?"):,}')
    print(f'  Candidates:       {h.get("candidates", "?"):,}')
    print(f'  Seat terms:       {h.get("seat_terms", "?"):,}')
    print(f'  Vacancies:        {h.get("vacancies", "?")}')
    print(f'  Ballot measures:  {h.get("ballot_measures_total", "?")} ({h.get("ballot_measures_2026", "?")} for 2026)')
    print(f'  Forecasts:        {h.get("forecasts", "?")}')

    # Next 7 days — prefer specials detail over aggregated counts
    print()
    print('NEXT 7 DAYS')
    upcoming_specials = [s for s in data['specials'] if 0 <= (days_until(s['election_date']) or 99) <= 7]
    specials_shown = set()
    if upcoming_specials:
        for s in upcoming_specials:
            status = f' [{s["result_status"]}]' if s.get('result_status') else ''
            print(f'  {format_date_short(s["election_date"])} — {s["state"]} {s["seat_label"]} ({s["election_type"]}){status}')
            specials_shown.add((s['election_date'], s['state']))
    # Show any non-special elections in 7-day window
    for item in data['elections_7d']:
        if (item['date'], item['state']) not in specials_shown:
            print(f'  {format_date_short(item["date"])} — {item["state"]}: {item["total"]} elections')
    if not data['elections_7d'] and not upcoming_specials:
        print('  (no elections)')

    # Next 8-14 days
    print()
    print('NEXT 8-14 DAYS')
    if data['elections_14d']:
        # Group by date
        by_date = {}
        for item in data['elections_14d']:
            by_date.setdefault(item['date'], []).append(item)

        for d, items in sorted(by_date.items()):
            states = [f'{it["state"]}: ~{it["total"]:,}' for it in items]
            # Check if this is a primary date
            primary_states = [it['state'] for it in items
                              if any('Primary' in t for t in it['types'])]
            if primary_states:
                print(f'  {format_date_short(d)} — PRIMARY DAY ({len(primary_states)} state{"s" if len(primary_states) != 1 else ""})')
                for st_summary in states:
                    print(f'    {st_summary}')
            else:
                for st_summary in states:
                    print(f'  {format_date_short(d)} — {st_summary}')
    else:
        print('  (no elections)')

    # Next 15-30 days
    print()
    print('NEXT 15-30 DAYS')
    if data['elections_30d']:
        by_date = {}
        for item in data['elections_30d']:
            by_date.setdefault(item['date'], []).append(item)

        for d, items in sorted(by_date.items()):
            states_str = ', '.join(sorted(set(it['state'] for it in items)))
            total = sum(it['total'] for it in items)
            print(f'  {format_date_short(d)} — {states_str} ({total:,} elections)')
    else:
        print('  (no elections)')

    # Primary calendar (next 90 days)
    upcoming_primaries = [p for p in data['primary_calendar'] if p['days_away'] <= 60]
    if upcoming_primaries:
        print()
        print('PRIMARY CALENDAR (next 60 days)')
        for p in upcoming_primaries:
            runoff = f' (runoff {format_date_short(p["runoff_date"])})' if p.get('runoff_date') else ''
            print(f'  {format_date_short(p["date"])} — {p["state"]}{runoff}  [{p["days_away"]}d]')

    # Filing deadlines
    print()
    print('FILING DEADLINES')
    if data['upcoming_deadlines']:
        print('  Closing soon:')
        for item in data['upcoming_deadlines']:
            level = f' ({item["level"]})' if item['level'] != 'All' else ''
            print(f'    {item["state"]}{level} — {format_date_short(item["deadline"])} ({item["days_away"]} days)')
    else:
        print('  (no deadlines in next 30 days)')

    if data['scrape_needed']:
        print('  Closed — scrape needed:')
        for item in data['scrape_needed']:
            cands = f' ({item.get("candidacies", 0)} candidacies loaded)' if item.get('candidacies') else ''
            print(f'    {item["state"]} (closed {format_date_short(item["deadline"])}){cands}')

    # Seat changes
    if data['seat_changes']:
        print()
        print('RECENT SEAT CHANGES (last 30 days)')
        for ch in data['seat_changes'][:10]:
            if ch.get('end_reason'):
                print(f'  {ch["state"]} {ch["seat_label"]}: {ch["holder_name"]} ({ch["party"]}) — {ch["end_reason"]} {format_date_short(ch["end_date"])}')
            elif ch.get('start_reason'):
                print(f'  {ch["state"]} {ch["seat_label"]}: {ch["holder_name"]} ({ch["party"]}) — {ch["start_reason"]} {format_date_short(ch["start_date"])}')
        if len(data['seat_changes']) > 10:
            print(f'  ... and {len(data["seat_changes"]) - 10} more')

    # Special elections
    future_specials = [s for s in data['specials'] if (days_until(s['election_date']) or -1) > 7]
    if future_specials:
        print()
        print('UPCOMING SPECIAL ELECTIONS')
        for s in future_specials[:15]:
            days = days_until(s['election_date'])
            status = f' [{s["result_status"]}]' if s.get('result_status') else ''
            print(f'  {format_date_short(s["election_date"])} — {s["state"]} {s["seat_label"]} ({s["election_type"]}){status}  [{days}d]')
        if len(future_specials) > 15:
            print(f'  ... and {len(future_specials) - 15} more')

    # Monitoring status
    print()
    print('MONITORING')
    mon = data['monitoring']
    for name, check in sorted(mon.get('checks', {}).items()):
        last_run = check.get('last_run', 'never')
        interval = check.get('interval_days')
        if last_run and last_run != 'never':
            days_ago = (TODAY - date.fromisoformat(last_run)).days
            if interval and days_ago > interval:
                marker = '!'
            else:
                marker = '✓'
            interval_str = f', due every {interval}' if interval else ', manual'
            print(f'  {marker} {name:30s} ({days_ago} days ago{interval_str})')
        else:
            print(f'  ? {name:30s} (never run)')

    # Uncontested primaries
    unc = data.get('uncontested_by_state', {})
    if unc:
        print()
        print('UNCONTESTED PRIMARIES')
        # Aggregate totals
        agg_d_total = sum(v['d_total'] for v in unc.values())
        agg_d_unc = sum(v['d_uncontested'] for v in unc.values())
        agg_r_total = sum(v['r_total'] for v in unc.values())
        agg_r_unc = sum(v['r_uncontested'] for v in unc.values())
        d_pct = (agg_d_unc / agg_d_total * 100) if agg_d_total > 0 else 0
        r_pct = (agg_r_unc / agg_r_total * 100) if agg_r_total > 0 else 0
        print(f'  Across {len(unc)} states with data:')
        print(f'    Dem primaries: {agg_d_unc}/{agg_d_total} ({d_pct:.0f}%) uncontested')
        print(f'    GOP primaries: {agg_r_unc}/{agg_r_total} ({r_pct:.0f}%) uncontested')

        # Top 5 states by uncontested rate
        state_rates = []
        for st, v in unc.items():
            total = v['d_total'] + v['r_total']
            unc_count = v['d_uncontested'] + v['r_uncontested']
            if total > 0:
                state_rates.append((st, unc_count, total, unc_count / total * 100))
        state_rates.sort(key=lambda x: -x[3])
        if state_rates:
            print('  Highest uncontested rates:')
            for st, unc_count, total, pct in state_rates[:5]:
                print(f'    {st}: {unc_count}/{total} ({pct:.0f}%)')

    # Action items
    print()
    print('ACTION ITEMS')
    if data['actions']:
        for a in data['actions']:
            print(f'  {a["priority"]:6s}: {a["text"]}')
    else:
        print('  (none)')

    print()


# ---------------------------------------------------------------------------
# Vault markdown output
# ---------------------------------------------------------------------------

def build_vault_markdown(data):
    """Build the auto-updated markdown section for the Obsidian note."""
    lines = []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    lines.append('## Current Status')
    lines.append(f'> Auto-updated by `election_briefing.py` on {now_str}')
    lines.append('')

    # Database Health
    h = data['health']
    lines.append('### Database Health')
    lines.append('| Metric | Count |')
    lines.append('|--------|------:|')
    lines.append(f'| Elections (2026 regular) | {h.get("regular_elections_2026", "?"):,} |')
    lines.append(f'| Special elections | {h.get("special_elections", "?"):,} |')
    lines.append(f'| Candidacies | {h.get("candidacies", "?"):,} |')
    lines.append(f'| Candidates | {h.get("candidates", "?"):,} |')
    lines.append(f'| Vacancies | {h.get("vacancies", "?")} |')
    lines.append(f'| Ballot measures (total/2026) | {h.get("ballot_measures_total", "?")}/{h.get("ballot_measures_2026", "?")} |')
    lines.append(f'| Forecasts | {h.get("forecasts", "?")} |')
    lines.append('')

    # Election Calendar
    lines.append('### Election Calendar')

    # Next 7 days — prefer specials detail
    lines.append('**Next 7 days:**')
    upcoming_specials = [s for s in data['specials'] if 0 <= (days_until(s['election_date']) or 99) <= 7]
    specials_shown = set()
    if upcoming_specials:
        for s in upcoming_specials:
            status = f' [{s["result_status"]}]' if s.get('result_status') else ''
            lines.append(f'- {format_date_short(s["election_date"])} — {s["state"]} {s["seat_label"]} ({s["election_type"]}){status}')
            specials_shown.add((s['election_date'], s['state']))
    for item in data['elections_7d']:
        if (item['date'], item['state']) not in specials_shown:
            lines.append(f'- {format_date_short(item["date"])} — {item["state"]}: {item["total"]} elections')
    if not data['elections_7d'] and not upcoming_specials:
        lines.append('- (none)')
    lines.append('')

    # Next 8-14 days
    lines.append('**Next 8-14 days:**')
    if data['elections_14d']:
        by_date = {}
        for item in data['elections_14d']:
            by_date.setdefault(item['date'], []).append(item)
        for d, items in sorted(by_date.items()):
            primary_states = [it['state'] for it in items if any('Primary' in t for t in it['types'])]
            if primary_states:
                state_summary = ', '.join(f'{it["state"]}: ~{it["total"]:,}' for it in items)
                lines.append(f'- **{format_date_short(d)} — PRIMARY DAY** ({state_summary})')
            else:
                for it in items:
                    lines.append(f'- {format_date_short(d)} — {it["state"]}: {it["total"]:,} elections')
    else:
        lines.append('- (none)')
    lines.append('')

    # Next 15-30 days
    lines.append('**Next 15-30 days:**')
    if data['elections_30d']:
        by_date = {}
        for item in data['elections_30d']:
            by_date.setdefault(item['date'], []).append(item)
        for d, items in sorted(by_date.items()):
            states_str = ', '.join(sorted(set(it['state'] for it in items)))
            total = sum(it['total'] for it in items)
            lines.append(f'- {format_date_short(d)} — {states_str} ({total:,} elections)')
    else:
        lines.append('- (none)')
    lines.append('')

    # Filing Deadlines
    lines.append('### Filing Deadlines')
    if data['upcoming_deadlines']:
        lines.append('| State | Deadline | Days | Level |')
        lines.append('|-------|----------|-----:|-------|')
        for item in data['upcoming_deadlines']:
            lines.append(f'| {item["state"]} | {format_date_short(item["deadline"])} | {item["days_away"]} | {item["level"]} |')
    else:
        lines.append('No deadlines closing in next 30 days.')
    lines.append('')

    if data['scrape_needed']:
        lines.append('**Scraping needed** (filing closed, candidacies not yet collected):')
        for item in data['scrape_needed']:
            lines.append(f'- [ ] {item["state"]} — closed {format_date_short(item["deadline"])}')
    lines.append('')

    # Special Elections
    future_specials = [s for s in data['specials'] if (days_until(s['election_date']) or -1) >= 0]
    if future_specials:
        lines.append('### Special Elections')
        lines.append('| Date | State | Seat | Type | Status |')
        lines.append('|------|-------|------|------|--------|')
        for s in future_specials[:20]:
            lines.append(f'| {format_date_short(s["election_date"])} | {s["state"]} | {s["seat_label"]} | {s["election_type"]} | {s.get("result_status", "")} |')
        if len(future_specials) > 20:
            lines.append(f'*...and {len(future_specials) - 20} more*')
        lines.append('')

    # Monitoring
    lines.append('### Monitoring Status')
    lines.append('| Check | Last Run | Interval | Status |')
    lines.append('|-------|----------|----------|--------|')
    mon = data['monitoring']
    for name, check in sorted(mon.get('checks', {}).items()):
        last_run = check.get('last_run', 'never')
        interval = check.get('interval_days')
        if last_run and last_run != 'never':
            days_ago = (TODAY - date.fromisoformat(last_run)).days
            if interval and days_ago > interval:
                status = 'OVERDUE'
            else:
                status = 'OK'
            interval_str = f'{interval}d' if interval else 'manual'
        else:
            days_ago = '—'
            status = '?'
            interval_str = f'{interval}d' if interval else 'manual'
        lines.append(f'| {name} | {last_run} | {interval_str} | {status} |')
    lines.append('')

    # Uncontested Primaries
    unc = data.get('uncontested_by_state', {})
    if unc:
        lines.append('### Uncontested Primaries')
        lines.append('| State | D Uncontested | D Total | D% | R Uncontested | R Total | R% |')
        lines.append('|-------|-------------:|--------:|---:|--------------:|--------:|---:|')
        for st in sorted(unc.keys()):
            v = unc[st]
            d_pct = (v['d_uncontested'] / v['d_total'] * 100) if v['d_total'] > 0 else 0
            r_pct = (v['r_uncontested'] / v['r_total'] * 100) if v['r_total'] > 0 else 0
            lines.append(f'| {st} | {v["d_uncontested"]} | {v["d_total"]} | {d_pct:.0f}% | {v["r_uncontested"]} | {v["r_total"]} | {r_pct:.0f}% |')
        lines.append('')

    # Action Items
    lines.append('### Action Items')
    if data['actions']:
        for a in data['actions']:
            lines.append(f'- [ ] **{a["priority"]}**: {a["text"]}')
    else:
        lines.append('- (none)')
    lines.append('')

    return '\n'.join(lines)


def write_vault_note(data):
    """Write or update the Obsidian project note with fresh briefing data."""
    new_section = build_vault_markdown(data)

    if os.path.exists(VAULT_NOTE_PATH):
        with open(VAULT_NOTE_PATH) as f:
            content = f.read()

        # Find the markers
        start_marker = '## Current Status'
        end_marker = '## Key Contacts'

        start_idx = content.find(start_marker)
        end_idx = content.find(end_marker)

        if start_idx != -1 and end_idx != -1:
            # Replace everything between markers
            updated = content[:start_idx] + new_section + '\n' + content[end_idx:]
        elif start_idx != -1:
            # No end marker — replace from start marker to end of file
            updated = content[:start_idx] + new_section
        else:
            # No markers found — append
            updated = content + '\n' + new_section
    else:
        # Create full initial template
        updated = generate_initial_note(new_section)

    os.makedirs(os.path.dirname(VAULT_NOTE_PATH), exist_ok=True)
    with open(VAULT_NOTE_PATH, 'w') as f:
        f.write(updated)

    print(f'  Vault note written: {VAULT_NOTE_PATH}')


def generate_initial_note(status_section):
    """Generate the full initial Obsidian project note."""
    return f"""# Elections Database

**Status:** Active
**Area:** [[Platform & Engineering]]
**Start Date:** 2026-01-15
**Target Date:** 2026-11-03 (General Election)
**Tags:** #project #elections

## Overview
> Comprehensive US state-level elections database and dashboard for the 2026 cycle. Covers all 50 states, ~7,900 seats, ~20,000 elections, and ~11,400 candidates. Built on Supabase PostgreSQL with a static site dashboard hosted on GitHub Pages.

## Goals / Success Criteria
- [x] Complete database schema covering all 50 states
- [x] Populate all seats, districts, and current officeholders
- [x] Load 2026 election records (primaries, generals, specials)
- [x] Build interactive dashboard with state maps
- [ ] Collect candidacy data for all states as filing deadlines close
- [ ] Track special elections and vacancies in real-time
- [ ] Prepare election night results collection pipeline
- [ ] Complete ballot measure tracking for 2026

{status_section}
## Key Contacts
| Source | URL | Notes |
|--------|-----|-------|
| Ballotpedia | ballotpedia.org | Primary data source |
| StateNavigate | Google Sheet | State legislature metadata |
| OpenStates | openstates.org | Supplementary CSV data |
| Sabato's Crystal Ball | centerforpolitics.org | Forecast ratings |

## Key Tasks
- [ ] Phase 2 candidacy collection (Mar-Sep 2026 as filing closes per state)
- [ ] Monitor for vacancies, party switches, special elections
- [ ] Build election night results pipeline
- [ ] Update forecasts as new ratings come out
- [ ] Site improvements: search, filters, mobile layout

## Recurring Tasks
- [ ] Weekly: Run election briefing (`python3 scripts/election_briefing.py`)
- [ ] Weekly: Check for new special elections
- [ ] Bi-weekly: Seat gap audit
- [ ] Bi-weekly: Update forecasts
- [ ] As needed: Scrape candidacies when filing deadlines close
- [ ] As needed: Export site data after DB updates

## Resources & Links
- GitHub: [w85kramer/elections](https://github.com/w85kramer/elections)
- Live site: [w85kramer.github.io/elections](https://w85kramer.github.io/elections/)
- Supabase project: `pikcvwulzfxgwfcfssxc`
- Ballotpedia calendar: ballotpedia.org/Elections_calendar

## Log
| Date | Update |
|------|--------|
| 2026-02-20 | Election briefing system created |
| 2026-02-19 | Site data export + interactive maps live |
| 2026-02-08 | Candidacy Phase 1 complete (11 states) |
| 2026-01-15 | Project started |
"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Election Briefing & Reminder System')
    parser.add_argument('--console-only', action='store_true',
                        help='Print briefing to console without writing to vault')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print queries without executing')
    parser.add_argument('--mark-done', type=str, metavar='CHECK_NAME',
                        help='Mark a monitoring check as done today')
    parser.add_argument('--json', action='store_true',
                        help='Output briefing data as JSON')
    args = parser.parse_args()

    # Handle --mark-done
    if args.mark_done:
        mon = load_monitoring_state()
        check_name = args.mark_done
        if check_name not in mon.get('checks', {}):
            print(f"Unknown check: {check_name}")
            print(f"Available: {', '.join(mon.get('checks', {}).keys())}")
            sys.exit(1)
        mon['checks'][check_name]['last_run'] = TODAY.isoformat()
        save_monitoring_state(mon)
        print(f"Marked '{check_name}' as done ({TODAY.isoformat()})")
        return

    # Build briefing
    data = build_briefing(dry_run=args.dry_run)
    if data is None:
        return  # dry-run already printed

    # Update last_briefing timestamp
    mon = load_monitoring_state()
    mon['last_briefing'] = datetime.now().isoformat()
    save_monitoring_state(mon)

    if args.json:
        # JSON output (convert non-serializable types)
        print(json.dumps(data, indent=2, default=str))
        return

    # Console output
    print_briefing(data)

    # Vault write
    if not args.console_only:
        print('Writing vault note...')
        write_vault_note(data)
        print(f'\n  Note: vault changes not auto-committed.')
        print(f'  To commit: cd ~/second-brain && git add -A && git commit -m "Update elections briefing" && git push')


if __name__ == '__main__':
    main()
