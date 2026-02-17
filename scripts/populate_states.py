"""
Populate the states table with all 50 US states.
Data sourced from Ballotpedia and NCSL.
"""
import httpx
import json
import sys
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL

def run_sql(query):
    resp = httpx.post(
        f'https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        json={'query': query},
        timeout=30
    )
    if resp.status_code != 201:
        print(f'ERROR: {resp.status_code} - {resp.text[:500]}')
        sys.exit(1)
    return resp.json()

# All 50 states data
# Format: (state_name, abbr, senate_seats, house_seats, senate_term, house_term,
#           jungle_primary, has_runoffs, multimember, gov_term, gov_term_limit,
#           next_gov_year, notes)
states = [
    ("Alabama", "AL", 35, 105, 4, 4, False, True, False, 4,
     "Yes (2 consecutive)", 2026, "Both chambers serve 4-year terms; primary runoffs required"),
    ("Alaska", "AK", 20, 40, 4, 2, True, False, False, 4,
     "Yes (2 consecutive)", 2026, "Top-four primary with ranked-choice general election"),
    ("Arizona", "AZ", 30, 60, 2, 2, False, False, True, 4,
     "Yes (2 consecutive)", 2026, "House has 2-member districts (30 districts, 60 members)"),
    ("Arkansas", "AR", 35, 100, 4, 2, False, True, False, 4,
     "Yes (2 consecutive)", 2026, "Primary runoffs required"),
    ("California", "CA", 40, 80, 4, 2, True, False, False, 4,
     "Yes (2 terms lifetime)", 2026, "Top-two jungle primary; Assembly districts"),
    ("Colorado", "CO", 35, 65, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Connecticut", "CT", 36, 151, 2, 2, False, False, False, 4,
     "None", 2026, "No governor term limits"),
    ("Delaware", "DE", 21, 41, 4, 2, False, False, False, 4,
     "Yes (2 terms lifetime)", 2028, None),
    ("Florida", "FL", 40, 120, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Georgia", "GA", 56, 180, 2, 2, False, True, False, 4,
     "Yes (2 consecutive)", 2026, "Runoffs required if no candidate gets 50%+ in primary or general"),
    ("Hawaii", "HI", 25, 51, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Idaho", "ID", 35, 70, 2, 2, False, False, True, 4,
     "None", 2026, "House has 2-member districts (35 districts, 70 members); no governor term limits"),
    ("Illinois", "IL", 59, 118, 4, 2, False, False, False, 4,
     "None", 2026, "Senate terms follow 4-4-2 cycle after redistricting; no governor term limits"),
    ("Indiana", "IN", 50, 100, 4, 2, False, False, False, 4,
     "Yes (2 terms in 12 years)", 2028, None),
    ("Iowa", "IA", 50, 100, 4, 2, False, False, False, 4,
     "None", 2026, "No governor term limits"),
    ("Kansas", "KS", 40, 125, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Kentucky", "KY", 38, 100, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2027, None),
    ("Louisiana", "LA", 39, 105, 4, 4, True, True, False, 4,
     "Yes (2 consecutive)", 2027, "Jungle primary system; both chambers serve 4-year terms; state legislature switching to closed primaries in 2027"),
    ("Maine", "ME", 35, 151, 2, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Maryland", "MD", 47, 141, 4, 4, False, False, True, 4,
     "Yes (2 consecutive)", 2026, "Both chambers serve 4-year terms; House of Delegates has multi-member districts (1-3 members per district)"),
    ("Massachusetts", "MA", 40, 160, 2, 2, False, False, False, 4,
     "None", 2026, "No governor term limits"),
    ("Michigan", "MI", 38, 110, 4, 2, False, False, False, 4,
     "Yes (2 terms lifetime)", 2026, None),
    ("Minnesota", "MN", 67, 134, 4, 2, False, False, False, 4,
     "None", 2026, "No governor term limits; Senate terms follow 4-4-2 cycle after redistricting"),
    ("Mississippi", "MS", 52, 122, 4, 4, False, True, False, 4,
     "Yes (2 consecutive)", 2027, "Both chambers serve 4-year terms; general election runoffs for statewide offices"),
    ("Missouri", "MO", 34, 163, 4, 2, False, False, False, 4,
     "Yes (2 terms lifetime)", 2028, None),
    ("Montana", "MT", 50, 100, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2028, None),
    ("Nebraska", "NE", 49, 0, 4, 0, True, False, False, 4,
     "Yes (2 consecutive)", 2026, "Unicameral legislature; nonpartisan ballot; top-two primary"),
    ("Nevada", "NV", 21, 42, 4, 2, False, False, False, 4,
     "Yes (2 terms lifetime)", 2026, "Assembly districts"),
    ("New Hampshire", "NH", 24, 400, 2, 2, False, False, True, 2,
     "None", 2026, "Largest state house in the US (400 members); multi-member House districts; 2-year governor terms; no governor term limits"),
    ("New Jersey", "NJ", 40, 80, 4, 2, False, False, True, 4,
     "Yes (2 consecutive)", 2029, "General Assembly has 2-member districts (40 districts, 80 members); odd-year elections"),
    ("New Mexico", "NM", 42, 70, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("New York", "NY", 63, 150, 2, 2, False, False, False, 4,
     "None", 2026, "Assembly districts; no governor term limits"),
    ("North Carolina", "NC", 50, 120, 2, 2, False, True, False, 4,
     "Yes (2 consecutive)", 2028, "Primary runoff if requested by 2nd place and winner got <30%"),
    ("North Dakota", "ND", 47, 94, 4, 4, False, False, True, 4,
     "Yes (2 terms)", 2028, "House has 2-member districts (47 districts, 94 members); both chambers serve 4-year terms"),
    ("Ohio", "OH", 33, 99, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Oklahoma", "OK", 48, 101, 4, 2, False, True, False, 4,
     "Yes (2 terms lifetime)", 2026, "Primary runoffs required"),
    ("Oregon", "OR", 30, 60, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Pennsylvania", "PA", 50, 203, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Rhode Island", "RI", 38, 75, 2, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("South Carolina", "SC", 46, 124, 4, 2, False, True, False, 4,
     "Yes (2 consecutive)", 2026, "Primary runoffs required"),
    ("South Dakota", "SD", 35, 70, 2, 2, False, False, True, 4,
     "Yes (2 consecutive)", 2026, "House has 2-member districts (35 districts, 70 members)"),
    ("Tennessee", "TN", 33, 99, 4, 2, False, False, False, 4,
     "Yes (2 consecutive)", 2026, None),
    ("Texas", "TX", 31, 150, 4, 2, False, True, False, 4,
     "None", 2026, "Primary runoffs required; no governor term limits"),
    ("Utah", "UT", 29, 75, 4, 2, False, False, False, 4,
     "None", 2028, "No governor term limits"),
    ("Vermont", "VT", 30, 150, 2, 2, False, False, True, 2,
     "None", 2026, "Multi-member districts in both chambers; 2-year governor terms; no governor term limits"),
    ("Virginia", "VA", 40, 100, 4, 2, False, False, False, 4,
     "Yes (1 consecutive)", 2029, "House of Delegates; odd-year elections; governor cannot serve consecutive terms"),
    ("Washington", "WA", 49, 98, 4, 2, True, False, True, 4,
     "None", 2028, "Top-two primary; House has 2-member districts (49 districts, 98 members); no governor term limits"),
    ("West Virginia", "WV", 34, 100, 4, 2, False, False, True, 4,
     "Yes (2 consecutive)", 2028, "House of Delegates; Senate has multi-member districts (17 districts, 34 senators)"),
    ("Wisconsin", "WI", 33, 99, 4, 2, False, False, False, 4,
     "None", 2026, "Assembly districts; no governor term limits"),
    ("Wyoming", "WY", 30, 62, 4, 2, False, False, False, 4,
     "Yes (2 terms in 16 years)", 2026, None),
]

# Build INSERT
values_parts = []
for s in states:
    name, abbr, sen, house, sen_t, house_t, jungle, runoff, multi, gov_t, gov_limit, gov_yr, notes = s
    notes_sql = f"'{notes}'" if notes else "NULL"
    notes_sql = notes_sql.replace("'", "''").replace("''", "'", 1)  # handle escaping
    # Re-escape properly
    if notes:
        notes_escaped = notes.replace("'", "''")
        notes_sql = f"'{notes_escaped}'"
    else:
        notes_sql = "NULL"

    values_parts.append(
        f"('{name}', '{abbr}', {sen}, {house}, {sen_t}, {house_t}, "
        f"{str(jungle).upper()}, {str(runoff).upper()}, {str(multi).upper()}, "
        f"{gov_t}, '{gov_limit}', {gov_yr}, {notes_sql})"
    )

sql = """
INSERT INTO states (
    state_name, abbreviation, senate_seats, house_seats,
    senate_term_years, house_term_years,
    uses_jungle_primary, has_runoffs, has_multimember_districts,
    gov_term_years, gov_term_limit, next_gov_election_year, notes
) VALUES
""" + ",\n".join(values_parts) + "\nRETURNING id, state_name, abbreviation;"

results = run_sql(sql)
print(f"Inserted {len(results)} states:")
for r in results:
    print(f"  {r['id']:3d}  {r['abbreviation']}  {r['state_name']}")
