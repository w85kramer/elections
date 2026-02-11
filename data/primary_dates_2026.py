"""
2026 State Primary Election Dates
=================================
Sources:
  - NCSL: https://www.ncsl.org/elections-and-campaigns/2026-state-primary-election-dates
  - 270toWin: https://www.270towin.com/2026-state-primary-calendar/
  - Ballotpedia: https://ballotpedia.org/Statewide_primary_elections_calendar
  - Arizona SoS: https://azsos.gov/elections/election-information/2026-election-info
  - Louisiana SoS: https://www.sos.la.gov/ElectionsAndVoting/PublishedDocuments/ElectionsCalendar2026.pdf

Notes:
  - Dates reflect statutory requirements as of Feb 8, 2026.
  - Arizona's primary was moved from Aug 4 to Jul 21 by law signed Feb 7, 2026.
  - Louisiana switched from jungle primary to closed primary system for 2026.
    State legislative elections in LA are held in odd years (next: 2027).
    The May 16 date is the congressional/federal primary.
  - Mississippi state legislative elections are held in odd years (next: 2027).
    The Mar 10 date is the congressional/federal primary.
  - Virginia state legislative elections are held in odd years (next: 2027).
    The Jun 16 date is the congressional/federal primary.
  - New Jersey state legislative elections are held in odd years (next: 2027).
    The Jun 2 date is the congressional/federal primary.
  - Wisconsin has two primaries: Feb 17 (spring/nonpartisan) and Aug 11
    (fall/partisan). The Aug 11 date is for state legislative races.
  - General election for all states: November 3, 2026.
"""

PRIMARY_DATES = {
    'AL': '2026-05-19',  # Runoff: 2026-06-16
    'AK': '2026-08-18',
    'AZ': '2026-07-21',  # Changed from Aug 4; signed into law Feb 7, 2026
    'AR': '2026-03-03',  # Runoff: 2026-03-31
    'CA': '2026-06-02',  # Top-two primary (all parties on one ballot)
    'CO': '2026-06-30',
    'CT': '2026-08-11',
    'DE': '2026-09-15',
    'FL': '2026-08-18',
    'GA': '2026-05-19',  # Runoff: 2026-06-16
    'HI': '2026-08-08',
    'ID': '2026-05-19',
    'IL': '2026-03-17',
    'IN': '2026-05-05',
    'IA': '2026-06-02',
    'KS': '2026-08-04',
    'KY': '2026-05-19',
    'LA': '2026-05-16',  # Congressional only; closed primary (new system). Runoff: 2026-06-27. State leg elections in odd years.
    'ME': '2026-06-09',
    'MD': '2026-06-23',
    'MA': '2026-09-01',
    'MI': '2026-08-04',
    'MN': '2026-08-11',
    'MS': '2026-03-10',  # Congressional only. Runoff: 2026-04-07. State leg elections in odd years.
    'MO': '2026-08-04',
    'MT': '2026-06-02',
    'NE': '2026-05-12',
    'NV': '2026-06-09',
    'NH': '2026-09-08',
    'NJ': '2026-06-02',  # Congressional only. State leg elections in odd years (next: 2027).
    'NM': '2026-06-02',
    'NY': '2026-06-23',
    'NC': '2026-03-03',  # Runoff: 2026-05-12 (triggered if no candidate gets 30%+)
    'ND': '2026-06-09',
    'OH': '2026-05-05',
    'OK': '2026-06-16',  # Runoff: 2026-08-25
    'OR': '2026-05-19',
    'PA': '2026-05-19',
    'RI': '2026-09-08',
    'SC': '2026-06-09',  # Runoff: 2026-06-23
    'SD': '2026-06-02',  # Runoff: 2026-07-28
    'TN': '2026-08-06',
    'TX': '2026-03-03',  # Runoff: 2026-05-26
    'UT': '2026-06-23',
    'VT': '2026-08-11',
    'VA': '2026-06-16',  # Congressional only. State leg elections in odd years (next: 2027).
    'WA': '2026-08-04',  # Top-two primary (all parties on one ballot)
    'WV': '2026-05-12',
    'WI': '2026-08-11',  # Partisan/fall primary. Spring primary (nonpartisan) is Feb 17.
    'WY': '2026-08-18',
}

# States with primary runoff elections
RUNOFF_DATES = {
    'AL': '2026-06-16',
    'AR': '2026-03-31',
    'GA': '2026-06-16',
    'LA': '2026-06-27',
    'MS': '2026-04-07',
    'NC': '2026-05-12',
    'OK': '2026-08-25',
    'SC': '2026-06-23',
    'SD': '2026-07-28',
    'TX': '2026-05-26',
}

# States where 2026 primary is congressional/federal only (no state legislative races)
CONGRESSIONAL_ONLY_STATES = ['LA', 'MS', 'NJ', 'VA']

# General election date
GENERAL_ELECTION_DATE = '2026-11-03'
