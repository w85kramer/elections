"""
Current Appointed/Ex-Officio Statewide Officeholders — 70 seats
================================================================
As of February 14, 2026.

Data sourced from Ballotpedia, official state websites, and web research (Feb 2026).

Covers 6 office types:
- Lt. Governor (2 ex officio)
- Attorney General (7 appointed)
- Secretary of State (12 appointed)
- Treasurer (12 appointed)
- Auditor (26 appointed)
- Controller (11 appointed)

Notes on party codes:
- 'R' = Republican, 'D' = Democrat, 'NP' = Nonpartisan
- Many appointed positions are formally nonpartisan even when appointed by partisan governors
- caucus left null for NP officers

Each entry: {state, office_type, name, first_name, last_name, party, caucus, start_date, start_reason}
"""

OFFICEHOLDERS = [
    # ══════════════════════════════════════════════════════════════════
    # LT. GOVERNOR (2 — Ex Officio: Senate Presidents)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'TN', 'office_type': 'Lt. Governor', 'name': 'Randy McNally', 'first_name': 'Randy', 'last_name': 'McNally', 'party': 'R', 'caucus': 'R', 'start_date': '2017-01-10', 'start_reason': 'elected'},
    {'state': 'WV', 'office_type': 'Lt. Governor', 'name': 'Randy E. Smith', 'first_name': 'Randy', 'last_name': 'Smith', 'party': 'R', 'caucus': 'R', 'start_date': '2025-01-08', 'start_reason': 'elected'},

    # ══════════════════════════════════════════════════════════════════
    # ATTORNEY GENERAL (7 — Appointed)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AK', 'office_type': 'Attorney General', 'name': 'Stephen Cox', 'first_name': 'Stephen', 'last_name': 'Cox', 'party': 'R', 'caucus': 'R', 'start_date': '2025-08-29', 'start_reason': 'appointed'},
    {'state': 'HI', 'office_type': 'Attorney General', 'name': 'Anne Lopez', 'first_name': 'Anne', 'last_name': 'Lopez', 'party': 'NP', 'caucus': None, 'start_date': '2022-12-05', 'start_reason': 'appointed'},
    {'state': 'ME', 'office_type': 'Attorney General', 'name': 'Aaron Frey', 'first_name': 'Aaron', 'last_name': 'Frey', 'party': 'D', 'caucus': 'D', 'start_date': '2019-01-08', 'start_reason': 'appointed'},
    {'state': 'NH', 'office_type': 'Attorney General', 'name': 'John Formella', 'first_name': 'John', 'last_name': 'Formella', 'party': 'R', 'caucus': 'R', 'start_date': '2021-04-22', 'start_reason': 'appointed'},
    {'state': 'NJ', 'office_type': 'Attorney General', 'name': 'Jennifer Davenport', 'first_name': 'Jennifer', 'last_name': 'Davenport', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-20', 'start_reason': 'appointed'},
    {'state': 'TN', 'office_type': 'Attorney General', 'name': 'Jonathan Skrmetti', 'first_name': 'Jonathan', 'last_name': 'Skrmetti', 'party': 'R', 'caucus': 'R', 'start_date': '2022-09-01', 'start_reason': 'appointed'},
    {'state': 'WY', 'office_type': 'Attorney General', 'name': 'Keith G. Kautz', 'first_name': 'Keith', 'last_name': 'Kautz', 'party': 'R', 'caucus': 'R', 'start_date': '2025-07-07', 'start_reason': 'appointed'},

    # ══════════════════════════════════════════════════════════════════
    # SECRETARY OF STATE (12 — Appointed)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'DE', 'office_type': 'Secretary of State', 'name': 'Charuni Patibanda-Sanchez', 'first_name': 'Charuni', 'last_name': 'Patibanda-Sanchez', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-29', 'start_reason': 'appointed'},
    {'state': 'FL', 'office_type': 'Secretary of State', 'name': 'Cord Byrd', 'first_name': 'Cord', 'last_name': 'Byrd', 'party': 'R', 'caucus': 'R', 'start_date': '2023-04-26', 'start_reason': 'appointed'},
    {'state': 'MD', 'office_type': 'Secretary of State', 'name': 'Susan Lee', 'first_name': 'Susan', 'last_name': 'Lee', 'party': 'D', 'caucus': 'D', 'start_date': '2023-02-14', 'start_reason': 'appointed'},
    {'state': 'ME', 'office_type': 'Secretary of State', 'name': 'Shenna Bellows', 'first_name': 'Shenna', 'last_name': 'Bellows', 'party': 'D', 'caucus': 'D', 'start_date': '2021-01-04', 'start_reason': 'appointed'},
    {'state': 'NH', 'office_type': 'Secretary of State', 'name': 'David Scanlan', 'first_name': 'David', 'last_name': 'Scanlan', 'party': 'R', 'caucus': 'R', 'start_date': '2022-01-10', 'start_reason': 'appointed'},
    {'state': 'NJ', 'office_type': 'Secretary of State', 'name': 'Dale Caldwell', 'first_name': 'Dale', 'last_name': 'Caldwell', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-20', 'start_reason': 'appointed'},
    {'state': 'NY', 'office_type': 'Secretary of State', 'name': 'Walter Mosley', 'first_name': 'Walter', 'last_name': 'Mosley', 'party': 'D', 'caucus': 'D', 'start_date': '2024-05-22', 'start_reason': 'appointed'},
    {'state': 'OK', 'office_type': 'Secretary of State', 'name': 'Benjamin Lepak', 'first_name': 'Benjamin', 'last_name': 'Lepak', 'party': 'R', 'caucus': 'R', 'start_date': '2025-10-02', 'start_reason': 'appointed'},
    {'state': 'PA', 'office_type': 'Secretary of State', 'name': 'Al Schmidt', 'first_name': 'Al', 'last_name': 'Schmidt', 'party': 'R', 'caucus': 'R', 'start_date': '2023-06-29', 'start_reason': 'appointed'},
    {'state': 'TN', 'office_type': 'Secretary of State', 'name': 'Tre Hargett', 'first_name': 'Tre', 'last_name': 'Hargett', 'party': 'R', 'caucus': 'R', 'start_date': '2009-01-15', 'start_reason': 'appointed'},
    {'state': 'TX', 'office_type': 'Secretary of State', 'name': 'Jane Nelson', 'first_name': 'Jane', 'last_name': 'Nelson', 'party': 'R', 'caucus': 'R', 'start_date': '2023-01-07', 'start_reason': 'appointed'},
    {'state': 'VA', 'office_type': 'Secretary of State', 'name': 'Candi Mundon King', 'first_name': 'Candi', 'last_name': 'Mundon King', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-17', 'start_reason': 'appointed'},

    # ══════════════════════════════════════════════════════════════════
    # TREASURER (12 — Appointed)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AK', 'office_type': 'Treasurer', 'name': 'Janelle Earls', 'first_name': 'Janelle', 'last_name': 'Earls', 'party': 'NP', 'caucus': None, 'start_date': '2025-08-08', 'start_reason': 'appointed'},
    {'state': 'GA', 'office_type': 'Treasurer', 'name': 'Steve McCoy', 'first_name': 'Steve', 'last_name': 'McCoy', 'party': 'NP', 'caucus': None, 'start_date': '2020-08-05', 'start_reason': 'appointed'},
    {'state': 'HI', 'office_type': 'Treasurer', 'name': 'Seth Colby', 'first_name': 'Seth', 'last_name': 'Colby', 'party': 'NP', 'caucus': None, 'start_date': '2025-12-08', 'start_reason': 'appointed'},
    {'state': 'MD', 'office_type': 'Treasurer', 'name': 'Dereck Davis', 'first_name': 'Dereck', 'last_name': 'Davis', 'party': 'D', 'caucus': 'D', 'start_date': '2021-12-17', 'start_reason': 'appointed'},
    {'state': 'ME', 'office_type': 'Treasurer', 'name': 'Joseph Perry', 'first_name': 'Joseph', 'last_name': 'Perry', 'party': 'D', 'caucus': 'D', 'start_date': '2025-01-06', 'start_reason': 'appointed'},
    {'state': 'MI', 'office_type': 'Treasurer', 'name': 'Rachael Eubanks', 'first_name': 'Rachael', 'last_name': 'Eubanks', 'party': 'NP', 'caucus': None, 'start_date': '2019-01-01', 'start_reason': 'appointed'},
    {'state': 'MN', 'office_type': 'Treasurer', 'name': 'Erin Campbell', 'first_name': 'Erin', 'last_name': 'Campbell', 'party': 'NP', 'caucus': None, 'start_date': '2023-08-15', 'start_reason': 'appointed'},
    {'state': 'MT', 'office_type': 'Treasurer', 'name': 'Brendan Beatty', 'first_name': 'Brendan', 'last_name': 'Beatty', 'party': 'NP', 'caucus': None, 'start_date': '2021-01-04', 'start_reason': 'appointed'},
    {'state': 'NH', 'office_type': 'Treasurer', 'name': 'Monica Mezzapelle', 'first_name': 'Monica', 'last_name': 'Mezzapelle', 'party': 'NP', 'caucus': None, 'start_date': '2020-03-25', 'start_reason': 'appointed'},
    {'state': 'NJ', 'office_type': 'Treasurer', 'name': 'Aaron Binder', 'first_name': 'Aaron', 'last_name': 'Binder', 'party': 'NP', 'caucus': None, 'start_date': '2026-01-20', 'start_reason': 'appointed'},
    {'state': 'TN', 'office_type': 'Treasurer', 'name': 'David Lillard Jr.', 'first_name': 'David', 'last_name': 'Lillard', 'party': 'R', 'caucus': 'R', 'start_date': '2009-01-15', 'start_reason': 'appointed'},
    {'state': 'VA', 'office_type': 'Treasurer', 'name': 'David Richardson', 'first_name': 'David', 'last_name': 'Richardson', 'party': 'NP', 'caucus': None, 'start_date': '2022-06-02', 'start_reason': 'appointed'},

    # ══════════════════════════════════════════════════════════════════
    # AUDITOR (26 — Appointed)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AK', 'office_type': 'Auditor', 'name': 'Kris Curtis', 'first_name': 'Kris', 'last_name': 'Curtis', 'party': 'NP', 'caucus': None, 'start_date': '2012-01-01', 'start_reason': 'appointed'},
    {'state': 'AZ', 'office_type': 'Auditor', 'name': 'Lindsey Perry', 'first_name': 'Lindsey', 'last_name': 'Perry', 'party': 'NP', 'caucus': None, 'start_date': '2018-04-18', 'start_reason': 'appointed'},
    {'state': 'CA', 'office_type': 'Auditor', 'name': 'Grant Parks', 'first_name': 'Grant', 'last_name': 'Parks', 'party': 'NP', 'caucus': None, 'start_date': '2023-01-16', 'start_reason': 'appointed'},
    {'state': 'CO', 'office_type': 'Auditor', 'name': 'Kerri L. Hunter', 'first_name': 'Kerri', 'last_name': 'Hunter', 'party': 'NP', 'caucus': None, 'start_date': '2021-07-01', 'start_reason': 'appointed'},
    {'state': 'CT', 'office_type': 'Auditor', 'name': 'John C. Geragosian', 'first_name': 'John', 'last_name': 'Geragosian', 'party': 'D', 'caucus': 'D', 'start_date': '2011-01-01', 'start_reason': 'appointed'},
    {'state': 'FL', 'office_type': 'Auditor', 'name': 'Sherrill F. Norman', 'first_name': 'Sherrill', 'last_name': 'Norman', 'party': 'NP', 'caucus': None, 'start_date': '2015-07-02', 'start_reason': 'appointed'},
    {'state': 'GA', 'office_type': 'Auditor', 'name': 'Greg S. Griffin', 'first_name': 'Greg', 'last_name': 'Griffin', 'party': 'NP', 'caucus': None, 'start_date': '2012-07-01', 'start_reason': 'appointed'},
    {'state': 'HI', 'office_type': 'Auditor', 'name': 'Leslie H. Kondo', 'first_name': 'Leslie', 'last_name': 'Kondo', 'party': 'NP', 'caucus': None, 'start_date': '2016-05-01', 'start_reason': 'appointed'},
    {'state': 'ID', 'office_type': 'Auditor', 'name': 'April Renfro', 'first_name': 'April', 'last_name': 'Renfro', 'party': 'NP', 'caucus': None, 'start_date': '2012-03-01', 'start_reason': 'appointed'},
    {'state': 'IL', 'office_type': 'Auditor', 'name': 'Frank J. Mautino', 'first_name': 'Frank', 'last_name': 'Mautino', 'party': 'NP', 'caucus': None, 'start_date': '2016-01-01', 'start_reason': 'appointed'},
    {'state': 'KS', 'office_type': 'Auditor', 'name': 'Chris Clarke', 'first_name': 'Chris', 'last_name': 'Clarke', 'party': 'NP', 'caucus': None, 'start_date': '2023-12-24', 'start_reason': 'appointed'},
    {'state': 'LA', 'office_type': 'Auditor', 'name': 'Michael J. Waguespack', 'first_name': 'Michael', 'last_name': 'Waguespack', 'party': 'NP', 'caucus': None, 'start_date': '2021-04-19', 'start_reason': 'appointed'},
    {'state': 'MD', 'office_type': 'Auditor', 'name': 'Brian S. Tanen', 'first_name': 'Brian', 'last_name': 'Tanen', 'party': 'NP', 'caucus': None, 'start_date': '2024-07-01', 'start_reason': 'appointed'},
    {'state': 'ME', 'office_type': 'Auditor', 'name': 'Matthew Dunlap', 'first_name': 'Matthew', 'last_name': 'Dunlap', 'party': 'D', 'caucus': 'D', 'start_date': '2022-11-14', 'start_reason': 'appointed'},
    {'state': 'MI', 'office_type': 'Auditor', 'name': 'Doug A. Ringler', 'first_name': 'Doug', 'last_name': 'Ringler', 'party': 'NP', 'caucus': None, 'start_date': '2014-06-09', 'start_reason': 'appointed'},
    {'state': 'NH', 'office_type': 'Auditor', 'name': 'Christine L. Young', 'first_name': 'Christine', 'last_name': 'Young', 'party': 'NP', 'caucus': None, 'start_date': '2016-01-01', 'start_reason': 'appointed'},
    {'state': 'NJ', 'office_type': 'Auditor', 'name': 'David J. Kaschak', 'first_name': 'David', 'last_name': 'Kaschak', 'party': 'NP', 'caucus': None, 'start_date': '2021-02-23', 'start_reason': 'appointed'},
    {'state': 'NV', 'office_type': 'Auditor', 'name': 'Daniel L. Crossman', 'first_name': 'Daniel', 'last_name': 'Crossman', 'party': 'NP', 'caucus': None, 'start_date': '2019-01-01', 'start_reason': 'appointed'},
    {'state': 'NY', 'office_type': 'Auditor', 'name': 'Thomas P. DiNapoli', 'first_name': 'Thomas', 'last_name': 'DiNapoli', 'party': 'D', 'caucus': 'D', 'start_date': '2007-02-07', 'start_reason': 'appointed'},
    {'state': 'OR', 'office_type': 'Auditor', 'name': 'Steve Bergmann', 'first_name': 'Steve', 'last_name': 'Bergmann', 'party': 'NP', 'caucus': None, 'start_date': '2025-01-13', 'start_reason': 'appointed'},
    {'state': 'RI', 'office_type': 'Auditor', 'name': 'David A. Bergantino', 'first_name': 'David', 'last_name': 'Bergantino', 'party': 'NP', 'caucus': None, 'start_date': '2023-01-06', 'start_reason': 'appointed'},
    {'state': 'SC', 'office_type': 'Auditor', 'name': 'Sue F. Moss', 'first_name': 'Sue', 'last_name': 'Moss', 'party': 'NP', 'caucus': None, 'start_date': '2025-02-03', 'start_reason': 'appointed'},
    {'state': 'TN', 'office_type': 'Auditor', 'name': 'Jason E. Mumpower', 'first_name': 'Jason', 'last_name': 'Mumpower', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-13', 'start_reason': 'appointed'},
    {'state': 'TX', 'office_type': 'Auditor', 'name': 'Lisa R. Collier', 'first_name': 'Lisa', 'last_name': 'Collier', 'party': 'NP', 'caucus': None, 'start_date': '2021-11-01', 'start_reason': 'appointed'},
    {'state': 'VA', 'office_type': 'Auditor', 'name': 'Staci A. Henshaw', 'first_name': 'Staci', 'last_name': 'Henshaw', 'party': 'NP', 'caucus': None, 'start_date': '2021-02-01', 'start_reason': 'appointed'},
    {'state': 'WI', 'office_type': 'Auditor', 'name': 'Joe Chrisman', 'first_name': 'Joe', 'last_name': 'Chrisman', 'party': 'NP', 'caucus': None, 'start_date': '2011-06-17', 'start_reason': 'appointed'},

    # ══════════════════════════════════════════════════════════════════
    # CONTROLLER (11 — Appointed)
    # ══════════════════════════════════════════════════════════════════
    {'state': 'AK', 'office_type': 'Controller', 'name': 'Kayla Wisner', 'first_name': 'Kayla', 'last_name': 'Wisner', 'party': 'NP', 'caucus': None, 'start_date': '2019-10-01', 'start_reason': 'appointed'},
    {'state': 'AL', 'office_type': 'Controller', 'name': 'Kathleen D. Baxter', 'first_name': 'Kathleen', 'last_name': 'Baxter', 'party': 'NP', 'caucus': None, 'start_date': '2017-08-02', 'start_reason': 'appointed'},
    {'state': 'CO', 'office_type': 'Controller', 'name': 'Robert Jaros', 'first_name': 'Robert', 'last_name': 'Jaros', 'party': 'NP', 'caucus': None, 'start_date': '2013-05-01', 'start_reason': 'appointed'},
    {'state': 'MA', 'office_type': 'Controller', 'name': 'William McNamara', 'first_name': 'William', 'last_name': 'McNamara', 'party': 'NP', 'caucus': None, 'start_date': '2020-02-21', 'start_reason': 'appointed'},
    {'state': 'ME', 'office_type': 'Controller', 'name': 'Douglas Cotnoir', 'first_name': 'Douglas', 'last_name': 'Cotnoir', 'party': 'NP', 'caucus': None, 'start_date': '2012-02-01', 'start_reason': 'appointed'},
    {'state': 'NC', 'office_type': 'Controller', 'name': 'Nels Roseland', 'first_name': 'Nels', 'last_name': 'Roseland', 'party': 'NP', 'caucus': None, 'start_date': '2022-07-01', 'start_reason': 'appointed'},
    {'state': 'NH', 'office_type': 'Controller', 'name': 'Dana Call', 'first_name': 'Dana', 'last_name': 'Call', 'party': 'NP', 'caucus': None, 'start_date': '2017-04-01', 'start_reason': 'appointed'},
    {'state': 'NJ', 'office_type': 'Controller', 'name': 'Shirley U. Emehelu', 'first_name': 'Shirley', 'last_name': 'Emehelu', 'party': 'D', 'caucus': 'D', 'start_date': '2026-01-20', 'start_reason': 'appointed'},
    {'state': 'NM', 'office_type': 'Controller', 'name': 'Mark Melhoff', 'first_name': 'Mark', 'last_name': 'Melhoff', 'party': 'NP', 'caucus': None, 'start_date': '2023-04-15', 'start_reason': 'appointed'},
    {'state': 'TN', 'office_type': 'Controller', 'name': 'Jason E. Mumpower', 'first_name': 'Jason', 'last_name': 'Mumpower', 'party': 'R', 'caucus': 'R', 'start_date': '2021-01-13', 'start_reason': 'appointed'},
    {'state': 'VA', 'office_type': 'Controller', 'name': 'Scott Adams', 'first_name': 'Scott', 'last_name': 'Adams', 'party': 'NP', 'caucus': None, 'start_date': '2024-08-23', 'start_reason': 'appointed'},
]
