#!/usr/bin/env python3
"""Backfill missing candidacy records for 2018-2025 general elections.

Data sourced from Ballotpedia research (February 2026).
Covers ~106 elections across all 50 states that had 0 candidacy records.

Usage:
    python3 scripts/backfill_missing_candidacies.py --dry-run
    python3 scripts/backfill_missing_candidacies.py
"""
import sys, os, time, argparse
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db_config import TOKEN, PROJECT_REF, API_URL
import httpx

def run_sql(query):
    for attempt in range(5):
        try:
            resp = httpx.post(
                API_URL,
                headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
                json={'query': query},
                timeout=120
            )
        except Exception as e:
            print(f'  Request error: {e}')
            time.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 201:
            return resp.json()
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f'  Rate limited, waiting {wait}s...')
            time.sleep(wait)
            continue
        print(f'  SQL ERROR {resp.status_code}: {resp.text[:300]}')
        return None
    print('  Max retries exceeded')
    return None

def esc(s):
    return str(s).replace("'", "''") if s else ''

# ══════════════════════════════════════════════════════════════
# DATA: (election_id, [(name, party, votes, pct, result, inc)])
# votes/pct = None when unavailable (e.g., canceled general)
# ══════════════════════════════════════════════════════════════

DATA = {
    # ── AK ──
    20254: [('Grier Hopkins', 'D', 5478, 54.0, 'Won', True),
            ('Keith Kurber', 'R', 4649, 45.8, 'Lost', False)],
    20298: [('Ben Carpenter', 'R', 6907, 96.6, 'Won', True)],
    # 20207 (AK Senate 19) skipped — vote total mismatch, needs investigation

    # ── AL ──
    21145: [('Dickie Drake', 'R', 13669, 65.6, 'Won', False),
            ('Jenn Gray', 'D', 7148, 34.3, 'Lost', False)],
    21154: [('David Wheeler', 'R', 9170, 54.3, 'Won', False),
            ('Jim Toomey', 'D', 7721, 45.7, 'Lost', False)],
    21152: [('Mike Shaw', 'R', 8590, 64.0, 'Won', False),
            ('Christian Coleman', 'D', 4816, 35.9, 'Lost', False)],
    20927: [('Andy Whitt', 'R', 11893, 95.2, 'Won', False)],
    20926: [('Andy Whitt', 'R', 11052, 80.1, 'Won', True),
            ('Greg Turner', 'L', 2597, 18.8, 'Lost', False)],
    21387: [('Wes Allen', 'R', 7804, 59.3, 'Won', True),
            ('Joel Lee Williams', 'D', 5348, 40.6, 'Lost', False)],
    21386: [('Marcus Paramore', 'R', 7855, 98.1, 'Won', False)],

    # ── AR ──
    22856: [('Aaron Pilkington', 'R', 1735, 100.0, 'Won', True)],
    22855: [('Aaron Pilkington', 'R', 8722, 76.8, 'Won', True),
            ('Whitney Freeman', 'D', 2639, 23.2, 'Lost', False)],
    22527: [('David Fielding', 'D', 5242, 56.8, 'Won', False),
            ('Chase McDowell', 'R', 3993, 43.2, 'Lost', False)],

    # ── AZ (multi-member: top 2 of 3 win) ──
    22002: [('Justin Heap', 'R', 50024, 36.9, 'Won', False),
            ('Barbara Parker', 'R', 49190, 36.3, 'Won', False),
            ('Helen Hunter', 'D', 36182, 26.7, 'Lost', False)],

    # ── CA ──
    23548: [('Ben Hueso', 'D', 152896, 65.9, 'Won', True),
            ('Luis R. Vargas', 'R', 79207, 34.1, 'Lost', False)],

    # ── CT ──
    26816: [("Tom O'Dea", 'R', 6522, 56.8, 'Won', False),
            ('Ross Tartell', 'D', 4959, 43.2, 'Lost', False)],
    26815: [("Tom O'Dea", 'R', 9762, 100.0, 'Won', True)],
    26814: [("Tom O'Dea", 'R', 5805, 53.6, 'Won', True),
            ('Victor Alvarez', 'D', 5019, 46.4, 'Lost', False)],
    26813: [("Tom O'Dea", 'R', 7937, 56.4, 'Won', True),
            ('Jason Bennett', 'D', 6146, 43.6, 'Lost', False)],

    # ── FL ──
    27713: [('Annette Taddeo', 'D', 90924, 53.5, 'Won', True),
            ('Marili Cancio', 'R', 79068, 46.5, 'Lost', False)],

    # ── HI ──
    31936: [('James Tokioka', 'D', None, None, 'Won', True)],  # general canceled
    31933: [('James Tokioka', 'D', 7712, 72.8, 'Won', True),
            ('Steve Yoder', 'R', 2880, 27.2, 'Lost', False)],

    # ── ID ──
    32888: [('Bill Goesling', 'R', 9888, 51.0, 'Won', False),
            ('Margaret Gannon', 'D', 9491, 49.0, 'Lost', False)],

    # ── IL ──
    35006: [('Mike Marron', 'R', 20348, 55.9, 'Won', False),
            ('Cynthia Cunningham', 'D', 16041, 44.1, 'Lost', False)],
    35003: [('Mike Marron', 'R', 27096, 58.8, 'Won', True),
            ('Cynthia Cunningham', 'D', 19007, 41.2, 'Lost', False)],
    35000: [('Mike Marron', 'R', 19613, 54.0, 'Won', True),
            ('Cynthia Cunningham', 'D', 16695, 46.0, 'Lost', False)],
    34997: [('Brandun Schweizer', 'R', 23546, 50.3, 'Won', False),
            ('Jarrett Clem', 'D', 23277, 49.7, 'Lost', False)],

    # ── IN ──
    36419: [('Rita Fleming', 'D', 13235, 55.6, 'Won', False),
            ('Matt Owen', 'R', 10022, 42.1, 'Lost', False),
            ('Thomas Keister', 'L', 524, 2.2, 'Lost', False)],
    36416: [('Rita Fleming', 'D', 19159, 77.6, 'Won', True),
            ('Russell Brooksbank', 'L', 5531, 22.4, 'Lost', False)],
    36413: [('Rita Fleming', 'D', 9302, 50.6, 'Won', True),
            ('Scott Hawkins', 'R', 9076, 49.4, 'Lost', False)],

    # ── KS ──
    38712: [('Michael Houser', 'R', 6103, 100.0, 'Won', False)],
    38710: [('Michael Houser', 'R', 8381, 100.0, 'Won', True)],
    38707: [('Michael Houser', 'R', 5847, 75.2, 'Won', True),
            ('Paul Rogers', 'D', 1928, 24.8, 'Lost', False)],
    40217: [('Russ Jennings', 'R', 4933, 100.0, 'Won', False)],
    40215: [('Russ Jennings', 'R', 6808, 100.0, 'Won', True)],
    40213: [('Bill Clifford', 'R', 4465, 100.0, 'Won', False)],
    40211: [('Lon Pishny', 'R', 5989, 100.0, 'Won', False)],
    39729: [('Jesse Burris', 'R', 7892, 70.1, 'Won', False),
            ('Edward Hackerott', 'D', 3361, 29.9, 'Lost', False)],

    # ── KY ──
    41190: [('Jeffrey Hoover', 'R', 14167, 100.0, 'Won', True)],
    41188: [('Joshua Branscum', 'R', 19498, 100.0, 'Won', False)],
    41187: [('Joshua Branscum', 'R', 13055, 100.0, 'Won', True)],
    41186: [('Joshua Branscum', 'R', 18293, 100.0, 'Won', True)],

    # ── MA ──
    44940: [('Carmine Lawrence Gentile', 'D', 15943, 99.0, 'Won', True)],
    44937: [('Carmine Lawrence Gentile', 'D', 17574, 68.9, 'Won', True),
            ('Ingrid Centurion', 'R', 7899, 31.0, 'Lost', False)],
    44935: [('Carmine Lawrence Gentile', 'D', 16338, 99.4, 'Won', True)],
    44932: [('Carmine Lawrence Gentile', 'D', 18547, 73.5, 'Won', True),
            ('Virginia Gardner', 'R', 6667, 26.4, 'Lost', False)],
    45391: [('Stephan Hay', 'D', 8520, 68.6, 'Won', False),
            ('Elmer Eubanks-Archbold', 'R', 3894, 31.4, 'Lost', False)],
    45389: [('Michael Kushmerek', 'D', 9770, 55.5, 'Won', False),
            ('Glenn Fossa', 'R', 7802, 44.4, 'Lost', False)],
    45387: [('Michael Kushmerek', 'D', 6824, 62.7, 'Won', True),
            ('Aaron Packard', 'R', 4058, 37.3, 'Lost', False)],
    45385: [('Michael Kushmerek', 'D', 13098, 96.5, 'Won', True)],
    44473: [('John Rogers', 'D', 14352, 98.7, 'Won', True)],
    44470: [('John Rogers', 'D', 18837, 98.7, 'Won', True)],
    44467: [('John Rogers', 'D', 12798, 97.9, 'Won', True)],
    44464: [('John Rogers', 'D', 17443, 98.2, 'Won', True)],

    # ── MD (multi-member 3-seat district, Seat A = top getter) ──
    43861: [('Veronica Turner', 'D', 35748, 35.1, 'Won', False),
            ('Jay Walker', 'D', 32571, 32.0, 'Won', False),
            ('Kris Valderrama', 'D', 31146, 30.6, 'Won', False)],
    43858: [('Veronica Turner', 'D', 30577, 33.8, 'Won', True),
            ('Jamila Woods', 'D', 29351, 32.4, 'Won', False),
            ('Kris Valderrama', 'D', 27086, 29.9, 'Won', True),
            ('JoAnn Fisher', 'R', 3442, 3.8, 'Lost', False)],

    # ── ME ──
    42084: [('Beth O\'Connor', 'R', 1941, 51.4, 'Won', False),
            ('Charles Galemmo', 'D', 1356, 35.9, 'Lost', False),
            ('Noah Cobb', 'I', 482, 12.8, 'Lost', False)],
    42083: [('Beth O\'Connor', 'R', 3269, 62.6, 'Won', True),
            ('Charles Galemmo', 'D', 1954, 37.4, 'Lost', False)],

    # ── MI ──
    47751: [('Brad Paquette', 'R', 20596, 61.3, 'Won', False),
            ('Dean Hill', 'D', 12978, 38.7, 'Lost', False)],
    47747: [('Brad Paquette', 'R', 28485, 62.7, 'Won', True),
            ('Dan VandenHeede', 'D', 16307, 35.9, 'Lost', False),
            ('Andrew Warner', 'NL', 628, 1.4, 'Lost', False)],
    47744: [('Gina Johnsen', 'R', 25765, 65.6, 'Won', False),
            ('Leah Groves', 'D', 13533, 34.4, 'Lost', False)],
    47741: [('Gina Johnsen', 'R', 33508, 68.6, 'Won', True),
            ('Christine Terpening', 'D', 15344, 31.4, 'Lost', False)],

    # ── MN ──
    49121: [('Kurt Daudt', 'R', 13249, 73.2, 'Won', False),
            ('Brad Brown', 'D', 4815, 26.6, 'Lost', False)],
    48443: [('Chris Eaton', 'D', 24291, 71.5, 'Won', True),
            ('Robert Marvin', 'R', 9647, 28.4, 'Lost', False)],

    # ── MO ──
    50731: [('Martha Stevens', 'D', 11548, 64.9, 'Won', False),
            ('Cathy Richards', 'R', 5954, 33.5, 'Lost', False),
            ('William Hastings', 'G', 288, 1.6, 'Lost', False)],
    50729: [('Martha Stevens', 'D', 16043, 100.0, 'Won', True)],
    50727: [('David T. Smith', 'D', 7549, 100.0, 'Won', False)],
    50725: [('David T. Smith', 'D', 11343, 100.0, 'Won', True)],
    50216: [('Tim Remole', 'R', 10811, 75.3, 'Won', True),
            ('Mitch Wrenn', 'D', 3542, 24.7, 'Lost', False)],

    # ── MS ──
    52699: [('Thomas Reynolds II', 'D', 7865, 100.0, 'Won', False)],
    52914: [('Lee Yancey', 'R', 7619, 77.4, 'Won', False),
            ('Jason McCarty', 'D', 2230, 22.6, 'Lost', False)],
    52912: [('Lee Yancey', 'R', 7326, 100.0, 'Won', True)],

    # ── MT ──
    53316: [('Doug Kary', 'R', 5120, 60.5, 'Won', True),
            ('Jennifer Merecki', 'D', 3349, 39.5, 'Lost', False)],
    53313: [('Daniel Zolnikov', 'R', 4853, 65.7, 'Won', False),
            ('Terry Dennis', 'D', 2529, 34.3, 'Lost', False)],
    53450: [('Terry Gauthier', 'R', 8504, 60.5, 'Won', False),
            ('Catherine Scott', 'D', 5560, 39.5, 'Lost', False)],

    # ── NC ──
    63579: [('Evelyn Terry', 'D', 18242, 72.7, 'Won', True),
            ('Scott Arnold', 'R', 6861, 27.3, 'Lost', False)],
    63577: [('Evelyn Terry', 'D', 28471, 100.0, 'Won', True)],

    # ── ND ──
    64328: [('Karen Krebsbach', 'R', 4255, 96.7, 'Won', True)],

    # ── NE (nonpartisan unicameral) ──
    55109: [('Timothy J. Gragert', 'NP', 7222, 51.6, 'Won', False),
            ('Keith Kube', 'NP', 6767, 48.4, 'Lost', False)],

    # ── NH ──
    57829: [('Eric Gallager', 'D', 1085, 98.8, 'Won', False)],
    57827: [('Eric Gallager', 'D', 1437, 99.9, 'Won', True)],
    58871: [('Judy Aron', 'R', 1483, 62.3, 'Won', False),
            ('Bruce Cragin', 'D', 894, 37.6, 'Lost', False)],
    58868: [('Judy Aron', 'R', 1806, 61.0, 'Won', True),
            ('Claudia Istel', 'D', 1154, 39.0, 'Lost', False)],

    # ── NV ──
    55482: [('Chris Edwards', 'R', 18328, 100.0, 'Won', False)],
    55480: [('Annie Black', 'R', 25599, 100.0, 'Won', False)],
    55478: [('Thaddeus Yurek', 'R', 26274, 100.0, 'Won', False)],
    55477: [('Thaddeus Yurek', 'R', 34907, 100.0, 'Won', True)],

    # ── NY ──
    62320: [('Stephen Hawley', 'R', 36150, 91.5, 'Won', True),
            ('Mark Glogowski', 'L', 3291, 8.3, 'Lost', False)],
    62319: [('Stephen Hawley', 'R', 48134, 91.3, 'Won', True),
            ('Mark Glogowski', 'L', 4506, 8.5, 'Lost', False)],
    62318: [('Stephen Hawley', 'R', 38071, 76.9, 'Won', True),
            ('Jennifer Keys', 'D', 11428, 23.1, 'Lost', False)],
    62317: [('Stephen Hawley', 'R', 50330, 99.6, 'Won', True)],
    61597: [('N. Nick Perry', 'D', 33491, 99.9, 'Won', True)],
    61596: [('N. Nick Perry', 'D', 43737, 99.9, 'Won', True)],

    # ── OH ──
    66214: [('Steven Arndt', 'R', 31172, 65.0, 'Won', False),
            ('Joe Helle', 'D', 16816, 35.0, 'Lost', False)],

    # ── OR ──
    68213: [('James Hieb', 'R', 23563, 66.3, 'Won', False),
            ('Walt Trandum', 'D', 11989, 33.7, 'Lost', False)],
    67368: [('Kim Thatcher', 'R', 34990, 56.1, 'Won', True),
            ('Sarah Grider', 'D', 27402, 43.9, 'Lost', False)],
    67365: [('Aaron Woods', 'D', 37521, 58.1, 'Won', False),
            ('John Velez', 'R', 27067, 41.9, 'Lost', False)],

    # ── PA ──
    69098: [('Perry Warren', 'D', 20597, 60.0, 'Won', True),
            ('Ryan Gallagher', 'R', 13731, 40.0, 'Lost', False)],
    69095: [('Perry Warren', 'D', 26269, 59.7, 'Won', True),
            ('Charles Adcock', 'R', 17748, 40.3, 'Lost', False)],
    69092: [('Perry Warren', 'D', 24205, 60.7, 'Won', True),
            ('Bernie Sauer', 'R', 15674, 39.3, 'Lost', False)],
    69089: [('Perry Warren', 'D', 27842, 58.9, 'Won', True),
            ('Bernie Sauer', 'R', 19472, 41.1, 'Lost', False)],
    68641: [('Mario Scavello', 'R', 54482, 55.4, 'Won', True),
            ('Tarah Probst', 'D', 42434, 43.1, 'Lost', False),
            ('Adam Reinhardt', 'L', 1280, 1.3, 'Lost', False)],
    68638: [('Rosemary Brown', 'R', 53769, 55.2, 'Won', False),
            ('Jennifer Shukaitis', 'D', 43583, 44.8, 'Lost', False)],

    # ── RI ──
    72210: [('Deborah Fellela', 'D', 3793, 94.5, 'Won', True)],
    72207: [('Deborah Fellela', 'D', 4053, 58.7, 'Won', True),
            ('Nicola Grasso', 'R', 2855, 41.3, 'Lost', False)],
    72204: [('Deborah Fellela', 'D', 2636, 51.9, 'Won', True),
            ('Nicola Grasso', 'R', 2441, 48.1, 'Lost', False)],
    72201: [('Deborah Fellela', 'D', 3894, 53.8, 'Won', True),
            ('Nicola Grasso', 'R', 3348, 46.2, 'Lost', False)],

    # ── SC ──
    72945: [('Mike Burns', 'R', 11603, 76.1, 'Won', True),
            ('Judi Buckley', 'D', 3651, 23.9, 'Lost', False)],
    72944: [('Mike Burns', 'R', 19274, 98.0, 'Won', True)],
    72943: [('Mike Burns', 'R', 14131, 98.4, 'Won', True)],
    72941: [('Mike Burns', 'R', 20078, 99.1, 'Won', True)],
    73123: [('Steven Long', 'R', 8729, 97.4, 'Won', True)],
    73122: [('Steven Long', 'R', 15691, 97.0, 'Won', True)],
    73121: [('Steven Long', 'R', 9375, 97.8, 'Won', True)],
    73120: [('Steven Long', 'R', 15208, 98.4, 'Won', True)],
    73474: [('Ivory Thigpen', 'D', 13364, 88.2, 'Won', True),
            ('Victor Kocher', 'L', 1788, 11.8, 'Lost', False)],
    73473: [('Ivory Thigpen', 'D', 18126, 87.4, 'Won', True),
            ('Victor Kocher', 'L', 2612, 12.6, 'Lost', False)],
    73472: [('Ivory Thigpen', 'D', 10115, 74.6, 'Won', True),
            ('Melissa McFadden', 'R', 3443, 25.4, 'Lost', False)],
    73470: [('Hamilton Grant', 'D', 14855, 75.4, 'Won', False),
            ('Rebecca Madsen', 'R', 4848, 24.6, 'Lost', False)],

    # ── SD (multi-member at-large, 2 seats) ──
    74263: [('Hugh Bartels', 'R', None, None, 'Won', False),
            ('Nancy York', 'R', None, None, 'Won', False),
            ('Brett Ries', 'D', 3497, 22.1, 'Lost', False),
            ('Diana Hane', 'D', 1661, 10.5, 'Lost', False)],
    74261: [('Hugh Bartels', 'R', 7314, 54.2, 'Won', True),
            ('Nancy York', 'R', 6179, 45.8, 'Won', True)],

    # ── TN ──
    75884: [('Larry Miller', 'D', 13584, 100.0, 'Won', True)],
    75882: [('Larry Miller', 'D', 17615, 100.0, 'Won', True)],
    75880: [('Larry Miller', 'D', 9178, 100.0, 'Won', True)],
    75877: [('Larry Miller', 'D', 14829, 70.8, 'Won', True),
            ('Larry Hunter', 'R', 5903, 28.2, 'Lost', False)],

    # ── TX ──
    76412: [('Chris Paddie', 'R', 45918, 100.0, 'Won', True)],
    76410: [('Chris Paddie', 'R', 62151, 100.0, 'Won', True)],
    77519: [('Craig Goldman', 'R', 35187, 53.2, 'Won', True),
            ('Beth Llewellyn McLaughlin', 'D', 29693, 44.9, 'Lost', False),
            ('Rod Wingo', 'L', 1245, 1.9, 'Lost', False)],
    77515: [('Craig Goldman', 'R', 43798, 52.5, 'Won', True),
            ('Elizabeth Beck', 'D', 37718, 45.2, 'Lost', False),
            ('Rod Wingo', 'L', 1927, 2.3, 'Lost', False)],
    77512: [('Craig Goldman', 'R', 37423, 58.2, 'Won', True),
            ('Laurin McLaurin', 'D', 26906, 41.8, 'Lost', False)],
    77507: [('John McQueeney', 'R', 51467, 58.1, 'Won', False),
            ('Carlos Walker', 'D', 37097, 41.9, 'Lost', False)],

    # ── UT ──
    78554: [('Kelly Miles', 'R', 7532, 54.5, 'Won', False),
            ('Jason Allen', 'D', 6286, 45.5, 'Lost', False)],
    78552: [('Kelly Miles', 'R', 10999, 60.3, 'Won', True),
            ('Jason Allen', 'D', 7235, 39.7, 'Lost', False)],

    # ── WI ──
    83636: [('Daniel Riemer', 'D', 15440, 79.6, 'Won', True),
            ('Matthew Bughman', 'L', 3960, 20.4, 'Lost', False)],
    83634: [('Daniel Riemer', 'D', 19430, 97.1, 'Won', True)],
    83631: [('Daniel Riemer', 'D', 12491, 61.9, 'Won', True),
            ('Zachary Marshall', 'R', 7694, 38.1, 'Lost', False)],

    # ── WV (22: multi-member pre-2022, single-member post-2022) ──
    82497: [('Zack Maynard', 'R', None, None, 'Won', False),
            ('Joe Jeffries', 'R', None, None, 'Won', False),
            ('Gary McCallister', 'D', 3062, 17.9, 'Lost', False),
            ('Jeff Eldridge', 'I', 2600, 15.2, 'Lost', False),
            ('Bill Bryant', 'D', 2002, 11.7, 'Lost', False)],
    82494: [('Zack Maynard', 'R', None, None, 'Won', True),
            ('Joe Jeffries', 'R', None, None, 'Won', True),
            ('Jeff Eldridge', 'D', 5005, 24.8, 'Lost', False),
            ('Cecil Silva', 'D', 1896, 9.4, 'Lost', False)],
    82492: [('Daniel Linville', 'R', 3495, 100.0, 'Won', False)],
    82490: [('Daniel Linville', 'R', 6091, 100.0, 'Won', True)],
    83022: [('S. Marshall Wilson', 'R', 5152, 100.0, 'Won', False)],
    83018: [('Don Forsht', 'R', 6157, 66.9, 'Won', False),
            ('Brad Noll', 'D', 2560, 27.8, 'Lost', False),
            ('Mary Kinnie', 'MP', 491, 5.3, 'Lost', False)],
    83015: [('Dana Ferrell', 'R', 3152, 69.0, 'Won', False),
            ('David Holmes', 'D', 1418, 31.0, 'Lost', False)],
    83013: [('Dana Ferrell', 'R', 6346, 100.0, 'Won', True)],
}


def main():
    parser = argparse.ArgumentParser(description='Backfill missing candidacies')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    args = parser.parse_args()

    # Step 1: Collect unique candidate names
    all_names = set()
    for eid, candidates in DATA.items():
        for c in candidates:
            all_names.add(c[0])

    print(f'Elections to backfill: {len(DATA)}')
    total_candidacies = sum(len(v) for v in DATA.values())
    print(f'Total candidacies to insert: {total_candidacies}')
    print(f'Unique candidate names: {len(all_names)}')

    # Step 2: Check which candidates already exist
    # Query in batches of 50 names to avoid SQL length issues
    existing_map = {}
    name_list = sorted(all_names)
    for i in range(0, len(name_list), 50):
        batch = name_list[i:i+50]
        names_sql = ','.join(f"'{esc(n)}'" for n in batch)
        result = run_sql(f"SELECT id, full_name FROM candidates WHERE full_name IN ({names_sql})")
        if result:
            for r in result:
                existing_map[r['full_name']] = r['id']
        time.sleep(1)

    missing = sorted(all_names - set(existing_map.keys()))
    print(f'\nExisting candidates found: {len(existing_map)}')
    print(f'New candidates to create: {len(missing)}')

    if missing:
        if args.dry_run:
            for name in missing:
                print(f'  [NEW] {name}')
        else:
            # Insert in batches of 30
            for i in range(0, len(missing), 30):
                batch = missing[i:i+30]
                values = ','.join(f"('{esc(n)}')" for n in batch)
                result = run_sql(
                    f"INSERT INTO candidates (full_name) VALUES {values} RETURNING id, full_name"
                )
                if result:
                    for r in result:
                        existing_map[r['full_name']] = r['id']
                    print(f'  Inserted {len(result)} candidates (batch {i//30+1})')
                else:
                    print(f'  FAILED to insert candidate batch {i//30+1}')
                    return
                time.sleep(2)

    # Step 3: Verify all candidate IDs are available
    missing_ids = []
    for eid, candidates in DATA.items():
        for c in candidates:
            if c[0] not in existing_map:
                missing_ids.append((eid, c[0]))
    if missing_ids:
        print(f'\nERROR: Missing candidate IDs for:')
        for eid, name in missing_ids:
            print(f'  election {eid}: {name}')
        return

    # Step 4: Build and insert candidacies
    print(f'\nInserting candidacies...')
    all_values = []
    for eid, candidates in sorted(DATA.items()):
        for name, party, votes, pct, result, is_inc in candidates:
            cid = existing_map[name]
            v = str(votes) if votes is not None else 'NULL'
            p = str(pct) if pct is not None else 'NULL'
            inc = 'true' if is_inc else 'false'
            all_values.append(
                f"({eid}, {cid}, '{esc(party)}', {v}, {p}, '{esc(result)}', {inc}, false)"
            )

    if args.dry_run:
        print(f'[DRY RUN] Would insert {len(all_values)} candidacies')
        for v in all_values[:10]:
            print(f'  {v}')
        if len(all_values) > 10:
            print(f'  ... and {len(all_values)-10} more')
        return

    # Insert in batches of 30
    total_inserted = 0
    for i in range(0, len(all_values), 30):
        chunk = all_values[i:i+30]
        sql = (
            "INSERT INTO candidacies "
            "(election_id, candidate_id, party, votes_received, vote_percentage, "
            "result, is_incumbent, is_write_in) VALUES " + ','.join(chunk)
        )
        result = run_sql(sql)
        if result is not None:
            total_inserted += len(chunk)
            print(f'  Batch {i//30+1}: inserted {len(chunk)} candidacies (total: {total_inserted})')
        else:
            print(f'  FAILED batch {i//30+1} — stopping')
            break
        time.sleep(2)

    print(f'\nDone! Inserted {total_inserted} candidacies across {len(DATA)} elections.')
    if total_inserted < len(all_values):
        print(f'WARNING: {len(all_values) - total_inserted} candidacies failed to insert.')


if __name__ == '__main__':
    main()
