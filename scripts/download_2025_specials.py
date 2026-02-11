"""
Download 2025 special election results from Ballotpedia.

Downloads individual district pages for all 93 state legislative special elections
held in 2025, parses votebox HTML, and outputs JSON for populate_2025_specials.py.

Usage:
    python3 scripts/download_2025_specials.py
    python3 scripts/download_2025_specials.py --state VA
    python3 scripts/download_2025_specials.py --state MS
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx

CACHE_DIR = '/tmp/bp_2025_specials'
OUTPUT_PATH = '/tmp/2025_special_results.json'

PARTY_MAP = {
    'D': 'D', 'R': 'R', 'I': 'I', 'L': 'L', 'G': 'G',
}

# ══════════════════════════════════════════════════════════════════════
# COMPLETE LIST OF 2025 SPECIAL ELECTIONS (93 races, excludes PR + canceled MN 40B original)
# ══════════════════════════════════════════════════════════════════════
# Fields:
#   state, chamber, district, bp_district (for URL),
#   former_incumbent, vacancy_reason,
#   general_date, primary_date, runoff_date
#
# chamber values: 'Senate', 'House', 'Assembly' (for CA/NY)
# LA uses jungle primary format (primary_date is actually the jungle primary date)

SPECIAL_ELECTIONS = [
    # ── ALABAMA ──
    {'state': 'AL', 'chamber': 'House', 'district': '11', 'bp_district': '11',
     'former_incumbent': 'Randall Shedd', 'vacancy_reason': 'resigned',
     'general_date': '2025-08-26', 'primary_date': '2025-05-13', 'runoff_date': '2025-06-10'},
    {'state': 'AL', 'chamber': 'House', 'district': '12', 'bp_district': '12',
     'former_incumbent': 'Corey Harbison', 'vacancy_reason': 'resigned',
     'general_date': '2025-10-28', 'primary_date': '2025-07-15', 'runoff_date': '2025-08-12'},
    {'state': 'AL', 'chamber': 'House', 'district': '13', 'bp_district': '13',
     'former_incumbent': 'Matt Woods', 'vacancy_reason': 'resigned',
     'general_date': '2026-01-13', 'primary_date': '2025-09-30', 'runoff_date': '2025-10-28'},
    {'state': 'AL', 'chamber': 'Senate', 'district': '5', 'bp_district': '5',
     'former_incumbent': 'Greg Reed', 'vacancy_reason': 'resigned',
     'general_date': '2025-06-24', 'primary_date': '2025-03-11', 'runoff_date': '2025-04-08'},

    # ── CALIFORNIA ──
    {'state': 'CA', 'chamber': 'Assembly', 'district': '32', 'bp_district': '32',
     'former_incumbent': 'Vince Fong', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-04-29', 'primary_date': '2025-02-25', 'runoff_date': None},
    {'state': 'CA', 'chamber': 'Assembly', 'district': '63', 'bp_district': '63',
     'former_incumbent': 'Bill Essayli', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-08-26', 'primary_date': '2025-06-24', 'runoff_date': None},
    {'state': 'CA', 'chamber': 'Senate', 'district': '36', 'bp_district': '36',
     'former_incumbent': 'Janet Nguyen', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-04-29', 'primary_date': '2025-02-25', 'runoff_date': None},

    # ── CONNECTICUT ──
    {'state': 'CT', 'chamber': 'House', 'district': '40', 'bp_district': '40',
     'former_incumbent': 'Christine Conley', 'vacancy_reason': 'resigned',
     'general_date': '2025-02-25', 'primary_date': None, 'runoff_date': None},
    {'state': 'CT', 'chamber': 'House', 'district': '113', 'bp_district': '113',
     'former_incumbent': 'Jason Perillo', 'vacancy_reason': 'resigned',
     'general_date': '2025-04-22', 'primary_date': None, 'runoff_date': None},
    {'state': 'CT', 'chamber': 'Senate', 'district': '21', 'bp_district': '21',
     'former_incumbent': 'Kevin Kelly', 'vacancy_reason': 'resigned',
     'general_date': '2025-02-25', 'primary_date': None, 'runoff_date': None},

    # ── DELAWARE ──
    {'state': 'DE', 'chamber': 'House', 'district': '20', 'bp_district': '20',
     'former_incumbent': 'Stell Selby', 'vacancy_reason': 'resigned',
     'general_date': '2025-08-05', 'primary_date': None, 'runoff_date': None},
    {'state': 'DE', 'chamber': 'Senate', 'district': '1', 'bp_district': '1',
     'former_incumbent': 'Sarah McBride', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-02-15', 'primary_date': None, 'runoff_date': None},
    {'state': 'DE', 'chamber': 'Senate', 'district': '5', 'bp_district': '5',
     'former_incumbent': 'Kyle Evans Gay', 'vacancy_reason': 'resigned',
     'general_date': '2025-02-15', 'primary_date': None, 'runoff_date': None},

    # ── FLORIDA ──
    {'state': 'FL', 'chamber': 'House', 'district': '3', 'bp_district': '3',
     'former_incumbent': 'Joel Rudman', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-06-10', 'primary_date': '2025-04-01', 'runoff_date': None},
    {'state': 'FL', 'chamber': 'House', 'district': '32', 'bp_district': '32',
     'former_incumbent': 'Debbie Mayfield', 'vacancy_reason': 'resigned',
     'general_date': '2025-06-10', 'primary_date': '2025-04-01', 'runoff_date': None},
    {'state': 'FL', 'chamber': 'House', 'district': '40', 'bp_district': '40',
     'former_incumbent': 'LaVon Bracy Davis', 'vacancy_reason': 'resigned',
     'general_date': '2025-09-02', 'primary_date': '2025-06-24', 'runoff_date': None},
    {'state': 'FL', 'chamber': 'House', 'district': '90', 'bp_district': '90',
     'former_incumbent': 'Joseph Casello', 'vacancy_reason': 'resigned',
     'general_date': '2025-12-09', 'primary_date': '2025-09-30', 'runoff_date': None},
    {'state': 'FL', 'chamber': 'Senate', 'district': '11', 'bp_district': '11',
     'former_incumbent': 'Blaise Ingoglia', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-12-09', 'primary_date': '2025-09-30', 'runoff_date': None},
    {'state': 'FL', 'chamber': 'Senate', 'district': '15', 'bp_district': '15',
     'former_incumbent': 'Geraldine Thompson', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-09-02', 'primary_date': '2025-06-24', 'runoff_date': None},
    {'state': 'FL', 'chamber': 'Senate', 'district': '19', 'bp_district': '19',
     'former_incumbent': 'Randy Fine', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-06-10', 'primary_date': '2025-04-01', 'runoff_date': None},

    # ── GEORGIA ──
    {'state': 'GA', 'chamber': 'House', 'district': '23', 'bp_district': '23',
     'former_incumbent': 'Mandi Ballinger', 'vacancy_reason': 'resigned',
     'general_date': '2025-12-09', 'primary_date': None, 'runoff_date': '2026-01-06'},
    {'state': 'GA', 'chamber': 'House', 'district': '106', 'bp_district': '106',
     'former_incumbent': 'Shelly Hutchinson', 'vacancy_reason': 'resigned',
     'general_date': '2025-11-04', 'primary_date': None, 'runoff_date': '2025-12-02'},
    {'state': 'GA', 'chamber': 'House', 'district': '121', 'bp_district': '121',
     'former_incumbent': 'Marcus Wiedower', 'vacancy_reason': 'resigned',
     'general_date': '2025-12-09', 'primary_date': None, 'runoff_date': '2026-01-06'},
    {'state': 'GA', 'chamber': 'Senate', 'district': '21', 'bp_district': '21',
     'former_incumbent': 'Brandon Beach', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-08-26', 'primary_date': None, 'runoff_date': '2025-09-23'},
    {'state': 'GA', 'chamber': 'Senate', 'district': '35', 'bp_district': '35',
     'former_incumbent': 'Jason Esteves', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-18', 'primary_date': None, 'runoff_date': '2025-12-16'},

    # ── IOWA ──
    {'state': 'IA', 'chamber': 'House', 'district': '7', 'bp_district': '7',
     'former_incumbent': 'Mike Sexton', 'vacancy_reason': 'resigned',
     'general_date': '2025-12-09', 'primary_date': None, 'runoff_date': None},
    {'state': 'IA', 'chamber': 'House', 'district': '78', 'bp_district': '78',
     'former_incumbent': 'Sami Scheetz', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-04-29', 'primary_date': None, 'runoff_date': None},
    {'state': 'IA', 'chamber': 'House', 'district': '100', 'bp_district': '100',
     'former_incumbent': 'Martin Graber', 'vacancy_reason': 'resigned',
     'general_date': '2025-03-11', 'primary_date': None, 'runoff_date': None},
    {'state': 'IA', 'chamber': 'Senate', 'district': '1', 'bp_district': '1',
     'former_incumbent': 'Rocky De Witt', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-08-26', 'primary_date': None, 'runoff_date': None},
    {'state': 'IA', 'chamber': 'Senate', 'district': '16', 'bp_district': '16',
     'former_incumbent': 'Claire Celsi', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-12-30', 'primary_date': None, 'runoff_date': None},
    {'state': 'IA', 'chamber': 'Senate', 'district': '35', 'bp_district': '35',
     'former_incumbent': 'Chris Cournoyer', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-01-28', 'primary_date': None, 'runoff_date': None},

    # ── KENTUCKY ──
    {'state': 'KY', 'chamber': 'Senate', 'district': '37', 'bp_district': '37',
     'former_incumbent': 'David Yates', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-12-16', 'primary_date': None, 'runoff_date': None},

    # ── LOUISIANA ──
    # LA uses jungle primary (nonpartisan blanket primary); what BP calls "primary" is our Special election
    {'state': 'LA', 'chamber': 'House', 'district': '45', 'bp_district': '45',
     'former_incumbent': 'Brach Myers', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-05-03', 'primary_date': None, 'runoff_date': '2025-06-07'},
    {'state': 'LA', 'chamber': 'House', 'district': '67', 'bp_district': '67',
     'former_incumbent': 'Larry Selders', 'vacancy_reason': 'resigned',
     'general_date': '2025-05-03', 'primary_date': None, 'runoff_date': '2025-06-07'},
    {'state': 'LA', 'chamber': 'Senate', 'district': '14', 'bp_district': '14',
     'former_incumbent': 'Cleo Fields', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-02-15', 'primary_date': None, 'runoff_date': '2025-03-29'},
    {'state': 'LA', 'chamber': 'Senate', 'district': '23', 'bp_district': '23',
     'former_incumbent': 'Jean-Paul Coussan', 'vacancy_reason': 'resigned',
     'general_date': '2025-02-15', 'primary_date': None, 'runoff_date': '2025-03-29'},

    # ── MAINE ──
    {'state': 'ME', 'chamber': 'House', 'district': '24', 'bp_district': '24',
     'former_incumbent': 'Joseph Perry', 'vacancy_reason': 'resigned',
     'general_date': '2025-02-25', 'primary_date': None, 'runoff_date': None},

    # ── MASSACHUSETTS ──
    {'state': 'MA', 'chamber': 'House', 'district': '3rd Bristol', 'bp_district': '3rd_Bristol',
     'former_incumbent': 'Carol Doherty', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-06-10', 'primary_date': '2025-05-13', 'runoff_date': None},
    {'state': 'MA', 'chamber': 'House', 'district': '6th Essex', 'bp_district': '6th_Essex',
     'former_incumbent': 'Jerry Parisella', 'vacancy_reason': 'resigned',
     'general_date': '2025-05-13', 'primary_date': '2025-04-15', 'runoff_date': None},

    # ── MINNESOTA ──
    # MN 40B original was canceled; rescheduled version is the one we track
    {'state': 'MN', 'chamber': 'House', 'district': '34B', 'bp_district': '34B',
     'former_incumbent': 'Melissa Hortman', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-09-16', 'primary_date': '2025-08-12', 'runoff_date': None},
    {'state': 'MN', 'chamber': 'House', 'district': '40B', 'bp_district': '40B',
     'former_incumbent': 'Jamie Becker-Finn', 'vacancy_reason': 'resigned',
     'general_date': '2025-03-11', 'primary_date': '2025-02-25', 'runoff_date': None},
    {'state': 'MN', 'chamber': 'Senate', 'district': '6', 'bp_district': '6',
     'former_incumbent': 'Justin Eichorn', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-04-29', 'primary_date': '2025-04-15', 'runoff_date': None},
    {'state': 'MN', 'chamber': 'Senate', 'district': '29', 'bp_district': '29',
     'former_incumbent': 'Bruce Anderson', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-26', 'runoff_date': None},
    {'state': 'MN', 'chamber': 'Senate', 'district': '47', 'bp_district': '47',
     'former_incumbent': 'Nicole Mitchell', 'vacancy_reason': 'resigned',
     'general_date': '2025-11-04', 'primary_date': '2025-08-26', 'runoff_date': None},
    {'state': 'MN', 'chamber': 'Senate', 'district': '60', 'bp_district': '60',
     'former_incumbent': 'Kari Dziedzic', 'vacancy_reason': 'died',
     'general_date': '2025-01-28', 'primary_date': '2025-01-14', 'runoff_date': None},

    # ── MISSISSIPPI ──
    {'state': 'MS', 'chamber': 'House', 'district': '16', 'bp_district': '16',
     'former_incumbent': 'Rickey Thompson', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'House', 'district': '22', 'bp_district': '22',
     'former_incumbent': 'Jon Lancaster', 'vacancy_reason': 'resigned',
     'general_date': '2025-03-25', 'primary_date': None, 'runoff_date': '2025-04-22'},
    {'state': 'MS', 'chamber': 'House', 'district': '23', 'bp_district': '23',
     'former_incumbent': 'Andrew Stepp', 'vacancy_reason': 'resigned',
     'general_date': '2025-03-25', 'primary_date': None, 'runoff_date': '2025-04-22'},
    {'state': 'MS', 'chamber': 'House', 'district': '26', 'bp_district': '26',
     'former_incumbent': 'Orlando Paden', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': None, 'runoff_date': '2025-12-02'},
    {'state': 'MS', 'chamber': 'House', 'district': '36', 'bp_district': '36',
     'former_incumbent': 'Karl Gibbs', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'House', 'district': '39', 'bp_district': '39',
     'former_incumbent': 'Dana Underwood McLean', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'House', 'district': '41', 'bp_district': '41',
     'former_incumbent': 'Kabir Karriem', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'House', 'district': '82', 'bp_district': '82',
     'former_incumbent': 'Charles Young', 'vacancy_reason': 'resigned',
     'general_date': '2025-03-25', 'primary_date': None, 'runoff_date': '2025-04-22'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '1', 'bp_district': '1',
     'former_incumbent': 'Michael McLendon', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '2', 'bp_district': '2',
     'former_incumbent': 'David L. Parker', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '11', 'bp_district': '11',
     'former_incumbent': 'Reginald Jackson', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '18', 'bp_district': '18',
     'former_incumbent': 'Jenifer Branning', 'vacancy_reason': 'resigned',
     'general_date': '2025-04-15', 'primary_date': None, 'runoff_date': None},
    {'state': 'MS', 'chamber': 'Senate', 'district': '19', 'bp_district': '19',
     'former_incumbent': 'Kevin Blackwell', 'vacancy_reason': 'resigned',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '24', 'bp_district': '24',
     'former_incumbent': 'David Jordan', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': None, 'runoff_date': '2025-12-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '26', 'bp_district': '26',
     'former_incumbent': 'John Horhn', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': None, 'runoff_date': '2025-12-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '34', 'bp_district': '34',
     'former_incumbent': 'Juan Barnett', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '41', 'bp_district': '41',
     'former_incumbent': 'Joey Fillingane', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '42', 'bp_district': '42',
     'former_incumbent': 'Robin Robinson', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '44', 'bp_district': '44',
     'former_incumbent': 'John Polk', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},
    {'state': 'MS', 'chamber': 'Senate', 'district': '45', 'bp_district': '45',
     'former_incumbent': 'Chris Johnson', 'vacancy_reason': 'died',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': '2025-09-02'},

    # ── NEW HAMPSHIRE ──
    {'state': 'NH', 'chamber': 'House', 'district': 'Coos-5', 'bp_district': 'Coos_County_District_5',
     'former_incumbent': 'Brian Valerino', 'vacancy_reason': 'resigned',
     'general_date': '2025-11-04', 'primary_date': '2025-09-16', 'runoff_date': None},
    {'state': 'NH', 'chamber': 'House', 'district': 'Strafford-12', 'bp_district': 'Strafford_County_District_12',
     'former_incumbent': 'Gerri Cannon', 'vacancy_reason': 'resigned',
     'general_date': '2025-06-24', 'primary_date': '2025-05-06', 'runoff_date': None},

    # ── NEW JERSEY ──
    {'state': 'NJ', 'chamber': 'Senate', 'district': '35', 'bp_district': '35',
     'former_incumbent': 'Nellie Pou', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-06-10', 'runoff_date': None},

    # ── NEW YORK ──
    {'state': 'NY', 'chamber': 'Assembly', 'district': '115', 'bp_district': '115',
     'former_incumbent': 'D. Billy Jones', 'vacancy_reason': 'resigned',
     'general_date': '2025-11-04', 'primary_date': None, 'runoff_date': None},
    {'state': 'NY', 'chamber': 'Senate', 'district': '22', 'bp_district': '22',
     'former_incumbent': 'Simcha Felder', 'vacancy_reason': 'resigned',
     'general_date': '2025-05-20', 'primary_date': None, 'runoff_date': None},

    # ── OKLAHOMA ──
    {'state': 'OK', 'chamber': 'House', 'district': '71', 'bp_district': '71',
     'former_incumbent': 'Amanda Swope', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-06-10', 'primary_date': '2025-04-01', 'runoff_date': '2025-05-13'},
    {'state': 'OK', 'chamber': 'House', 'district': '74', 'bp_district': '74',
     'former_incumbent': 'Mark Vancuren', 'vacancy_reason': 'resigned',
     'general_date': '2025-06-10', 'primary_date': '2025-04-01', 'runoff_date': '2025-05-13'},
    {'state': 'OK', 'chamber': 'House', 'district': '97', 'bp_district': '97',
     'former_incumbent': 'Jason Lowe', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-09-09', 'primary_date': '2025-06-10', 'runoff_date': '2025-08-12'},
    {'state': 'OK', 'chamber': 'Senate', 'district': '8', 'bp_district': '8',
     'former_incumbent': 'Roger Thompson', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-05-13', 'primary_date': '2025-03-04', 'runoff_date': None},

    # ── PENNSYLVANIA ──
    {'state': 'PA', 'chamber': 'House', 'district': '35', 'bp_district': '35',
     'former_incumbent': 'Matthew Gergely', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-03-25', 'primary_date': None, 'runoff_date': None},
    {'state': 'PA', 'chamber': 'Senate', 'district': '36', 'bp_district': '36',
     'former_incumbent': 'Ryan Aument', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-03-25', 'primary_date': None, 'runoff_date': None},

    # ── RHODE ISLAND ──
    {'state': 'RI', 'chamber': 'Senate', 'district': '4', 'bp_district': '4',
     'former_incumbent': 'Dominick Ruggerio', 'vacancy_reason': 'resigned',
     'general_date': '2025-08-05', 'primary_date': '2025-07-08', 'runoff_date': None},

    # ── SOUTH CAROLINA ──
    {'state': 'SC', 'chamber': 'House', 'district': '21', 'bp_district': '21',
     'former_incumbent': 'Bobby Cox', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-12-23', 'primary_date': '2025-10-21', 'runoff_date': '2025-11-04'},
    {'state': 'SC', 'chamber': 'House', 'district': '50', 'bp_district': '50',
     'former_incumbent': 'Will Wheeler', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-06-03', 'primary_date': '2025-04-01', 'runoff_date': None},
    {'state': 'SC', 'chamber': 'House', 'district': '88', 'bp_district': '88',
     'former_incumbent': 'RJ May', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-12-23', 'primary_date': '2025-10-21', 'runoff_date': '2025-11-04'},
    {'state': 'SC', 'chamber': 'House', 'district': '113', 'bp_district': '113',
     'former_incumbent': 'Marvin Pendarvis', 'vacancy_reason': 'resigned',
     'general_date': '2025-03-25', 'primary_date': '2025-01-21', 'runoff_date': None},
    {'state': 'SC', 'chamber': 'Senate', 'district': '12', 'bp_district': '12',
     'former_incumbent': 'Roger Nutt', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-12-23', 'primary_date': '2025-10-21', 'runoff_date': '2025-11-04'},

    # ── TEXAS ──
    {'state': 'TX', 'chamber': 'Senate', 'district': '9', 'bp_district': '9',
     'former_incumbent': 'Kelly Hancock', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': None, 'runoff_date': '2026-01-31'},

    # ── VIRGINIA ──
    {'state': 'VA', 'chamber': 'House of Delegates', 'district': '26', 'bp_district': '26',
     'former_incumbent': 'Kannan Srinivasan', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-01-07', 'primary_date': None, 'runoff_date': None},
    {'state': 'VA', 'chamber': 'Senate', 'district': '10', 'bp_district': '10',
     'former_incumbent': 'John McGuire', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-01-07', 'primary_date': None, 'runoff_date': None},
    {'state': 'VA', 'chamber': 'Senate', 'district': '32', 'bp_district': '32',
     'former_incumbent': 'Suhas Subramanyam', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-01-07', 'primary_date': None, 'runoff_date': None},

    # ── WASHINGTON ──
    {'state': 'WA', 'chamber': 'House', 'district': '33-Pos1', 'bp_district': '33',
     'former_incumbent': 'Tina Orwall', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'House', 'district': '34-Pos1', 'bp_district': '34',
     'former_incumbent': 'Emily Alvarado', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'House', 'district': '41-Pos1', 'bp_district': '41',
     'former_incumbent': 'Tana Senn', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'House', 'district': '48-Pos1', 'bp_district': '48',
     'former_incumbent': 'Vandana Slatter', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'Senate', 'district': '5', 'bp_district': '5',
     'former_incumbent': 'Bill Ramos', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'Senate', 'district': '26', 'bp_district': '26',
     'former_incumbent': 'Emily Randall', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'Senate', 'district': '33', 'bp_district': '33',
     'former_incumbent': 'Karen Keiser', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'Senate', 'district': '34', 'bp_district': '34',
     'former_incumbent': 'Joe Nguyen', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
    {'state': 'WA', 'chamber': 'Senate', 'district': '48', 'bp_district': '48',
     'former_incumbent': 'Patty Kuderer', 'vacancy_reason': 'appointed_elsewhere',
     'general_date': '2025-11-04', 'primary_date': '2025-08-05', 'runoff_date': None},
]


# ══════════════════════════════════════════════════════════════════════
# STATE-SPECIFIC URL BUILDERS
# ══════════════════════════════════════════════════════════════════════

STATE_NAMES = {
    'AL': 'Alabama', 'CA': 'California', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'IA': 'Iowa', 'KY': 'Kentucky',
    'LA': 'Louisiana', 'ME': 'Maine', 'MA': 'Massachusetts', 'MN': 'Minnesota',
    'MS': 'Mississippi', 'NH': 'New_Hampshire', 'NJ': 'New_Jersey',
    'NY': 'New_York', 'OK': 'Oklahoma', 'PA': 'Pennsylvania',
    'RI': 'Rhode_Island', 'SC': 'South_Carolina', 'TX': 'Texas',
    'VA': 'Virginia', 'WA': 'Washington',
}

# BP URL chamber names (state → chamber → URL fragment)
CHAMBER_URL_NAMES = {
    'AL': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'CA': {'Assembly': 'State_Assembly', 'Senate': 'State_Senate'},
    'CT': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'DE': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'FL': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'GA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'IA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'KY': {'Senate': 'State_Senate'},
    'LA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'ME': {'House': 'House_of_Representatives'},
    'MA': {'House': 'House_of_Representatives'},
    'MN': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'MS': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'NH': {'House': 'House_of_Representatives'},
    'NJ': {'Senate': 'State_Senate'},
    'NY': {'Assembly': 'State_Assembly', 'Senate': 'State_Senate'},
    'OK': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'PA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'RI': {'Senate': 'State_Senate'},
    'SC': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
    'TX': {'Senate': 'State_Senate'},
    'VA': {'House of Delegates': 'House_of_Delegates', 'Senate': 'State_Senate'},
    'WA': {'House': 'House_of_Representatives', 'Senate': 'State_Senate'},
}


def build_bp_url(race):
    """Build Ballotpedia district page URL for a special election race."""
    state = race['state']
    chamber = race['chamber']
    bp_district = race['bp_district']
    state_name = STATE_NAMES[state]

    chamber_url = CHAMBER_URL_NAMES.get(state, {}).get(chamber, chamber)

    if state == 'MA':
        # Massachusetts: Massachusetts_House_of_Representatives_{district_name}_District
        return f'https://ballotpedia.org/{state_name}_{chamber_url}_{bp_district}_District'
    elif state == 'NH':
        # New Hampshire: New_Hampshire_House_of_Representatives_District_{County}_{N}
        # bp_district is like 'Coos_County_District_5' but BP URL uses 'District_Coos_5'
        parts = bp_district.split('_')
        # Expected: 'Coos_County_District_5' -> county='Coos', num='5'
        if 'County' in parts:
            county_idx = parts.index('County')
            county = '_'.join(parts[:county_idx])
            num = parts[-1]
            return f'https://ballotpedia.org/{state_name}_{chamber_url}_District_{county}_{num}'
        return f'https://ballotpedia.org/{state_name}_{chamber_url}_{bp_district}'
    elif state == 'WA':
        # Washington: omit "State" from URL for House; Senate keeps "State"
        if chamber == 'Senate':
            return f'https://ballotpedia.org/Washington_State_Senate_District_{bp_district}'
        else:
            return f'https://ballotpedia.org/Washington_House_of_Representatives_District_{bp_district}'
    else:
        # Standard: {State}_{Chamber}_District_{N}
        return f'https://ballotpedia.org/{state_name}_{chamber_url}_District_{bp_district}'


# ══════════════════════════════════════════════════════════════════════
# DOWNLOAD + PARSE
# ══════════════════════════════════════════════════════════════════════

def download_page(url, cache_key, max_retries=3):
    """Download a Ballotpedia page with caching and 202 retry."""
    cache_path = os.path.join(CACHE_DIR, f'{cache_key}.html')
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            return f.read()

    for attempt in range(max_retries):
        try:
            resp = httpx.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
                follow_redirects=True,
                timeout=30
            )
            if resp.status_code == 200:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(resp.text)
                return resp.text
            elif resp.status_code == 202:
                # CDN warming — wait and retry
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                print(f'    WARNING: HTTP 202 (CDN warming) for {url} after {max_retries} retries')
                return None
            else:
                print(f'    WARNING: HTTP {resp.status_code} for {url}')
                return None
        except Exception as e:
            print(f'    WARNING: Download failed for {url}: {e}')
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None
    return None


def parse_results_table(html_text, election_label, max_winners=1):
    """
    Parse a results_table votebox from Ballotpedia HTML.
    Reused from download_2025_results.py with minor adaptations.
    """
    idx = html_text.find(election_label)
    if idx == -1:
        idx_lower = html_text.lower().find(election_label.lower())
        if idx_lower == -1:
            return None
        idx = idx_lower

    # Check if this election was canceled (label exists but no actual race)
    # Look both before (up to 300 chars) and after the label match
    before_start = max(0, idx - 300)
    check_text = html_text[before_start:idx + 1500].lower()
    if 'canceled' in check_text or 'cancelled' in check_text:
        cancel_idx = check_text.find('canceled')
        if cancel_idx == -1:
            cancel_idx = check_text.find('cancelled')
        # Position of our label within check_text
        label_pos = idx - before_start
        # Only consider 'canceled' if it's close to our label (within ~400 chars)
        if abs(cancel_idx - label_pos) < 400:
            return None

    rt_idx = html_text.find('results_table', idx)
    if rt_idx == -1 or rt_idx > idx + 3000:
        return None

    table_end = html_text.find('</table>', rt_idx)
    if table_end == -1:
        return None

    table_html = html_text[rt_idx:table_end + 10]
    candidates = []

    for row_match in re.finditer(
        r'<tr\s+class="results_row\s*(winner)?\s*"[^>]*>(.*?)</tr>',
        table_html, re.DOTALL
    ):
        is_winner = row_match.group(1) == 'winner'
        row_html = row_match.group(2)

        name_cell = re.search(
            r'class="votebox-results-cell--text"[^>]*>(.*?)</td>',
            row_html, re.DOTALL
        )
        if not name_cell:
            continue

        cell_html = name_cell.group(1)

        if 'Other/Write-in' in cell_html or 'Write-in' in cell_html:
            continue

        is_incumbent = bool(re.search(r'<b><u><a', cell_html))

        name_link = re.search(r'<a\s+href="[^"]*"[^>]*>(.*?)</a>', cell_html)
        if not name_link:
            continue
        name = htmlmod.unescape(name_link.group(1).strip())

        party_match = re.search(r'\(([A-Z])\)', cell_html)
        party = party_match.group(1) if party_match else None
        if party and party in PARTY_MAP:
            party = PARTY_MAP[party]

        pct_match = re.search(r'class="percentage_number">([\d.]+)</div>', row_html)
        vote_pct = float(pct_match.group(1)) if pct_match else None

        votes_matches = re.findall(
            r'class="votebox-results-cell--number">([\d,]+)</td>',
            row_html
        )
        votes = int(votes_matches[-1].replace(',', '')) if votes_matches else None

        candidates.append({
            'name': name,
            'party': party,
            'is_incumbent': is_incumbent,
            'is_winner': is_winner,
            'votes': votes,
            'vote_pct': vote_pct,
        })

    total_match = re.search(
        r'Total\s+votes:\s*([\d,]+)',
        html_text[rt_idx:table_end + 2000]
    )
    total_votes = int(total_match.group(1).replace(',', '')) if total_match else None

    if not candidates:
        return None

    return {
        'candidates': candidates,
        'total_votes': total_votes,
    }


def build_election_labels(race):
    """
    Build label patterns to search for on the Ballotpedia page.
    Returns dict of {election_type: [label_patterns_to_try]}.
    """
    state = race['state']
    state_name = STATE_NAMES[state].replace('_', ' ')
    chamber = race['chamber']
    bp_district = race['bp_district'].replace('_', ' ')

    # Build the chamber name as BP uses it
    if state == 'MA':
        chamber_str = f'{state_name} House of Representatives {bp_district} District'
    elif state == 'NH':
        # BP uses "New Hampshire House of Representatives Coos 5" (no County/District)
        # bp_district is like 'Coos_County_District_5', extract county and number
        parts = bp_district.replace('_', ' ').split()
        if 'County' in parts:
            ci = parts.index('County')
            county = ' '.join(parts[:ci])
            num = parts[-1]
            nh_label = f'{county} {num}'
        else:
            nh_label = bp_district.replace('_', ' ')
        chamber_str = f'{state_name} House of Representatives {nh_label}'
    elif state == 'WA':
        if chamber == 'Senate':
            chamber_str = f'Washington State Senate District {bp_district}'
        else:
            # BP House pages use "Washington House of Representatives" (no "State")
            chamber_str = f'Washington House of Representatives District {bp_district}'
    elif chamber == 'Assembly':
        if state == 'CA':
            chamber_str = f'{state_name} State Assembly District {bp_district}'
        elif state == 'NY':
            chamber_str = f'{state_name} State Assembly District {bp_district}'
    elif chamber == 'House of Delegates':
        chamber_str = f'{state_name} House of Delegates District {bp_district}'
    elif chamber == 'Senate':
        chamber_str = f'{state_name} State Senate District {bp_district}'
    else:
        chamber_str = f'{state_name} House of Representatives District {bp_district}'

    labels = {}

    # Special general election
    labels['Special'] = [
        f'Special general election for {chamber_str}',
        f'special general election for {chamber_str}',
        f'Special election for {chamber_str}',
        f'special election for {chamber_str}',
    ]

    # LA jungle primary = the special election itself
    if state == 'LA':
        labels['Special'] = [
            f'Special nonpartisan primary for {chamber_str}',
            f'special nonpartisan primary for {chamber_str}',
            f'Special nonpartisan blanket primary for {chamber_str}',
            f'Special election for {chamber_str}',
            f'special election for {chamber_str}',
        ]
        if race.get('runoff_date'):
            labels['Special_Runoff'] = [
                f'Special general election for {chamber_str}',
                f'special general election for {chamber_str}',
                f'Special runoff for {chamber_str}',
                f'special runoff for {chamber_str}',
                f'Special general election runoff for {chamber_str}',
            ]
    elif state in ('CA', 'WA'):
        # Top-two primary states
        labels['Special'] = [
            f'Special general election for {chamber_str}',
            f'special general election for {chamber_str}',
            f'Special election for {chamber_str}',
        ]
        if race.get('primary_date'):
            labels['Special_Primary'] = [
                f'Special top-two primary for {chamber_str}',
                f'Special open primary for {chamber_str}',
                f'Special primary for {chamber_str}',
                f'special primary for {chamber_str}',
                f'Special nonpartisan blanket primary for {chamber_str}',
                f'Special nonpartisan primary for {chamber_str}',
                f'special nonpartisan primary for {chamber_str}',
            ]
    else:
        # Standard primary states
        if race.get('primary_date'):
            labels['Special_Primary_D'] = [
                f'Special Democratic primary for {chamber_str}',
                f'special Democratic primary for {chamber_str}',
                f'Special Democratic primary election for {chamber_str}',
            ]
            labels['Special_Primary_R'] = [
                f'Special Republican primary for {chamber_str}',
                f'special Republican primary for {chamber_str}',
                f'Special Republican primary election for {chamber_str}',
            ]

        if race.get('runoff_date') and state != 'LA':
            labels['Special_Runoff'] = [
                f'Special general runoff election for {chamber_str}',
                f'special general runoff election for {chamber_str}',
                f'Special general election runoff for {chamber_str}',
                f'special general election runoff for {chamber_str}',
                f'Special runoff for {chamber_str}',
                f'Special runoff election for {chamber_str}',
            ]
            # AL uses "Special Republican primary runoff"
            labels['Special_Primary_Runoff_R'] = [
                f'Special Republican primary runoff for {chamber_str}',
                f'special Republican primary runoff for {chamber_str}',
            ]
            labels['Special_Primary_Runoff_D'] = [
                f'Special Democratic primary runoff for {chamber_str}',
                f'special Democratic primary runoff for {chamber_str}',
            ]

    return labels


def build_state_special_url(state):
    """Build URL for the state-level special elections summary page on BP."""
    state_name = STATE_NAMES[state]
    return f'https://ballotpedia.org/{state_name}_state_legislative_special_elections,_2025'


def try_parse_elections(html, race, labels):
    """Try to parse election results from HTML using the given label patterns.
    Returns list of parsed election dicts."""
    elections = []
    for etype, patterns in labels.items():
        parsed = None
        for label in patterns:
            parsed = parse_results_table(html, label)
            if parsed:
                break

        if parsed:
            if etype == 'Special':
                date = race['general_date']
            elif etype in ('Special_Primary_Runoff_D', 'Special_Primary_Runoff_R'):
                date = race['runoff_date']  # primary runoff uses runoff_date
            elif 'Primary' in etype:
                date = race['primary_date']
            elif 'Runoff' in etype:
                date = race['runoff_date']
            else:
                date = race['general_date']

            elections.append({
                'type': etype,
                'date': date,
                'total_votes': parsed['total_votes'],
                'candidates': parsed['candidates'],
            })
    return elections


# Cache for state-level special election pages
_state_special_pages = {}


def process_race(race):
    """Download and parse results for a single special election race."""
    state = race['state']
    chamber = race['chamber']
    district = race['district']
    race_key = f'{state}_{chamber.replace(" ", "")}_{district}'

    # 1. Try the district page first
    url = build_bp_url(race)
    cache_key = f'special_{race_key}'.replace('/', '_').replace('-', '_')

    html = download_page(url, cache_key)
    labels = build_election_labels(race)

    elections = []
    if html:
        elections = try_parse_elections(html, race, labels)

    # 2. If no Special general found on district page, try state special elections page
    if not any(e['type'] == 'Special' for e in elections):
        if state not in _state_special_pages:
            state_url = build_state_special_url(state)
            state_cache_key = f'state_specials_{state}'
            _state_special_pages[state] = download_page(state_url, state_cache_key)
            time.sleep(0.5)

        state_html = _state_special_pages.get(state)
        if state_html:
            state_elections = try_parse_elections(state_html, race, labels)
            # Merge: add any election types found on state page that weren't on district page
            found_types = {e['type'] for e in elections}
            for se in state_elections:
                if se['type'] not in found_types:
                    elections.append(se)

    # 3. If primary found but no general, check if general was "not required" or
    #    "won outright in the primary" (CA/WA top-two, or single-party districts)
    if (not any(e['type'] == 'Special' for e in elections)
        and any('Primary' in e['type'] for e in elections)
        and html):
        html_lower = html.lower()
        general_skipped = (
            'not required' in html_lower
            or 'was not necessary' in html_lower
            or 'won outright in the primary' in html_lower
            or 'was canceled after only' in html_lower
            or 'general election was canceled' in html_lower
        )
        if general_skipped:
            # Promote the first primary to be the Special (final) election
            for e in elections:
                if 'Primary' in e['type']:
                    e['type'] = 'Special'
                    e['primary_promoted'] = True
                    print(f'  {race_key}: General not required — primary winner won outright')
                    break

    # 4. Check for canceled elections (winner won without appearing on ballot)
    if not any(e['type'] == 'Special' for e in elections) and html:
        canceled_match = re.search(
            r'canceled.*?<a\s+href="[^"]*"[^>]*>([^<]+)</a>\s*\(([A-Z])\)\s*won',
            html, re.DOTALL | re.IGNORECASE
        )
        if not canceled_match:
            canceled_match = re.search(
                r'<a\s+href="[^"]*"[^>]*>([^<]+)</a>\s*\(([A-Z])\)\s*won\s+the\s+election\s+without',
                html, re.DOTALL | re.IGNORECASE
            )
        if canceled_match:
            winner_name = htmlmod.unescape(canceled_match.group(1).strip())
            winner_party = canceled_match.group(2)
            elections.append({
                'type': 'Special',
                'date': race['general_date'],
                'total_votes': 0,
                'candidates': [{
                    'name': winner_name,
                    'party': winner_party,
                    'is_incumbent': False,
                    'is_winner': True,
                    'votes': 0,
                    'vote_pct': 100.0,
                }],
                'canceled': True,
            })
            print(f'  {race_key}: CANCELED — {winner_name} ({winner_party}) won unopposed')

    if not elections:
        if not html:
            print(f'  {race_key}: FAILED to download')
        else:
            print(f'  {race_key}: No elections found on district or state page')
        return None

    # Try to extract vacancy reason from page text
    vacancy_reason = race['vacancy_reason']
    if vacancy_reason == 'resigned':
        # Try to find more detail from the page
        resign_match = re.search(
            r'seat became vacant.*?(resign|left|stepped down|appointed)',
            html[:5000], re.IGNORECASE
        )
        if resign_match:
            text = resign_match.group(1).lower()
            if 'appoint' in text:
                vacancy_reason = 'appointed_elsewhere'

    result = {
        'state': state,
        'chamber': chamber,
        'district': district,
        'former_incumbent': race['former_incumbent'],
        'vacancy_reason': vacancy_reason,
        'elections': elections,
    }

    # Quick summary
    special_gen = next((e for e in elections if e['type'] == 'Special'), None)
    if special_gen:
        winners = [c['name'] for c in special_gen['candidates'] if c['is_winner']]
        print(f'  {race_key}: {len(elections)} phases, '
              f'winner: {", ".join(winners) if winners else "NONE"}, '
              f'total: {special_gen["total_votes"]}')
    else:
        print(f'  {race_key}: {len(elections)} phases (no Special general found)')

    return result


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='Download 2025 special election results')
    parser.add_argument('--state', type=str,
                        help='Process a single state (e.g., VA, MS)')
    parser.add_argument('--output', type=str, default=OUTPUT_PATH,
                        help='Output JSON path')
    args = parser.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)

    races = SPECIAL_ELECTIONS
    if args.state:
        races = [r for r in races if r['state'] == args.state.upper()]
        if not races:
            print(f'No special elections found for {args.state}')
            sys.exit(1)

    print(f'Processing {len(races)} special election races...')
    if args.state:
        print(f'Filtered to state: {args.state.upper()}')

    all_results = []
    failed = []

    from collections import Counter
    by_state = Counter(r['state'] for r in races)
    print(f'\nRaces by state:')
    for st, cnt in sorted(by_state.items()):
        print(f'  {st}: {cnt}')

    for i, race in enumerate(races):
        result = process_race(race)
        if result:
            all_results.append(result)
        else:
            failed.append(f"{race['state']} {race['chamber']} {race['district']}")

        # Rate limit
        if i < len(races) - 1:
            time.sleep(0.5)

    # Summary
    print(f'\n{"=" * 60}')
    print(f'SUMMARY')
    print(f'{"=" * 60}')
    print(f'Total races processed: {len(all_results)} / {len(races)}')

    if failed:
        print(f'\nFailed ({len(failed)}):')
        for f_race in failed:
            print(f'  {f_race}')

    # Count election types
    etype_counts = Counter()
    total_candidates = 0
    total_winners = 0
    for r in all_results:
        for e in r['elections']:
            etype_counts[e['type']] += 1
            total_candidates += len(e['candidates'])
            total_winners += sum(1 for c in e['candidates'] if c['is_winner'])

    print(f'\nElection records by type:')
    for etype, cnt in sorted(etype_counts.items()):
        print(f'  {etype}: {cnt}')
    print(f'Total election records: {sum(etype_counts.values())}')
    print(f'Total candidates: {total_candidates}')
    print(f'Total winners: {total_winners}')

    # Races without a winner in the Special general
    no_winner = []
    for r in all_results:
        special_gen = next((e for e in r['elections'] if e['type'] == 'Special'), None)
        if special_gen and not any(c['is_winner'] for c in special_gen['candidates']):
            no_winner.append(f"{r['state']} {r['chamber']} {r['district']}")
    if no_winner:
        print(f'\nWARNING: {len(no_winner)} races without a winner in Special general:')
        for nw in no_winner:
            print(f'  {nw}')

    # Winners by party
    party_wins = Counter()
    for r in all_results:
        special_gen = next((e for e in r['elections'] if e['type'] == 'Special'), None)
        if special_gen:
            for c in special_gen['candidates']:
                if c['is_winner']:
                    party_wins[c['party']] += 1
    print(f'\nSpecial general winners by party:')
    for p, cnt in party_wins.most_common():
        print(f'  {p}: {cnt}')

    # Write output
    with open(args.output, 'w') as f_out:
        json.dump(all_results, f_out, indent=2)
    print(f'\nWritten to {args.output}')


if __name__ == '__main__':
    main()
