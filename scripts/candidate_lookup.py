"""
Shared candidate lookup — find or create candidates with fuzzy matching.

Used by all populate scripts to prevent duplicate candidate records.
Before inserting a new candidate, searches existing candidates in the same
state for fuzzy name matches.

Usage:
    from candidate_lookup import CandidateLookup

    lookup = CandidateLookup(run_sql)
    # Load candidates for the states you'll be working with
    lookup.load_state('NH')

    # Returns existing candidate_id or creates new record
    candidate_id = lookup.find_or_create(
        full_name='Josh Hernandez',
        state='NM',
        seat_id=1234,        # optional — improves matching
        party='R',            # optional — stored on candidacy, not candidate
    )
"""

import re
import unicodedata


# ── Nickname mappings ──

NICKNAMES = {
    'william': {'bill', 'billy', 'will', 'willy', 'willie'},
    'robert': {'bob', 'bobby', 'rob', 'robby', 'robbie'},
    'richard': {'rick', 'dick', 'rich', 'richie'},
    'james': {'jim', 'jimmy', 'jamie'},
    'john': {'jack', 'johnny', 'jon'},
    'joseph': {'joe', 'joey'},
    'michael': {'mike', 'mikey'},
    'thomas': {'tom', 'tommy'},
    'charles': {'charlie', 'chuck', 'chas'},
    'edward': {'ed', 'eddie', 'ted', 'teddy', 'eddy'},
    'elizabeth': {'liz', 'lizzy', 'beth', 'betty', 'eliza', 'liza'},
    'margaret': {'maggie', 'meg', 'peggy', 'marge', 'margie'},
    'catherine': {'cathy', 'kate', 'kathy', 'cat', 'katie'},
    'katherine': {'kathy', 'kate', 'katie', 'kat'},
    'patricia': {'pat', 'patty', 'tricia'},
    'jennifer': {'jen', 'jenny'},
    'jessica': {'jess', 'jessie'},
    'stephanie': {'steph'},
    'christopher': {'chris'},
    'nicholas': {'nick', 'nicky'},
    'timothy': {'tim', 'timmy'},
    'stephen': {'steve', 'steven'},
    'steven': {'steve', 'stephen'},
    'daniel': {'dan', 'danny'},
    'matthew': {'matt'},
    'anthony': {'tony'},
    'donald': {'don', 'donny'},
    'kenneth': {'ken', 'kenny'},
    'ronald': {'ron', 'ronny'},
    'lawrence': {'larry'},
    'raymond': {'ray'},
    'gerald': {'jerry', 'gerry'},
    'benjamin': {'ben', 'benny'},
    'samuel': {'sam', 'sammy'},
    'deborah': {'deb', 'debbie', 'debby'},
    'debra': {'deb', 'debbie', 'debby'},
    'virginia': {'ginny', 'ginger'},
    'dorothy': {'dot', 'dottie'},
    'barbara': {'barb', 'barbie'},
    'alexander': {'alex'},
    'alexandra': {'alex', 'lexi'},
    'jonathan': {'jon', 'john'},
    'nathaniel': {'nate', 'nathan'},
    'nathan': {'nate'},
    'phillip': {'phil'},
    'philip': {'phil'},
    'zachary': {'zach', 'zack'},
    'frederick': {'fred', 'freddy'},
    'douglas': {'doug'},
    'harold': {'hal', 'harry'},
    'leonard': {'len', 'lenny'},
    'arthur': {'art'},
    'clifford': {'cliff'},
    'russell': {'russ'},
    'terrence': {'terry'},
    'theresa': {'terry', 'tess'},
    'andrew': {'andy', 'drew'},
    'gregory': {'greg'},
    'jeffrey': {'jeff'},
    'peter': {'pete'},
    'walter': {'walt'},
    'franklin': {'frank'},
    'wesley': {'wes'},
    'cameron': {'cam'},
    'joshua': {'josh'},
    'rebecca': {'becky', 'becca'},
    'susan': {'sue'},
    'suzanne': {'sue'},
    'pamela': {'pam'},
    'cynthia': {'cindy'},
    'melanie': {'mel'},
    'jacqueline': {'jackie'},
}

# Build reverse lookup: nickname → set of canonical forms
_NICK_REVERSE = {}
for canonical, nicks in NICKNAMES.items():
    all_forms = {canonical} | nicks
    for form in all_forms:
        if form not in _NICK_REVERSE:
            _NICK_REVERSE[form] = set()
        _NICK_REVERSE[form] |= all_forms


def strip_accents(s):
    """Remove diacritics/accents (ñ→n, é→e)."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def normalize_name(name):
    """Normalize a name for comparison: lowercase, strip accents/suffixes/quotes."""
    if not name:
        return ''
    n = strip_accents(name).lower().strip()
    # Remove quoted nicknames
    n = re.sub(r'"[^"]*"?', '', n)
    # Remove suffixes
    n = re.sub(r',?\s*(jr\.?|sr\.?|iii|iv|ii)\s*$', '', n, flags=re.IGNORECASE)
    # Remove single-letter middle initials with period
    n = re.sub(r'\b[a-z]\.\s*', '', n)
    # Collapse whitespace
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def split_name(full_name):
    """Split a full name into (first, last). Returns normalized lowercase."""
    norm = normalize_name(full_name)
    parts = norm.split()
    if not parts:
        return ('', '')
    if len(parts) == 1:
        return (parts[0], parts[0])
    return (parts[0], parts[-1])


def first_names_match(first1, first2):
    """Check if two first names match, accounting for nicknames and prefixes."""
    if first1 == first2:
        return True
    # Prefix match (3+ chars): Jeff/Jeffrey, Liz/Elizabeth
    if len(first1) >= 3 and len(first2) >= 3:
        if first1.startswith(first2) or first2.startswith(first1):
            return True
    # Nickname match
    forms1 = _NICK_REVERSE.get(first1, {first1})
    forms2 = _NICK_REVERSE.get(first2, {first2})
    if forms1 & forms2:
        return True
    return False


class CandidateLookup:
    """
    Maintains a per-state cache of existing candidates for fuzzy matching.

    Usage:
        lookup = CandidateLookup(run_sql_func)
        lookup.load_state('NH')
        cid = lookup.find_or_create('Josh Hernandez', state='NM')
    """

    def __init__(self, run_sql):
        """
        Args:
            run_sql: callable that takes a SQL string and returns list of dicts.
        """
        self.run_sql = run_sql
        # state_abbr → { normalized_last_name → [ {id, full_name, first, last} ] }
        self._cache = {}
        self._loaded_states = set()

    def load_state(self, state_abbr):
        """Load all candidates associated with a state into the cache."""
        if state_abbr in self._loaded_states:
            return
        rows = self.run_sql(f"""
            SELECT DISTINCT c.id, c.full_name, c.first_name, c.last_name
            FROM candidates c
            LEFT JOIN candidacies ca ON ca.candidate_id = c.id
            LEFT JOIN elections e ON ca.election_id = e.id
            LEFT JOIN seats s ON e.seat_id = s.id
            LEFT JOIN districts d ON s.district_id = d.id
            LEFT JOIN states st ON d.state_id = st.id
            LEFT JOIN seat_terms stm ON stm.candidate_id = c.id
            LEFT JOIN seats s2 ON stm.seat_id = s2.id
            LEFT JOIN districts d2 ON s2.district_id = d2.id
            LEFT JOIN states st2 ON d2.state_id = st2.id
            WHERE st.abbreviation = '{state_abbr}' OR st2.abbreviation = '{state_abbr}'
        """)
        by_last = {}
        for r in rows:
            first, last = split_name(r['full_name'])
            entry = {'id': r['id'], 'full_name': r['full_name'],
                     'first': first, 'last': last}
            by_last.setdefault(last, []).append(entry)
        self._cache[state_abbr] = by_last
        self._loaded_states.add(state_abbr)

    def find_match(self, full_name, state):
        """
        Search for an existing candidate matching this name in the given state.

        Returns candidate_id if a match is found, None otherwise.
        """
        self.load_state(state)
        first, last = split_name(full_name)
        if not last:
            return None

        by_last = self._cache.get(state, {})
        candidates = by_last.get(last, [])
        if not candidates:
            return None

        # Score all candidates with the same last name
        best_id = None
        best_score = 0

        for c in candidates:
            if c['first'] == first:
                # Exact first + last match
                return c['id']
            if first_names_match(first, c['first']):
                # Nickname or prefix match — score by specificity
                score = 0.9 if (first.startswith(c['first']) or
                                c['first'].startswith(first)) else 0.8
                if score > best_score:
                    best_score = score
                    best_id = c['id']

        return best_id

    def find_or_create(self, full_name, state, first_name=None, last_name=None,
                       gender=None):
        """
        Find an existing candidate or create a new one.

        Returns candidate_id (int).
        """
        existing = self.find_match(full_name, state)
        if existing:
            return existing

        # Create new candidate
        fn = first_name or full_name.split()[0] if full_name.split() else ''
        ln = last_name or full_name.split()[-1] if full_name.split() else ''
        gender_sql = f"'{gender}'" if gender else 'NULL'

        result = self.run_sql(f"""
            INSERT INTO candidates (full_name, first_name, last_name, gender)
            VALUES ('{full_name.replace("'", "''")}',
                    '{fn.replace("'", "''")}',
                    '{ln.replace("'", "''")}',
                    {gender_sql})
            RETURNING id
        """)
        new_id = result[0]['id']

        # Add to cache
        first, last = split_name(full_name)
        entry = {'id': new_id, 'full_name': full_name,
                 'first': first, 'last': last}
        by_last = self._cache.setdefault(state, {})
        by_last.setdefault(last, []).append(entry)

        return new_id
