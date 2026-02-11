"""
Download and parse Ballotpedia ballot measure index pages for 2024, 2025, 2026.

Extracts structured data (state, date, type, title, subject, description, result,
votes, measure page URLs) from "By state" sections. Saves parsed data to JSON
and downloads individual measure pages for AI description generation.

Usage:
    python3 scripts/download_ballot_measures.py                # all 3 years
    python3 scripts/download_ballot_measures.py --year 2024    # one year
    python3 scripts/download_ballot_measures.py --skip-pages   # skip individual page downloads
"""
import sys
import os
import re
import json
import time
import argparse
import html as htmlmod

import httpx

# 50 US states only (no DC, territories)
US_STATES = {
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
    'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
    'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
    'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
    'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
    'New_Hampshire', 'New_Jersey', 'New_Mexico', 'New_York',
    'North_Carolina', 'North_Dakota', 'Ohio', 'Oklahoma', 'Oregon',
    'Pennsylvania', 'Rhode_Island', 'South_Carolina', 'South_Dakota',
    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
    'West_Virginia', 'Wisconsin', 'Wyoming',
}

# BP type code → (measure_type, sponsor_type)
TYPE_MAP = {
    'LRCA': ('Legislative Constitutional Amendment', 'Legislature'),
    'CICA': ('Initiated Constitutional Amendment', 'Citizen'),
    'LRSS': ('Legislative Referendum', 'Legislature'),
    'CISS': ('Initiated State Statute', 'Citizen'),
    'BI': ('Bond Measure', 'Legislature'),
    'LRAQ': ('Advisory Question', 'Legislature'),
    'VR': ('Veto Referendum', 'Citizen'),
    'CCQ': ('Advisory Question', 'Other'),
    'ABR': ('Legislative Referendum', 'Legislature'),
    'IndISS': ('Initiated State Statute', 'Citizen'),
}

# Default general election dates by year
DEFAULT_DATES = {
    2024: 'November 5, 2024',
    2025: 'November 4, 2025',
    2026: 'November 3, 2026',
}

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'}


def download_page(url, retries=1):
    """Download a URL with retries. Returns HTML text or None."""
    for attempt in range(retries + 1):
        try:
            resp = httpx.get(
                url, headers=HEADERS, follow_redirects=True,
                timeout=httpx.Timeout(15.0, connect=10.0)
            )
            if resp.status_code == 200:
                return resp.text
            print(f'  WARNING: HTTP {resp.status_code} for {url}', flush=True)
        except Exception as e:
            print(f'  WARNING: Download failed (attempt {attempt+1}): {e}', flush=True)
        if attempt < retries:
            time.sleep(2)
    return None


def parse_index_page(html_text, year):
    """
    Parse a Ballotpedia ballot measures index page.

    Returns list of dicts with keys:
        state, election_date, type_code, measure_type, sponsor_type,
        measure_number, short_title, subject_category, bp_description,
        result, votes_yes, votes_no, yes_pct, no_pct, measure_url, year
    """
    by_state = html_text.find('id="By_state"')
    if by_state == -1:
        by_state = html_text.find('By state')
    if by_state == -1:
        print(f'  ERROR: No "By state" section found for {year}')
        return []

    # Find ALL h3 tags after "By state" — used as section boundaries
    # We match both the mw-headline id format and raw <h3> tags
    all_h3_positions = [m.start() for m in re.finditer(r'<h3>', html_text[by_state:])]
    all_h3_positions = [by_state + p for p in all_h3_positions]

    # Find h3 state headers with extractable IDs
    h3_pattern = re.compile(
        r'<h3>(?:<span[^>]*>)*<span class="mw-headline" id="([^"]+)">',
    )
    h3_matches = list(h3_pattern.finditer(html_text, by_state))

    measures = []

    for i, h3_match in enumerate(h3_matches):
        state_id = h3_match.group(1)

        # Skip non-state entries
        if state_id not in US_STATES:
            continue

        state_name = state_id.replace('_', ' ')

        # Get section between this h3 and the next h3 (ANY h3, not just states)
        section_start = h3_match.start()
        # Find the next h3 position after this one
        section_end = len(html_text)
        for pos in all_h3_positions:
            if pos > section_start + 10:
                section_end = pos
                break
        section = html_text[section_start:section_end]

        # Find all bptable tables within this state's section
        table_pattern = re.compile(
            r'<table[^>]*class="[^"]*bptable blue[^"]*"[^>]*>(.*?)</table>',
            re.DOTALL,
        )
        table_matches = list(table_pattern.finditer(section))

        # Find all bold date markers within the section
        # Dates appear as <b>March 5, 2024:</b> or <b>November 4:</b>
        date_pattern = re.compile(r'<b>([A-Z][a-z]+\s+\d+(?:,\s*\d{4})?)\s*:?\s*</b>')
        date_matches = list(date_pattern.finditer(section))

        for tbl_idx, tbl_match in enumerate(table_matches):
            # Determine election date for this table
            election_date = _find_date_for_table(
                section, tbl_match.start(), date_matches, year
            )

            table_html = tbl_match.group(1)

            # Parse header to detect column order
            columns = _parse_header(table_html)

            # Parse data rows
            row_measures = _parse_data_rows(
                table_html, columns, state_name, election_date, year
            )
            measures.extend(row_measures)

    return measures


def _find_date_for_table(section, table_pos, date_matches, year):
    """Find the most recent date marker before this table position."""
    best_date = None
    for dm in date_matches:
        if dm.start() < table_pos:
            best_date = dm.group(1)
        else:
            break

    if best_date is None:
        return DEFAULT_DATES.get(year, f'November 3, {year}')

    # Normalize: add year if missing
    if not re.search(r'\d{4}', best_date):
        best_date = f'{best_date}, {year}'

    return best_date


def _parse_header(table_html):
    """Parse the header row to determine column indices."""
    header_match = re.search(r'<tr>(.*?)</tr>', table_html, re.DOTALL)
    if not header_match:
        return {}

    ths = re.findall(r'<th[^>]*>(.*?)</th>', header_match.group(1), re.DOTALL)
    # Strip HTML from header names
    col_names = [re.sub(r'<[^>]+>', '', th).strip() for th in ths]

    columns = {}
    for idx, name in enumerate(col_names):
        columns[name] = idx

    return columns


def _parse_data_rows(table_html, columns, state_name, election_date, year):
    """Parse all data rows from a table."""
    # Find all <tr> that contain <td> (skip header row with <th>)
    row_pattern = re.compile(r'<tr>(.*?)</tr>', re.DOTALL)
    rows = row_pattern.findall(table_html)

    measures = []
    for row_html in rows:
        # Skip header rows
        if '<th' in row_html:
            continue

        tds = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
        if len(tds) < 2:
            continue

        measure = _parse_row(tds, columns, state_name, election_date, year)
        if measure:
            measures.append(measure)

    return measures


def _parse_row(tds, columns, state_name, election_date, year):
    """Parse a single data row into a measure dict."""
    # Dynamic column mapping based on header
    # Possible columns: Type, Title, Subject, Description, Result, Yes Votes, No Votes
    # Some tables omit Subject (most 2024 tables)

    has_subject = 'Subject' in columns
    has_result = 'Result' in columns

    # Determine column indices
    type_idx = columns.get('Type', 0)
    title_idx = columns.get('Title', 1)

    if has_subject:
        subject_idx = columns.get('Subject')
        desc_idx = columns.get('Description', subject_idx + 1 if subject_idx is not None else 3)
    else:
        subject_idx = None
        desc_idx = columns.get('Description', 2)

    # Parse type code
    type_html = tds[type_idx] if type_idx < len(tds) else ''
    type_match = re.search(r'>([A-Z][A-Za-z]{1,8})</a>', type_html)
    type_code = type_match.group(1) if type_match else None

    type_info = TYPE_MAP.get(type_code, (None, None))
    measure_type, sponsor_type = type_info

    # Parse title + URL
    title_html = tds[title_idx] if title_idx < len(tds) else ''
    title_match = re.search(
        r'<a href="(https://ballotpedia\.org/[^"]+)"[^>]*>([^<]+)</a>',
        title_html,
    )
    if not title_match:
        # Some might use relative URLs
        title_match = re.search(r'<a href="(/[^"]+)"[^>]*>([^<]+)</a>', title_html)

    if not title_match:
        return None

    measure_url = title_match.group(1)
    if measure_url.startswith('/'):
        measure_url = f'https://ballotpedia.org{measure_url}'
    measure_number = htmlmod.unescape(title_match.group(2).strip())

    # Some titles in the link are abbreviated; the title attribute has the full title
    full_title_match = re.search(r'title="([^"]+)"', title_html)
    short_title = htmlmod.unescape(full_title_match.group(1).strip()) if full_title_match else measure_number

    # Parse subject
    subject_category = None
    if subject_idx is not None and subject_idx < len(tds):
        subject_html = tds[subject_idx]
        # Subject may contain links; extract text
        subjects = re.findall(r'>([^<]+)</a>', subject_html)
        if subjects:
            subject_category = '; '.join(s.strip() for s in subjects if s.strip())
        else:
            subject_category = _strip_html(subject_html).strip() or None

    # Parse description
    bp_description = None
    if desc_idx is not None and desc_idx < len(tds):
        bp_description = _strip_html(tds[desc_idx]).strip() or None

    # Parse result (2024/2025 only)
    result = None
    votes_yes = None
    votes_no = None
    yes_pct = None
    no_pct = None

    if has_result:
        result_idx = columns.get('Result')
        yes_idx = columns.get('Yes Votes')
        no_idx = columns.get('No Votes')

        if result_idx is not None and result_idx < len(tds):
            result_html = tds[result_idx]
            if 'alt="Approved"' in result_html:
                result = 'Passed'
            elif 'alt="Defeated"' in result_html:
                result = 'Failed'
            elif 'alt="Overturned"' in result_html:
                result = 'Failed'  # Court-overturned = effectively failed

        if yes_idx is not None and yes_idx < len(tds):
            votes_yes, yes_pct = _parse_votes(tds[yes_idx])
        if no_idx is not None and no_idx < len(tds):
            votes_no, no_pct = _parse_votes(tds[no_idx])

    return {
        'state': state_name,
        'election_date': election_date,
        'year': year,
        'type_code': type_code,
        'measure_type': measure_type,
        'sponsor_type': sponsor_type,
        'measure_number': measure_number,
        'short_title': short_title,
        'subject_category': subject_category,
        'bp_description': bp_description,
        'result': result,
        'votes_yes': votes_yes,
        'votes_no': votes_no,
        'yes_pct': yes_pct,
        'no_pct': no_pct,
        'measure_url': measure_url,
    }


def _parse_votes(td_html):
    """Parse vote count like '1,234,567 (64%)' from a td. Returns (count, pct)."""
    match = re.search(r'([\d,]+)\s*\((\d+(?:\.\d+)?)%\)', td_html)
    if match:
        count = int(match.group(1).replace(',', ''))
        pct = float(match.group(2))
        return count, pct
    return None, None


def _strip_html(s):
    """Remove HTML tags from a string."""
    text = re.sub(r'<[^>]+>', '', s)
    return htmlmod.unescape(text).strip()


def download_individual_pages(measures, skip_existing=True):
    """Download individual measure pages from Ballotpedia."""
    os.makedirs('/tmp/bp_measures', exist_ok=True)

    total = len(measures)
    downloaded = 0
    skipped = 0
    failed = 0

    for i, m in enumerate(measures):
        year = m['year']
        state_slug = m['state'].replace(' ', '_').lower()

        # Create a safe filename from the URL
        url_path = m['measure_url'].split('ballotpedia.org/')[-1]
        # Truncate long filenames
        safe_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', url_path)[:120]
        filename = f'{safe_name}.html'

        year_dir = f'/tmp/bp_measures/{year}'
        os.makedirs(year_dir, exist_ok=True)
        filepath = f'{year_dir}/{filename}'

        # Store filepath in measure for later use
        m['html_file'] = filepath

        if skip_existing and os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
            skipped += 1
            continue

        if (i + 1) % 20 == 0 or downloaded % 10 == 0:
            print(f'  Downloading {i+1}/{total} ({downloaded} new, {skipped} cached, {failed} failed)...', flush=True)

        html_text = download_page(m['measure_url'])
        if html_text:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_text)
            downloaded += 1
        else:
            failed += 1
            m['html_file'] = None

        # Rate limiting
        time.sleep(0.3)

    print(f'  Done: {downloaded} downloaded, {skipped} cached, {failed} failed', flush=True)


def main():
    parser = argparse.ArgumentParser(description='Download and parse Ballotpedia ballot measures')
    parser.add_argument('--year', type=int, choices=[2024, 2025, 2026],
                        help='Process a single year')
    parser.add_argument('--skip-pages', action='store_true',
                        help='Skip downloading individual measure pages')
    args = parser.parse_args()

    years = [args.year] if args.year else [2024, 2025, 2026]

    all_measures = []

    for year in years:
        print(f'\n{"=" * 60}')
        print(f'YEAR: {year}')
        print(f'{"=" * 60}')

        # Check for cached index page
        cache_file = f'/tmp/bp_{year}_ballot_measures.html'
        if os.path.exists(cache_file) and os.path.getsize(cache_file) > 10000:
            print(f'  Using cached index page: {cache_file}')
            with open(cache_file, 'r', encoding='utf-8') as f:
                html_text = f.read()
        else:
            url = f'https://ballotpedia.org/{year}_ballot_measures'
            print(f'  Downloading index page: {url}')
            html_text = download_page(url)
            if not html_text:
                print(f'  ERROR: Could not download {year} index page')
                continue
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(html_text)

        # Parse
        print(f'  Parsing measures...')
        measures = parse_index_page(html_text, year)
        print(f'  Found {len(measures)} measures across {len(set(m["state"] for m in measures))} states')

        # Summary
        if measures:
            by_state = {}
            for m in measures:
                by_state.setdefault(m['state'], []).append(m)
            print(f'  States: {", ".join(f"{s}({len(ms)})" for s, ms in sorted(by_state.items()))}')

            if year in (2024, 2025):
                passed = sum(1 for m in measures if m['result'] == 'Passed')
                failed = sum(1 for m in measures if m['result'] == 'Failed')
                no_result = sum(1 for m in measures if m['result'] is None)
                print(f'  Results: {passed} passed, {failed} failed, {no_result} no result')

            type_counts = {}
            for m in measures:
                tc = m['type_code'] or 'Unknown'
                type_counts[tc] = type_counts.get(tc, 0) + 1
            print(f'  Types: {type_counts}')

        all_measures.extend(measures)

        # Download individual pages
        if not args.skip_pages:
            print(f'\n  Downloading individual measure pages...')
            download_individual_pages(measures)

    # Save parsed data
    output_file = '/tmp/ballot_measures_parsed.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_measures, f, indent=2, ensure_ascii=False)

    print(f'\n{"=" * 60}')
    print(f'SUMMARY')
    print(f'{"=" * 60}')
    print(f'Total measures: {len(all_measures)}')
    for year in years:
        year_measures = [m for m in all_measures if m['year'] == year]
        print(f'  {year}: {len(year_measures)} measures')
    print(f'Saved to: {output_file}')


if __name__ == '__main__':
    main()
