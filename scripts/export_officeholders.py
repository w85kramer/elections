#!/usr/bin/env python3
"""Regenerate officeholders.json from per-state candidate files, using current_office for office_type."""
import json, os, glob
from datetime import datetime

OFFICE_MAP = {
    'Governor': 'Governor',
    'Lt. Governor': 'Lt. Governor',
    'Attorney General': 'Attorney General',
    'Secretary of State': 'Secretary of State',
    'Treasurer': 'Treasurer',
    'Auditor': 'Auditor',
    'Controller': 'Controller',
    'Agriculture Commissioner': 'Agriculture Commissioner',
    'Superintendent': 'Superintendent',
    'State Senate': 'State Senate',
    'State House': 'State House',
    'State Senator': 'State Senate',
    'State Representative': 'State House',
    'State Legislature': 'State Legislature',
}

data_dir = '/home/billkramer/elections/site/data/candidates'
officeholders = []

for fp in sorted(glob.glob(os.path.join(data_dir, '*.json'))):
    with open(fp) as f:
        state_data = json.load(f)

    st = state_data['state']
    for cand in state_data['candidates']:
        co = cand.get('current_office')
        if not co:
            continue

        # Get office type from current_office (the actual role)
        raw_ot = co.get('office_type', '')
        office = OFFICE_MAP.get(raw_ot, raw_ot)

        # Party: actual registered party for display
        party = co.get('party') or cand.get('party') or ''
        # Caucus: who they govern with (for coloring/filtering)
        caucus = co.get('caucus') or cand.get('caucus') or party

        officeholders.append({
            'id': cand.get('id', 0),
            'name': cand['full_name'],
            'state': st,
            'party': party,
            'caucus': caucus,
            'office': office,
            'chamber': co.get('chamber', ''),
            'district': co.get('district', ''),
        })

# Sort by state, then office priority, then name
office_order = {'Governor': 0, 'Lt. Governor': 1, 'Attorney General': 2, 'Secretary of State': 3,
                'Treasurer': 4, 'Auditor': 5, 'Controller': 6, 'Agriculture Commissioner': 7,
                'Superintendent': 8, 'State Senate': 9, 'State House': 10, 'State Legislature': 11}
officeholders.sort(key=lambda o: (o['state'], office_order.get(o['office'], 99), o['name']))

out = {
    'generated_at': datetime.utcnow().isoformat() + 'Z',
    'total': len(officeholders),
    'officeholders': officeholders,
}

out_path = '/home/billkramer/elections/site/data/officeholders.json'
with open(out_path, 'w') as f:
    json.dump(out, f, separators=(',', ':'))

print(f'Written {len(officeholders)} officeholders to {out_path}')
print(f'Size: {os.path.getsize(out_path) / 1024:.0f} KB')

# Verify the fix
from collections import Counter
office_counts = Counter(o['office'] for o in officeholders)
for office, count in sorted(office_counts.items(), key=lambda x: -x[1]):
    print(f'  {office}: {count}')

# Check GA specifically
ga_govs = [o for o in officeholders if o['state'] == 'GA' and o['office'] == 'Governor']
print(f'\nGA Governors: {[o["name"] for o in ga_govs]}')
ga_ltgov = [o for o in officeholders if o['state'] == 'GA' and o['office'] == 'Lt. Governor']
print(f'GA Lt. Governors: {[o["name"] for o in ga_ltgov]}')
ga_sos = [o for o in officeholders if o['state'] == 'GA' and o['office'] == 'Secretary of State']
print(f'GA Sec of State: {[o["name"] for o in ga_sos]}')
