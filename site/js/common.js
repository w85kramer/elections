/* ============================================================
   Elections Site — Common Utilities
   ============================================================ */

const STATE_NAMES = {
  AL:'Alabama',AK:'Alaska',AZ:'Arizona',AR:'Arkansas',CA:'California',
  CO:'Colorado',CT:'Connecticut',DE:'Delaware',FL:'Florida',GA:'Georgia',
  HI:'Hawaii',ID:'Idaho',IL:'Illinois',IN:'Indiana',IA:'Iowa',
  KS:'Kansas',KY:'Kentucky',LA:'Louisiana',ME:'Maine',MD:'Maryland',
  MA:'Massachusetts',MI:'Michigan',MN:'Minnesota',MS:'Mississippi',MO:'Missouri',
  MT:'Montana',NE:'Nebraska',NV:'Nevada',NH:'New Hampshire',NJ:'New Jersey',
  NM:'New Mexico',NY:'New York',NC:'North Carolina',ND:'North Dakota',OH:'Ohio',
  OK:'Oklahoma',OR:'Oregon',PA:'Pennsylvania',RI:'Rhode Island',SC:'South Carolina',
  SD:'South Dakota',TN:'Tennessee',TX:'Texas',UT:'Utah',VT:'Vermont',
  VA:'Virginia',WA:'Washington',WV:'West Virginia',WI:'Wisconsin',WY:'Wyoming'
};

const LOWER_CHAMBER_MAP = {
  CA:'Assembly', NV:'Assembly', NY:'Assembly', WI:'Assembly', NJ:'Assembly',
  MD:'House of Delegates', VA:'House of Delegates', WV:'House of Delegates',
  NE:'Legislature'
};

function getLowerChamberName(state) {
  return LOWER_CHAMBER_MAP[state] || 'House';
}

function getChamberLabel(state, chamber) {
  if (chamber === 'Senate') return 'Senate';
  if (chamber === 'Legislature') return 'Legislature';
  return getLowerChamberName(state);
}

const MONTHS = ['Jan.','Feb.','Mar.','Apr.','May','Jun.','Jul.','Aug.','Sep.','Oct.','Nov.','Dec.'];

function formatDate(dateStr) {
  if (!dateStr) return '—';
  const d = new Date(dateStr + 'T00:00:00');
  return `${MONTHS[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
}

function formatDateShort(dateStr) {
  if (!dateStr) return '—';
  const d = new Date(dateStr + 'T00:00:00');
  return `${MONTHS[d.getMonth()]} ${d.getDate()}`;
}

function partyColor(party) {
  if (party === 'D') return '#6cb3d2';
  if (party === 'R') return '#e50963';
  if (party === 'I') return '#8b567f';
  if (party === 'NP') return '#d0ac60';
  return '#ccc';
}

function partyLabel(party) {
  if (party === 'D') return 'Democrat';
  if (party === 'R') return 'Republican';
  if (party === 'I') return 'Independent';
  if (party === 'NP') return 'Nonpartisan';
  return party || 'Vacant';
}

function partyBadgeClass(party) {
  if (party === 'D') return 'party-d';
  if (party === 'R') return 'party-r';
  if (party === 'NP') return 'party-np';
  return 'party-i';
}

/** Infer candidate party from election type when party is null/unknown */
function inferParty(candidateParty, electionType, stateAbbr) {
  if (candidateParty) return candidateParty;
  // NE is officially nonpartisan
  if (stateAbbr === 'NE') return 'NP';
  // Infer from primary type
  if (electionType && electionType.includes('_D')) return 'D';
  if (electionType && electionType.includes('_R')) return 'R';
  if (electionType && electionType.includes('_L')) return 'L';
  return null;
}

async function loadJSON(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to load ${url}: ${resp.status}`);
  return resp.json();
}

function numberWithCommas(x) {
  if (x == null) return '—';
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function initTooltip(el) {
  return {
    show(html) {
      el.innerHTML = html;
      el.classList.add('visible');
    },
    move(e) {
      let x = e.clientX + 14;
      let y = e.clientY + 14;
      if (x + 300 > window.innerWidth) x = e.clientX - 300;
      if (y + 200 > window.innerHeight) y = e.clientY - 200;
      el.style.left = x + 'px';
      el.style.top = y + 'px';
    },
    hide() {
      el.classList.remove('visible');
    }
  };
}

/**
 * Interpolate between color stops.
 * stops: array of {pct, color} where pct is 0-1 and color is [r,g,b]
 */
function interpolateColor(pct, stops) {
  if (pct <= stops[0].pct) return rgbStr(stops[0].color);
  if (pct >= stops[stops.length - 1].pct) return rgbStr(stops[stops.length - 1].color);
  for (let i = 0; i < stops.length - 1; i++) {
    if (pct >= stops[i].pct && pct <= stops[i + 1].pct) {
      const t = (pct - stops[i].pct) / (stops[i + 1].pct - stops[i].pct);
      const c0 = stops[i].color;
      const c1 = stops[i + 1].color;
      return rgbStr([
        Math.round(c0[0] + (c1[0] - c0[0]) * t),
        Math.round(c0[1] + (c1[1] - c0[1]) * t),
        Math.round(c0[2] + (c1[2] - c0[2]) * t),
      ]);
    }
  }
  return rgbStr(stops[stops.length - 1].color);
}

function rgbStr(c) {
  return `rgb(${c[0]},${c[1]},${c[2]})`;
}

/** Parse a margin string like "+12.3" or "-5.7" to a float. Positive = Dem. */
function parseMargin(str) {
  if (!str) return 0;
  return parseFloat(str);
}

/**
 * Build an Oregon-style chamber composition bar as SVG.
 * @param {HTMLElement} container - element to insert SVG into
 * @param {Object} data - {total, d, r, other, vacant, seats_up_2026, supermajority}
 * @param {string} label - chamber name e.g. "Senate"
 */
function buildChamberBar(container, data, label) {
  const W = 500, H = 80, BAR_Y = 20, BAR_H = 30;
  const total = data.total;
  if (!total) return;

  const majority = Math.floor(total / 2) + 1;
  const superM = data.supermajority || Math.ceil(total * 2 / 3);
  const scale = W / total;

  // Leading party goes first (left side); minority on right
  const rLeads = data.r > data.d;
  const leadCount = rLeads ? data.r : data.d;
  const leadColor = rLeads ? '#e50963' : '#6cb3d2';
  const leadLabel = rLeads ? `R: ${data.r}` : `D: ${data.d}`;
  const trailCount = rLeads ? data.d : data.r;
  const trailColor = rLeads ? '#6cb3d2' : '#e50963';
  const trailLabel = rLeads ? `D: ${data.d}` : `R: ${data.r}`;

  const segments = [
    { count: leadCount, color: leadColor, label: leadLabel },
    { count: data.other || 0, color: '#888', label: data.other ? `O: ${data.other}` : '' },
    { count: data.vacant || 0, color: '#ccc', label: data.vacant ? `V: ${data.vacant}` : '' },
    { count: trailCount, color: trailColor, label: trailLabel },
  ];

  let svg = `<svg width="100%" viewBox="0 0 ${W} ${H + 30}" xmlns="http://www.w3.org/2000/svg" style="max-width:${W}px">`;

  // Draw segments
  let x = 0;
  for (const seg of segments) {
    if (seg.count === 0) continue;
    const w = seg.count * scale;
    svg += `<rect x="${x}" y="${BAR_Y}" width="${w}" height="${BAR_H}" fill="${seg.color}" rx="0"/>`;

    // Label centered in segment if wide enough
    if (w > 40) {
      svg += `<text x="${x + w / 2}" y="${BAR_Y + BAR_H / 2 + 1}" text-anchor="middle" dominant-baseline="central"
        fill="white" font-family="Work Sans,sans-serif" font-weight="700" font-size="13">${seg.label}</text>`;
    }
    x += w;
  }

  // Majority line — measures from the leading party's side (left)
  const majX = majority * scale;
  svg += `<line x1="${majX}" y1="${BAR_Y - 4}" x2="${majX}" y2="${BAR_Y + BAR_H + 4}" stroke="#043858" stroke-width="2"/>`;
  svg += `<text x="${majX}" y="${BAR_Y - 8}" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="600" font-size="11" fill="#043858">Majority (${majority})</text>`;

  // Supermajority line (dashed)
  if (superM < total) {
    const smX = superM * scale;
    svg += `<line x1="${smX}" y1="${BAR_Y - 4}" x2="${smX}" y2="${BAR_Y + BAR_H + 4}" stroke="#043858" stroke-width="1.5" stroke-dasharray="4,3"/>`;
    svg += `<text x="${smX}" y="${BAR_Y + BAR_H + 16}" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="400" font-size="10" fill="#666">\u2154 (${superM})</text>`;
  }

  // Seats up notation
  const upText = data.seats_up_2026 === total
    ? `All ${total} up in 2026`
    : `${data.seats_up_2026} of ${total} up in 2026`;
  svg += `<text x="${W / 2}" y="${H + 22}" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="400" font-size="12" fill="#666">${upText}</text>`;

  svg += '</svg>';
  container.innerHTML = svg;
}

/**
 * Build a nested coalition composition bar for chambers with coalition caucuses (e.g. Alaska).
 * Shows majority coalition and minority sections, each subdivided by party.
 * @param {HTMLElement} container - element to insert SVG into
 * @param {Object} data - {total, majority: {total, segments: [{party,count}]}, minority: {total, segments}, vacant, seats_up_2026, supermajority}
 * @param {string} label - chamber name
 */
function buildCoalitionBar(container, data, label) {
  const W = 500, BAR_Y = 24, BAR_H = 30, TOTAL_H = 100;
  const total = data.total;
  if (!total) return;

  const majority = Math.floor(total / 2) + 1;
  const superM = data.supermajority || Math.ceil(total * 2 / 3);
  const scale = W / total;

  let svg = `<svg width="100%" viewBox="0 0 ${W} ${TOTAL_H}" xmlns="http://www.w3.org/2000/svg" style="max-width:${W}px">`;

  // Build segments: majority parties then minority parties then vacant.
  // Within majority, put parties that also appear in minority LAST so they sit
  // adjacent to the same party in minority — the purple bracket splits them visually.
  const minPartySet = new Set(data.minority.segments.filter(s => s.count > 0).map(s => s.party));
  const majSorted = data.majority.segments.slice().sort((a, b) => {
    const aShared = minPartySet.has(a.party) ? 1 : 0;
    const bShared = minPartySet.has(b.party) ? 1 : 0;
    if (aShared !== bShared) return aShared - bShared;
    return b.count - a.count;
  });
  const minSorted = data.minority.segments.slice().sort((a, b) => b.count - a.count);
  const allSegments = [];

  for (const s of majSorted) {
    if (s.count > 0) allSegments.push({ ...s, coalition: 'majority' });
  }
  for (const s of minSorted) {
    if (s.count > 0) allSegments.push({ ...s, coalition: 'minority' });
  }
  if (data.vacant > 0) {
    allSegments.push({ party: 'V', count: data.vacant, coalition: 'vacant' });
  }

  // Draw bar segments
  let x = 0;
  for (const seg of allSegments) {
    const w = seg.count * scale;
    const color = seg.party === 'V' ? '#ccc' : partyColor(seg.party);
    svg += `<rect x="${x}" y="${BAR_Y}" width="${w}" height="${BAR_H}" fill="${color}"/>`;
    const lbl = `${seg.party}: ${seg.count}`;
    if (w > 38) {
      svg += `<text x="${x + w / 2}" y="${BAR_Y + BAR_H / 2 + 1}" text-anchor="middle" dominant-baseline="central"
        fill="white" font-family="Work Sans,sans-serif" font-weight="700" font-size="12">${lbl}</text>`;
    }
    x += w;
  }

  // Purple bracket around majority coalition section
  const majW = data.majority.total * scale;
  svg += `<rect x="0" y="${BAR_Y - 2}" width="${majW}" height="${BAR_H + 4}" fill="none" stroke="#8b567f" stroke-width="2.5" rx="2"/>`;

  // Coalition labels above bar
  svg += `<text x="${majW / 2}" y="12" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="700" font-size="11" fill="#8b567f">Majority Coalition (${data.majority.total})</text>`;

  const minW = data.minority.total * scale;
  if (minW > 0) {
    svg += `<text x="${majW + minW / 2}" y="12" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="600" font-size="11" fill="#666">Minority (${data.minority.total})</text>`;
  }

  // Majority threshold line
  const majX = majority * scale;
  svg += `<line x1="${majX}" y1="${BAR_Y - 4}" x2="${majX}" y2="${BAR_Y + BAR_H + 4}" stroke="#043858" stroke-width="2"/>`;
  svg += `<text x="${majX}" y="${BAR_Y + BAR_H + 18}" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="600" font-size="11" fill="#043858">Majority (${majority})</text>`;

  // Supermajority line (dashed)
  if (superM < total) {
    const smX = superM * scale;
    svg += `<line x1="${smX}" y1="${BAR_Y - 4}" x2="${smX}" y2="${BAR_Y + BAR_H + 4}" stroke="#043858" stroke-width="1.5" stroke-dasharray="4,3"/>`;
    svg += `<text x="${smX}" y="${BAR_Y + BAR_H + 30}" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="400" font-size="10" fill="#666">\u2154 (${superM})</text>`;
  }

  // Seats up notation
  const upText = data.seats_up_2026 === total
    ? `All ${total} up in 2026`
    : `${data.seats_up_2026} of ${total} up in 2026`;
  svg += `<text x="${W / 2}" y="${TOTAL_H - 4}" text-anchor="middle" font-family="Work Sans,sans-serif" font-weight="400" font-size="12" fill="#666">${upText}</text>`;

  svg += '</svg>';
  container.innerHTML = svg;
}
