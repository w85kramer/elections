/* ============================================================
   Elections Site — Supabase Live Data
   Fetches uncertified 2026 elections directly from PostgREST
   to avoid needing a full static export on every data update.
   Falls back silently to static data on any failure.
   ============================================================ */

const SUPABASE_URL = 'https://pikcvwulzfxgwfcfssxc.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBpa2N2d3VsemZ4Z3dmY2Zzc3hjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA1NjcyODUsImV4cCI6MjA4NjE0MzI4NX0.d4AiCcr4OvPbkX2d728PKJro1v_csy7qGI6zOENWjd4';
const POSTGREST_BASE = SUPABASE_URL + '/rest/v1';
const LIVE_ELECTION_YEAR = 2026;
const LIVE_QUERY_TIMEOUT_MS = 5000;
const LIVE_CACHE_TTL_MS = 60000; // 60 seconds

/**
 * Check if a district has any 2026 elections that aren't certified yet.
 * If all are certified (or none exist), skip the live query entirely.
 */
function needsLiveData(district) {
  for (const seat of district.seats) {
    for (const e of seat.elections) {
      if (e.year === LIVE_ELECTION_YEAR && e.result_status !== 'Certified') {
        return true;
      }
    }
  }
  return false;
}

/** Build a sessionStorage cache key from sorted seat IDs. */
function liveCacheKey(seatIds) {
  return 'live_elections_' + seatIds.slice().sort((a, b) => a - b).join(',');
}

/** Get cached live data if still fresh (< TTL). Returns null if stale or missing. */
function getCachedLiveData(seatIds) {
  try {
    const raw = sessionStorage.getItem(liveCacheKey(seatIds));
    if (!raw) return null;
    const cached = JSON.parse(raw);
    if (Date.now() - cached.ts > LIVE_CACHE_TTL_MS) return null;
    return cached.data;
  } catch {
    return null;
  }
}

/** Cache live election data with current timestamp. */
function setCachedLiveData(seatIds, data) {
  try {
    sessionStorage.setItem(liveCacheKey(seatIds), JSON.stringify({
      ts: Date.now(),
      data: data,
    }));
  } catch {
    // sessionStorage unavailable or quota exceeded — degrade gracefully
  }
}

/**
 * Fetch 2026 elections for given seat IDs from Supabase PostgREST.
 * Uses resource embedding to include candidacies + candidate names in one query.
 * 5-second timeout via AbortController.
 */
async function fetchLiveElections(seatIds) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), LIVE_QUERY_TIMEOUT_MS);

  const select = 'id,seat_id,election_type,election_date,election_year,' +
    'result_status,total_votes_cast,is_open_seat,filing_deadline,forecast_rating,' +
    'candidacies(party,caucus,votes_received,vote_percentage,result,is_incumbent,is_write_in,' +
    'candidates(full_name))';

  const seatList = seatIds.join(',');
  const url = `${POSTGREST_BASE}/elections?select=${select}` +
    `&election_year=eq.${LIVE_ELECTION_YEAR}&seat_id=in.(${seatList})`;

  try {
    const resp = await fetch(url, {
      headers: {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
      },
      signal: controller.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) throw new Error('PostgREST ' + resp.status);
    return await resp.json();
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

/**
 * Transform a PostgREST election row into the static JSON shape the frontend expects.
 * Maps column names and sorts candidates (Won > Advanced > by votes desc).
 */
function transformElection(pg) {
  const candidates = (pg.candidacies || []).map(function(cy) {
    var obj = {
      name: cy.candidates ? cy.candidates.full_name : 'Unknown',
      party: cy.party,
      votes: cy.votes_received,
      pct: cy.vote_percentage != null ? parseFloat(cy.vote_percentage) : null,
      result: cy.result,
      is_incumbent: cy.is_incumbent,
      is_write_in: cy.is_write_in,
    };
    if (cy.caucus) obj.caucus = cy.caucus;
    return obj;
  });

  // Sort: Won first, then Advanced, then by votes descending (matches export script)
  candidates.sort(function(a, b) {
    var order = { Won: 0, Advanced: 1 };
    var aOrd = order[a.result] != null ? order[a.result] : 2;
    var bOrd = order[b.result] != null ? order[b.result] : 2;
    if (aOrd !== bOrd) return aOrd - bOrd;
    return (b.votes || 0) - (a.votes || 0);
  });

  return {
    year: pg.election_year,
    type: pg.election_type,
    date: pg.election_date,
    total_votes: pg.total_votes_cast,
    is_open_seat: pg.is_open_seat,
    result_status: pg.result_status,
    filing_deadline: pg.filing_deadline,
    forecast_rating: pg.forecast_rating,
    candidates: candidates,
  };
}

/**
 * Merge live 2026 elections into the district's static data (mutates in place).
 * For each seat with live data, removes static 2026 elections and appends live ones.
 * Seats with no live results keep their static elections unchanged.
 */
function mergeLiveElections(district, liveElections) {
  // Group live elections by seat_id
  var bySeat = {};
  for (var i = 0; i < liveElections.length; i++) {
    var pg = liveElections[i];
    if (!bySeat[pg.seat_id]) bySeat[pg.seat_id] = [];
    bySeat[pg.seat_id].push(transformElection(pg));
  }

  for (var j = 0; j < district.seats.length; j++) {
    var seat = district.seats[j];
    var liveForSeat = bySeat[seat.seat_id];
    if (!liveForSeat) continue;
    // Remove static 2026 elections, replace with live
    seat.elections = seat.elections
      .filter(function(e) { return e.year !== LIVE_ELECTION_YEAR; })
      .concat(liveForSeat);
  }
}
