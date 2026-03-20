/**
 * Office Overview renderer.
 * Fetches office_overviews.json and renders a callout-style overview box
 * for a given state + office key. Returns empty string if no entry exists.
 */
const OfficeOverview = (function() {
  let _cache = null;

  async function _load() {
    if (_cache) return _cache;
    try {
      const resp = await fetch('data/office_overviews.json');
      if (!resp.ok) return {};
      _cache = await resp.json();
      return _cache;
    } catch (e) {
      return {};
    }
  }

  /**
   * Render an overview HTML string for the given state and office.
   * @param {string} stateAbbr - e.g. "NY"
   * @param {string} officeKey - e.g. "ltgov", "governor", "ag", "sos", "legislature"
   * @param {string} [fallbackTitle] - default title if entry has no title field
   * @returns {Promise<string>} HTML string (empty if no overview exists)
   */
  async function render(stateAbbr, officeKey, fallbackTitle) {
    const data = await _load();
    const stateData = data[stateAbbr];
    if (!stateData) return '';
    const entry = stateData[officeKey];
    if (!entry || !entry.points || entry.points.length === 0) return '';

    const title = entry.title || fallbackTitle || 'About This Office';
    const bullets = entry.points.map(p => `<li>${p}</li>`).join('');
    const updated = entry.updated
      ? `<div class="office-overview-updated">Updated ${entry.updated}</div>`
      : '';

    return `<div class="office-overview">
      <div class="office-overview-title">${title}</div>
      <ul>${bullets}</ul>
      ${updated}
    </div>`;
  }

  return { render };
})();
