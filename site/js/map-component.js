/* ============================================================
   Elections Site — Reusable Map Component
   Renders a US state map from shared geometry data.
   Supports dynamic coloring, tooltips, legends, and recoloring.
   ============================================================ */

(function () {
  'use strict';

  var GEOMETRY_URL = 'data/us-states.json';
  var _geometryCache = null;
  var _geometryLoading = null;

  /** Load shared state geometry (cached after first load). */
  function loadGeometry() {
    if (_geometryCache) return Promise.resolve(_geometryCache);
    if (_geometryLoading) return _geometryLoading;
    _geometryLoading = fetch(GEOMETRY_URL).then(function (r) {
      if (!r.ok) throw new Error('Failed to load ' + GEOMETRY_URL);
      return r.json();
    }).then(function (data) {
      _geometryCache = data;
      return data;
    });
    return _geometryLoading;
  }

  /**
   * MapRenderer — renders a colored US map into a container element.
   *
   * @param {string|HTMLElement} container - CSS selector or DOM element
   * @param {Object} config
   * @param {Object} config.data - { AK: { category: '...', info: {...} }, ... }
   * @param {Object} config.colors - { category_name: '#hex', ... }
   * @param {Function} config.tooltipFn - function(stateAbbr, info) => HTML string
   * @param {Function} [config.onClickState] - function(stateAbbr) — default navigates to state.html
   * @param {Array} [config.legend] - [{label: '...', color: '#hex'}, ...]
   * @param {string} [config.defaultColor] - fallback color for states without data (default '#d9d9d9')
   * @param {string} [config.title] - optional title above the map
   * @param {string} [config.sourceText] - optional source text below the map
   */
  function MapRenderer(container, config) {
    this.el = typeof container === 'string' ? document.querySelector(container) : container;
    if (!this.el) throw new Error('MapRenderer: container not found');
    this.config = config;
    this.data = config.data || {};
    this.colors = config.colors || {};
    this.tooltipFn = config.tooltipFn || function () { return ''; };
    this.onClickState = config.onClickState || function (abbr) {
      window.location.href = 'state.html?st=' + abbr;
    };
    this.defaultColor = config.defaultColor || '#d9d9d9';
    this.stateElements = {};
    this._tooltip = null;
    this._svg = null;

    var self = this;
    loadGeometry().then(function (geo) {
      self._render(geo);
    });
  }

  MapRenderer.prototype._render = function (geo) {
    var self = this;
    var el = this.el;

    // Build wrapper HTML
    var wrapper = document.createElement('div');
    wrapper.className = 'map-renderer';

    // Title
    if (this.config.title) {
      var titleEl = document.createElement('h2');
      titleEl.className = 'map-renderer-title';
      titleEl.innerHTML = this.config.title;
      wrapper.appendChild(titleEl);
    }

    // Legend
    if (this.config.legend && this.config.legend.length > 0) {
      var legendEl = document.createElement('div');
      legendEl.className = 'map-renderer-legend';
      for (var i = 0; i < this.config.legend.length; i++) {
        var item = this.config.legend[i];
        var row = document.createElement('div');
        row.className = 'legend-row';
        row.innerHTML = '<div class="legend-swatch" style="background:' + item.color + '"></div>' +
          '<div class="legend-label">' + item.label + '</div>';
        if (item.tooltip) row.title = item.tooltip;
        row.style.cursor = item.tooltip ? 'help' : '';
        legendEl.appendChild(row);
      }
      wrapper.appendChild(legendEl);
    }

    // SVG map
    var mapDiv = document.createElement('div');
    mapDiv.className = 'map-renderer-svg';

    var ns = 'http://www.w3.org/2000/svg';
    var svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('viewBox', geo.viewBox);
    svg.setAttribute('class', 'us-map');
    svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', 'United States map showing state data');
    svg.style.width = '100%';
    svg.style.height = 'auto';

    var statesG = document.createElementNS(ns, 'g');
    statesG.setAttribute('id', 'states');

    var states = geo.states;
    var abbrs = Object.keys(states).sort();

    for (var j = 0; j < abbrs.length; j++) {
      var abbr = abbrs[j];
      var st = states[abbr];
      var stData = this.data[abbr] || {};
      var category = stData.category || '';
      var fillColor = this.colors[category] || this.defaultColor;

      var g = document.createElementNS(ns, 'g');
      g.setAttribute('class', 'state-group');
      g.setAttribute('data-state', abbr);
      g.setAttribute('role', 'button');
      g.setAttribute('aria-label', (st.name || abbr));
      g.setAttribute('tabindex', '0');
      g.style.cursor = 'pointer';

      // Fill paths
      var fillG = document.createElementNS(ns, 'g');
      fillG.setAttribute('class', 'state-fill');
      fillG.setAttribute('fill', fillColor);
      for (var p = 0; p < st.paths.length; p++) {
        var path = document.createElementNS(ns, 'path');
        path.setAttribute('d', st.paths[p]);
        fillG.appendChild(path);
      }
      g.appendChild(fillG);

      // Stroke paths (white borders)
      var strokeG = document.createElementNS(ns, 'g');
      strokeG.setAttribute('fill', 'none');
      strokeG.setAttribute('stroke', '#ffffff');
      strokeG.setAttribute('stroke-width', '1.0');
      for (var q = 0; q < st.paths.length; q++) {
        var sPath = document.createElementNS(ns, 'path');
        sPath.setAttribute('d', st.paths[q]);
        strokeG.appendChild(sPath);
      }
      g.appendChild(strokeG);

      // Small state SVG label paths (CT, DE, HI, etc.)
      if (st.label_paths) {
        for (var lp = 0; lp < st.label_paths.length; lp++) {
          var labelPath = document.createElementNS(ns, 'path');
          labelPath.setAttribute('d', st.label_paths[lp]);
          labelPath.setAttribute('fill', fillColor);
          labelPath.setAttribute('class', 'state-label-path');
          g.appendChild(labelPath);
        }
      }

      statesG.appendChild(g);
      this.stateElements[abbr] = g;
    }

    // Text labels
    var labelsG = document.createElementNS(ns, 'g');
    labelsG.setAttribute('id', 'labels');
    for (var k = 0; k < abbrs.length; k++) {
      var a = abbrs[k];
      var s = states[a];
      if (s.label_x != null) {
        var text = document.createElementNS(ns, 'text');
        text.setAttribute('x', s.label_x);
        text.setAttribute('y', s.label_y);
        text.setAttribute('class', 'state-label');
        text.textContent = a;
        labelsG.appendChild(text);
      }
    }

    svg.appendChild(statesG);
    svg.appendChild(labelsG);
    mapDiv.appendChild(svg);
    wrapper.appendChild(mapDiv);

    this._svg = svg;

    // Source text
    if (this.config.sourceText) {
      var srcEl = document.createElement('div');
      srcEl.className = 'map-renderer-source';
      srcEl.innerHTML = this.config.sourceText;
      wrapper.appendChild(srcEl);
    }

    // Tooltip
    var tooltipEl = document.createElement('div');
    tooltipEl.className = 'tooltip';
    tooltipEl.setAttribute('id', 'map-tooltip-' + Date.now());
    wrapper.appendChild(tooltipEl);
    this._tooltip = tooltipEl;

    el.appendChild(wrapper);

    // Attach events
    this._attachEvents(statesG);
  };

  MapRenderer.prototype._attachEvents = function (statesG) {
    var self = this;
    var tooltip = this._tooltip;

    statesG.addEventListener('mouseenter', handleEnter, true);
    statesG.addEventListener('mousemove', handleMove, true);
    statesG.addEventListener('mouseleave', handleLeave, true);
    statesG.addEventListener('click', handleClick, true);

    // Keyboard support: Enter/Space to activate state
    statesG.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' || e.key === ' ') {
        var g = findStateGroup(e);
        if (g) {
          e.preventDefault();
          self.onClickState(g.getAttribute('data-state'));
        }
      }
    }, true);

    function findStateGroup(e) {
      var el = e.target;
      while (el && el !== statesG) {
        if (el.classList && el.classList.contains('state-group')) return el;
        el = el.parentNode;
      }
      return null;
    }

    function handleEnter(e) {
      var g = findStateGroup(e);
      if (!g) return;
      var abbr = g.getAttribute('data-state');
      var info = (self.data[abbr] || {}).info || self.data[abbr] || {};
      var html = self.tooltipFn(abbr, info);
      if (html) {
        tooltip.innerHTML = html;
        tooltip.classList.add('visible');
      }
    }

    function handleMove(e) {
      var x = e.clientX + 14;
      var y = e.clientY + 14;
      if (x + 300 > window.innerWidth) x = e.clientX - 300;
      if (y + 200 > window.innerHeight) y = e.clientY - 200;
      tooltip.style.left = x + 'px';
      tooltip.style.top = y + 'px';
    }

    function handleLeave(e) {
      var g = findStateGroup(e);
      if (!g) return;
      // Only hide if we actually left the state group
      var related = e.relatedTarget;
      while (related) {
        if (related === g) return;
        related = related.parentNode;
      }
      tooltip.classList.remove('visible');
    }

    function handleClick(e) {
      var g = findStateGroup(e);
      if (!g) return;
      self.onClickState(g.getAttribute('data-state'));
    }
  };

  /**
   * Recolor the map with new data/colors without re-rendering SVG geometry.
   * @param {Object} newData - { AK: { category: '...', info: {...} }, ... }
   * @param {Object} newColors - { category_name: '#hex', ... }
   */
  MapRenderer.prototype.recolor = function (newData, newColors) {
    this.data = newData || this.data;
    this.colors = newColors || this.colors;
    var geo = _geometryCache;
    if (!geo) return;

    for (var abbr in this.stateElements) {
      var g = this.stateElements[abbr];
      var stData = this.data[abbr] || {};
      var category = stData.category || '';
      var fillColor = this.colors[category] || this.defaultColor;

      // Update fill
      var fillG = g.querySelector('.state-fill');
      if (fillG) fillG.setAttribute('fill', fillColor);

      // Update label paths for small states
      var labelPaths = g.querySelectorAll('.state-label-path');
      for (var i = 0; i < labelPaths.length; i++) {
        labelPaths[i].setAttribute('fill', fillColor);
      }
    }
  };

  /**
   * Update the legend display.
   * @param {Array} legend - [{label: '...', color: '#hex'}, ...]
   */
  MapRenderer.prototype.updateLegend = function (legend) {
    var legendEl = this.el.querySelector('.map-renderer-legend');
    if (!legendEl) return;
    legendEl.innerHTML = '';
    for (var i = 0; i < legend.length; i++) {
      var row = document.createElement('div');
      row.className = 'legend-row';
      row.innerHTML = '<div class="legend-swatch" style="background:' + legend[i].color + '"></div>' +
        '<div class="legend-label">' + legend[i].label + '</div>';
      if (legend[i].tooltip) row.title = legend[i].tooltip;
      row.style.cursor = legend[i].tooltip ? 'help' : '';
      legendEl.appendChild(row);
    }
  };

  // Expose globally
  window.MapRenderer = MapRenderer;

})();
