/* ============================================================
   Elections Site — Global Navigation
   Fetches nav/footer fragments, injects into page, sets active states.
   ============================================================ */

(function () {
  'use strict';

  // Topic detection: map filename patterns to nav topic keys
  var TOPIC_MAP = {
    'trifectas': 'trifectas',
    'legislatures': 'legislatures',
    'governors': 'governors',
    'ag': 'ag',
    'ltgov': 'ltgov',
    'sos': 'sos',
    'ballot-measures': 'ballot-measures',
    'candidates': 'candidates',
    'candidate': 'candidates',
    'calendar': 'calendar',
    'swing-calculator': 'swing-calculator',
    'state': 'state',
    'district': 'state',
    'statewide': 'statewide',
    'governor': 'governors',
  };

  // Subnav configs: topic -> array of {label, href, pattern}
  var SUBNAVS = {
    trifectas: [
      { label: 'Overview', href: 'trifectas-101.html' },
      { label: '2026 Elections', href: 'trifectas.html' },
      { label: 'Trends & Analytics', href: 'trifectas-analytics.html' },
    ],
    legislatures: [
      { label: 'Overview', href: 'legislatures-101.html' },
      { label: '2026 Elections', href: 'legislatures.html' },
      { label: 'Trends & Analytics', href: 'legislatures-analytics.html' },
    ],
    governors: [
      { label: 'Overview', href: 'governors-101.html' },
      { label: '2026 Elections', href: 'governors.html' },
      { label: 'Trends & Analytics', href: 'governors-analytics.html' },
    ],
    ag: [
      { label: 'Overview', href: 'ag-101.html' },
      { label: '2026 Elections', href: 'ag.html' },
      { label: 'Trends & Analytics', href: 'ag-analytics.html' },
    ],
    ltgov: [
      { label: 'Overview', href: 'ltgov-101.html' },
      { label: '2026 Elections', href: 'ltgov.html' },
      { label: 'Trends & Analytics', href: 'ltgov-analytics.html' },
    ],
    sos: [
      { label: 'Overview', href: 'sos-101.html' },
      { label: '2026 Elections', href: 'sos.html' },
      { label: 'Trends & Analytics', href: 'sos-analytics.html' },
    ],
    'ballot-measures': [
      { label: 'Overview', href: 'ballot-measures-101.html' },
      { label: '2026 Measures', href: 'ballot-measures.html' },
      { label: 'Trends & Analytics', href: 'ballot-measures-analytics.html' },
    ],
  };

  // State abbreviations for the state picker grid
  var STATE_ABBRS = [
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
  ];

  /** Detect current page filename */
  function getPageFile() {
    var path = window.location.pathname;
    var file = path.split('/').pop() || 'index.html';
    return file;
  }

  /** Detect the current topic from the filename */
  function detectTopic(filename) {
    // Strip extension and suffixes like -101, -analytics
    var base = filename.replace('.html', '');
    // Direct match first
    if (TOPIC_MAP[base]) return TOPIC_MAP[base];
    // Try removing suffixes
    var stripped = base.replace(/-101$/, '').replace(/-analytics$/, '').replace(/_partisan$/, '').replace(/_2026$/, '');
    if (TOPIC_MAP[stripped]) return TOPIC_MAP[stripped];
    return null;
  }

  /** Detect if this is a statewide-office subtopic */
  function isStatewideSubtopic(topic) {
    return topic === 'ag' || topic === 'ltgov' || topic === 'sos';
  }

  /** Fetch an HTML fragment and return as text */
  function fetchFragment(url) {
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error('Failed to load ' + url);
      return r.text();
    });
  }

  /** Build the state picker grid */
  function buildStateGrid(container) {
    for (var i = 0; i < STATE_ABBRS.length; i++) {
      var a = document.createElement('a');
      a.href = 'state.html?st=' + STATE_ABBRS[i];
      a.textContent = STATE_ABBRS[i];
      a.className = 'nav-state-link';
      container.appendChild(a);
    }
  }

  /** Set active state on nav items */
  function setActiveNav(nav, topic) {
    // Direct nav items
    var items = nav.querySelectorAll('.nav-item[data-topic]');
    for (var i = 0; i < items.length; i++) {
      if (items[i].getAttribute('data-topic') === topic) {
        items[i].classList.add('active');
      }
    }
    // Dropdown items
    var dropdowns = nav.querySelectorAll('.nav-dropdown');
    for (var j = 0; j < dropdowns.length; j++) {
      var dd = dropdowns[j];
      var links = dd.querySelectorAll('.nav-dropdown-menu a');
      for (var k = 0; k < links.length; k++) {
        if (links[k].getAttribute('data-topic') === topic) {
          links[k].classList.add('active');
          dd.querySelector('.nav-dropdown-btn').classList.add('active');
          break;
        }
      }
      // Also mark statewide parent if subtopic matches
      if (dd.getAttribute('data-topic') === 'statewide' && isStatewideSubtopic(topic)) {
        dd.querySelector('.nav-dropdown-btn').classList.add('active');
      }
    }
  }

  /** Build and inject the contextual subnav if applicable */
  function buildSubnav(topic, filename) {
    var config = SUBNAVS[topic];
    if (!config) return null;

    var div = document.createElement('div');
    div.className = 'topic-subnav';
    for (var i = 0; i < config.length; i++) {
      var a = document.createElement('a');
      a.href = config[i].href;
      a.textContent = config[i].label;
      if (config[i].href === filename) {
        a.classList.add('active');
      }
      div.appendChild(a);
    }
    return div;
  }

  /** Setup hamburger toggle */
  function setupHamburger() {
    var btn = document.getElementById('nav-hamburger');
    var menu = document.getElementById('nav-menu');
    if (!btn || !menu) return;
    btn.addEventListener('click', function () {
      var expanded = menu.classList.toggle('open');
      btn.classList.toggle('open');
      btn.setAttribute('aria-expanded', expanded);
    });
  }

  /** Setup dropdown hover/click behavior */
  function setupDropdowns(nav) {
    var dropdowns = nav.querySelectorAll('.nav-dropdown');
    for (var i = 0; i < dropdowns.length; i++) {
      (function (dd) {
        var btn = dd.querySelector('.nav-dropdown-btn');
        // Click toggle for mobile
        btn.addEventListener('click', function (e) {
          e.preventDefault();
          e.stopPropagation();
          // Close others
          var siblings = nav.querySelectorAll('.nav-dropdown.open');
          for (var s = 0; s < siblings.length; s++) {
            if (siblings[s] !== dd) siblings[s].classList.remove('open');
          }
          dd.classList.toggle('open');
        });
      })(dropdowns[i]);
    }
    // Close dropdowns when clicking outside
    document.addEventListener('click', function (e) {
      if (!e.target.closest('.nav-dropdown')) {
        var open = nav.querySelectorAll('.nav-dropdown.open');
        for (var x = 0; x < open.length; x++) open[x].classList.remove('open');
      }
    });
  }

  /** Main init */
  function init() {
    var navTarget = document.getElementById('global-nav');
    var footerTarget = document.getElementById('global-footer');

    if (!navTarget && !footerTarget) return;

    // Determine base path for fragments (handle subdirectories)
    var basePath = '';
    var scripts = document.querySelectorAll('script[src*="nav.js"]');
    if (scripts.length > 0) {
      var src = scripts[0].getAttribute('src');
      basePath = src.replace('js/nav.js', '');
    }

    var promises = [];
    if (navTarget) promises.push(fetchFragment(basePath + 'components/nav.html'));
    else promises.push(Promise.resolve(null));
    if (footerTarget) promises.push(fetchFragment(basePath + 'components/footer.html'));
    else promises.push(Promise.resolve(null));

    Promise.all(promises).then(function (results) {
      var filename = getPageFile();
      var topic = detectTopic(filename);

      // Inject nav
      if (results[0] && navTarget) {
        navTarget.innerHTML = results[0];
        var nav = navTarget;

        // Build state picker grid
        var stateGrid = document.getElementById('nav-state-grid');
        if (stateGrid) buildStateGrid(stateGrid);

        // Set active nav item
        if (topic) setActiveNav(nav, topic);

        // Setup interactions
        setupHamburger();
        setupDropdowns(nav);

        // Insert subnav after the nav element if applicable
        var subnav = buildSubnav(topic, filename);
        if (subnav) {
          var subnavWrapper = document.getElementById('topic-subnav');
          if (subnavWrapper) {
            subnavWrapper.appendChild(subnav);
          }
        }
      }

      // Inject footer
      if (results[1] && footerTarget) {
        footerTarget.innerHTML = results[1];
      }
    }).catch(function (err) {
      console.warn('Nav/footer load failed:', err);
    });
  }

  // Run after DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
