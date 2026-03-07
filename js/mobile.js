// js/mobile.js
// Mobile Controller – redesigned for full‑screen map with floating tiles

class MobileController {
    constructor(app) {
        this.app = app;
        this.map = null;
        // In the MobileController constructor
        this.quantileBreaks = [];
        this.elements = {
            container: null,
            kpiAvg: null,
            kpiTotal: null,
            kpiRoutes: null,
            searchInput: null,
            searchBtn: null,
            filterBtn: null,
            themeToggle: null,
            tabTime: null,
            tabFreq: null,
            tabAbout: null,
            viewRoutes: null,
            viewWards: null,
            viewNeighbourhoods: null,
            viewHotspots: null,
            legendToggle: null,
            legendPanel: null,
            legendClose: null,
            legendMin: null,
            legendMax: null,
            legendGradient: null,
            legendMetric: null,
            filterDrawer: null,
            filterDrawerClose: null,
            filterDrawerContent: null,
            filterDrawerReset: null,
            filterDrawerApply: null,
            aboutPanel: null,
            aboutClose: null,
            aboutContent: null,
            searchResults: null          // new: mobile search results container
        };

        this.filterDrawerOpen = false;
        this.searchDebounce = null;
    }

    init() {
        console.log('📱 Initialising redesigned mobile interface...');

        document.body.classList.add('mobile-active');

        let mobileContainer = document.getElementById('mobileContainer');
        if (!mobileContainer) {
            mobileContainer = this.createMobileContainer();
            document.getElementById('app').appendChild(mobileContainer);
        } else {
            mobileContainer.style.display = 'flex';
            mobileContainer.innerHTML = '';
        }
        this.elements.container = mobileContainer;

        this.buildLayout();
        this.cacheElements();
        this.attachEvents();

        this.updateLegend(this.app.state.currentMetric);
        this.populateAbout();

        setTimeout(() => {
            if (this.app.map) this.app.map.invalidateSize();
        }, 100);

        console.log('✅ Mobile interface ready');
    }

    createMobileContainer() {
        const container = document.createElement('div');
        container.id = 'mobileContainer';
        container.className = 'mobile-container';
        container.style.display = 'flex';
        container.style.flexDirection = 'column';
        container.style.position = 'absolute';
        container.style.top = '0';
        container.style.left = '0';
        container.style.width = '100%';
        container.style.height = '100%';
        container.style.pointerEvents = 'none';
        container.style.zIndex = '1000';
        return container;
    }

    buildLayout() {
        const container = this.elements.container;

        // KPI row
        const kpiRow = document.createElement('div');
        kpiRow.className = 'mobile-row kpi-row';
        kpiRow.innerHTML = `
            <div class="mobile-tile kpi-tile" id="mobileKpiAvg">
                <span class="kpi-label">Avg Delay</span>
                <span class="kpi-value">--</span>
            </div>
            <div class="mobile-tile kpi-tile" id="mobileKpiTotal">
                <span class="kpi-label">Total Incidents</span>
                <span class="kpi-value">--</span>
            </div>
            <div class="mobile-tile kpi-tile" id="mobileKpiRoutes">
                <span class="kpi-label">Routes</span>
                <span class="kpi-value">--</span>
            </div>
        `;
        container.appendChild(kpiRow);

        // Search row with integrated results container
        const searchRow = document.createElement('div');
        searchRow.className = 'mobile-row search-row';
        searchRow.innerHTML = `
            <div class="mobile-tile search-tile">
                <input type="text" id="mobileSearchInput" placeholder="Search route # or name" class="mobile-search-input">
                <button id="mobileSearchBtn" class="mobile-icon-btn"><i class="fas fa-search"></i></button>
                <div id="mobileSearchResults" class="mobile-search-results"></div>
            </div>
            <div class="mobile-tile icon-tile" id="mobileFilterBtn">
                <i class="fas fa-sliders-h"></i>
            </div>
            <div class="mobile-tile icon-tile" id="mobileThemeToggle">
                <i class="fas fa-moon"></i>
            </div>
            <div class="mobile-tile icon-tile" id="mobileRankingsBtn">
                <i class="fas fa-list"></i>
            </div>
        `;
        container.appendChild(searchRow);

        // Tabs
        const tabRow = document.createElement('div');
        tabRow.className = 'mobile-row tab-row';
        tabRow.innerHTML = `
            <div class="mobile-tile tab-tile active" id="mobileTabTime">Time</div>
            <div class="mobile-tile tab-tile" id="mobileTabFreq">Frequency</div>
            <div class="mobile-tile tab-tile" id="mobileTabAbout">About</div>
        `;
        container.appendChild(tabRow);

        // View row
        const viewRow = document.createElement('div');
        viewRow.className = 'mobile-row view-row';
        viewRow.innerHTML = `
            <div class="mobile-tile view-tile active" data-view="routes" id="mobileViewRoutes">Routes</div>
            <div class="mobile-tile view-tile" data-view="wards" id="mobileViewWards">Wards</div>
            <div class="mobile-tile view-tile" data-view="neighbourhoods" id="mobileViewNeighbourhoods">Districts</div>
            <div class="mobile-tile view-tile" data-view="hotspots" id="mobileViewHotspots">Hotspots</div>
        `;
        container.appendChild(viewRow);

        // Legend toggle
        const legendToggle = document.createElement('button');
        legendToggle.id = 'mobileLegendToggle';
        legendToggle.className = 'mobile-legend-toggle';
        legendToggle.innerHTML = '<i class="fas fa-info"></i>';
        container.appendChild(legendToggle);

        // Legend panel
        const legendPanel = document.createElement('div');
        legendPanel.id = 'mobileLegendPanel';
        legendPanel.className = 'mobile-legend-panel';
        legendPanel.innerHTML = `
            <div class="legend-content">
                <div class="legend-gradient" id="mobileLegendGradient"></div>
                <div class="legend-labels">
                    <span id="mobileLegendMin">0</span>
                    <span id="mobileLegendMax">1000</span>
                </div>
                <span class="legend-metric" id="mobileLegendMetric">Avg Delay (min)</span>
            </div>
        `;
        container.appendChild(legendPanel);

        // Filter drawer
        const filterDrawer = document.createElement('div');
        filterDrawer.id = 'mobileFilterDrawer';
        filterDrawer.className = 'filter-drawer';
        filterDrawer.innerHTML = `
            <div class="drawer-header">
                <h3>Filters</h3>
                <button id="mobileFilterDrawerClose" class="drawer-close"><i class="fas fa-times"></i></button>
            </div>
            <div class="drawer-content" id="mobileFilterDrawerContent"></div>
            <div class="drawer-footer">
                <button id="mobileFilterReset" class="btn-secondary">Reset</button>
                <button id="mobileFilterApply" class="btn-primary">Apply</button>
            </div>
        `;
        container.appendChild(filterDrawer);

        // About panel
        const aboutPanel = document.createElement('div');
        aboutPanel.id = 'mobileAboutPanel';
        aboutPanel.className = 'about-panel';
        aboutPanel.innerHTML = `
            <div class="about-header">
                <h2></h2>
                <button id="mobileAboutClose" class="about-close"><i class="fas fa-times"></i></button>
            </div>
            <div class="about-content" id="mobileAboutContent"></div>
        `;
        container.appendChild(aboutPanel);

        // Rankings drawer
        const rankingsDrawer = document.createElement('div');
        rankingsDrawer.id = 'mobileRankingsDrawer';
        rankingsDrawer.className = 'rankings-drawer';  // we'll style similar to filter-drawer
        rankingsDrawer.innerHTML = `
            <div class="drawer-header">
                <h3>Rankings</h3>
                <button id="mobileRankingsDrawerClose" class="drawer-close"><i class="fas fa-times"></i></button>
            </div>
            <div class="drawer-content" id="mobileRankingsDrawerContent"></div>
        `;
        container.appendChild(rankingsDrawer);
    }


    

    cacheElements() {
        this.elements.kpiAvg = document.getElementById('mobileKpiAvg');
        this.elements.kpiTotal = document.getElementById('mobileKpiTotal');
        this.elements.kpiRoutes = document.getElementById('mobileKpiRoutes');
        this.elements.searchInput = document.getElementById('mobileSearchInput');
        this.elements.searchBtn = document.getElementById('mobileSearchBtn');
        this.elements.filterBtn = document.getElementById('mobileFilterBtn');
        this.elements.themeToggle = document.getElementById('mobileThemeToggle');
        this.elements.tabTime = document.getElementById('mobileTabTime');
        this.elements.tabFreq = document.getElementById('mobileTabFreq');
        this.elements.tabAbout = document.getElementById('mobileTabAbout');
        this.elements.viewRoutes = document.getElementById('mobileViewRoutes');
        this.elements.viewWards = document.getElementById('mobileViewWards');
        this.elements.viewNeighbourhoods = document.getElementById('mobileViewNeighbourhoods');
        this.elements.viewHotspots = document.getElementById('mobileViewHotspots');
        this.elements.legendToggle = document.getElementById('mobileLegendToggle');
        this.elements.legendPanel = document.getElementById('mobileLegendPanel');
        this.elements.legendClose = document.getElementById('mobileLegendClose');
        this.elements.legendMin = document.getElementById('mobileLegendMin');
        this.elements.legendMax = document.getElementById('mobileLegendMax');
        this.elements.legendGradient = document.getElementById('mobileLegendGradient');
        this.elements.legendMetric = document.getElementById('mobileLegendMetric');
        this.elements.filterDrawer = document.getElementById('mobileFilterDrawer');
        this.elements.filterDrawerClose = document.getElementById('mobileFilterDrawerClose');
        this.elements.filterDrawerContent = document.getElementById('mobileFilterDrawerContent');
        this.elements.filterDrawerReset = document.getElementById('mobileFilterReset');
        this.elements.filterDrawerApply = document.getElementById('mobileFilterApply');
        this.elements.aboutPanel = document.getElementById('mobileAboutPanel');
        this.elements.aboutClose = document.getElementById('mobileAboutClose');
        this.elements.aboutContent = document.getElementById('mobileAboutContent');
        this.elements.searchResults = document.getElementById('mobileSearchResults');
        this.elements.rankingsBtn = document.getElementById('mobileRankingsBtn');
        this.elements.rankingsDrawer = document.getElementById('mobileRankingsDrawer');
        this.elements.rankingsDrawerClose = document.getElementById('mobileRankingsDrawerClose');
        this.elements.rankingsDrawerContent = document.getElementById('mobileRankingsDrawerContent');
    }

    attachEvents() {
        // Search input with debounce
        if (this.elements.searchInput) {
            this.elements.searchInput.addEventListener('input', () => {
                clearTimeout(this.searchDebounce);
                const query = this.elements.searchInput.value.trim();
                this.searchDebounce = setTimeout(() => {
                    this.handleSearch(query);
                }, 300);
            });
        }
        if (this.elements.searchBtn) {
            this.elements.searchBtn.addEventListener('click', () => {
                const query = this.elements.searchInput?.value.trim() || '';
                this.handleSearch(query);
            });
        }

        // Filter button
        if (this.elements.filterBtn) {
            this.elements.filterBtn.addEventListener('click', () => this.openFilterDrawer());
        }

        // Theme toggle
        if (this.elements.themeToggle) {
            this.elements.themeToggle.addEventListener('click', () => {
                this.app.handleThemeChange();
                this.updateThemeIcon();
            });
        }

        // Tabs
        if (this.elements.tabTime) {
            this.elements.tabTime.addEventListener('click', () => this.setActiveTab('time'));
        }
        if (this.elements.tabFreq) {
            this.elements.tabFreq.addEventListener('click', () => this.setActiveTab('frequency'));
        }
        if (this.elements.tabAbout) {
            this.elements.tabAbout.addEventListener('click', () => this.openAbout());
        }

        // View buttons
        if (this.elements.viewRoutes) {
            this.elements.viewRoutes.addEventListener('click', () => this.setActiveView('routes'));
        }
        if (this.elements.viewWards) {
            this.elements.viewWards.addEventListener('click', () => this.setActiveView('wards'));
        }
        if (this.elements.viewNeighbourhoods) {
            this.elements.viewNeighbourhoods.addEventListener('click', () => this.setActiveView('neighbourhoods'));
        }
        if (this.elements.viewHotspots) {
            this.elements.viewHotspots.addEventListener('click', () => this.setActiveView('hotspots'));
        }

        // Legend
        if (this.elements.legendToggle) {
            this.elements.legendToggle.addEventListener('click', () => this.toggleLegend());
        }
        if (this.elements.legendClose) {
            this.elements.legendClose.addEventListener('click', () => this.closeLegend());
        }

        // Filter drawer
        if (this.elements.filterDrawerClose) {
            this.elements.filterDrawerClose.addEventListener('click', () => this.closeFilterDrawer());
        }
        if (this.elements.filterDrawerReset) {
            this.elements.filterDrawerReset.addEventListener('click', () => this.resetFilters());
        }
        if (this.elements.filterDrawerApply) {
            this.elements.filterDrawerApply.addEventListener('click', () => {
                this.applyFilters();
                this.closeFilterDrawer();
            });
        }

        if (this.elements.rankingsBtn) {
            this.elements.rankingsBtn.addEventListener('click', () => this.openRankingsDrawer());
        }
        if (this.elements.rankingsDrawerClose) {
            this.elements.rankingsDrawerClose.addEventListener('click', () => this.closeRankingsDrawer());
        }

        // Close rankings drawer
        if (this.rankingsDrawerOpen && this.elements.rankingsDrawer) {
            if (!this.elements.rankingsDrawer.contains(e.target) && e.target !== this.elements.rankingsBtn) {
                this.closeRankingsDrawer();
            }
        }

        // About close
        if (this.elements.aboutClose) {
            this.elements.aboutClose.addEventListener('click', () => this.closeAbout());
        }

        // Global clicks to close search results, legend, filter drawer
        document.addEventListener('click', (e) => {
            // Search results
            if (this.elements.searchResults && this.elements.searchResults.style.display === 'block') {
                if (!this.elements.searchResults.contains(e.target) &&
                    e.target !== this.elements.searchInput &&
                    e.target !== this.elements.searchBtn) {
                    this.elements.searchResults.style.display = 'none';
                }
            }
            // Legend
            if (this.elements.legendPanel && this.elements.legendPanel.classList.contains('visible')) {
                if (!this.elements.legendPanel.contains(e.target) && e.target !== this.elements.legendToggle) {
                    this.closeLegend();
                }
            }
            // Filter drawer
            if (this.filterDrawerOpen && this.elements.filterDrawer) {
                if (!this.elements.filterDrawer.contains(e.target) && e.target !== this.elements.filterBtn) {
                    this.closeFilterDrawer();
                }
            }
        });
    }


    openRankingsDrawer() {
        if (!this.elements.rankingsDrawer) return;
        if (this.app.rightSidebar) {
            this.elements.rankingsDrawerContent.innerHTML = this.app.rightSidebar.getMobileHTML();
        } else {
            this.elements.rankingsDrawerContent.innerHTML = '<p>No data</p>';
        }
        this.elements.rankingsDrawer.classList.add('open');
        this.rankingsDrawerOpen = true;
        this.attachRankingsModeListeners();
    }

    attachRankingsModeListeners() {
        const modeButtons = this.elements.rankingsDrawer.querySelectorAll('.mode-option');
        if (!modeButtons.length) return;
        modeButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const mode = e.target.dataset.mode;
                modeButtons.forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.updateRankingsLists(mode);
            });
        });
    }

    updateRankingsLists(mode) {
        if (!this.app.rightSidebar) return;
        const lists = this.app.rightSidebar.getMobileListsHTML(mode);
        const globalContainer = this.elements.rankingsDrawer.querySelector('#mobileRankingsGlobal');
        const visibleContainer = this.elements.rankingsDrawer.querySelector('#mobileRankingsVisible');
        if (globalContainer) globalContainer.innerHTML = lists.global;
        if (visibleContainer) visibleContainer.innerHTML = lists.visible;
    }
    
    closeRankingsDrawer() {
        if (this.elements.rankingsDrawer) {
            this.elements.rankingsDrawer.classList.remove('open');
            this.rankingsDrawerOpen = false;
        }
    }
    // ==================== SEARCH ====================

    handleSearch(query) {
        // Update suggestions
        this.showSearchSuggestions(query);
        // Apply filter to app (debounced already)
        this.app.applyFilters({ searchQuery: query });
    }

    showSearchSuggestions(query) {
        const resultsContainer = this.elements.searchResults;
        if (!resultsContainer) return;

        const trimmed = query ? query.trim() : '';
        if (!trimmed || this.app.state.currentView !== 'routes') {
            resultsContainer.style.display = 'none';
            return;
        }

        const routes = this.app.state.routesData || [];
        const matches = routes.filter(route => {
            const routeNum = route.route ? route.route.toString().toLowerCase() : '';
            const routeName = route.longName ? route.longName.toLowerCase() : '';
            const q = trimmed.toLowerCase();
            return routeNum.includes(q) || routeName.includes(q);
        }).slice(0, 10);

        if (matches.length === 0) {
            resultsContainer.innerHTML = '<div class="mobile-search-result-item">No routes found</div>';
            resultsContainer.style.display = 'block';
            return;
        }

        let html = '';
        matches.forEach(r => {
            html += `<div class="mobile-search-result-item" data-route="${r.route}">
                <span class="mobile-search-result-route">${r.route}</span>
                <span class="mobile-search-result-name">${r.longName || ''}</span>
            </div>`;
        });
        resultsContainer.innerHTML = html;
        resultsContainer.style.display = 'block';

        // Add click handlers to each result
        resultsContainer.querySelectorAll('.mobile-search-result-item').forEach(item => {
            item.addEventListener('click', (e) => {
                const routeNum = item.dataset.route;
                if (routeNum) {
                    // Set input value
                    if (this.elements.searchInput) {
                        this.elements.searchInput.value = routeNum;
                    }
                    // Highlight route on map (and show popup) – pass route number
                    if (this.app.mapVisualizer) {
                        this.app.mapVisualizer.highlightRoute(routeNum);
                    }
                    // Hide results
                    resultsContainer.style.display = 'none';
                }
            });
        });
    }

    // ==================== TABS & VIEWS ====================

    setActiveTab(tab) {
        [this.elements.tabTime, this.elements.tabFreq, this.elements.tabAbout].forEach(el => {
            if (el) el.classList.remove('active');
        });
        if (tab === 'time') this.elements.tabTime?.classList.add('active');
        if (tab === 'frequency') this.elements.tabFreq?.classList.add('active');
        if (tab === 'about') {
            this.openAbout();
            return;
        }

        this.app.state.currentMetric = tab;
        this.app.updateView();
        this.updateLegendMetricLabel();
    }

    setActiveView(view) {
        [this.elements.viewRoutes, this.elements.viewWards, this.elements.viewNeighbourhoods, this.elements.viewHotspots].forEach(el => {
            if (el) el.classList.remove('active');
        });
        const viewMap = {
            routes: this.elements.viewRoutes,
            wards: this.elements.viewWards,
            neighbourhoods: this.elements.viewNeighbourhoods,
            hotspots: this.elements.viewHotspots
        };
        if (viewMap[view]) viewMap[view].classList.add('active');

        this.app.state.currentView = view;
        this.app.updateView();

        if (this.elements.searchInput) {
            this.elements.searchInput.disabled = (view !== 'routes');
            this.elements.searchInput.placeholder = (view === 'routes') ? 'Search route # or name' : 'Search only in Routes';
        }
        // Hide any open search results when changing view
        if (this.elements.searchResults) {
            this.elements.searchResults.style.display = 'none';
        }
    }

    // ==================== FILTER DRAWER ====================

    openFilterDrawer() {
        if (!this.elements.filterDrawer) return;
        this.buildFilterDrawerFromScratch();
        this.elements.filterDrawer.classList.add('open');
        this.filterDrawerOpen = true;
    }

    closeFilterDrawer() {
        if (!this.elements.filterDrawer) return;
        this.elements.filterDrawer.classList.remove('open');
        this.filterDrawerOpen = false;
    }

    buildFilterDrawerFromScratch() {
        const contentDiv = this.elements.filterDrawerContent;
        if (!contentDiv) return;
        contentDiv.innerHTML = '';

        const view = this.app.state.currentView;
        const years = this.app.getDistinctYearsForView(view) || [];
        const transitTypes = this.app.getDistinctTransitForView(view) || [];
        const categories = this.app.getDistinctCategoriesForView(view) || [];

        const currentFilters = this.app.state.filters;

        // Helper to create a dropdown section
        const createDropdown = (label, options, selectedValues) => {
            const container = document.createElement('div');
            container.className = 'filter-dropdown';

            // Dropdown header (clickable)
            const header = document.createElement('div');
            header.className = 'dropdown-header';
            header.innerHTML = `
                <span class="dropdown-label">${label}</span>
                <span class="dropdown-selected-count">${selectedValues.length} selected</span>
                <i class="fas fa-chevron-down dropdown-icon"></i>
            `;

            // Dropdown content (checkbox panel, hidden initially)
            const content = document.createElement('div');
            content.className = 'dropdown-content';
            content.style.display = 'none';

            // "Select All" option
            const selectAllLabel = document.createElement('label');
            selectAllLabel.className = 'checkbox-label select-all';
            const selectAllCheck = document.createElement('input');
            selectAllCheck.type = 'checkbox';
            selectAllCheck.className = 'select-all-checkbox';
            selectAllCheck.checked = (selectedValues.length === options.length);
            selectAllLabel.appendChild(selectAllCheck);
            selectAllLabel.appendChild(document.createTextNode('Select All'));
            content.appendChild(selectAllLabel);

            // Individual options
            options.forEach(opt => {
                const label = document.createElement('label');
                label.className = 'checkbox-label';
                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.value = opt;
                cb.checked = selectedValues.includes(opt);
                label.appendChild(cb);
                label.appendChild(document.createTextNode(opt));
                content.appendChild(label);
            });

            container.appendChild(header);
            container.appendChild(content);

            // Toggle dropdown on header click
            header.addEventListener('click', (e) => {
                // Optional: close other dropdowns? For simplicity, just toggle this one.
                const isOpen = content.style.display === 'block';
                content.style.display = isOpen ? 'none' : 'block';
                header.querySelector('.dropdown-icon').style.transform = isOpen ? 'rotate(0deg)' : 'rotate(180deg)';
            });

            // Update selected count when checkboxes change
            const updateCount = () => {
                const checkboxes = content.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
                const checkedCount = Array.from(checkboxes).filter(cb => cb.checked).length;
                header.querySelector('.dropdown-selected-count').textContent = `${checkedCount} selected`;
            };

            // "Select All" logic
            selectAllCheck.addEventListener('change', (e) => {
                const checkboxes = content.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
                checkboxes.forEach(cb => cb.checked = e.target.checked);
                updateCount();
            });

            // Individual checkboxes update select all and count
            content.addEventListener('change', (e) => {
                if (e.target.classList.contains('select-all-checkbox')) return;
                const checkboxes = content.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
                const allChecked = Array.from(checkboxes).every(cb => cb.checked);
                selectAllCheck.checked = allChecked;
                updateCount();
            });

            return container;
        };

        const selectedYears = currentFilters.year || years;
        const selectedTransit = currentFilters.transitTypes || transitTypes;
        const selectedCategories = currentFilters.incidentCategories || categories;

        contentDiv.appendChild(createDropdown('Year', years, selectedYears));
        contentDiv.appendChild(createDropdown('Transit Type', transitTypes, selectedTransit));
        contentDiv.appendChild(createDropdown('Incident Category', categories, selectedCategories));

        // Active only checkbox (remains as simple checkbox)
        const activeGroup = document.createElement('div');
        activeGroup.className = 'filter-group';
        activeGroup.innerHTML = `
            <label class="checkbox-label">
                <input type="checkbox" id="mobileActiveOnly" ${currentFilters.activeOnly ? 'checked' : ''}> Hide inactive routes
            </label>
        `;
        contentDiv.appendChild(activeGroup);
    }

    resetFilters() {
        if (this.app.uiController) {
            this.app.uiController.resetFilters();
        }
        this.closeFilterDrawer();
    }

    applyFilters() {
        const content = this.elements.filterDrawerContent;

        // Year checkboxes
        const yearDropdown = content.querySelector('.filter-dropdown:nth-child(1) .dropdown-content');
        const yearCheckboxes = yearDropdown ? yearDropdown.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)') : [];
        
        // Transit checkboxes
        const transitDropdown = content.querySelector('.filter-dropdown:nth-child(2) .dropdown-content');
        const transitCheckboxes = transitDropdown ? transitDropdown.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)') : [];
        
        // Category checkboxes
        const categoryDropdown = content.querySelector('.filter-dropdown:nth-child(3) .dropdown-content');
        const categoryCheckboxes = categoryDropdown ? categoryDropdown.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)') : [];

        const activeOnlyCheck = content.querySelector('#mobileActiveOnly');

        const selectedYears = Array.from(yearCheckboxes).filter(cb => cb.checked).map(cb => parseInt(cb.value, 10));
        const selectedTransit = Array.from(transitCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
        const selectedCategories = Array.from(categoryCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
        const activeOnly = activeOnlyCheck ? activeOnlyCheck.checked : true;

        const totalYears = yearCheckboxes.length;
        const totalTransit = transitCheckboxes.length;
        const totalCategories = categoryCheckboxes.length;

        const filters = {
            year: selectedYears.length === totalYears ? null : selectedYears,
            transitTypes: selectedTransit.length === totalTransit ? null : selectedTransit,
            incidentCategories: selectedCategories.length === totalCategories ? null : selectedCategories,
            activeOnly: activeOnly,
            searchQuery: this.app.state.filters.searchQuery
        };

        this.app.applyFilters(filters);
    }

    // ==================== LEGEND ====================


    setLegendVisibility(visible) {
        if (this.elements.legendPanel) {
            if (visible) {
                this.elements.legendPanel.classList.remove('hidden');
            } else {
                this.elements.legendPanel.classList.add('hidden');
                this.elements.legendPanel.classList.remove('visible'); // close if open
            }
        }
        if (this.elements.legendToggle) {
            if (visible) {
                this.elements.legendToggle.classList.remove('hidden');
            } else {
                this.elements.legendToggle.classList.add('hidden');
            }
        }
    }

    toggleLegend() {
        if (this.elements.legendPanel) {
            this.elements.legendPanel.classList.toggle('visible');
        }
    }

    closeLegend() {
        if (this.elements.legendPanel) {
            this.elements.legendPanel.classList.remove('visible');
        }
    }


    updateDiscreteLegend(metric) {
        // Safety: if hotspots, hide and return
        if (this.app.state.currentView === 'hotspots') {
            this.setLegendVisibility(false);
            return;
        }

        const panel = this.elements.legendPanel;
        if (!panel) {
            console.warn('Mobile legend panel not found');
            return;
        }
        if (!this.quantileBreaks || this.quantileBreaks.length < 2) {
            console.warn('No quantile breaks available');
            return;
        }

        const timeColors = ['#E0B3FF', '#C180FF', '#A34DFF', '#7A1FFF', '#5A00B3'];
        const freqColors = ['#98D4E2', '#6A93C1', '#53639F', '#6C387F', '#42144F'];
        const colors = metric === 'time' ? timeColors : freqColors;
        const breaks = this.quantileBreaks;

        let html = '<div class="legend-discrete">';
        html += `<div class="legend-metric-title">${metric === 'time' ? 'Avg Delay (min)' : 'Incident Count'}</div>`;

        for (let i = 0; i < colors.length; i++) {
            const low = breaks[i].toFixed(metric === 'time' ? 1 : 0);
            const high = breaks[i + 1].toFixed(metric === 'time' ? 1 : 0);
            html += `
                <div class="legend-item">
                    <span class="legend-color" style="background: ${colors[i]}; width: 20px; height: 12px; display: inline-block; margin-right: 8px;"></span>
                    <span class="legend-range">${low} – ${high}</span>
                </div>
            `;
        }
        html += '</div>';

        // Replace content – preserve header structure but without "Legend" text
        const contentDiv = panel.querySelector('.legend-content');
        if (contentDiv) {
            contentDiv.innerHTML = html;
        } else {
            // Rebuild panel without the "Legend" header text
            panel.innerHTML = `
                <div class="legend-header">
                    <button id="mobileLegendClose" class="legend-close"><i class="fas fa-times"></i></button>
                </div>
                <div class="legend-content">${html}</div>
            `;
            document.getElementById('mobileLegendClose')?.addEventListener('click', () => this.closeLegend());
        }
        console.log('Mobile discrete legend updated', breaks);
    }


    updateLegend(metric) {
        // Hide legend if current view is hotspots
        if (this.app.state.currentView === 'hotspots') {
            this.setLegendVisibility(false);
            return;
        } else {
            this.setLegendVisibility(true);
        }

        if (!this.elements.legendPanel) return;

        // If quantile breaks are available, use discrete legend
        if (this.quantileBreaks && this.quantileBreaks.length > 1) {
            this.updateDiscreteLegend(metric);
            return;
        }

        // Fallback to continuous gradient (original code)
        const data = this.app.getFilteredData();
        let min = 0, max = 0;
        if (metric === 'time') {
            const values = data.map(d => d.avgDelay || 0).filter(v => v > 0);
            min = values.length ? Math.min(...values) : 0;
            max = values.length ? Math.max(...values) : 60;
        } else {
            const values = data.map(d => d.totalCount || 0).filter(v => v > 0);
            min = values.length ? Math.min(...values) : 0;
            max = values.length ? Math.max(...values) : 1000;
        }

        const formatValue = (val, isTime) => isTime ? val.toFixed(1) : Math.round(val).toString();
        const formatCompact = (num, isTime) => {
            if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
            if (num >= 1000) return (num / 1000).toFixed(1) + 'k';
            return formatValue(num, isTime);
        };

        if (this.elements.legendMin) {
            this.elements.legendMin.textContent = formatCompact(min, metric === 'time');
        }
        if (this.elements.legendMax) {
            this.elements.legendMax.textContent = formatCompact(max, metric === 'time');
        }
        if (this.elements.legendMetric) {
            this.elements.legendMetric.textContent = metric === 'time' ? 'Avg Delay (min)' : 'Incident Count';
        }
        if (this.elements.legendGradient) {
            if (metric === 'time') {
                this.elements.legendGradient.style.background = 'linear-gradient(to right, #fbc4c4, #8B0000)';
            } else {
                this.elements.legendGradient.style.background = 'linear-gradient(to right, #b8e2ff, #aa00ff)';
            }
        }
    }

    updateLegendMetricLabel() {
        const metric = this.app.state.currentMetric;
        if (this.elements.legendMetric) {
            this.elements.legendMetric.textContent = metric === 'time' ? 'Avg Delay (min)' : 'Incident Count';
        }
    }

    // ==================== KPI ====================

    updateKPI(avgDelay, totalIncidents, routesReporting) {
        if (this.elements.kpiAvg) {
            this.elements.kpiAvg.querySelector('.kpi-value').textContent = avgDelay;
        }
        if (this.elements.kpiTotal) {
            this.elements.kpiTotal.querySelector('.kpi-value').textContent = totalIncidents.toLocaleString();
        }
        if (this.elements.kpiRoutes) {
            this.elements.kpiRoutes.querySelector('.kpi-value').textContent = routesReporting.toLocaleString();
        }
    }

    // ==================== ABOUT ====================

    openAbout() {
        if (this.elements.aboutPanel) {
            // Hide any open overlays
            if (this.elements.searchResults) {
                this.elements.searchResults.style.display = 'none';
            }
            this.closeFilterDrawer();
            this.closeLegend();

            // Show about panel
            this.elements.aboutPanel.classList.add('active');

            // Deactivate all metric tabs
            [this.elements.tabTime, this.elements.tabFreq, this.elements.tabAbout].forEach(el => {
                if (el) el.classList.remove('active');
            });
            // Activate the About tab
            if (this.elements.tabAbout) this.elements.tabAbout.classList.add('active');
        }
    }

    closeAbout() {
        if (this.elements.aboutPanel) {
            // Hide about panel
            this.elements.aboutPanel.classList.remove('active');

            // Deactivate all metric tabs
            [this.elements.tabTime, this.elements.tabFreq, this.elements.tabAbout].forEach(el => {
                if (el) el.classList.remove('active');
            });

            // Activate the tab corresponding to the current metric
            const metric = this.app.state.currentMetric;
            if (metric === 'time' && this.elements.tabTime) {
                this.elements.tabTime.classList.add('active');
            } else if (metric === 'frequency' && this.elements.tabFreq) {
                this.elements.tabFreq.classList.add('active');
            }
        }
    }
    populateAbout() {
        if (this.elements.aboutContent && this.app.uiController) {
            const aboutHtml = this.app.uiController.populateAbout();
            this.elements.aboutContent.innerHTML = aboutHtml;
        }
    }

    // ==================== THEME ====================

    updateThemeIcon() {
        const icon = this.elements.themeToggle?.querySelector('i');
        if (icon) {
            icon.className = this.app.state.theme === 'dark' ? 'fas fa-moon' : 'fas fa-sun';
        }
    }

    // ==================== BOTTOM SHEET (disabled on mobile) ====================

    showBottomSheet(feature) {
        // Intentionally empty – we use map popups only on mobile
    }

    // ==================== CLEANUP ====================

    destroy() {
        document.body.classList.remove('mobile-active');
        if (this.elements.container) {
            this.elements.container.style.display = 'none';
        }
    }
}