// js/uiController.js
// User Interface Controller for TTC Bus Delay Analytics

class UIController {
    constructor(app) {
        this.app = app;
        this.quantileBreaks = [];
        // DOM elements
        this.elements = {
            controlPanel: null,
            panelToggle: null,
            panelContent: null,
            viewButtons: [],
            metricButtons: [],
            // Custom dropdown containers
            yearDropdown: null,
            transitDropdown: null,
            incidentDropdown: null,
            activeOnlyCheckbox: null,
            searchInput: null,
            filterBadge: null,
            themeToggle: null,
            kpiPrimaryLabel: null,
            kpiPrimaryValue: null,
            kpiPrimaryTrend: null,
            kpiTotalIncidents: null,
            kpiTotalTrend: null,
            kpiRoutesReporting: null,
            kpiRoutesTrend: null,
            legendOverlay: null,
            legendMin: null,
            legendMax: null,
            legendMetric: null,
            tooltip: null,
            bottomSheet: null,
            sheetTitle: null,
            sheetContent: null,
            sheetClose: null,
            mobileFilterBtn: null,
            filterDrawer: null,
            drawerClose: null,
            resetFiltersMobile: null,
            applyFiltersMobile: null
        };

        // Store current filter options and selected values
        this.filterOptions = {
            years: [],
            transitTypes: [],
            categories: []
        };
        this.selectedFilters = {
            years: [],
            transitTypes: [],
            categories: []
        };

        this.searchDebounce = null;
    }

    // Inside uiController.js, in the init() method, after cacheElements() and attachEvents(), add:
    init() {
        console.log('🎨 Initializing UI Controller...');
        this.cacheElements();
        this.attachEvents();
        this.populateFilters();            // will use current view
        this.applyThemeClass();
        this.populateAbout();
        
        // Ensure legend is visible by default
        if (this.elements.legendOverlay) {
            this.elements.legendOverlay.classList.remove('hidden');
        }
    }

    cacheElements() {
        // Control panel
        this.elements.controlPanel = document.getElementById('controlPanel');
        this.elements.panelToggle = document.getElementById('panelToggle');
        this.elements.panelContent = document.getElementById('panelContent');

        // View buttons
        this.elements.viewButtons = document.querySelectorAll('.view-btn');
        this.elements.layerItems = document.querySelectorAll('.layer-item');

        // Metric buttons
        this.elements.metricButtons = document.querySelectorAll('.metric-btn');
        this.elements.navItems = document.querySelectorAll('.nav-item');
        this.elements.dashboardNavItem = document.querySelector('.nav-item[data-nav="dashboard"]');

        // Custom dropdown containers (these divs will hold the dropdowns)
        this.elements.yearDropdown = document.getElementById('yearFilter');
        this.elements.transitDropdown = document.getElementById('transitFilter');
        this.elements.incidentDropdown = document.getElementById('incidentFilter');
        this.elements.activeOnlyCheckbox = document.getElementById('activeOnlyFilter');
        this.elements.searchInput = document.getElementById('routeSearchInput');
        this.elements.filterBadge = document.getElementById('activeFilterCount');

        // Theme toggle
        this.elements.themeToggle = document.getElementById('themeToggle');

        // KPI elements
        this.elements.kpiPrimaryLabel = document.getElementById('kpiPrimaryLabel');
        this.elements.kpiPrimaryValue = document.getElementById('kpiPrimaryValue');
        this.elements.kpiPrimaryTrend = document.getElementById('kpiPrimaryTrend');
        this.elements.kpiTotalIncidents = document.getElementById('kpiTotalIncidents');
        this.elements.kpiTotalTrend = document.getElementById('kpiTotalTrend');
        this.elements.kpiRoutesReporting = document.getElementById('kpiRoutesReporting');
        this.elements.kpiRoutesTrend = document.getElementById('kpiRoutesTrend');

        // Legend
        this.elements.legendOverlay = document.getElementById('legendOverlay');
        this.elements.legendMin = document.getElementById('legendMin');
        this.elements.legendMax = document.getElementById('legendMax');
        this.elements.legendMetric = document.getElementById('legendMetric');

        // Tooltip
        this.elements.tooltip = document.getElementById('customTooltip');

        // Bottom sheet
        this.elements.bottomSheet = document.getElementById('bottomSheet');
        this.elements.sheetTitle = document.getElementById('sheetTitle');
        this.elements.sheetContent = document.getElementById('sheetContent');
        this.elements.sheetClose = document.getElementById('sheetClose');

        // Mobile elements
        this.elements.mobileFilterBtn = document.getElementById('mobileFilterBtn');
        this.elements.filterDrawer = document.getElementById('filterDrawer');
        this.elements.drawerClose = document.getElementById('filterDrawerClose');
        this.elements.resetFiltersMobile = document.getElementById('resetFiltersMobile');
        this.elements.applyFiltersMobile = document.getElementById('applyFiltersMobile');

        // Mobile KPI elements
        this.elements.mobileKpiPrimary = document.getElementById('mobileKpiPrimary');
        this.elements.mobileKpiTotal = document.getElementById('mobileKpiTotal');
        this.elements.mobileKpiRoutes = document.getElementById('mobileKpiRoutes');
        this.elements.resetFiltersBtn = document.getElementById('resetFiltersBtn');

        //About 
        this.elements.aboutNavItem = document.querySelector('.nav-item[data-nav="about"]');
    }

    attachEvents() {
        // Panel toggle
        if (this.elements.panelToggle) {
            this.elements.panelToggle.addEventListener('click', () => this.togglePanel());
        }
        // About close button
        const aboutClose = document.getElementById('aboutCloseBtn');
        if (aboutClose) {
            aboutClose.addEventListener('click', () => this.hideAbout());
        }

        // View buttons
        this.elements.viewButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const view = e.currentTarget.dataset.view;
                this.setActiveView(view);
            });
        });

        // Layer buttons
        this.elements.layerItems.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const layer = e.currentTarget.dataset.layer;
                this.setActiveView(layer);
            });
        });

        // Metric buttons
        this.elements.metricButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const metric = e.currentTarget.dataset.metric;
                this.setActiveMetric(metric);
            });
        });

        // Primary nav items
        document.querySelectorAll('.nav-item').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const nav = e.currentTarget.dataset.nav;
                
                if (nav === 'about') {
                    this.showAbout();
                } 
                else if (nav === 'dashboard') {
                    // Open dashboard modal instead of switching mode
                    if (this.app.dashboardController) {
                        this.app.dashboardController.openModal();
                    }
                } 
                else {
                    // Time or Frequency: stay in map mode
                    this.setActiveMetric(nav);
                }
            });
        });

        // Active only checkbox
        if (this.elements.activeOnlyCheckbox) {
            this.elements.activeOnlyCheckbox.addEventListener('change', () => this.applyFiltersFromUI());
        }

        // Search input with debounce
        if (this.elements.searchInput) {
            this.elements.searchInput.addEventListener('input', () => {
                clearTimeout(this.searchDebounce);
                const query = this.elements.searchInput.value.trim();
                this.searchDebounce = setTimeout(() => {
                    this.updateSearchSuggestions(query);
                }, 300);
            });
        }

        // Click outside search results to close
        document.addEventListener('click', (e) => {
            const results = document.getElementById('searchResults');
            const input = this.elements.searchInput;
            if (results && input && !results.contains(e.target) && e.target !== input) {
                results.style.display = 'none';
            }
        });

        // Theme toggle
        if (this.elements.themeToggle) {
            this.elements.themeToggle.addEventListener('click', () => this.app.handleThemeChange());
        }

        // Bottom sheet close
        if (this.elements.sheetClose) {
            this.elements.sheetClose.addEventListener('click', () => this.hideBottomSheet());
        }

        // Legend toggle
        const legendToggle = document.getElementById('legendToggleBtn');
        if (legendToggle) {
            legendToggle.addEventListener('click', () => this.toggleLegend());
        }

        // Mobile filter button
        if (this.elements.mobileFilterBtn) {
            this.elements.mobileFilterBtn.addEventListener('click', () => this.openFilterDrawer());
        }
        if (this.elements.drawerClose) {
            this.elements.drawerClose.addEventListener('click', () => this.closeFilterDrawer());
        }
        if (this.elements.resetFiltersMobile) {
            this.elements.resetFiltersMobile.addEventListener('click', () => this.resetFilters());
        }
        if (this.elements.applyFiltersMobile) {
            this.elements.applyFiltersMobile.addEventListener('click', () => {
                this.applyFiltersFromUI();
                this.closeFilterDrawer();
            });
        }

        // Mobile search input
        const mobileSearchInput = document.getElementById('mobileRouteSearch');
        const mobileSearchBtn = document.getElementById('mobileSearchBtn');
        if (mobileSearchInput) {
            mobileSearchInput.addEventListener('input', () => {
                clearTimeout(this.searchDebounce);
                const query = mobileSearchInput.value.trim();
                this.searchDebounce = setTimeout(() => {
                    this.updateSearchSuggestions(query);
                }, 300);
            });
        }
        if (mobileSearchBtn) {
            mobileSearchBtn.addEventListener('click', () => {
                const query = mobileSearchInput?.value.trim();
                if (query) this.updateSearchSuggestions(query);
            });
        }

        if (this.elements.resetFiltersBtn) {
            this.elements.resetFiltersBtn.addEventListener('click', () => this.resetFilters());
        }

        // Click outside to close tooltip and dropdowns
        document.addEventListener('click', (e) => {
            // Close tooltip
            if (this.elements.tooltip && !this.elements.tooltip.contains(e.target)) {
                this.hideTooltip();
            }
            // Close all open dropdowns if click outside any dropdown container
            if (!e.target.closest('.custom-dropdown')) {
                document.querySelectorAll('.custom-dropdown-panel').forEach(panel => {
                    panel.style.display = 'none';
                });
            }
        });
    }

    // ==================== CUSTOM DROPDOWN HELPERS ====================

    pluralize(word, count) {
        if (count === 1) return word;
        // Handle irregular plural for "Category"
        if (word === 'Category') return 'Categories';
        return word + 's';
    }

    createDropdown(containerId, options, selectedValues, label) {
        const container = document.getElementById(containerId);
        if (!container) return;

        // Clear container
        container.innerHTML = '';
        container.classList.add('custom-dropdown');

        // Create dropdown button
        const button = document.createElement('button');
        button.className = 'custom-dropdown-btn filter-select';
        button.type = 'button';
        button.innerHTML = `<span class="dropdown-label">${label}</span> <i class="fas fa-chevron-down"></i>`;

        // Create dropdown panel
        const panel = document.createElement('div');
        panel.className = 'custom-dropdown-panel';
        panel.style.display = 'none';

        // "Select All" option
        const selectAllLabel = document.createElement('label');
        selectAllLabel.className = 'dropdown-option select-all';
        const selectAllCheckbox = document.createElement('input');
        selectAllCheckbox.type = 'checkbox';
        selectAllCheckbox.className = 'select-all-checkbox';
        selectAllCheckbox.checked = (selectedValues.length === options.length);
        selectAllLabel.appendChild(selectAllCheckbox);
        selectAllLabel.appendChild(document.createTextNode('Select All'));
        panel.appendChild(selectAllLabel);

        // Individual options
        options.forEach(opt => {
            const label = document.createElement('label');
            label.className = 'dropdown-option';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.value = opt;
            checkbox.checked = selectedValues.includes(opt);
            label.appendChild(checkbox);
            label.appendChild(document.createTextNode(opt));
            panel.appendChild(label);
        });

        container.appendChild(button);
        container.appendChild(panel);

        // Toggle panel on button click
        button.addEventListener('click', (e) => {
            e.stopPropagation();
            const isVisible = panel.style.display === 'block';
            // Close all other dropdowns
            document.querySelectorAll('.custom-dropdown-panel').forEach(p => p.style.display = 'none');
            panel.style.display = isVisible ? 'none' : 'block';
        });

        // Handle checkbox changes
        panel.addEventListener('change', (e) => {
            const checkboxes = panel.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
            const selectAll = panel.querySelector('.select-all-checkbox');

            if (e.target.classList.contains('select-all-checkbox')) {
                // Select All logic
                checkboxes.forEach(cb => cb.checked = selectAll.checked);
            } else {
                // Update Select All checkbox based on individual selections
                const allChecked = Array.from(checkboxes).every(cb => cb.checked);
                selectAll.checked = allChecked;
            }

            // Update button label
            const selected = Array.from(checkboxes).filter(cb => cb.checked).map(cb => cb.value);
            const labelSpan = button.querySelector('.dropdown-label');
            if (selected.length === 0) {
                labelSpan.textContent = `No ${label}`;
            } else if (selected.length === options.length) {
                labelSpan.textContent = `All ${label}s`;
            } else {
                labelSpan.textContent = `${selected.length} ${label}${selected.length > 1 ? 's' : ''}`;
            }

            // Trigger filter update
            this.applyFiltersFromUI();
        });

        // Initialize button label
        const initialSelected = selectedValues;
        const labelSpan = button.querySelector('.dropdown-label');
        if (initialSelected.length === 0) {
            labelSpan.textContent = `No ${label}`;
        } else if (initialSelected.length === options.length) {
            // All options selected – use plural form
            labelSpan.textContent = `All ${this.pluralize(label, 2)}`;
        } else {
            // Some but not all selected
            labelSpan.textContent = `${initialSelected.length} ${this.pluralize(label, initialSelected.length)}`;
        }
    }

    // Get selected values from a dropdown container
    getDropdownValues(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return [];
        const panel = container.querySelector('.custom-dropdown-panel');
        if (!panel) return [];
        const checkboxes = panel.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
        return Array.from(checkboxes).filter(cb => cb.checked).map(cb => cb.value);
    }

    // Set selected values in a dropdown (without triggering events)
    setDropdownValues(containerId, values) {
        const container = document.getElementById(containerId);
        if (!container) return;
        const panel = container.querySelector('.custom-dropdown-panel');
        if (!panel) return;
        const checkboxes = panel.querySelectorAll('input[type="checkbox"]');
        const optionCheckboxes = panel.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
        const selectAll = panel.querySelector('.select-all-checkbox');

        optionCheckboxes.forEach(cb => {
            cb.checked = values.includes(cb.value);
        });
        const allChecked = Array.from(optionCheckboxes).every(cb => cb.checked);
        if (selectAll) selectAll.checked = allChecked;

        // Update button label
        const button = container.querySelector('.custom-dropdown-btn');
        const labelSpan = button.querySelector('.dropdown-label');
        const options = optionCheckboxes.length;
        if (values.length === 0) {
            labelSpan.textContent = `No ${labelSpan.dataset.label || 'options'}`;
        } else if (values.length === options) {
            labelSpan.textContent = `All ${labelSpan.dataset.label || 'options'}s`;
        } else {
            labelSpan.textContent = `${values.length} ${labelSpan.dataset.label || 'options'}${values.length > 1 ? 's' : ''}`;
        }
    }

    // ==================== FILTER METHODS ====================

    resetFiltersToDefault() {
        // Year dropdown
        const yearPanel = this.elements.yearDropdown?.querySelector('.custom-dropdown-panel');
        if (yearPanel) {
            const selectAll = yearPanel.querySelector('.select-all-checkbox');
            if (selectAll) selectAll.checked = true;
            const checkboxes = yearPanel.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
            checkboxes.forEach(cb => cb.checked = true);
            const button = this.elements.yearDropdown.querySelector('.custom-dropdown-btn');
            const labelSpan = button.querySelector('.dropdown-label');
            labelSpan.textContent = `All Years`;
        }

        // Transit dropdown
        const transitPanel = this.elements.transitDropdown?.querySelector('.custom-dropdown-panel');
        if (transitPanel) {
            const selectAll = transitPanel.querySelector('.select-all-checkbox');
            if (selectAll) selectAll.checked = true;
            const checkboxes = transitPanel.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
            checkboxes.forEach(cb => cb.checked = true);
            const button = this.elements.transitDropdown.querySelector('.custom-dropdown-btn');
            const labelSpan = button.querySelector('.dropdown-label');
            labelSpan.textContent = `All Types`;
        }

        // Incident dropdown
        const incidentPanel = this.elements.incidentDropdown?.querySelector('.custom-dropdown-panel');
        if (incidentPanel) {
            const selectAll = incidentPanel.querySelector('.select-all-checkbox');
            if (selectAll) selectAll.checked = true;
            const checkboxes = incidentPanel.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
            checkboxes.forEach(cb => cb.checked = true);
            const button = this.elements.incidentDropdown.querySelector('.custom-dropdown-btn');
            const labelSpan = button.querySelector('.dropdown-label');
            labelSpan.textContent = `All Categories`;
        }

        // Active only checkbox
        if (this.elements.activeOnlyCheckbox) this.elements.activeOnlyCheckbox.checked = true;

        // Search input
        if (this.elements.searchInput) this.elements.searchInput.value = '';

        // Apply filters immediately
        this.applyFiltersFromUI();
    }

    populateFiltersForView(view) {
        // Get distinct values from app
        const years = this.app.getDistinctYearsForView(view) || [];
        const transitTypes = this.app.getDistinctTransitForView(view) || [];
        const categories = this.app.getDistinctCategoriesForView(view) || [];

        // Store options
        this.filterOptions.years = years;
        this.filterOptions.transitTypes = transitTypes;
        this.filterOptions.categories = categories;

        // By default, select all options (meaning no filter)
        const defaultYears = years;
        const defaultTransit = transitTypes;
        const defaultCategories = categories;

        // Create dropdowns
        if (this.elements.yearDropdown) {
            this.createDropdown('yearFilter', years, defaultYears, 'Year');
        }
        if (this.elements.transitDropdown) {
            this.createDropdown('transitFilter', transitTypes, defaultTransit, 'Type');
        }
        if (this.elements.incidentDropdown) {
            this.createDropdown('incidentFilter', categories, defaultCategories, 'Category');
        }
    }

    populateFilters() {
        this.populateFiltersForView(this.app.state.currentView);
    }

    applyFiltersFromUI() {
        const filters = {};

        // Year: get selected values; if all selected, pass null (no filter)
        const selectedYears = this.getDropdownValues('yearFilter');
        filters.year = (selectedYears.length === this.filterOptions.years.length) ? null : selectedYears.map(y => Number(y));

        // Transit types
        const selectedTransit = this.getDropdownValues('transitFilter');
        filters.transitTypes = (selectedTransit.length === this.filterOptions.transitTypes.length) ? [] : selectedTransit;

        // Incident categories
        const selectedCategories = this.getDropdownValues('incidentFilter');
        filters.incidentCategories = (selectedCategories.length === this.filterOptions.categories.length) ? [] : selectedCategories;

        // Active only
        filters.activeOnly = this.elements.activeOnlyCheckbox ? this.elements.activeOnlyCheckbox.checked : true;

        // Search query
        filters.searchQuery = this.elements.searchInput ? this.elements.searchInput.value.trim() : '';

        // Update badge count
        const filterCount = this.countActiveFilters(filters);
        if (this.elements.filterBadge) {
            this.elements.filterBadge.textContent = filterCount;
        }

        this.app.applyFilters(filters);
    }

    countActiveFilters(filters) {
        let count = 0;
        if (filters.year && filters.year.length > 0) count++;  // year filter active only if specific years selected
        if (filters.transitTypes && filters.transitTypes.length > 0) count++;
        if (filters.incidentCategories && filters.incidentCategories.length > 0) count++;
        if (filters.activeOnly === false) count++;
        if (filters.searchQuery && filters.searchQuery.trim() !== '') count++;
        return count;
    }

    resetFilters() {
        this.resetFiltersToDefault();
    }

    // ==================== VIEW / METRIC CHANGES ====================

    setActiveView(view) {
        if (this.app.state.currentView === view) return;
        this.app.state.currentView = view;

        // Update UI buttons
        this.elements.viewButtons.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.view === view);
        });
        this.elements.layerItems.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.layer === view);
        });

        // Enable/disable route search and active only based on view
        const isRoutesView = view === 'routes';
        if (this.elements.searchInput) {
            this.elements.searchInput.disabled = !isRoutesView;
            this.elements.searchInput.placeholder = isRoutesView ? 'e.g. 36, 36A' : 'Search only in Routes view';
        }
        if (this.elements.activeOnlyCheckbox) {
            this.elements.activeOnlyCheckbox.disabled = !isRoutesView;
        }

        // Repopulate filter dropdowns for the new view
        this.populateFiltersForView(view);

        // Update KPI label and re-render
        this.updateKPILabel();
        this.app.updateView();
    }

    setActiveMetric(metric) {
        if (this.app.state.currentMetric === metric) return;
        this.app.state.currentMetric = metric;

        // Update UI buttons
        this.elements.metricButtons.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.metric === metric);
        });
        this.elements.navItems.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.nav === metric);
        });

        // Update KPI label
        this.updateKPILabel();

        // Re-render view
        this.app.updateView();
    }

    updateKPILabel() {
        if (this.elements.kpiPrimaryLabel) {
            this.elements.kpiPrimaryLabel.textContent = 'Avg Delay';
        }
    }

    // ==================== SEARCH SUGGESTIONS ====================

    updateSearchSuggestions(query) {
        // Only show suggestions if we are in routes view
        if (this.app.state.currentView !== 'routes') {
            const resultsContainer = document.getElementById('searchResults');
            if (resultsContainer) resultsContainer.style.display = 'none';
            return;
        }

        const resultsContainer = document.getElementById('searchResults');
        if (!resultsContainer) return;

        const trimmed = query ? query.trim() : '';
        if (!trimmed) {
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
            resultsContainer.innerHTML = '<div class="search-result-item">No routes found</div>';
            resultsContainer.style.display = 'block';
        } else {
            let html = '';
            matches.forEach(r => {
                html += `<div class="search-result-item" data-route="${r.route}">
                    <span class="search-result-route">${r.route}</span>
                    <span class="search-result-name">${r.longName || ''}</span>
                </div>`;
            });
            resultsContainer.innerHTML = html;
            resultsContainer.style.display = 'block';
        }

        // --- decide whether to open upward or downward ---
        const input = this.elements.searchInput;
        if (input) {
            const inputRect = input.getBoundingClientRect();
            const spaceBelow = window.innerHeight - inputRect.bottom;
            // if less than 200px below, open upward
            if (spaceBelow < 200) {
                resultsContainer.classList.add('upward');
            } else {
                resultsContainer.classList.remove('upward');
            }
        }
        // -------------------------------------------------------

        // Attach click handlers to results (unchanged)
        resultsContainer.querySelectorAll('.search-result-item').forEach(item => {
            item.addEventListener('click', () => {
                const routeNum = item.dataset.route;
                if (routeNum) {
                    // Set search input value
                    if (this.elements.searchInput) {
                        this.elements.searchInput.value = routeNum;
                    }
                    // Highlight route using mapVisualizer with route number
                    if (this.app.mapVisualizer) {
                        this.app.mapVisualizer.highlightRoute(routeNum);
                    }
                    // Close the suggestions dropdown
                    resultsContainer.style.display = 'none';
                }
            });
        });
    }

    // ==================== KPI & LEGEND ====================

    updateKPI(primaryValue, totalIncidents, routesReporting) {
        if (this.elements.kpiPrimaryValue) {
            this.elements.kpiPrimaryValue.textContent = primaryValue;
        }
        if (this.elements.kpiTotalIncidents) {
            this.elements.kpiTotalIncidents.textContent = totalIncidents.toLocaleString();
        }
        if (this.elements.kpiRoutesReporting) {
            this.elements.kpiRoutesReporting.textContent = routesReporting.toLocaleString();
        }

        if (this.elements.mobileKpiPrimary) {
            this.elements.mobileKpiPrimary.textContent = primaryValue;
        }
        if (this.elements.mobileKpiTotal) {
            this.elements.mobileKpiTotal.textContent = totalIncidents.toLocaleString();
        }
        if (this.elements.mobileKpiRoutes) {
            this.elements.mobileKpiRoutes.textContent = routesReporting.toLocaleString();
        }
    }
    

    updateLegendGradient(metric) {
        const gradientEl = document.querySelector('.legend-gradient');
        if (!gradientEl) return;
        if (metric === 'time') {
            gradientEl.style.background = 'linear-gradient(to right, #fbc4c4, #8B0000)';
        } else {
            gradientEl.style.background = 'linear-gradient(to right, #b8e2ff, #aa00ff)';
        }
    }

    setLegendVisibility(visible) {
        const legendOverlay = document.getElementById('legendOverlay');
        if (legendOverlay) {
            legendOverlay.style.display = visible ? 'block' : 'none';
        }
    }



    updateLegend(min, max, metricLabel) {
        // Hide legend if current view is hotspots
        if (this.app.state.currentView === 'hotspots') {
            this.setLegendVisibility(false);
            return;
        } else {
            this.setLegendVisibility(true);
        }

        // If quantile breaks are available, use discrete legend
        if (this.quantileBreaks && this.quantileBreaks.length > 1) {
            this.updateDiscreteLegend(this.quantileBreaks, this.app.state.currentMetric);
            return;
        }

        // Fallback to continuous gradient (original code)
        if (this.elements.legendMin) {
            this.elements.legendMin.textContent = typeof min === 'number' ? min.toFixed(1) : min;
        }
        if (this.elements.legendMax) {
            this.elements.legendMax.textContent = typeof max === 'number' ? max.toFixed(1) : max;
        }
        if (this.elements.legendMetric) {
            this.elements.legendMetric.textContent = metricLabel;
        }
        this.updateLegendGradient(this.app.state.currentMetric);
    }


    // ==================== TOOLTIP ====================

    showTooltip(feature) {
        if (!feature) {
            this.hideTooltip();
            return;
        }
        if (!this.elements.tooltip) return;

        let content = '';
        if (feature.type === 'route') {
            content = `<b>${feature.name}</b><br>Avg Delay: ${feature.avgDelay.toFixed(1)} min<br>Incidents: ${feature.totalCount}`;
        } else if (feature.type === 'ward' || feature.type === 'neighbourhood') {
            content = `<b>${feature.name}</b><br>Avg Delay: ${feature.avgDelay.toFixed(1)} min<br>Incidents: ${feature.totalCount}`;
        } else if (feature.type === 'stop') {
            content = `<b>${feature.name}</b><br>Avg Delay: ${feature.avgDelay.toFixed(1)} min<br>Incidents: ${feature.totalCount}`;
        }

        this.elements.tooltip.innerHTML = content;
        this.elements.tooltip.style.display = 'block';
    }

    hideTooltip() {
        if (this.elements.tooltip) {
            this.elements.tooltip.style.display = 'none';
        }
    }

    // ==================== BOTTOM SHEET ====================

    showBottomSheet(feature) {
        if (!feature) {
            this.hideBottomSheet();
            return;
        }

        this.elements.sheetTitle.textContent = feature.name || 'Details';
        let html = '<div class="sheet-details">';
        if (feature.type === 'route') {
            html += `<p><strong>Route:</strong> ${feature.route}</p>`;
            html += `<p><strong>Average Delay:</strong> ${feature.avgDelay.toFixed(1)} min</p>`;
            html += `<p><strong>Total Incidents:</strong> ${feature.totalCount.toLocaleString()}</p>`;
        } else if (feature.type === 'ward' || feature.type === 'neighbourhood') {
            html += `<p><strong>Area:</strong> ${feature.name}</p>`;
            html += `<p><strong>Average Delay:</strong> ${feature.avgDelay.toFixed(1)} min</p>`;
            html += `<p><strong>Total Incidents:</strong> ${feature.totalCount.toLocaleString()}</p>`;
            if (feature.details && feature.details.stopCount) {
                html += `<p><strong>Stops with data:</strong> ${feature.details.stopCount}</p>`;
            }
        } else if (feature.type === 'stop') {
            html += `<p><strong>Stop:</strong> ${feature.name}</p>`;
            html += `<p><strong>Average Delay:</strong> ${feature.avgDelay.toFixed(1)} min</p>`;
            html += `<p><strong>Incidents at this stop:</strong> ${feature.totalCount.toLocaleString()}</p>`;
        }
        html += '</div>';
        this.elements.sheetContent.innerHTML = html;

        this.elements.bottomSheet.classList.add('open');
    }

    hideBottomSheet() {
        this.elements.bottomSheet.classList.remove('open');
    }

    // ==================== PANEL & DRAWER ====================

    togglePanel() {
        const isCollapsed = this.app.state.panelCollapsed;
        this.app.state.panelCollapsed = !isCollapsed;
        if (this.elements.controlPanel) {
            this.elements.controlPanel.classList.toggle('collapsed', !isCollapsed);
        }
        if (this.elements.panelToggle) {
            const icon = this.elements.panelToggle.querySelector('i');
            if (icon) {
                // When collapsed (now true), show hamburger; when expanded (false), show left chevron
                icon.className = isCollapsed ? 'fas fa-chevron-left' : 'fas fa-bars';
            }
        }
    }

    setActiveNav(nav) {
        if (!this.elements.navItems) return;
        this.elements.navItems.forEach(item => {
            item.classList.toggle('active', item.dataset.nav === nav);
        });
    }

    populateAbout() {
        const content = `
            <h1>About This Project</h1>
            <p class="coffee-link">
                <a href="https://ko-fi.com/kbains" target="_blank">☕ Support the Project</a>
            </p>

            <section>
                <h2>A Personal Journey</h2>
                <p>For years, I’ve relied on the <strong>16 McCowan</strong> bus. Like many riders, I often wondered: <em>Why is my bus always delayed?</em> What started as a simple college project analysing a single route slowly grew into something much bigger. Today, it’s a full‑fledged exploration of Toronto’s entire bus network, powered by over a decade of public data.</p>
                <p>Behind the scenes, this project represents <strong>hundreds of hours</strong> of data processing, validation, and refinement about <strong>90% of my time</strong> goes into cleaning and matching data to ensure it tells a coherent story. The result is a dataset covering <strong>more than 900,000 delay incidents</strong> across <strong>over 9,500 unique stop locations</strong>, grouped into <strong>500+ route variants</strong> (under base route numbers). With <strong>12+ years of historical records (2014–2026)</strong>, we can finally see patterns.</p>
                <p class="disclaimer">
                    <strong>Please read:</strong> This is a personal, non‑professional project. I do not guarantee the accuracy or completeness of the data, and it should <strong>not</strong> be used as evidence in any legal, operational, or official capacity. The visualisations are meant for curiosity and general awareness, not for decision‑making that could affect safety, schedules, or rights.
                </p>
            </section>

            <section>
                <h2>Project Evolution</h2>
                <p>This journey has evolved through several iterations, each adding new depth:</p>
                <ul class="links-list">
                    <li><strong>2023:</strong> <a href="https://public.tableau.com/app/profile/karman.bains/viz/TTCDelayDash/Dashboard1" target="_blank">Tableau Dashboard</a> – my first public look at delay patterns.</li>
                    <li><strong>2024:</strong> <a href="https://app.powerbi.com/view?r=eyJrIjoiOTRkYTMyZjctMjU3Yi00MTQzLTg0NTItZTQ2YjQwMzRkYWRjIiwidCI6ImI2NDE3Y2QwLTFmNzMtNDQ3MS05YTM5LTIwOTUzODIyYTM0YSIsImMiOjN9&source=post_page-----20d7b475d736------------------------------------" target="_blank">Power BI Semantic Model</a> – integrated multiple datasets for deeper analysis.</li>
                    <li><strong>2025:</strong> <a href="https://ttcdelay.streamlit.app/" target="_blank">Streamlit App</a> – experimental trend predictions and geospatial mapping.</li>
                    <li><strong>Present:</strong> <a href="https://ttcdelay.kbains.com">Web App</a>- This interactive web app, making the data accessible to everyone, anywhere.</li>
                    <p>You can a previous version of the story behind the data in my <a href="https://medium.com/@bsinghkarman/tracking-time-lost-a-data-dive-into-torontos-public-transit-delays-20d7b475d736" target="_blank">Medium Article</a>.</p>
                </ul>
                
            </section>

            <section>
                <h2>Methodology & Limitations</h2>
                <p>All data is sourced from <strong>Toronto Open Data</strong> and refreshed automatically once a month using custom scripts. I combine delay records with TTC route, trip, and stop data, then map them to geographic boundaries using <strong>GeoJSON</strong> files provided by the city.</p>
                <h3>Location Matching</h3>
                <p>One of the biggest challenges is matching free‑text location names (like "<em>Kennedy Station</em>") to official stop IDs. Because the data lacks a direct key, I use <strong>natural language processing (NLP)</strong> techniques to match over <strong>20,000 location strings</strong> to the ~9,500 official stops. This achieves a <strong>93% match rate</strong> with an estimated accuracy of <strong>91%</strong> – a reasonable compromise given the inconsistency in naming conventions.</p>
                <h3>Route Variants</h3>
                <p>The source data does not distinguish between route variants (e.g., <em>129A</em> vs. <em>129B</em>). All variants are therefore grouped under the base route number (<em>129</em>). While this loses some granularity, it still provides a valuable overview of corridor‑level performance.</p>
                <h3>Data Quality Caveats</h3>
                <ul>
                    <li>Some Routes e.g.<strong>Line 4 (Sheppard)</strong> records have known quality issues (less than 1% of the data); they may be slightly underrepresented.</li>
                    <li>About <strong>7–8%</strong> of location strings remain unmatched after NLP matching, often due to ambiguous or outdated stop names.</li>
                    <li>Historical routes that no longer exist (e.g., the old <em>5‑Bay</em>) are excluded because they cannot be reliably geocoded.</li>
                </ul>
                <p>Despite these imperfections, the processed dataset retains <strong>98.7% of valid, geolocated incidents</strong>, enough to reveal meaningful patterns.</p>
            </section>

            <section>
                <h2>References</h2>
                <p>The following resources were essential for building this project:</p>
                <ul>
                    <li><a href="https://open.toronto.ca/" target="_blank">City of Toronto Open Data Portal</a> – Primary source for Toronto Transit data.</li>
                    <li><a href="https://www.ttc.ca/routes-and-schedules" target="_blank">TTC Official Website</a> – Current route and schedule information.</li>
                    <li><a href="https://www.nycsubway.org/wiki/TTC_Streetcar_Lines" target="_blank">NYC Subway</a> – Historical context for Toronto’s surface routes.</li>
                    <li><a href="https://transittoronto.ca/bus/routes/" target="_blank">Transit Toronto</a> – Detailed route history for Historical Routes Validation.</li>
                </ul>
            </section>

            <section>
                <h2>Contacts</h2>
                <ul class="contact-list">
                    <li><i class="fas fa-envelope"></i> <a href="mailto:bsinghkarman@gmail.com">bsinghkarman@gmail.com</a></li>
                    <li><i class="fas fa-globe"></i> <a href="https://kbains.com" target="_blank">Portfolio</a></li>
                    <li><i class="fab fa-github"></i> <a href="https://github.com/bainskarman" target="_blank">GitHub</a></li>
                </ul>
            </section>
        `;

        // Update desktop about container if it exists
        const desktopContainer = document.getElementById('aboutContent');
        if (desktopContainer) {
            desktopContainer.innerHTML = content;
        }

        // Return the HTML so mobile can use it
        return content;
    }

    openFilterDrawer() {
        if (this.elements.filterDrawer) {
            this.elements.filterDrawer.classList.add('open');
        }
    }

    closeFilterDrawer() {
        if (this.elements.filterDrawer) {
            this.elements.filterDrawer.classList.remove('open');
        }
    }

    // ==================== ABOUT ====================

    showAbout() {
        console.log('Showing about');
        const aboutContainer = document.getElementById('about-container');
        if (!aboutContainer) {
            console.error('About container not found');
            return;
        }

        // Deactivate all nav items
        this.elements.navItems.forEach(btn => btn.classList.remove('active'));
        // Activate the about button
        if (this.elements.aboutNavItem) {
            this.elements.aboutNavItem.classList.add('active');
        }

        // Show about container
        aboutContainer.classList.add('active');

        // Optionally hide other floating UI if needed (they are already behind due to z-index)
    }

    updateDiscreteLegend(breaks, metric) {
        // Double‑check view (safety)
        if (this.app.state.currentView === 'hotspots') {
            this.setLegendVisibility(false);
            return;
        }

        const legendOverlay = document.getElementById('legendOverlay');
        if (!legendOverlay) {
            console.warn('Legend overlay not found');
            return;
        }

        // Colors from mapVisualizer (5 shades each)
        const timeColors = [
            '#E0B3FF', // light lavender
            '#C180FF', // soft purple
            '#A34DFF', // medium purple
            '#7A1FFF', // vibrant violet
            '#5A00B3'  // deep violet
        ];
        const freqColors = [
            '#98D4E2', // light blue
            '#6A93C1', // blue‑gray
            '#53639F', // slate blue
            '#6C387F', // purple
            '#42144F'  // deep purple
        ];
        const colors = metric === 'time' ? timeColors : freqColors;

        let html = '<div class="legend-discrete">';
        html += `<div class="legend-metric-title">${metric === 'time' ? 'Avg Delay (min)' : 'Incident Count'}</div>`;

        for (let i = 0; i < colors.length; i++) {
            const low = breaks[i].toFixed(metric === 'time' ? 1 : 0);
            const high = breaks[i + 1].toFixed(metric === 'time' ? 1 : 0);
            html += `
                <div class="legend-item">
                    <span class="legend-color" style="background: ${colors[i]};"></span>
                    <span class="legend-range">${low} – ${high}</span>
                </div>
            `;
        }
        html += '</div>';

        legendOverlay.innerHTML = html;
        console.log('Discrete legend updated', breaks); // Debug
    }

    hideAbout() {
        console.log('Hiding about');
        const aboutContainer = document.getElementById('about-container');
        if (!aboutContainer) return;

        // Hide about container
        aboutContainer.classList.remove('active');

        // Restore active state for the current metric button
        const currentMetric = this.app.state.currentMetric;
        this.elements.navItems.forEach(btn => {
            if (btn.dataset.nav === currentMetric) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
    }

    // ==================== THEME ====================

    applyThemeClass() {
        document.documentElement.setAttribute('data-theme', this.app.state.theme);
    }

    // ==================== ERROR HANDLING ====================

    showError(message) {
        alert(message);
    }
}