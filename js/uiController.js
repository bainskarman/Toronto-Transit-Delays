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
                } else {
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
            return;
        }

        let html = '';
        matches.forEach(r => {
            html += `<div class="search-result-item" data-route="${r.route}">
                <span class="search-result-route">${r.route}</span>
                <span class="search-result-name">${r.longName || ''}</span>
            </div>`;
        });
        resultsContainer.innerHTML = html;
        resultsContainer.style.display = 'block';

        resultsContainer.querySelectorAll('.search-result-item').forEach(item => {
            item.addEventListener('click', () => {
                const routeNum = item.dataset.route;
                // Find the full route data
                const routeData = this.app.state.routesData.find(r => r.route === routeNum);
                if (routeData) {
                    // Set search input to the route number (for user feedback)
                    if (this.elements.searchInput) {
                        this.elements.searchInput.value = routeNum;
                    }
                    // Highlight the route on the map
                    if (this.app.mapVisualizer) {
                        this.app.mapVisualizer.highlightRoute(routeData);
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

    populateAbout() {
        const content = `
            <h1>About This Project</h1>
            <p class="coffee-link">
                <a href="https://ko-fi.com/kbains" target="_blank">☕ Support the Project</a>
            </p>

            <section>
                <h2> Introduction</h2>
                <p>Public transit is the backbone of Toronto, connecting millions of people every day. This project started with a simple goal: to better understand how the TTC performs by analyzing over a decade of delay data. By examining patterns across routes, neighbourhoods, and incident types, we can uncover insights that help riders plan their journeys and inform conversations about transit reliability.</p>
                <p>What began as a personal data exploration has grown into a comprehensive, interactive dashboard that visualizes where and when delays occur most frequently. Whether you're a daily commuter, a transit planner, or just curious about the system, this tool offers a transparent look at TTC operations.</p>
            </section>

            <section>
                <h2> Project Evolution</h2>
                <p>This project has evolved through several stages:</p>
                <ul>
                    <li><strong>2023:</strong> Initial Tableau dashboard exploring basic delay patterns.</li>
                    <li><strong>2024:</strong> Expanded to a Power BI semantic data model, integrating multiple datasets for deeper analysis.</li>
                    <li><strong>2025:</strong> Development of a Streamlit app for forecasting and trend analysis, combined with GIS work to map routes geographically.</li>
                    <li><strong>Present:</strong> This web app, built with Leaflet and custom aggregations, makes the data accessible and interactive for everyone.</li>
                </ul>
            </section>

            <section>
                <h2> Limitations & Methodology</h2>
                <p>While we strive for accuracy, there are important considerations when interpreting the data:</p>
                <ul>
                    <li><strong>Geocoding precision:</strong> Approximately 93% of incidents are successfully mapped to specific stops using location names. The remaining 7% could not be precisely located due to inconsistent naming, so the overall spatial accuracy is estimated at 90%.</li>
                    <li><strong>Route variants:</strong> The dataset does not distinguish between route variants (e.g., 129A vs 129B). All variants are grouped under the base route number (e.g., 129).</li>
                </ul>
            </section>

            <section>
                <h2> Previous Work</h2>
                <p>Explore earlier iterations of this analysis:</p>
                <ul class="links-list">
                    <li><a href="https://public.tableau.com/app/profile/karman.bains/viz/TTCDelayDash/Dashboard1" target="_blank">Tableau Dashboard (2023)</a></li>
                    <li><a href="https://app.powerbi.com/view?r=eyJrIjoiOTRkYTMyZjctMjU3Yi00MTQzLTg0NTItZTQ2YjQwMzRkYWRjIiwidCI6ImI2NDE3Y2QwLTFmNzMtNDQ3MS05YTM5LTIwOTUzODIyYTM0YSIsImMiOjN9&source=post_page-----20d7b475d736------------------------------------" target="_blank">Power BI Semantic Model (2024)</a></li>
                    <li><a href="https://ttcdelay.streamlit.app/" target="_blank">Streamlit App (2025)</a></li>
                    <li><a href="https://medium.com/@bsinghkarman/tracking-time-lost-a-data-dive-into-torontos-public-transit-delays-20d7b475d736" target="_blank">Medium Article (May 2025)</a></li>
                </ul>
            </section>

            <section>
                <h2> Contact & Links</h2>
                <ul class="contact-list">
                    <li><i class="fas fa-envelope"></i> <a href="mailto:bsinghkarman@gmail.com">bsinghkarman@gmail.com</a></li>
                    <li><i class="fas fa-globe"></i> <a href="https://bainskarman.github.io/portfolio.io/" target="_blank">Portfolio</a></li>
                    <li><i class="fab fa-github"></i> <a href="https://github.com/bainskarman" target="_blank">GitHub</a></li>
                </ul>
            </section>

            <section>
                <h2> Data Sources</h2>
                <p>This project relies on open data from:</p>
                <ul>
                    <li><a href="https://www.toronto.ca/city-government/data-research-maps/neighbourhoods-communities/ward-profiles/" target="_blank">Toronto Neighbourhoods</a> – Geographic boundaries, neighbourhoods, and infrastructure.</li>
                    <li><a href="https://open.toronto.ca/" target="_blank">City of Toronto Open Data</a> – TTC Routes, Schedules, Trips, Shapes, and Delays.</li>
                    <li><a href="https://transittoronto.ca/bus/routes/196-york-univer.shtml" target="_blank">Toronto Transit</a> – TTC Historical Routes.</li>
                    <li><a href="https://www.ttc.ca/routes-and-schedules" target="_blank">TTC</a> – TTC Present Routes and Schedules.</li>
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