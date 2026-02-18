// js/app.js
// Main Application Controller for TTC Bus Delay Analytics (updated for new mobile design)

class TTCVisualizationApp {
    constructor() {
        // Core properties
        this.dataLoader = null;
        this.mapVisualizer = null;
        this.uiController = null;
        this.mobileController = null;
        this.map = null;

        // Application state
        this.state = {
            // Data
            routesData: [],
            wardsData: [],
            neighbourhoodsData: [],
            hotspotsAggregated: [],
            routeGeometries: {},
            wardGeometries: null,
            neighbourhoodGeometries: null,

            // UI state
            currentView: 'routes',
            currentMetric: 'time',
            filters: {
                year: null,
                transitTypes: null,
                incidentCategories: null,
                activeOnly: true,
                searchQuery: ''
            },
            theme: 'dark',
            panelCollapsed: false,
            mobile: window.innerWidth <= 768,
            selectedFeature: null,
            hoveredFeature: null
        };

        // New properties for aggregated data
        this.wardsAgg = [];
        this.neighbourhoodsAgg = [];
        this.hotspotsAgg = [];

        // Bind methods
        this.init = this.init.bind(this);
        this.loadData = this.loadData.bind(this);
        this.updateView = this.updateView.bind(this);
        this.applyFilters = this.applyFilters.bind(this);
        this.handleResize = this.handleResize.bind(this);
        this.handleThemeChange = this.handleThemeChange.bind(this);
        this.onFeatureClick = this.onFeatureClick.bind(this);
        this.onFeatureHover = this.onFeatureHover.bind(this);
    }

    async init() {
        console.log('🚀 Initializing TTC Bus Delay Analytics...');

        try {
            // Initialize modules
            this.dataLoader = new DataLoader();
            this.uiController = new UIController(this);
            this.mapVisualizer = new MapVisualizer(this);
            this.mobileController = new MobileController(this);   // <-- uses new MobileController

            // Load all data
            await this.loadData();

            // Initialize map (will be created by MapVisualizer)
            this.map = this.mapVisualizer.init('map', this.state);

            // Set up UI event listeners (buttons, panels, etc.)
            this.uiController.init();

            // If mobile, activate mobile layout
            if (this.state.mobile) {
                this.mobileController.init();
            }

            // Set initial view (routes, time metric)
            await this.updateView();

            // Listen to window resize
            window.addEventListener('resize', this.handleResize);

            // Listen to theme changes (system preference)
            this.setupThemeListener();

            console.log('✅ Application initialized');
        } catch (error) {
            console.error('❌ Initialization failed:', error);
            this.uiController.showError('Failed to initialize application. Please refresh.');
        }
    }

    async loadData() {
        console.log('📊 Loading all data from assets...');
        try {
            const data = await this.dataLoader.loadAll();

            // Process route data (unchanged)
            this.processRouteData(data.routeAnalysis);

            // Store aggregated location data
            this.wardsAgg = data.wardsAgg;
            this.neighbourhoodsAgg = data.neighbourhoodsAgg;
            this.hotspotsAgg = data.hotspotsAgg;

            // Store geometries
            this.state.routeGeometries = data.routeGeometries;
            this.state.wardGeometries = data.wardsGeoJSON;
            this.state.neighbourhoodGeometries = data.neighbourhoodsGeoJSON;

            console.log('✅ Data loaded and processed');
            console.log(`   Wards: ${this.wardsAgg.length} aggregated rows`);
            console.log(`   Neighbourhoods: ${this.neighbourhoodsAgg.length} rows`);
            console.log(`   Hotspots: ${this.hotspotsAgg.length} rows`);
        } catch (error) {
            console.error('❌ Data loading error:', error);
            throw error;
        }
    }

    processRouteData(routeAnalysis) {
        // Store raw rows for later filtering
        this.rawRouteData = routeAnalysis.map(row => ({
            route: String(row.Route).trim(),
            year: parseInt(row.Year, 10),
            transit: row.Transit ? row.Transit.trim() : '',
            category: row.Incident_Category ? row.Incident_Category.trim() : '',
            delayCount: parseInt(row.Delay_Count, 10) || 0,
            totalDelayMin: parseFloat(row.Total_Delay_Min) || 0,
            active2025: row.active_in_2025 === 'True' || row.active_in_2025 === true,
            longName: row.route_long_name ? row.route_long_name.trim() : `Route ${row.Route}`,
            rank: parseInt(row.rank, 10) || 1
        }));

        // Build metadata map for quick lookup
        const routeMetadata = new Map();
        this.rawRouteData.forEach(row => {
            if (!routeMetadata.has(row.route)) {
                routeMetadata.set(row.route, {
                    longName: row.longName,
                    active2025: row.active2025
                });
            }
        });

        // Also build the per-route aggregated data for the routes view (initially unfiltered)
        const routeMap = new Map();
        this.rawRouteData.forEach(row => {
            if (!routeMap.has(row.route)) {
                routeMap.set(row.route, {
                    route: row.route,
                    longName: row.longName,
                    active2025: row.active2025,
                    years: new Set(),
                    transitTypes: new Set(),
                    categories: new Set(),
                    totalDelayCount: 0,
                    totalDelayMinutes: 0,
                });
            }
            const entry = routeMap.get(row.route);
            entry.years.add(row.year);
            entry.transitTypes.add(row.transit);
            entry.categories.add(row.category);
            entry.totalDelayCount += row.delayCount;
            entry.totalDelayMinutes += row.totalDelayMin;
        });

        this.state.routesData = Array.from(routeMap.values()).map(r => ({
            ...r,
            years: Array.from(r.years).sort((a,b) => a-b),
            transitTypes: Array.from(r.transitTypes),
            categories: Array.from(r.categories),
            avgDelay: r.totalDelayCount > 0 ? r.totalDelayMinutes / r.totalDelayCount : 0
        }));

        console.log(`Processed ${this.rawRouteData.length} raw rows, ${this.state.routesData.length} routes`);
    }

    getKPIRouteRows() {
        // Get all filtered rows (including duplicates)
        const allRows = this.getFilteredRouteRows();
        // Keep only primary rows (rank === 1)
        return allRows.filter(row => row.rank === 1);
    }

    getFilteredRouteRows() {
        if (!this.rawRouteData || !Array.isArray(this.rawRouteData)) {
            console.warn('rawRouteData not available');
            return [];
        }

        const filters = this.state.filters;

        return this.rawRouteData.filter(row => {
            // Year filter
            if (filters.year !== null) {
                if (!Array.isArray(filters.year) || filters.year.length === 0) return false;
                if (!filters.year.includes(row.year)) return false;
            }

            // Transit types filter
            if (filters.transitTypes !== null && filters.transitTypes.length > 0) {
                if (!filters.transitTypes.includes(row.transit)) return false;
            }

            // Incident categories filter
            if (filters.incidentCategories !== null && filters.incidentCategories.length > 0) {
                if (!filters.incidentCategories.includes(row.category)) return false;
            }

            // Active only filter
            if (filters.activeOnly && !row.active2025) return false;

            // Search query filter
            if (filters.searchQuery && filters.searchQuery.trim() !== '') {
                const q = filters.searchQuery.toLowerCase().trim();
                const routeMatch = row.route.toLowerCase().includes(q);
                const nameMatch = row.longName.toLowerCase().includes(q);
                if (!routeMatch && !nameMatch) return false;
            }

            return true;
        });
    }
    
    // Add this method to the TTCVisualizationApp class
    getFilteredRoutesData() {
        if (!this.rawRouteData) return [];

        const filters = this.state.filters;

        return this.rawRouteData.filter(row => {
            // Year filter: if year array is not empty, check if row.year is in the array
            if (filters.year && filters.year.length > 0) {
                if (!filters.year.includes(row.year)) return false;
            }

            // Transit types filter
            if (filters.transitTypes && filters.transitTypes.length > 0) {
                if (!filters.transitTypes.includes(row.transit)) return false;
            }

            // Incident categories filter
            if (filters.incidentCategories && filters.incidentCategories.length > 0) {
                if (!filters.incidentCategories.includes(row.category)) return false;
            }

            // Active only filter
            if (filters.activeOnly && !row.active2025) return false;

            // Search query filter
            if (filters.searchQuery && filters.searchQuery.trim() !== '') {
                const q = filters.searchQuery.toLowerCase().trim();
                const routeMatch = row.route.toLowerCase().includes(q);
                const nameMatch = row.longName.toLowerCase().includes(q);
                if (!routeMatch && !nameMatch) return false;
            }

            return true;
        });
    }

    // Helper: get distinct years for a given view
    getDistinctYearsForView(view) {
        let data = [];
        switch (view) {
            case 'routes':
                data = this.state.routesData;
                break;
            case 'wards':
                data = this.wardsAgg;
                break;
            case 'neighbourhoods':
                data = this.neighbourhoodsAgg;
                break;
            case 'hotspots':
                data = this.hotspotsAgg;
                break;
        }
        const years = new Set();
        data.forEach(item => {
            if (item.year) years.add(item.year);
            else if (item.years) item.years.forEach(y => years.add(y)); // for routes
        });
        return Array.from(years).sort((a, b) => b - a); // descending
    }

    // Helper: get distinct transit types for a given view
    getDistinctTransitForView(view) {
        let data = [];
        switch (view) {
            case 'routes':
                data = this.state.routesData;
                break;
            case 'wards':
                data = this.wardsAgg;
                break;
            case 'neighbourhoods':
                data = this.neighbourhoodsAgg;
                break;
            case 'hotspots':
                data = this.hotspotsAgg;
                break;
        }
        const types = new Set();
        data.forEach(item => {
            if (item.transit) types.add(item.transit);
            else if (item.transitTypes) item.transitTypes.forEach(t => types.add(t)); // routes
        });
        return Array.from(types).sort();
    }

    // Helper: get distinct incident categories for a given view
    getDistinctCategoriesForView(view) {
        let data = [];
        switch (view) {
            case 'routes':
                data = this.state.routesData;
                break;
            case 'wards':
                data = this.wardsAgg;
                break;
            case 'neighbourhoods':
                data = this.neighbourhoodsAgg;
                break;
            case 'hotspots':
                data = this.hotspotsAgg;
                break;
        }
        const cats = new Set();
        data.forEach(item => {
            if (item.category) cats.add(item.category);
            else if (item.categories) item.categories.forEach(c => cats.add(c)); // routes
        });
        return Array.from(cats).sort();
    }

    // New method: aggregate an array of (ward/neighbourhood/hotspot) entries by current filters
    aggregateByFilters(dataArray, groupByKey) {
        const filters = this.state.filters;

        const filtered = dataArray.filter(entry => {
            // Year filter
            if (filters.year && filters.year.length > 0) {
                if (!filters.year.includes(entry.year)) return false;
            }

            // Transit types filter
            if (filters.transitTypes && filters.transitTypes.length > 0) {
                if (!filters.transitTypes.includes(entry.transit)) return false;
            }

            // Incident categories filter
            if (filters.incidentCategories && filters.incidentCategories.length > 0) {
                if (!filters.incidentCategories.includes(entry.category)) return false;
            }

            return true;
        });

        const grouped = new Map();
        filtered.forEach(entry => {
            let key;
            if (groupByKey === 'ward') key = entry.ward;
            else if (groupByKey === 'neighbourhood') key = entry.neighbourhood;
            else if (groupByKey === 'hotspot') key = `${entry.lat},${entry.lon}`;

            if (!grouped.has(key)) {
                const base = (groupByKey === 'hotspot')
                    ? { lat: entry.lat, lon: entry.lon }
                    : { name: key };
                grouped.set(key, {
                    ...base,
                    totalDelayCount: 0,
                    totalDelayMinutes: 0
                });
            }
            const grp = grouped.get(key);
            grp.totalDelayCount += entry.delay_count;
            grp.totalDelayMinutes += entry.total_delay_min;
        });

        return Array.from(grouped.values()).map(g => ({
            ...g,
            avgDelay: g.totalDelayCount > 0 ? g.totalDelayMinutes / g.totalDelayCount : 0,
            totalCount: g.totalDelayCount
        }));
    }

    async updateView() {
        await this.mapVisualizer.renderView(
            this.state.currentView,
            this.state.currentMetric,
            this.getFilteredData()
        );
        const breaks = this.mapVisualizer.currentBreaks; // or a getter
        this.updateKPI();
        this.updateLegend(breaks);
    }

    getFilteredData() {
        const { currentView } = this.state;

        switch (currentView) {
            case 'routes': {
                const filteredRows = this.getFilteredRouteRows(); // or getFilteredRoutesData()? We need a method that returns raw rows
                // Group by route
                const routeMap = new Map();
                filteredRows.forEach(row => {
                    if (!routeMap.has(row.route)) {
                        routeMap.set(row.route, {
                            route: row.route,
                            longName: row.longName,
                            active2025: row.active2025,
                            years: new Set(),
                            transitTypes: new Set(),
                            categories: new Set(),
                            totalDelayCount: 0,
                            totalDelayMinutes: 0,
                        });
                    }
                    const entry = routeMap.get(row.route);
                    entry.years.add(row.year);
                    entry.transitTypes.add(row.transit);
                    entry.categories.add(row.category);
                    entry.totalDelayCount += row.delayCount;
                    entry.totalDelayMinutes += row.totalDelayMin;
                });
                return Array.from(routeMap.values()).map(r => ({
                    ...r,
                    years: Array.from(r.years).sort((a,b) => a-b),
                    transitTypes: Array.from(r.transitTypes),
                    categories: Array.from(r.categories),
                    avgDelay: r.totalDelayCount > 0 ? r.totalDelayMinutes / r.totalDelayCount : 0
                }));
            }

            case 'wards':
                return this.aggregateByFilters(this.wardsAgg, 'ward');

            case 'neighbourhoods':
                return this.aggregateByFilters(this.neighbourhoodsAgg, 'neighbourhood');

            case 'hotspots':
                return this.aggregateByFilters(this.hotspotsAgg, 'hotspot');

            default:
                return [];
        }
    }
    
    updateKPI() {
        const filteredRows = this.getKPIRouteRows(); // rank=1 only

        const totalIncidents = filteredRows.reduce((sum, r) => sum + r.delayCount, 0);
        const totalDelayMin = filteredRows.reduce((sum, r) => sum + r.totalDelayMin, 0);
        const avgDelay = totalIncidents > 0 ? (totalDelayMin / totalIncidents).toFixed(1) + ' min' : '0 min';
        const routesReporting = new Set(filteredRows.map(r => r.route)).size;

        // Update desktop KPI
        this.uiController.updateKPI(avgDelay, totalIncidents, routesReporting);

        // Update mobile KPI if mobile is active
        if (this.state.mobile && this.mobileController) {
            this.mobileController.updateKPI(avgDelay, totalIncidents, routesReporting);
        }
    }

    updateLegend(breaks) {
        // Compute min/max for fallback (if needed)
        const data = this.getFilteredData();
        let min = 0, max = 0;
        if (this.state.currentMetric === 'time') {
            const values = data.map(d => d.avgDelay || 0).filter(v => v > 0);
            min = values.length ? Math.min(...values) : 0;
            max = values.length ? Math.max(...values) : 60;
        } else {
            const values = data.map(d => d.totalCount || 0).filter(v => v > 0);
            min = values.length ? Math.min(...values) : 0;
            max = values.length ? Math.max(...values) : 1000;
        }
        const metricLabel = this.state.currentMetric === 'time' ? 'Avg Delay (min)' : 'Incident Count';

        // Pass breaks to controllers
        this.uiController.quantileBreaks = breaks;
        this.uiController.updateLegend(min, max, metricLabel);

        if (this.state.mobile && this.mobileController) {
            this.mobileController.quantileBreaks = breaks;
            this.mobileController.updateLegend(this.state.currentMetric);
        }
    }

    applyFilters(newFilters) {
        this.state.filters = { ...this.state.filters, ...newFilters };
        this.updateView();
    }

    onFeatureClick(feature) {
        this.state.selectedFeature = feature;
        // On desktop, show popup (handled by mapVisualizer), on mobile we rely only on popup
        if (this.state.mobile && this.mobileController) {
            // Do nothing – the map popup is already shown by mapVisualizer
            // Optionally, you could still show a small bottom sheet, but user requested removal.
            // So we comment out the call:
            // this.mobileController.showBottomSheet(feature);
        } else {
            // Desktop uses map popup (already handled)
        }
    }

    onFeatureHover(feature) {
        this.state.hoveredFeature = feature;
        // Only desktop uses tooltip
        if (!this.state.mobile) {
            this.uiController.showTooltip(feature);
        }
    }

    handleResize() {
        const mobile = window.innerWidth <= 768;
        if (mobile !== this.state.mobile) {
            this.state.mobile = mobile;
            if (mobile) {
                this.mobileController.init();
                // Hide desktop UI (already handled by body class in mobileController)
            } else {
                this.mobileController.destroy();
                // Show desktop UI (body class removed)
            }
        }
        if (this.map) {
            this.map.invalidateSize();
        }
    }

    setupThemeListener() {
        const darkModeMedia = window.matchMedia('(prefers-color-scheme: dark)');
        const setTheme = (e) => {
            if (!localStorage.getItem('theme')) {
                this.state.theme = e.matches ? 'dark' : 'light';
                this.applyTheme();
            }
        };
        darkModeMedia.addEventListener('change', setTheme);
        const savedTheme = localStorage.getItem('theme');
        if (savedTheme) {
            this.state.theme = savedTheme;
        } else {
            this.state.theme = darkModeMedia.matches ? 'dark' : 'light';
        }
        this.applyTheme();
    }

    handleThemeChange() {
        this.state.theme = this.state.theme === 'dark' ? 'light' : 'dark';
        localStorage.setItem('theme', this.state.theme);
        this.applyTheme();
    }

    applyTheme() {
        document.documentElement.setAttribute('data-theme', this.state.theme);
        if (this.mapVisualizer) {
            this.mapVisualizer.setTheme(this.state.theme);
        }
        // Update mobile theme icon if needed
        if (this.state.mobile && this.mobileController) {
            this.mobileController.updateThemeIcon();
        }
    }
}

// Instantiate on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    window.ttcApp = new TTCVisualizationApp();
    window.ttcApp.init();
});