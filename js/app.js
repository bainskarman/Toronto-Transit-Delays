// Main Application Controller for TTC Delay Visualization
class TTCVisualizationApp {
    constructor() {
        this.currentTheme = 'dark';
        this.currentVisualization = 'delay';
        this.map = null;
        this.mapVisualizer = null;
        this.dataLoader = null;
        this.uiController = null;
        
        // Application state
        this.state = {
            routes: [],
            routeGeometries: {},
            locationAnalysis: [],
            summaryStats: {},
            filteredRoutes: [],
            currentViewport: null,
            selectedRoute: null,
            searchQuery: '',
            filters: {
                delayThreshold: 10,
                routeType: 'all'
            }
        };

        this.init();
    }

    async init() {
        console.log('🚍 Initializing TTC Delay Visualization...');
        
        try {
            // Initialize modules
            this.dataLoader = new DataLoader();
            this.mapVisualizer = new MapVisualizer();
            this.uiController = new UIController(this);
            
            // Load application data
            await this.loadData();
            
            // Initialize UI components
            this.uiController.init();
            
            // Initialize map
            await this.initializeMap();
            
            // Set up event listeners
            this.setupEventListeners();
            
            // Update UI with initial data
            this.updateUI();
            
            console.log('🎉 TTC Delay Visualization initialized successfully');
            
        } catch (error) {
            console.error('❌ Failed to initialize application:', error);
            this.showError('Failed to initialize application. Please refresh the page.');
        }
    }

    async loadData() {
        console.log('📊 Loading application data...');
        
        try {
            // Load all data in parallel
            const [routes, geometries, locationAnalysis, summaryStats] = await Promise.all([
                this.dataLoader.loadRoutePerformance(),
                this.dataLoader.loadRouteGeometries(),
                this.dataLoader.loadLocationAnalysis(),
                this.dataLoader.loadSummaryStatistics()
            ]);

            this.state.routes = routes;
            this.state.routeGeometries = geometries;
            this.state.locationAnalysis = locationAnalysis;
            this.state.summaryStats = summaryStats;
            this.state.filteredRoutes = this.filterRoutes();

            console.log(`✅ Loaded ${routes.length} routes, ${Object.keys(geometries).length} geometries`);
            
        } catch (error) {
            console.error('❌ Error loading data:', error);
            throw error;
        }
    }

    async initializeMap() {
        console.log('🗺️ Initializing map...');
        
        try {
            // Initialize Leaflet map
            this.map = L.map('map', {
                center: [43.6532, -79.3832], // Toronto coordinates
                zoom: 11,
                zoomControl: false,
                attributionControl: true
            });

            // Add base tile layer
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(this.map);

            // Add zoom control to bottom right
            L.control.zoom({
                position: 'bottomright'
            }).addTo(this.map);

            // Initialize map visualizer with map instance
            this.mapVisualizer.init(this.map, this.state.routeGeometries, this.state.routes);
            
            // Load initial visualization
            await this.switchVisualization(this.currentVisualization);

            // Set up map event listeners
            this.setupMapEvents();

            console.log('✅ Map initialized successfully');

        } catch (error) {
            console.error('❌ Error initializing map:', error);
            throw error;
        }
    }

    setupEventListeners() {
        // Theme toggle
        document.getElementById('themeToggle').addEventListener('click', () => {
            this.toggleTheme();
        });

        // Visualization toggles
        document.querySelectorAll('.toggle-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const visualType = e.currentTarget.dataset.visual;
                this.switchVisualization(visualType);
            });
        });

        // Route search
        const searchInput = document.getElementById('routeSearch');
        searchInput.addEventListener('input', (e) => {
            this.handleSearch(e.target.value);
        });

        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.performSearch();
            }
        });

        // Filter controls
        document.getElementById('delayRange').addEventListener('input', (e) => {
            this.updateDelayFilter(parseInt(e.target.value));
        });

        document.getElementById('routeType').addEventListener('change', (e) => {
            this.updateRouteTypeFilter(e.target.value);
        });

        // Map controls
        document.getElementById('fullscreenBtn').addEventListener('click', () => {
            this.toggleFullscreen();
        });

        document.getElementById('locateBtn').addEventListener('click', () => {
            this.locateUser();
        });

        document.getElementById('resetViewBtn').addEventListener('click', () => {
            this.resetMapView();
        });

        // Footer links
        document.getElementById('aboutBtn').addEventListener('click', () => {
            this.showAboutModal();
        });

        document.getElementById('dataSourceBtn').addEventListener('click', () => {
            this.showDataSourceModal();
        });

        // Window resize
        window.addEventListener('resize', () => {
            this.handleResize();
        });
    }

    setupMapEvents() {
        // Viewport change events
        this.map.on('moveend', () => {
            this.handleViewportChange();
        });

        this.map.on('zoomend', () => {
            this.handleViewportChange();
        });

        // Route click events will be handled by map visualizer
    }

    async switchVisualization(visualType) {
    console.log(`🔄 Switching to ${visualType} visualization...`);
    
    try {
        // Update UI state
        this.currentVisualization = visualType;
        this.uiController.updateVisualizationToggles(visualType);

        // Clear existing visualization
        this.mapVisualizer.clearVisualization();

        // Show loading state
        this.uiController.showLoadingState();

        // Apply new visualization
        let success = false;
        switch (visualType) {
            case 'delay':
                success = await this.mapVisualizer.showRouteDelays(this.state.filteredRoutes);
                break;
            case 'comparison':
                success = await this.mapVisualizer.showRouteComparison(this.state.filteredRoutes);
                break;
            case 'frequency':
                success = await this.mapVisualizer.showDelayFrequency(this.state.filteredRoutes);
                break;
            default:
                console.warn(`Unknown visualization type: ${visualType}`);
                success = await this.mapVisualizer.showRouteDelays(this.state.filteredRoutes);
        }

        // Update legend
        this.updateMapLegend();

        console.log(`✅ Switched to ${visualType} visualization - Success: ${success}`);

    } catch (error) {
        console.error(`❌ Error switching to ${visualType} visualization:`, error);
        this.showError(`Failed to load ${visualType} visualization`);
    } finally {
        // ALWAYS hide loading state, even if there's an error
        this.uiController.hideLoadingState();
    }
}

    filterRoutes() {
        let filtered = [...this.state.routes];

        // Apply delay threshold filter
        filtered = filtered.filter(route => 
            route.Avg_Delay_Min >= this.state.filters.delayThreshold
        );

        // Apply route type filter (if we had that data)
        if (this.state.filters.routeType !== 'all') {
            // This would need route type data to be implemented
        }

        // Apply search filter
        if (this.state.searchQuery) {
            const query = this.state.searchQuery.toLowerCase();
            filtered = filtered.filter(route => 
                route.Route.toString().toLowerCase().includes(query) ||
                (route.route_long_name && route.route_long_name.toLowerCase().includes(query))
            );
        }

        return filtered;
    }

    handleSearch(query) {
        this.state.searchQuery = query;
        
        // Debounce search execution
        clearTimeout(this.searchTimeout);
        this.searchTimeout = setTimeout(() => {
            this.performSearch();
        }, 300);
    }

    performSearch() {
        this.state.filteredRoutes = this.filterRoutes();
        
        if (this.state.searchQuery) {
            // If we have search results, update visualization
            this.switchVisualization(this.currentVisualization);
            
            // Update search results UI
            this.uiController.updateSearchResults(this.state.filteredRoutes);
        } else {
            // Clear search results
            this.uiController.clearSearchResults();
        }
    }

    updateDelayFilter(value) {
        this.state.filters.delayThreshold = value;
        document.getElementById('delayValue').textContent = `${value}+ min`;
        
        // Reapply filters
        this.state.filteredRoutes = this.filterRoutes();
        this.switchVisualization(this.currentVisualization);
    }

    updateRouteTypeFilter(value) {
        this.state.filters.routeType = value;
        
        // Reapply filters
        this.state.filteredRoutes = this.filterRoutes();
        this.switchVisualization(this.currentVisualization);
    }

    handleViewportChange() {
        const bounds = this.map.getBounds();
        this.state.currentViewport = bounds;
        
        // Update viewport insights
        this.updateViewportInsights();
    }

    updateViewportInsights() {
        if (!this.state.currentViewport) return;

        const bounds = this.state.currentViewport;
        const routesInView = this.state.filteredRoutes.filter(route => {
            const routeId = route.Route.toString();
            const geometry = this.state.routeGeometries[routeId];
            
            if (!geometry) return false;
            
            // Check if any coordinate is within bounds
            return geometry.some(coord => 
                bounds.contains(L.latLng(coord[0], coord[1]))
            );
        });

        // Sort by delay and take top 5
        const topRoutes = routesInView
            .sort((a, b) => b.Avg_Delay_Min - a.Avg_Delay_Min)
            .slice(0, 5);

        this.uiController.updateViewportInsights(topRoutes, routesInView.length);
    }

    updateMapLegend() {
        const legend = this.mapVisualizer.getCurrentLegend();
        this.uiController.updateMapLegend(legend);
    }

    updateUI() {
        // Update metrics
        this.uiController.updateMetrics(this.state.summaryStats);
        
        // Update top routes list
        const topRoutes = this.state.routes
            .sort((a, b) => b.Avg_Delay_Min - a.Avg_Delay_Min)
            .slice(0, 10);
        this.uiController.updateTopRoutes(topRoutes);
        
        // Update data summary
        this.uiController.updateDataSummary(this.state.summaryStats);
        
        // Initialize charts
        this.uiController.initializeCharts(this.state.routes);
    }

    toggleTheme() {
        this.currentTheme = this.currentTheme === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', this.currentTheme);
        
        // Update theme button icon
        const themeIcon = document.querySelector('.theme-icon');
        themeIcon.textContent = this.currentTheme === 'dark' ? '🌙' : '☀️';
        
        // Store preference
        localStorage.setItem('theme', this.currentTheme);
        
        // Notify map visualizer about theme change
        if (this.mapVisualizer) {
            this.mapVisualizer.onThemeChange(this.currentTheme);
        }
    }

    toggleFullscreen() {
        if (!document.fullscreenElement) {
            document.documentElement.requestFullscreen().catch(err => {
                console.error('Error attempting to enable fullscreen:', err);
            });
        } else {
            document.exitFullscreen();
        }
    }

    locateUser() {
        if (!navigator.geolocation) {
            this.showError('Geolocation is not supported by your browser');
            return;
        }

        navigator.geolocation.getCurrentPosition(
            (position) => {
                const { latitude, longitude } = position.coords;
                this.map.setView([latitude, longitude], 13);
                this.showNotification('Location found!', 'success');
            },
            (error) => {
                console.error('Error getting location:', error);
                this.showError('Unable to get your location');
            }
        );
    }

    resetMapView() {
        this.map.setView([43.6532, -79.3832], 11);
        this.showNotification('Map view reset', 'info');
    }

    handleResize() {
        // Refresh map on resize
        if (this.map) {
            setTimeout(() => {
                this.map.invalidateSize();
            }, 250);
        }
    }

    showAboutModal() {
        // Simple about modal implementation
        const aboutContent = `
            <h2>About TTC Delay Visualization</h2>
            <p>This interactive visualization platform provides insights into Toronto Transit Commission (TTC) bus delays and performance metrics.</p>
            <p><strong>Features:</strong></p>
            <ul>
                <li>Real-time delay visualization across routes</li>
                <li>Heatmap of delay hotspots</li>
                <li>Route comparison and frequency analysis</li>
                <li>Interactive search and filtering</li>
            </ul>
            <p><strong>Data Sources:</strong> TTC open data, processed and analyzed for visualization.</p>
            <p><em>Note: This is an independent project and not affiliated with TTC or the City of Toronto.</em></p>
        `;
        
        this.showModal('About', aboutContent);
    }

    showDataSourceModal() {
        const dataContent = `
            <h2>Data Sources & Methodology</h2>
            <p><strong>Data Sources:</strong></p>
            <ul>
                <li>TTC Route Performance Data</li>
                <li>Route Geometry Information</li>
                <li>Delay Incident Reports</li>
                <li>Location Analysis Data</li>
            </ul>
            <p><strong>Methodology:</strong></p>
            <ul>
                <li>Data processed and cleaned for accuracy</li>
                <li>Average delays calculated from historical data</li>
                <li>Geospatial analysis for route mapping</li>
                <li>Real-time data updates (when available)</li>
            </ul>
            <p><strong>Last Data Update:</strong> ${this.state.summaryStats.updatedAt || 'N/A'}</p>
        `;
        
        this.showModal('Data Sources', dataContent);
    }

    showModal(title, content) {
        // Create modal overlay
        const modalOverlay = document.createElement('div');
        modalOverlay.className = 'modal-overlay active';
        modalOverlay.innerHTML = `
            <div class="modal-content">
                <div class="modal-header">
                    <h3 class="modal-title">${title}</h3>
                    <button class="modal-close">&times;</button>
                </div>
                <div class="modal-body">
                    ${content}
                </div>
            </div>
        `;

        // Add to document
        document.body.appendChild(modalOverlay);

        // Close handlers
        const closeModal = () => {
            modalOverlay.classList.remove('active');
            setTimeout(() => {
                document.body.removeChild(modalOverlay);
            }, 300);
        };

        modalOverlay.querySelector('.modal-close').addEventListener('click', closeModal);
        modalOverlay.addEventListener('click', (e) => {
            if (e.target === modalOverlay) closeModal();
        });
    }

    showNotification(message, type = 'info') {
        this.uiController.showNotification(message, type);
    }

    showError(message) {
        this.showNotification(message, 'error');
    }

    // Public method to select a route from UI
    selectRoute(routeId) {
        const route = this.state.routes.find(r => r.Route.toString() === routeId);
        if (route && this.mapVisualizer) {
            this.mapVisualizer.highlightRoute(routeId);
            this.state.selectedRoute = route;
            this.uiController.updateRouteDetails(route);
        }
    }

    // Public method to clear selection
    clearSelection() {
        if (this.mapVisualizer) {
            this.mapVisualizer.clearHighlight();
        }
        this.state.selectedRoute = null;
        this.uiController.clearRouteDetails();
    }

    // Get application state for debugging
    getState() {
        return {
            ...this.state,
            currentTheme: this.currentTheme,
            currentVisualization: this.currentVisualization
        };
    }
}

// Load user preferences from localStorage
function loadUserPreferences() {
    const savedTheme = localStorage.getItem('theme');
    if (savedTheme) {
        document.documentElement.setAttribute('data-theme', savedTheme);
    }
}

// Initialize application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    loadUserPreferences();
    
    // Global app instance
    window.ttcApp = new TTCVisualizationApp();
});

// Export for module usage (if needed)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = TTCVisualizationApp;
}