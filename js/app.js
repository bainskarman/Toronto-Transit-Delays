// Main Application Controller for TTC Delay Visualization
class TTCVisualizationApp {
    constructor() {
        // Fixed to dark theme only
        this.currentTheme = 'dark';
        this.currentVisualization = 'delay';
        this.map = null;
        this.mapVisualizer = null;
        this.dataLoader = null;
        this.uiController = null;
        this.dashboardActive = false;
        
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
            
            // Initialize map (only for delay/frequency modes)
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
        console.log('📊 Loading ALL historical data (2014-2025)...');
        
        try {
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
            this.state.filteredRoutes = routes;

            console.log(`✅ Loaded ALL ${routes.length} routes from 2014-2025`);
            
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
                attributionControl: true,
                // Prevent auto-zooming behavior
                maxBounds: [
                    [43.58, -79.63], // Southwest bounds
                    [43.86, -79.12]  // Northeast bounds (Toronto area)
                ],
                maxBoundsViscosity: 1.0 // Strict bounds enforcement
            });

            // Add base tile layer (dark mode only)
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                subdomains: 'abcd',
                maxZoom: 19,
                // Prevent tile layer from causing zoom issues
                updateWhenIdle: true,
                keepBuffer: 4
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
        // Visualization toggles - delay, frequency, and dashboard
        document.querySelectorAll('.toggle-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const visualType = e.currentTarget.dataset.visual;
                this.switchVisualization(visualType);
            });
        });

        // Historical route search
        const searchInput = document.getElementById('routeSearch');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.handleHistoricalSearch(e.target.value);
            });

            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.performHistoricalSearch();
                }
            });
        }

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

        // Window resize
        window.addEventListener('resize', () => {
            this.handleResize();
        });

        const hideInactiveCheckbox = document.getElementById('hideInactiveRoutes');
        if (hideInactiveCheckbox) {
            hideInactiveCheckbox.addEventListener('change', () => {
                this.state.filteredRoutes = this.filterRoutes();
                if (this.currentVisualization === 'delay') {
                    this.mapVisualizer.showRouteDelays(this.state.filteredRoutes);
                } else if (this.currentVisualization === 'frequency') {
                    this.mapVisualizer.showDelayFrequency(this.state.filteredRoutes);
                }
                this.updateTopRoutesFromFiltered();
                this.handleViewportChange();
            });
        }

        // Dashboard iframe events - REMOVED OLD DASHBOARD EVENTS
        // We'll handle dashboard communication differently
    }

    setupMapEvents() {
        if (!this.map) return;
        
        // Prevent unwanted zoom behavior
        this.map.on('zoomstart', () => {
            clearTimeout(this.zoomTimeout);
        });

        this.map.on('moveend', () => {
            this.handleViewportChange();
        });

        this.map.on('zoomend', () => {
            this.handleViewportChange();
        });
    }

    async switchVisualization(visualType) {
        console.log(`🔄 Switching to ${visualType} visualization...`);
        
        try {
            // Update UI state
            this.currentVisualization = visualType;
            
            // Update active toggle button
            this.updateToggleButtons(visualType);
            
            // Clear existing visualization if switching away from map
            if (this.mapVisualizer && visualType !== 'dashboard') {
                this.mapVisualizer.clearVisualization();
            }
            
            // Show/hide dashboard
            if (visualType === 'dashboard') {
                this.showDashboard();
                return;
            } else {
                this.hideDashboard();
            }

            // Show loading state for map visualizations
            if (visualType !== 'dashboard') {
                this.uiController.showLoadingState();
            }

            // Apply new visualization
            let success = false;
            switch (visualType) {
                case 'delay':
                    success = await this.mapVisualizer.showRouteDelays(this.state.filteredRoutes);
                    break;
                case 'frequency':
                    success = await this.mapVisualizer.showDelayFrequency(this.state.filteredRoutes);
                    break;
                default:
                    console.warn(`Unknown visualization type: ${visualType}`);
                    success = await this.mapVisualizer.showRouteDelays(this.state.filteredRoutes);
            }

            // Update legend
            if (visualType !== 'dashboard') {
                this.updateMapLegend();
            }

            console.log(`✅ Switched to ${visualType} visualization - Success: ${success}`);

        } catch (error) {
            console.error(`❌ Error switching to ${visualType} visualization:`, error);
            this.showError(`Failed to load ${visualType} visualization`);
        } finally {
            if (visualType !== 'dashboard') {
                this.uiController.hideLoadingState();
            }
        }
    }

    // Update toggle button states
    updateToggleButtons(activeVisual) {
        document.querySelectorAll('.toggle-btn').forEach(btn => {
            if (btn.dataset.visual === activeVisual) {
                btn.classList.add('active');
                btn.setAttribute('aria-selected', 'true');
            } else {
                btn.classList.remove('active');
                btn.setAttribute('aria-selected', 'false');
            }
        });
    }

    // Dashboard methods - SIMPLIFIED VERSION
    showDashboard() {
        console.log('📊 Showing dashboard...');
        
        this.dashboardActive = true;
        document.body.classList.add('dashboard-active');
        
        // Get elements
        const dashboardContainer = document.getElementById('dashboard-container');
        const iframe = document.getElementById('dashboard-frame');
        
        // Show container
        if (dashboardContainer) {
            dashboardContainer.style.display = 'block';
            dashboardContainer.style.opacity = '1';
            dashboardContainer.style.visibility = 'visible';
        }
        
        // Hide map and sidebars
        document.querySelectorAll('.sidebar, .map-container').forEach(el => {
            if (el) el.style.display = 'none';
        });
        
        // Ensure iframe is loaded
        if (iframe) {
            // Set source if not already set
            const currentSrc = iframe.src;
            const dashboardPath = 'Dash/dashboard.html';
            
            if (!currentSrc.includes('dashboard.html')) {
                iframe.src = dashboardPath;
                console.log('Setting iframe src to:', dashboardPath);
            }
            
            // Clear any existing event listeners
            iframe.onload = null;
            iframe.onerror = null;
            
            // Set new event listener for iframe load
            iframe.onload = () => {
                console.log('✅ Dashboard iframe loaded successfully');
            };
            
            iframe.onerror = (error) => {
                console.error('❌ Dashboard iframe failed to load:', error);
                this.showError('Failed to load dashboard. Please try again.');
            };
        }
        
        console.log('✅ Dashboard shown');
    }

    hideDashboard() {
        console.log('📊 Hiding dashboard...');
        
        this.dashboardActive = false;
        document.body.classList.remove('dashboard-active');
        
        // Hide dashboard container
        const dashboardContainer = document.getElementById('dashboard-container');
        if (dashboardContainer) {
            dashboardContainer.style.display = 'none';
            dashboardContainer.style.opacity = '0';
            dashboardContainer.style.visibility = 'hidden';
        }
        
        // Show map and sidebars
        document.querySelectorAll('.sidebar, .map-container').forEach(el => {
            if (el) el.style.display = '';
        });
        
        // Restore main content grid
        const mainContent = document.querySelector('.main-content');
        if (mainContent) {
            mainContent.style.gridTemplateColumns = '320px 1fr 320px';
        }
        
        // Reset map if needed
        if (this.map) {
            setTimeout(() => {
                this.map.invalidateSize();
            }, 100);
        }
        
        console.log('✅ Dashboard hidden');
    }

    // Historical search methods
    handleHistoricalSearch(query) {
        this.state.searchQuery = query;
        
        // Debounce search execution
        clearTimeout(this.searchTimeout);
        this.searchTimeout = setTimeout(() => {
            this.performHistoricalSearch();
        }, 300);
    }

    performHistoricalSearch() {
        this.state.filteredRoutes = this.filterRoutes();
        
        if (this.state.searchQuery) {
            // Update search results UI
            if (this.uiController.updateSearchResults) {
                this.uiController.updateSearchResults(this.state.filteredRoutes);
            }
        } else {
            // Clear search results
            if (this.uiController.clearSearchResults) {
                this.uiController.clearSearchResults();
            }
        }
    }

    // Filter routes for historical data
    filterRoutes() {
        let filtered = [...this.state.routes];

        // Apply active filter if checkbox is checked
        const hideInactiveCheckbox = document.getElementById('hideInactiveRoutes');
        if (hideInactiveCheckbox && hideInactiveCheckbox.checked) {
            filtered = filtered.filter(route => route.active_in_2025 === true);
        }

        // Apply search query
        if (this.state.searchQuery) {
            const query = this.state.searchQuery.toLowerCase();
            filtered = filtered.filter(route => 
                route.Route.toString().toLowerCase().includes(query) ||
                (route.route_long_name && route.route_long_name.toLowerCase().includes(query))
            );
        }

        return filtered;
    }

    handleViewportChange() {
        if (!this.map || this.dashboardActive) return;
        
        const bounds = this.map.getBounds();
        this.state.currentViewport = bounds;
        
        // Update viewport insights
        this.updateViewportInsights();
    }

    updateViewportInsights() {
        if (!this.state.currentViewport || this.dashboardActive) return;

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

        if (this.uiController.updateViewportInsights) {
            this.uiController.updateViewportInsights(topRoutes, routesInView.length);
        }
    }

    updateMapLegend() {
        if (this.dashboardActive) return;
        
        const legend = this.mapVisualizer.getCurrentLegend();
        if (this.uiController.updateMapLegend) {
            this.uiController.updateMapLegend(legend);
        }
    }

    updateUI() {
        // Update metrics
        if (this.uiController.updateMetrics) {
            this.uiController.updateMetrics(this.state.summaryStats);
        }
        
        // Update top routes list
        const topRoutes = this.state.routes
            .sort((a, b) => b.Avg_Delay_Min - a.Avg_Delay_Min)
            .slice(0, 10);
        
        if (this.uiController.updateTopRoutes) {
            this.uiController.updateTopRoutes(topRoutes);
        }
        
        // Update data summary
        if (this.uiController.updateDataSummary) {
            this.uiController.updateDataSummary(this.state.summaryStats);
        }
        
        // Initialize charts
        if (this.uiController.initializeCharts) {
            this.uiController.initializeCharts(this.state.routes);
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
        if (this.dashboardActive) return;
        
        if (!navigator.geolocation) {
            this.showError('Geolocation is not supported by your browser');
            return;
        }

        navigator.geolocation.getCurrentPosition(
            (position) => {
                const { latitude, longitude } = position.coords;
                if (this.map) {
                    this.map.setView([latitude, longitude], 13);
                }
                this.showNotification('Location found!', 'success');
            },
            (error) => {
                console.error('Error getting location:', error);
                this.showError('Unable to get your location');
            }
        );
    }

    resetMapView() {
        if (this.dashboardActive || !this.map) return;
        
        this.map.flyTo([43.6532, -79.3832], 11, {
            animate: true,
            duration: 1
        });
        this.showNotification('Map view reset', 'info');
    }

    handleResize() {
        // Refresh map on resize if not in dashboard mode
        if (this.map && !this.dashboardActive) {
            setTimeout(() => {
                this.map.invalidateSize();
            }, 250);
        }
        
        // Notify UI controller
        if (this.uiController.onResize) {
            this.uiController.onResize();
        }
    }

    showNotification(message, type = 'info') {
        if (this.uiController.showNotification) {
            this.uiController.showNotification(message, type);
        }
    }

    showError(message) {
        this.showNotification(message, 'error');
    }

    // Route selection methods
    selectRoute(routeId) {
        const route = this.state.routes.find(r => r.Route.toString() === routeId);
        if (route && this.mapVisualizer) {
            this.mapVisualizer.highlightRoute(routeId);
            this.state.selectedRoute = route;
            if (this.uiController.updateRouteDetails) {
                this.uiController.updateRouteDetails(route);
            }
        }
    }

    // Public method to clear selection
    clearSelection() {
        if (this.mapVisualizer) {
            this.mapVisualizer.clearHighlight();
        }
        
        this.state.selectedRoute = null;
        if (this.uiController.clearRouteDetails) {
            this.uiController.clearRouteDetails();
        }
    }

    // Get application state for debugging
    getState() {
        return {
            ...this.state,
            currentTheme: this.currentTheme,
            currentVisualization: this.currentVisualization,
            dashboardActive: this.dashboardActive
        };
    }
}

// Load user preferences from localStorage (dark theme only)
function loadUserPreferences() {
    // Force dark theme
    const savedTheme = 'dark';
    document.documentElement.setAttribute('data-theme', savedTheme);
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