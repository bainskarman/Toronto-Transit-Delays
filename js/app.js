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
            // Live tracking state
            liveTracking: {
                enabled: false,
                currentRoute: '16',
                allBuses: [],  // All buses from API
                filteredBuses: [], // Buses filtered by current route
                isLoading: false,
                lastUpdated: null,
                selectedBus: null,
                availableRoutes: [] // Routes with active buses
            }
        };

        // Live Tracking Configuration
        this.LIVE_TRACKING_CONFIG = {
            API_URL: '/api/live-buses',
            REFRESH_INTERVAL: 30000,
            DEFAULT_ROUTE: '16',
            MAX_BUSES: 100,
            BUS_ICON_COLORS: {
                moving: '#10b981',    // Green - moving
                slow: '#f59e0b',      // Yellow - slow moving
                stopped: '#ef4444'    // Red - stopped
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
                // FIX: Prevent auto-zooming behavior
                maxBounds: [
                    [43.58, -79.63], // Southwest bounds
                    [43.86, -79.12]  // Northeast bounds (Toronto area)
                ],
                maxBoundsViscosity: 1.0 // Strict bounds enforcement
            });

            // Add base tile layer
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                subdomains: 'abcd',
                maxZoom: 19,
                // FIX: Prevent tile layer from causing zoom issues
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

        // Live route search with autocomplete
        const liveSearchInput = document.getElementById('liveRouteSearch');
        const liveSearchBtn = document.getElementById('liveSearchBtn');
        
        if (liveSearchInput) {
            liveSearchInput.addEventListener('input', (e) => {
                this.handleLiveRouteInput(e.target.value);
            });
            
            liveSearchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.updateLiveRoute();
                }
            });
            
            // Focus event to show available routes
            liveSearchInput.addEventListener('focus', () => {
                this.showLiveRouteSuggestions();
            });
        }
        
        if (liveSearchBtn) {
            liveSearchBtn.addEventListener('click', () => {
                this.updateLiveRoute();
            });
        }

        // Live refresh button
        const refreshBtn = document.getElementById('refreshLiveBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => {
                this.refreshLivePositions();
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

        // Footer links
        document.getElementById('aboutBtn').addEventListener('click', () => {
            this.showAboutModal();
        });

        document.getElementById('dataSourceBtn').addEventListener('click', () => {
            this.showDataSourceModal();
        });

        // Analytics link
        document.getElementById('analyticsLink')?.addEventListener('click', (e) => {
            e.preventDefault();
            window.open('https://ttcdelay.streamlit.app/', '_blank');
        });

        // Window resize
        window.addEventListener('resize', () => {
            this.handleResize();
        });
    }

    setupMapEvents() {
        if (!this.map) return;
        
        // FIX: Prevent unwanted zoom behavior
        this.map.on('zoomstart', () => {
            // Clear any pending zoom actions
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
            this.uiController.updateVisualizationToggles(visualType);

            // Show/hide appropriate UI sections
            if (this.uiController.toggleLiveTrackingUI) {
                this.uiController.toggleLiveTrackingUI(visualType === 'live');
            }

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
                case 'live':
                    success = await this.loadAndDisplayLiveBuses();
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
            this.uiController.hideLoadingState();
        }
    }

    // Live Tracking Methods
    async loadAndDisplayLiveBuses() {
        console.log(`🚍 Loading live buses for route ${this.state.liveTracking.currentRoute}...`);
        
        try {
            // Set loading state
            this.state.liveTracking.isLoading = true;
            if (this.uiController.updateLiveLoadingState) {
                this.uiController.updateLiveLoadingState(true);
            }
            
            // Fetch ALL live bus data
            const allBuses = await this.fetchLiveBusData();
            
            if (!allBuses || allBuses.length === 0) {
                throw new Error('No live bus data available');
            }
            
            // Update state with all buses
            this.state.liveTracking.allBuses = allBuses;
            this.state.liveTracking.lastUpdated = new Date();
            
            // Extract available routes for autocomplete
            this.updateAvailableRoutes(allBuses);
            
            // Filter buses by current route
            const filteredBuses = this.filterBusesByRoute(allBuses, this.state.liveTracking.currentRoute);
            this.state.liveTracking.filteredBuses = filteredBuses;
            
            // Display filtered buses on map
            const success = await this.mapVisualizer.showLiveBuses(filteredBuses, this.state.liveTracking.currentRoute);
            
            // Update UI
            if (this.uiController.updateLiveBusList) {
                this.uiController.updateLiveBusList(filteredBuses);
            }
            if (this.uiController.updateLiveStats) {
                this.uiController.updateLiveStats(filteredBuses.length, this.state.liveTracking.lastUpdated);
            }
            
            console.log(`✅ Loaded ${filteredBuses.length} live buses for route ${this.state.liveTracking.currentRoute}`);
            return success;
            
        } catch (error) {
            console.error('❌ Error loading live buses:', error);
            this.showError('Failed to load live bus data. The TTC API might be temporarily unavailable.');
            return false;
        } finally {
            this.state.liveTracking.isLoading = false;
            if (this.uiController.updateLiveLoadingState) {
                this.uiController.updateLiveLoadingState(false);
            }
        }
    }

    async fetchLiveBusData() {
        console.log('📡 Fetching live bus data from TTC API...');
        
        try {
            const response = await fetch(this.LIVE_TRACKING_CONFIG.API_URL, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'Cache-Control': 'no-cache'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            if (!data.vehicles || !Array.isArray(data.vehicles)) {
                throw new Error('Invalid response format from TTC API');
            }
            
            // Process vehicles into our format
            const buses = data.vehicles.map(vehicle => {
                const position = vehicle.position || {};
                const trip = vehicle.trip || {};
                const vehicleInfo = vehicle.vehicle || {};
                
                return {
                    vehicle_id: vehicleInfo.id || 'Unknown',
                    vehicle_label: vehicleInfo.label || `Bus ${vehicleInfo.id || 'Unknown'}`,
                    route_id: trip.route_id || 'Unknown',
                    latitude: parseFloat(position.latitude) || 0,
                    longitude: parseFloat(position.longitude) || 0,
                    speed_mps: parseFloat(position.speed) || 0,
                    bearing: parseFloat(position.bearing) || 0,
                    timestamp: vehicle.timestamp || new Date().toISOString(),
                    occupancy_status: vehicle.occupancy_status || 'UNKNOWN',
                    trip_id: trip.trip_id || 'Unknown'
                };
            }).filter(bus => bus.latitude && bus.longitude); // Filter out buses without valid coordinates
            
            console.log(`✅ Received ${buses.length} valid live buses from TTC API`);
            return buses;
            
        } catch (error) {
            console.error('❌ Error fetching live bus data:', error);
            throw new Error(`Unable to connect to TTC live data: ${error.message}`);
        }
    }

    filterBusesByRoute(buses, routeId) {
        if (!routeId || routeId === 'all') {
            return buses;
        }
        
        return buses.filter(bus => {
            const busRoute = bus.route_id.toString();
            const searchRoute = routeId.toString();
            
            // Exact match or route contains the search string
            return busRoute === searchRoute || busRoute.includes(searchRoute);
        });
    }

    updateAvailableRoutes(buses) {
        const routeSet = new Set();
        buses.forEach(bus => {
            if (bus.route_id && bus.route_id !== 'Unknown') {
                routeSet.add(bus.route_id.toString());
            }
        });
        
        this.state.liveTracking.availableRoutes = Array.from(routeSet).sort((a, b) => {
            // Sort routes numerically
            const numA = parseInt(a) || 0;
            const numB = parseInt(b) || 0;
            return numA - numB;
        });
        
        // Update UI with available routes
        if (this.uiController.updateRouteSuggestions) {
            this.uiController.updateRouteSuggestions(this.state.liveTracking.availableRoutes);
        }
    }

    // Live route management
    handleLiveRouteInput(inputValue) {
        // Store the input value but don't filter yet
        this.state.liveTracking.currentRoute = inputValue.trim();
        
        // Show suggestions if input has content
        if (inputValue.trim() && this.uiController.showLiveRouteSuggestions) {
            const suggestions = this.getRouteSuggestions(inputValue.trim());
            this.uiController.showLiveRouteSuggestions(suggestions);
        } else if (this.uiController.hideLiveRouteSuggestions) {
            this.uiController.hideLiveRouteSuggestions();
        }
    }

    getRouteSuggestions(searchTerm) {
        if (!searchTerm || !this.state.liveTracking.availableRoutes) {
            return [];
        }
        
        const term = searchTerm.toLowerCase();
        return this.state.liveTracking.availableRoutes
            .filter(route => route.toLowerCase().includes(term))
            .slice(0, 10); // Limit to 10 suggestions
    }

    showLiveRouteSuggestions() {
        if (this.uiController.showLiveRouteSuggestions) {
            const suggestions = this.state.liveTracking.currentRoute 
                ? this.getRouteSuggestions(this.state.liveTracking.currentRoute)
                : this.state.liveTracking.availableRoutes.slice(0, 10);
            this.uiController.showLiveRouteSuggestions(suggestions);
        }
    }

    updateLiveRoute() {
        const input = document.getElementById('liveRouteSearch');
        if (!input) return;
        
        const routeNumber = input.value.trim();
        
        if (!routeNumber) {
            // Default to showing all buses
            this.state.liveTracking.currentRoute = 'all';
            input.value = 'all';
        } else {
            this.state.liveTracking.currentRoute = routeNumber;
        }
        
        // Update UI
        const currentRouteElement = document.getElementById('currentLiveRoute');
        if (currentRouteElement) {
            currentRouteElement.textContent = routeNumber || 'all';
        }
        
        // Hide suggestions
        if (this.uiController.hideLiveRouteSuggestions) {
            this.uiController.hideLiveRouteSuggestions();
        }
        
        // Refresh the data with new filter
        if (this.currentVisualization === 'live') {
            this.refreshLivePositions();
        }
    }

    selectLiveRouteFromSuggestion(route) {
        const input = document.getElementById('liveRouteSearch');
        if (input) {
            input.value = route;
            this.state.liveTracking.currentRoute = route;
            this.updateLiveRoute();
        }
    }

    refreshLivePositions() {
        if (this.currentVisualization === 'live') {
            this.loadAndDisplayLiveBuses();
            this.showNotification('Live positions refreshed', 'success');
        }
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
            // If we have search results, update visualization
            this.switchVisualization(this.currentVisualization);
            
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

        // Apply search filter only
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
        if (!this.map) return;
        
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

        if (this.uiController.updateViewportInsights) {
            this.uiController.updateViewportInsights(topRoutes, routesInView.length);
        }
    }

    updateMapLegend() {
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

    toggleTheme() {
        this.currentTheme = this.currentTheme === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', this.currentTheme);
        
        // Update theme button icon
        const themeIcon = document.querySelector('.theme-icon');
        if (themeIcon) {
            themeIcon.textContent = this.currentTheme === 'dark' ? '🌙' : '☀️';
        }
        
        // Store preference
        localStorage.setItem('theme', this.currentTheme);
        
        // Force map refresh with timeout to ensure DOM is updated
        setTimeout(() => {
            if (this.map) {
                this.map.invalidateSize({ animate: false });
            }
            
            // Notify map visualizer about theme change
            if (this.mapVisualizer) {
                this.mapVisualizer.onThemeChange(this.currentTheme);
            }
        }, 150);
        
        console.log(`🎨 Theme switched to ${this.currentTheme}`);
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
        if (this.map) {
            // FIX: Use flyTo for smoother reset with bounds
            this.map.flyTo([43.6532, -79.3832], 11, {
                animate: true,
                duration: 1
            });
        }
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
        const aboutContent = `
            <h2>About TTC Delay Visualization</h2>
            <p>This interactive visualization platform provides insights into Toronto Transit Commission (TTC) bus delays and performance metrics.</p>
            <p><strong>Features:</strong></p>
            <ul>
                <li>Real-time delay visualization across routes</li>
                <li>Heatmap of delay hotspots</li>
                <li>Route comparison and frequency analysis</li>
                <li><strong>NEW:</strong> Live bus tracking with real-time positions from TTC GTFS-RT API</li>
                <li>Interactive search and filtering</li>
            </ul>
            <p><strong>Live Tracking:</strong> Shows real-time bus positions using TTC's official GTFS-RT API.</p>
            <p><strong>Historical Data:</strong> ${this.state.summaryStats.time_period || '2014-2025'}</p>
            <p><strong>Advanced Analytics:</strong> <a href="https://ttcdelay.streamlit.app/" target="_blank" style="color: var(--accent-primary);">View time series, hourly patterns, and forecasting</a></p>
            <p><em>Note: This is an independent project and not affiliated with TTC or the City of Toronto.</em></p>
        `;
        
        this.showModal('About', aboutContent);
    }

    showDataSourceModal() {
        const dataContent = `
            <h2>Data Sources & Methodology</h2>
            <p><strong>Historical Data Sources:</strong></p>
            <ul>
                <li>TTC Route Performance Data (2014-2025)</li>
                <li>Route Geometry Information</li>
                <li>Delay Incident Reports</li>
                <li>Location Analysis Data</li>
            </ul>
            <p><strong>Live Data Source:</strong></p>
            <ul>
                <li>TTC GTFS-RT Real-time Vehicle Positions API</li>
                <li>Updated on manual refresh</li>
                <li>Route-specific bus tracking</li>
            </ul>
            <p><strong>Available Live Data Fields:</strong></p>
            <ul>
                <li>Real Vehicle IDs (not simulated)</li>
                <li>Route Numbers</li>
                <li>GPS Coordinates (Latitude/Longitude)</li>
                <li>Speed (meters per second)</li>
                <li>Bearing (compass direction)</li>
                <li>Occupancy Status</li>
                <li>Timestamp</li>
            </ul>
            <p><strong>Historical Data Period:</strong> ${this.state.summaryStats.time_period || '2014-2025'}</p>
            <p><strong>Live Data Status:</strong> <span id="liveDataStatusModal">${this.state.liveTracking.filteredBuses.length > 0 ? 'Connected' : 'Not connected'}</span></p>
            <p><strong>Advanced Analytics:</strong> <a href="https://ttcdelay.streamlit.app/" target="_blank" style="color: var(--accent-primary);">Explore time series analysis and forecasting</a></p>
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
        if (this.uiController.showNotification) {
            this.uiController.showNotification(message, type);
        }
    }

    showError(message) {
        this.showNotification(message, 'error');
    }

    // Bus selection methods
    selectLiveBus(bus) {
        this.state.liveTracking.selectedBus = bus;
        if (this.mapVisualizer.highlightBus) {
            this.mapVisualizer.highlightBus(bus.vehicle_id);
        }
        if (this.uiController.updateBusDetails) {
            this.uiController.updateBusDetails(bus);
        }
    }

    // Public method to select a route or bus from UI
    selectRoute(routeId) {
        if (this.currentVisualization === 'live') {
            // In live mode, select a bus if it's a bus ID
            const bus = this.state.liveTracking.filteredBuses.find(b => b.vehicle_id === routeId);
            if (bus) {
                this.selectLiveBus(bus);
                return;
            }
            // Otherwise fall through to route selection
        }
        
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
        
        if (this.currentVisualization === 'live') {
            this.state.liveTracking.selectedBus = null;
            if (this.uiController.clearBusDetails) {
                this.uiController.clearBusDetails();
            }
        } else {
            this.state.selectedRoute = null;
            if (this.uiController.clearRouteDetails) {
                this.uiController.clearRouteDetails();
            }
        }
    }

    // Get application state for debugging
    getState() {
        return {
            ...this.state,
            currentTheme: this.currentTheme,
            currentVisualization: this.currentVisualization,
            liveTrackingConfig: this.LIVE_TRACKING_CONFIG
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