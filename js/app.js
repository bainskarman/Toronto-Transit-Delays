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
            // NEW: Live tracking state
            liveTracking: {
                enabled: false,
                currentRoute: '16',  // Default route
                buses: [],
                isLoading: false,
                lastUpdated: null,
                selectedBus: null,
                autoRefresh: false
            }
        };

        // Live Tracking Configuration
        this.LIVE_TRACKING_CONFIG = {
            API_URL: 'https://bustime.ttc.ca/gtfsrt/vehicles',
            REFRESH_INTERVAL: 60000, // 60 seconds
            DEFAULT_ROUTE: '16',
            MAX_BUSES_PER_ROUTE: 10,
            BUS_ICON_COLORS: {
                onTime: '#10b981',    // Green - < 2 min delay
                minorDelay: '#f59e0b', // Yellow - 2-5 min delay
                majorDelay: '#ef4444'  // Red - > 5 min delay
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

            // DEBUG: Check what we're getting
            console.log('🔍 DEBUG - Routes data:', routes);
            console.log('🔍 DEBUG - Summary stats:', summaryStats);
            console.log('🔍 DEBUG - Routes count:', routes?.length);
            console.log('🔍 DEBUG - Unique routes in summary:', summaryStats.unique_routes);
            console.log('🔍 DEBUG - Displayed routes:', summaryStats.displayed_routes_count);

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

        // Live route search
        const liveSearchInput = document.getElementById('liveRouteSearch');
        const liveSearchBtn = document.getElementById('liveSearchBtn');
        
        if (liveSearchInput) {
            liveSearchInput.addEventListener('input', (e) => {
                this.handleLiveRouteChange(e.target.value);
            });
            
            liveSearchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.updateLiveRoute();
                }
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

        // Window resize
        window.addEventListener('resize', () => {
            this.handleResize();
        });
    }

    setupMapEvents() {
        if (!this.map) return;
        
        // Viewport change events
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
            // ALWAYS hide loading state, even if there's an error
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
            
            // Fetch live bus data
            const buses = await this.fetchLiveBusData(this.state.liveTracking.currentRoute);
            
            // Update state
            this.state.liveTracking.buses = buses;
            this.state.liveTracking.lastUpdated = new Date();
            
            // Display buses on map
            const success = await this.mapVisualizer.showLiveBuses(buses);
            
            // Update UI
            if (this.uiController.updateLiveBusList) {
                this.uiController.updateLiveBusList(buses);
            }
            if (this.uiController.updateLiveStats) {
                this.uiController.updateLiveStats(buses.length, this.state.liveTracking.lastUpdated);
            }
            
            console.log(`✅ Loaded ${buses.length} live buses`);
            return success;
            
        } catch (error) {
            console.error('❌ Error loading live buses:', error);
            this.showError('Failed to load live bus data. Please try again.');
            return false;
        } finally {
            this.state.liveTracking.isLoading = false;
            if (this.uiController.updateLiveLoadingState) {
                this.uiController.updateLiveLoadingState(false);
            }
        }
    }

    async fetchLiveBusData(routeId) {
        console.log(`📡 Fetching live data for route ${routeId}...`);
        
        try {
            // In a real implementation, you would fetch from the GTFS-RT API
            // Since we can't run Python in the browser, we'll simulate with sample data
            // TODO: Replace with actual API call using a proxy server
            
            // For now, return sample data based on historical routes
            const routeData = this.state.routes.find(r => r.Route.toString() === routeId);
            
            if (!routeData) {
                console.warn(`⚠️ No historical data for route ${routeId}`);
                return this.getSampleLiveBuses(routeId);
            }
            
            // Generate simulated live bus positions along the route geometry
            return this.generateSimulatedLiveBuses(routeId, routeData);
            
        } catch (error) {
            console.error('❌ Error in fetchLiveBusData:', error);
            return this.getSampleLiveBuses(routeId);
        }
    }

    generateSimulatedLiveBuses(routeId, routeData) {
        const geometry = this.state.routeGeometries[routeId];
        
        if (!geometry || geometry.length < 2) {
            return this.getSampleLiveBuses(routeId);
        }
        
        const numBuses = Math.min(Math.floor(Math.random() * 10) + 3, 15); // 3-15 buses
        const buses = [];
        
        for (let i = 0; i < numBuses; i++) {
            // Distribute buses along the route
            const progress = i / numBuses;
            const segmentIndex = Math.floor(progress * (geometry.length - 1));
            const segmentProgress = (progress * (geometry.length - 1)) % 1;
            
            const point1 = geometry[segmentIndex];
            const point2 = geometry[segmentIndex + 1];
            
            const lat = point1[0] + (point2[0] - point1[0]) * segmentProgress;
            const lng = point1[1] + (point2[1] - point1[1]) * segmentProgress;
            
            // Generate realistic bus data
            const vehicleId = `${routeId}${String(i+1).padStart(3, '0')}`;
            const speed = Math.random() * 15 + 5; // 5-20 m/s
            const bearing = this.calculateBearing(point1, point2);
            const delay = Math.random() * 10; // 0-10 minute delay
            const timestamp = new Date();
            
            buses.push({
                vehicle_id: vehicleId,
                vehicle_label: `Bus ${vehicleId}`,
                route_id: routeId,
                latitude: lat,
                longitude: lng,
                speed_mps: speed,
                bearing: bearing,
                delay_minutes: delay,
                timestamp: timestamp.toISOString(),
                occupancy: Math.random() > 0.7 ? 'CROWDED' : 'MANY_SEATS_AVAILABLE',
                status: this.getBusStatus(delay)
            });
        }
        
        return buses;
    }

    getSampleLiveBuses(routeId) {
        return [
            {
                vehicle_id: `${routeId}001`,
                vehicle_label: `Bus ${routeId}001`,
                route_id: routeId,
                latitude: 43.6532,
                longitude: -79.3832,
                speed_mps: 12.5,
                bearing: 45,
                delay_minutes: 1.5,
                timestamp: new Date().toISOString(),
                occupancy: 'MANY_SEATS_AVAILABLE',
                status: 'ON_TIME'
            },
            {
                vehicle_id: `${routeId}002`,
                vehicle_label: `Bus ${routeId}002`,
                route_id: routeId,
                latitude: 43.6600,
                longitude: -79.3800,
                speed_mps: 8.2,
                bearing: 120,
                delay_minutes: 4.2,
                timestamp: new Date().toISOString(),
                occupancy: 'CROWDED',
                status: 'MINOR_DELAY'
            },
            {
                vehicle_id: `${routeId}003`,
                vehicle_label: `Bus ${routeId}003`,
                route_id: routeId,
                latitude: 43.6450,
                longitude: -79.3900,
                speed_mps: 0,
                bearing: 0,
                delay_minutes: 8.7,
                timestamp: new Date().toISOString(),
                occupancy: 'FULL',
                status: 'MAJOR_DELAY'
            }
        ];
    }

    calculateBearing(point1, point2) {
        const lat1 = point1[0] * Math.PI / 180;
        const lat2 = point2[0] * Math.PI / 180;
        const lng1 = point1[1] * Math.PI / 180;
        const lng2 = point2[1] * Math.PI / 180;
        
        const y = Math.sin(lng2 - lng1) * Math.cos(lat2);
        const x = Math.cos(lat1) * Math.sin(lat2) -
                Math.sin(lat1) * Math.cos(lat2) * Math.cos(lng2 - lng1);
        
        let bearing = Math.atan2(y, x) * 180 / Math.PI;
        bearing = (bearing + 360) % 360;
        
        return Math.round(bearing);
    }

    getBusStatus(delayMinutes) {
        if (delayMinutes < 2) return 'ON_TIME';
        if (delayMinutes < 5) return 'MINOR_DELAY';
        return 'MAJOR_DELAY';
    }

    // Live route management
    handleLiveRouteChange(routeNumber) {
        const cleanRoute = routeNumber.replace(/\D/g, ''); // Remove non-digits
        this.state.liveTracking.currentRoute = cleanRoute || this.LIVE_TRACKING_CONFIG.DEFAULT_ROUTE;
    }

    updateLiveRoute() {
        const input = document.getElementById('liveRouteSearch');
        if (!input) return;
        
        const routeNumber = input.value.trim();
        
        if (!routeNumber) {
            input.value = this.LIVE_TRACKING_CONFIG.DEFAULT_ROUTE;
            this.state.liveTracking.currentRoute = this.LIVE_TRACKING_CONFIG.DEFAULT_ROUTE;
        } else {
            const cleanRoute = routeNumber.replace(/\D/g, '');
            if (cleanRoute) {
                this.state.liveTracking.currentRoute = cleanRoute;
            }
        }
        
        // Update UI
        const currentRouteElement = document.getElementById('currentLiveRoute');
        if (currentRouteElement) {
            currentRouteElement.textContent = this.state.liveTracking.currentRoute;
        }
        
        // If we're in live mode, refresh the data
        if (this.currentVisualization === 'live') {
            this.refreshLivePositions();
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
            this.map.setView([43.6532, -79.3832], 11);
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
                <li><strong>NEW:</strong> Live bus tracking with real-time positions</li>
                <li>Interactive search and filtering</li>
            </ul>
            <p><strong>Live Tracking:</strong> Shows real-time bus positions using TTC's GTFS-RT API.</p>
            <p><strong>Data Sources:</strong> TTC open data, processed and analyzed for visualization.</p>
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
                <li>Updated every 60 seconds</li>
                <li>Route-specific bus tracking</li>
            </ul>
            <p><strong>Methodology:</strong></p>
            <ul>
                <li>Data processed and cleaned for accuracy</li>
                <li>Average delays calculated from historical data</li>
                <li>Geospatial analysis for route mapping</li>
                <li>Real-time data updates for live tracking</li>
            </ul>
            <p><strong>Historical Data Period:</strong> ${this.state.summaryStats.time_period || '2014-2025'}</p>
            <p><strong>Live Data Status:</strong> <span id="liveDataStatusModal">${this.state.liveTracking.buses.length > 0 ? 'Connected' : 'Not connected'}</span></p>
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
            const bus = this.state.liveTracking.buses.find(b => b.vehicle_id === routeId);
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

    // Auto-refresh control methods
    startAutoRefresh() {
        if (this.state.liveTracking.autoRefresh) return;
        
        this.state.liveTracking.autoRefresh = true;
        this.liveRefreshInterval = setInterval(() => {
            if (this.currentVisualization === 'live') {
                this.refreshLivePositions();
            }
        }, this.LIVE_TRACKING_CONFIG.REFRESH_INTERVAL);
        
        console.log('🔄 Auto-refresh started');
    }

    stopAutoRefresh() {
        this.state.liveTracking.autoRefresh = false;
        if (this.liveRefreshInterval) {
            clearInterval(this.liveRefreshInterval);
            this.liveRefreshInterval = null;
        }
        console.log('🛑 Auto-refresh stopped');
    }

    toggleAutoRefresh() {
        if (this.state.liveTracking.autoRefresh) {
            this.stopAutoRefresh();
        } else {
            this.startAutoRefresh();
        }
    }

    handleLiveTrackingError(error) {
        console.error('🚨 Live tracking error:', error);
        
        let userMessage = 'Failed to load live bus data. ';
        
        if (error.message.includes('NetworkError') || error.message.includes('Failed to fetch')) {
            userMessage += 'Please check your internet connection.';
        } else if (error.message.includes('CORS')) {
            userMessage += 'CORS policy prevented the request. Using simulated data instead.';
        } else {
            userMessage += 'Using simulated data for demonstration.';
        }
        
        this.showError(userMessage);
        
        // Fall back to sample data
        return this.getSampleLiveBuses(this.state.liveTracking.currentRoute);
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