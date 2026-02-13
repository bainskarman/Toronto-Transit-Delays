// ==================== MOBILE CONTROLLER - UPDATED ====================
class MobileController {
    constructor() {
        this.currentTab = 'delay';
        this.mobileMap = null;
        this.mobileMapVisualizer = null;
        this.searchTimeout = null;
        this.isLegendOpen = false;
        this.filteredRoutes = null;
        this.mobileData = null;
        this.dataLoader = null;
        this.highlightedRoute = null;
        this.isDataLoaded = false;
        
        // Store event handler references for cleanup
        this.boundHandleSearch = null;
        this.boundSearchClick = null;
        this.boundSearchEnter = null;
        
        // Initialize
        this.init();
    }

    async init() {
        console.log('📱 Initializing mobile interface...');
        
        try {
            // Check if we're on mobile portrait
            if (!this.isMobilePortrait()) {
                console.log('📱 Not on mobile portrait, skipping mobile initialization');
                return;
            }
            
            // Initialize data loader
            this.dataLoader = new DataLoader();
            
            // Show mobile container
            this.showMobileContainer();
            
            // Initialize mobile components
            await this.initializeMobileMap();
            await this.loadMobileData();
            
            // Setup event listeners AFTER elements are ready
            this.setupEventListeners();
            
            // Initialize first visualization
            await this.switchMobileTab('delay');
            
            console.log('✅ Mobile interface initialized');
            
        } catch (error) {
            console.error('❌ Mobile initialization error:', error);
            this.showMobileError('Failed to initialize mobile view. Please try again.');
        }
    }


    isMobilePortrait() {
            return window.innerWidth <= 768 && window.innerHeight > window.innerWidth;
        }


    showMobileContainer() {
        const mobileContainer = document.querySelector('.mobile-container');
        const desktopContainer = document.querySelector('.app-container');
        
        if (mobileContainer) {
            mobileContainer.style.display = 'flex';
            mobileContainer.setAttribute('aria-hidden', 'false');
        }
        
        if (desktopContainer) {
            desktopContainer.style.display = 'none';
        }
        
        document.body.classList.add('mobile-active');
    }

    hideMobileContainer() {
        const mobileContainer = document.querySelector('.mobile-container');
        const desktopContainer = document.querySelector('.app-container');
        
        if (mobileContainer) {
            mobileContainer.style.display = 'none';
            mobileContainer.setAttribute('aria-hidden', 'true');
        }
        
        if (desktopContainer) {
            desktopContainer.style.display = 'flex';
        }
        
        // Remove mobile class from body
        document.body.classList.remove('mobile-active');
    }

    async initializeMobileMap() {
        console.log('🗺️ Initializing mobile map...');
        
        try {
            const mapContainer = document.getElementById('mobile-map');
            if (!mapContainer) {
                throw new Error('Mobile map container not found');
            }
            
            this.mobileMap = L.map('mobile-map', {
                center: [43.6532, -79.3832],
                zoom: 11,
                zoomControl: false,
                attributionControl: false,
                maxBounds: [
                    [43.58, -79.63],
                    [43.86, -79.12]
                ],
                maxBoundsViscosity: 1.0,
                tap: false,
                touchZoom: true,
                scrollWheelZoom: false,
                dragging: true
            });

            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(this.mobileMap);

            // Disable double-click zoom
            this.mobileMap.doubleClickZoom.disable();
            
            console.log('✅ Mobile map initialized');
            
        } catch (error) {
            console.error('❌ Mobile map initialization error:', error);
            throw error;
        }
    }

    async loadMobileData() {
        console.log('📊 Loading mobile data...');
        
        try {
            // Show loading state
            this.showMobileLoading(true);
            
            // Load all required data
            const [routes, geometries, summaryStats] = await Promise.all([
                this.dataLoader.loadRoutePerformance(),
                this.dataLoader.loadRouteGeometries(),
                this.dataLoader.loadSummaryStatistics()
            ]);
            
            console.log('📊 Routes loaded:', routes.length);
            console.log('🗺️ Geometries loaded:', Object.keys(geometries).length);
            console.log('📈 Summary loaded:', summaryStats);
            
            // Filter out routes without geometry
            const routesWithGeometry = routes.filter(route => {
                const routeId = route.Route.toString();
                return geometries[routeId] && geometries[routeId].length > 0;
            });
            
            this.mobileData = { 
                routes: routesWithGeometry, 
                geometries, 
                summary: summaryStats 
            };
            
            // Store filtered routes for search
            this.filteredRoutes = routesWithGeometry;
            
            this.isDataLoaded = true;
            
            // Update mobile KPIs
            this.updateMobileKPIs();
            
            // Hide loading state
            this.showMobileLoading(false);
            
            console.log('✅ Mobile data loaded successfully');
            
        } catch (error) {
            console.error('❌ Mobile data loading error:', error);
            this.showMobileLoading(false);
            this.showMobileError('Failed to load data. Please check your connection and try again.');
            
            // Try to use sample data as fallback
            await this.loadSampleData();
        }
    }

    async loadSampleData() {
        console.log('🔄 Loading sample data for demonstration...');
        
        try {
            const sampleRoutes = this.dataLoader.getSampleRoutePerformance();
            const sampleGeometries = this.dataLoader.getSampleRouteGeometries();
            const sampleSummary = this.dataLoader.getDefaultStatistics();
            
            this.mobileData = {
                routes: sampleRoutes,
                geometries: sampleGeometries,
                summary: sampleSummary
            };
            
            // Store filtered routes for search
            this.filteredRoutes = sampleRoutes;
            
            this.isDataLoaded = true;
            
            this.updateMobileKPIs();
            console.log('✅ Sample data loaded');
            
        } catch (error) {
            console.error('❌ Sample data loading error:', error);
            this.showMobileError('No data available. Please check data files.');
            this.isDataLoaded = false;
        }
    }

    showMobileLoading(show) {
        const mapSection = document.querySelector('.mobile-map-section');
        if (!mapSection) return;
        
        if (show) {
            // Create or show loading overlay
            let loadingOverlay = mapSection.querySelector('.mobile-loading-overlay');
            if (!loadingOverlay) {
                loadingOverlay = document.createElement('div');
                loadingOverlay.className = 'mobile-loading-overlay';
                loadingOverlay.innerHTML = `
                    <div class="mobile-loading-content">
                        <div class="mobile-loading-spinner"></div>
                        <p>Loading data...</p>
                    </div>
                `;
                mapSection.appendChild(loadingOverlay);
            }
            loadingOverlay.style.display = 'flex';
        } else {
            // Hide loading overlay
            const loadingOverlay = mapSection.querySelector('.mobile-loading-overlay');
            if (loadingOverlay) {
                loadingOverlay.style.display = 'none';
            }
        }
    }

    showMobileError(message) {
        const mapSection = document.querySelector('.mobile-map-section');
        if (!mapSection) return;
        
        // Remove existing error
        const existingError = mapSection.querySelector('.mobile-error-overlay');
        if (existingError) {
            existingError.remove();
        }
        
        // Create error overlay
        const errorOverlay = document.createElement('div');
        errorOverlay.className = 'mobile-error-overlay';
        errorOverlay.innerHTML = `
            <div class="mobile-error-content">
                <div class="mobile-error-icon">⚠️</div>
                <p class="mobile-error-message">${message}</p>
            </div>
        `;
        mapSection.appendChild(errorOverlay);
    }

    updateMobileKPIs() {
        if (!this.mobileData || !this.mobileData.summary) {
            console.warn('No mobile data available for KPIs');
            return;
        }
        
        const summary = this.mobileData.summary;
        
        // Update Total Incidents
        const totalIncidentsEl = document.getElementById('mobileTotalIncidents');
        if (totalIncidentsEl) {
            const total = summary.total_delays || summary.data_points || 0;
            totalIncidentsEl.textContent = this.formatNumber(total);
        }
        
        // Update Average Delay
        const avgDelayEl = document.getElementById('mobileAvgDelay');
        if (avgDelayEl) {
            const avg = summary.avg_delay_minutes || summary.avg_delay_min || 0;
            avgDelayEl.textContent = `${avg.toFixed(1)} min`;
        }
    }

    formatNumber(num) {
        if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
        if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
        return num.toLocaleString();
    }

    setupEventListeners() {
        // Segmented control tab switching
        document.querySelectorAll('.mobile-segment').forEach(segment => {
            segment.addEventListener('click', (e) => {
                const visual = e.currentTarget.dataset.visual;
                this.switchMobileTab(visual);
            });
        });

        // Route search - FIXED EVENT LISTENERS
        const searchInput = document.getElementById('mobileRouteSearch');
        const searchBtn = document.getElementById('mobileSearchBtn');
        
        if (searchInput) {
            // Clear existing event listeners first
            searchInput.removeEventListener('input', this.handleSearchInput);
            searchInput.removeEventListener('keypress', this.handleSearchKeypress);
            
            // Add new event listeners
            this.handleSearchInput = (e) => {
                this.handleMobileSearch(e.target.value);
            };
            
            this.handleSearchKeypress = (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault(); // Prevent form submission
                    this.performMobileSearch();
                }
            };
            
            searchInput.addEventListener('input', this.handleSearchInput);
            searchInput.addEventListener('keypress', this.handleSearchKeypress);
            
            // Close search results when input loses focus
            searchInput.addEventListener('blur', () => {
                setTimeout(() => {
                    const searchResults = document.getElementById('mobileSearchResults');
                    if (searchResults) {
                        searchResults.style.display = 'none';
                    }
                }, 200);
            });
        }
        
        if (searchBtn) {
            // Clear existing event listener
            searchBtn.removeEventListener('click', this.handleSearchClick);
            
            // Add new event listener
            this.handleSearchClick = () => {
                this.performMobileSearch();
            };
            
            searchBtn.addEventListener('click', this.handleSearchClick);
        }

        // Legend toggle
        const legendToggle = document.getElementById('mobileLegendToggle');
        const legendClose = document.getElementById('mobileLegendClose');
        
        if (legendToggle) {
            legendToggle.addEventListener('click', () => {
                this.toggleLegendPanel(true);
            });
        }
        
        if (legendClose) {
            legendClose.addEventListener('click', () => {
                this.toggleLegendPanel(false);
            });
        }

        // Close legend when clicking outside
        document.addEventListener('click', (e) => {
            const legendPanel = document.getElementById('mobileLegendPanel');
            const legendToggle = document.getElementById('mobileLegendToggle');
            
            if (this.isLegendOpen && 
                legendPanel && 
                legendToggle &&
                !legendPanel.contains(e.target) && 
                !legendToggle.contains(e.target)) {
                this.toggleLegendPanel(false);
            }
        });

        // Window resize handler
        window.addEventListener('resize', () => {
            this.handleMobileResize();
        });

        // Prevent zoom on double-tap for map
        if (this.mobileMap) {
            this.mobileMap.doubleClickZoom.disable();
        }
    }

    async switchMobileTab(tab) {
        console.log(`📱 Switching to ${tab} tab...`);
        
        // Update active tab
        document.querySelectorAll('.mobile-segment').forEach(segment => {
            const isActive = segment.dataset.visual === tab;
            segment.classList.toggle('active', isActive);
            segment.setAttribute('aria-selected', isActive);
        });
        
        // Show/hide content based on tab
        const mapSection = document.getElementById('mobile-map-section');
        const aboutContent = document.getElementById('mobile-about-content');
        
        if (tab === 'about') {
            // Show about content
            if (mapSection) mapSection.style.display = 'none';
            if (aboutContent) {
                aboutContent.style.display = 'block';
                aboutContent.classList.add('active');
                aboutContent.setAttribute('aria-hidden', 'false');
                
                // Load about content if not already loaded
                if (!aboutContent.innerHTML.trim()) {
                    await this.loadAboutContent();
                }
            }
        } else {
            // Show map
            if (mapSection) mapSection.style.display = 'block';
            if (aboutContent) {
                aboutContent.style.display = 'none';
                aboutContent.classList.remove('active');
                aboutContent.setAttribute('aria-hidden', 'true');
            }
            
            // Update map visualization
            await this.updateMobileMapVisualization(tab);
        }
        
        this.currentTab = tab;
        
        // Close legend if open
        this.toggleLegendPanel(false);
    }

    async updateMobileMapVisualization(visualType, filteredRoutes = null) {
        console.log(`🗺️ Updating mobile map visualization: ${visualType}`);
        
        // Use filtered routes if provided, otherwise use all routes
        const routesToShow = filteredRoutes || this.filteredRoutes || this.mobileData?.routes;
        
        if (!this.mobileMap || !this.mobileData || !routesToShow) {
            console.error('❌ Missing required data for visualization');
            this.showMobileError('No data available for visualization');
            return;
        }
        
        try {
            // Clear previous visualization
            if (this.mobileMapVisualizer) {
                this.mobileMapVisualizer.clearVisualization();
            } else {
                // Initialize visualizer if not already done
                this.mobileMapVisualizer = new MapVisualizer();
                this.mobileMapVisualizer.init(
                    this.mobileMap, 
                    this.mobileData.geometries, 
                    routesToShow
                );
                
                // Store original popup methods
                this.originalCreateRoutePopup = this.mobileMapVisualizer.createRoutePopup;
                this.originalCreateFrequencyPopup = this.mobileMapVisualizer.createFrequencyPopup;
            }
            
            // Override popup methods for mobile
            this.mobileMapVisualizer.createRoutePopup = (route, routeName, avgDelay, delayCount) => 
                this.createMobileRoutePopup(route, routeName, avgDelay, delayCount);
            
            this.mobileMapVisualizer.createFrequencyPopup = (route, routeName, delayCount, avgDelay) => 
                this.createMobileFrequencyPopup(route, routeName, delayCount, avgDelay);
            
            // Apply new visualization
            let success = false;
            if (visualType === 'delay') {
                success = await this.mobileMapVisualizer.showRouteDelays(routesToShow);
            } else if (visualType === 'frequency') {
                success = await this.mobileMapVisualizer.showDelayFrequency(routesToShow);
            }
            
            // Update legend content
            this.updateMobileLegend(visualType);
            
            // Hide legend by default
            this.toggleLegendPanel(false);
            
            if (success) {
                console.log(`✅ Mobile ${visualType} visualization updated successfully`);
            } else {
                console.warn(`⚠️ Mobile ${visualType} visualization had issues`);
                if (routesToShow.length === 0) {
                    this.showMobileError('No routes found. Try a different search.');
                } else {
                    this.showMobileError('No routes to display. Try a different area or search.');
                }
            }
            
        } catch (error) {
            console.error(`❌ Mobile visualization error:`, error);
            this.showMobileError(`Failed to load ${visualType} visualization`);
        }
    }

    // Mobile-specific popup creation methods
    createMobileRoutePopup(route, routeName, avgDelay, delayCount) {
        return `
            <div class="mobile-route-popup">
                <div class="mobile-popup-header">
                    <h3>Route ${route.Route}: ${routeName}</h3>
                </div>
                <div class="mobile-popup-content">
                    <div>Avg Delay: <strong>${avgDelay.toFixed(1)} min</strong></div>
                    <div>Total Delays: ${delayCount.toLocaleString()}</div>
                </div>
                <div class="mobile-popup-actions">
                    <button class="mobile-popup-btn" onclick="window.mobileController.zoomToRoute('${route.Route}')">
                        <i class="fas fa-search-location"></i> Zoom
                    </button>
                </div>
            </div>
        `;
    }

    createMobileFrequencyPopup(route, routeName, delayCount, avgDelay) {
        return `
            <div class="mobile-route-popup">
                <div class="mobile-popup-header">
                    <h3>Route ${route.Route}: ${routeName}</h3>
                </div>
                <div class="mobile-popup-content">
                    <div>Total Delays: <strong>${delayCount.toLocaleString()}</strong></div>
                    <div>Avg Delay: ${avgDelay.toFixed(1)} min</div>
                </div>
                <div class="mobile-popup-actions">
                    <button class="mobile-popup-btn" onclick="window.mobileController.zoomToRoute('${route.Route}')">
                        <i class="fas fa-search-location"></i> Zoom
                    </button>
                </div>
            </div>
        `;
    }

    updateMobileLegend(visualType) {
        const legendContent = document.getElementById('mobileLegendContent');
        if (!legendContent) return;
        
        let legendHTML = '';
        
        if (visualType === 'delay') {
            legendHTML = `
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #10b981;"></div>
                    <div class="legend-label-mobile"><strong>0-5 min</strong> (Low Delay)</div>
                </div>
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #f59e0b;"></div>
                    <div class="legend-label-mobile"><strong>5-10 min</strong> (Moderate)</div>
                </div>
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #ef4444;"></div>
                    <div class="legend-label-mobile"><strong>10-15 min</strong> (High)</div>
                </div>
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #7c3aed;"></div>
                    <div class="legend-label-mobile"><strong>15+ min</strong> (Critical)</div>
                </div>
            `;
        } else if (visualType === 'frequency') {
            legendHTML = `
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #93c5fd;"></div>
                    <div class="legend-label-mobile"><strong>Low</strong> Frequency</div>
                </div>
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #3b82f6;"></div>
                    <div class="legend-label-mobile"><strong>Medium</strong> Frequency</div>
                </div>
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #1d4ed8;"></div>
                    <div class="legend-label-mobile"><strong>High</strong> Frequency</div>
                </div>
                <div class="legend-item-mobile">
                    <div class="legend-color-mobile" style="background: #7e22ce;"></div>
                    <div class="legend-label-mobile"><strong>Very High</strong> Frequency</div>
                </div>
            `;
        }
        
        legendContent.innerHTML = legendHTML;
        
        // Always show the legend toggle button for both tabs
        const legendToggle = document.getElementById('mobileLegendToggle');
        if (legendToggle) {
            legendToggle.style.display = 'flex';
        }
    }

    toggleLegendPanel(show) {
        const legendPanel = document.getElementById('mobileLegendPanel');
        const legendToggle = document.getElementById('mobileLegendToggle');
        
        if (!legendPanel || !legendToggle) return;
        
        this.isLegendOpen = show;
        
        if (show) {
            legendPanel.setAttribute('aria-hidden', 'false');
            legendToggle.classList.add('active');
            legendToggle.innerHTML = '<i class="fas fa-times"></i>';
        } else {
            legendPanel.setAttribute('aria-hidden', 'true');
            legendToggle.classList.remove('active');
            legendToggle.innerHTML = '<i class="fas fa-info"></i>';
        }
    }

    handleMobileSearch(query) {
        clearTimeout(this.searchTimeout);
        this.searchTimeout = setTimeout(() => {
            this.performMobileSearch();
        }, 300);
    }

    performMobileSearch() {
        const searchInput = document.getElementById('mobileRouteSearch');
        const searchResults = document.getElementById('mobileSearchResults');
        
        if (!searchInput || !searchResults) {
            console.warn('Search components not found');
            return;
        }
        
        // Check if mobile data is loaded
        if (!this.mobileData || !this.mobileData.routes) {
            console.warn('Mobile data not loaded yet');
            searchResults.innerHTML = `
                <div class="mobile-search-result">
                    <div class="route-name">Data not loaded yet</div>
                    <div class="route-delay">Please wait...</div>
                </div>
            `;
            searchResults.style.display = 'block';
            return;
        }
        
        const query = searchInput.value.trim().toLowerCase();
        
        if (!query) {
            searchResults.style.display = 'none';
            searchResults.innerHTML = '';
            
            // If search is cleared, show all routes
            this.filteredRoutes = this.mobileData.routes;
            this.updateMobileMapVisualization(this.currentTab, this.filteredRoutes);
            return;
        }
        
        console.log('Searching for:', query);
        console.log('Available routes:', this.mobileData.routes.length);
        
        // Filter routes - Fixed search logic
        this.filteredRoutes = this.mobileData.routes.filter(route => {
            if (!route) return false;
            
            const routeId = route.Route ? route.Route.toString().toLowerCase() : '';
            const routeName = route.route_long_name ? route.route_long_name.toLowerCase() : '';
            
            // Search in both route ID and route name
            return routeId.includes(query) || routeName.includes(query);
        });
        
        console.log('Found routes:', this.filteredRoutes.length);
        
        // Display results
        if (this.filteredRoutes.length > 0) {
            const resultsToShow = this.filteredRoutes.slice(0, 5); // Limit dropdown to 5 results
            
            searchResults.innerHTML = resultsToShow.map(route => {
                const routeId = route.Route ? route.Route.toString() : 'Unknown';
                const routeName = route.route_long_name || 'Unnamed Route';
                const avgDelay = route.Avg_Delay_Min ? route.Avg_Delay_Min.toFixed(1) : 'N/A';
                const delayCount = route.Delay_Count ? route.Delay_Count.toLocaleString() : '0';
                
                return `
                    <div class="mobile-search-result" data-route-id="${routeId}">
                        <div class="route-name">Route ${routeId}: ${routeName}</div>
                        <div class="route-delay">${avgDelay} min avg • ${delayCount} delays</div>
                    </div>
                `;
            }).join('');
            
            searchResults.style.display = 'block';
            
            // Add click handlers
            searchResults.querySelectorAll('.mobile-search-result').forEach(result => {
                result.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const routeId = result.dataset.routeId;
                    console.log('Selected route:', routeId);
                    
                    // Close search results
                    searchResults.style.display = 'none';
                    searchInput.value = '';
                    searchInput.blur();
                    
                    // Zoom to route
                    this.zoomToRoute(routeId);
                });
            });
            
            // Update map visualization with filtered routes
            this.updateMobileMapVisualization(this.currentTab, this.filteredRoutes);
            
        } else {
            searchResults.innerHTML = `
                <div class="mobile-search-result">
                    <div class="route-name">No routes found</div>
                    <div class="route-delay">Try a different search term</div>
                </div>
            `;
            searchResults.style.display = 'block';
            
            // Clear map if no routes found
            if (this.mobileMapVisualizer) {
                this.mobileMapVisualizer.clearVisualization();
            }
        }
        
        // Close search results when clicking outside
        setTimeout(() => {
            const closeHandler = (e) => {
                if (!searchResults.contains(e.target) && e.target !== searchInput) {
                    searchResults.style.display = 'none';
                    document.removeEventListener('click', closeHandler);
                }
            };
            document.addEventListener('click', closeHandler);
        }, 100);
    }


    zoomToRoute(routeId) {
        if (!this.mobileMap || !this.mobileData || !this.mobileData.geometries) {
            console.warn('Cannot zoom: Map or data not available');
            return;
        }
        
        const geometry = this.mobileData.geometries[routeId];
        
        if (geometry && geometry.length > 0) {
            // Create a polyline to get bounds
            const polyline = L.polyline(geometry);
            const bounds = polyline.getBounds();
            
            // Fit map to route bounds
            this.mobileMap.fitBounds(bounds, {
                padding: [50, 50],
                animate: true,
                duration: 0.5
            });
            
            // Highlight the route temporarily
            this.highlightRoute(routeId);
        }
    }

    highlightRoute(routeId) {
        if (!this.mobileMap || !this.mobileData || !this.mobileData.geometries) return;
        
        const geometry = this.mobileData.geometries[routeId];
        
        if (geometry) {
            // Clear any existing highlight
            if (this.highlightedRoute) {
                this.mobileMap.removeLayer(this.highlightedRoute);
            }
            
            // Create highlight polyline
            this.highlightedRoute = L.polyline(geometry, {
                color: '#fbbf24',
                weight: 6,
                opacity: 0.9
            }).addTo(this.mobileMap);
            
            // Bring to front
            this.highlightedRoute.bringToFront();
            
            // Remove highlight after 5 seconds
            setTimeout(() => {
                if (this.highlightedRoute) {
                    this.mobileMap.removeLayer(this.highlightedRoute);
                    this.highlightedRoute = null;
                }
            }, 5000);
        }
    }

    async loadAboutContent() {
        const aboutContent = document.getElementById('mobile-about-content');
        if (!aboutContent) {
            console.error('About content container not found');
            return;
        }
        
        try {
            // Create mobile-optimized about content based on your structure
            aboutContent.innerHTML = `
                <div class="mobile-about-inner">
                    <div class="mobile-about-back">
                        <button class="mobile-about-back-btn" aria-label="Back to map">
                            <i class="fas fa-arrow-left"></i>
                        </button>
                        <h1 class="mobile-about-title">About TTC Delays</h1>
                    </div>
                    
                    <!-- Introduction Section -->
                    <section class="mobile-about-section">
                        <div class="mobile-section-header">
                            <span class="mobile-section-icon">👋</span>
                            <h2 class="mobile-section-title">Introduction</h2>
                        </div>
                        <div class="mobile-about-text">
                            As someone who <span class="mobile-highlight">relies entirely on public transit</span>, I've spent countless hours waiting for buses that either never show up or arrive so late they might as well be a different route entirely.
                        </div>
                        <div class="mobile-about-text">
                            This web app is my attempt to bring transparency to TTC performance, helping fellow transit users make informed decisions.
                    </section>

                    <!-- Project Story Section -->
                    <section class="mobile-about-section">
                        <div class="mobile-section-header">
                            <span class="mobile-section-icon">📊</span>
                            <h2 class="mobile-section-title">Project Story</h2>
                        </div>
                        
                        <div class="mobile-project-story">
                            <div class="mobile-story-paragraph">
                                <div class="mobile-story-content">
                                    This project began in 2023 as simple curiosity about TTC delays, starting with a basic 
                                    <a href="https://public.tableau.com/app/profile/karman.bains/viz/TTCDelayDash/Dashboard1" target="_blank" class="mobile-story-link">Tableau dashboard</a>. 
                                    In 2024, I expanded it into a comprehensive 
                                    <a href="https://app.powerbi.com/view?r=eyJrIjoiOTRkYTMyZjctMjU3Yi00MTQzLTg0NTItZTQ2YjQwMzRkYWRjIiwidCI6ImI2NDE3Y2QwLTFmNzMtNDQ3MS05YTM5LTIwOTUzODIyYTM0YSIsImMiOjN9&source=post_page-----20d7b475d736------------------------------------" target="_blank" class="mobile-story-link">Power BI semantic data model</a>, 
                                    merging multiple datasets for deeper analysis. The project evolved further with a 
                                    <a href="https://ttcdelay.streamlit.app/" target="_blank" class="mobile-story-link">Streamlit app</a> for forecasting and trend analysis, 
                                    and GIS work to map routes geographically. In May 2025, I shared insights in a 
                                    <a href="https://medium.com/@bsinghkarman/tracking-time-lost-a-data-dive-into-torontos-public-transit-delays-20d7b475d736" target="_blank" class="mobile-story-link">Medium article</a>, 
                                    and the current web app was inspired by projects like the 
                                    <a href="https://toronto-parking-production.up.railway.app/" target="_blank" class="mobile-story-link">Toronto Parking Analysis</a>.
                                </div>
                            </div>
                        </div>
                    </section>

                    <!-- Contact Section -->
                    <section class="mobile-about-section">
                        <div class="mobile-section-header">
                            <span class="mobile-section-icon">📬</span>
                            <h2 class="mobile-section-title">Contact</h2>
                        </div>
                        
                        <div class="mobile-contact-grid">
                            <div class="mobile-contact-item">
                                <div class="mobile-contact-icon">
                                    <i class="fas fa-envelope"></i>
                                </div>
                                <div class="mobile-contact-info">
                                    <span class="mobile-contact-label">Email</span>
                                    <a href="mailto:bsinghkarman@gmail.com" class="mobile-contact-link">bsinghkarman@gmail.com</a>
                                </div>
                            </div>
                            
                            <div class="mobile-contact-item">
                                <div class="mobile-contact-icon">
                                    <i class="fas fa-briefcase"></i>
                                </div>
                                <div class="mobile-contact-info">
                                    <span class="mobile-contact-label">Portfolio</span>
                                    <a href="https://bainskarman.github.io/portfolio.io/" target="_blank" class="mobile-contact-link">bainskarman.github.io</a>
                                </div>
                            </div>
                            
                            <div class="mobile-contact-item">
                                <div class="mobile-contact-icon">
                                    <i class="fab fa-github"></i>
                                </div>
                                <div class="mobile-contact-info">
                                    <span class="mobile-contact-label">GitHub</span>
                                    <a href="https://github.com/bainskarman" target="_blank" class="mobile-contact-link">github.com/bainskarman</a>
                                </div>
                            </div>
                        </div>
                    </section>

                    <!-- References Section -->
                    <section class="mobile-about-section">
                        <div class="mobile-section-header">
                            <span class="mobile-section-icon">📚</span>
                            <h2 class="mobile-section-title">References & Data Sources</h2>
                        </div>
                        
                        <div class="mobile-about-text">
                            This project would not be possible without the open data provided by these organizations:
                        </div>
                        
                        <div class="mobile-references">
                            <div class="mobile-reference-item">
                                <a href="https://www.ttc.ca/About-the-TTC/Projects-and-Initiatives/Open-Data" target="_blank" class="mobile-reference-link">
                                    TTC Open Data Portal
                                </a>
                                <div class="mobile-reference-desc">
                                    Historical delay data, route information, and schedule data
                                </div>
                            </div>
                            
                            <div class="mobile-reference-item">
                                <a href="https://open.toronto.ca/" target="_blank" class="mobile-reference-link">
                                    City of Toronto Open Data
                                </a>
                                <div class="mobile-reference-desc">
                                    Geographic data, neighborhood boundaries, and infrastructure information
                                </div>
                            </div>
                        </div>
                    </section>

                    <!-- Support Section -->
                    <section class="mobile-about-section mobile-support-section">
                        <div class="mobile-section-header">
                            <span class="mobile-section-icon">☕</span>
                            <h2 class="mobile-section-title">Support This Project</h2>
                        </div>
                        
                        <div class="mobile-about-text">
                            If you find this visualization useful, consider supporting my work:
                        </div>
                        
                        <button id="mobile-coffee-btn" class="mobile-coffee-btn">
                            <i class="fas fa-coffee"></i>
                            <span>Buy Me a Coffee</span>
                        </button>
                    </section>

                </div>
            `;
            
            // Add back button functionality
            const backButton = aboutContent.querySelector('.mobile-about-back-btn');
            if (backButton) {
                backButton.addEventListener('click', () => {
                    this.switchMobileTab('delay');
                });
            }
            
            // Add coffee button functionality
            const coffeeBtn = aboutContent.querySelector('#mobile-coffee-btn');
            if (coffeeBtn) {
                coffeeBtn.addEventListener('click', () => {
                    window.open('https://buymeacoffee.com/bainskarman', '_blank');
                });
            }
            
            // Add click handlers for all external links
            aboutContent.querySelectorAll('a').forEach(link => {
                link.addEventListener('click', (e) => {
                    e.preventDefault();
                    window.open(link.href, '_blank');
                });
            });
            
            // Add click handlers for email links
            aboutContent.querySelectorAll('a[href^="mailto:"]').forEach(link => {
                link.addEventListener('click', (e) => {
                    // Allow default behavior for email links
                    e.stopPropagation();
                });
            });
            
            console.log('✅ About content loaded successfully');
            
        } catch (error) {
            console.error('❌ About content loading error:', error);
            aboutContent.innerHTML = `
                <div class="mobile-about-back">
                    <button class="mobile-about-back-btn" onclick="window.mobileController.switchMobileTab('delay')">
                        <i class="fas fa-arrow-left"></i>
                        <span>Back</span>
                    </button>
                    <h1 class="mobile-about-title">About</h1>
                </div>
                <div class="mobile-about-error">
                    <p><i class="fas fa-exclamation-triangle"></i> Unable to load about content. Please try again.</p>
                    <button onclick="window.mobileController.loadAboutContent()" class="mobile-retry-btn">
                        <i class="fas fa-redo"></i> Retry
                    </button>
                </div>
            `;
        }
    }

    handleMobileResize() {
        // Refresh map on resize
        if (this.mobileMap) {
            setTimeout(() => {
                this.mobileMap.invalidateSize();
            }, 100);
        }
        
        // Check if orientation changed
        if (this.isMobile()) {
            this.showMobileContainer();
        } else {
            this.hideMobileContainer();
        }
    }

    // Public methods
    getMobileState() {
        return {
            currentTab: this.currentTab,
            isLegendOpen: this.isLegendOpen,
            hasData: !!this.mobileData,
            routesCount: this.mobileData?.routes?.length || 0
        };
    }
}

// Initialize mobile controller when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Check if we should initialize mobile
    const isMobilePortrait = window.innerWidth <= 768 && window.innerHeight > window.innerWidth;
    
    if (isMobilePortrait) {
        console.log('📱 Mobile portrait detected, initializing mobile interface');
        window.mobileController = new MobileController();
    } else {
        console.log('🖥️ Desktop detected, skipping mobile initialization');
    }
});

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = MobileController;
}