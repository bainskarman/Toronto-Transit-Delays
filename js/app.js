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
        this.isMobile = window.innerWidth <= 768;
        this.mobileLegendVisible = false;
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

    setupMobileView() {
  if (!this.isMobile) return;
  
  console.log('📱 Setting up mobile view...');
  
  // Update mobile KPIs
  this.updateMobileKPIs();
  
  // Setup mobile navigation
  this.setupMobileNavigation();
  
  // Setup mobile search
  this.setupMobileSearch();
  
  // Setup mobile legend toggle
  this.setupMobileLegend();
  
  // Hide dashboard tab
  this.hideDashboardOnMobile();
  
  // Force initial visualization to delay
  this.switchVisualization('delay');
}

updateMobileKPIs() {
  if (!this.isMobile || !this.state.summaryStats) return;
  
  const stats = this.state.summaryStats;
  document.getElementById('mobileTotalDelays').textContent = 
    stats.total_delays?.toLocaleString() || '--';
  document.getElementById('mobileAvgDelay').textContent = 
    (stats.avg_delay_minutes?.toFixed(1) || '--') + ' min';
}

setupMobileNavigation() {
  const mobileNavBtns = document.querySelectorAll('.mobile-nav-btn');
  mobileNavBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      const visualType = e.currentTarget.dataset.visual;
      
      // Remove active class from all buttons
      mobileNavBtns.forEach(b => b.classList.remove('active'));
      
      // Add active class to clicked button
      btn.classList.add('active');
      
      // Switch visualization
      this.switchVisualization(visualType);
      
      // On mobile, close legend when switching
      this.hideMobileLegend();
    });
  });
}

setupMobileSearch() {
  const searchInput = document.getElementById('mobileRouteSearch');
  const searchResults = document.getElementById('mobileSearchResults');
  
  if (!searchInput || !searchResults) return;
  
  let searchTimeout;
  
  searchInput.addEventListener('input', (e) => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
      this.handleMobileSearch(e.target.value);
    }, 300);
  });
  
  searchInput.addEventListener('focus', () => {
    if (searchResults.innerHTML.trim()) {
      searchResults.style.display = 'block';
    }
  });
  
  // Close search results when clicking outside
  document.addEventListener('click', (e) => {
    if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
      searchResults.style.display = 'none';
    }
  });
}

handleMobileSearch(query) {
  const searchResults = document.getElementById('mobileSearchResults');
  if (!searchResults) return;
  
  if (!query.trim()) {
    searchResults.innerHTML = '';
    searchResults.style.display = 'none';
    return;
  }
  
  const filteredRoutes = this.state.routes.filter(route => 
    route.Route.toString().toLowerCase().includes(query.toLowerCase()) ||
    (route.route_long_name && route.route_long_name.toLowerCase().includes(query.toLowerCase()))
  ).slice(0, 5); // Limit to 5 results on mobile
  
  if (filteredRoutes.length === 0) {
    searchResults.innerHTML = `
      <div class="search-result-item" style="padding: var(--space-sm); color: var(--text-muted);">
        No routes found
      </div>
    `;
  } else {
    searchResults.innerHTML = filteredRoutes.map(route => `
      <div class="search-result-item" data-route-id="${route.Route}">
        <div style="display: flex; align-items: center; gap: var(--space-sm);">
          <div style="width: 24px; height: 24px; background: var(--accent-primary); 
                      border-radius: var(--radius-sm); display: flex; align-items: center; 
                      justify-content: center; color: white; font-size: 0.75rem; font-weight: bold;">
            ${route.Route}
          </div>
          <div style="flex: 1;">
            <div style="font-size: 0.875rem; font-weight: 500; color: var(--text-primary);">
              ${route.route_long_name || `Route ${route.Route}`}
            </div>
            <div style="font-size: 0.75rem; color: var(--text-secondary);">
              ${route.Avg_Delay_Min.toFixed(1)} min avg delay
            </div>
          </div>
        </div>
      </div>
    `).join('');
    
    // Add click handlers
    searchResults.querySelectorAll('.search-result-item').forEach(item => {
      item.addEventListener('click', () => {
        const routeId = item.dataset.routeId;
        this.selectRoute(routeId);
        searchResults.style.display = 'none';
        searchInput.value = '';
        
        // Zoom to selected route on mobile
        if (this.mapVisualizer) {
          this.mapVisualizer.highlightRoute(routeId);
        }
      });
    });
  }
  
  searchResults.style.display = 'block';
}

    setupMobileLegend() {
        const legendToggle = document.getElementById('mobileLegendToggle');
        const legendClose = document.getElementById('mobileLegendClose');
        const legend = document.getElementById('mobileLegend');
        
        if (!legendToggle || !legend || !legendClose) return;
        
        legendToggle.addEventListener('click', () => {
            this.toggleMobileLegend();
        });
        
        legendClose.addEventListener('click', () => {
            this.hideMobileLegend();
        });
        
        // Close legend when clicking outside on mobile
        document.addEventListener('click', (e) => {
            if (this.mobileLegendVisible && 
                !legend.contains(e.target) && 
                !legendToggle.contains(e.target)) {
            this.hideMobileLegend();
            }
        });
        }

        toggleMobileLegend() {
        const legend = document.getElementById('mobileLegend');
        if (!legend) return;
        
        if (this.mobileLegendVisible) {
            this.hideMobileLegend();
        } else {
            this.showMobileLegend();
        }
        }

        showMobileLegend() {
        const legend = document.getElementById('mobileLegend');
        const legendContent = document.getElementById('mobileLegendContent');
        
        if (!legend || !legendContent) return;
        
        // Get current legend content from map visualizer
        const currentLegend = this.mapVisualizer.getCurrentLegend();
        if (currentLegend) {
            legendContent.innerHTML = currentLegend;
        } else {
            legendContent.innerHTML = `
            <div style="color: var(--text-secondary); text-align: center; padding: var(--space-md);">
                No legend available
            </div>
            `;
        }
        
        legend.classList.add('visible');
        this.mobileLegendVisible = true;
        }

        hideMobileLegend() {
        const legend = document.getElementById('mobileLegend');
        if (!legend) return;
        
        legend.classList.remove('visible');
        this.mobileLegendVisible = false;
        }

        hideDashboardOnMobile() {
        if (!this.isMobile) return;
        
        // Hide dashboard toggle button
        const dashboardToggle = document.querySelector('.toggle-btn[data-visual="dashboard"]');
        if (dashboardToggle) {
            dashboardToggle.style.display = 'none';
        }
        
        // Ensure we're not on dashboard view
        if (this.currentVisualization === 'dashboard') {
            this.switchVisualization('delay');
        }
        }

        // Add to init() method in TTCVisualizationApp
        async init() {
        console.log('🚍 Initializing TTC Delay Visualization...');
        
        try {
            // Check if mobile
            this.isMobile = window.innerWidth <= 768;
            
            // Initialize modules
            this.dataLoader = new DataLoader();
            this.mapVisualizer = new MapVisualizer();
            this.uiController = new UIController(this);
            
            // Load application data
            await this.loadData();
            
            // Initialize UI components
            this.uiController.init();
            
            // Setup mobile view if needed
            if (this.isMobile) {
            this.setupMobileView();
            }
            
            // Initialize map (only for delay/frequency modes)
            await this.initializeMap();
            
            // Set up event listeners
            this.setupEventListeners();
            
            // Update UI with initial data
            this.updateUI();
            
            // Update mobile KPIs if on mobile
            if (this.isMobile) {
            this.updateMobileKPIs();
            }
            
            console.log('🎉 TTC Delay Visualization initialized successfully');
            
        } catch (error) {
            console.error('❌ Failed to initialize application:', error);
            this.showError('Failed to initialize application. Please refresh the page.');
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
            
            // Update active toggle buttons (both desktop and mobile)
            this.updateToggleButtons(visualType);
            
            // Handle dashboard - hide on mobile
            if (visualType === 'dashboard' && this.isMobile) {
            console.log('📱 Dashboard not available on mobile');
            this.showNotification('Dashboard not available on mobile view', 'info');
            return;
            }
            
            // Clear existing visualization if switching away from map
            if (this.mapVisualizer && visualType !== 'dashboard' && visualType !== 'about') {
            this.mapVisualizer.clearVisualization();
            }
            
            // Show/hide dashboard
            if (visualType === 'dashboard') {
            this.showDashboard();
            return;
            } else if (visualType === 'about') {
            this.showAboutPage();
            return;
            } else {
            this.hideDashboard();
            this.hideAboutPage();
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
            
            // Update legend (mobile or desktop)
            this.updateMapLegend();
            
            // Update mobile legend if open
            if (this.isMobile && this.mobileLegendVisible) {
            this.showMobileLegend();
            }
            
            console.log(`✅ Switched to ${visualType} visualization - Success: ${success}`);
            
        } catch (error) {
            console.error(`❌ Error switching to ${visualType} visualization:`, error);
            this.showError(`Failed to load ${visualType} visualization`);
        }
        }
    
    showAboutPage() {
  if (this.isMobile) {
    // On mobile, show about page
    document.body.classList.add('about-active');
    document.querySelector('.map-container').style.display = 'none';
    document.querySelector('.mobile-header').style.display = 'none';
    document.querySelector('.mobile-legend-toggle').style.display = 'none';
  }
}

    hideAboutPage() {
        if (this.isMobile) {
            document.body.classList.remove('about-active');
            document.querySelector('.map-container').style.display = 'block';
            document.querySelector('.mobile-header').style.display = 'flex';
            document.querySelector('.mobile-legend-toggle').style.display = 'flex';
        }
        }

        // Add resize handler
        handleResize() {
        const wasMobile = this.isMobile;
        this.isMobile = window.innerWidth <= 768;
        
        if (wasMobile !== this.isMobile) {
            // Mobile state changed, reload appropriate view
            location.reload();
        }
        
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


    // Update toggle button states
    updateToggleButtons(activeVisual) {
        // Update desktop buttons
        document.querySelectorAll('.toggle-btn').forEach(btn => {
            if (btn.dataset.visual === activeVisual) {
            btn.classList.add('active');
            btn.setAttribute('aria-selected', 'true');
            } else {
            btn.classList.remove('active');
            btn.setAttribute('aria-selected', 'false');
            }
        });
        
        // Update mobile buttons
        if (this.isMobile) {
            document.querySelectorAll('.mobile-nav-btn').forEach(btn => {
            if (btn.dataset.visual === activeVisual) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
            });
        }
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