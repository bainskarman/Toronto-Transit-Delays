// User Interface Controller for TTC Delay Visualization
class UIController {
    constructor(app) {
        this.app = app;
        this.notificationContainer = null;
        this.charts = new Map();
        this.searchTimeout = null;
        this.isMobile = window.innerWidth <= 768;
        
        this.init();
    }

    init() {
        console.log('🎛️ Initializing UI controller...');
        
        this.createNotificationContainer();
        this.setupMobileView();
        this.setupAccessibility();
        
        console.log('✅ UI controller initialized');
    }

    // Metrics and Data Display
    updateMetrics(summaryStats) {
        console.log('📊 SummaryStats received for metrics:', summaryStats);
        
        // Handle cases where data might be missing or undefined
        const metrics = {
            totalDelays: summaryStats?.total_delays?.toLocaleString() || '0',
            avgDelay: (summaryStats?.avg_delay_minutes || summaryStats?.avg_delay_min)?.toFixed(1) + ' min' || '0 min',
            routesTracked: this.getRoutesTracked(summaryStats),
            coverage: summaryStats?.coverage_percentage?.toFixed(1) + '%' || '0%'
        };

        console.log('📊 Calculated metrics:', metrics);

        // Update metric cards
        document.getElementById('totalDelays').textContent = metrics.totalDelays;
        document.getElementById('avgDelay').textContent = metrics.avgDelay;
        document.getElementById('routesTracked').textContent = metrics.routesTracked;
        document.getElementById('coverage').textContent = metrics.coverage;

        // Update last refreshed date
        this.updateLastRefreshedDate(summaryStats);
    }

    // NEW: Helper method to get routes tracked with fallbacks
    getRoutesTracked(summaryStats) {
        if (!summaryStats) return '--';
        
        // Try multiple possible field names
        const routesCount = 
            summaryStats.displayed_routes_count ||
            summaryStats.unique_routes || 
            summaryStats.routes_tracked ||
            summaryStats.total_routes;
        
        console.log('🔍 Routes tracked calculation:', {
            displayed_routes_count: summaryStats.displayed_routes_count,
            unique_routes: summaryStats.unique_routes,
            routes_tracked: summaryStats.routes_tracked,
            total_routes: summaryStats.total_routes,
            final: routesCount
        });
        
        return routesCount ? routesCount.toLocaleString() : '--';
    }

    updateTopRoutes(routes) {
        const container = document.getElementById('topRoutesList');
        
        if (!routes || routes.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <span>📊</span>
                    <p>No route data available</p>
                </div>
            `;
            return;
        }

        const routesHtml = routes.map((route, index) => {
            const routeId = route.Route.toString();
            const routeName = route.route_long_name || `Route ${routeId}`;
            const avgDelay = route.Avg_Delay_Min.toFixed(1);
            const delayCount = route.Delay_Count.toLocaleString();
            const delayClass = this.getDelayClass(route.Avg_Delay_Min);
            
            return `
                <div class="route-item" data-route-id="${routeId}" role="button" tabindex="0">
                    <div class="route-header">
                        <span class="route-name">${index + 1}. ${routeName}</span>
                        <span class="route-delay ${delayClass}">${avgDelay} min</span>
                    </div>
                    <div class="route-meta">
                        <span>${delayCount} delays</span>
                        <span>${route.On_Time_Percentage ? route.On_Time_Percentage.toFixed(1) + '% on time' : ''}</span>
                    </div>
                </div>
            `;
        }).join('');

        container.innerHTML = routesHtml;

        // Add click handlers
        container.querySelectorAll('.route-item').forEach(item => {
            item.addEventListener('click', () => {
                const routeId = item.dataset.routeId;
                this.app.selectRoute(routeId);
            });
            
            item.addEventListener('keypress', (e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    const routeId = item.dataset.routeId;
                    this.app.selectRoute(routeId);
                }
            });
        });
    }

    updateViewportInsights(topRoutes, totalInView) {
        const container = document.getElementById('viewportRoutes');
        const hint = document.getElementById('viewportHint');

        if (!topRoutes || topRoutes.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <span>🔍</span>
                    <p>No routes in current view</p>
                </div>
            `;
            hint.textContent = 'Zoom in to see street-level insights and the top 5 streets for this area.';
            return;
        }

        if (this.app.map.getZoom() < 13) {
            container.innerHTML = `
                <div class="empty-state">
                    <span>🔍</span>
                    <p>Zoom in for detailed insights</p>
                </div>
            `;
            hint.textContent = 'Zoom to street level to see the top 5 streets for this area.';
            return;
        }

        const routesHtml = topRoutes.map((route, index) => {
            const routeId = route.Route.toString();
            const routeName = route.route_long_name || `Route ${routeId}`;
            const avgDelay = route.Avg_Delay_Min.toFixed(1);
            const delayClass = this.getDelayClass(route.Avg_Delay_Min);
            
            return `
                <div class="viewport-route" data-route-id="${routeId}" role="button" tabindex="0">
                    <div class="viewport-route-rank">${index + 1}</div>
                    <div class="viewport-route-info">
                        <div class="viewport-route-name">${routeName}</div>
                        <div class="viewport-route-delay ${delayClass}">${avgDelay} min avg delay</div>
                    </div>
                </div>
            `;
        }).join('');

        container.innerHTML = routesHtml;
        hint.textContent = `Showing top ${topRoutes.length} of ${totalInView} routes in view`;

        // Add click handlers
        container.querySelectorAll('.viewport-route').forEach(item => {
            item.addEventListener('click', () => {
                const routeId = item.dataset.routeId;
                this.app.selectRoute(routeId);
            });
        });
    }

    updateLastRefreshedDate(summaryStats) {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement && summaryStats) {
            // Use data_refresh_date for the header (when data was last refreshed)
            const refreshDate = summaryStats.data_refresh_date || summaryStats.updated_at;
            if (refreshDate) {
                const date = new Date(refreshDate);
                lastUpdatedElement.textContent = date.toLocaleDateString();
            } else {
                lastUpdatedElement.textContent = '--';
            }
        }
    }

    updateDataSummary(summaryStats) {
        // Show data period (e.g., "2014-2025")
        document.getElementById('timePeriod').textContent = summaryStats.time_period || '--';
        
        // Show total delay incidents
        document.getElementById('dataPoints').textContent = summaryStats.total_delays?.toLocaleString() || '--';
        
        // Show most recent data date
        if (summaryStats.data_most_recent_date) {
            try {
                const recentDate = new Date(summaryStats.data_most_recent_date);
                document.getElementById('dataUpdate').textContent = recentDate.toLocaleDateString();
            } catch (e) {
                document.getElementById('dataUpdate').textContent = '--';
            }
        } else {
            document.getElementById('dataUpdate').textContent = '--';
        }
    }

    // Chart Management
    initializeCharts(routes) {
        this.initializeDelayDistributionChart(routes);
        this.initializePeakHoursChart(routes);
    }

    initializeDelayDistributionChart(routes) {
        const canvas = document.getElementById('delayChart');
        if (!canvas) return;

        const ctx = canvas.getContext('2d');
        
        // Prepare data for delay distribution
        const delayRanges = [
            { min: 0, max: 5, label: '0-5 min', count: 0 },
            { min: 5, max: 10, label: '5-10 min', count: 0 },
            { min: 10, max: 15, label: '10-15 min', count: 0 },
            { min: 15, max: Infinity, label: '15+ min', count: 0 }
        ];

        routes.forEach(route => {
            const delay = route.Avg_Delay_Min;
            const range = delayRanges.find(r => delay >= r.min && delay < r.max);
            if (range) range.count++;
        });

        const data = {
            labels: delayRanges.map(r => r.label),
            datasets: [{
                data: delayRanges.map(r => r.count),
                backgroundColor: [
                    '#10b981', // green
                    '#f59e0b', // yellow
                    '#ef4444', // red
                    '#7c3aed'  // purple
                ],
                borderWidth: 0,
                borderRadius: 4
            }]
        };

        this.charts.set('delayDistribution', new Chart(ctx, {
            type: 'bar',
            data: data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (context) => {
                                const total = routes.length;
                                const percentage = ((context.parsed.y / total) * 100).toFixed(1);
                                return `${context.parsed.y} routes (${percentage}%)`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(255, 255, 255, 0.1)' },
                        ticks: { color: '#a0aec0' }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: '#a0aec0', maxRotation: 45 }
                    }
                }
            }
        }));
    }

    initializePeakHoursChart(routes) {
        // This would be implemented when we have time-based data
        // For now, we'll create a placeholder
        console.log('📈 Peak hours chart placeholder initialized');
    }

    // Search Functionality
    updateSearchResults(routes) {
        const container = document.getElementById('searchResults');
        
        if (!routes || routes.length === 0) {
            container.innerHTML = `
                <div class="search-result-item">
                    <div class="search-result-content">
                        <div class="search-result-title">No routes found</div>
                        <div class="search-result-subtitle">Try a different search term</div>
                    </div>
                </div>
            `;
            container.style.display = 'block';
            return;
        }

        const resultsHtml = routes.slice(0, 8).map(route => {
            const routeId = route.Route.toString();
            const routeName = route.route_long_name || `Route ${routeId}`;
            const avgDelay = route.Avg_Delay_Min.toFixed(1);
            const delayCount = route.Delay_Count.toLocaleString();
            const delayClass = this.getDelayClass(route.Avg_Delay_Min);
            
            return `
                <div class="search-result-item" data-route-id="${routeId}" role="button" tabindex="0">
                    <div class="search-result-icon">${routeId}</div>
                    <div class="search-result-content">
                        <div class="search-result-title">${routeName}</div>
                        <div class="search-result-meta">
                            <span class="delay-indicator ${delayClass}">${avgDelay} min avg</span>
                            <span>${delayCount} delays</span>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        container.innerHTML = resultsHtml;
        container.style.display = 'block';

        // Add click handlers
        container.querySelectorAll('.search-result-item').forEach(item => {
            item.addEventListener('click', () => {
                const routeId = item.dataset.routeId;
                this.app.selectRoute(routeId);
                this.clearSearchResults();
                document.getElementById('routeSearch').value = '';
            });
        });
    }

    clearSearchResults() {
        const container = document.getElementById('searchResults');
        container.style.display = 'none';
        container.innerHTML = '';
    }

    // Visualization Controls
    updateVisualizationToggles(activeVisual) {
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

    updateMapLegend(legendHtml) {
        const container = document.getElementById('mapLegend');
        if (legendHtml) {
            container.innerHTML = legendHtml;
            container.style.display = 'block';
        } else {
            container.style.display = 'none';
        }
    }

    // Route Details
    updateRouteDetails(route) {
        // This would update a dedicated route details panel
        // For now, we'll show a notification
        const routeName = route.route_long_name || `Route ${route.Route}`;
        this.showNotification(`Selected: ${routeName} - ${route.Avg_Delay_Min.toFixed(1)} min avg delay`, 'info');
    }

    clearRouteDetails() {
        // Clear any route-specific UI elements
    }

    // Loading States
    showLoadingState() {
        const mapContainer = document.querySelector('.map-container');
        if (!mapContainer.querySelector('.loading-overlay')) {
            const overlay = document.createElement('div');
            overlay.className = 'loading-overlay';
            overlay.innerHTML = `
                <div class="loading-content">
                    <div class="loading-spinner large"></div>
                    <p>Loading visualization...</p>
                </div>
            `;
            mapContainer.appendChild(overlay);
        }
    }

    hideLoadingState() {
        const overlay = document.querySelector('.loading-overlay');
        if (overlay) {
            overlay.remove();
        }
    }

    // Notification System
    createNotificationContainer() {
        this.notificationContainer = document.createElement('div');
        this.notificationContainer.className = 'notification-container';
        document.body.appendChild(this.notificationContainer);
    }

    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `notification ${type} show`;
        
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌'
        };

        notification.innerHTML = `
            <div class="notification-icon">${icons[type] || icons.info}</div>
            <div class="notification-content">
                <div class="notification-title">${this.getNotificationTitle(type)}</div>
                <div class="notification-message">${message}</div>
            </div>
            <button class="notification-close" aria-label="Close notification">&times;</button>
        `;

        this.notificationContainer.appendChild(notification);

        // Auto-remove after 5 seconds
        const autoRemove = setTimeout(() => {
            this.removeNotification(notification);
        }, 5000);

        // Close button handler
        notification.querySelector('.notification-close').addEventListener('click', () => {
            clearTimeout(autoRemove);
            this.removeNotification(notification);
        });

        return notification;
    }

    removeNotification(notification) {
        notification.classList.remove('show');
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 300);
    }

    getNotificationTitle(type) {
        const titles = {
            info: 'Information',
            success: 'Success',
            warning: 'Warning',
            error: 'Error'
        };
        return titles[type] || 'Notification';
    }

    // Mobile View Management
    setupMobileView() {
        if (!this.isMobile) return;

        // Add mobile-specific classes
        document.body.classList.add('mobile-view');
        
        // Setup mobile menu toggle
        this.setupMobileMenu();
        
        // Adjust chart sizes for mobile
        this.adjustChartsForMobile();
    }

    setupMobileMenu() {
        // Create mobile menu button if it doesn't exist
        if (!document.querySelector('.mobile-menu-toggle')) {
            const toggle = document.createElement('button');
            toggle.className = 'mobile-menu-toggle';
            toggle.innerHTML = '☰';
            toggle.setAttribute('aria-label', 'Toggle menu');
            
            toggle.addEventListener('click', () => {
                document.body.classList.toggle('mobile-menu-open');
            });
            
            document.querySelector('.app-header').appendChild(toggle);
        }
    }

    adjustChartsForMobile() {
        // Adjust chart canvas sizes for mobile
        const canvases = document.querySelectorAll('canvas');
        canvases.forEach(canvas => {
            canvas.style.maxWidth = '100%';
            canvas.style.height = 'auto';
        });
    }

    // Accessibility Features
    setupAccessibility() {
        // Add skip to main content link
        this.addSkipLink();
        
        // Setup keyboard navigation
        this.setupKeyboardNavigation();
        
        // Announce dynamic content changes
        this.setupLiveRegions();
    }

    addSkipLink() {
        const skipLink = document.createElement('a');
        skipLink.href = '#main-content';
        skipLink.className = 'skip-link';
        skipLink.textContent = 'Skip to main content';
        
        document.body.insertBefore(skipLink, document.body.firstChild);
    }

    setupKeyboardNavigation() {
        // Add keyboard navigation for custom components
        document.addEventListener('keydown', (e) => {
            // Handle escape key for modals and overlays
            if (e.key === 'Escape') {
                this.handleEscapeKey();
            }
            
            // Handle tab key for custom focus management
            if (e.key === 'Tab') {
                this.handleTabKey(e);
            }
        });
    }

    setupLiveRegions() {
        // Create live region for dynamic content announcements
        const liveRegion = document.createElement('div');
        liveRegion.id = 'live-region';
        liveRegion.setAttribute('aria-live', 'polite');
        liveRegion.setAttribute('aria-atomic', 'true');
        liveRegion.className = 'sr-only';
        document.body.appendChild(liveRegion);
    }

    announceToScreenReader(message) {
        const liveRegion = document.getElementById('live-region');
        if (liveRegion) {
            liveRegion.textContent = message;
            
            // Clear after announcement
            setTimeout(() => {
                liveRegion.textContent = '';
            }, 1000);
        }
    }

    // Utility Methods
    getDelayClass(delay) {
        if (delay < 5) return 'low';
        if (delay < 10) return 'medium';
        if (delay < 15) return 'high';
        return 'critical';
    }

    handleEscapeKey() {
        // Close modals, search results, etc.
        this.clearSearchResults();
        
        // Close any open popups
        const openPopups = document.querySelectorAll('.leaflet-popup');
        openPopups.forEach(popup => {
            popup.remove();
        });
    }

    handleTabKey(e) {
        // Ensure custom components are tabbable
        const focusableElements = document.querySelectorAll(
            'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );
        
        const firstElement = focusableElements[0];
        const lastElement = focusableElements[focusableElements.length - 1];
        
        if (e.shiftKey && document.activeElement === firstElement) {
            lastElement.focus();
            e.preventDefault();
        } else if (!e.shiftKey && document.activeElement === lastElement) {
            firstElement.focus();
            e.preventDefault();
        }
    }

    // Responsive Helpers
    onResize() {
        const wasMobile = this.isMobile;
        this.isMobile = window.innerWidth <= 768;
        
        if (wasMobile !== this.isMobile) {
            // Mobile state changed
            if (this.isMobile) {
                this.setupMobileView();
            } else {
                document.body.classList.remove('mobile-view', 'mobile-menu-open');
            }
        }
        
        // Update charts on resize
        this.charts.forEach(chart => {
            chart.resize();
        });
    }

    // Cleanup
    destroy() {
        // Clean up charts
        this.charts.forEach(chart => {
            chart.destroy();
        });
        this.charts.clear();
        
        // Remove event listeners
        // (We would need to store references to remove them properly)
        
        // Remove notification container
        if (this.notificationContainer) {
            this.notificationContainer.remove();
        }
    }

    // Debug and Development
    getUIState() {
        return {
            isMobile: this.isMobile,
            activeCharts: Array.from(this.charts.keys()),
            notificationCount: this.notificationContainer?.children.length || 0
        };
    }
}

// Screen reader only class for accessibility
const style = document.createElement('style');
style.textContent = `
    .sr-only {
        position: absolute;
        width: 1px;
        height: 1px;
        padding: 0;
        margin: -1px;
        overflow: hidden;
        clip: rect(0, 0, 0, 0);
        white-space: nowrap;
        border: 0;
    }
    
    .skip-link {
        position: absolute;
        top: -40px;
        left: 6px;
        background: #000;
        color: #fff;
        padding: 8px;
        z-index: 10000;
        text-decoration: none;
    }
    
    .skip-link:focus {
        top: 6px;
    }
    
    .loading-overlay {
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.7);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 1000;
    }
    
    .loading-content {
        text-align: center;
        color: white;
    }
    
    .loading-spinner.large {
        width: 48px;
        height: 48px;
        border-width: 4px;
    }
    
    .empty-state {
        text-align: center;
        padding: var(--space-xl);
        color: var(--text-muted);
    }
    
    .empty-state span {
        font-size: 2rem;
        display: block;
        margin-bottom: var(--space-md);
    }
`;
document.head.appendChild(style);

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = UIController;
}