// Map Visualization Engine for TTC Delay Visualization
class MapVisualizer {
    constructor() {
        this.map = null;
        this.routeGeometries = {};
        this.routes = [];
        this.currentVisualization = null;
        this.activeLayers = new Map();
        this.legend = null;
        this.colorScales = new Map();
        this.highlightedRoute = null;
        
        // Visualization configurations (only delay and frequency)
        this.config = {
            routeDelay: {
                colors: ['#10b981', '#f59e0b', '#ef4444', '#7c3aed'],
                weight: 2,
                opacity: 0.8
            },
            frequency: {
                colors: ['#93c5fd', '#3b82f6', '#1d4ed8', '#7e22ce'],
                minWeight: 2,
                maxWeight: 3,
                opacity: 0.8
            }
        };
    }

    init(map, routeGeometries, routes) {
        console.log('🗺️ Initializing map visualizer...');
        
        this.map = map;
        this.routeGeometries = routeGeometries;
        this.routes = routes;
        
        // Initialize color scales
        this.initializeColorScales();
        
        console.log('✅ Map visualizer initialized');
    }

    initializeColorScales() {
        // --- Delay-based color scale (hardcoded thresholds) ---
        this.colorScales.set('delay', (avgDelay) => {
            if (avgDelay < 20) return '#10b981';   // green
            if (avgDelay < 45) return '#f59e0b';   // orange
            return '#ef4444';                        // purple
        });

        // --- Frequency-based color scale (unchanged) ---
        const maxFrequency = Math.max(...this.routes.map(r => r.Delay_Count));
        this.colorScales.set('frequency', this.createColorScale(
            [0, maxFrequency * 0.3, maxFrequency * 0.6, maxFrequency],
            this.config.frequency.colors
        ));

        console.log('🎨 Color scales initialized (delay: fixed thresholds, frequency: dynamic)');
    }

    createColorScale(breaks, colors) {
        return (value) => {
            if (value <= breaks[0]) return colors[0];
            if (value >= breaks[breaks.length - 1]) return colors[colors.length - 1];
            
            for (let i = 0; i < breaks.length - 1; i++) {
                if (value >= breaks[i] && value <= breaks[i + 1]) {
                    const ratio = (value - breaks[i]) / (breaks[i + 1] - breaks[i]);
                    return this.interpolateColor(colors[i], colors[i + 1], ratio);
                }
            }
            
            return colors[0];
        };
    }

    interpolateColor(color1, color2, ratio) {
        const hex = (color) => color.replace('#', '');
        const r1 = parseInt(hex(color1).substring(0, 2), 16);
        const g1 = parseInt(hex(color1).substring(2, 4), 16);
        const b1 = parseInt(hex(color1).substring(4, 6), 16);
        
        const r2 = parseInt(hex(color2).substring(0, 2), 16);
        const g2 = parseInt(hex(color2).substring(2, 4), 16);
        const b2 = parseInt(hex(color2).substring(4, 6), 16);
        
        const r = Math.round(r1 + (r2 - r1) * ratio);
        const g = Math.round(g1 + (g2 - g1) * ratio);
        const b = Math.round(b1 + (b2 - b1) * ratio);
        
        return `#${((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1)}`;
    }

    // Add to MapVisualizer class in mapVisualizer.js
    showMobileRouteDelays(routes) {
        console.log('📱 Showing mobile route delays...');
        
        if (!routes || routes.length === 0) {
            console.error('❌ No routes data available for mobile');
            return false;
        }

        if (!this.map) {
            console.error('❌ Map not initialized');
            return false;
        }

        // Clear any existing layers
        this.clearVisualization();

        // Use simplified visualization for mobile
        const colorScale = this.colorScales.get('delay');
        let routesAdded = 0;

        routes.forEach(route => {
            const routeId = route.Route.toString();
            const geometry = this.routeGeometries[routeId];
            
            if (!geometry || geometry.length === 0) {
                return;
            }

            const avgDelay = route.Avg_Delay_Min;
            const delayCount = route.Delay_Count;
            const routeName = route.route_long_name || `Route ${routeId}`;
            const color = colorScale(avgDelay);

            try {
                const popupContent = this.createRoutePopup(route, routeName, avgDelay, delayCount);
                
                const polyline = L.polyline(geometry, {
                    color: color,
                    weight: 3, // Thinner lines for mobile
                    opacity: 0.7,
                    className: 'mobile-route-line'
                })
                .bindPopup(popupContent)
                .addTo(this.map);

                this.activeLayers.set(routeId, polyline);
                routesAdded++;

            } catch (error) {
                console.warn(`⚠️ Error adding route ${routeId} to mobile map:`, error);
            }
        });

        console.log(`✅ Mobile route delays: ${routesAdded} routes displayed`);
        return routesAdded > 0;
    }


    showMobileDelayFrequency(routes) {
        console.log('📱 Showing mobile delay frequency...');
        
        if (!routes || routes.length === 0) {
            console.error('❌ No routes data available');
            return false;
        }

        if (!this.map) {
            console.error('❌ Map not initialized');
            return false;
        }

        // Clear any existing layers
        this.clearVisualization();

        // Use simplified visualization for mobile
        const colorScale = this.colorScales.get('frequency');
        let routesAdded = 0;

        routes.forEach(route => {
            const routeId = route.Route.toString();
            const geometry = this.routeGeometries[routeId];
            
            if (!geometry || geometry.length === 0) {
                return;
            }

            const delayCount = route.Delay_Count;
            const color = colorScale(delayCount);

            try {
                const polyline = L.polyline(geometry, {
                    color: color,
                    weight: 3, // Thinner lines for mobile
                    opacity: 0.7,
                    className: 'mobile-route-line'
                }).addTo(this.map);

                this.activeLayers.set(routeId, polyline);
                routesAdded++;

            } catch (error) {
                console.warn(`⚠️ Error adding frequency route ${routeId} to mobile map:`, error);
            }
        });

        console.log(`✅ Mobile delay frequency: ${routesAdded} routes displayed`);
        return routesAdded > 0;
    }

    async showRouteDelays(filteredRoutes) {
        console.log('🔄 Showing route delays visualization...');
        
        this.clearVisualization();
        this.currentVisualization = 'delay';
        
        // Ensure we have data to show
        const routesToShow = filteredRoutes && filteredRoutes.length > 0 ? filteredRoutes : this.routes;
        
        if (!routesToShow || routesToShow.length === 0) {
            console.error('❌ No routes data available for visualization');
            this.showError('No route data available. Please check data files.');
            return 0;
        }

        if (Object.keys(this.routeGeometries).length === 0) {
            console.error('❌ No route geometries available');
            this.showError('No route geometry data available.');
            return 0;
        }

        console.log(`🗺️ Rendering ${routesToShow.length} routes with ${Object.keys(this.routeGeometries).length} geometries`);
        
        const colorScale = this.colorScales.get('delay');
        let routesAdded = 0;
        
        try {
            routesToShow.forEach(route => {
                const routeId = route.Route.toString();
                const geometry = this.routeGeometries[routeId];
                
                if (!geometry || geometry.length === 0) {
                    console.warn(`⚠️ No geometry for route ${routeId}`);
                    return;
                }
                
                const avgDelay = route.Avg_Delay_Min;
                const delayCount = route.Delay_Count;
                const routeName = route.route_long_name || `Route ${routeId}`;
                
                const color = colorScale(avgDelay);
                
                const popupContent = this.createRoutePopup(route, routeName, avgDelay, delayCount);
                
                try {
                    const polyline = L.polyline(geometry, {
                        color: color,
                        weight: this.config.routeDelay.weight,
                        opacity: this.config.routeDelay.opacity,
                        className: 'route-line'
                    })
                    .bindPopup(popupContent)
                    .on('click', (e) => this.onRouteClick(routeId, e))
                    .addTo(this.map);
                    
                    this.activeLayers.set(routeId, polyline);
                    routesAdded++;
                    
                } catch (error) {
                    console.warn(`⚠️ Error adding route ${routeId} to map:`, error);
                }
            });

            // Create legend
            this.createDelayLegend(colorScale);
            
            console.log(`✅ Route delays visualization: ${routesAdded} routes displayed`);
            
        } catch (error) {
            console.error('❌ Error in route delays visualization:', error);
            this.showError('Failed to render route visualization.');
        }
        
        return routesAdded;
    }

    async showDelayFrequency(filteredRoutes) {
        console.log('📈 Showing delay frequency visualization...');
        
        this.clearVisualization();
        this.currentVisualization = 'frequency';
        
        const colorScale = this.colorScales.get('frequency');
        const routesToShow = filteredRoutes || this.routes;
        
        const maxFrequency = Math.max(...routesToShow.map(r => r.Delay_Count));
        const minFrequency = Math.min(...routesToShow.map(r => r.Delay_Count));
        
        let routesAdded = 0;
        
        routesToShow.forEach(route => {
            const routeId = route.Route.toString();
            const geometry = this.routeGeometries[routeId];
            
            if (!geometry || geometry.length === 0) return;
            
            const delayCount = route.Delay_Count;
            const avgDelay = route.Avg_Delay_Min;
            const routeName = route.route_long_name || `Route ${routeId}`;
            
            const color = colorScale(delayCount);
            
            // Calculate line weight based on frequency
            const weightRange = this.config.frequency.maxWeight - this.config.frequency.minWeight;
            const frequencyRatio = (delayCount - minFrequency) / (maxFrequency - minFrequency);
            const weight = this.config.frequency.minWeight + (frequencyRatio * weightRange);
            
            const popupContent = this.createFrequencyPopup(route, routeName, delayCount, avgDelay);
            
            try {
                const polyline = L.polyline(geometry, {
                    color: color,
                    weight: weight,
                    opacity: 0.7,
                    className: 'route-line'
                })
                .bindPopup(popupContent)
                .on('click', (e) => this.onRouteClick(routeId, e))
                .addTo(this.map);
                
                this.activeLayers.set(routeId, polyline);
                routesAdded++;
                
            } catch (error) {
                console.warn(`⚠️ Error adding frequency route ${routeId}:`, error);
            }
        });
        
        // Create frequency legend
        this.createFrequencyLegend(colorScale, maxFrequency);
        
        console.log(`✅ Delay frequency visualization: ${routesAdded} routes displayed`);
        return routesAdded;
    }

    // Route Popup Creation Methods
    createRoutePopup(route, routeName, avgDelay, delayCount) {
        const delayLevel = this.getDelayLevel(avgDelay);
        const delayClass = this.getDelayClass(avgDelay);
        
        return `
            <div class="route-popup">
                <div class="popup-header">
                    <h3>Route ${route.Route}: ${routeName}</h3>
                    <span class="delay-indicator ${delayClass}">${delayLevel}</span>
                </div>
                <div class="popup-content">
                    <div class="popup-metric">
                        <span class="metric-label">Average Delay:</span>
                        <span class="metric-value">${avgDelay.toFixed(1)} minutes</span>
                    </div>
                    <div class="popup-metric">
                        <span class="metric-label">Total Delays:</span>
                        <span class="metric-value">${delayCount.toLocaleString()}</span>
                    </div>
                    ${route.Delay_Frequency ? `
                    <div class="popup-metric">
                        <span class="metric-label">Delay Frequency:</span>
                        <span class="metric-value">${route.Delay_Frequency.toFixed(1)} per day</span>
                    </div>
                    ` : ''}
                </div>
                <div class="popup-actions">
                    <button class="popup-btn" onclick="window.ttcApp.selectRoute('${route.Route}')">
                        <i class="fas fa-search-location"></i> Focus on Route
                    </button>
                </div>
            </div>
        `;
    }

    createFrequencyPopup(route, routeName, delayCount, avgDelay) {
        return `
            <div class="route-popup">
                <div class="popup-header">
                    <h3>Route ${route.Route}: ${routeName}</h3>
                    <span class="delay-indicator medium">Frequent Delays</span>
                </div>
                <div class="popup-content">
                    <div class="popup-metric">
                        <span class="metric-label">Total Delays:</span>
                        <span class="metric-value">${delayCount.toLocaleString()}</span>
                    </div>
                    <div class="popup-metric">
                        <span class="metric-label">Average Delay:</span>
                        <span class="metric-value">${avgDelay.toFixed(1)} minutes</span>
                    </div>
                    <div class="popup-metric">
                        <span class="metric-label">Delay Frequency Rank:</span>
                        <span class="metric-value">#${this.getFrequencyRank(route.Route.toString())}</span>
                    </div>
                </div>
            </div>
        `;
    }

    getDelayLevel(delay) {
        if (delay < 5) return 'Low';
        if (delay < 10) return 'Moderate';
        if (delay < 15) return 'High';
        return 'Critical';
    }

    getDelayClass(delay) {
        if (delay < 5) return 'low';
        if (delay < 10) return 'medium';
        if (delay < 15) return 'high';
        return 'critical';
    }

    getFrequencyRank(routeId) {
        const sortedRoutes = [...this.routes].sort((a, b) => b.Delay_Count - a.Delay_Count);
        return sortedRoutes.findIndex(route => route.Route.toString() === routeId) + 1;
    }

    onRouteClick(routeId, event) {
        console.log(`📍 Route ${routeId} clicked`);
        
        // Highlight the clicked route
        this.highlightRoute(routeId);
        
        // Notify the main app
        if (window.ttcApp) {
            window.ttcApp.selectRoute(routeId);
        }
        
        // Open popup
        event.target.openPopup();
    }

    highlightRoute(routeId) {
        // Clear previous highlight
        this.clearHighlight();
        
        const layer = this.findRouteLayer(routeId);
        if (layer) {
            // Store original style
            const originalStyle = {
                color: layer.options.color,
                weight: layer.options.weight,
                opacity: layer.options.opacity
            };
            
            // Apply highlight style
            layer.setStyle({
                color: '#fbbf24',
                weight: originalStyle.weight + 2,
                opacity: 1
            });
            
            // Bring to front
            layer.bringToFront();
            
            this.highlightedRoute = {
                layer: layer,
                originalStyle: originalStyle,
                routeId: routeId
            };
            
            // Zoom to route bounds with smooth animation
            const bounds = layer.getBounds();
            if (bounds.isValid()) {
                this.map.flyToBounds(bounds, { 
                    padding: [20, 20],
                    animate: true,
                    duration: 1
                });
            }
        }
    }

    findRouteLayer(routeId) {
        for (const [key, layer] of this.activeLayers) {
            if (key === routeId) {
                return layer;
            }
        }
        return null;
    }

    clearHighlight() {
        if (this.highlightedRoute) {
            const { layer, originalStyle } = this.highlightedRoute;
            layer.setStyle(originalStyle);
            this.highlightedRoute = null;
        }
    }

    clearVisualization() {
        console.log('🗑️ Clearing current visualization...');
        
        // Remove all active layers (routes)
        this.activeLayers.forEach((layer, key) => {
            this.map.removeLayer(layer);
        });
        this.activeLayers.clear();
        
        // Remove legend
        if (this.legend) {
            this.map.removeControl(this.legend);
            this.legend = null;
        }
        
        // Clear highlight
        this.clearHighlight();
        
        this.currentVisualization = null;
    }

    // Legend creation methods - UPDATED with toggle button
    createDelayLegend(colorScale) {
        const maxDelay = Math.max(...this.routes.map(r => r.Avg_Delay_Min));
        const breaks = [0, maxDelay * 0.3, maxDelay * 0.6, maxDelay];
        
        // Remove existing legend if it exists
        if (this.legend) {
            this.map.removeControl(this.legend);
        }
        
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-toggle-container');
            div.innerHTML = `
                <button class="legend-toggle-btn" title="Show/Hide Legend">
                    <i class="fas fa-info"></i>
                </button>
                <div class="legend-container" style="display: none;">
                    <div class="legend-title">
                        <span><i class="fas fa-clock"></i> Average Delay (minutes)</span>
                    </div>
                    <div class="legend-scale">
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[0])}"></div>
                            <span class="legend-label">0 - ${breaks[1].toFixed(1)}</span>
                        </div>
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[1])}"></div>
                            <span class="legend-label">${breaks[1].toFixed(1)} - ${breaks[2].toFixed(1)}</span>
                        </div>
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[2])}"></div>
                            <span class="legend-label">${breaks[2].toFixed(1)} - ${breaks[3].toFixed(1)}</span>
                        </div>
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[3])}"></div>
                            <span class="legend-label">${breaks[3].toFixed(1)}+</span>
                        </div>
                    </div>
                </div>
            `;
            
            // Add click event to toggle button
            const toggleBtn = div.querySelector('.legend-toggle-btn');
            const legendContainer = div.querySelector('.legend-container');
            
            toggleBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                const isVisible = legendContainer.style.display === 'block';
                legendContainer.style.display = isVisible ? 'none' : 'block';
                toggleBtn.classList.toggle('active', !isVisible);
                toggleBtn.innerHTML = isVisible ? '<i class="fas fa-info"></i>' : '<i class="fas fa-times"></i>';
            });
            
            // Close legend when clicking outside
            document.addEventListener('click', (e) => {
                if (!div.contains(e.target)) {
                    legendContainer.style.display = 'none';
                    toggleBtn.classList.remove('active');
                    toggleBtn.innerHTML = '<i class="fas fa-info"></i>';
                }
            });
            
            return div;
        };
        
        this.legend = legend;
        legend.addTo(this.map);
    }

    createFrequencyLegend(colorScale, maxFrequency) {
        const breaks = [0, maxFrequency * 0.3, maxFrequency * 0.6, maxFrequency];
        
        // Remove existing legend if it exists
        if (this.legend) {
            this.map.removeControl(this.legend);
        }
        
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-toggle-container');
            div.innerHTML = `
                <button class="legend-toggle-btn" title="Show/Hide Legend">
                    <i class="fas fa-info"></i>
                </button>
                <div class="legend-container" style="display: none;">
                    <div class="legend-title">
                        <span><i class="fas fa-chart-line"></i> Delay Frequency</span>
                    </div>
                    <div class="legend-scale">
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[0])}"></div>
                            <span class="legend-label">0 - ${Math.round(breaks[1])}</span>
                        </div>
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[1])}"></div>
                            <span class="legend-label">${Math.round(breaks[1])} - ${Math.round(breaks[2])}</span>
                        </div>
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[2])}"></div>
                            <span class="legend-label">${Math.round(breaks[2])} - ${Math.round(breaks[3])}</span>
                        </div>
                        <div class="legend-item">
                            <div class="legend-color" style="background: ${colorScale(breaks[3])}"></div>
                            <span class="legend-label">${Math.round(breaks[3])}+</span>
                        </div>
                    </div>
                </div>
            `;
            
            // Add click event to toggle button
            const toggleBtn = div.querySelector('.legend-toggle-btn');
            const legendContainer = div.querySelector('.legend-container');
            
            toggleBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                const isVisible = legendContainer.style.display === 'block';
                legendContainer.style.display = isVisible ? 'none' : 'block';
                toggleBtn.classList.toggle('active', !isVisible);
                toggleBtn.innerHTML = isVisible ? '<i class="fas fa-info"></i>' : '<i class="fas fa-times"></i>';
            });
            
            // Close legend when clicking outside
            document.addEventListener('click', (e) => {
                if (!div.contains(e.target)) {
                    legendContainer.style.display = 'none';
                    toggleBtn.classList.remove('active');
                    toggleBtn.innerHTML = '<i class="fas fa-info"></i>';
                }
            });
            
            return div;
        };
        
        this.legend = legend;
        legend.addTo(this.map);
    }

    

    getCurrentLegend() {
        return this.legend ? this.legend.getContainer().innerHTML : null;
    }

    // REMOVED: onThemeChange method - only dark theme
    
    // Utility methods
    isValidCoordinate(lat, lng) {
        return lat !== null && 
               lng !== null && 
               !isNaN(lat) && 
               !isNaN(lng) &&
               lat >= -90 && lat <= 90 &&
               lng >= -180 && lng <= 180;
    }

    showError(message) {
        // Create a temporary error message on the map
        const errorDiv = L.DomUtil.create('div', 'map-error');
        errorDiv.innerHTML = `
            <div class="error-message">
                <span class="error-icon">⚠️</span>
                <span class="error-text">${message}</span>
            </div>
        `;
        
        // Add to map
        const errorControl = L.control({ position: 'topleft' });
        errorControl.onAdd = () => errorDiv;
        errorControl.addTo(this.map);
        
        // Remove after 5 seconds
        setTimeout(() => {
            if (errorControl && this.map) {
                this.map.removeControl(errorControl);
            }
        }, 5000);
    }

    // Public method to get visualization stats
    getVisualizationStats() {
        return {
            currentVisualization: this.currentVisualization,
            activeLayers: this.activeLayers.size,
            highlightedRoute: this.highlightedRoute ? this.highlightedRoute.routeId : null,
            totalRoutes: this.routes.length,
            routesWithGeometry: Object.keys(this.routeGeometries).length
        };
    }

    // Export current visualization data
    exportVisualizationData() {
        const baseData = {
            type: this.currentVisualization,
            routes: this.routes.map(route => ({
                id: route.Route,
                name: route.route_long_name,
                avgDelay: route.Avg_Delay_Min,
                delayCount: route.Delay_Count,
                hasGeometry: !!this.routeGeometries[route.Route.toString()]
            })),
            bounds: this.map.getBounds().toBBoxString(),
            zoom: this.map.getZoom()
        };
        
        return baseData;
    }

    // New method: Fit bounds to all routes
    fitToRoutes() {
        if (this.activeLayers.size === 0) return false;
        
        try {
            let bounds = null;
            
            // Collect bounds from all active layers
            this.activeLayers.forEach((layer, key) => {
                if (layer.getBounds) {
                    const layerBounds = layer.getBounds();
                    if (layerBounds.isValid()) {
                        bounds = bounds ? bounds.extend(layerBounds) : layerBounds;
                    }
                }
            });
            
            if (bounds && bounds.isValid()) {
                this.map.fitBounds(bounds, { 
                    padding: [50, 50],
                    maxZoom: 14,
                    animate: true,
                    duration: 1
                });
                return true;
            }
        } catch (error) {
            console.error('Error fitting to routes:', error);
        }
        
        return false;
    }

    // New method: Get route geometry by ID
    getRouteGeometry(routeId) {
        return this.routeGeometries[routeId] || null;
    }

    // New method: Show specific route
    showSingleRoute(routeId) {
        const route = this.routes.find(r => r.Route.toString() === routeId);
        if (!route) return false;
        
        const geometry = this.getRouteGeometry(routeId);
        if (!geometry) return false;
        
        // Clear current visualization
        this.clearVisualization();
        
        // Show only this route
        if (this.currentVisualization === 'delay') {
            const colorScale = this.colorScales.get('delay');
            const color = colorScale(route.Avg_Delay_Min);
            
            const polyline = L.polyline(geometry, {
                color: color,
                weight: 6,
                opacity: 0.9
            })
            .bindPopup(this.createRoutePopup(route, route.route_long_name, route.Avg_Delay_Min, route.Delay_Count))
            .addTo(this.map);
            
            this.activeLayers.set(routeId, polyline);
            
            // Fit to this route
            const bounds = polyline.getBounds();
            if (bounds.isValid()) {
                this.map.fitBounds(bounds, { padding: [50, 50], animate: true });
            }
            
            return true;
        } else if (this.currentVisualization === 'frequency') {
            const colorScale = this.colorScales.get('frequency');
            const color = colorScale(route.Delay_Count);
            
            const polyline = L.polyline(geometry, {
                color: color,
                weight: 6,
                opacity: 0.9
            })
            .bindPopup(this.createFrequencyPopup(route, route.route_long_name, route.Delay_Count, route.Avg_Delay_Min))
            .addTo(this.map);
            
            this.activeLayers.set(routeId, polyline);
            
            // Fit to this route
            const bounds = polyline.getBounds();
            if (bounds.isValid()) {
                this.map.fitBounds(bounds, { padding: [50, 50], animate: true });
            }
            
            return true;
        }
        
        return false;
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = MapVisualizer;
}