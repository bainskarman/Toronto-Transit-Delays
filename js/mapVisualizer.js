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
        
        // Visualization configurations
        this.config = {
            routeDelay: {
                colors: ['#10b981', '#f59e0b', '#ef4444', '#7c3aed'],
                weight: 4,
                opacity: 0.7
            },
            heatmap: {
                radius: 15,
                blur: 10,
                gradient: {
                    0.4: '#10b981',
                    0.6: '#f59e0b',
                    0.8: '#ef4444',
                    1.0: '#7c3aed'
                }
            },
            comparison: {
                highDelay: {
                    color: '#ef4444',
                    weight: 6,
                    opacity: 0.8
                },
                lowDelay: {
                    color: '#10b981',
                    weight: 4,
                    opacity: 0.8
                }
            },
            frequency: {
                colors: ['#93c5fd', '#3b82f6', '#1d4ed8', '#7e22ce'],
                minWeight: 3,
                maxWeight: 8
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
        // Delay-based color scale
        const maxDelay = Math.max(...this.routes.map(r => r.Avg_Delay_Min));
        this.colorScales.set('delay', this.createColorScale(
            [0, maxDelay * 0.3, maxDelay * 0.6, maxDelay],
            this.config.routeDelay.colors
        ));

        // Frequency-based color scale
        const maxFrequency = Math.max(...this.routes.map(r => r.Delay_Count));
        this.colorScales.set('frequency', this.createColorScale(
            [0, maxFrequency * 0.3, maxFrequency * 0.6, maxFrequency],
            this.config.frequency.colors
        ));

        console.log('🎨 Color scales initialized');
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
    
    async showRouteComparison(filteredRoutes) {
        console.log('📊 Showing route comparison visualization...');
        
        this.clearVisualization();
        this.currentVisualization = 'comparison';
        
        const routesToShow = filteredRoutes || this.routes;
        
        // Get top 10 most delayed routes
        const topDelayed = [...routesToShow]
            .sort((a, b) => b.Avg_Delay_Min - a.Avg_Delay_Min)
            .slice(0, 10);
        
        // Get top 10 least delayed routes (with reasonable delay count)
        const leastDelayed = [...routesToShow]
            .filter(route => route.Delay_Count > 10)
            .sort((a, b) => a.Avg_Delay_Min - b.Avg_Delay_Min)
            .slice(0, 10);
        
        let routesAdded = 0;
        
        // Add most delayed routes (red)
        topDelayed.forEach(route => {
            this.addComparisonRoute(route, 'highDelay');
            routesAdded++;
        });
        
        // Add least delayed routes (green)
        leastDelayed.forEach(route => {
            this.addComparisonRoute(route, 'lowDelay');
            routesAdded++;
        });
        
        // Create comparison legend
        this.createComparisonLegend();
        
        console.log(`✅ Route comparison visualization: ${routesAdded} routes displayed`);
        return routesAdded;
    }

    addComparisonRoute(route, type) {
        const routeId = route.Route.toString();
        const geometry = this.routeGeometries[routeId];
        
        if (!geometry || geometry.length === 0) return;
        
        const config = this.config.comparison[type];
        const routeName = route.route_long_name || `Route ${routeId}`;
        
        const popupContent = this.createComparisonPopup(route, routeName, type);
        
        try {
            const polyline = L.polyline(geometry, {
                color: config.color,
                weight: config.weight,
                opacity: config.opacity,
                className: type === 'highDelay' ? 'route-line-delayed' : 'route-line'
            })
            .bindPopup(popupContent)
            .bindTooltip(
                `${type === 'highDelay' ? '🚨 High' : '✅ Low'} Delay: Route ${routeId} - ${route.Avg_Delay_Min.toFixed(1)} min`,
                { permanent: false, direction: 'auto' }
            )
            .on('click', (e) => this.onRouteClick(routeId, e))
            .addTo(this.map);
            
            this.activeLayers.set(`${type}_${routeId}`, polyline);
            
        } catch (error) {
            console.warn(`⚠️ Error adding ${type} route ${routeId}:`, error);
        }
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
                .bindTooltip(`Route ${routeId}: ${delayCount} delays`, {
                    permanent: false,
                    direction: 'auto'
                })
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
                    <div class="popup-metric">
                        <span class="metric-label">On-Time Performance:</span>
                        <span class="metric-value">${route.On_Time_Percentage ? route.On_Time_Percentage.toFixed(1) + '%' : 'N/A'}</span>
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
                        📍 Focus on Route
                    </button>
                </div>
            </div>
        `;
    }

    createComparisonPopup(route, routeName, type) {
        const isHighDelay = type === 'highDelay';
        
        return `
            <div class="route-popup">
                <div class="popup-header">
                    <h3>${isHighDelay ? '🚨 High Delay' : '✅ Low Delay'}: Route ${route.Route}</h3>
                    <span class="delay-indicator ${isHighDelay ? 'critical' : 'low'}">
                        ${isHighDelay ? 'Critical' : 'Low'}
                    </span>
                </div>
                <div class="popup-content">
                    <div class="popup-metric">
                        <span class="metric-label">Route Name:</span>
                        <span class="metric-value">${routeName}</span>
                    </div>
                    <div class="popup-metric">
                        <span class="metric-label">Average Delay:</span>
                        <span class="metric-value">${route.Avg_Delay_Min.toFixed(1)} minutes</span>
                    </div>
                    <div class="popup-metric">
                        <span class="metric-label">Total Delays:</span>
                        <span class="metric-value">${route.Delay_Count.toLocaleString()}</span>
                    </div>
                    <div class="popup-metric">
                        <span class="metric-label">Rank:</span>
                        <span class="metric-value">${isHighDelay ? 'Top 10 Most Delayed' : 'Top 10 Least Delayed'}</span>
                    </div>
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
            
            // Zoom to route bounds
            const bounds = layer.getBounds();
            if (bounds.isValid()) {
                this.map.fitBounds(bounds, { padding: [20, 20] });
            }
        }
    }

    findRouteLayer(routeId) {
        for (const [key, layer] of this.activeLayers) {
            if (key === routeId || key.endsWith(`_${routeId}`)) {
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
        
        // Remove all active layers
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

    // Legend creation methods
    createDelayLegend(colorScale) {
        const maxDelay = Math.max(...this.routes.map(r => r.Avg_Delay_Min));
        const breaks = [0, maxDelay * 0.3, maxDelay * 0.6, maxDelay];
        
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-container');
            div.innerHTML = `
                <div class="legend-title">
                    <span>🚍 Average Delay (minutes)</span>
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
            `;
            return div;
        };
        
        this.legend = legend;
        legend.addTo(this.map);
    }

    createHeatmapLegend() {
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-container');
            div.innerHTML = `
                <div class="legend-title">
                    <span>🔥 Delay Hotspots</span>
                </div>
                <div class="legend-scale">
                    <div class="legend-item">
                        <div class="legend-color" style="background: #10b981"></div>
                        <span class="legend-label">Low Frequency</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #f59e0b"></div>
                        <span class="legend-label">Medium</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #ef4444"></div>
                        <span class="legend-label">High</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: #7c3aed"></div>
                        <span class="legend-label">Very High</span>
                    </div>
                </div>
            `;
            return div;
        };
        
        this.legend = legend;
        legend.addTo(this.map);
    }

    createComparisonLegend() {
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-container');
            div.innerHTML = `
                <div class="legend-title">
                    <span>📊 Route Comparison</span>
                </div>
                <div class="legend-scale">
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${this.config.comparison.highDelay.color}; height: 6px"></div>
                        <span class="legend-label">Top 10 Most Delayed</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${this.config.comparison.lowDelay.color}; height: 4px"></div>
                        <span class="legend-label">Top 10 Least Delayed</span>
                    </div>
                </div>
            `;
            return div;
        };
        
        this.legend = legend;
        legend.addTo(this.map);
    }

    createFrequencyLegend(colorScale, maxFrequency) {
        const breaks = [0, maxFrequency * 0.3, maxFrequency * 0.6, maxFrequency];
        
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-container');
            div.innerHTML = `
                <div class="legend-title">
                    <span>📈 Delay Frequency</span>
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
            `;
            return div;
        };
        
        this.legend = legend;
        legend.addTo(this.map);
    }

    getCurrentLegend() {
        return this.legend ? this.legend.getContainer().innerHTML : null;
    }

    
// Replace the entire onThemeChange method with this:
onThemeChange(theme) {
    console.log(`🎨 Updating map for ${theme} theme...`);
    
    try {
        // Store current view to restore after tile layer change
        const currentCenter = this.map.getCenter();
        const currentZoom = this.map.getZoom();
        
        // Remove ONLY tile layers, preserve other layers (routes, markers, etc.)
        this.map.eachLayer((layer) => {
            if (layer instanceof L.TileLayer) {
                this.map.removeLayer(layer);
            }
        });

        // Use more reliable tile providers with proper error handling
        const tileProviders = {
            dark: [
                'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
                'https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png',
                'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png' // fallback
            ],
            light: [
                'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
                'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
                'https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png' // fallback
            ]
        };

        const primaryUrl = theme === 'dark' ? tileProviders.dark[0] : tileProviders.light[0];
        const fallbackUrls = theme === 'dark' ? tileProviders.dark.slice(1) : tileProviders.light.slice(1);

        // Create primary tile layer with enhanced configuration
        const primaryLayer = L.tileLayer(primaryUrl, {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
            subdomains: 'abcd',
            maxZoom: 20,
            minZoom: 1,
            noWrap: true,
            updateWhenIdle: true,
            reuseTiles: false,
            crossOrigin: true,
            detectRetina: true
        });

        // Add primary layer to map
        primaryLayer.addTo(this.map);
        
        // Set up fallback mechanism
        let currentFallbackIndex = 0;
        
        primaryLayer.on('tileerror', (e) => {
            console.warn('⚠️ Primary tile failed, trying fallback...');
            
            if (currentFallbackIndex < fallbackUrls.length) {
                // Remove failed layer
                this.map.removeLayer(primaryLayer);
                
                // Add fallback layer
                const fallbackLayer = L.tileLayer(fallbackUrls[currentFallbackIndex], {
                    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                    subdomains: 'abcd',
                    maxZoom: 20,
                    minZoom: 1,
                    noWrap: true,
                    updateWhenIdle: true,
                    reuseTiles: false,
                    crossOrigin: true
                });
                
                fallbackLayer.addTo(this.map);
                currentFallbackIndex++;
                
                console.log(`🔄 Switched to fallback tile provider ${currentFallbackIndex}`);
            }
        });

        // Force complete map refresh
        setTimeout(() => {
            this.map.setView(currentCenter, currentZoom, { animate: false });
            this.map.invalidateSize({ pan: false });
            
            // Double refresh to ensure tiles load
            setTimeout(() => {
                this.map.invalidateSize({ pan: false });
                primaryLayer.redraw();
            }, 500);
            
        }, 200);

        console.log(`✅ Map theme updated to ${theme}`);

    } catch (error) {
        console.error('❌ Error updating map theme:', error);
        // Emergency fallback to OSM
        const emergencyLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap',
            maxZoom: 19
        }).addTo(this.map);
    }
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
        return {
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
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = MapVisualizer;
}