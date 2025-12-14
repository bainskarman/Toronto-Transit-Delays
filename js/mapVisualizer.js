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
        
        // NEW: Live tracking properties
        this.busMarkers = new Map(); // Store bus markers by vehicle_id
        this.selectedBus = null;
        this.busIconCache = new Map();
        this.routeLines = new Map(); // Store route lines for live mode
        
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
            },
            // NEW: Live tracking configuration
            liveTracking: {
                iconSize: [32, 32],
                iconAnchor: [16, 16],
                popupAnchor: [0, -16],
                colors: {
                    onTime: '#10b981',    // Green
                    minorDelay: '#f59e0b', // Yellow
                    majorDelay: '#ef4444', // Red
                    selected: '#3b82f6'    // Blue for selected bus
                },
                routeLine: {
                    color: '#94a3b8',
                    weight: 2,
                    opacity: 0.5,
                    dashArray: '5, 10'
                }
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
        
        console.log(`🔍 Total routes available for comparison: ${routesToShow.length}`);
        
        // Get top 10 most delayed routes
        const topDelayed = [...routesToShow]
            .sort((a, b) => b.Avg_Delay_Min - a.Avg_Delay_Min)
            .slice(0, 10);
        
        // Get top 10 least delayed routes with less restrictive filtering
        const leastDelayed = [...routesToShow]
            .filter(route => route.Delay_Count >= 1)
            .sort((a, b) => a.Avg_Delay_Min - b.Avg_Delay_Min)
            .slice(0, 10);
        
        console.log('🚨 Most Delayed Routes:', topDelayed.map(r => `${r.Route}: ${r.Avg_Delay_Min}min`));
        console.log('✅ Least Delayed Routes:', leastDelayed.map(r => `${r.Route}: ${r.Avg_Delay_Min}min`));
        
        let routesAdded = 0;
        
        // Add most delayed routes (red)
        console.log('🔴 Adding most delayed routes...');
        topDelayed.forEach(route => {
            this.addComparisonRoute(route, 'highDelay');
            routesAdded++;
        });
        
        // Add least delayed routes (green)
        console.log('🟢 Adding least delayed routes...');
        leastDelayed.forEach(route => {
            this.addComparisonRoute(route, 'lowDelay');
            routesAdded++;
        });
        
        // Create comparison legend
        this.createComparisonLegend();
        
        console.log(`✅ Route comparison visualization: ${routesAdded} routes displayed (${topDelayed.length} most delayed + ${leastDelayed.length} least delayed)`);
        return routesAdded;
    }

    addComparisonRoute(route, type) {
        const routeId = route.Route.toString();
        const geometry = this.routeGeometries[routeId];
        
        if (!geometry || geometry.length === 0) {
            console.warn(`⚠️ No geometry for ${type} route ${routeId}: ${route.route_long_name}`);
            return;
        }

        const config = this.config.comparison[type];
        const routeName = route.route_long_name || `Route ${routeId}`;
        
        console.log(`📍 Adding ${type} route: ${routeId} - ${routeName} (${route.Avg_Delay_Min}min avg)`);
        
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

    // NEW: Live Tracking Methods
    async showLiveBuses(buses) {
        console.log(`🚍 Showing ${buses.length} live buses...`);
        
        this.clearVisualization();
        this.currentVisualization = 'live';
        
        if (!buses || buses.length === 0) {
            console.warn('⚠️ No buses to display');
            this.showError('No live bus data available');
            return 0;
        }

        // Get the route from first bus
        const routeId = buses[0].route_id;
        console.log(`📍 Showing buses for route ${routeId}`);
        
        // Show route geometry if available
        this.showRouteForLiveTracking(routeId);
        
        // Center map on first bus if available
        const firstBus = buses.find(b => b.latitude && b.longitude);
        if (firstBus) {
            this.map.setView([firstBus.latitude, firstBus.longitude], 14);
        } else {
            // Center on Toronto
            this.map.setView([43.6532, -79.3832], 13);
        }

        let busesAdded = 0;
        
        buses.forEach(bus => {
            try {
                this.addBusMarker(bus);
                busesAdded++;
            } catch (error) {
                console.warn(`⚠️ Error adding bus ${bus.vehicle_id}:`, error);
            }
        });

        // Create live tracking legend
        this.createLiveLegend();
        
        console.log(`✅ Live tracking: ${busesAdded} buses displayed`);
        return busesAdded;
    }

    showRouteForLiveTracking(routeId) {
        const geometry = this.routeGeometries[routeId];
        
        if (!geometry || geometry.length === 0) {
            console.warn(`⚠️ No geometry for route ${routeId}`);
            return;
        }
        
        // Add the route line for context
        const routeLine = L.polyline(geometry, {
            color: this.config.liveTracking.routeLine.color,
            weight: this.config.liveTracking.routeLine.weight,
            opacity: this.config.liveTracking.routeLine.opacity,
            dashArray: this.config.liveTracking.routeLine.dashArray,
            className: 'route-line-dashed'
        }).addTo(this.map);
        
        this.routeLines.set(routeId, routeLine);
    }

    addBusMarker(bus) {
        const busId = bus.vehicle_id;
        const position = [bus.latitude, bus.longitude];
        
        // Validate coordinates
        if (!this.isValidCoordinate(position[0], position[1])) {
            console.warn(`⚠️ Invalid coordinates for bus ${busId}: ${position}`);
            return null;
        }
        
        // Get bus status and corresponding color
        const status = this.getBusStatus(bus.delay_minutes);
        const color = this.config.liveTracking.colors[status];
        
        // Create custom bus icon
        const icon = this.createBusIcon(busId, color, status, bus.bearing);
        
        // Create popup content
        const popupContent = this.createBusPopup(bus);
        
        // Create marker with rotation
        const marker = L.marker(position, {
            icon: icon,
            rotationAngle: bus.bearing || 0,
            rotationOrigin: 'center',
            zIndexOffset: 1000,
            title: `Bus ${bus.vehicle_label} (${bus.route_id})`,
            alt: `Bus ${bus.vehicle_label} on route ${bus.route_id}`
        })
        .bindPopup(popupContent)
        .bindTooltip(
            `Bus ${bus.vehicle_label}<br>${bus.delay_minutes ? bus.delay_minutes.toFixed(1) + ' min delay' : 'No delay data'}`,
            {
                permanent: false,
                direction: 'top',
                opacity: 0.9,
                className: 'bus-tooltip'
            }
        );
        
        // Add click handler
        marker.on('click', (e) => {
            this.onBusClick(busId, e);
        });
        
        // Add hover effects
        marker.on('mouseover', () => {
            marker.setZIndexOffset(2000);
        });
        
        marker.on('mouseout', () => {
            if (!this.selectedBus || this.selectedBus.busId !== busId) {
                marker.setZIndexOffset(1000);
            }
        });
        
        // Add to map and store reference
        marker.addTo(this.map);
        this.busMarkers.set(busId, {
            marker: marker,
            bus: bus,
            status: status
        });
        
        return marker;
    }

    createBusIcon(busId, color, status, bearing) {
        const cacheKey = `${busId}_${color}_${status}`;
        
        // Check cache first
        if (this.busIconCache.has(cacheKey)) {
            return this.busIconCache.get(cacheKey);
        }
        
        // Create SVG icon
        const iconHtml = `
            <svg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg">
                <!-- Outer circle -->
                <circle cx="30" cy="30" r="28" fill="white" stroke="${color}" stroke-width="2"/>
                
                <!-- Bus icon -->
                <g transform="translate(15, 15) scale(0.3)">
                    <path d="M20 0H80C88.2843 0 95 6.71573 95 15V55C95 63.2843 88.2843 70 80 70H20C11.7157 70 5 63.2843 5 55V15C5 6.71573 11.7157 0 20 0Z" fill="${color}"/>
                    <rect x="20" y="10" width="60" height="15" rx="2" fill="white"/>
                    <circle cx="30" y="55" r="10" fill="#333"/>
                    <circle cx="70" y="55" r="10" fill="#333"/>
                    <circle cx="30" y="55" r="6" fill="white"/>
                    <circle cx="70" y="55" r="6" fill="white"/>
                </g>
                
                <!-- Status indicator -->
                <circle cx="45" cy="15" r="5" fill="${color}"/>
                
                <!-- Bus number background -->
                <rect x="10" y="40" width="40" height="15" rx="3" fill="white" opacity="0.8"/>
                <text x="30" y="52" text-anchor="middle" font-family="Arial, sans-serif" font-size="10" font-weight="bold" fill="${color}">${busId.slice(-3)}</text>
            </svg>
        `;
        
        // Create DivIcon with HTML content
        const icon = L.divIcon({
            html: iconHtml,
            className: `bus-icon bus-status-${status}`,
            iconSize: this.config.liveTracking.iconSize,
            iconAnchor: this.config.liveTracking.iconAnchor,
            popupAnchor: this.config.liveTracking.popupAnchor
        });
        
        // Cache the icon
        this.busIconCache.set(cacheKey, icon);
        
        return icon;
    }

    getBusStatus(delayMinutes) {
        if (delayMinutes < 2) return 'onTime';
        if (delayMinutes < 5) return 'minorDelay';
        return 'majorDelay';
    }

    createBusPopup(bus) {
        const status = this.getBusStatus(bus.delay_minutes);
        const statusText = this.getBusStatusText(status);
        const speedMph = bus.speed_mps ? (bus.speed_mps * 2.23694).toFixed(1) : 'N/A';
        
        // Format timestamp
        let timeString = 'Unknown';
        if (bus.timestamp) {
            const time = new Date(bus.timestamp);
            timeString = time.toLocaleTimeString([], { 
                hour: '2-digit', 
                minute: '2-digit',
                second: '2-digit'
            });
        }
        
        // Determine occupancy emoji
        let occupancyEmoji = '🟢';
        let occupancyText = 'Many seats available';
        if (bus.occupancy === 'CROWDED') {
            occupancyEmoji = '🟡';
            occupancyText = 'Crowded';
        } else if (bus.occupancy === 'FULL') {
            occupancyEmoji = '🔴';
            occupancyText = 'Full';
        } else if (bus.occupancy === 'MANY_SEATS_AVAILABLE') {
            occupancyEmoji = '🟢';
            occupancyText = 'Many seats';
        }
        
        return `
            <div class="bus-popup">
                <div class="popup-header">
                    <h3>${bus.vehicle_label}</h3>
                    <span class="bus-status ${status}">${statusText}</span>
                </div>
                <div class="popup-content">
                    <div class="popup-row">
                        <span class="label">Route:</span>
                        <span class="value">${bus.route_id}</span>
                    </div>
                    <div class="popup-row">
                        <span class="label">Delay:</span>
                        <span class="value ${status}">${bus.delay_minutes ? bus.delay_minutes.toFixed(1) + ' minutes' : 'N/A'}</span>
                    </div>
                    <div class="popup-row">
                        <span class="label">Speed:</span>
                        <span class="value">${bus.speed_mps ? bus.speed_mps.toFixed(1) + ' m/s' : 'N/A'} (${speedMph} mph)</span>
                    </div>
                    <div class="popup-row">
                        <span class="label">Bearing:</span>
                        <span class="value">${bus.bearing ? bus.bearing.toFixed(0) + '°' : 'N/A'}</span>
                    </div>
                    <div class="popup-row">
                        <span class="label">Occupancy:</span>
                        <span class="value">${occupancyEmoji} ${occupancyText}</span>
                    </div>
                    <div class="popup-row">
                        <span class="label">Last Update:</span>
                        <span class="value">${timeString}</span>
                    </div>
                </div>
                <div class="popup-actions">
                    <button class="popup-btn select-bus" onclick="window.ttcApp.selectRoute('${bus.vehicle_id}')">
                        📍 Track This Bus
                    </button>
                </div>
            </div>
        `;
    }

    getBusStatusText(status) {
        const statusMap = {
            onTime: 'On Time',
            minorDelay: 'Minor Delay',
            majorDelay: 'Major Delay'
        };
        return statusMap[status] || 'Unknown';
    }

    // Bus interaction handlers
    onBusClick(busId, event) {
        console.log(`🚍 Bus ${busId} clicked`);
        
        // Highlight the clicked bus
        this.highlightBus(busId);
        
        // Notify the main app
        if (window.ttcApp) {
            window.ttcApp.selectRoute(busId);
        }
        
        // Open popup
        event.target.openPopup();
    }

    highlightBus(busId) {
        // Clear previous highlight
        this.clearBusHighlight();
        
        const busData = this.busMarkers.get(busId);
        if (busData) {
            const { marker, bus } = busData;
            
            // Store original icon
            const originalIcon = marker.options.icon;
            
            // Create highlighted icon
            const highlightedIcon = this.createBusIcon(
                busId, 
                this.config.liveTracking.colors.selected,
                'selected',
                bus.bearing
            );
            
            // Apply highlight
            marker.setIcon(highlightedIcon);
            
            // Add pulse animation
            this.animateBusPulse(marker);
            
            // Bring to front
            marker.bringToFront();
            marker.setZIndexOffset(2000);
            
            // Store highlight data
            this.selectedBus = {
                marker: marker,
                busId: busId,
                originalIcon: originalIcon,
                bus: bus
            };
            
            // Zoom to bus position with padding
            const bounds = this.map.getBounds();
            const busPos = marker.getLatLng();
            
            // Only zoom if bus is near the edge or map is zoomed out
            if (!bounds.contains(busPos) || this.map.getZoom() < 14) {
                this.map.setView(busPos, 16, {
                    animate: true,
                    duration: 1
                });
            }
        }
    }

    animateBusPulse(marker) {
        const iconElement = marker.getElement();
        if (!iconElement) return;
        
        // Add pulse animation
        iconElement.style.animation = 'bus-pulse 1s ease-in-out 2';
        
        // Remove animation after it completes
        setTimeout(() => {
            if (iconElement) {
                iconElement.style.animation = '';
            }
        }, 2000);
    }

    clearBusHighlight() {
        if (this.selectedBus) {
            const { marker, originalIcon, busId } = this.selectedBus;
            
            // Restore original icon if marker exists
            if (marker && originalIcon) {
                marker.setIcon(originalIcon);
                marker.setZIndexOffset(1000);
            }
            
            // Remove selected class
            const busData = this.busMarkers.get(busId);
            if (busData && busData.marker) {
                busData.marker.getElement()?.classList.remove('selected');
            }
            
            this.selectedBus = null;
        }
    }

    // Focus on a specific bus
    focusOnBus(busId) {
        const busData = this.busMarkers.get(busId);
        if (busData) {
            const { marker } = busData;
            const position = marker.getLatLng();
            
            // Center map on bus
            this.map.setView(position, 16, {
                animate: true,
                duration: 1
            });
            
            // Highlight the bus
            this.highlightBus(busId);
            
            // Open popup
            marker.openPopup();
        }
    }

    // Center map on a bus without zooming in too much
    centerOnBus(busId) {
        const busData = this.busMarkers.get(busId);
        if (busData) {
            const { marker } = busData;
            const position = marker.getLatLng();
            
            // Center map on bus at current zoom level
            this.map.setView(position, this.map.getZoom(), {
                animate: true,
                duration: 0.5
            });
            
            // Highlight the bus
            this.highlightBus(busId);
        }
    }

    // Update bus positions for real-time updates
    updateBusPositions(buses) {
        console.log(`🔄 Updating ${buses.length} bus positions...`);
        
        let updatedCount = 0;
        let addedCount = 0;
        
        // Track which buses are still active
        const activeBusIds = new Set();
        
        buses.forEach(bus => {
            const busId = bus.vehicle_id;
            activeBusIds.add(busId);
            
            if (this.busMarkers.has(busId)) {
                // Update existing bus marker
                const busData = this.busMarkers.get(busId);
                const { marker } = busData;
                
                // Update position if coordinates are valid
                if (this.isValidCoordinate(bus.latitude, bus.longitude)) {
                    const newPos = [bus.latitude, bus.longitude];
                    marker.setLatLng(newPos);
                }
                
                // Update bearing if available
                if (bus.bearing !== undefined) {
                    marker.options.rotationAngle = bus.bearing;
                }
                
                // Update tooltip with current delay
                const delayText = bus.delay_minutes ? bus.delay_minutes.toFixed(1) + ' min delay' : 'No delay data';
                marker.setTooltipContent(`Bus ${bus.vehicle_label}<br>${delayText}`);
                
                // Update popup content
                const newPopup = this.createBusPopup(bus);
                marker.setPopupContent(newPopup);
                
                // Update status and color if needed
                const newStatus = this.getBusStatus(bus.delay_minutes);
                const newColor = this.config.liveTracking.colors[newStatus];
                
                if (busData.status !== newStatus) {
                    const newIcon = this.createBusIcon(busId, newColor, newStatus, bus.bearing);
                    marker.setIcon(newIcon);
                    
                    // If this bus is selected, update its highlight
                    if (this.selectedBus && this.selectedBus.busId === busId) {
                        this.selectedBus.originalIcon = newIcon;
                    }
                }
                
                // Update bus data
                busData.bus = bus;
                busData.status = newStatus;
                
                updatedCount++;
            } else {
                // Add new bus marker
                this.addBusMarker(bus);
                addedCount++;
            }
        });
        
        // Remove buses that are no longer active
        const busesToRemove = [];
        this.busMarkers.forEach((busData, busId) => {
            if (!activeBusIds.has(busId)) {
                busesToRemove.push(busId);
            }
        });
        
        busesToRemove.forEach(busId => {
            const busData = this.busMarkers.get(busId);
            if (busData) {
                const { marker } = busData;
                this.map.removeLayer(marker);
                this.busMarkers.delete(busId);
                
                // If removed bus was selected, clear selection
                if (this.selectedBus && this.selectedBus.busId === busId) {
                    this.clearBusHighlight();
                }
            }
        });
        
        console.log(`✅ Live update: ${updatedCount} updated, ${addedCount} added, ${busesToRemove.length} removed`);
        return { 
            updated: updatedCount, 
            added: addedCount, 
            removed: busesToRemove.length 
        };
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
        this.clearBusHighlight();
    }

    clearVisualization() {
        console.log('🗑️ Clearing current visualization...');
        
        // Remove all active layers (routes)
        this.activeLayers.forEach((layer, key) => {
            this.map.removeLayer(layer);
        });
        this.activeLayers.clear();
        
        // Remove route lines for live tracking
        this.routeLines.forEach((line, routeId) => {
            this.map.removeLayer(line);
        });
        this.routeLines.clear();
        
        // Remove all bus markers
        this.clearAllBusMarkers();
        
        // Remove legend
        if (this.legend) {
            this.map.removeControl(this.legend);
            this.legend = null;
        }
        
        // Clear highlight
        this.clearHighlight();
        
        this.currentVisualization = null;
    }

    clearAllBusMarkers() {
        this.busMarkers.forEach((busData, busId) => {
            const { marker } = busData;
            this.map.removeLayer(marker);
        });
        this.busMarkers.clear();
        this.busIconCache.clear();
        this.selectedBus = null;
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

    createLiveLegend() {
        const legend = L.control({ position: 'bottomleft' });
        
        legend.onAdd = () => {
            const div = L.DomUtil.create('div', 'legend-container live-legend-container');
            div.innerHTML = `
                <div class="legend-title">
                    <span>🚍 Live Bus Status</span>
                </div>
                <div class="legend-scale">
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${this.config.liveTracking.colors.onTime}"></div>
                        <span class="legend-label">On Time (&lt; 2 min)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${this.config.liveTracking.colors.minorDelay}"></div>
                        <span class="legend-label">Minor Delay (2-5 min)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${this.config.liveTracking.colors.majorDelay}"></div>
                        <span class="legend-label">Major Delay (&gt; 5 min)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color" style="background: ${this.config.liveTracking.colors.selected}"></div>
                        <span class="legend-label">Selected Bus</span>
                    </div>
                </div>
                <div class="legend-hint">
                    <small>Click a bus for details</small>
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
                
                // Redraw bus icons to match theme
                this.redrawBusIconsForTheme(theme);
                
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

    // Helper method to redraw bus icons for theme
    redrawBusIconsForTheme(theme) {
        // Clear icon cache to force recreation with theme-appropriate colors
        this.busIconCache.clear();
        
        // Recreate all bus markers with new icons
        this.busMarkers.forEach((busData, busId) => {
            const { marker, bus, status } = busData;
            const color = this.config.liveTracking.colors[status];
            
            // Create new icon
            const newIcon = this.createBusIcon(busId, color, status, bus.bearing);
            
            // Apply new icon
            marker.setIcon(newIcon);
        });
        
        // If there's a selected bus, update its highlight
        if (this.selectedBus) {
            const { busId, bus } = this.selectedBus;
            const highlightedIcon = this.createBusIcon(
                busId, 
                this.config.liveTracking.colors.selected,
                'selected',
                bus.bearing
            );
            
            if (this.selectedBus.marker) {
                this.selectedBus.marker.setIcon(highlightedIcon);
            }
        }
    }

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
            routesWithGeometry: Object.keys(this.routeGeometries).length,
            // NEW: Live tracking stats
            liveBuses: this.busMarkers.size,
            selectedBus: this.selectedBus ? this.selectedBus.busId : null,
            routeLines: this.routeLines.size
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
        
        // Add live bus data if in live mode
        if (this.currentVisualization === 'live') {
            baseData.liveBuses = Array.from(this.busMarkers.entries()).map(([busId, busData]) => ({
                id: busId,
                route: busData.bus.route_id,
                position: busData.marker.getLatLng(),
                status: busData.status,
                delay: busData.bus.delay_minutes
            }));
        }
        
        return baseData;
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = MapVisualizer;
}