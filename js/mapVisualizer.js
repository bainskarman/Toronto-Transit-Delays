// js/mapVisualizer.js
// Map Visualization Engine for TTC Bus Delay Analytics

class MapVisualizer {
    constructor(app) {
        this.app = app;
        this.map = null;
        this.baseLayers = {
            dark: L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>, &copy; CartoDB'
            }),
            light: L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>, &copy; CartoDB'
            })
        };
        this.currentTileLayer = null;
        this.currentView = null;
        this.layers = {
            routes: L.layerGroup(),
            wards: L.layerGroup(),
            neighbourhoods: L.layerGroup(),
            hotspots: L.layerGroup()
        };
        this.currentHeatLayer = null;
        this.routeLayers = new Map();
        this.highlightedRoute = null;
    }

    getPopupOptions() {
        if (this.app.state.mobile) {
            return { offset: L.point(0, 35) };
        }
        return {};
    }

    init(mapElementId, state) {
        const isMobile = state.mobile;
        const mapOptions = {
            center: [43.6532, -79.3832],
            zoom: isMobile ? 10 : 11,           // zoom out more on mobile
            zoomControl: false,
            attributionControl: true,
            maxBoundsViscosity: 1.0
        };
        // Apply maxBounds only on desktop
        if (!isMobile) {
            mapOptions.maxBounds = L.latLngBounds([43.58, -79.63], [43.86, -79.12]);
        }
        this.map = L.map(mapElementId, mapOptions);

        const theme = state.theme || 'dark';
        this.currentTileLayer = this.baseLayers[theme].addTo(this.map);

        L.control.zoom({ position: 'bottomright' }).addTo(this.map);

        this.layers.routes.addTo(this.map);
        this.layers.wards.addTo(this.map);
        this.layers.neighbourhoods.addTo(this.map);
        this.layers.hotspots.addTo(this.map);

        this.map.on('click', (e) => {
            this.app.onFeatureClick(null);
        });

        return this.map;
    }

    setTheme(theme) {
        if (this.currentTileLayer) {
            this.map.removeLayer(this.currentTileLayer);
        }
        this.currentTileLayer = this.baseLayers[theme].addTo(this.map);
    }

    clearView() {
        this.layers.routes.clearLayers();
        this.layers.wards.clearLayers();
        this.layers.neighbourhoods.clearLayers();
        this.layers.hotspots.clearLayers();
        this.routeLayers.clear();
        this.highlightedRoute = null;

        if (this.currentHeatLayer) {
            this.map.removeLayer(this.currentHeatLayer);
            this.currentHeatLayer = null;
        }
    }

    computeMinMax(data, metric) {
        const values = data
            .map(d => metric === 'time' ? (d.avgDelay || 0) : (d.totalCount || d.totalDelayCount || 0))
            .filter(v => v > 0);
        if (values.length === 0) return { min: 0, max: 1 };
        return {
            min: Math.min(...values),
            max: Math.max(...values)
        };
    }

    async renderView(view, metric, data) {
        this.clearView();
        this.currentView = view;

        // Extract values for coloring (skip zero values)
        const values = data
            .map(d => metric === 'time' ? (d.avgDelay || 0) : (d.totalCount || d.totalDelayCount || 0))
            .filter(v => v > 0);

        // Compute quantile breaks – use 5 bins to match the 5‑color palettes
        const numBins = 5; // adjust if you want more/less shades
        this.currentBreaks = values.length > 0 ? this.computeQuantileBreaks(values, numBins) : [0, 1];

        switch (view) {
            case 'routes': this.renderRoutes(data, metric); break;
            case 'wards': this.renderWards(data, metric); break;
            case 'neighbourhoods': this.renderNeighbourhoods(data, metric); break;
            case 'hotspots': this.renderHotspots(data, metric); break;
        }
    }

    createFeaturePopup(feature) {
        const type = feature.type;
        const totalHours = (feature.totalDelayMinutes || 0) / 60;
        const hoursFormatted = totalHours.toFixed(1);

        let title = '';
        if (type === 'route') {
            title = `<strong>Route ${feature.route}</strong><br>${feature.name}`;
        } else if (type === 'ward' || type === 'neighbourhood') {
            title = `<strong>${feature.name}</strong>`;
        } else if (type === 'stop') {
            title = `<strong>${feature.name}</strong>`;
        }

        return `
            <div class="feature-popup">
                <div class="popup-header">${title}</div>
                <div class="popup-content">
                    <div class="popup-row">
                        <span class="popup-label">Avg Delay:</span>
                        <span class="popup-value">${feature.avgDelay.toFixed(1)} min</span>
                    </div>
                    <div class="popup-row">
                        <span class="popup-label">Total Incidents:</span>
                        <span class="popup-value">${feature.totalCount.toLocaleString()}</span>
                    </div>
                    <div class="popup-row">
                        <span class="popup-label">Total Hours Lost:</span>
                        <span class="popup-value">${hoursFormatted} h</span>
                    </div>
                </div>
            </div>
        `;
    }

    highlightRoute(routeData) {
        if (!routeData) return;

        const routeId = routeData.route;
        if (this.highlightedRoute === routeId) return;

        if (this.highlightedRoute) {
            const prevLayer = this.routeLayers.get(this.highlightedRoute);
            if (prevLayer) {
                const metric = this.app.state.currentMetric;
                const value = metric === 'time' ? routeData.avgDelay : routeData.totalDelayCount;
                const color = this.getColor(value, metric);
                prevLayer.setStyle({ color: color, weight: 2 });
            }
        }

        const layer = this.routeLayers.get(routeId);
        if (layer) {
            layer.setStyle({ color: '#ffff00', weight: 4 });
            layer.bringToFront();

            const popupContent = this.createFeaturePopup({
                type: 'route',
                route: routeData.route,
                name: routeData.longName,
                avgDelay: routeData.avgDelay,
                totalCount: routeData.totalDelayCount,
                totalDelayMinutes: routeData.totalDelayMinutes
            });

            layer.bindPopup(popupContent, this.getPopupOptions()).openPopup();

            this.highlightedRoute = routeId;
        }
    }

    renderRoutes(routesData, metric) {
        const geometries = this.app.state.routeGeometries;

        routesData.forEach(route => {
            const routeId = route.route;
            const coords = geometries[routeId];
            if (!coords || coords.length === 0) return;

            let value = metric === 'time' ? route.avgDelay : route.totalDelayCount;
            const color = this.getColor(value, metric);

            const polyline = L.polyline(coords, {
                color: color,
                weight: 1.6,
                opacity: 0.8,
                smoothFactor: 1
            });

            this.routeLayers.set(routeId, polyline);

            const popupContent = this.createFeaturePopup({
                type: 'route',
                route: route.route,
                name: route.longName,
                avgDelay: route.avgDelay,
                totalCount: route.totalDelayCount,
                totalDelayMinutes: route.totalDelayMinutes
            });

            polyline.bindPopup(popupContent, this.getPopupOptions());

            polyline.on('click', (e) => {
                this.highlightRoute(route);
                this.app.onFeatureClick({
                    type: 'route',
                    route: route.route,
                    name: route.longName,
                    avgDelay: route.avgDelay,
                    totalCount: route.totalDelayCount,
                    totalDelayMinutes: route.totalDelayMinutes,
                    details: route
                });
            });

            polyline.addTo(this.layers.routes);
        });
    }

    renderWards(wardsData, metric) {
        const wardGeometries = this.app.state.wardGeometries;
        if (!wardGeometries || !wardGeometries.features) return;

        const dataMap = new Map();
        wardsData.forEach(w => dataMap.set(w.name, w));

        L.geoJSON(wardGeometries, {
            style: (feature) => {
                const wardName = feature.properties.AREA_NAME;
                const wardData = dataMap.get(wardName);
                let value = metric === 'time' ? (wardData ? wardData.avgDelay : 0) : (wardData ? wardData.totalCount : 0);
                return {
                    fillColor: this.getColor(value, metric),
                    fillOpacity: 0.6,
                    color: '#ffffff',
                    weight: 1,
                    opacity: 0.8
                };
            },
            onEachFeature: (feature, layer) => {
                const wardName = feature.properties.AREA_NAME;
                const wardData = dataMap.get(wardName) || { avgDelay: 0, totalCount: 0, totalDelayMinutes: 0 };

                const popupContent = this.createFeaturePopup({
                    type: 'ward',
                    name: wardName,
                    avgDelay: wardData.avgDelay,
                    totalCount: wardData.totalCount,
                    totalDelayMinutes: wardData.totalDelayMinutes || 0
                });

                layer.bindPopup(popupContent, this.getPopupOptions());

                layer.on('mouseover', (e) => {
                    this.app.onFeatureHover({
                        type: 'ward',
                        name: wardName,
                        avgDelay: wardData.avgDelay,
                        totalCount: wardData.totalCount
                    });
                });
                layer.on('mouseout', () => {
                    this.app.onFeatureHover(null);
                });
                layer.on('click', (e) => {
                    L.DomEvent.stopPropagation(e);
                    this.app.onFeatureClick({
                        type: 'ward',
                        name: wardName,
                        avgDelay: wardData.avgDelay,
                        totalCount: wardData.totalCount,
                        totalDelayMinutes: wardData.totalDelayMinutes || 0,
                        details: wardData
                    });
                });
            }
        }).addTo(this.layers.wards);
    }

    renderNeighbourhoods(neighbourhoodsData, metric) {
        const neighbourhoodGeometries = this.app.state.neighbourhoodGeometries;
        if (!neighbourhoodGeometries || !neighbourhoodGeometries.features) return;

        const dataMap = new Map();
        neighbourhoodsData.forEach(n => dataMap.set(n.name, n));

        L.geoJSON(neighbourhoodGeometries, {
            style: (feature) => {
                const areaName = feature.properties.AREA_NAME;
                const nData = dataMap.get(areaName);
                let value = metric === 'time' ? (nData ? nData.avgDelay : 0) : (nData ? nData.totalCount : 0);
                return {
                    fillColor: this.getColor(value, metric),
                    fillOpacity: 0.6,
                    color: '#ffffff',
                    weight: 1,
                    opacity: 0.8
                };
            },
            onEachFeature: (feature, layer) => {
                const areaName = feature.properties.AREA_NAME;
                const nData = dataMap.get(areaName) || { avgDelay: 0, totalCount: 0, totalDelayMinutes: 0 };

                const popupContent = this.createFeaturePopup({
                    type: 'neighbourhood',
                    name: areaName,
                    avgDelay: nData.avgDelay,
                    totalCount: nData.totalCount,
                    totalDelayMinutes: nData.totalDelayMinutes || 0
                });

                layer.bindPopup(popupContent, this.getPopupOptions());

                layer.on('mouseover', (e) => {
                    this.app.onFeatureHover({
                        type: 'neighbourhood',
                        name: areaName,
                        avgDelay: nData.avgDelay,
                        totalCount: nData.totalCount
                    });
                });
                layer.on('mouseout', () => {
                    this.app.onFeatureHover(null);
                });
                layer.on('click', (e) => {
                    L.DomEvent.stopPropagation(e);
                    this.app.onFeatureClick({
                        type: 'neighbourhood',
                        name: areaName,
                        avgDelay: nData.avgDelay,
                        totalCount: nData.totalCount,
                        totalDelayMinutes: nData.totalDelayMinutes || 0,
                        details: nData
                    });
                });
            }
        }).addTo(this.layers.neighbourhoods);
    }

    renderHotspots(hotspotsData, metric) {
        if (typeof L.heatLayer === 'undefined') {
            console.warn('Leaflet.heat plugin not loaded. Cannot render heatmap.');
            return;
        }
        if (!hotspotsData || hotspotsData.length === 0) return;

        const heatData = hotspotsData.map(stop => {
            const intensity = metric === 'time' ? stop.avgDelay : stop.totalCount;
            return [stop.lat, stop.lon, intensity];
        });

        if (this.currentHeatLayer) {
            this.map.removeLayer(this.currentHeatLayer);
        }

        this.currentHeatLayer = L.heatLayer(heatData, {
            radius: 2,
            blur: 2.5,
            maxZoom: 15,
            minOpacity: 0.3,
            gradient: { 0.35: 'blue', 0.55: 'lime', 0.65: 'yellow', 0.78: 'red' }
        }).addTo(this.layers.hotspots);
    }

    // Add this method:
    computeQuantileBreaks(values, numBreaks = 5) {
        if (values.length === 0) return [];
        const sorted = [...values].sort((a, b) => a - b);
        const breaks = [];
        for (let i = 0; i <= numBreaks; i++) {
            const p = i / numBreaks;
            const index = Math.floor(p * (sorted.length - 1));
            breaks.push(sorted[index]);
        }
        return breaks;
    }

    getColor(value, metric) {
        if (!this.currentBreaks || this.currentBreaks.length < 2) {
            return '#888888'; // fallback gray
        }

        // Find which quantile bin this value falls into (0‑based index)
        let bin = 0;
        for (let i = 1; i < this.currentBreaks.length; i++) {
            if (value <= this.currentBreaks[i]) {
                bin = i - 1;
                break;
            }
        }
        // If value > max, put in last bin
        if (value > this.currentBreaks[this.currentBreaks.length - 1]) {
            bin = this.currentBreaks.length - 2;
        }

        if (metric === 'time') {
            // Digital Rose palette: cyan → pink → red
            const colors = [
                [224, 179, 255], // #E0B3FF (light lavender)
                [193, 128, 255], // #C180FF (soft purple)
                [163, 77, 255],  // #A34DFF (medium purple)
                [122, 31, 255],  // #7A1FFF (vibrant violet)
                [90, 0, 179]     // #5A00B3 (deep violet)
                ];
            bin = Math.min(bin, colors.length - 1);
            const [r, g, b] = colors[bin];
            return `rgb(${r}, ${g}, ${b})`;
        } else {
            // Isoluminant approximation: light blue → blue‑gray → purple → deep purple
            // This sequence maintains near‑constant luminance while shifting hue.
            const colors = [
                [152, 212, 226], // #98D4E2 (light blue)
                [106, 147, 193], // #6A93C1 (blue‑gray)
                [83, 99, 159],   // #53639F (slate blue)
                [108, 56, 127],  // #6C387F (purple)
                [66, 20, 79]     // #42144F (deep purple)
            ];
            bin = Math.min(bin, colors.length - 1);
            const [r, g, b] = colors[bin];
            return `rgb(${r}, ${g}, ${b})`;
        }
    }
}