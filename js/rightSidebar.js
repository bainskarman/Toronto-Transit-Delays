// js/rightSidebar.js
// Right sidebar displaying top/bottom rankings (global & visible)

class RightSidebar {
    constructor(app) {
        this.app = app;
        this.mode = 'top'; // 'top' or 'bottom'
        this.isCollapsed = true; // start collapsed
        this.elements = {
            panel: null,
            toggleBtn: null,
            content: null,
            modeSwitch: null,
            globalList: null,
            visibleList: null
        };
        this.initDesktop();
    }

    initDesktop() {
        // Create the toggle button (visible when collapsed)
        const toggleBtn = document.createElement('button');
        toggleBtn.id = 'rightSidebarToggleBtn';
        toggleBtn.className = 'right-sidebar-toggle-btn';
        toggleBtn.innerHTML = '<i class="fas fa-list-ul"></i>';
        toggleBtn.setAttribute('aria-label', 'Show rankings');
        toggleBtn.style.display = 'none'; // hidden initially (we start collapsed but show toggle? we'll set after panel creation)
        document.getElementById('app').appendChild(toggleBtn);

        // Create the panel (hidden initially)
        const panel = document.createElement('div');
        panel.id = 'rightSidebar';
        panel.className = 'right-sidebar';
        panel.style.display = 'none'; // start hidden

        // Header with mode switch and collapse button
        const header = document.createElement('div');
        header.className = 'sidebar-header';
        header.innerHTML = `
            <div class="mode-switch">
                <button class="mode-option active" data-mode="top">Worse</button>
                <button class="mode-option" data-mode="bottom">Best</button>
            </div>
            <button class="sidebar-close" id="rightSidebarClose" aria-label="Close sidebar">
                <i class="fas fa-chevron-right"></i>
            </button>
        `;
        panel.appendChild(header);

        // Content container
        const content = document.createElement('div');
        content.className = 'sidebar-content';
        content.innerHTML = `
            <div class="rankings-section">
                <h4 class="rankings-title" id="globalRankingsTitle">Global</h4>
                <div id="rankingsGlobal" class="rankings-list"></div>
            </div>
            <div class="rankings-section">
                <h4 class="rankings-title" id="visibleRankingsTitle">Visible Area</h4>
                <div id="rankingsVisible" class="rankings-list"></div>
            </div>
        `;
        panel.appendChild(content);

        document.getElementById('app').appendChild(panel);

        // Cache elements
        this.elements.panel = panel;
        this.elements.toggleBtn = toggleBtn;
        this.elements.modeSwitch = header.querySelectorAll('.mode-option');
        this.elements.closeBtn = document.getElementById('rightSidebarClose');
        this.elements.globalList = document.getElementById('rankingsGlobal');
        this.elements.visibleList = document.getElementById('rankingsVisible');
        this.elements.globalTitle = document.getElementById('globalRankingsTitle');
        this.elements.visibleTitle = document.getElementById('visibleRankingsTitle');

        // Click listeners for rankings (event delegation)
        this.elements.globalList.addEventListener('click', (e) => {
            const item = e.target.closest('.rankings-item');
            if (item) {
                const id = item.dataset.id;
                if (id) this.handleRankingClick(id);
            }
        });

        this.elements.visibleList.addEventListener('click', (e) => {
            const item = e.target.closest('.rankings-item');
            if (item) {
                const id = item.dataset.id;
                if (id) this.handleRankingClick(id);
            }
        });

        // Attach events
        toggleBtn.addEventListener('click', () => this.expand());
        this.elements.closeBtn.addEventListener('click', () => this.collapse());
        this.elements.modeSwitch.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const mode = e.target.dataset.mode;
                this.setMode(mode);
            });
        });

        // Initial state: collapsed (toggle visible, panel hidden)
        this.collapse();
    }

    setMode(mode) {
        if (this.mode === mode) return;
        this.mode = mode;
        // Update active class
        this.elements.modeSwitch.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.mode === mode);
        });
        // Refresh content
        this.update();
    }

    expand() {
        this.isCollapsed = false;
        this.elements.toggleBtn.style.display = 'none';
        this.elements.panel.style.display = 'block';
        this.update(); // refresh when opening
    }

    collapse() {
        this.isCollapsed = true;
        this.elements.toggleBtn.style.display = 'flex';
        this.elements.panel.style.display = 'none';
    }

    // Main update method – rebuilds lists based on current app state
    update() {
        if (this.isCollapsed) return;

        const view = this.app.state.currentView;
        const metric = this.app.state.currentMetric;
        const mode = this.mode;
        const titleBase = this.getTitleBase(view, metric);
        const map = this.app.map;

        if (view === 'hotspots') {
            // Global rankings: no bounds
            const globalRankings = this.getRankings(null, metric, mode, null);
            // Visible rankings: pass current map bounds
            const visibleRankings = this.getRankings(null, metric, mode, map ? map.getBounds() : null);

            this.elements.globalTitle.textContent = `${titleBase} (Global)`;
            this.elements.visibleTitle.textContent = `${titleBase} (Visible)`;

            this.renderList(this.elements.globalList, globalRankings, metric);
            this.renderList(this.elements.visibleList, visibleRankings, metric);
        } else {
            // Other views: use aggregated data
            const filteredData = this.filterValidData(this.app.getFilteredData());
            const visibleData = this.filterValidData(this.getVisibleData());

            const globalRankings = this.getRankings(filteredData, metric, mode);
            const visibleRankings = this.getRankings(visibleData, metric, mode);

            this.elements.globalTitle.textContent = `${titleBase} (Global)`;
            this.elements.visibleTitle.textContent = `${titleBase} (Visible)`;

            this.renderList(this.elements.globalList, globalRankings, metric);
            this.renderList(this.elements.visibleList, visibleRankings, metric);
        }
    }

    // Filter out invalid items (currently only for routes)
    filterValidData(data) {
        const view = this.app.state.currentView;
        // Only filter for routes view
        if (view !== 'routes') return data;

        return data.filter(item => {
            const longName = item.longName;
            // Exclude if missing, empty, or exactly "Unknown" (case-insensitive)
            if (!longName) return false;
            const trimmed = longName.trim().toLowerCase();
            if (trimmed === '' || trimmed === 'unknown') return false;
            return true;
        });
    }

    // Get top or bottom 5 entries based on metric and mode
    getRankings(data, metric, mode, bounds = null) {
        const view = this.app.state.currentView;

        // --- Hotspots: use raw data for per‑year conditions and optional bounds ---
        if (view === 'hotspots') {
            const rawData = this.app.hotspotsAgg;
            if (!rawData || rawData.length === 0) return [];

            // Apply current filters (year, transit, category)
            const filters = this.app.state.filters;
            let filtered = rawData.filter(entry => {
                if (filters.year && filters.year.length > 0 && !filters.year.includes(entry.year)) return false;
                if (filters.transitTypes && filters.transitTypes.length > 0 && !filters.transitTypes.includes(entry.transit)) return false;
                if (filters.incidentCategories && filters.incidentCategories.length > 0 && !filters.incidentCategories.includes(entry.category)) return false;
                return true;
            });

            // If bounds provided (for visible list), filter by map view
            if (bounds) {
                filtered = filtered.filter(entry => bounds.contains([entry.lat, entry.lon]));
            }

            // Group by stop
            const stopMap = new Map();
            filtered.forEach(entry => {
                const key = `${entry.lat},${entry.lon}`;
                if (!stopMap.has(key)) {
                    stopMap.set(key, {
                        lat: entry.lat,
                        lon: entry.lon,
                        stop_name: entry.stop_name,
                        years: {},
                        totalDelayCount: 0,
                        totalDelayMinutes: 0
                    });
                }
                const stop = stopMap.get(key);
                if (!stop.years[entry.year]) stop.years[entry.year] = 0;
                stop.years[entry.year] += entry.delay_count;
                stop.totalDelayCount += entry.delay_count;
                stop.totalDelayMinutes += entry.total_delay_min;
            });

            // Apply per‑year condition (at least one year with ≥10 incidents) and avg delay <522
            const validStops = [];
            stopMap.forEach(stop => {
                const hasYearWithMin = Object.values(stop.years).some(count => count >= 10);
                if (hasYearWithMin) {
                    stop.avgDelay = stop.totalDelayCount > 0 ? stop.totalDelayMinutes / stop.totalDelayCount : 0;
                    if (stop.avgDelay < 522) {
                        validStops.push(stop);
                    }
                }
            });

            // Sort by value
            const sorted = [...validStops].sort((a, b) => {
                const valA = metric === 'time' ? (a.avgDelay || 0) : (a.totalDelayCount || 0);
                const valB = metric === 'time' ? (b.avgDelay || 0) : (b.totalDelayCount || 0);
                return mode === 'top' ? valB - valA : valA - valB;
            });

            // Take first 5
            const result = [];
            for (let i = 0; i < Math.min(5, sorted.length); i++) {
                const item = sorted[i];
                const value = metric === 'time' ? (item.avgDelay || 0) : (item.totalDelayCount || 0);
                result.push({ item, value });
            }
            return result;
        }


        // --- Routes: group variants by base route number before ranking ---
        if (view === 'routes') {
            // First, filter out invalid items (unknown routes)
            let validData = this.filterValidData(data);

            // Group by base route number
            const baseMap = new Map();
            validData.forEach(item => {
                // Extract base number (e.g., "12A" → "12")
                const baseMatch = item.route.match(/^(\d+)/);
                const base = baseMatch ? baseMatch[1] : item.route; // fallback to full if no digits

                if (!baseMap.has(base)) {
                    baseMap.set(base, {
                        route: base,
                        representativeId: item.route, // store the first variant's actual ID (e.g., "12A")
                        longName: item.longName,
                        active2025: item.active2025,
                        totalDelayCount: 0,
                        totalDelayMinutes: 0
                    });
                }
                const group = baseMap.get(base);
                group.totalDelayCount += item.totalDelayCount || 0;
                group.totalDelayMinutes += item.totalDelayMinutes || 0;
            });

            // Compute average delay for each group
            const groupedData = Array.from(baseMap.values()).map(g => ({
                ...g,
                avgDelay: g.totalDelayCount > 0 ? g.totalDelayMinutes / g.totalDelayCount : 0,
                totalCount: g.totalDelayCount // for consistency with other views
            }));

            // Sort by value
            const sorted = [...groupedData].sort((a, b) => {
                const valA = metric === 'time' ? (a.avgDelay || 0) : (a.totalCount || 0);
                const valB = metric === 'time' ? (b.avgDelay || 0) : (b.totalCount || 0);
                return mode === 'top' ? valB - valA : valA - valB;
            });

            // Take first 5
            const result = [];
            for (let i = 0; i < Math.min(5, sorted.length); i++) {
                const item = sorted[i];
                const value = metric === 'time' ? (item.avgDelay || 0) : (item.totalCount || 0);
                result.push({ item, value });
            }
            return result;
        }

        // --- Other views (wards, neighbourhoods) ---
        let validData = this.filterValidData(data);
        const sorted = [...validData].sort((a, b) => {
            const valA = metric === 'time' ? (a.avgDelay || 0) : (a.totalCount || a.totalDelayCount || 0);
            const valB = metric === 'time' ? (b.avgDelay || 0) : (b.totalCount || b.totalDelayCount || 0);
            return mode === 'top' ? valB - valA : valA - valB;
        });

        const result = [];
        for (let i = 0; i < Math.min(5, sorted.length); i++) {
            const item = sorted[i];
            const value = metric === 'time' ? (item.avgDelay || 0) : (item.totalCount || item.totalDelayCount || 0);
            result.push({ item, value });
        }
        return result;
    }

    // Get data for features currently visible on map
    getVisibleData() {
        const map = this.app.map;
        if (!map) return [];
        const bounds = map.getBounds();
        const view = this.app.state.currentView;
        const allData = this.app.getFilteredData(); // already filtered

        if (view === 'routes') {
            const geometries = this.app.state.routeGeometries;
            return allData.filter(route => {
                const coords = geometries[route.route];
                if (!coords || coords.length === 0) return false;
                // coords are stored as [lat, lng] (Leaflet format)
                return coords.some(coord => bounds.contains(coord));
            });
        } else if (view === 'wards') {
            const wardGeo = this.app.state.wardGeometries;
            if (!wardGeo || !wardGeo.features) return [];
            return allData.filter(ward => {
                const feature = wardGeo.features.find(f => f.properties.AREA_NAME === ward.name);
                if (!feature) return false;
                const layer = L.geoJSON(feature);
                const featureBounds = layer.getBounds();
                return bounds.intersects(featureBounds);
            });
        } else if (view === 'neighbourhoods') {
            const neighGeo = this.app.state.neighbourhoodGeometries;
            if (!neighGeo || !neighGeo.features) return [];
            return allData.filter(neigh => {
                const feature = neighGeo.features.find(f => f.properties.AREA_NAME === neigh.name);
                if (!feature) return false;
                const layer = L.geoJSON(feature);
                const featureBounds = layer.getBounds();
                return bounds.intersects(featureBounds);
            });
        } else if (view === 'hotspots') {
            // Points: check if within bounds
            return allData.filter(hotspot => {
                return bounds.contains([hotspot.lat, hotspot.lon]);
            });
        }
        return [];
    }

    // Generate title base for current view/metric and mode
    getTitleBase(view, metric) {
        const modeText = this.mode === 'top' ? 'most' : 'least';
        if (metric === 'time') {
            if (view === 'routes') return `Routes with ${modeText} avg delay`;
            if (view === 'wards') return `Wards with ${modeText} avg delay`;
            if (view === 'neighbourhoods') return `Districts with ${modeText} avg delay`;
            if (view === 'hotspots') return `Stops with ${modeText} avg delay`;
        } else {
            if (view === 'routes') return `Routes with ${modeText} incidents`;
            if (view === 'wards') return `Wards with ${modeText} incidents`;
            if (view === 'neighbourhoods') return `Districts with ${modeText} incidents`;
            if (view === 'hotspots') return `Stops with ${modeText} incidents`;
        }
        return 'Rankings';
    }

    // Render one list (global or visible)
    renderList(container, entries, metric) {
        if (!container) return;
        if (entries.length === 0) {
            container.innerHTML = '<div class="rankings-empty">No data</div>';
            return;
        }

        let html = '';
        entries.forEach((entry, idx) => {
            const displayName = this.getDisplayName(entry.item);
            const value = entry.value;
            const formattedValue = metric === 'time' ? value.toFixed(1) + ' min' : value.toLocaleString();

            // Rank display: medals for 1-3, keycap numbers for 4-5
            let rankDisplay;
            if (idx === 0) rankDisplay = '🥇';
            else if (idx === 1) rankDisplay = '🥈';
            else if (idx === 2) rankDisplay = '🥉';
            else if (idx === 3) rankDisplay = '4️⃣';
            else if (idx === 4) rankDisplay = '5️⃣';
            else rankDisplay = (idx + 1).toString(); // fallback

            html += `
                <div class="rankings-item" data-id="${this.getItemId(entry.item)}">
                    <span class="rankings-rank">${rankDisplay}</span>
                    <span class="rankings-name">${displayName}</span>
                    <span class="rankings-value">${formattedValue}</span>
                </div>
            `;
        });
        container.innerHTML = html;
    }


    getDisplayName(item) {
        const view = this.app.state.currentView;
        if (view === 'routes') {
            if (item.longName && item.longName.toLowerCase() !== 'unknown' && item.longName.trim() !== '') {
                return `${item.route} - ${item.longName}`;
            } else {
                return `Route ${item.route}`;
            }
        } else if (view === 'wards' || view === 'neighbourhoods') {
            return item.name || 'Unknown';
        } else if (view === 'hotspots') {
            // Use stop_name if available
            if (item.stop_name && item.stop_name.trim() !== '') {
                return item.stop_name;
            } else {
                return `Stop (${item.lat?.toFixed(4)}, ${item.lon?.toFixed(4)})`;
            }
        }
        return 'Unknown';
    }

    getItemId(item) {
        const view = this.app.state.currentView;
        if (view === 'routes') {
            // Use the stored representativeId (variant ID) for geometry lookup
            return item.representativeId || item.route;
        }
        if (view === 'wards') return item.name;
        if (view === 'neighbourhoods') return item.name;
        if (view === 'hotspots') return `${item.lat},${item.lon}`;
        return '';
    }

    handleRankingClick(id) {
        const view = this.app.state.currentView;
        const map = this.app.map;
        if (!map) return;

        if (view === 'routes') {
            // id is the representative variant ID (e.g., "12A")
            this.app.mapVisualizer.highlightRoute(id);
        } 
        else if (view === 'wards') {
            const wardGeo = this.app.state.wardGeometries;
            if (!wardGeo) return;
            const feature = wardGeo.features.find(f => f.properties.AREA_NAME === id);
            if (feature) {
                const layer = L.geoJSON(feature);
                map.fitBounds(layer.getBounds(), { padding: [20, 20] });
            }
        } 
        else if (view === 'neighbourhoods') {
            const neighGeo = this.app.state.neighbourhoodGeometries;
            if (!neighGeo) return;
            const feature = neighGeo.features.find(f => f.properties.AREA_NAME === id);
            if (feature) {
                const layer = L.geoJSON(feature);
                map.fitBounds(layer.getBounds(), { padding: [20, 20] });
            }
        } 
        else if (view === 'hotspots') {
            const [lat, lon] = id.split(',').map(Number);
            if (!isNaN(lat) && !isNaN(lon)) {
                map.flyTo([lat, lon], 16, { duration: 1.5 });
            }
        }
    }

    // ==================== MOBILE DRAWER METHODS ====================

    // Returns HTML for the mobile drawer content (without header)
    getMobileHTML() {
        const mode = this.mode;
        const lists = this.getMobileListsHTML(mode);
        return `
            <div class="mobile-rankings-mode-switch">
                <button class="mode-option ${mode === 'top' ? 'active' : ''}" data-mode="top">Worse</button>
                <button class="mode-option ${mode === 'bottom' ? 'active' : ''}" data-mode="bottom">Best</button>
            </div>
            <div class="mobile-rankings-sections">
                <div class="mobile-rankings-section">
                    <h3>Global</h3>
                    <div id="mobileRankingsGlobal" class="rankings-list">${lists.global}</div>
                </div>
                <div class="mobile-rankings-section">
                    <h3>Visible</h3>
                    <div id="mobileRankingsVisible" class="rankings-list">${lists.visible}</div>
                </div>
            </div>
        `;
    }

    // Returns HTML strings for global and visible lists for a given mode
    getMobileListsHTML(mode) {
        const view = this.app.state.currentView;
        const metric = this.app.state.currentMetric;
        const map = this.app.map;

        if (view === 'hotspots') {
            // Global rankings: no bounds
            const globalRankings = this.getRankings(null, metric, mode, null);
            // Visible rankings: pass current map bounds
            const visibleRankings = this.getRankings(null, metric, mode, map ? map.getBounds() : null);

            return {
                global: this.renderMobileList(globalRankings, metric),
                visible: this.renderMobileList(visibleRankings, metric)
            };
        } else {
            // Other views: use aggregated data
            const allData = this.filterValidData(this.app.getFilteredData());
            const visibleData = this.filterValidData(this.getVisibleData());

            const globalRankings = this.getRankings(allData, metric, mode);
            const visibleRankings = this.getRankings(visibleData, metric, mode);

            return {
                global: this.renderMobileList(globalRankings, metric),
                visible: this.renderMobileList(visibleRankings, metric)
            };
        }
    }

    // Helper to render a mobile list
    renderMobileList(entries, metric) {
        if (entries.length === 0) return '<div class="rankings-empty">No data</div>';
        let html = '';
        entries.forEach((entry, idx) => {
            const name = this.getDisplayName(entry.item);
            const value = metric === 'time' ? entry.value.toFixed(1) + ' min' : entry.value.toLocaleString();

            let rankDisplay;
            if (idx === 0) rankDisplay = '🥇';
            else if (idx === 1) rankDisplay = '🥈';
            else if (idx === 2) rankDisplay = '🥉';
            else if (idx === 3) rankDisplay = '4️⃣';
            else if (idx === 4) rankDisplay = '5️⃣';
            else rankDisplay = (idx + 1).toString();

            html += `
                <div class="mobile-rankings-item">
                    <span class="mobile-rankings-rank">${rankDisplay}</span>
                    <span class="mobile-rankings-name">${name}</span>
                    <span class="mobile-rankings-value">${value}</span>
                </div>
            `;
        });
        return html;
    }
}