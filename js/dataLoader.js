// js/dataLoader.js
// Data Loading and Processing Module

class DataLoader {
    constructor() {
        this.basePath = 'assets/data/';
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 minutes
    }

    // Generic fetch with caching
    async fetchFile(filename, type = 'json') {
        const cacheKey = `${type}_${filename}`;
        if (this.isCacheValid(cacheKey)) {
            return this.cache.get(cacheKey).data;
        }

        const url = this.basePath + filename;
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Failed to fetch ${filename}: ${response.statusText}`);
        }

        let data;
        if (type === 'json') {
            data = await response.json();
        } else if (type === 'csv') {
            const text = await response.text();
            data = this.parseCSV(text);
        } else if (type === 'geojson') {
            data = await response.json(); // GeoJSON is just JSON
        }

        this.cache.set(cacheKey, { data, timestamp: Date.now() });
        return data;
    }

    parseCSV(csvText) {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return [];
        const headers = lines[0].split(',').map(h => h.trim());
        const result = [];
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',').map(v => v.trim());
            const row = {};
            headers.forEach((header, index) => {
                row[header] = values[index] || '';
            });
            result.push(row);
        }
        return result;
    }

    isCacheValid(key) {
        if (!this.cache.has(key)) return false;
        const cached = this.cache.get(key);
        return (Date.now() - cached.timestamp) < this.cacheTimeout;
    }

    // Specific load methods for each required file
    async loadRouteAnalysis() {
        return this.fetchFile('route_analysis.csv', 'csv');
    }

    // New aggregated data files
    async loadWardsAggregated() {
        return this.fetchFile('wards_aggregated.json', 'json');
    }

    async loadNeighbourhoodsAggregated() {
        return this.fetchFile('neighbourhoods_aggregated.json', 'json');
    }

    async loadHotspotsAggregated() {
        return this.fetchFile('hotspots_aggregated.json', 'json');
    }

    async loadRouteGeometries() {
        return this.fetchFile('route_geometries.json', 'json');
    }

    async loadWardBoundaries() {
        return this.fetchFile('gtawards.geojson', 'geojson');
    }

    async loadNeighbourhoodBoundaries() {
        return this.fetchFile('toronto.geojson', 'geojson');
    }

    // Convenience method to load all datasets at once
    async loadAll() {
        const [
            routeAnalysis,
            wardsAgg,
            neighbourhoodsAgg,
            hotspotsAgg,
            routeGeometries,
            wardsGeoJSON,
            neighbourhoodsGeoJSON
        ] = await Promise.all([
            this.loadRouteAnalysis(),
            this.loadWardsAggregated(),
            this.loadNeighbourhoodsAggregated(),
            this.loadHotspotsAggregated(),
            this.loadRouteGeometries(),
            this.loadWardBoundaries(),
            this.loadNeighbourhoodBoundaries()
        ]);
        return {
            routeAnalysis,
            wardsAgg,
            neighbourhoodsAgg,
            hotspotsAgg,
            routeGeometries,
            wardsGeoJSON,
            neighbourhoodsGeoJSON
        };
    }

    // Clear cache
    clearCache() {
        this.cache.clear();
    }
}