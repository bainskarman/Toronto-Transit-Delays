// Data Loading and Processing Module for TTC Delay Visualization
class DataLoader {
    constructor() {
        this.basePath = 'assets/data/';
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 minutes cache
    }

   async loadRoutePerformance() {
    console.log('📈 Loading route performance data...');
    
    try {
        const data = await this.fetchCSV('route_performance.csv');
        console.log('✅ Raw CSV data loaded:', data.length, 'rows');
        
        if (!data || data.length === 0) {
            console.warn('⚠️ CSV data is empty, using sample data');
            return this.getSampleRoutePerformance();
        }

        const processedData = data.map(row => {
            // Debug each row
            const avgDelay = this.parseNumber(row.Avg_Delay_Min);
            if (avgDelay === null || avgDelay === 0) {
                console.warn('⚠️ Invalid delay value in row:', row);
            }
            
            return {
                Route: this.cleanRouteId(row.Route),
                route_long_name: row.route_long_name || `Route ${row.Route}`,
                Avg_Delay_Min: avgDelay,
                Delay_Count: this.parseNumber(row.Delay_Count),
                Total_Delay_Min: this.parseNumber(row.Total_Delay_Min),
                On_Time_Percentage: this.parseNumber(row.On_Time_Percentage),
                Delay_Frequency: this.parseNumber(row.Delay_Frequency)
            };
        }).filter(route => {
            const isValid = route.Avg_Delay_Min !== null && 
                           route.Delay_Count !== null &&
                           route.Route;
            if (!isValid) {
                console.warn('❌ Filtered out invalid route:', route);
            }
            return isValid;
        });

        console.log(`✅ Processed ${processedData.length} valid route records`);
        
        if (processedData.length === 0) {
            console.warn('⚠️ No valid routes after processing, using sample data');
            return this.getSampleRoutePerformance();
        }

        return processedData;

    } catch (error) {
        console.error('❌ Error loading route performance data:', error);
        return this.getSampleRoutePerformance();
    }
}

    async loadRouteGeometries() {
        console.log('🗺️ Loading route geometries...');
        
        try {
            const data = await this.fetchJSON('route_geometries.json');
            
            // Process and clean geometry data
            const processedGeometries = {};
            let validCount = 0;
            let emptyCount = 0;

            for (const [routeId, coordinates] of Object.entries(data)) {
                const cleanRouteId = this.cleanRouteId(routeId);
                
                if (coordinates && Array.isArray(coordinates) && coordinates.length > 0) {
                    // Filter out invalid coordinates
                    const validCoords = coordinates.filter(coord => 
                        coord && 
                        Array.isArray(coord) && 
                        coord.length === 2 &&
                        this.isValidCoordinate(coord[0], coord[1])
                    );

                    if (validCoords.length > 0) {
                        processedGeometries[cleanRouteId] = validCoords;
                        validCount++;
                    } else {
                        emptyCount++;
                    }
                } else {
                    emptyCount++;
                }
            }

            console.log(`✅ Loaded ${validCount} valid route geometries, ${emptyCount} empty/invalid`);
            return processedGeometries;

        } catch (error) {
            console.error('❌ Error loading route geometries:', error);
            
            // Return sample geometries for demonstration
            console.warn('⚠️ Using sample geometries for demonstration');
            return this.getSampleRouteGeometries();
        }
    }

    async loadLocationAnalysis() {
        console.log('📍 Loading location analysis...');
        
        try {
            const data = await this.fetchCSV('location_analysis.csv');
            
            // Process location data
            const processedData = data.map(row => ({
                location_id: row.location_id,
                location_name: row.location_name,
                total_delays: this.parseNumber(row.total_delays),
                avg_delay_min: this.parseNumber(row.avg_delay_min),
                latitude: this.parseNumber(row.latitude),
                longitude: this.parseNumber(row.longitude),
                route_count: this.parseNumber(row.route_count),
                peak_hours: row.peak_hours ? JSON.parse(row.peak_hours) : []
            })).filter(location => 
                location.latitude !== null && 
                location.longitude !== null &&
                this.isValidCoordinate(location.latitude, location.longitude)
            );

            console.log(`✅ Loaded ${processedData.length} location analysis records`);
            return processedData;

        } catch (error) {
            console.error('❌ Error loading location analysis:', error);
            return [];
        }
    }

    async loadSummaryStatistics() {
        console.log('📊 Loading summary statistics...');
        
        try {
            const data = await this.fetchJSON('summary_statistics.json');
            
            // Ensure all required fields with fallbacks
            const stats = {
                total_delays: data.total_delays || 0,
                avg_delay_min: data.avg_delay_minutes || 0,
                total_routes: data.unique_routes || 0,
                coverage_percentage: data.coverage_percentage || 0,
                data_points: data.data_points || 0,
                time_period: data.time_period || 'Last 30 days',
                updated_at: data.updated_at || new Date().toISOString(),
                peak_delay_hour: data.peak_delay_hour || '08:00',
                most_delayed_route: data.most_delayed_route || 'Unknown'
            };

            console.log('✅ Loaded summary statistics');
            return stats;

        } catch (error) {
            console.error('❌ Error loading summary statistics:', error);
            
            // Return default statistics
            return this.getDefaultStatistics();
        }
    }

    // Helper method to fetch CSV files
    async fetchCSV(filename) {
        const cacheKey = `csv_${filename}`;
        
        // Check cache first
        if (this.isCacheValid(cacheKey)) {
            return this.cache.get(cacheKey).data;
        }

        const response = await fetch(`${this.basePath}${filename}`);
        
        if (!response.ok) {
            throw new Error(`Failed to fetch ${filename}: ${response.statusText}`);
        }

        const csvText = await response.text();
        const data = this.parseCSV(csvText);
        
        // Cache the result
        this.cache.set(cacheKey, {
            data: data,
            timestamp: Date.now()
        });

        return data;
    }

    // Helper method to fetch JSON files
    async fetchJSON(filename) {
        const cacheKey = `json_${filename}`;
        
        // Check cache first
        if (this.isCacheValid(cacheKey)) {
            return this.cache.get(cacheKey).data;
        }

        const response = await fetch(`${this.basePath}${filename}`);
        
        if (!response.ok) {
            throw new Error(`Failed to fetch ${filename}: ${response.statusText}`);
        }

        const data = await response.json();
        
        // Cache the result
        this.cache.set(cacheKey, {
            data: data,
            timestamp: Date.now()
        });

        return data;
    }

    // Parse CSV text into array of objects
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

    // Clean and validate route IDs
    cleanRouteId(routeId) {
        if (typeof routeId === 'number') return routeId.toString();
        if (typeof routeId === 'string') return routeId.trim();
        return '';
    }

    // Parse numbers with validation
    parseNumber(value) {
        if (value === null || value === undefined || value === '') return null;
        
        const num = parseFloat(value);
        return isNaN(num) ? null : num;
    }

    // Validate geographic coordinates
    isValidCoordinate(lat, lng) {
        return lat !== null && 
               lng !== null && 
               !isNaN(lat) && 
               !isNaN(lng) &&
               lat >= -90 && lat <= 90 &&
               lng >= -180 && lng <= 180;
    }

    // Cache validation
    isCacheValid(key) {
        if (!this.cache.has(key)) return false;
        
        const cached = this.cache.get(key);
        return (Date.now() - cached.timestamp) < this.cacheTimeout;
    }

    // Clear cache
    clearCache() {
        this.cache.clear();
        console.log('🗑️ Data cache cleared');
    }

    // Get cache statistics
    getCacheStats() {
        return {
            size: this.cache.size,
            keys: Array.from(this.cache.keys())
        };
    }

    // Sample data for demonstration purposes
    getSampleRoutePerformance() {
        return [
            { Route: '501', route_long_name: 'Queen Street', Avg_Delay_Min: 8.5, Delay_Count: 245, Total_Delay_Min: 2082.5, On_Time_Percentage: 72.3, Delay_Frequency: 2.1 },
            { Route: '504', route_long_name: 'King Street', Avg_Delay_Min: 12.3, Delay_Count: 189, Total_Delay_Min: 2324.7, On_Time_Percentage: 65.8, Delay_Frequency: 3.4 },
            { Route: '505', route_long_name: 'Dundas Street', Avg_Delay_Min: 6.7, Delay_Count: 156, Total_Delay_Min: 1045.2, On_Time_Percentage: 81.2, Delay_Frequency: 1.8 },
            { Route: '506', route_long_name: 'Carlton Street', Avg_Delay_Min: 15.2, Delay_Count: 278, Total_Delay_Min: 4225.6, On_Time_Percentage: 58.9, Delay_Frequency: 4.2 },
            { Route: '509', route_long_name: 'Harbourfront', Avg_Delay_Min: 4.3, Delay_Count: 98, Total_Delay_Min: 421.4, On_Time_Percentage: 88.7, Delay_Frequency: 1.2 },
            { Route: '510', route_long_name: 'Spadina Avenue', Avg_Delay_Min: 9.8, Delay_Count: 167, Total_Delay_Min: 1636.6, On_Time_Percentage: 74.5, Delay_Frequency: 2.5 }
        ];
    }

    getSampleRouteGeometries() {
        // Sample coordinates around Toronto downtown
        const torontoCenter = [43.6532, -79.3832];
        
        return {
            '501': this.generateRouteCoordinates(torontoCenter, 0.02, 15),
            '504': this.generateRouteCoordinates(torontoCenter, 0.025, 20),
            '505': this.generateRouteCoordinates(torontoCenter, 0.018, 12),
            '506': this.generateRouteCoordinates(torontoCenter, 0.022, 18),
            '509': this.generateRouteCoordinates(torontoCenter, 0.015, 10),
            '510': this.generateRouteCoordinates(torontoCenter, 0.02, 16)
        };
    }

    generateRouteCoordinates(center, radius, pointCount) {
        const coordinates = [];
        
        for (let i = 0; i < pointCount; i++) {
            const angle = (i / pointCount) * Math.PI * 2;
            const lat = center[0] + Math.cos(angle) * radius * (0.5 + Math.random() * 0.5);
            const lng = center[1] + Math.sin(angle) * radius * (0.5 + Math.random() * 0.5);
            coordinates.push([lat, lng]);
        }
        
        return coordinates;
    }

    getDefaultStatistics() {
        return {
            total_delays: 12478,
            avg_delay_min: 8.7,
            total_routes: 156,
            coverage_percentage: 87.5,
            data_points: 456892,
            time_period: 'Last 30 days',
            updated_at: new Date().toISOString(),
            peak_delay_hour: '08:00',
            most_delayed_route: '506 - Carlton Street'
        };
    }

    // Data validation and quality checks
    validateDataQuality(routes, geometries) {
        const issues = [];
        
        // Check for routes without geometry
        const routesWithoutGeometry = routes.filter(route => 
            !geometries[route.Route.toString()]
        );
        
        if (routesWithoutGeometry.length > 0) {
            issues.push(`${routesWithoutGeometry.length} routes missing geometry data`);
        }
        
        // Check for geometries without route data
        const geometriesWithoutRoutes = Object.keys(geometries).filter(geoRouteId => 
            !routes.find(route => route.Route.toString() === geoRouteId)
        );
        
        if (geometriesWithoutRoutes.length > 0) {
            issues.push(`${geometriesWithoutRoutes.length} geometries without route data`);
        }
        
        // Check for invalid delay values
        const invalidDelays = routes.filter(route => 
            route.Avg_Delay_Min < 0 || route.Avg_Delay_Min > 60
        );
        
        if (invalidDelays.length > 0) {
            issues.push(`${invalidDelays.length} routes with unrealistic delay values`);
        }
        
        return {
            isValid: issues.length === 0,
            issues: issues,
            stats: {
                total_routes: routes.length,
                routes_with_geometry: routes.length - routesWithoutGeometry.length,
                total_geometries: Object.keys(geometries).length
            }
        };
    }

    // Export data for debugging
    exportDataSummary(routes, geometries, locationAnalysis, summaryStats) {
        return {
            routes: {
                count: routes.length,
                sample: routes.slice(0, 3),
                delay_stats: {
                    min: Math.min(...routes.map(r => r.Avg_Delay_Min)),
                    max: Math.max(...routes.map(r => r.Avg_Delay_Min)),
                    avg: routes.reduce((sum, r) => sum + r.Avg_Delay_Min, 0) / routes.length
                }
            },
            geometries: {
                count: Object.keys(geometries).length,
                sample_keys: Object.keys(geometries).slice(0, 3),
                total_coordinates: Object.values(geometries).reduce((sum, coords) => sum + coords.length, 0)
            },
            locations: {
                count: locationAnalysis.length
            },
            summary: summaryStats
        };
    }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DataLoader;
}