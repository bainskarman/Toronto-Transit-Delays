// Data Loading and Processing Module for TTC Delay Visualization
class DataLoader {
    constructor() {
        this.basePath = 'assets/data/';
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 minutes cache
    }

   // Add this method to your DataLoader class
filterDataByDateRange(data, startDate, endDate) {
    if (!startDate || !endDate) {
        console.log('📅 No date range provided, returning all data');
        return data;
    }
    
    console.log(`📅 Filtering data from ${startDate} to ${endDate}`);
    console.log(`📊 Starting with ${data.length} records`);
    
    // Debug: check what date fields are available
    const dateFields = [];
    if (data.length > 0) {
        const sample = data[0];
        Object.keys(sample).forEach(key => {
            if (key.toLowerCase().includes('date')) {
                dateFields.push(key);
            }
        });
        console.log('📅 Available date fields:', dateFields);
    }
    
    const filteredData = data.filter(item => {
        // Try multiple date field names
        let itemDate;
        
        // Try different date field names
        if (item.Date) {
            itemDate = new Date(item.Date);
        } else if (item.Report_Date) {
            itemDate = new Date(item.Report_Date);
        } else if (item['Report Date']) {
            itemDate = new Date(item['Report Date']);
        } else if (item.ReportDate) {
            itemDate = new Date(item.ReportDate);
        } else if (item.date) {
            itemDate = new Date(item.date);
        } else {
            // If no date field found, check all fields that might contain dates
            for (const key in item) {
                if (key.toLowerCase().includes('date') && item[key]) {
                    try {
                        itemDate = new Date(item[key]);
                        if (!isNaN(itemDate.getTime())) {
                            console.log(`✅ Found date in field: ${key}`, item[key]);
                            break;
                        }
                    } catch (e) {
                        // Continue to next field
                    }
                }
            }
        }
        
        // If we still don't have a valid date, include the item (don't filter it out)
        if (!itemDate || isNaN(itemDate.getTime())) {
            console.log('⚠️ No valid date found for item:', item);
            return true; // Keep items without dates for now
        }
        
        // Adjust end date to include the entire day
        const adjustedEndDate = new Date(endDate);
        adjustedEndDate.setHours(23, 59, 59, 999);
        
        const isInRange = itemDate >= startDate && itemDate <= adjustedEndDate;
        
        if (!isInRange) {
            console.log('❌ Date out of range:', itemDate, 'Range:', startDate, 'to', adjustedEndDate);
        }
        
        return isInRange;
    });
    
    console.log(`✅ Filtered to ${filteredData.length} records`);
    
    if (filteredData.length === 0) {
        console.log('❌ No data after filtering. Sample of original data:');
        console.log(data.slice(0, 3));
    }
    
    return filteredData;
}
debugDataStructure(data) {
    if (!data || data.length === 0) {
        console.log('❌ No data available for debugging');
        return;
    }
    
    console.log('🔍 Debugging data structure:');
    console.log('First item:', data[0]);
    console.log('Total items:', data.length);
    
    // Check date fields
    const sampleWithDate = data.find(item => item.Date || item.Report_Date || item['Report Date']);
    console.log('Sample with date field:', sampleWithDate);
    
    // Check date formats
    const dates = data
        .map(item => item.Date || item.Report_Date || item['Report Date'])
        .filter(Boolean)
        .slice(0, 5);
    console.log('Sample dates:', dates);
    
    // Check route data
    const routes = [...new Set(data.map(item => item.Route).filter(Boolean))].slice(0, 10);
    console.log('Sample routes:', routes);
}
// Update loadRoutePerformance to support date filtering
async loadRoutePerformance(dateRange = null) {
    console.log('📈 Loading route performance data...');
    
    try {
        const data = await this.fetchCSV('route_performance.csv');
        console.log('✅ Raw CSV data loaded:', data.length, 'rows');
        
        if (!data || data.length === 0) {
            console.warn('⚠️ CSV data is empty, using sample data');
            return this.getSampleRoutePerformance();
        }

        const processedData = data.map(row => {
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

async loadRawDelayData() {
    console.log('📊 Loading raw delay data for date filtering...');
    
    try {
        const response = await fetch(`${this.basePath}all_delay_data_merged.json`);
        if (!response.ok) {
            throw new Error(`Failed to fetch raw delay data: ${response.statusText}`);
        }
        
        const rawData = await response.json();
        console.log(`✅ Loaded ${rawData.length} raw delay records`);
        return rawData;
        
    } catch (error) {
        console.error('❌ Error loading raw delay data:', error);
        return [];
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

calculateMetricsFromFilteredData(filteredData) {
    if (!filteredData || filteredData.length === 0) {
        return this.getDefaultStatistics();
    }
    
    // Calculate metrics from filtered data
    const totalDelays = filteredData.length;
    
    // Calculate valid delays (Min Delay > 0)
    const validDelays = filteredData.filter(item => 
        item['Min Delay'] > 0 || (item.Delay && item.Delay > 0)
    ).length;
    
    // Calculate average delay
    const delayValues = filteredData
        .map(item => item['Min Delay'] || item.Delay || 0)
        .filter(delay => delay > 0);
    
    const avgDelay = delayValues.length > 0 
        ? delayValues.reduce((sum, delay) => sum + delay, 0) / delayValues.length 
        : 0;
    
    // Calculate unique routes and vehicles
    const uniqueRoutes = new Set(filteredData.map(item => item.Route).filter(Boolean)).size;
    const uniqueVehicles = new Set(filteredData.map(item => item.Vehicle).filter(Boolean)).size;
    const uniqueLocations = new Set(filteredData.map(item => item.Location).filter(Boolean)).size;
    
    // Calculate date range
    const dates = filteredData
        .map(item => {
            if (item.Date) return new Date(item.Date);
            if (item.Report_Date) return new Date(item.Report_Date);
            if (item['Report Date']) return new Date(item['Report Date']);
            return null;
        })
        .filter(date => date && !isNaN(date.getTime()));
    
    const oldestDate = dates.length > 0 ? new Date(Math.min(...dates)) : null;
    const mostRecentDate = dates.length > 0 ? new Date(Math.max(...dates)) : null;
    
    // Calculate peak hour from filtered data
    const peakHour = this.calculatePeakHourFromData(filteredData);
    
    return {
        total_delays: totalDelays,
        valid_delays: validDelays,
        avg_delay_minutes: Math.round(avgDelay * 100) / 100,
        unique_routes: uniqueRoutes,
        unique_vehicles: uniqueVehicles,
        unique_locations: uniqueLocations,
        data_points: totalDelays,
        coverage_percentage: Math.round((uniqueRoutes / 150) * 100), // Approximate
        time_period: oldestDate && mostRecentDate 
            ? `${oldestDate.getFullYear()}-${mostRecentDate.getFullYear()}`
            : 'Custom Range',
        updated_at: new Date().toISOString(),
        data_refresh_date: new Date().toISOString().split('T')[0],
        data_oldest_date: oldestDate ? oldestDate.toISOString() : null,
        data_most_recent_date: mostRecentDate ? mostRecentDate.toISOString() : null,
        data_update_date: new Date().toISOString().split('T')[0],
        peak_delay_hour: peakHour,
        most_delayed_route: 'Calculating...',
        displayed_routes_count: uniqueRoutes,
        total_routes_count: uniqueRoutes,
        data_quality: {
            valid_delay_percentage: totalDelays > 0 ? Math.round((validDelays / totalDelays) * 100) : 0,
            route_coverage: uniqueRoutes,
            location_coverage: uniqueLocations,
            date_range_available: oldestDate !== null && mostRecentDate !== null
        }
    };
}

calculatePeakHourFromData(data) {
    try {
        const hours = data
            .map(item => {
                if (item.Time) {
                    const timeStr = item.Time.toString();
                    const match = timeStr.match(/(\d{1,2}):/);
                    return match ? parseInt(match[1]) : null;
                }
                return null;
            })
            .filter(hour => hour !== null);
        
        if (hours.length > 0) {
            const hourCounts = hours.reduce((acc, hour) => {
                acc[hour] = (acc[hour] || 0) + 1;
                return acc;
            }, {});
            
            const peakHour = Object.keys(hourCounts).reduce((a, b) => 
                hourCounts[a] > hourCounts[b] ? a : b
            );
            
            return `${peakHour.toString().padStart(2, '0')}:00`;
        }
    } catch (e) {
        console.error('Error calculating peak hour:', e);
    }
    
    return "08:00";
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
            
            // Use the exact property names from your Python output
            const stats = {
                total_delays: data.total_delays || data.valid_delays || 0,
                avg_delay_minutes: data.avg_delay_minutes || data.avg_delay_min || 0,
                total_routes: data.unique_routes || data.total_routes || 0,
                coverage_percentage: data.coverage_percentage || 0,
                data_points: data.data_points || data.total_delays || 0,
                time_period: data.time_period || 'Last 30 days',
                updated_at: data.updated_at || new Date().toISOString(),
                data_refresh_date: data.data_refresh_date || data.updated_at,
                data_most_recent_date: data.data_most_recent_date,
                data_oldest_date: data.data_oldest_date,
                peak_delay_hour: data.peak_delay_hour || '08:00',
                most_delayed_route: data.most_delayed_route || 'Unknown'
            };

            console.log('✅ Loaded summary statistics');
            return stats;

        } catch (error) {
            console.error('❌ Error loading summary statistics:', error);
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
            avg_delay_minutes: 8.7, // CHANGED: Use minutes instead of min
            total_routes: 156,
            coverage_percentage: 87.5,
            data_points: 456892,
            time_period: '2014-2025', // CHANGED: Use the actual period
            updated_at: new Date().toISOString(),
            data_refresh_date: new Date().toISOString().split('T')[0], // NEW
            data_most_recent_date: new Date().toISOString(), // NEW
            data_oldest_date: new Date('2014-01-01').toISOString(), // NEW
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