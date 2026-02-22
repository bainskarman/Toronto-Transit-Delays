// js/dashboard.js
// Dashboard Controller – redesigned with three tabs and light palette

class DashboardController {
    constructor(app) {
        this.app = app;
        this.data = {
            delay_distribution: null,
            top_incident_causes: null,
            weekly_patterns: null,
            daily_patterns: null,
            hourly_frequency_delay: null,
            monthly_trends: null,
            weekday_hour_heatmap: null,
            time_of_day_comparison: null,
            yearly_trends: null,
            route_scatter_data: null,
            top_delayed_routes: null,          // avg delay (top 10)
            top_delayed_routes_count: null,    // incident count (top 10)
            improving_routes: null,
            declining_routes: null,
            monthly_comparison: null            // new dataset
        };
        this.charts = {};
        this.currentTab = 'overview';
        this.loaded = false;

        // Light palette (light blue → deep purple)
        this.palette = [
            [152, 212, 226], // #98D4E2
            [106, 147, 193], // #6A93C1
            [83, 99, 159],   // #53639F
            [108, 56, 127],  // #6C387F
            [66, 20, 79]     // #42144F
        ];

        // DOM elements
        this.content = document.getElementById('dashboardContent');
        this.contentInner = document.getElementById('dashboardContentInner');
    }

    async loadData() {
        if (this.loaded) return;
        console.log('📊 Loading dashboard data...');
        try {
            const datasets = [
                'delay_distribution',
                'top_incident_causes',
                'weekly_patterns',
                'daily_patterns',
                'hourly_frequency_delay',
                'monthly_trends',
                'weekday_hour_heatmap',
                'time_of_day_comparison',
                'yearly_trends',
                'route_scatter_data',
                'top_delayed_routes',
                'top_delayed_routes_count',
                'improving_routes',
                'declining_routes',
                'monthly_comparison'                // added
            ];

            const loadPromises = datasets.map(name => this.loadDataset(name));
            const results = await Promise.all(loadPromises);

            datasets.forEach((name, index) => {
                if (results[index]) this.data[name] = results[index];
                else console.warn(`⚠️ ${name} not loaded`);
            });

            // Combine improving and declining routes into one object for chart use
            this.data.improving_declining = {
                improving: this.data.improving_routes || [],
                declining: this.data.declining_routes || []
            };

            this.loaded = true;
            console.log('✅ Dashboard data loaded', this.data);
        } catch (error) {
            console.error('❌ Failed to load dashboard data:', error);
            this.showError('Could not load dashboard data.');
        }
    }

    async loadDataset(name) {
        const paths = [
            `assets/data/dashboard/${name}.json`,
            `../assets/data/dashboard/${name}.json`,
            `/assets/data/dashboard/${name}.json`
        ];
        for (const path of paths) {
            try {
                const response = await fetch(path);
                if (response.ok) return await response.json();
            } catch (e) { /* ignore */ }
        }
        return null;
    }

    updateGlobalKPIs() {
        const kpis = this.data.kpi_metrics;
        if (!kpis) return;

        const formatNumber = (num) => {
            if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
            if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
            return num.toLocaleString();
        };

        const kpiPrimary = document.getElementById('kpiPrimaryValue');
        const kpiTotal = document.getElementById('kpiTotalIncidents');
        const kpiRoutes = document.getElementById('kpiRoutesReporting');

        if (kpiPrimary) kpiPrimary.textContent = (kpis.avg_delay_minutes?.toFixed(1) || '--') + ' min';
        if (kpiTotal) kpiTotal.textContent = formatNumber(kpis.total_incidents) || '--';
        if (kpiRoutes) kpiRoutes.textContent = formatNumber(kpis.routes_tracked) || '--';
    }

    showTab(tabName) {
        if (!this.loaded) {
            this.loadData().then(() => this.renderTab(tabName));
        } else {
            this.renderTab(tabName);
        }

        // Update active state on floating tab buttons
        const tabButtons = document.querySelectorAll('.dashboard-tab-item');
        tabButtons.forEach(btn => {
            btn.classList.toggle('active', btn.dataset.tab === tabName);
        });

        this.currentTab = tabName;
    }

    renderTab(tabName) {
        this.contentInner.innerHTML = '';
        switch (tabName) {
            case 'overview':
                this.renderOverview();
                break;
            case 'time-patterns':
                this.renderTimePatterns();
                break;
            case 'route-analysis':
                this.renderRouteAnalysis();
                break;
            default:
                this.contentInner.innerHTML = '<p>Select a tab</p>';
        }
        setTimeout(() => this.resizeCharts(), 100);
    }

    // ---------- Helper methods ----------
    showEmpty(container, icon, message) {
        if (!container) return;
        container.innerHTML = `<div class="empty-state"><i class="fas ${icon}"></i><p>${message}</p></div>`;
    }

    formatNumber(num) {
        if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
        if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
        return num.toString();
    }

    // Get color from palette based on ratio 0..1
    getColor(ratio) {
        const palette = this.palette;
        if (!palette.length) return '#cccccc';
        if (ratio <= 0) return `rgb(${palette[0].join(',')})`;
        if (ratio >= 1) return `rgb(${palette[palette.length-1].join(',')})`;

        const segment = ratio * (palette.length - 1);
        const idx = Math.floor(segment);
        const t = segment - idx;
        const nextIdx = Math.min(idx + 1, palette.length - 1);
        const c1 = palette[idx];
        const c2 = palette[nextIdx];
        const r = Math.round(c1[0] + t * (c2[0] - c1[0]));
        const g = Math.round(c1[1] + t * (c2[1] - c1[1]));
        const b = Math.round(c1[2] + t * (c2[2] - c1[2]));
        return `rgb(${r},${g},${b})`;
    }

    // Get a fixed color from palette by index (0..4)
    getFixedColor(index) {
        const i = Math.min(index, this.palette.length - 1);
        return `rgb(${this.palette[i].join(',')})`;
    }

    // Chart.js options with consistent styling – legend optional
    getChartOptions(title, xTitle, yTitle, isHorizontal = false, showLegend = false) {
        const options = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: showLegend, labels: { color: '#a0aec0' } },
                tooltip: {
                    backgroundColor: '#1a1d29',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: '#4a5568',
                    borderWidth: 1,
                    padding: 8,
                    bodyFont: { family: 'Inter, sans-serif', size: 12 },
                    titleFont: { family: 'Inter, sans-serif', size: 12, weight: 'bold' }
                }
            },
            scales: {
                x: {
                    title: { display: !!xTitle, text: xTitle, color: '#a0aec0' },
                    grid: isHorizontal ? { display: false } : { color: 'rgba(255,255,255,0.1)' },
                    ticks: { color: '#a0aec0', maxRotation: 0, minRotation: 0 }
                },
                y: {
                    title: { display: !!yTitle, text: yTitle, color: '#a0aec0' },
                    grid: isHorizontal ? { color: 'rgba(255,255,255,0.1)' } : { display: false },
                    ticks: { color: '#a0aec0' }
                }
            }
        };
        return options;
    }

    // ---------- Overview Tab ----------
    renderOverview() {
        const html = `
            <h2 class="section-header">Overview</h2>
            <div class="dashboard-disclaimer">
                <i class="fas fa-info-circle"></i> This dashboard shows data for <strong>bus routes only</strong>.
            </div>
            <div class="charts-grid">
                <div class="chart-container" id="chart-delay-distribution">
                    <div class="chart-header"><h3 class="chart-title">Delay Distribution</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Distribution of delays by duration categories (minutes)</div>
                </div>
                <div class="chart-container" id="chart-top-causes">
                    <div class="chart-header"><h3 class="chart-title">Top Incident Causes</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Most common reasons for delays (top 5)</div>
                </div>
            </div>
            <div class="charts-grid">
                <div class="chart-container" id="chart-weekly-patterns">
                    <div class="chart-header"><h3 class="chart-title">Weekly Delay Patterns</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Delay incidents by day of week</div>
                </div>
                <div class="chart-container" id="chart-yearly-trends">
                    <div class="chart-header"><h3 class="chart-title">Yearly Delay Trends</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Average delay per year from 2014 to 2025</div>
                </div>
            </div>
        `;
        this.contentInner.innerHTML = html;
        this.createDelayDistribution();
        this.createTopCauses();
        this.createWeeklyPatterns();
        this.createYearlyTrends();
    }

    createDelayDistribution() {
        const container = document.getElementById('chart-delay-distribution');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.delay_distribution;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-chart-bar', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const labels = data.map(d => String(d.range));
        const counts = data.map(d => d.count);
        const maxCount = Math.max(...counts);
        const colors = counts.map(v => this.getColor(v / maxCount));

        this.charts.delayDistribution = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: counts,
                    backgroundColor: colors,
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: this.getChartOptions('', 'Delay Range', 'Number of Delays')
        });
    }

    createTopCauses() {
        const container = document.getElementById('chart-top-causes');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.top_incident_causes;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-exclamation-circle', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const top5 = data.slice(0, 5);
        const labels = top5.map(d => String(d.incident_type));
        const counts = top5.map(d => d.count);
        const maxCount = Math.max(...counts);
        const colors = counts.map(v => this.getColor(v / maxCount));

        this.charts.topCauses = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: counts,
                    backgroundColor: colors,
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: this.getChartOptions('', 'Cause', 'Incident Count')
        });
    }

    createWeeklyPatterns() {
        const container = document.getElementById('chart-weekly-patterns');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.weekly_patterns;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-calendar-week', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
        const counts = weekdays.map((day, idx) => {
            const item = data.find(d => d.weekday === day || d.weekday === idx);
            return item ? item.incident_count : 0;
        });

        const color = this.getFixedColor(2);

        this.charts.weeklyPatterns = new Chart(ctx, {
            type: 'line',
            data: {
                labels: weekdays,
                datasets: [{
                    data: counts,
                    borderColor: color,
                    backgroundColor: this.hexToRgba(color, 0.1),
                    borderWidth: 3,
                    fill: true,
                    tension: 0.3,
                    pointBackgroundColor: color,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }]
            },
            options: this.getChartOptions('', 'Day of Week', 'Incident Count')
        });
    }

    createYearlyTrends() {
        const container = document.getElementById('chart-yearly-trends');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.yearly_trends;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-chart-line', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const labels = data.map(d => String(d.year));
        const avgDelays = data.map(d => d.avg_delay);
        const color = this.getFixedColor(3);

        this.charts.yearlyTrends = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    data: avgDelays,
                    borderColor: color,
                    backgroundColor: this.hexToRgba(color, 0.1),
                    borderWidth: 3,
                    fill: true,
                    tension: 0.3,
                    pointBackgroundColor: color,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }]
            },
            options: this.getChartOptions('', 'Year', 'Average Delay (min)')
        });
    }

    // ---------- Time Patterns Tab ----------
    renderTimePatterns() {
        const html = `
            <h2 class="section-header">Time Patterns</h2>
            <div class="dashboard-disclaimer">
                <i class="fas fa-info-circle"></i> This dashboard shows data for <strong>bus routes only</strong>.
            </div>
            <div class="charts-grid">
                <div class="chart-container" id="chart-hourly">
                    <div class="chart-header"><h3 class="chart-title">Hourly Delay Patterns</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Incident frequency and average delay by hour of day</div>
                </div>
                <div class="chart-container" id="chart-daily">
                    <div class="chart-header"><h3 class="chart-title">Daily Delay Patterns</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Hourly patterns on weekdays vs weekends</div>
                </div>
            </div>
            <div class="charts-grid">
                <div class="chart-container" id="chart-monthly">
                    <div class="chart-header"><h3 class="chart-title">Monthly Trends</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Average delay across months</div>
                </div>
                <div class="chart-container" id="chart-timeofday">
                    <div class="chart-header"><h3 class="chart-title">Time of Day Comparison</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Average delay by time period (morning, afternoon, etc.)</div>
                </div>
            </div>            
            <div class="chart-container full-width" id="chart-heatmap">
                <div class="chart-header"><h3 class="chart-title">Weekday vs Hour Heatmap</h3></div>
                <div class="chart-wrapper" style="height: 280px;"></div>
                <div class="chart-description">Heatmap of delay concentration by day and hour</div>
            </div>
            
        `;
        this.contentInner.innerHTML = html;
        this.createHourly();
        this.createDaily();
        this.createMonthly();
        this.createTimeOfDay();
        this.createMonthlyComparison();
        this.createHeatmap();
    }

    createHourly() {
        const container = document.getElementById('chart-hourly');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.hourly_frequency_delay;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-hourglass-half', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const hours = data.map(d => {
            const h = d.hour;
            if (h === 0) return '12 AM';
            if (h < 12) return `${h} AM`;
            if (h === 12) return '12 PM';
            return `${h-12} PM`;
        });

        const color1 = this.getFixedColor(1); // for bars
        const color2 = this.getFixedColor(4); // for line

        this.charts.hourly = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: hours,
                datasets: [
                    {
                        label: 'Incident Count',
                        data: data.map(d => d.incident_count),
                        backgroundColor: color1,
                        yAxisID: 'y',
                        order: 1
                    },
                    {
                        label: 'Average Delay (min)',
                        data: data.map(d => d.avg_delay),
                        type: 'line',
                        borderColor: color2,
                        backgroundColor: 'transparent',
                        borderWidth: 3,
                        pointBackgroundColor: color2,
                        pointBorderColor: color2,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        yAxisID: 'y1',
                        order: 0,
                        tension: 0.3,
                        fill: false
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { labels: { color: '#a0aec0' } },
                    tooltip: {
                        backgroundColor: '#1a1d29',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#4a5568',
                        borderWidth: 1,
                        padding: 8
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#a0aec0' },
                        title: { display: true, text: 'Incident Count', color: '#a0aec0' }
                    },
                    y1: {
                        beginAtZero: true,
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: { color: color2 },
                        title: { display: true, text: 'Avg Delay (min)', color: color2 }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: '#a0aec0', maxRotation: 0 }
                    }
                }
            }
        });
    }

    createDaily() {
        const container = document.getElementById('chart-daily');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.daily_patterns;
        if (!data || !data.weekday_aggregate || !data.weekend_aggregate) {
            this.showEmpty(container, 'fa-sun', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const hours = data.weekday_aggregate.map(d => {
            const h = d.hour;
            if (h === 0) return '12 AM';
            if (h < 12) return `${h} AM`;
            if (h === 12) return '12 PM';
            return `${h-12} PM`;
        });

        const color1 = this.getFixedColor(0);
        const color2 = this.getFixedColor(3);

        this.charts.daily = new Chart(ctx, {
            type: 'line',
            data: {
                labels: hours,
                datasets: [
                    {
                        label: 'Weekdays',
                        data: data.weekday_aggregate.map(d => d.incident_count),
                        borderColor: color1,
                        backgroundColor: this.hexToRgba(color1, 0.1),
                        borderWidth: 3,
                        fill: true,
                        tension: 0.2,
                        pointBackgroundColor: color1,
                        pointRadius: 3,
                        pointHoverRadius: 5
                    },
                    {
                        label: 'Weekends',
                        data: data.weekend_aggregate.map(d => d.incident_count),
                        borderColor: color2,
                        backgroundColor: this.hexToRgba(color2, 0.1),
                        borderWidth: 3,
                        fill: true,
                        tension: 0.2,
                        pointBackgroundColor: color2,
                        pointRadius: 3,
                        pointHoverRadius: 5
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#a0aec0' } },
                    tooltip: {
                        backgroundColor: '#1a1d29',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#4a5568',
                        borderWidth: 1,
                        padding: 8
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Hour of Day', color: '#a0aec0' },
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#a0aec0', maxRotation: 0 }
                    },
                    y: {
                        title: { display: true, text: 'Incident Count', color: '#a0aec0' },
                        grid: { display: false },
                        ticks: { color: '#a0aec0', beginAtZero: true }
                    }
                }
            }
        });
    }

    createMonthly() {
        const container = document.getElementById('chart-monthly');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.monthly_trends;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-calendar-alt', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const labels = data.map(d => monthNames[d.month - 1] || d.month);
        const avgDelays = data.map(d => d.avg_delay);
        const color = this.getFixedColor(2);

        this.charts.monthly = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    data: avgDelays,
                    borderColor: color,
                    backgroundColor: this.hexToRgba(color, 0.1),
                    borderWidth: 3,
                    fill: true,
                    tension: 0.3,
                    pointBackgroundColor: color,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }]
            },
            options: this.getChartOptions('', 'Month', 'Average Delay (min)')
        });
    }

    createTimeOfDay() {
        const container = document.getElementById('chart-timeofday');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.time_of_day_comparison;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-clock', 'No data');
            return;
        }

        // Sort periods in chronological order if a sort order is available
        // Assume data has a 'sort_order' field, or we define an order array
        const periodOrder = [
            'Early Morning', 'Morning Peak', 'Midday', 'Afternoon Peak', 
            'Evening', 'Late Night'
        ];
        const sorted = [...data].sort((a, b) => {
            const indexA = periodOrder.indexOf(a.period);
            const indexB = periodOrder.indexOf(b.period);
            if (indexA === -1 && indexB === -1) return 0;
            if (indexA === -1) return 1;
            if (indexB === -1) return -1;
            return indexA - indexB;
        });

        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const periods = sorted.map(d => d.period);
        const counts = sorted.map(d => d.incident_count);
        const maxCount = Math.max(...counts);
        const colors = counts.map(v => this.getColor(v / maxCount));

        this.charts.timeOfDay = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: periods,
                datasets: [{
                    data: counts,
                    backgroundColor: colors,
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: this.getChartOptions('', 'Time Period', 'Incident Count')
        });
    }

    createMonthlyComparison() {
        const container = document.getElementById('chart-monthly-comparison');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.monthly_comparison;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-calendar-check', 'No monthly comparison data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const months = data.map(d => d.month);
        const color1 = this.getFixedColor(0); // current year incidents
        const color2 = this.getFixedColor(1); // previous year incidents
        const color3 = this.getFixedColor(3); // current year avg delay
        const color4 = this.getFixedColor(4); // previous year avg delay

        this.charts.monthlyComparison = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: months,
                datasets: [
                    {
                        label: 'Current Year Incidents',
                        data: data.map(d => d.current_incident_count),
                        backgroundColor: color1,
                        yAxisID: 'y',
                        order: 1
                    },
                    {
                        label: 'Previous Year Incidents',
                        data: data.map(d => d.previous_incident_count),
                        backgroundColor: color2,
                        yAxisID: 'y',
                        order: 1
                    },
                    {
                        label: 'Current Year Avg Delay',
                        data: data.map(d => d.current_avg_delay),
                        type: 'line',
                        borderColor: color3,
                        backgroundColor: 'transparent',
                        borderWidth: 3,
                        pointBackgroundColor: color3,
                        pointBorderColor: color3,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        yAxisID: 'y1',
                        order: 0,
                        tension: 0.3,
                        fill: false
                    },
                    {
                        label: 'Previous Year Avg Delay',
                        data: data.map(d => d.previous_avg_delay),
                        type: 'line',
                        borderColor: color4,
                        backgroundColor: 'transparent',
                        borderWidth: 3,
                        borderDash: [5, 5],
                        pointBackgroundColor: color4,
                        pointBorderColor: color4,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        yAxisID: 'y1',
                        order: 0,
                        tension: 0.3,
                        fill: false
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { labels: { color: '#a0aec0' } },
                    tooltip: {
                        backgroundColor: '#1a1d29',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#4a5568',
                        borderWidth: 1,
                        padding: 8
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#a0aec0', callback: (v) => v.toLocaleString() },
                        title: { display: true, text: 'Incident Count', color: '#a0aec0' }
                    },
                    y1: {
                        beginAtZero: true,
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: { color: '#a0aec0' },
                        title: { display: true, text: 'Average Delay (minutes)', color: '#a0aec0' }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: '#a0aec0', maxRotation: 0 }
                    }
                }
            }
        });

        // --- Force the wrapper height (fix for min-height override) ---
        if (wrapper) {
            wrapper.style.height = '280px';
            wrapper.style.minHeight = 'unset';
        }
    }


    
    createHeatmap() {
        const container = document.getElementById('chart-heatmap');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.weekday_hour_heatmap;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-th-large', 'No data');
            return;
        }

        const weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
        const hours = Array.from({ length: 24 }, (_, i) => i);

        // Build 7x24 matrix
        const matrix = [];
        for (let d = 0; d < 7; d++) {
            matrix[d] = [];
            for (let h = 0; h < 24; h++) {
                const entry = data.find(item => item.weekday === weekdays[d] && item.hour === h);
                matrix[d][h] = entry ? entry.incident_count : 0;
            }
        }

        const maxVal = Math.max(...matrix.flat());

        let gridHtml = '<div class="heatmap-grid" style="display: grid; grid-template-columns: 100px repeat(24, 1fr); gap: 2px; padding: 10px; border-radius: 8px;">';
        // Header row (hours) – smaller font
        gridHtml += '<div style="color: #a0aec0; font-weight: bold; text-align: right; padding-right: 10px; font-size: 11px;"></div>';
        for (let h = 0; h < 24; h++) {
            let hourLabel = h === 0 ? '12 AM' : h < 12 ? `${h} AM` : h === 12 ? '12 PM' : `${h-12} PM`;
            gridHtml += `<div style="color: #a0aec0; font-weight: bold; text-align: center; font-size: 11px;">${hourLabel}</div>`;
        }

        // Data rows – day labels also smaller
        for (let d = 0; d < 7; d++) {
            gridHtml += `<div style="color: #a0aec0; font-weight: bold; text-align: right; padding-right: 10px; font-size: 11px;">${weekdays[d]}</div>`;
            for (let h = 0; h < 24; h++) {
                const val = matrix[d][h];
                const ratio = maxVal > 0 ? val / maxVal : 0;
                const bgColor = this.getColor(ratio);
                const textColor = ratio > 0.6 ? '#ffffff' : '#000000';
                gridHtml += `<div style="background-color: ${bgColor}; color: ${textColor}; text-align: center; padding: 8px 2px; font-size: 11px; border-radius: 4px;" title="${weekdays[d]} ${h}:00 - ${val.toLocaleString()} incidents">${this.formatNumber(val)}</div>`;
            }
        }
        gridHtml += '</div>';

        wrapper.innerHTML = gridHtml;
    }
    // ---------- Route Analysis Tab ----------
    renderRouteAnalysis() {
        const html = `
            <h2 class="section-header">Route Analysis</h2>
            <div class="dashboard-disclaimer">
                <i class="fas fa-info-circle"></i> This dashboard shows data for <strong>bus routes only</strong>.
            </div>
            <div class="charts-grid">
                <div class="chart-container" id="chart-top-incidents">
                    <div class="chart-header"><h3 class="chart-title">Top Delayed Routes by Incidents</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Routes with highest number of delay incidents (top 10)</div>
                </div>
                <div class="chart-container" id="chart-top-avg">
                    <div class="chart-header"><h3 class="chart-title">Top Routes by Average Delay</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Routes with longest average delay (top 10)</div>
                </div>
            </div>
            <div class="charts-grid">
                <div class="chart-container" id="chart-scatter">
                    <div class="chart-header"><h3 class="chart-title">Route Frequency vs Severity</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Incident frequency vs average delay per route</div>
                </div>
                <div class="chart-container" id="chart-improving">
                    <div class="chart-header"><h3 class="chart-title">Top Improving vs Declining Routes (2025)</h3></div>
                    <div class="chart-wrapper" style="height: 280px;"></div>
                    <div class="chart-description">Routes that improved or declined most in 2025 compared to 2024</div>
                </div>
            </div>
        `;
        this.contentInner.innerHTML = html;
        this.createTopIncidents();
        this.createTopAvgDelay();
        this.createScatter();
        this.createImprovingDeclining();
    }

    createTopIncidents() {
        const container = document.getElementById('chart-top-incidents');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.top_delayed_routes_count;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-list-ol', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const top10 = data.slice(0, 10);
        const labels = top10.map(r => {
            const name = r.route_name || `Route ${r.route_number}`;
            return name.length > 20 ? name.substring(0, 20) + '…' : name;
        });
        const incidents = top10.map(r => r.incident_count);
        const maxInc = Math.max(...incidents);
        const colors = incidents.map(v => this.getColor(v / maxInc));

        this.charts.topIncidents = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: incidents,
                    backgroundColor: colors,
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: {
                indexAxis: 'y',
                ...this.getChartOptions('', 'Incident Count', 'Route', true)
            }
        });
    }

    createTopAvgDelay() {
        const container = document.getElementById('chart-top-avg');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.top_delayed_routes; // using avg delay data
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-clock', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const top10 = data.slice(0, 10);
        const labels = top10.map(r => {
            const name = r.route_name || `Route ${r.route_number}`;
            return name.length > 20 ? name.substring(0, 20) + '…' : name;
        });
        const delays = top10.map(r => r.avg_delay);
        const maxDelay = Math.max(...delays);
        const colors = delays.map(v => this.getColor(v / maxDelay));

        this.charts.topAvg = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: delays,
                    backgroundColor: colors,
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: {
                indexAxis: 'y',
                ...this.getChartOptions('', 'Average Delay (min)', 'Route', true)
            }
        });
    }

    createScatter() {
        const container = document.getElementById('chart-scatter');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');
        const data = this.data.route_scatter_data;
        if (!data || !data.length) {
            this.showEmpty(container, 'fa-crosshairs', 'No data');
            return;
        }
        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const maxDelay = Math.max(...data.map(r => r.avg_delay));
        const points = data.map(r => ({
            x: r.incident_count,
            y: r.avg_delay,
            route: r.route_number
        }));
        const colors = data.map(r => this.getColor(r.avg_delay / maxDelay));

        this.charts.scatter = new Chart(ctx, {
            type: 'scatter',
            data: {
                datasets: [{
                    data: points,
                    backgroundColor: colors,
                    pointRadius: 5,
                    pointHoverRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: '#1a1d29',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#4a5568',
                        borderWidth: 1,
                        padding: 8,
                        callbacks: {
                            label: (ctx) => {
                                const p = ctx.raw;
                                return `Route ${p.route}: Incidents: ${p.x.toLocaleString()}, Avg Delay: ${p.y.toFixed(1)} min`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'logarithmic',
                        title: { display: true, text: 'Incident Count (log)', color: '#a0aec0' },
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#a0aec0', callback: (v) => v.toLocaleString() }
                    },
                    y: {
                        title: { display: true, text: 'Average Delay (min)', color: '#a0aec0' },
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#a0aec0' }
                    }
                }
            }
        });
    }

    createImprovingDeclining() {
        const container = document.getElementById('chart-improving');
        if (!container) return;
        const wrapper = container.querySelector('.chart-wrapper');

        const improving = this.data.improving_routes || [];
        const declining = this.data.declining_routes || [];

        const topImproving = improving.slice(0, 5);
        const topDeclining = declining.slice(0, 5);
        const all = [...topImproving, ...topDeclining];

        if (all.length === 0) {
            this.showEmpty(container, 'fa-exchange-alt', 'No improving/declining data');
            return;
        }

        wrapper.innerHTML = '<canvas></canvas>';
        const ctx = wrapper.querySelector('canvas').getContext('2d');

        const labels = all.map(r => {
            const name = r.route_name || `Route ${r.route_number}`;
            return name.length > 20 ? name.substring(0, 20) + '…' : name;
        });
        const changes = all.map(r => r.percent_change);
        const colors = changes.map(p => p < 0 ? this.getFixedColor(0) : this.getFixedColor(4));

        this.charts.improvingDeclining = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: changes,
                    backgroundColor: colors,
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: '#1a1d29',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#4a5568',
                        borderWidth: 1,
                        padding: 8,
                        callbacks: {
                            label: (ctx) => {
                                const val = ctx.raw;
                                const direction = val < 0 ? 'improved' : 'declined';
                                return `${Math.abs(val).toFixed(1)}% ${direction}`;
                            }
                        }
                    },
                    annotation: {
                        annotations: {
                            line0: {
                                type: 'line',
                                yMin: 0,
                                yMax: 0,
                                borderColor: '#a0aec0',
                                borderWidth: 1,
                                label: { enabled: false }
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        title: { display: true, text: 'Route', color: '#a0aec0' },
                        grid: { color: 'rgba(255,255,255,0.1)' },
                        ticks: { color: '#a0aec0', maxRotation: 0 }
                    },
                    y: {
                        title: { display: true, text: 'Change (%)', color: '#a0aec0' },
                        grid: { display: false },
                        ticks: { color: '#a0aec0' }
                    }
                }
            }
        });
    }

    // ---------- Utility ----------
    hexToRgba(rgbStr, alpha) {
        if (rgbStr.startsWith('rgb')) {
            return rgbStr.replace('rgb', 'rgba').replace(')', `, ${alpha})`);
        }
        return rgbStr;
    }

    resizeCharts() {
        if (window.Plotly) {
            document.querySelectorAll('.chart-wrapper .js-plotly-plot').forEach(plot => {
                Plotly.Plots.resize(plot);
            });
        }
    }

    showError(message) {
        this.contentInner.innerHTML = `<div class="error-message">${message}</div>`;
    }
}