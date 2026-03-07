// js/dashboard.js
// Dashboard Modal – redesigned as a popup overlay with two tabs: Trends and Performance

class DashboardController {
    constructor(app) {
        this.app = app;
        this.data = {
            weekly_patterns: null,
            yearly_trends: null,
            hourly_frequency_delay: null,
            top_incident_causes: null,
            route_performance: null
        };
        this.charts = {};
        this.modal = null;
        this.isVisible = false;
        this.loaded = false;
        this.currentTab = 'trends'; // 'trends' or 'performance'

        // Year filter for Trends tab
        this.yearFilter = 'all'; // 'all', 'last1', 'last2', 'last5', 'custom'
        this.customYear = null;     // will be set to latest year when data loads
        this.availableYears = [];   // populated from data

        // Sort state for performance tab
        this.sortColumn = 'avg_delay_2025';
        this.sortDirection = 'desc';

        // Light palette (light blue → deep purple) – same as before
        this.palette = [
            [152, 212, 226], // #98D4E2
            [106, 147, 193], // #6A93C1
            [83, 99, 159],   // #53639F
            [108, 56, 127],  // #6C387F
            [66, 20, 79]     // #42144F
        ];

        // Create modal structure immediately
        this.initModal();
    }

    // -----------------------------------------------------------------
    // Modal Creation
    // -----------------------------------------------------------------
    initModal() {
        // Create overlay
        const overlay = document.createElement('div');
        overlay.id = 'dashboardModalOverlay';
        overlay.className = 'dashboard-modal-overlay';
        overlay.style.display = 'none'; // hidden initially

        // Create modal container
        const modal = document.createElement('div');
        modal.className = 'dashboard-modal';

        // Close button
        const closeBtn = document.createElement('button');
        closeBtn.className = 'dashboard-modal-close';
        closeBtn.innerHTML = '&times;';
        closeBtn.setAttribute('aria-label', 'Close dashboard');
        closeBtn.addEventListener('click', () => this.closeModal());

        // Title
        const title = document.createElement('h2');
        title.className = 'dashboard-modal-title';
        title.textContent = 'TTC Delay Analysis';

        // Tab bar (floating island style)
        const tabBar = document.createElement('div');
        tabBar.className = 'dashboard-modal-tabbar';
        tabBar.innerHTML = `
            <button class="dashboard-tab-item active" data-tab="trends">Trends</button>
            <button class="dashboard-tab-item" data-tab="performance">Performance</button>
        `;

        // Content containers
        const trendsContent = document.createElement('div');
        trendsContent.id = 'modal-trends-content';
        trendsContent.className = 'dashboard-modal-tabcontent active';

        const performanceContent = document.createElement('div');
        performanceContent.id = 'modal-performance-content';
        performanceContent.className = 'dashboard-modal-tabcontent';

        // Add filter bar to trends content
        const filterBar = document.createElement('div');
        filterBar.className = 'trends-filter-bar';
        filterBar.innerHTML = `
            <label>Time Range:</label>
            <div class="filter-options">
                <label><input type="radio" name="yearFilter" value="all" checked> All Years</label>
                <label><input type="radio" name="yearFilter" value="last1"> Last 1 Year</label>
                <label><input type="radio" name="yearFilter" value="last2"> Last 2 Years</label>
                <label><input type="radio" name="yearFilter" value="last5"> Last 5 Years</label>
                <label><input type="radio" name="yearFilter" value="custom"> Custom Year</label>
                <select id="custom-year-select" disabled>
                    <option>Select year</option>
                </select>
            </div>
        `;
        trendsContent.appendChild(filterBar);

        // Grid for trends (four charts)
        const grid = document.createElement('div');
        grid.className = 'dashboard-modal-grid';

        const containers = [
            { id: 'modal-weekly', title: 'Weekly Delay Patterns' },
            { id: 'modal-yearly', title: 'Yearly Avg Delay Trends' },
            { id: 'modal-hourly', title: 'Hourly Delay Patterns' },
            { id: 'modal-causes', title: 'Top Incident Causes' }
        ];

        containers.forEach(c => {
            const card = document.createElement('div');
            card.className = 'dashboard-modal-card';

            const header = document.createElement('div');
            header.className = 'dashboard-modal-card-header';
            header.innerHTML = `<h3>${c.title}</h3>`;

            const wrapper = document.createElement('div');
            wrapper.className = 'dashboard-modal-card-wrapper';
            wrapper.id = c.id;

            card.appendChild(header);
            card.appendChild(wrapper);
            grid.appendChild(card);
        });

        trendsContent.appendChild(grid);

        // Performance content will be populated dynamically
        performanceContent.innerHTML = `
            <div class="performance-sheet-title">2025 Performance Sheet</div>
            <div class="performance-controls">
                <label>Sort by:</label>
                <select id="performance-sort-select">
                    <option value="avg_delay_2025" selected>Avg Delay</option>
                    <option value="total_incidents_2025">Total Incidents</option>
                    <option value="reliability_score">Reliability Score</option>
                    <option value="avg_delay_change_2024_2025_pct">Improvement (%)</option>
                </select>
                <button id="performance-sort-direction" class="sort-direction-btn">
                    <i class="fas fa-arrow-down"></i> Desc
                </button>
            </div>
            <div class="performance-table-container">
                <table class="performance-table" id="performance-table">
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Route</th>
                            <th>Route Name</th>
                            <th>Common Incident</th>
                            <th>Total Incidents</th>
                            <th>Avg Delay</th>
                            <th>Reliability Score</th>
                            <th>Improvement (%)</th>
                        </tr>
                    </thead>
                    <tbody id="performance-table-body">
                        <tr><td colspan="8">Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        `;

        // Assemble modal
        modal.appendChild(closeBtn);
        modal.appendChild(title);
        modal.appendChild(tabBar);
        modal.appendChild(trendsContent);
        modal.appendChild(performanceContent);
        overlay.appendChild(modal);

        // Append to body
        document.body.appendChild(overlay);

        // Store references
        this.modal = overlay;
        this.tabBar = tabBar;
        this.trendsContent = trendsContent;
        this.performanceContent = performanceContent;
        this.filterBar = filterBar;
        this.grid = grid;

        // Attach tab switching
        tabBar.querySelectorAll('.dashboard-tab-item').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const tab = e.target.dataset.tab;
                this.switchTab(tab);
            });
        });
    }

    // -----------------------------------------------------------------
    // Tab Switching
    // -----------------------------------------------------------------
    switchTab(tab) {
        if (this.currentTab === tab) return;
        this.currentTab = tab;

        // Update tab buttons
        this.tabBar.querySelectorAll('.dashboard-tab-item').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.tab === tab);
        });

        // Update content visibility
        this.trendsContent.classList.toggle('active', tab === 'trends');
        this.performanceContent.classList.toggle('active', tab === 'performance');

        // If switching to performance and data loaded, render
        if (tab === 'performance' && this.loaded) {
            this.renderPerformanceTab();
            this.attachPerformanceListeners();
        }
    }

    // -----------------------------------------------------------------
    // Data Loading
    // -----------------------------------------------------------------
    async loadData() {
        if (this.loaded) return;
        console.log('📊 Loading dashboard modal data...');
        try {
            // Load both trends and performance datasets
            const datasets = [
                'weekly_patterns',
                'yearly_trends',
                'hourly_frequency_delay',
                'top_incident_causes',
                'route_performance'
            ];

            const loadPromises = datasets.map(name => this.loadDataset(name));
            const results = await Promise.all(loadPromises);

            datasets.forEach((name, index) => {
                if (results[index]) this.data[name] = results[index];
                else console.warn(`⚠️ ${name} not loaded`);
            });

            // Extract available years from yearly_trends
            if (this.data.yearly_trends && this.data.yearly_trends.length) {
                this.availableYears = this.data.yearly_trends.map(d => d.year).sort((a,b) => b - a);
                this.customYear = this.availableYears[0]; // latest year
            }

            this.loaded = true;
            console.log('✅ Dashboard modal data loaded', this.data);
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

    // -----------------------------------------------------------------
    // Modal Control
    // -----------------------------------------------------------------
    async openModal() {
        if (!this.loaded) {
            await this.loadData();
        }
        this.modal.style.display = 'flex';
        this.isVisible = true;

        // Populate custom year dropdown
        this.populateYearDropdown();

        // Attach filter listeners
        this.attachFilterListeners();

        // Render trends charts with current filter (all years by default)
        this.renderTrendsCharts();

        // If performance tab is active, render it
        if (this.currentTab === 'performance') {
            this.renderPerformanceTab();
            this.attachPerformanceListeners();
        }

        // Slight delay to ensure containers are visible before chart init
        setTimeout(() => this.resizeCharts(), 100);
    }

    closeModal() {
        this.modal.style.display = 'none';
        this.isVisible = false;
    }

    // -----------------------------------------------------------------
    // Year Filter Helpers
    // -----------------------------------------------------------------
    populateYearDropdown() {
        const select = document.getElementById('custom-year-select');
        if (!select) return;
        select.innerHTML = '';
        this.availableYears.forEach(year => {
            const option = document.createElement('option');
            option.value = year;
            option.textContent = year;
            if (year === this.customYear) option.selected = true;
            select.appendChild(option);
        });
    }

    attachFilterListeners() {
        const radios = this.filterBar.querySelectorAll('input[name="yearFilter"]');
        const customSelect = document.getElementById('custom-year-select');

        radios.forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.yearFilter = e.target.value;
                if (this.yearFilter === 'custom') {
                    customSelect.disabled = false;
                } else {
                    customSelect.disabled = true;
                }
                this.renderTrendsCharts();
            });
        });

        if (customSelect) {
            customSelect.addEventListener('change', (e) => {
                this.customYear = parseInt(e.target.value, 10);
                if (this.yearFilter === 'custom') {
                    this.renderTrendsCharts();
                }
            });
        }
    }

    // Get list of years to include based on current filter
    getSelectedYears() {
        if (!this.availableYears.length) return [];

        const maxYear = Math.max(...this.availableYears);

        switch (this.yearFilter) {
            case 'all':
                return this.availableYears;
            case 'last1':
                return [maxYear];
            case 'last2':
                return this.availableYears.filter(y => y >= maxYear - 1);
            case 'last5':
                return this.availableYears.filter(y => y >= maxYear - 4);
            case 'custom':
                return this.customYear ? [this.customYear] : [maxYear];
            default:
                return this.availableYears;
        }
    }

    // -----------------------------------------------------------------
    // Filtered Data Aggregation
    // -----------------------------------------------------------------
    getFilteredWeeklyData(years) {
        const allData = this.data.weekly_patterns || [];
        const selected = allData.filter(item => years.includes(item.year));
        if (!selected.length) return [];

        const weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
        const result = weekdays.map(weekday => {
            let totalIncidents = 0;
            let totalDelayMin = 0;
            selected.forEach(yearItem => {
                const dayData = yearItem.weekly.find(w => w.weekday === weekday);
                if (dayData) {
                    totalIncidents += dayData.incident_count;
                    totalDelayMin += dayData.incident_count * dayData.avg_delay; // sum of minutes
                }
            });
            const avgDelay = totalIncidents > 0 ? totalDelayMin / totalIncidents : 0;
            return {
                weekday,
                incident_count: totalIncidents,
                avg_delay: Math.round(avgDelay * 10) / 10
            };
        });
        return result;
    }

    getFilteredHourlyData(years) {
        const allData = this.data.hourly_frequency_delay || [];
        const selected = allData.filter(item => years.includes(item.year));
        if (!selected.length) return [];

        const result = [];
        for (let h = 0; h < 24; h++) {
            let totalIncidents = 0;
            let totalDelayMin = 0;
            selected.forEach(yearItem => {
                const hourData = yearItem.hourly_data.find(d => d.hour === h);
                if (hourData) {
                    totalIncidents += hourData.incident_count;
                    totalDelayMin += hourData.incident_count * hourData.avg_delay;
                }
            });
            const avgDelay = totalIncidents > 0 ? totalDelayMin / totalIncidents : 0;
            result.push({
                hour: h,
                incident_count: totalIncidents,
                avg_delay: Math.round(avgDelay * 10) / 10
            });
        }
        return result;
    }

    getFilteredTopCauses(years, topN = 5) {
        const allData = this.data.top_incident_causes || [];
        const selected = allData.filter(item => years.includes(item.year));
        if (!selected.length) return [];

        // Aggregate counts across years
        const causeMap = new Map();
        selected.forEach(yearItem => {
            yearItem.causes.forEach(cause => {
                const key = cause.incident_type;
                causeMap.set(key, (causeMap.get(key) || 0) + cause.count);
            });
        });

        // Convert to array, sort, take topN
        const aggregated = Array.from(causeMap.entries()).map(([incident_type, count]) => ({
            incident_type,
            count
        }));
        aggregated.sort((a, b) => b.count - a.count);
        return aggregated.slice(0, topN);
    }

    getFilteredYearlyData(years) {
        const allData = this.data.yearly_trends || [];
        return allData.filter(item => years.includes(item.year)).sort((a,b) => a.year - b.year);
    }

    // -----------------------------------------------------------------
    // Trends Charts Rendering (with filtering)
    // -----------------------------------------------------------------
    renderTrendsCharts() {
        const years = this.getSelectedYears();
        if (!years.length) {
            this.showEmpty(document.getElementById('modal-weekly'), 'No data for selected period');
            this.showEmpty(document.getElementById('modal-yearly'), 'No data for selected period');
            this.showEmpty(document.getElementById('modal-hourly'), 'No data for selected period');
            this.showEmpty(document.getElementById('modal-causes'), 'No data for selected period');
            return;
        }

        this.createWeeklyPatterns(years);
        this.createYearlyTrends(years);
        this.createHourly(years);
        this.createTopCauses(years);
    }

    createWeeklyPatterns(years) {
        const container = document.getElementById('modal-weekly');
        if (!container) return;
        const data = this.getFilteredWeeklyData(years);
        if (!data.length) {
            this.showEmpty(container, 'No weekly data');
            return;
        }

        container.innerHTML = '<canvas></canvas>';
        const canvas = container.querySelector('canvas');
        const ctx = canvas.getContext('2d');

        const weekdays = data.map(d => d.weekday);
        const counts = data.map(d => d.incident_count);
        const color = this.getFixedColor(2);

        this.charts.weekly = new Chart(ctx, {
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

    createYearlyTrends(years) {
        const container = document.getElementById('modal-yearly');
        if (!container) return;
        const data = this.getFilteredYearlyData(years);
        if (!data.length) {
            this.showEmpty(container, 'No yearly data');
            return;
        }

        container.innerHTML = '<canvas></canvas>';
        const canvas = container.querySelector('canvas');
        const ctx = canvas.getContext('2d');

        const labels = data.map(d => String(d.year));
        const avgDelays = data.map(d => d.avg_delay);
        const color = this.getFixedColor(3);

        this.charts.yearly = new Chart(ctx, {
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

    createHourly(years) {
        const container = document.getElementById('modal-hourly');
        if (!container) return;
        const data = this.getFilteredHourlyData(years);
        if (!data.length) {
            this.showEmpty(container, 'No hourly data');
            return;
        }

        container.innerHTML = '<canvas></canvas>';
        const canvas = container.querySelector('canvas');
        const ctx = canvas.getContext('2d');

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
                        ticks: { color: '#a0aec0', maxRotation: 45 }
                    }
                }
            }
        });
    }

    createTopCauses(years) {
        const container = document.getElementById('modal-causes');
        if (!container) return;
        const data = this.getFilteredTopCauses(years, 5);
        if (!data.length) {
            this.showEmpty(container, 'No cause data');
            return;
        }

        container.innerHTML = '<canvas></canvas>';
        const canvas = container.querySelector('canvas');
        const ctx = canvas.getContext('2d');

        const labels = data.map(d => String(d.incident_type));
        const counts = data.map(d => d.count);
        const maxCount = Math.max(...counts);
        const colors = counts.map(v => this.getColor(v / maxCount));

        this.charts.causes = new Chart(ctx, {
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

    // -----------------------------------------------------------------
    // Performance Tab Rendering (with colored arrows based on sign)
    // -----------------------------------------------------------------
    renderPerformanceTab() {
        const data = this.data.route_performance;
        if (!data || !data.length) {
            document.getElementById('performance-table-body').innerHTML = '<tr><td colspan="8">No data available</td></tr>';
            return;
        }

        // Sync sort column with dropdown
        const select = document.getElementById('performance-sort-select');
        if (select) {
            this.sortColumn = select.value;
        }

        // Filter: avg_delay_2025 not null and > 1, route_name not null
        let filtered = data.filter(item => 
            item.avg_delay_2025 != null && 
            item.avg_delay_2025 > 1 && 
            item.route_name != null && 
            item.route_name.trim() !== ''
        );

        // Sort
        filtered.sort((a, b) => {
            let valA = a[this.sortColumn];
            let valB = b[this.sortColumn];
            if (valA == null) valA = this.sortDirection === 'desc' ? -Infinity : Infinity;
            if (valB == null) valB = this.sortDirection === 'desc' ? -Infinity : Infinity;
            if (this.sortDirection === 'desc') {
                return valB - valA;
            } else {
                return valA - valB;
            }
        });

        // Build table rows
        let html = '';
        filtered.forEach((item, index) => {
            const rank = index + 1;
            let rankDisplay;
            if (rank === 1) rankDisplay = '🥇';
            else if (rank === 2) rankDisplay = '🥈';
            else if (rank === 3) rankDisplay = '🥉';
            else rankDisplay = `<span class="rank-number">${rank}</span>`;

            // Format improvement with arrow and sign-based color
            let improvementHtml = '—';
            if (item.avg_delay_change_2024_2025_pct != null) {
                const change = item.avg_delay_change_2024_2025_pct;
                const isNegative = change < 0;
                // Red for negative, green for positive (as requested)
                const color = isNegative ? '#f56565' : '#48bb78';
                const arrow = isNegative ? '↓' : '↑';
                // Show signed percentage (e.g., "-6.7" or "6.7")
                const signedValue = change.toFixed(1);
                improvementHtml = `<span style="color: ${color};">${arrow} ${signedValue}%</span>`;
            }

            const reliability = item.reliability_score != null ? item.reliability_score.toFixed(1) : '—';
            const totalInc = item.total_incidents_2025 != null ? item.total_incidents_2025.toLocaleString() : '—';
            const avgDelay = item.avg_delay_2025 != null ? item.avg_delay_2025.toFixed(1) + ' min' : '—';

            html += `<tr>
                <td>${rankDisplay}</td>
                <td>${item.route || '—'}</td>
                <td>${item.route_name || '—'}</td>
                <td>${item.most_common_incident_cause || '—'}</td>
                <td>${totalInc}</td>
                <td>${avgDelay}</td>
                <td>${reliability}</td>
                <td>${improvementHtml}</td>
            </tr>`;
        });

        document.getElementById('performance-table-body').innerHTML = html;
    }

    attachPerformanceListeners() {
        const select = document.getElementById('performance-sort-select');
        const dirBtn = document.getElementById('performance-sort-direction');
        if (!select || !dirBtn) return;

        const newSelect = select.cloneNode(true);
        select.parentNode.replaceChild(newSelect, select);
        const newDirBtn = dirBtn.cloneNode(true);
        dirBtn.parentNode.replaceChild(newDirBtn, dirBtn);

        newSelect.addEventListener('change', (e) => {
            this.sortColumn = e.target.value;
            this.renderPerformanceTab();
        });

        newDirBtn.addEventListener('click', () => {
            this.sortDirection = this.sortDirection === 'desc' ? 'asc' : 'desc';
            newDirBtn.innerHTML = this.sortDirection === 'desc' 
                ? '<i class="fas fa-arrow-down"></i> Desc' 
                : '<i class="fas fa-arrow-up"></i> Asc';
            this.renderPerformanceTab();
        });
    }

    // -----------------------------------------------------------------
    // Helper Methods
    // -----------------------------------------------------------------
    showEmpty(container, message) {
        if (!container) return;
        container.innerHTML = `<div class="dashboard-modal-empty"><i class="fas fa-chart-bar"></i><p>${message}</p></div>`;
    }

    showError(message) {
        console.error(message);
        if (this.modal) {
            const grid = this.modal.querySelector('.dashboard-modal-grid');
            if (grid) grid.innerHTML = `<div class="dashboard-modal-error">${message}</div>`;
        }
    }

    getChartOptions(title, xTitle, yTitle) {
        return {
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
                    bodyFont: { family: 'Inter, sans-serif', size: 12 },
                    titleFont: { family: 'Inter, sans-serif', size: 12, weight: 'bold' }
                }
            },
            scales: {
                x: {
                    title: { display: !!xTitle, text: xTitle, color: '#a0aec0' },
                    grid: { color: 'rgba(255,255,255,0.1)' },
                    ticks: { color: '#a0aec0', maxRotation: 0 }
                },
                y: {
                    title: { display: !!yTitle, text: yTitle, color: '#a0aec0' },
                    grid: { display: false },
                    ticks: { color: '#a0aec0', beginAtZero: true }
                }
            }
        };
    }

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

    getFixedColor(index) {
        const i = Math.min(index, this.palette.length - 1);
        return `rgb(${this.palette[i].join(',')})`;
    }

    hexToRgba(rgbStr, alpha) {
        if (rgbStr.startsWith('rgb')) {
            return rgbStr.replace('rgb', 'rgba').replace(')', `, ${alpha})`);
        }
        return rgbStr;
    }

    resizeCharts() {
        Object.values(this.charts).forEach(chart => {
            if (chart && chart.update) chart.update();
        });
    }
}