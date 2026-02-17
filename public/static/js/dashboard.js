let charts = {};
let allRecords = [];
let allAppsData = [];
let showingAllApps = false;
let currentPage = 1;
let totalPages = 1;
let perPage = 25;
let currentTheme = localStorage.getItem('theme') || 'dark';
let currentSort = { field: 'created', dir: 'desc' };

// Auto-refresh state
let autoRefreshEnabled = localStorage.getItem('autoRefresh') === 'true';
let autoRefreshInterval = 15000; // 15 seconds
let autoRefreshTimer = null;

// Colorful palette for Top Applications chart
const appBarColors = [
  '#3b82f6', '#f97316', '#22c55e', '#a855f7', '#ef4444',
  '#22d3ee', '#eab308', '#ec4899', '#84cc16', '#6366f1',
  '#14b8a6', '#f43f5e', '#8b5cf6', '#10b981', '#06b6d4',
  '#d946ef', '#facc15', '#2dd4bf'
];

// Apply saved theme on load
if (currentTheme === 'light') {
  document.documentElement.setAttribute('data-theme', 'light');
  document.getElementById('themeIcon').textContent = '☀️';
}

// Fetch GitHub stars
async function fetchGitHubStars() {
  try {
    const resp = await fetch('https://api.github.com/repos/community-scripts/ProxmoxVE');
    const data = await resp.json();
    if (data.stargazers_count) {
      document.getElementById('starCount').textContent = data.stargazers_count.toLocaleString();
    }
  } catch (e) {
    console.log('Could not fetch GitHub stars');
  }
}
fetchGitHubStars();

function toggleTheme() {
  if (currentTheme === 'dark') {
    document.documentElement.setAttribute('data-theme', 'light');
    document.getElementById('themeIcon').textContent = '☀️';
    currentTheme = 'light';
  } else {
    document.documentElement.removeAttribute('data-theme');
    document.getElementById('themeIcon').textContent = '🌙';
    currentTheme = 'dark';
  }
  localStorage.setItem('theme', currentTheme);
  if (Object.keys(charts).length > 0) {
    refreshData();
  }
}

function handleGlobalSearch(event) {
  if (event.key === 'Enter') {
    const query = event.target.value.trim();
    if (query) {
      document.getElementById('filterApp').value = query;
      filterTable();
      document.querySelector('.section-card:last-of-type').scrollIntoView({ behavior: 'smooth' });
    }
  }
}

// Keyboard shortcut for search
document.addEventListener('keydown', function(e) {
  if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
    e.preventDefault();
    document.getElementById('globalSearch').focus();
  }
});

const chartDefaults = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      labels: { color: '#8b949e' }
    }
  },
  scales: {
    x: {
      ticks: { color: '#8b949e' },
      grid: { color: '#2d3748' }
    },
    y: {
      ticks: { color: '#8b949e' },
      grid: { color: '#2d3748' }
    }
  }
};

async function fetchData() {
  const activeBtn = document.querySelector('.filter-btn.active');
  const days = activeBtn ? activeBtn.dataset.days : '1';
  const repo = document.querySelector('.source-btn.active')?.dataset.repo || 'ProxmoxVE';

  // Show loading indicator
  document.getElementById('loadingIndicator').style.display = 'flex';
  document.getElementById('cacheStatus').textContent = '';

  try {
    // Add cache-busting timestamp for filter changes to ensure fresh data
    const cacheBuster = '&_t=' + Date.now();
    const response = await fetch('/api/dashboard?days=' + days + '&repo=' + repo + cacheBuster);
    if (!response.ok) throw new Error('Failed to fetch data');

    // Check cache status from header
    const cacheHit = response.headers.get('X-Cache') === 'HIT';
    document.getElementById('cacheStatus').textContent = cacheHit ? '(cached)' : '(fresh)';

    return await response.json();
  } catch (error) {
    document.getElementById('error').style.display = 'flex';
    document.getElementById('errorText').textContent = error.message;
    throw error;
  } finally {
    document.getElementById('loadingIndicator').style.display = 'none';
  }
}

function updateStats(data) {
  // Use total_all_time for display if available, otherwise total_installs
  const displayTotal = data.total_all_time || data.total_installs;
  document.getElementById('totalInstalls').textContent = displayTotal.toLocaleString();

  // Failed count (separate card)
  document.getElementById('failedCount').textContent = (data.failed_count || 0).toLocaleString();
  document.getElementById('failedSubtitle').textContent = data.failed_count > 0 ? 'installation failures' : 'no failures';

  // Aborted count (separate card)
  document.getElementById('abortedCount').textContent = (data.aborted_count || 0).toLocaleString();

  document.getElementById('successRate').textContent = data.success_rate.toFixed(1) + '%';
  document.getElementById('successSubtitle').textContent = data.success_count.toLocaleString() + ' successful installations';
  document.getElementById('lastUpdated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  document.getElementById('error').style.display = 'none';

  // Most Popular - update podium
  function formatCompact(n) {
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'k';
    return n.toString();
  }
  if (data.top_apps && data.top_apps.length >= 3) {
    document.getElementById('podium1App').textContent = data.top_apps[0].app;
    document.getElementById('podium1Count').textContent = formatCompact(data.top_apps[0].count);
    document.getElementById('podium2App').textContent = data.top_apps[1].app;
    document.getElementById('podium2Count').textContent = formatCompact(data.top_apps[1].count);
    document.getElementById('podium3App').textContent = data.top_apps[2].app;
    document.getElementById('podium3Count').textContent = formatCompact(data.top_apps[2].count);
  } else if (data.top_apps && data.top_apps.length > 0) {
    document.getElementById('podium1App').textContent = data.top_apps[0].app;
    document.getElementById('podium1Count').textContent = formatCompact(data.top_apps[0].count);
  }

  // Store all apps data for View All feature
  allAppsData = data.top_apps || [];

  // Error Analysis
  updateErrorAnalysis(data.error_analysis || []);

  // Failed Apps
  updateFailedApps(data.failed_apps || []);
}

function updateErrorAnalysis(errors) {
  const container = document.getElementById('errorList');
  if (!errors || errors.length === 0) {
    container.innerHTML = '<div style="padding: 20px; color: var(--text-muted); text-align: center; font-size: 13px;">No errors recorded</div>';
    return;
  }
  container.innerHTML = errors.slice(0, 6).map(e =>
    '<div class="error-item">' +
    '<div style="min-width:0;flex:1;">' +
    '<div class="pattern">' + escapeHtml(e.pattern) + '</div>' +
    '<div class="meta">' + e.unique_apps + ' app' + (e.unique_apps !== 1 ? 's' : '') + ' affected</div>' +
    '</div>' +
    '<span class="count-badge">' + e.count.toLocaleString() + 'x</span>' +
    '</div>'
  ).join('');
}

function updateFailedApps(apps) {
  const container = document.getElementById('failedAppsGrid');
  const activeDays = parseInt(document.querySelector('.filter-btn.active')?.dataset.days || '1');
  let minInstalls = 10;
  if (activeDays <= 1) minInstalls = 5;
  else if (activeDays <= 7) minInstalls = 15;
  else if (activeDays <= 30) minInstalls = 40;
  else if (activeDays <= 90) minInstalls = 100;
  else minInstalls = 100;
  document.getElementById('failedAppsThreshold').textContent = '(min. ' + minInstalls + ' installs)';
  if (!apps || apps.length === 0) {
    container.innerHTML = '<div style="padding: 20px; color: var(--text-muted); text-align: center; font-size: 13px;">Not enough data (min. ' + minInstalls + ' installs)</div>';
    return;
  }
  container.innerHTML = apps.slice(0, 8).map(a => {
    const typeClass = (a.type || '').toLowerCase();
    const typeBadge = a.type && a.type !== 'unknown' ? '<span class="type-badge ' + typeClass + '">' + a.type.toUpperCase() + '</span>' : '';
    const rate = a.failure_rate;
    const severityClass = rate >= 30 ? 'critical' : rate >= 15 ? 'warning' : 'moderate';
    return '<div class="failed-app-card">' +
      '<div class="app-info">' + typeBadge + '<span class="app-name">' + escapeHtml(a.app) + '</span>' +
      '<span class="details">' + a.failed_count + '/' + a.total_count + '</span>' +
      '</div>' +
      '<span class="failure-rate ' + severityClass + '">' + rate.toFixed(1) + '%</span>' +
      '</div>';
  }).join('');
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function formatTimestamp(ts) {
  if (!ts) return '-';
  const d = new Date(ts);
  // Format: "Feb 11, 2026, 4:33 PM"
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  });
}

function initSortableHeaders() {
  document.querySelectorAll('th.sortable').forEach(th => {
    th.style.cursor = 'pointer';
    th.addEventListener('click', () => sortByColumn(th.dataset.sort));
  });
}

function sortByColumn(field) {
  if (currentSort.field === field) {
    currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
  } else {
    currentSort.field = field;
    currentSort.dir = 'desc';
  }

  document.querySelectorAll('th.sortable').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    th.textContent = th.textContent.replace(/[▲▼]/g, '').trim();
  });

  const activeTh = document.querySelector('th[data-sort=\"' + field + '\"]');
  if (activeTh) {
    activeTh.classList.add(currentSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
    activeTh.textContent += ' ' + (currentSort.dir === 'asc' ? '▲' : '▼');
  }

  currentPage = 1;
  fetchPaginatedRecords();
}

function toggleAllApps() {
  showingAllApps = !showingAllApps;
  const btn = document.getElementById('viewAllAppsBtn');
  const container = document.getElementById('appsChartContainer');

  if (showingAllApps) {
    btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg> Show Less';
    container.style.height = '600px';
  } else {
    btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg> View All';
    container.style.height = '420px';
  }

  updateAppsChart(allAppsData);
}

function updateAppsChart(topApps) {
  const displayApps = showingAllApps ? topApps.slice(0, 30) : topApps.slice(0, 15);
  const colors = displayApps.map((_, i) => appBarColors[i % appBarColors.length]);

  if (charts.apps) charts.apps.destroy();
  charts.apps = new Chart(document.getElementById('appsChart'), {
    type: 'bar',
    data: {
      labels: displayApps.map(a => a.app),
      datasets: [{
        label: 'Installations',
        data: displayApps.map(a => a.count),
        backgroundColor: colors,
        borderRadius: 6,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'x',
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(21, 27, 35, 0.95)',
          titleColor: '#e2e8f0',
          bodyColor: '#e2e8f0',
          borderColor: '#2d3748',
          borderWidth: 1,
          padding: 12,
          displayColors: true,
          callbacks: {
            label: function(ctx) {
              return ctx.parsed.y.toLocaleString() + ' installations';
            }
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#8b949e',
            maxRotation: 45,
            minRotation: 45
          },
          grid: { display: false }
        },
        y: {
          beginAtZero: true,
          ticks: {
            color: '#8b949e',
            callback: function(value) {
              if (value >= 1000) return (value / 1000).toFixed(0) + 'k';
              return value;
            }
          },
          grid: { color: '#2d3748' }
        }
      }
    }
  });
}

function updateCharts(data) {
  // Daily chart
  if (charts.daily) charts.daily.destroy();
  charts.daily = new Chart(document.getElementById('dailyChart'), {
    type: 'line',
    data: {
      labels: data.daily_stats.map(d => d.date.slice(5)),
      datasets: [
        {
          label: 'Success',
          data: data.daily_stats.map(d => d.success),
          borderColor: '#22c55e',
          backgroundColor: 'rgba(34, 197, 94, 0.1)',
          fill: true,
          tension: 0.4,
          borderWidth: 2
        },
        {
          label: 'Failed',
          data: data.daily_stats.map(d => d.failed),
          borderColor: '#ef4444',
          backgroundColor: 'rgba(239, 68, 68, 0.1)',
          fill: true,
          tension: 0.4,
          borderWidth: 2
        }
      ]
    },
    options: {
      ...chartDefaults,
      plugins: { legend: { display: true, position: 'top', labels: { color: '#8b949e', usePointStyle: true } } }
    }
  });

  // OS distribution - horizontal bar chart
  if (charts.os) charts.os.destroy();
  charts.os = new Chart(document.getElementById('osChart'), {
    type: 'bar',
    data: {
      labels: data.os_distribution.map(o => o.os),
      datasets: [{
        data: data.os_distribution.map(o => o.count),
        backgroundColor: appBarColors.slice(0, data.os_distribution.length),
        borderRadius: 4,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              return ctx.parsed.x.toLocaleString() + ' installations';
            }
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#8b949e',
            callback: function(v) { return v >= 1000 ? (v / 1000).toFixed(0) + 'k' : v; }
          },
          grid: { color: '#2d3748' }
        },
        y: {
          ticks: { color: '#8b949e' },
          grid: { display: false }
        }
      }
    }
  });

  // Status pie chart
  if (charts.status) charts.status.destroy();
  charts.status = new Chart(document.getElementById('statusChart'), {
    type: 'doughnut',
    data: {
      labels: ['Success', 'Failed', 'Installing'],
      datasets: [{
        data: [data.success_count, data.failed_count, data.installing_count],
        backgroundColor: ['#22c55e', '#ef4444', '#eab308'],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { color: '#8b949e', padding: 12 } }
      }
    }
  });

  // Top apps chart
  updateAppsChart(data.top_apps || []);
}

function updateTable(records) {
  allRecords = records || [];

  // Populate OS filter
  const osFilter = document.getElementById('filterOs');
  const uniqueOs = [...new Set(allRecords.map(r => r.os_type).filter(Boolean))];
  osFilter.innerHTML = '<option value="">All OS</option>' +
    uniqueOs.map(os => '<option value="' + os + '">' + os + '</option>').join('');

  filterTable();
}

function changePerPage() {
  perPage = parseInt(document.getElementById('perPageSelect').value);
  currentPage = 1;
  fetchPaginatedRecords();
}

async function fetchPaginatedRecords() {
  const status = document.getElementById('filterStatus').value;
  const app = document.getElementById('filterApp').value;
  const os = document.getElementById('filterOs').value;
  const type = document.getElementById('filterType').value;

  try {
    const activeBtn = document.querySelector('.filter-btn.active');
    const days = activeBtn ? activeBtn.dataset.days : '1';
    const repo = document.querySelector('.source-btn.active')?.dataset.repo || 'ProxmoxVE';

    let url = '/api/records?page=' + currentPage + '&limit=' + perPage + '&days=' + days + '&repo=' + encodeURIComponent(repo);
    if (status) url += '&status=' + encodeURIComponent(status);
    if (app) url += '&app=' + encodeURIComponent(app);
    if (os) url += '&os=' + encodeURIComponent(os);
    if (type) url += '&type=' + encodeURIComponent(type);
    if (currentSort.field) {
      url += '&sort=' + (currentSort.dir === 'desc' ? '-' : '') + currentSort.field;
    }

    const response = await fetch(url);
    if (!response.ok) throw new Error('Failed to fetch records');
    const data = await response.json();

    totalPages = data.total_pages || 1;
    document.getElementById('pageInfo').textContent = 'Page ' + currentPage + ' of ' + totalPages + ' (' + data.total + ' total)';
    document.getElementById('prevBtn').disabled = currentPage <= 1;
    document.getElementById('nextBtn').disabled = currentPage >= totalPages;

    renderTableRows(data.records || []);
  } catch (e) {
    console.error('Pagination error:', e);
  }
}

function prevPage() {
  if (currentPage > 1) {
    currentPage--;
    fetchPaginatedRecords();
  }
}

function nextPage() {
  if (currentPage < totalPages) {
    currentPage++;
    fetchPaginatedRecords();
  }
}

// Store current records for detail view
let currentRecords = [];

function renderTableRows(records) {
  const tbody = document.getElementById('recordsTable');
  currentRecords = records;

  if (records.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8"><div class="loading" style="padding: 40px;">No records found</div></td></tr>';
    return;
  }

  tbody.innerHTML = records.map((r, index) => {
    const statusClass = r.status || 'unknown';
    const typeClass = (r.type || '').toLowerCase();
    const diskSize = r.disk_size ? r.disk_size + 'GB' : '-';
    const coreCount = r.core_count || '-';
    const ramSize = r.ram_size ? r.ram_size + 'MB' : '-';
    const created = r.created ? formatTimestamp(r.created) : '-';
    const osDisplay = r.os_type ? (r.os_type + (r.os_version ? ' ' + r.os_version : '')) : '-';

    return '<tr class="clickable-row" onclick="showRecordDetail(' + index + ')">' +
      '<td><span class="status-badge ' + statusClass + '">' + escapeHtml(r.status || 'unknown') + '</span></td>' +
      '<td><span class="type-badge ' + typeClass + '">' + escapeHtml((r.type || '-').toUpperCase()) + '</span></td>' +
      '<td><strong>' + escapeHtml(r.nsapp || '-') + '</strong></td>' +
      '<td>' + escapeHtml(osDisplay) + '</td>' +
      '<td>' + diskSize + '</td>' +
      '<td style="text-align: center;">' + coreCount + '</td>' +
      '<td>' + ramSize + '</td>' +
      '<td>' + created + '</td>' +
      '</tr>';
  }).join('');
}

function showRecordDetail(index) {
  const record = currentRecords[index];
  if (!record) return;

  const modal = document.getElementById('detailModal');
  const modalTitle = document.getElementById('modalTitle').querySelector('span');
  const modalBody = document.getElementById('modalBody');

  modalTitle.textContent = record.nsapp || 'Record Details';

  // Build detail content with sections
  let html = '';

  // General Information Section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg> General Information</div>';
  html += '<div class="detail-grid">';
  html += buildDetailItem('App Name', record.nsapp);
  html += buildDetailItem('Status', record.status, 'status-' + (record.status || 'unknown'));
  html += buildDetailItem('Type', formatType(record.type));
  html += buildDetailItem('Method', record.method || 'default');
  html += buildDetailItem('Random ID', record.random_id, 'mono');
  html += '</div></div>';

  // System Resources Section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2" ry="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg> System Resources</div>';
  html += '<div class="detail-grid">';
  html += buildDetailItem('CPU Cores', record.core_count ? record.core_count + ' Cores' : null);
  html += buildDetailItem('RAM', record.ram_size ? formatBytes(record.ram_size * 1024 * 1024) : null);
  html += buildDetailItem('Disk Size', record.disk_size ? record.disk_size + ' GB' : null);
  html += buildDetailItem('CT Type', record.ct_type !== undefined ? (record.ct_type === 1 ? 'Unprivileged' : 'Privileged') : null);
  html += '</div></div>';

  // Operating System Section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg> Operating System</div>';
  html += '<div class="detail-grid">';
  html += buildDetailItem('OS Type', record.os_type);
  html += buildDetailItem('OS Version', record.os_version);
  html += buildDetailItem('PVE Version', record.pve_version);
  html += '</div></div>';

  // Hardware Section (CPU & GPU)
  const hasHardwareInfo = record.cpu_vendor || record.cpu_model || record.gpu_vendor || record.gpu_model || record.ram_speed;
  if (hasHardwareInfo) {
    html += '<div class="detail-section">';
    html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg> Hardware</div>';
    html += '<div class="detail-grid">';
    html += buildDetailItem('CPU Vendor', record.cpu_vendor);
    html += buildDetailItem('CPU Model', record.cpu_model);
    html += buildDetailItem('RAM Speed', record.ram_speed);
    html += buildDetailItem('GPU Vendor', record.gpu_vendor);
    html += buildDetailItem('GPU Model', record.gpu_model);
    html += buildDetailItem('GPU Passthrough', formatPassthrough(record.gpu_passthrough));
    html += '</div></div>';
  }

  // Installation Details Section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Installation</div>';
  html += '<div class="detail-grid">';
  html += buildDetailItem('Exit Code', record.exit_code !== undefined ? record.exit_code : null, record.exit_code === 0 ? 'status-success' : (record.exit_code ? 'status-failed' : ''));
  html += buildDetailItem('Duration', record.install_duration ? formatDuration(record.install_duration) : null);
  html += buildDetailItem('Error Category', record.error_category);
  html += '</div></div>';

  // Error Section (if present)
  if (record.error) {
    html += '<div class="detail-section">';
    html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg> Error Details</div>';
    html += '<div class="error-box">' + escapeHtml(record.error) + '</div>';
    html += '</div>';
  }

  // Timestamps Section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-header"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg> Timestamps</div>';
  html += '<div class="detail-grid">';
  html += buildDetailItem('Created', formatFullTimestamp(record.created));
  html += buildDetailItem('Updated', formatFullTimestamp(record.updated));
  html += '</div></div>';

  modalBody.innerHTML = html;
  modal.classList.add('active');
  document.body.style.overflow = 'hidden';
}

function buildDetailItem(label, value, extraClass) {
  if (value === null || value === undefined || value === '') {
    return '<div class="detail-item"><div class="label">' + escapeHtml(label) + '</div><div class="value" style="color: var(--text-secondary);">—</div></div>';
  }
  const valueClass = extraClass ? 'value ' + extraClass : 'value';
  return '<div class="detail-item"><div class="label">' + escapeHtml(label) + '</div><div class="' + valueClass + '">' + escapeHtml(String(value)) + '</div></div>';
}

function formatType(type) {
  if (!type) return null;
  const types = {
    'lxc': 'LXC Container',
    'vm': 'Virtual Machine',
    'addon': 'Add-on',
    'pve': 'Proxmox VE',
    'tool': 'Tool'
  };
  return types[type.toLowerCase()] || type;
}

function formatPassthrough(pt) {
  if (!pt) return null;
  const modes = {
    'igpu': 'Integrated GPU',
    'dgpu': 'Dedicated GPU',
    'vgpu': 'Virtual GPU',
    'none': 'None',
    'unknown': 'Unknown'
  };
  return modes[pt.toLowerCase()] || pt;
}

function formatBytes(bytes) {
  if (!bytes) return null;
  const gb = bytes / (1024 * 1024 * 1024);
  if (gb >= 1) return gb.toFixed(1) + ' GB';
  const mb = bytes / (1024 * 1024);
  return mb.toFixed(0) + ' MB';
}

function formatDuration(seconds) {
  if (!seconds) return null;
  if (seconds < 60) return seconds + 's';
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins < 60) return mins + 'm ' + secs + 's';
  const hours = Math.floor(mins / 60);
  const remainMins = mins % 60;
  return hours + 'h ' + remainMins + 'm';
}

function formatFullTimestamp(ts) {
  if (!ts) return null;
  const d = new Date(ts);
  return d.toLocaleDateString() + ' ' + d.toLocaleTimeString();
}

function closeModal() {
  const modal = document.getElementById('detailModal');
  modal.classList.remove('active');
  document.body.style.overflow = '';
}

function closeModalOutside(event) {
  if (event.target === document.getElementById('detailModal')) {
    closeModal();
  }
}

// Close modal with Escape key
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    closeModal();
    closeHealthModal();
  }
});

function filterTable() {
  currentPage = 1;
  fetchPaginatedRecords();
}

function exportCSV() {
  if (allRecords.length === 0) {
    alert('No data to export');
    return;
  }

  const headers = ['App', 'Status', 'OS Type', 'OS Version', 'Type', 'Method', 'Cores', 'RAM (MB)', 'Disk (GB)', 'Exit Code', 'Error', 'PVE Version'];
  const rows = allRecords.map(r => [
    r.nsapp || '',
    r.status || '',
    r.os_type || '',
    r.os_version || '',
    r.type || '',
    r.method || '',
    r.core_count || '',
    r.ram_size || '',
    r.disk_size || '',
    r.exit_code || '',
    (r.error || '').replace(/,/g, ';'),
    r.pve_version || ''
  ]);

  const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'telemetry_' + new Date().toISOString().slice(0, 10) + '.csv';
  a.click();
  URL.revokeObjectURL(url);
}

async function showHealthCheck() {
  const modal = document.getElementById('healthModal');
  const body = document.getElementById('healthModalBody');
  body.innerHTML = '<div class="loading">Checking...</div>';
  modal.classList.add('active');
  document.body.style.overflow = 'hidden';

  try {
    const resp = await fetch('/healthz');
    const data = await resp.json();

    const isOk = data.status === 'ok';
    const statusClass = isOk ? 'ok' : 'error';
    const icon = isOk ? '✅' : '❌';
    const title = isOk ? 'All Systems Operational' : 'Service Degraded';

    let html = '<div class="health-status ' + statusClass + '">';
    html += '<span class="icon">' + icon + '</span>';
    html += '<div class="details">';
    html += '<div class="title">' + title + '</div>';
    html += '<div class="subtitle">Last checked: ' + new Date().toLocaleTimeString() + '</div>';
    html += '</div></div>';

    html += '<div class="health-info">';
    html += '<div><span>Status</span><span>' + data.status + '</span></div>';
    html += '<div><span>Server Time</span><span>' + new Date(data.time).toLocaleString() + '</span></div>';
    if (data.pocketbase) {
      html += '<div><span>PocketBase</span><span>' + (data.pocketbase === 'connected' ? '🟢 Connected' : '🔴 ' + data.pocketbase) + '</span></div>';
    }
    if (data.version) {
      html += '<div><span>Version</span><span>' + data.version + '</span></div>';
    }
    html += '</div>';

    body.innerHTML = html;
  } catch (e) {
    body.innerHTML = '<div class="health-status error"><span class="icon">❌</span><div class="details"><div class="title">Connection Failed</div><div class="subtitle">' + e.message + '</div></div></div>';
  }
}

function closeHealthModal(event) {
  if (event && event.target !== document.getElementById('healthModal')) return;
  document.getElementById('healthModal').classList.remove('active');
  document.body.style.overflow = '';
}

async function refreshData() {
  try {
    const data = await fetchData();
    updateStats(data);
    updateCharts(data);
    // Refresh paginated Installation Log with current filters (NOT from cached recent_records)
    currentPage = 1;
    fetchPaginatedRecords();
  } catch (e) {
    console.error(e);
  }
}

// Initial load
refreshData();
initSortableHeaders();

// Source button clicks
document.querySelectorAll('.source-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.source-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    refreshData();
  });
});

// Quickfilter button clicks
document.querySelectorAll('.filter-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    refreshData();
  });
});

// Auto-refresh functionality
function toggleAutoRefresh() {
  autoRefreshEnabled = document.getElementById('autoRefreshToggle').checked;
  localStorage.setItem('autoRefresh', autoRefreshEnabled);

  const intervalDisplay = document.getElementById('refreshInterval');

  if (autoRefreshEnabled) {
    intervalDisplay.classList.add('active');
    startAutoRefresh();
  } else {
    intervalDisplay.classList.remove('active');
    stopAutoRefresh();
  }
}

function startAutoRefresh() {
  stopAutoRefresh(); // Clear any existing timer

  let countdown = autoRefreshInterval / 1000;
  const intervalDisplay = document.getElementById('refreshInterval');

  // Update countdown display
  const countdownTimer = setInterval(() => {
    countdown--;
    if (countdown <= 0) {
      countdown = autoRefreshInterval / 1000;
    }
    intervalDisplay.textContent = countdown + 's';
  }, 1000);

  // Actual refresh
  autoRefreshTimer = setInterval(() => {
    refreshData();
    countdown = autoRefreshInterval / 1000;
  }, autoRefreshInterval);

  // Store countdown timer for cleanup
  autoRefreshTimer.countdownTimer = countdownTimer;
}

function stopAutoRefresh() {
  if (autoRefreshTimer) {
    clearInterval(autoRefreshTimer);
    if (autoRefreshTimer.countdownTimer) {
      clearInterval(autoRefreshTimer.countdownTimer);
    }
    autoRefreshTimer = null;
  }
  document.getElementById('refreshInterval').textContent = '15s';
}


// Initialize auto-refresh state on load
document.getElementById('autoRefreshToggle').checked = autoRefreshEnabled;
if (autoRefreshEnabled) {
  document.getElementById('refreshInterval').classList.add('active');
  startAutoRefresh();
}
