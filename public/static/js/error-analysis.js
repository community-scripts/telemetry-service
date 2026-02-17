let charts = {};
let currentData = null;
let currentTheme = localStorage.getItem('theme') || 'dark';
if (currentTheme === 'light') document.documentElement.setAttribute('data-theme', 'light');

const catColors = {
  'apt': '#ef4444', 'network': '#3b82f6', 'permission': '#f97316',
  'command_not_found': '#a855f7', 'user_aborted': '#64748b', 'timeout': '#eab308',
  'storage': '#ec4899', 'resource': '#f97316', 'dependency': '#22d3ee',
  'signal': '#eab308', 'config': '#84cc16', 'unknown': '#64748b',
  'uncategorized': '#94a3b8'
};

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function escapeAttr(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/\n/g, '&#10;').replace(/\r/g, '&#13;');
}

function toggleError(id) {
  var s = document.getElementById(id + '-short');
  var f = document.getElementById(id + '-full');
  if (f && s) {
    if (f.style.display === 'none') { f.style.display = 'block'; s.style.display = 'none'; }
    else { f.style.display = 'none'; s.style.display = 'block'; }
  }
}

function formatTimestamp(ts) {
  if (!ts) return '-';
  return new Date(ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true });
}

async function fetchData() {
  const days = document.querySelector('.filter-btn.active')?.dataset.days || '1';
  const repo = document.querySelector('.source-btn.active')?.dataset.repo || 'ProxmoxVE';
  try {
    const resp = await fetch('/api/errors?days=' + days + '&repo=' + repo);
    if (!resp.ok) throw new Error('Fetch failed');
    return await resp.json();
  } catch (e) {
    console.error(e);
    return null;
  }
}

function updateStats(data) {
  document.getElementById('totalErrors').textContent = (data.total_errors || 0).toLocaleString();
  document.getElementById('failRate').textContent = (data.overall_fail_rate || 0).toFixed(1) + '%';
  document.getElementById('stuckCount2').textContent = (data.stuck_installing || 0).toLocaleString();
  document.getElementById('totalInstalls').textContent = (data.total_installs || 0).toLocaleString();

  // Stuck banner
  if (data.stuck_installing > 0) {
    document.getElementById('stuckBanner').style.display = 'flex';
    document.getElementById('stuckCount').textContent = data.stuck_installing;
  } else {
    document.getElementById('stuckBanner').style.display = 'none';
  }
}

function updateExitCodeTable(exitCodes) {
  const tbody = document.getElementById('exitCodeTable');
  if (!exitCodes || exitCodes.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px;">No exit code data</td></tr>';
    return;
  }
  const maxCount = Math.max(...exitCodes.map(e => e.count));
  tbody.innerHTML = exitCodes.map(e => {
    const barWidth = (e.count / maxCount * 100).toFixed(0);
    const codeClass = e.exit_code === 0 ? 'ok' : 'err';
    const catClass = (e.category || 'unknown').replace(/ /g, '_');
    return '<tr>' +
      '<td><span class="exit-code ' + codeClass + '">' + e.exit_code + '</span></td>' +
      '<td>' + escapeHtml(e.description) + '</td>' +
      '<td><span class="category-badge ' + catClass + '">' + escapeHtml(e.category) + '</span></td>' +
      '<td><strong>' + e.count.toLocaleString() + '</strong></td>' +
      '<td>' + e.percentage.toFixed(1) + '%</td>' +
      '<td style="min-width:150px;"><div class="progress-bar"><div class="progress-bar-fill" style="width:' + barWidth + '%;background:var(--accent-red);"></div></div></td>' +
      '</tr>';
  }).join('');
}

function updateCategoryTable(categories) {
  const tbody = document.getElementById('categoryTable');
  if (!categories || categories.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:24px;">No category data</td></tr>';
    return;
  }
  tbody.innerHTML = categories.map(c => {
    const catClass = (c.category || 'unknown').replace(/ /g, '_');
    return '<tr>' +
      '<td><span class="category-badge ' + catClass + '">' + escapeHtml(c.category) + '</span></td>' +
      '<td><strong>' + c.count.toLocaleString() + '</strong></td>' +
      '<td>' + c.percentage.toFixed(1) + '%</td>' +
      '<td style="font-size:12px;color:var(--text-secondary);max-width:400px;overflow:hidden;text-overflow:ellipsis;">' + escapeHtml(c.top_apps) + '</td>' +
      '</tr>';
  }).join('');
}

let allAppErrors = [];
function updateAppErrorTable(apps) {
  allAppErrors = apps || [];
  filterAppTable();
}

function filterAppTable() {
  const filter = (document.getElementById('appFilter').value || '').toLowerCase();
  const filtered = filter ? allAppErrors.filter(a => a.app.toLowerCase().includes(filter)) : allAppErrors;
  const tbody = document.getElementById('appErrorTable');
  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px;">No matching apps</td></tr>';
    return;
  }
  tbody.innerHTML = filtered.map((a, idx) => {
    const typeClass = (a.type || '').toLowerCase();
    const failRateColor = a.failure_rate > 50 ? 'var(--accent-red)' : a.failure_rate > 20 ? 'var(--accent-orange)' : 'var(--accent-yellow)';
    const topCat = a.top_category ? '<span class="category-badge ' + a.top_category + '">' + escapeHtml(a.top_category) + '</span>' : '-';
    const errorId = 'err-app-' + idx;
    const shortError = escapeHtml((a.top_error || '-').substring(0, 120));
    const fullError = escapeHtml(a.top_error || '-');
    const isLong = (a.top_error || '').length > 120;
    return '<tr>' +
      '<td><strong>' + escapeHtml(a.app) + '</strong></td>' +
      '<td><span class="type-badge ' + typeClass + '">' + (a.type || '-').toUpperCase() + '</span></td>' +
      '<td>' + a.total_count + '</td>' +
      '<td style="color:var(--accent-red);font-weight:600;">' + a.failed_count + '</td>' +
      '<td style="color:var(--accent-purple);">' + (a.aborted_count || 0) + '</td>' +
      '<td style="color:' + failRateColor + ';font-weight:600;">' + a.failure_rate.toFixed(1) + '%</td>' +
      '<td>' + (a.top_exit_code ? '<span class="exit-code err">' + a.top_exit_code + '</span>' : '-') + '</td>' +
      '<td class="error-text">' +
      '<div id="' + errorId + '-short">' + shortError + (isLong ? ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show more</a>' : '') + '</div>' +
      (isLong ? '<div id="' + errorId + '-full" style="display:none;white-space:pre-wrap;word-break:break-all;max-height:600px;overflow-y:auto;">' + fullError + ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show less</a></div>' : '') +
      '</td>' +
      '<td><button class="btn issue-btn" data-app="' + escapeAttr(a.app) + '" data-exit="' + (a.top_exit_code || 0) + '" data-error="' + escapeAttr(a.top_error || '') + '" data-rate="' + a.failure_rate.toFixed(1) + '">🐛 Issue</button></td>' +
      '</tr>';
  }).join('');
}

function updateRecentErrors(errors) {
  const tbody = document.getElementById('recentErrorTable');
  if (!errors || errors.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px;">No recent errors</td></tr>';
    return;
  }
  tbody.innerHTML = errors.map((e, idx) => {
    const statusClass = e.status || 'unknown';
    const typeClass = (e.type || '').toLowerCase();
    const codeClass = e.exit_code === 0 ? 'ok' : 'err';
    const catClass = (e.error_category || 'unknown').replace(/ /g, '_');
    const os = e.os_type ? e.os_type + (e.os_version ? ' ' + e.os_version : '') : '-';
    const errorId = 'err-recent-' + idx;
    const shortError = escapeHtml((e.error || '-').substring(0, 120));
    const fullError = escapeHtml(e.error || '-');
    const isLong = (e.error || '').length > 120;
    return '<tr>' +
      '<td><span class="status-badge ' + statusClass + '">' + escapeHtml(e.status) + '</span></td>' +
      '<td><span class="type-badge ' + typeClass + '">' + (e.type || '-').toUpperCase() + '</span></td>' +
      '<td><strong>' + escapeHtml(e.nsapp) + '</strong></td>' +
      '<td><span class="exit-code ' + codeClass + '">' + e.exit_code + '</span></td>' +
      '<td><span class="category-badge ' + catClass + '">' + escapeHtml(e.error_category || 'unknown') + '</span></td>' +
      '<td class="error-text">' +
      '<div id="' + errorId + '-short">' + shortError + (isLong ? ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show more</a>' : '') + '</div>' +
      (isLong ? '<div id="' + errorId + '-full" style="display:none;white-space:pre-wrap;word-break:break-all;max-height:600px;overflow-y:auto;">' + fullError + ' <a href="#" onclick="toggleError(\'' + errorId + '\');return false;" style="color:var(--accent-blue);font-size:11px;">show less</a></div>' : '') +
      '</td>' +
      '<td>' + escapeHtml(os) + '</td>' +
      '<td style="white-space:nowrap;">' + formatTimestamp(e.created) + '</td>' +
      '<td><button class="btn issue-btn" data-app="' + escapeAttr(e.nsapp) + '" data-exit="' + e.exit_code + '" data-error="' + escapeAttr(e.error || '') + '" data-rate="0">🐛</button></td>' +
      '</tr>';
  }).join('');
}

function updateCharts(data) {
  // Timeline chart
  if (charts.timeline) charts.timeline.destroy();
  const timeline = data.error_timeline || [];
  charts.timeline = new Chart(document.getElementById('timelineChart'), {
    type: 'line',
    data: {
      labels: timeline.map(d => d.date.slice(5)),
      datasets: [
        { label: 'Failed', data: timeline.map(d => d.failed), borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.1)', fill: true, tension: 0.4, borderWidth: 2 },
        { label: 'Aborted', data: timeline.map(d => d.aborted), borderColor: '#a855f7', backgroundColor: 'rgba(168,85,247,0.1)', fill: true, tension: 0.4, borderWidth: 2 }
      ]
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#8b949e', usePointStyle: true } } }, scales: { x: { ticks: { color: '#8b949e' }, grid: { color: '#2d3748' } }, y: { ticks: { color: '#8b949e' }, grid: { color: '#2d3748' } } } }
  });

  // Category pie chart
  if (charts.category) charts.category.destroy();
  const cats = data.category_stats || [];
  charts.category = new Chart(document.getElementById('categoryChart'), {
    type: 'doughnut',
    data: {
      labels: cats.map(c => c.category),
      datasets: [{ data: cats.map(c => c.count), backgroundColor: cats.map(c => catColors[c.category] || '#64748b'), borderWidth: 0 }]
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { color: '#8b949e', padding: 12 } } } }
  });
}

// GitHub Issue Modal
function openIssueModal(app, exitCode, errorText, failRate) {
  const title = '[Telemetry] ' + app + ': Error (exit code ' + exitCode + ')';
  const fence = String.fromCharCode(96, 96, 96);
  const body = '## Telemetry Error Report\n\n' +
    '**Application:** ' + app + '\n' +
    '**Exit Code:** ' + exitCode + '\n' +
    '**Failure Rate:** ' + failRate + '%\n\n' +
    '### Error Details\n' + fence + '\n' + errorText + '\n' + fence + '\n\n' +
    '---\n*Created from telemetry error analysis dashboard.*';
  document.getElementById('issueTitle').value = title;
  document.getElementById('issueBody').value = body;
  document.getElementById('issueAlert').style.display = 'none';
  document.getElementById('issueModal').classList.add('active');
}

function closeIssueModal() {
  document.getElementById('issueModal').classList.remove('active');
}

async function submitIssue() {
  const btn = document.getElementById('submitIssueBtn');
  const alert = document.getElementById('issueAlert');
  const password = document.getElementById('issuePassword').value;
  if (!password) { alert.className = 'alert-box error'; alert.textContent = 'Password required'; alert.style.display = 'block'; return; }

  btn.disabled = true;
  btn.textContent = 'Creating...';

  try {
    const resp = await fetch('/api/github/create-issue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        password: password,
        title: document.getElementById('issueTitle').value,
        body: document.getElementById('issueBody').value,
        labels: document.getElementById('issueLabels').value.split(',').map(l => l.trim()).filter(Boolean)
      })
    });
    const data = await resp.json();
    if (resp.ok && data.success) {
      alert.className = 'alert-box success';
      alert.innerHTML = '✅ Issue created! <a href="' + data.issue_url + '" target="_blank" style="color:var(--accent-green);">View on GitHub →</a>';
      alert.style.display = 'block';
    } else {
      throw new Error(data.error || data.message || resp.statusText || 'Failed');
    }
  } catch (e) {
    alert.className = 'alert-box error';
    alert.textContent = '❌ ' + e.message;
    alert.style.display = 'block';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Create Issue';
  }
}

// Cleanup Modal
function triggerCleanup() {
  document.getElementById('cleanupAlert').style.display = 'none';
  document.getElementById('cleanupModal').classList.add('active');
}

function closeCleanupModal() {
  document.getElementById('cleanupModal').classList.remove('active');
}

async function runCleanup() {
  const btn = document.getElementById('runCleanupBtn');
  const alert = document.getElementById('cleanupAlert');
  const password = document.getElementById('cleanupPassword').value;
  if (!password) { alert.className = 'alert-box error'; alert.textContent = 'Password required'; alert.style.display = 'block'; return; }

  btn.disabled = true;
  btn.textContent = 'Running...';

  try {
    const resp = await fetch('/api/cleanup/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: password })
    });
    const data = await resp.json();
    if (resp.ok) {
      alert.className = 'alert-box success';
      alert.textContent = '✅ ' + data.message;
      alert.style.display = 'block';
      setTimeout(() => { closeCleanupModal(); refreshData(); }, 2000);
    } else {
      throw new Error(data.message || resp.statusText || 'Failed');
    }
  } catch (e) {
    alert.className = 'alert-box error';
    alert.textContent = '❌ ' + e.message;
    alert.style.display = 'block';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Run Cleanup';
  }
}

async function refreshData() {
  const data = await fetchData();
  if (!data) return;
  currentData = data;
  updateStats(data);
  updateExitCodeTable(data.exit_code_stats);
  updateCategoryTable(data.category_stats);
  updateAppErrorTable(data.app_errors);
  updateRecentErrors(data.recent_errors);
  updateCharts(data);
}

// Filter button handling
document.querySelectorAll('.filter-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    refreshData();
  });
});
document.querySelectorAll('.source-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.source-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    refreshData();
  });
});
document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeIssueModal(); closeCleanupModal(); } });

// Event delegation for Issue buttons (avoids inline onclick escaping issues)
document.addEventListener('click', function(e) {
  var btn = e.target.closest('.issue-btn');
  if (!btn) return;
  var app = btn.getAttribute('data-app') || '';
  var exitCode = parseInt(btn.getAttribute('data-exit') || '0', 10);
  var errorText = btn.getAttribute('data-error') || '';
  var rate = parseFloat(btn.getAttribute('data-rate') || '0');
  openIssueModal(app, exitCode, errorText, rate);
});

// Initial load
refreshData();
