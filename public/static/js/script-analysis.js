let currentData = null;
let expandTop = false;
let expandBottom = false;
let expandRecent = false;
const LIMIT = 10;

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function formatTimestamp(ts) {
  if (!ts) return '-';
  return new Date(ts).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true });
}

async function fetchData() {
  const days = document.querySelector('.filter-btn.active')?.dataset.days || '30';
  const repo = 'ProxmoxVE';
  try {
    const resp = await fetch('/api/scripts?days=' + days + '&repo=' + repo);
    if (!resp.ok) throw new Error('Fetch failed');
    return await resp.json();
  } catch (e) {
    console.error('Fetch error:', e);
    return null;
  }
}

function updateStats(data) {
  document.getElementById('totalInstalls').textContent = (data.total_installs || 0).toLocaleString();
  document.getElementById('uniqueScripts').textContent = (data.total_scripts || 0).toLocaleString();
  const avg = data.total_scripts > 0 ? (data.total_installs / data.total_scripts).toFixed(1) : '0';
  document.getElementById('avgInstalls').textContent = avg;
}

function renderTopTable() {
  const tbody = document.getElementById('topTableBody');
  if (!currentData || !currentData.top_scripts) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--text-muted);padding:24px;">No data</td></tr>';
    return;
  }
  const search = (document.getElementById('searchTop').value || '').toLowerCase();
  let scripts = currentData.top_scripts;
  if (search) {
    scripts = scripts.filter(s => s.app.toLowerCase().includes(search) || (s.type || '').toLowerCase().includes(search));
  }
  const limit = expandTop ? scripts.length : Math.min(LIMIT, scripts.length);
  const shown = scripts.slice(0, limit);

  tbody.innerHTML = shown.map((s, idx) => {
    const typeClass = (s.type || '').toLowerCase();
    const rateColor = s.success_rate >= 90 ? 'var(--accent-green)' : s.success_rate >= 70 ? 'var(--accent-yellow)' : 'var(--accent-red)';
    const total = s.success + s.failed + s.aborted + s.installing;
    const pctSuccess = total > 0 ? (s.success / total * 100) : 0;
    const pctFailed = total > 0 ? (s.failed / total * 100) : 0;
    const pctAborted = total > 0 ? (s.aborted / total * 100) : 0;
    const pctInstalling = total > 0 ? (s.installing / total * 100) : 0;
    const ipd = (s.installs_per_day || 0).toFixed(2);
    const ipdColor = s.installs_per_day >= 10 ? 'var(--accent-green)' : s.installs_per_day >= 1 ? 'var(--accent-cyan)' : 'var(--text-muted)';
    return '<tr>' +
      '<td style="color:var(--text-muted);font-weight:600;">' + (idx + 1) + '</td>' +
      '<td><strong>' + escapeHtml(s.app) + '</strong></td>' +
      '<td><span class="type-badge ' + typeClass + '">' + (s.type || '-').toUpperCase() + '</span></td>' +
      '<td style="font-weight:600;">' + s.total.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-green);">' + s.success.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-red);">' + s.failed.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-purple);">' + s.aborted.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-yellow);">' + s.installing.toLocaleString() + '</td>' +
      '<td style="color:' + rateColor + ';font-weight:600;">' + s.success_rate.toFixed(1) + '%</td>' +
      '<td style="color:' + ipdColor + ';font-weight:600;">' + ipd + '</td>' +
      '<td><div class="success-bar">' +
      '<div class="seg-success" style="width:' + pctSuccess + '%"></div>' +
      '<div class="seg-failed" style="width:' + pctFailed + '%"></div>' +
      '<div class="seg-aborted" style="width:' + pctAborted + '%"></div>' +
      '<div class="seg-installing" style="width:' + pctInstalling + '%"></div>' +
      '</div></td>' +
      '</tr>';
  }).join('');

  document.getElementById('expandTopBtn').textContent = expandTop ? 'Show Top 10' : 'Show All (' + scripts.length + ')';
}

function renderBottomTable() {
  const tbody = document.getElementById('bottomTableBody');
  if (!currentData || !currentData.top_scripts) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--text-muted);padding:24px;">No data</td></tr>';
    return;
  }
  const search = (document.getElementById('searchBottom').value || '').toLowerCase();
  // Reverse: least used first
  let scripts = [...currentData.top_scripts].reverse();
  if (search) {
    scripts = scripts.filter(s => s.app.toLowerCase().includes(search) || (s.type || '').toLowerCase().includes(search));
  }
  const limit = expandBottom ? scripts.length : Math.min(LIMIT, scripts.length);
  const shown = scripts.slice(0, limit);
  const totalScripts = currentData.top_scripts.length;

  tbody.innerHTML = shown.map((s, idx) => {
    const typeClass = (s.type || '').toLowerCase();
    const rateColor = s.success_rate >= 90 ? 'var(--accent-green)' : s.success_rate >= 70 ? 'var(--accent-yellow)' : 'var(--accent-red)';
    const total = s.success + s.failed + s.aborted + s.installing;
    const pctSuccess = total > 0 ? (s.success / total * 100) : 0;
    const pctFailed = total > 0 ? (s.failed / total * 100) : 0;
    const pctAborted = total > 0 ? (s.aborted / total * 100) : 0;
    const pctInstalling = total > 0 ? (s.installing / total * 100) : 0;
    const ipd = (s.installs_per_day || 0).toFixed(2);
    const ipdColor = s.installs_per_day >= 10 ? 'var(--accent-green)' : s.installs_per_day >= 1 ? 'var(--accent-cyan)' : 'var(--text-muted)';
    return '<tr>' +
      '<td style="color:var(--text-muted);font-weight:600;">' + (totalScripts - idx) + '</td>' +
      '<td><strong>' + escapeHtml(s.app) + '</strong></td>' +
      '<td><span class="type-badge ' + typeClass + '">' + (s.type || '-').toUpperCase() + '</span></td>' +
      '<td style="font-weight:600;">' + s.total.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-green);">' + s.success.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-red);">' + s.failed.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-purple);">' + s.aborted.toLocaleString() + '</td>' +
      '<td style="color:var(--accent-yellow);">' + s.installing.toLocaleString() + '</td>' +
      '<td style="color:' + rateColor + ';font-weight:600;">' + s.success_rate.toFixed(1) + '%</td>' +
      '<td style="color:' + ipdColor + ';font-weight:600;">' + ipd + '</td>' +
      '<td><div class="success-bar">' +
      '<div class="seg-success" style="width:' + pctSuccess + '%"></div>' +
      '<div class="seg-failed" style="width:' + pctFailed + '%"></div>' +
      '<div class="seg-aborted" style="width:' + pctAborted + '%"></div>' +
      '<div class="seg-installing" style="width:' + pctInstalling + '%"></div>' +
      '</div></td>' +
      '</tr>';
  }).join('');

  document.getElementById('expandBottomBtn').textContent = expandBottom ? 'Show Bottom 10' : 'Show All (' + scripts.length + ')';
}

function renderRecentTable() {
  const tbody = document.getElementById('recentTableBody');
  if (!currentData || !currentData.recent_scripts) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:24px;">No data</td></tr>';
    return;
  }
  const search = (document.getElementById('searchRecent').value || '').toLowerCase();
  let scripts = currentData.recent_scripts;
  if (search) {
    scripts = scripts.filter(s => s.app.toLowerCase().includes(search) || (s.status || '').toLowerCase().includes(search) || (s.type || '').toLowerCase().includes(search));
  }
  const limit = expandRecent ? scripts.length : Math.min(LIMIT, scripts.length);
  const shown = scripts.slice(0, limit);

  tbody.innerHTML = shown.map(s => {
    const typeClass = (s.type || '').toLowerCase();
    const statusClass = s.status || 'unknown';
    const codeClass = s.exit_code === 0 ? 'ok' : 'err';
    const os = s.os_type ? s.os_type + (s.os_version ? ' ' + s.os_version : '') : '-';
    return '<tr>' +
      '<td><strong>' + escapeHtml(s.app) + '</strong></td>' +
      '<td><span class="type-badge ' + typeClass + '">' + (s.type || '-').toUpperCase() + '</span></td>' +
      '<td><span class="status-badge ' + statusClass + '">' + escapeHtml(s.status) + '</span></td>' +
      '<td><span class="exit-code ' + codeClass + '">' + s.exit_code + '</span></td>' +
      '<td>' + escapeHtml(os) + '</td>' +
      '<td>' + escapeHtml(s.pve_version || '-') + '</td>' +
      '<td>' + escapeHtml(s.method || '-') + '</td>' +
      '<td style="white-space:nowrap;">' + formatTimestamp(s.created) + '</td>' +
      '</tr>';
  }).join('');

  document.getElementById('expandRecentBtn').textContent = expandRecent ? 'Show Last 10' : 'Show All (' + scripts.length + ')';
}

function toggleExpand(which) {
  if (which === 'top') {
    expandTop = !expandTop;
    renderTopTable();
  } else if (which === 'bottom') {
    expandBottom = !expandBottom;
    renderBottomTable();
  } else {
    expandRecent = !expandRecent;
    renderRecentTable();
  }
}

async function refreshData() {
  const data = await fetchData();
  if (!data) return;
  currentData = data;
  updateStats(data);
  renderTopTable();
  renderBottomTable();
  renderRecentTable();
}

document.querySelectorAll('.filter-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    refreshData();
  });
});
refreshData();
