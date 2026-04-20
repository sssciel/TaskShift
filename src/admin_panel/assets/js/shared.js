const TRANSLATIONS = window.__TASKSHIFT_APP_TRANSLATIONS__ || {};

const statusBox = document.getElementById('statusBox');
const panelTitle = document.getElementById('panelTitle');
const panelSubtitle = document.getElementById('panelSubtitle');
const languageSelect = document.getElementById('languageSelect');
const clusterSourceText = document.getElementById('clusterSourceText');
const clusterOverviewText = document.getElementById('clusterOverviewText');
const clusterMetrics = document.getElementById('clusterMetrics');
const clusterOverviewMetrics = document.getElementById('clusterOverviewMetrics');
const clusterTree = document.getElementById('clusterTree');
const clusterSourceSelect = document.getElementById('clusterSourceSelect');
const reloadClusterSourceButton = document.getElementById('reloadClusterSourceButton');
const systemStatusSummary = document.getElementById('systemStatusSummary');
const systemStatusMetrics = document.getElementById('systemStatusMetrics');
const systemCountdownText = document.getElementById('systemCountdownText');
const systemLastRunText = document.getElementById('systemLastRunText');
const systemQueueSummary = document.getElementById('systemQueueSummary');
const systemQueueList = document.getElementById('systemQueueList');
const systemErrorDetails = document.getElementById('systemErrorDetails');
const systemErrorLog = document.getElementById('systemErrorLog');
const manualMaxLaunchedJobsInput = document.getElementById('manualMaxLaunchedJobsInput');
const runSchedulerNowButton = document.getElementById('runSchedulerNowButton');
const fileToolbar = document.getElementById('fileToolbar');
const editorPath = document.getElementById('editorPath');
const configEditor = document.getElementById('configEditor');
const saveButton = document.getElementById('saveButton');
const reloadButton = document.getElementById('reloadButton');
const calendarOverviewMetrics = document.getElementById('calendarOverviewMetrics');
const calendarYearList = document.getElementById('calendarYearList');
const calendarFileList = document.getElementById('calendarFileList');
const calendarEditorMeta = document.getElementById('calendarEditorMeta');
const calendarEditor = document.getElementById('calendarEditor');
const saveCalendarButton = document.getElementById('saveCalendarButton');
const reloadCalendarButton = document.getElementById('reloadCalendarButton');
const newCalendarYearInput = document.getElementById('newCalendarYearInput');
const copyCalendarYearSelect = document.getElementById('copyCalendarYearSelect');
const newCalendarFileInput = document.getElementById('newCalendarFileInput');
const templateCalendarYearSelect = document.getElementById('templateCalendarYearSelect');
const createCalendarYearButton = document.getElementById('createCalendarYearButton');
const createCalendarFileButton = document.getElementById('createCalendarFileButton');
const taskshiftLogPath = document.getElementById('taskshiftLogPath');
const taskshiftLogMetrics = document.getElementById('taskshiftLogMetrics');
const taskshiftStatusFilters = document.getElementById('taskshiftStatusFilters');
const taskshiftLogSearchInput = document.getElementById('taskshiftLogSearchInput');
const reloadTaskshiftLogButton = document.getElementById('reloadTaskshiftLogButton');
const taskshiftPrevPageButton = document.getElementById('taskshiftPrevPageButton');
const taskshiftNextPageButton = document.getElementById('taskshiftNextPageButton');
const taskshiftLogPageInfo = document.getElementById('taskshiftLogPageInfo');
const taskshiftLogList = document.getElementById('taskshiftLogList');
const jobLogPath = document.getElementById('jobLogPath');
const jobLogMetrics = document.getElementById('jobLogMetrics');
const jobStatusFilters = document.getElementById('jobStatusFilters');
const jobLogIdInput = document.getElementById('jobLogIdInput');
const jobLogSearchInput = document.getElementById('jobLogSearchInput');
const reloadJobLogButton = document.getElementById('reloadJobLogButton');
const jobPrevPageButton = document.getElementById('jobPrevPageButton');
const jobNextPageButton = document.getElementById('jobNextPageButton');
const jobLogPageInfo = document.getElementById('jobLogPageInfo');
const jobLogList = document.getElementById('jobLogList');

let clusterPayload = null;
let clusterSourcesCatalog = null;
let systemStatusPayload = null;
let selectedClusterSourcePath = null;
let configTargets = [];
let selectedConfigId = null;
let calendarCatalog = null;
let selectedCalendarYear = null;
let selectedCalendarFile = null;
let taskshiftLogPayload = null;
let jobLogPayload = null;
let taskshiftLogPage = 1;
let jobLogPage = 1;
let currentLanguage = 'ru';
let systemStatusPollHandle = null;
let systemCountdownHandle = null;

function getPreferredLanguage() {
  const stored = localStorage.getItem('taskshift-admin-language');
  if (stored === 'ru' || stored === 'en') {
    return stored;
  }
  return (navigator.language || '').toLowerCase().startsWith('ru') ? 'ru' : 'en';
}

function t(key, vars = {}) {
  const table = TRANSLATIONS[currentLanguage] || TRANSLATIONS.en || {};
  const fallback = (TRANSLATIONS.en || {})[key] || key;
  const template = table[key] || fallback;
  return template.replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? ''));
}

function setText(selector, key, vars = {}) {
  const node = document.querySelector(selector);
  if (node) {
    node.textContent = t(key, vars);
  }
}

function setHtml(selector, key, vars = {}) {
  const node = document.querySelector(selector);
  if (node) {
    node.innerHTML = t(key, vars);
  }
}

function setPlaceholder(selector, key) {
  const node = document.querySelector(selector);
  if (node) {
    node.setAttribute('placeholder', t(key));
  }
}

function configTargetLabel(target) {
  return t(`config.${target.id}`) || target.label;
}

function applyLanguage() {
  document.documentElement.lang = currentLanguage;
  languageSelect.value = currentLanguage;
  setText('.brand h1', 'brand.title');
  setText('.brand p', 'brand.description');
  setText('.tab-button[data-tab="system"] .tab-label', 'tab.system.label');
  setText('.tab-button[data-tab="system"] .tab-caption', 'tab.system.caption');
  setText('.tab-button[data-tab="cluster"] .tab-label', 'tab.cluster.label');
  setText('.tab-button[data-tab="cluster"] .tab-caption', 'tab.cluster.caption');
  setText('.tab-button[data-tab="configs"] .tab-label', 'tab.configs.label');
  setText('.tab-button[data-tab="configs"] .tab-caption', 'tab.configs.caption');
  setText('.tab-button[data-tab="calendars"] .tab-label', 'tab.calendars.label');
  setText('.tab-button[data-tab="calendars"] .tab-caption', 'tab.calendars.caption');
  setText('.tab-button[data-tab="logs"] .tab-label', 'tab.logs.label');
  setText('.tab-button[data-tab="logs"] .tab-caption', 'tab.logs.caption');
  setText('.tab-button[data-tab="jobs"] .tab-label', 'tab.jobs.label');
  setText('.tab-button[data-tab="jobs"] .tab-caption', 'tab.jobs.caption');
  setText('.side-card:nth-of-type(1) h3', 'side.auth.title');
  setHtml('.side-card:nth-of-type(1) p', 'side.auth.text');
  setText('.side-card:nth-of-type(2) h3', 'side.edit.title');
  setText('.side-card:nth-of-type(2) p', 'side.edit.text');
  setText('.logout', 'button.logout');
  setText('[data-panel="system"] .system-card h3', 'system.title');
  setText('[data-panel="system"] .system-grid .system-pane:nth-child(1) h4', 'system.next.title');
  setText('[data-panel="system"] .system-grid .system-pane:nth-child(2) h4', 'system.last.title');
  setText('[data-panel="system"] .system-queue-header h4', 'system.queue.title');
  setText('#systemErrorSummary', 'system.error.summary');
  setPlaceholder('#manualMaxLaunchedJobsInput', 'system.manualMaxPlaceholder');
  setText('#runSchedulerNowButton', 'button.runSchedulerNow');
  if (!systemStatusPayload) {
    setText('#systemStatusSummary', 'system.summary.loading');
    setText('#systemCountdownText', 'system.next.loading');
    setText('#systemLastRunText', 'system.last.loading');
    setText('#systemQueueSummary', 'system.queue.loading');
  }
  setText('#clusterTopologyHeading', 'cluster.topology.title');
  setText('#clusterTopologyIntro', 'cluster.topology.intro');
  setText('[data-panel="cluster"] .grid.two .card:nth-child(1) h3', 'cluster.active.title');
  setText('[data-panel="cluster"] .grid.two .card:nth-child(2) h3', 'cluster.overview.title');
  setText('#reloadClusterSourceButton', 'cluster.reloadSource');
  if (!clusterPayload) {
    setText('#clusterSourceText', 'cluster.loadingSource');
    setText('#clusterOverviewText', 'cluster.loadingOverview');
  }
  setText('[data-panel="configs"] .card h3', 'configs.title');
  setText('[data-panel="configs"] .card p', 'configs.description');
  setText('#editorPath', 'configs.loadingTarget');
  setText('#saveButton', 'button.saveFile');
  setText('#reloadButton', 'button.reloadDisk');
  setText('[data-panel="calendars"] .grid.two .card:nth-child(1) h3', 'cal.discovery.title');
  setHtml('[data-panel="calendars"] .grid.two .card:nth-child(1) p', 'cal.discovery.desc');
  setText('[data-panel="calendars"] .card.stack > div:nth-child(1) h3', 'cal.createYear.title');
  setText('[data-panel="calendars"] .card.stack > div:nth-child(1) p', 'cal.createYear.desc');
  setText('#createCalendarYearButton', 'cal.createYear.button');
  setText('[data-panel="calendars"] .card.stack > div:nth-child(2) h3', 'cal.addFile.title');
  setText('[data-panel="calendars"] .card.stack > div:nth-child(2) p', 'cal.addFile.desc');
  setText('#createCalendarFileButton', 'cal.addFile.button');
  setText('[data-panel="calendars"] .calendar-sidebar .card:nth-child(1) h3', 'cal.years.title');
  setText('[data-panel="calendars"] .calendar-sidebar .card:nth-child(2) h3', 'cal.files.title');
  setText('[data-panel="calendars"] .calendar-layout > .card h3', 'cal.editor.title');
  if (!selectedCalendarYear || !selectedCalendarFile) {
    setText('#calendarEditorMeta', 'cal.editor.loading');
  }
  setText('#saveCalendarButton', 'button.saveCalendar');
  setText('#reloadCalendarButton', 'button.reloadCalendar');
  setText('[data-panel="logs"] .card:nth-child(1) h3', 'logs.service.title');
  setText('[data-panel="logs"] .card:nth-child(2) h3', 'logs.filters.title');
  if (!taskshiftLogPayload) {
    setText('#taskshiftLogPath', 'logs.loadingPath');
  }
  setPlaceholder('#taskshiftLogSearchInput', 'logs.searchPlaceholder');
  setText('#reloadTaskshiftLogButton', 'button.reloadLog');
  setText('#taskshiftPrevPageButton', 'button.prevPage');
  setText('#taskshiftNextPageButton', 'button.nextPage');
  setText('[data-panel="jobs"] .card:nth-child(1) h3', 'jobs.title');
  setText('[data-panel="jobs"] .card:nth-child(2) h3', 'jobs.filters.title');
  if (!jobLogPayload) {
    setText('#jobLogPath', 'jobs.loadingPath');
  }
  setPlaceholder('#jobLogIdInput', 'jobs.idPlaceholder');
  setPlaceholder('#jobLogSearchInput', 'jobs.searchPlaceholder');
  setText('#reloadJobLogButton', 'button.reloadJobsLog');
  setText('#jobPrevPageButton', 'button.prevPage');
  setText('#jobNextPageButton', 'button.nextPage');
  const activeTab = document.querySelector('.tab-button.active')?.dataset.tab || 'system';
  activateTab(activeTab);
  if (configTargets.length > 0) renderFileButtons();
  if (clusterSourcesCatalog) renderClusterSourceControls();
  if (systemStatusPayload) renderSystemStatus(systemStatusPayload);
  if (clusterPayload) renderClusterTree(clusterPayload);
  if (calendarCatalog) renderCalendarOverview();
  if (taskshiftLogPayload) renderTaskshiftLogs(taskshiftLogPayload);
  if (jobLogPayload) renderJobLogs(jobLogPayload);
  if (!statusBox.textContent || statusBox.textContent === 'Ready.') {
    setStatus(t('status.ready'));
  }
}

function setStatus(message, isError = false) {
  statusBox.textContent = message;
  statusBox.style.color = isError ? '#8d2d1f' : 'var(--steel)';
  statusBox.style.borderColor = isError ? 'rgba(141,45,31,0.2)' : 'rgba(56,84,107,0.14)';
  statusBox.style.background = isError ? 'rgba(141,45,31,0.08)' : 'rgba(255,255,255,0.66)';
}

function activateTab(tabName) {
  document.querySelectorAll('.tab-button').forEach((button) => {
    button.classList.toggle('active', button.dataset.tab === tabName);
  });
  document.querySelectorAll('.panel').forEach((panel) => {
    panel.classList.toggle('active', panel.dataset.panel === tabName);
  });
  panelTitle.textContent = t(`panel.${tabName}.title`);
  panelSubtitle.textContent = t(`panel.${tabName}.subtitle`);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatDate(value) {
  if (!value) {
    return t('common.dateUnknown');
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(currentLanguage === 'ru' ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date);
}

function createChip(text) {
  const chip = document.createElement('span');
  chip.className = 'chip';
  chip.textContent = text;
  return chip;
}

function renderMetrics(container, values) {
  container.innerHTML = '';
  values.forEach((value) => {
    const metric = document.createElement('span');
    metric.className = 'metric';
    metric.textContent = value;
    container.appendChild(metric);
  });
}

function getSystemStatusLabel(status) {
  return t(`system.status.${status || 'idle'}`);
}

function getTranslatedStatusLabel(prefix, status, fallbackStatus = 'UNKNOWN') {
  const normalizedStatus = String(status || fallbackStatus);
  const key = `${prefix}.${normalizedStatus}`;
  const table = TRANSLATIONS[currentLanguage] || TRANSLATIONS.en || {};
  return table[key] || (TRANSLATIONS.en || {})[key] || normalizedStatus;
}

function getQueueStatusLabel(status) {
  return getTranslatedStatusLabel('queue.status', status, 'PENDING');
}

function getJobLogStatusLabel(status) {
  return getTranslatedStatusLabel('job.status', status, 'UNKNOWN');
}

function getSystemTriggerLabel(trigger) {
  return t(`system.trigger.${trigger || 'unknown'}`);
}

function formatDuration(value) {
  if (value === null || value === undefined || value === '') {
    return t('label.none');
  }
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) {
    return String(value);
  }
  if (seconds < 1) {
    return `${seconds.toFixed(3)}s`;
  }
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.round(seconds % 60);
  return `${minutes}m ${remainder}s`;
}

function formatCountdown(totalSeconds) {
  if (totalSeconds === null || totalSeconds === undefined || Number.isNaN(Number(totalSeconds))) {
    return t('label.none');
  }
  const safeSeconds = Math.max(0, Number(totalSeconds));
  const hours = Math.floor(safeSeconds / 3600);
  const minutes = Math.floor((safeSeconds % 3600) / 60);
  const seconds = Math.floor(safeSeconds % 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function getSelectedStatuses(container) {
  return Array.from(container.querySelectorAll('input[type="checkbox"]:checked')).map((input) => input.value);
}

function renderStatusFilters(container, availableStatuses, selectedStatuses, labelResolver = null) {
  container.innerHTML = '';
  availableStatuses.forEach((status) => {
    const labelText = labelResolver ? labelResolver(status) : status;
    const label = document.createElement('label');
    label.className = 'status-toggle';
    label.innerHTML = `
      <input type="checkbox" value="${escapeHtml(status)}" ${selectedStatuses.includes(status) ? 'checked' : ''}>
      <span>${escapeHtml(labelText)}</span>
    `;
    container.appendChild(label);
  });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    credentials: 'same-origin',
    headers: {
      'Accept': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  });
  if (response.status === 401) {
    window.location.reload();
    throw new Error(t('status.unauthorized'));
  }
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || 'Request failed');
  }
  return payload;
}
