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
const resourceSnapshotText = document.getElementById('resourceSnapshotText');
const resourceTotalsText = document.getElementById('resourceTotalsText');
const resourceMetrics = document.getElementById('resourceMetrics');
const resourceTotalMetrics = document.getElementById('resourceTotalMetrics');
const resourceTree = document.getElementById('resourceTree');
const reloadResourceTreeButton = document.getElementById('reloadResourceTreeButton');
const systemStatusSummary = document.getElementById('systemStatusSummary');
const systemStatusMetrics = document.getElementById('systemStatusMetrics');
const systemCountdownText = document.getElementById('systemCountdownText');
const systemLastRunText = document.getElementById('systemLastRunText');
const systemQueueSummary = document.getElementById('systemQueueSummary');
const systemQueueList = document.getElementById('systemQueueList');
const systemErrorDetails = document.getElementById('systemErrorDetails');
const systemErrorLog = document.getElementById('systemErrorLog');
const failedPoolCleanupText = document.getElementById('failedPoolCleanupText');
const manualMaxLaunchedJobsInput = document.getElementById('manualMaxLaunchedJobsInput');
const runSchedulerNowButton = document.getElementById('runSchedulerNowButton');
const resetFailedJobsCacheButton = document.getElementById('resetFailedJobsCacheButton');
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
const serviceLogPath = document.getElementById('serviceLogPath');
const serviceLogMetrics = document.getElementById('serviceLogMetrics');
const serviceStatusFilters = document.getElementById('serviceStatusFilters');
const serviceLogSearchInput = document.getElementById('serviceLogSearchInput');
const reloadServiceLogButton = document.getElementById('reloadServiceLogButton');
const servicePrevPageButton = document.getElementById('servicePrevPageButton');
const serviceNextPageButton = document.getElementById('serviceNextPageButton');
const serviceLogPageInfo = document.getElementById('serviceLogPageInfo');
const serviceLogList = document.getElementById('serviceLogList');
const schedulerLogPath = document.getElementById('schedulerLogPath');
const schedulerLogMetrics = document.getElementById('schedulerLogMetrics');
const schedulerStatusFilters = document.getElementById('schedulerStatusFilters');
const schedulerLogSearchInput = document.getElementById('schedulerLogSearchInput');
const reloadSchedulerLogButton = document.getElementById('reloadSchedulerLogButton');
const schedulerPrevPageButton = document.getElementById('schedulerPrevPageButton');
const schedulerNextPageButton = document.getElementById('schedulerNextPageButton');
const schedulerLogPageInfo = document.getElementById('schedulerLogPageInfo');
const schedulerLogList = document.getElementById('schedulerLogList');
const mlModelSummary = document.getElementById('mlModelSummary');
const mlModelMetrics = document.getElementById('mlModelMetrics');
const mlModelDataDir = document.getElementById('mlModelDataDir');
const mlForecastWindowText = document.getElementById('mlForecastWindowText');
const mlPredictionCaption = document.getElementById('mlPredictionCaption');
const mlPredictionChart = document.getElementById('mlPredictionChart');
const mlSeasonalityCaption = document.getElementById('mlSeasonalityCaption');
const mlDailySeasonalityChart = document.getElementById('mlDailySeasonalityChart');
const mlWeeklySeasonalityChart = document.getElementById('mlWeeklySeasonalityChart');
const mlYearlySeasonalityChart = document.getElementById('mlYearlySeasonalityChart');
const mlLogPath = document.getElementById('mlLogPath');
const mlLogMetrics = document.getElementById('mlLogMetrics');
const mlStatusFilters = document.getElementById('mlStatusFilters');
const mlLogSearchInput = document.getElementById('mlLogSearchInput');
const reloadMlLogButton = document.getElementById('reloadMlLogButton');
const mlPrevPageButton = document.getElementById('mlPrevPageButton');
const mlNextPageButton = document.getElementById('mlNextPageButton');
const mlLogPageInfo = document.getElementById('mlLogPageInfo');
const mlLogList = document.getElementById('mlLogList');
const jobRuntimeLogPath = document.getElementById('jobRuntimeLogPath');
const jobRuntimeLogMetrics = document.getElementById('jobRuntimeLogMetrics');
const jobRuntimeStatusFilters = document.getElementById('jobRuntimeStatusFilters');
const jobRuntimeLogSearchInput = document.getElementById('jobRuntimeLogSearchInput');
const reloadJobRuntimeLogButton = document.getElementById('reloadJobRuntimeLogButton');
const jobRuntimePrevPageButton = document.getElementById('jobRuntimePrevPageButton');
const jobRuntimeNextPageButton = document.getElementById('jobRuntimeNextPageButton');
const jobRuntimeLogPageInfo = document.getElementById('jobRuntimeLogPageInfo');
const jobRuntimeLogList = document.getElementById('jobRuntimeLogList');
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
let resourceTreePayload = null;
let systemStatusPayload = null;
let selectedClusterSourcePath = null;
let configTargets = [];
let selectedConfigId = null;
let calendarCatalog = null;
let selectedCalendarYear = null;
let selectedCalendarFile = null;
let serviceLogPayload = null;
let schedulerLogPayload = null;
let mlModelInsightsPayload = null;
let mlLogPayload = null;
let jobRuntimeLogPayload = null;
let jobLogPayload = null;
let serviceLogPage = 1;
let schedulerLogPage = 1;
let mlLogPage = 1;
let jobRuntimeLogPage = 1;
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
  setText('.tab-button[data-tab="resources"] .tab-label', 'tab.resources.label');
  setText('.tab-button[data-tab="resources"] .tab-caption', 'tab.resources.caption');
  setText('.tab-button[data-tab="configs"] .tab-label', 'tab.configs.label');
  setText('.tab-button[data-tab="configs"] .tab-caption', 'tab.configs.caption');
  setText('.tab-button[data-tab="calendars"] .tab-label', 'tab.calendars.label');
  setText('.tab-button[data-tab="calendars"] .tab-caption', 'tab.calendars.caption');
  setText('.tab-button[data-tab="service_logs"] .tab-label', 'tab.service_logs.label');
  setText('.tab-button[data-tab="service_logs"] .tab-caption', 'tab.service_logs.caption');
  setText('.tab-button[data-tab="scheduler_logs"] .tab-label', 'tab.scheduler_logs.label');
  setText('.tab-button[data-tab="scheduler_logs"] .tab-caption', 'tab.scheduler_logs.caption');
  setText('.tab-button[data-tab="ml_logs"] .tab-label', 'tab.ml_logs.label');
  setText('.tab-button[data-tab="ml_logs"] .tab-caption', 'tab.ml_logs.caption');
  setText('.tab-button[data-tab="job_runtime_logs"] .tab-label', 'tab.job_runtime_logs.label');
  setText('.tab-button[data-tab="job_runtime_logs"] .tab-caption', 'tab.job_runtime_logs.caption');
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
  setText('#failedPoolCleanupText', 'system.failedPoolCleanupLoading');
  setPlaceholder('#manualMaxLaunchedJobsInput', 'system.manualMaxPlaceholder');
  setText('#runSchedulerNowButton', 'button.runSchedulerNow');
  setText('#resetFailedJobsCacheButton', 'button.resetFailedJobsCache');
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
  setText('#resourceTreeHeading', 'resources.title');
  setText('#resourceTreeIntro', 'resources.intro');
  setText('[data-panel="resources"] .grid.two .card:nth-child(1) h3', 'resources.snapshot.title');
  setText('[data-panel="resources"] .grid.two .card:nth-child(2) h3', 'resources.totals.title');
  setText('#reloadResourceTreeButton', 'button.reloadResources');
  if (!resourceTreePayload) {
    setText('#resourceSnapshotText', 'resources.loadingSnapshot');
    setText('#resourceTotalsText', 'resources.loadingTotals');
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
  setText('[data-panel="service_logs"] .card:nth-child(1) h3', 'serviceLogs.title');
  setText('[data-panel="service_logs"] .card:nth-child(2) h3', 'runtimeLogs.filters.title');
  if (!serviceLogPayload) {
    setText('#serviceLogPath', 'runtimeLogs.loadingPath');
  }
  setPlaceholder('#serviceLogSearchInput', 'serviceLogs.searchPlaceholder');
  setText('#reloadServiceLogButton', 'button.reloadLog');
  setText('#servicePrevPageButton', 'button.prevPage');
  setText('#serviceNextPageButton', 'button.nextPage');
  setText('[data-panel="scheduler_logs"] .card:nth-child(1) h3', 'schedulerLogs.title');
  setText('[data-panel="scheduler_logs"] .card:nth-child(2) h3', 'runtimeLogs.filters.title');
  if (!schedulerLogPayload) {
    setText('#schedulerLogPath', 'runtimeLogs.loadingPath');
  }
  setPlaceholder('#schedulerLogSearchInput', 'schedulerLogs.searchPlaceholder');
  setText('#reloadSchedulerLogButton', 'button.reloadLog');
  setText('#schedulerPrevPageButton', 'button.prevPage');
  setText('#schedulerNextPageButton', 'button.nextPage');
  setText('[data-panel="ml_logs"] > .grid.two:nth-of-type(1) .card:nth-child(1) h3', 'mlModel.card.summaryTitle');
  setText('[data-panel="ml_logs"] > .grid.two:nth-of-type(1) .card:nth-child(2) h3', 'mlModel.card.forecastWindowTitle');
  setText('[data-panel="ml_logs"] > .grid.two:nth-of-type(2) .card:nth-child(1) h3', 'mlModel.card.predictionTitle');
  setText('[data-panel="ml_logs"] > .grid.two:nth-of-type(2) .card:nth-child(2) h3', 'mlModel.card.seasonalityTitle');
  setText('[data-panel="ml_logs"] > .grid.two:nth-of-type(3) .card:nth-child(1) h3', 'mlLogs.title');
  setText('[data-panel="ml_logs"] > .grid.two:nth-of-type(3) .card:nth-child(2) h3', 'runtimeLogs.filters.title');
  if (!mlLogPayload) {
    setText('#mlLogPath', 'runtimeLogs.loadingPath');
  }
  if (!mlModelInsightsPayload) {
    setText('#mlModelSummary', 'mlModel.loadingSummary');
    setText('#mlForecastWindowText', 'mlModel.loadingForecastWindow');
    setText('#mlPredictionCaption', 'mlModel.loadingPredictionChart');
    setText('#mlSeasonalityCaption', 'mlModel.loadingSeasonalityChart');
  }
  setPlaceholder('#mlLogSearchInput', 'mlLogs.searchPlaceholder');
  setText('#reloadMlLogButton', 'button.reloadLog');
  setText('#mlPrevPageButton', 'button.prevPage');
  setText('#mlNextPageButton', 'button.nextPage');
  setText('[data-panel="job_runtime_logs"] > .grid.two .card:nth-child(1) h3', 'jobRuntimeLogs.title');
  setText('[data-panel="job_runtime_logs"] > .grid.two .card:nth-child(2) h3', 'runtimeLogs.filters.title');
  if (!jobRuntimeLogPayload) {
    setText('#jobRuntimeLogPath', 'runtimeLogs.loadingPath');
  }
  setPlaceholder('#jobRuntimeLogSearchInput', 'jobRuntimeLogs.searchPlaceholder');
  setText('#reloadJobRuntimeLogButton', 'button.reloadLog');
  setText('#jobRuntimePrevPageButton', 'button.prevPage');
  setText('#jobRuntimeNextPageButton', 'button.nextPage');
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
  if (resourceTreePayload) renderResourceTree(resourceTreePayload);
  if (calendarCatalog) renderCalendarOverview();
  if (serviceLogPayload) renderServiceLogs(serviceLogPayload);
  if (schedulerLogPayload) renderSchedulerRuntimeLogs(schedulerLogPayload);
  if (mlModelInsightsPayload) renderForecastModelInsights(mlModelInsightsPayload);
  if (mlLogPayload) renderMlRuntimeLogs(mlLogPayload);
  if (jobRuntimeLogPayload) renderJobRuntimeLogs(jobRuntimeLogPayload);
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
