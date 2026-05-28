function getServiceLogLevelLabel(level) {
  return getTranslatedStatusLabel('serviceLogs.level', level, level || 'UNKNOWN');
}

function renderServiceLogs(payload) {
  serviceLogPayload = payload;
  serviceLogPath.textContent = payload.file;
  renderMetrics(serviceLogMetrics, [
    t('metric.shown', { count: payload.shown_entries }),
    t('metric.filtered', { count: payload.filtered_entries }),
    t('metric.total', { count: payload.total_entries }),
    t('metric.statuses', { count: payload.available_statuses.length }),
  ]);
  renderStatusFilters(serviceStatusFilters, payload.available_statuses, payload.selected_statuses, getServiceLogLevelLabel);
  serviceLogPageInfo.textContent = t('logs.pageInfo', { page: payload.page, total: payload.total_pages });
  servicePrevPageButton.disabled = !payload.has_prev_page;
  serviceNextPageButton.disabled = !payload.has_next_page;

  if (serviceLogSearchInput.value !== payload.query) {
    serviceLogSearchInput.value = payload.query;
  }

  serviceLogList.innerHTML = '';
  if (payload.entries.length === 0) {
    serviceLogList.innerHTML = `<div class="empty-state">${escapeHtml(t('serviceLogs.noEntries'))}</div>`;
    return;
  }

  payload.entries.forEach((entry) => {
    const item = document.createElement('article');
    item.className = 'log-entry';
    const rawMessage = entry.raw || entry.message || '';
    item.innerHTML = `
      <div class="log-entry-header">
        <div class="log-entry-title">
          <strong>${escapeHtml(entry.timestamp || 'No timestamp')}</strong>
          <div class="log-entry-meta">${escapeHtml(entry.source || 'service')}</div>
        </div>
        <span class="log-level ${escapeHtml(entry.level || 'INFO')}">${escapeHtml(getServiceLogLevelLabel(entry.level || 'INFO'))}</span>
      </div>
      <pre class="log-entry-message">${escapeHtml(rawMessage)}</pre>
    `;
    serviceLogList.appendChild(item);
  });
}

function renderStructuredRuntimeLogs(payload, view) {
  view.payloadRef.set(payload);
  view.pathNode.textContent = payload.file;
  renderMetrics(view.metricsNode, [
    t('metric.shown', { count: payload.shown_entries }),
    t('metric.filtered', { count: payload.filtered_entries }),
    t('metric.total', { count: payload.total_entries }),
    t('metric.statuses', { count: payload.available_statuses.length }),
    t('metric.runs', { count: payload.run_count || 0 }),
  ]);
  renderStatusFilters(
    view.statusFiltersNode,
    payload.available_statuses,
    payload.selected_statuses,
    getRuntimeLogStatusLabel,
  );
  view.pageInfoNode.textContent = t('logs.pageInfo', {
    page: payload.page,
    total: payload.total_pages,
  });
  view.prevButton.disabled = !payload.has_prev_page;
  view.nextButton.disabled = !payload.has_next_page;

  if (view.searchInput.value !== payload.query) {
    view.searchInput.value = payload.query;
  }

  view.listNode.innerHTML = '';
  if (payload.entries.length === 0) {
    view.listNode.innerHTML = `<div class="empty-state">${escapeHtml(t(view.emptyKey))}</div>`;
    return;
  }

  payload.entries.forEach((entry) => {
    const item = document.createElement('article');
    item.className = 'log-entry';
    const chips = buildRuntimeLogChips(entry, view.kind);
    const details = buildRuntimeLogDetails(entry);
    const jobId = String(entry.job_id || '').trim();
    const jobUrl = jobId ? `https://lk.hpc.hse.ru/job/${encodeURIComponent(jobId)}/` : '';
    item.innerHTML = `
      <div class="log-entry-header">
        <div class="log-entry-title">
          <strong>${escapeHtml(entry.timestamp || 'No timestamp')}</strong>
          <div class="log-entry-meta">${escapeHtml(entry.source || 'runtime')}</div>
        </div>
        <span class="log-level ${escapeHtml(entry.status || 'UNKNOWN')}">${escapeHtml(getRuntimeLogStatusLabel(entry.status || 'UNKNOWN'))}</span>
      </div>
      <div class="chips" style="margin-bottom: 10px;">
        ${chips.map((chip) => `<span class="chip">${escapeHtml(chip)}</span>`).join('')}
      </div>
      <pre class="log-entry-message">${escapeHtml(entry.message || '')}</pre>
      ${details ? `<details class="runtime-log-details"><summary>${escapeHtml(t('runtimeLogs.details'))}</summary><pre>${escapeHtml(details)}</pre></details>` : ''}
      ${jobUrl ? `<div class="log-entry-actions"><a class="secondary log-entry-link" href="${escapeHtml(jobUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(t('button.goToJob'))}</a></div>` : ''}
    `;
    view.listNode.appendChild(item);
  });
}

function buildRuntimeLogChips(entry, kind) {
  const chips = [];
  if (entry.run_id) {
    chips.push(t('runtimeLogs.runId', { value: entry.run_id }));
  }
  if (entry.job_id) {
    chips.push(t('runtimeLogs.jobId', { value: entry.job_id }));
  }
  if (entry.trigger) {
    chips.push(t('runtimeLogs.trigger', { value: entry.trigger }));
  }

  const data = entry.data || {};
  if (kind === 'scheduler') {
    if (data.pending_job_count !== undefined) {
      chips.push(t('runtimeLogs.pendingCount', { count: data.pending_job_count }));
    }
    if (data.running_job_count !== undefined) {
      chips.push(t('runtimeLogs.runningCount', { count: data.running_job_count }));
    }
    if (data.launched_count !== undefined) {
      chips.push(t('runtimeLogs.launchedCount', { count: data.launched_count }));
    }
  }
  if (kind === 'ml') {
    if (data.model_kind) {
      chips.push(t('runtimeLogs.modelKind', { value: data.model_kind }));
    }
    if (data.training_row_count !== undefined) {
      chips.push(t('runtimeLogs.trainingRows', { count: data.training_row_count }));
    }
  }
  if (kind === 'job') {
    if (data.partition) {
      chips.push(t('queue.partition', { value: data.partition }));
    }
    if (data.reason) {
      chips.push(t('runtimeLogs.reason', { value: data.reason }));
    }
  }
  return chips;
}

function buildRuntimeLogDetails(entry) {
  const data = { ...(entry.data || {}) };
  [
    'category',
    'status',
    'event_type',
    'level',
    'message',
    'timestamp',
    'timestamp_unix',
    'source',
  ].forEach((key) => delete data[key]);
  if (Object.keys(data).length === 0) {
    return '';
  }
  return JSON.stringify(data, null, 2);
}

function getRuntimeLogStatusLabel(status) {
  return getTranslatedStatusLabel('runtime.status', status, 'UNKNOWN');
}

function renderForecastModelInsights(payload) {
  mlModelInsightsPayload = payload;

  if (!payload.available) {
    mlModelSummary.textContent = payload.error || t('mlModel.unavailable');
    renderMetrics(mlModelMetrics, []);
    mlModelDataDir.textContent = payload.data_dir || payload.model_dir || t('label.none');
    mlForecastWindowText.textContent = t('mlModel.noForecastWindow');
    mlPredictionCaption.textContent = t('mlModel.noPredictionHistory');
    mlSeasonalityCaption.textContent = t('mlModel.noSeasonality');
    mlPredictionChart.innerHTML = `<div class="empty-state">${escapeHtml(t('mlModel.noPredictionHistory'))}</div>`;
    [mlDailySeasonalityChart, mlWeeklySeasonalityChart, mlYearlySeasonalityChart].forEach((chart) => {
      chart.innerHTML = `<div class="empty-state">${escapeHtml(t('mlModel.noSeasonality'))}</div>`;
    });
    return;
  }

  mlModelSummary.textContent = t('mlModel.summary', {
    trained: formatDate(payload.trained_at),
    model: payload.model_kind || t('label.none'),
    target: payload.target_name || t('label.none'),
  });
  renderMetrics(mlModelMetrics, [
    t('mlModel.metric.trainedAt', { value: formatDate(payload.trained_at) }),
    t('mlModel.metric.trainingRows', { count: payload.training_row_count || 0 }),
    t('mlModel.metric.currentGpu', {
      value:
        payload.current_gpu_percent === null ||
        payload.current_gpu_percent === undefined
          ? t('label.none')
          : `${payload.current_gpu_percent}%`,
    }),
    t('mlModel.metric.horizon', {
      value: payload.forecast_prediction_horizon_hours || t('label.none'),
    }),
  ]);
  mlModelDataDir.textContent = t('mlModel.paths', {
    dataDir: payload.forecast_data_dir || t('label.none'),
    modelDir: payload.model_dir || t('label.none'),
  });

  if (payload.forecast_window) {
    mlForecastWindowText.textContent = t('mlModel.forecastWindow', {
      start: formatDate(payload.forecast_window.start_at),
      end: formatDate(payload.forecast_window.end_at),
      value: payload.forecast_window.predicted_gpu_percent,
    });
  } else {
    mlForecastWindowText.textContent = t('mlModel.noForecastWindow');
  }

  if (payload.prediction_error) {
    mlPredictionCaption.textContent = payload.prediction_error;
    mlPredictionChart.innerHTML = `<div class="empty-state">${escapeHtml(payload.prediction_error)}</div>`;
  } else {
    mlPredictionCaption.textContent = t('mlModel.predictionCaption');
    renderLineChart(mlPredictionChart, {
      series: [
        {
          label: t('mlModel.series.predicted'),
          color: '#1976d2',
          points: (payload.future_forecast || []).map((point) => ({
            x: point.window_start_at || point.time,
            y: point.predicted_gpu_mean_6h,
          })),
        },
      ],
      yLabel: '% GPU',
      nowMarker: {
        x: payload.forecast_start_at || payload.latest_observation_time,
        label: t('mlModel.nowMarker', {
          value:
            payload.current_gpu_percent === null ||
            payload.current_gpu_percent === undefined
              ? t('label.none')
              : `${payload.current_gpu_percent}%`,
        }),
      },
    });
  }

  if (payload.seasonality_error) {
    mlSeasonalityCaption.textContent = payload.seasonality_error;
    [mlDailySeasonalityChart, mlWeeklySeasonalityChart, mlYearlySeasonalityChart].forEach((chart) => {
      chart.innerHTML = `<div class="empty-state">${escapeHtml(payload.seasonality_error)}</div>`;
    });
  } else {
    mlSeasonalityCaption.textContent = t('mlModel.seasonalityCaption', {
      year: payload.seasonality?.year || '',
    });
    renderLineChart(mlDailySeasonalityChart, {
      series: [
        {
          label: t('mlModel.series.dailySeasonality'),
          color: '#0f8a6a',
          points: (payload.seasonality?.daily || []).map((point) => ({
            x: point.x,
            y: point.y,
          })),
        },
      ],
      yLabel: '% GPU',
      baseline: 0,
      footer: t('mlModel.seasonalityDailyFooter'),
    });
    renderLineChart(mlWeeklySeasonalityChart, {
      series: [
        {
          label: t('mlModel.series.weeklySeasonality'),
          color: '#b56b18',
          points: (payload.seasonality?.weekly || []).map((point) => ({
            x: point.x,
            label: point.label,
            y: point.y,
          })),
        },
      ],
      yLabel: '% GPU',
      baseline: 0,
      footer: t('mlModel.seasonalityWeeklyFooter'),
    });
    renderLineChart(mlYearlySeasonalityChart, {
      series: [
        {
          label: t('mlModel.series.yearlySeasonality'),
          color: '#6a45a8',
          points: (payload.seasonality?.yearly || []).map((point) => ({
            x: point.x,
            label: point.label,
            y: point.y,
          })),
        },
      ],
      yLabel: '% GPU',
      baseline: 0,
      footer: payload.seasonality_method_note || '',
    });
  }
}

function renderLineChart(container, { series, yLabel = '', baseline = null, footer = '', nowMarker = null }) {
  const allPoints = series.flatMap((entry) => entry.points || []).filter((point) => point.y !== null && point.y !== undefined);
  if (allPoints.length === 0) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(t('mlModel.chartEmpty'))}</div>`;
    return;
  }

  const width = 640;
  const height = 280;
  const padding = { top: 20, right: 16, bottom: 48, left: 44 };
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const yValues = allPoints.map((point) => Number(point.y));
  let minY = Math.min(...yValues);
  let maxY = Math.max(...yValues);
  if (baseline !== null) {
    minY = Math.min(minY, baseline);
    maxY = Math.max(maxY, baseline);
  }
  if (minY === maxY) {
    minY -= 1;
    maxY += 1;
  }

  const primaryPoints = (series.find((entry) => entry.points && entry.points.length)?.points || []).filter((point) => point.y !== null && point.y !== undefined);
  const toX = (index, count) => {
    if (count <= 1) {
      return padding.left + innerWidth / 2;
    }
    return padding.left + (index / (count - 1)) * innerWidth;
  };
  const toY = (value) => padding.top + ((maxY - value) / (maxY - minY)) * innerHeight;

  const lines = series
    .map((entry) => {
      const validPoints = (entry.points || []).filter((point) => point.y !== null && point.y !== undefined);
      if (validPoints.length === 0) {
        return '';
      }
      const coordinates = validPoints
        .map((point, index) => `${toX(index, validPoints.length)},${toY(Number(point.y))}`)
        .join(' ');
      return `<polyline fill="none" stroke="${entry.color}" stroke-width="2.5" points="${coordinates}" />`;
    })
    .join('');

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
    const value = maxY - (maxY - minY) * ratio;
    const y = padding.top + innerHeight * ratio;
    return {
      value,
      y,
    };
  });

  const baselineLine = baseline === null
    ? ''
    : `<line x1="${padding.left}" y1="${toY(baseline)}" x2="${width - padding.right}" y2="${toY(baseline)}" stroke="rgba(79,96,117,0.35)" stroke-dasharray="4 4" />`;

  const xTicks = buildXTicks(primaryPoints).map((tick) => {
    const x = toX(tick.index, primaryPoints.length);
    return `<line x1="${x}" y1="${height - padding.bottom}" x2="${x}" y2="${height - padding.bottom + 5}" stroke="rgba(79,96,117,0.35)" />
      <text x="${x}" y="${height - padding.bottom + 20}" text-anchor="${tick.anchor}" fill="#4f6075" font-size="11">${escapeHtml(tick.label)}</text>`;
  }).join('');

  const nowLine = nowMarker && primaryPoints.length
    ? `<line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" stroke="#d33f49" stroke-width="1.5" stroke-dasharray="5 4" />
       <text x="${padding.left + 6}" y="${padding.top + 12}" fill="#9d2730" font-size="11">${escapeHtml(nowMarker.label || 'now')}</text>`
    : '';

  const legend = series
    .filter((entry) => entry.points && entry.points.length > 0)
    .map(
      (entry) =>
        `<span class="chart-legend-item"><span class="chart-legend-swatch" style="background:${entry.color}"></span>${escapeHtml(entry.label)}</span>`,
    )
    .join('');

  container.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${escapeHtml(yLabel)}">
      <rect x="0" y="0" width="${width}" height="${height}" rx="16" fill="rgba(247,251,255,0.66)"></rect>
      ${yTicks
        .map(
          (tick) =>
            `<line x1="${padding.left}" y1="${tick.y}" x2="${width - padding.right}" y2="${tick.y}" stroke="rgba(21,101,192,0.08)" />
             <text x="${padding.left - 8}" y="${tick.y + 4}" text-anchor="end" fill="#4f6075" font-size="11">${tick.value.toFixed(1)}</text>`,
        )
        .join('')}
      ${baselineLine}
      ${xTicks}
      ${nowLine}
      ${lines}
    </svg>
    <div class="chart-legend">${legend}</div>
    ${footer ? `<p class="chart-footer">${escapeHtml(footer)}</p>` : ''}
  `;
}

function buildXTicks(points) {
  if (!points.length) return [];
  const indexes = points.length <= 2
    ? [...new Set([0, points.length - 1])]
    : [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
  return indexes.map((index, position) => ({
    index,
    label: formatChartXLabel(points[index]),
    anchor: position === 0 ? 'start' : position === indexes.length - 1 ? 'end' : 'middle',
  }));
}

function formatChartXLabel(point) {
  if (!point) return '';
  if (point.label) return String(point.label);
  const value = point.x;
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime()) && String(value).includes('T')) {
    return parsed.toLocaleString(undefined, {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }
  return String(value);
}

function renderSchedulerRuntimeLogs(payload) {
  renderStructuredRuntimeLogs(payload, {
    kind: 'scheduler',
    payloadRef: { set: (value) => { schedulerLogPayload = value; } },
    pathNode: schedulerLogPath,
    metricsNode: schedulerLogMetrics,
    statusFiltersNode: schedulerStatusFilters,
    searchInput: schedulerLogSearchInput,
    pageInfoNode: schedulerLogPageInfo,
    prevButton: schedulerPrevPageButton,
    nextButton: schedulerNextPageButton,
    listNode: schedulerLogList,
    emptyKey: 'schedulerLogs.noEntries',
  });
}

function renderMlRuntimeLogs(payload) {
  renderStructuredRuntimeLogs(payload, {
    kind: 'ml',
    payloadRef: { set: (value) => { mlLogPayload = value; } },
    pathNode: mlLogPath,
    metricsNode: mlLogMetrics,
    statusFiltersNode: mlStatusFilters,
    searchInput: mlLogSearchInput,
    pageInfoNode: mlLogPageInfo,
    prevButton: mlPrevPageButton,
    nextButton: mlNextPageButton,
    listNode: mlLogList,
    emptyKey: 'mlLogs.noEntries',
  });
}

function renderJobRuntimeLogs(payload) {
  renderStructuredRuntimeLogs(payload, {
    kind: 'job',
    payloadRef: { set: (value) => { jobRuntimeLogPayload = value; } },
    pathNode: jobRuntimeLogPath,
    metricsNode: jobRuntimeLogMetrics,
    statusFiltersNode: jobRuntimeStatusFilters,
    searchInput: jobRuntimeLogSearchInput,
    pageInfoNode: jobRuntimeLogPageInfo,
    prevButton: jobRuntimePrevPageButton,
    nextButton: jobRuntimeNextPageButton,
    listNode: jobRuntimeLogList,
    emptyKey: 'jobRuntimeLogs.noEntries',
  });
}

function renderJobLogs(payload) {
  jobLogPayload = payload;
  jobLogPath.textContent = payload.file;
  renderMetrics(jobLogMetrics, [
    t('metric.shown', { count: payload.shown_entries }),
    t('metric.filtered', { count: payload.filtered_entries }),
    t('metric.total', { count: payload.total_entries }),
    t('metric.statuses', { count: payload.available_statuses.length }),
  ]);
  renderStatusFilters(jobStatusFilters, payload.available_statuses, payload.selected_statuses, getJobLogStatusLabel);
  jobLogPageInfo.textContent = t('jobs.pageInfo', { page: payload.page, total: payload.total_pages });
  jobPrevPageButton.disabled = !payload.has_prev_page;
  jobNextPageButton.disabled = !payload.has_next_page;

  if (jobLogIdInput.value !== payload.job_id) {
    jobLogIdInput.value = payload.job_id;
  }
  if (jobLogSearchInput.value !== payload.query) {
    jobLogSearchInput.value = payload.query;
  }

  jobLogList.innerHTML = '';
  if (payload.entries.length === 0) {
    jobLogList.innerHTML = `<div class="empty-state">${escapeHtml(t('jobs.noEntries'))}</div>`;
    return;
  }

  payload.entries.forEach((entry) => {
    const item = document.createElement('article');
    item.className = 'log-entry';
    const jobId = String(entry.job_id || '').trim();
    const jobUrl = jobId ? `https://lk.hpc.hse.ru/job/${encodeURIComponent(jobId)}/` : '';
    item.innerHTML = `
      <div class="log-entry-header">
        <div class="log-entry-title">
          <strong>${escapeHtml(t('job.label', { id: entry.job_id || 'unknown' }))}</strong>
          <div class="log-entry-meta">${escapeHtml(entry.job_name || 'Unnamed job')}</div>
        </div>
        <span class="log-level ${escapeHtml(entry.status || 'UNKNOWN')}">${escapeHtml(getJobLogStatusLabel(entry.status || 'UNKNOWN'))}</span>
      </div>
      <div class="chips" style="margin-bottom: 10px;">
        <span class="chip">${escapeHtml(t('job.launched', { value: entry.launched_at || 'n/a' }))}</span>
      </div>
      ${jobUrl ? `<div class="log-entry-actions"><a class="secondary log-entry-link" href="${escapeHtml(jobUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(t('button.goToJob'))}</a></div>` : ''}
    `;
    jobLogList.appendChild(item);
  });
}

async function loadStructuredRuntimeLog(view) {
  const params = new URLSearchParams();
  if (view.searchInput.value.trim()) {
    params.set('q', view.searchInput.value.trim());
  }
  getSelectedStatuses(view.statusFiltersNode).forEach((status) => params.append('statuses', status));
  params.set('page', String(view.getPage()));
  params.set('limit', '100');
  setStatus(t(view.loadingStatusKey));
  const payload = await fetchJson(`${view.endpoint}?${params.toString()}`);
  view.setPage(payload.page);
  view.render(payload);
  setStatus(t(view.loadedStatusKey, { count: payload.shown_entries }));
}

async function loadSchedulerRuntimeLogs() {
  return loadStructuredRuntimeLog({
    endpoint: '/api/logs/scheduler-runtime',
    searchInput: schedulerLogSearchInput,
    statusFiltersNode: schedulerStatusFilters,
    getPage: () => schedulerLogPage,
    setPage: (value) => { schedulerLogPage = value; },
    render: renderSchedulerRuntimeLogs,
    loadingStatusKey: 'status.loadingSchedulerLogs',
    loadedStatusKey: 'status.loadedSchedulerLogs',
  });
}

async function loadServiceLogs() {
  const params = new URLSearchParams();
  if (serviceLogSearchInput.value.trim()) {
    params.set('q', serviceLogSearchInput.value.trim());
  }
  getSelectedStatuses(serviceStatusFilters).forEach((status) => params.append('statuses', status));
  params.set('page', String(serviceLogPage));
  params.set('limit', '100');
  setStatus(t('status.loadingServiceLog'));
  const payload = await fetchJson(`/api/logs/taskshift?${params.toString()}`);
  serviceLogPage = payload.page;
  renderServiceLogs(payload);
  setStatus(t('status.loadedServiceLog', { count: payload.shown_entries }));
}

async function loadForecastModelInsights() {
  setStatus(t('status.loadingForecastModel'));
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 30000);
  try {
    const payload = await fetchJson('/api/forecast/insights', {
      signal: controller.signal,
    });
    renderForecastModelInsights(payload);
    setStatus(t('status.loadedForecastModel'));
  } catch (error) {
    const message = error.name === 'AbortError'
      ? t('mlModel.loadTimeout')
      : error.message;
    renderForecastModelInsights({
      available: false,
      error: message,
      data_dir: null,
      model_dir: null,
    });
    setStatus(message, true);
  } finally {
    window.clearTimeout(timeout);
  }
}

async function loadMlRuntimeLogs() {
  return loadStructuredRuntimeLog({
    endpoint: '/api/logs/forecast-runtime',
    searchInput: mlLogSearchInput,
    statusFiltersNode: mlStatusFilters,
    getPage: () => mlLogPage,
    setPage: (value) => { mlLogPage = value; },
    render: renderMlRuntimeLogs,
    loadingStatusKey: 'status.loadingMlLogs',
    loadedStatusKey: 'status.loadedMlLogs',
  });
}

async function loadJobRuntimeLogs() {
  return loadStructuredRuntimeLog({
    endpoint: '/api/logs/job-runtime',
    searchInput: jobRuntimeLogSearchInput,
    statusFiltersNode: jobRuntimeStatusFilters,
    getPage: () => jobRuntimeLogPage,
    setPage: (value) => { jobRuntimeLogPage = value; },
    render: renderJobRuntimeLogs,
    loadingStatusKey: 'status.loadingJobRuntimeLogs',
    loadedStatusKey: 'status.loadedJobRuntimeLogs',
  });
}

async function loadJobLogs() {
  const params = new URLSearchParams();
  if (jobLogIdInput.value.trim()) {
    params.set('job_id', jobLogIdInput.value.trim());
  }
  if (jobLogSearchInput.value.trim()) {
    params.set('q', jobLogSearchInput.value.trim());
  }
  getSelectedStatuses(jobStatusFilters).forEach((status) => params.append('statuses', status));
  params.set('page', String(jobLogPage));
  params.set('limit', '100');
  setStatus(t('status.loadingJobsLog'));
  const payload = await fetchJson(`/api/logs/jobs?${params.toString()}`);
  jobLogPage = payload.page;
  renderJobLogs(payload);
  setStatus(t('status.loadedJobsLog', { count: payload.shown_entries }));
}
