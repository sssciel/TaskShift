function renderTaskshiftLogs(payload) {
  taskshiftLogPayload = payload;
  taskshiftLogPath.textContent = payload.file;
  renderMetrics(taskshiftLogMetrics, [
    t('metric.shown', { count: payload.shown_entries }),
    t('metric.filtered', { count: payload.filtered_entries }),
    t('metric.total', { count: payload.total_entries }),
    t('metric.statuses', { count: payload.available_statuses.length }),
  ]);
  renderStatusFilters(taskshiftStatusFilters, payload.available_statuses, payload.selected_statuses);
  taskshiftLogPageInfo.textContent = t('logs.pageInfo', { page: payload.page, total: payload.total_pages });
  taskshiftPrevPageButton.disabled = !payload.has_prev_page;
  taskshiftNextPageButton.disabled = !payload.has_next_page;

  if (taskshiftLogSearchInput.value !== payload.query) {
    taskshiftLogSearchInput.value = payload.query;
  }

  taskshiftLogList.innerHTML = '';
  if (payload.entries.length === 0) {
    taskshiftLogList.innerHTML = `<div class="empty-state">${escapeHtml(t('logs.noEntries'))}</div>`;
    return;
  }

  payload.entries.forEach((entry) => {
    const item = document.createElement('article');
    item.className = 'log-entry';
    item.innerHTML = `
      <div class="log-entry-header">
        <div class="log-entry-title">
          <strong>${escapeHtml(entry.timestamp || 'No timestamp')}</strong>
          <div class="log-entry-meta">${escapeHtml(entry.source || 'unknown source')}</div>
        </div>
        <span class="log-level ${escapeHtml(entry.level || 'OTHER')}">${escapeHtml(entry.level || 'OTHER')}</span>
      </div>
      <pre class="log-entry-message">${escapeHtml(entry.message || '')}</pre>
    `;
    taskshiftLogList.appendChild(item);
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
    `;
    jobLogList.appendChild(item);
  });
}

async function loadTaskshiftLogs() {
  const params = new URLSearchParams();
  if (taskshiftLogSearchInput.value.trim()) {
    params.set('q', taskshiftLogSearchInput.value.trim());
  }
  getSelectedStatuses(taskshiftStatusFilters).forEach((status) => params.append('statuses', status));
  params.set('page', String(taskshiftLogPage));
  params.set('limit', '100');
  setStatus(t('status.loadingServiceLog'));
  const payload = await fetchJson(`/api/logs/taskshift?${params.toString()}`);
  taskshiftLogPage = payload.page;
  renderTaskshiftLogs(payload);
  setStatus(t('status.loadedServiceLog', { count: payload.shown_entries }));
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
