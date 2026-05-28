document.querySelectorAll('.tab-button').forEach((button) => {
  button.addEventListener('click', () => activateTab(button.dataset.tab));
});

currentLanguage = getPreferredLanguage();
languageSelect.value = currentLanguage;
languageSelect.addEventListener('change', () => {
  currentLanguage = languageSelect.value;
  localStorage.setItem('taskshift-admin-language', currentLanguage);
  applyLanguage();
});

applyLanguage();

saveButton.addEventListener('click', () => saveConfigFile().catch((error) => setStatus(error.message, true)));
reloadButton.addEventListener('click', () => loadConfigFile(selectedConfigId).catch((error) => setStatus(error.message, true)));
saveCalendarButton.addEventListener('click', () => saveCalendarFile().catch((error) => setStatus(error.message, true)));
reloadCalendarButton.addEventListener('click', () => {
  if (selectedCalendarYear && selectedCalendarFile) {
    loadCalendarFile(selectedCalendarYear, selectedCalendarFile).catch((error) => setStatus(error.message, true));
  }
});
createCalendarYearButton.addEventListener('click', () => createCalendarYear().catch((error) => setStatus(error.message, true)));
createCalendarFileButton.addEventListener('click', () => createCalendarFile().catch((error) => setStatus(error.message, true)));
runSchedulerNowButton.addEventListener('click', () => runSchedulerNow().catch((error) => setStatus(error.message, true)));
resetFailedJobsCacheButton.addEventListener('click', () => resetFailedJobsCache().catch((error) => setStatus(error.message, true)));
reloadClusterSourceButton.addEventListener('click', () => loadClusterTree().catch((error) => setStatus(error.message, true)));
reloadResourceTreeButton.addEventListener('click', () => loadResourceTree().catch((error) => setStatus(error.message, true)));
clusterSourceSelect.addEventListener('change', () => {
  selectedClusterSourcePath = clusterSourceSelect.value;
  loadClusterTree().catch((error) => setStatus(error.message, true));
});
reloadServiceLogButton.addEventListener('click', () => loadServiceLogs().catch((error) => setStatus(error.message, true)));
reloadSchedulerLogButton.addEventListener('click', () => loadSchedulerRuntimeLogs().catch((error) => setStatus(error.message, true)));
reloadMlLogButton.addEventListener('click', () => loadMlRuntimeLogs().catch((error) => setStatus(error.message, true)));
reloadJobRuntimeLogButton.addEventListener('click', () => loadJobRuntimeLogs().catch((error) => setStatus(error.message, true)));
reloadJobLogButton.addEventListener('click', () => loadJobLogs().catch((error) => setStatus(error.message, true)));
servicePrevPageButton.addEventListener('click', () => {
  serviceLogPage = Math.max(1, serviceLogPage - 1);
  loadServiceLogs().catch((error) => setStatus(error.message, true));
});
serviceNextPageButton.addEventListener('click', () => {
  serviceLogPage += 1;
  loadServiceLogs().catch((error) => setStatus(error.message, true));
});
schedulerPrevPageButton.addEventListener('click', () => {
  schedulerLogPage = Math.max(1, schedulerLogPage - 1);
  loadSchedulerRuntimeLogs().catch((error) => setStatus(error.message, true));
});
schedulerNextPageButton.addEventListener('click', () => {
  schedulerLogPage += 1;
  loadSchedulerRuntimeLogs().catch((error) => setStatus(error.message, true));
});
mlPrevPageButton.addEventListener('click', () => {
  mlLogPage = Math.max(1, mlLogPage - 1);
  loadMlRuntimeLogs().catch((error) => setStatus(error.message, true));
});
mlNextPageButton.addEventListener('click', () => {
  mlLogPage += 1;
  loadMlRuntimeLogs().catch((error) => setStatus(error.message, true));
});
jobRuntimePrevPageButton.addEventListener('click', () => {
  jobRuntimeLogPage = Math.max(1, jobRuntimeLogPage - 1);
  loadJobRuntimeLogs().catch((error) => setStatus(error.message, true));
});
jobRuntimeNextPageButton.addEventListener('click', () => {
  jobRuntimeLogPage += 1;
  loadJobRuntimeLogs().catch((error) => setStatus(error.message, true));
});
jobPrevPageButton.addEventListener('click', () => {
  jobLogPage = Math.max(1, jobLogPage - 1);
  loadJobLogs().catch((error) => setStatus(error.message, true));
});
jobNextPageButton.addEventListener('click', () => {
  jobLogPage += 1;
  loadJobLogs().catch((error) => setStatus(error.message, true));
});
serviceStatusFilters.addEventListener('change', () => {
  serviceLogPage = 1;
  loadServiceLogs().catch((error) => setStatus(error.message, true));
});
schedulerStatusFilters.addEventListener('change', () => {
  schedulerLogPage = 1;
  loadSchedulerRuntimeLogs().catch((error) => setStatus(error.message, true));
});
mlStatusFilters.addEventListener('change', () => {
  mlLogPage = 1;
  loadMlRuntimeLogs().catch((error) => setStatus(error.message, true));
});
jobRuntimeStatusFilters.addEventListener('change', () => {
  jobRuntimeLogPage = 1;
  loadJobRuntimeLogs().catch((error) => setStatus(error.message, true));
});
jobStatusFilters.addEventListener('change', () => {
  jobLogPage = 1;
  loadJobLogs().catch((error) => setStatus(error.message, true));
});
schedulerLogSearchInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    schedulerLogPage = 1;
    loadSchedulerRuntimeLogs().catch((error) => setStatus(error.message, true));
  }
});
mlLogSearchInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    mlLogPage = 1;
    loadMlRuntimeLogs().catch((error) => setStatus(error.message, true));
  }
});
jobRuntimeLogSearchInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    jobRuntimeLogPage = 1;
    loadJobRuntimeLogs().catch((error) => setStatus(error.message, true));
  }
});
jobLogIdInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    jobLogPage = 1;
    loadJobLogs().catch((error) => setStatus(error.message, true));
  }
});
jobLogSearchInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    jobLogPage = 1;
    loadJobLogs().catch((error) => setStatus(error.message, true));
  }
});
serviceLogSearchInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    serviceLogPage = 1;
    loadServiceLogs().catch((error) => setStatus(error.message, true));
  }
});

async function initializePanel() {
  await loadClusterSources();
  await Promise.all([
    loadSystemStatus(),
    loadClusterTree(),
    loadResourceTree().catch((error) => setStatus(error.message, true)),
    loadConfigTargets(),
    loadCalendarCatalog(),
    loadForecastModelInsights(),
    loadServiceLogs(),
    loadSchedulerRuntimeLogs(),
    loadMlRuntimeLogs(),
    loadJobRuntimeLogs(),
    loadJobLogs(),
  ]);
  if (systemStatusPollHandle === null) {
    systemStatusPollHandle = window.setInterval(() => {
      loadSystemStatus({ silent: true }).catch(() => {});
    }, 5000);
  }
  if (systemCountdownHandle === null) {
    systemCountdownHandle = window.setInterval(() => {
      refreshSystemCountdownView();
    }, 1000);
  }
  setStatus(t('status.panelLoaded'));
}

initializePanel().catch((error) => setStatus(error.message, true));
