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
reloadClusterSourceButton.addEventListener('click', () => loadClusterTree().catch((error) => setStatus(error.message, true)));
clusterSourceSelect.addEventListener('change', () => {
  selectedClusterSourcePath = clusterSourceSelect.value;
  loadClusterTree().catch((error) => setStatus(error.message, true));
});
reloadTaskshiftLogButton.addEventListener('click', () => loadTaskshiftLogs().catch((error) => setStatus(error.message, true)));
reloadJobLogButton.addEventListener('click', () => loadJobLogs().catch((error) => setStatus(error.message, true)));
taskshiftPrevPageButton.addEventListener('click', () => {
  taskshiftLogPage = Math.max(1, taskshiftLogPage - 1);
  loadTaskshiftLogs().catch((error) => setStatus(error.message, true));
});
taskshiftNextPageButton.addEventListener('click', () => {
  taskshiftLogPage += 1;
  loadTaskshiftLogs().catch((error) => setStatus(error.message, true));
});
jobPrevPageButton.addEventListener('click', () => {
  jobLogPage = Math.max(1, jobLogPage - 1);
  loadJobLogs().catch((error) => setStatus(error.message, true));
});
jobNextPageButton.addEventListener('click', () => {
  jobLogPage += 1;
  loadJobLogs().catch((error) => setStatus(error.message, true));
});
taskshiftStatusFilters.addEventListener('change', () => {
  taskshiftLogPage = 1;
  loadTaskshiftLogs().catch((error) => setStatus(error.message, true));
});
jobStatusFilters.addEventListener('change', () => {
  jobLogPage = 1;
  loadJobLogs().catch((error) => setStatus(error.message, true));
});
taskshiftLogSearchInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    taskshiftLogPage = 1;
    loadTaskshiftLogs().catch((error) => setStatus(error.message, true));
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

async function initializePanel() {
  await loadClusterSources();
  await Promise.all([
    loadSystemStatus(),
    loadClusterTree(),
    loadConfigTargets(),
    loadCalendarCatalog(),
    loadTaskshiftLogs(),
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
