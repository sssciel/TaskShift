function buildSystemMetrics(payload) {
  const service = payload.service || {};
  const lastRun = payload.last_run || {};
  const controls = payload.controls || {};
  return [
    t("metric.schedulerState", { value: getSystemStatusLabel(service.status) }),
    t("metric.nextRun", {
      value:
        service.running && service.next_run_at
          ? formatDate(service.next_run_at)
          : t("label.none"),
    }),
    t("metric.countdown", {
      value:
        service.running && service.next_run_at
          ? formatCountdown(service.countdown_seconds)
          : t("label.none"),
    }),
    t("metric.maxLaunch", {
      value: controls.default_max_launched_jobs ?? t("label.none"),
    }),
    t("metric.pendingQueue", { count: lastRun.pending_job_count || 0 }),
    t("metric.attemptedQueue", {
      count: (lastRun.attempted_job_ids || []).length,
    }),
    t("metric.runningJobs", { count: lastRun.running_job_count || 0 }),
    t("metric.failedPool", { count: lastRun.failed_job_pool_size || 0 }),
  ];
}

function refreshSystemCountdownView() {
  if (!systemStatusPayload) {
    return;
  }

  const service = systemStatusPayload.service || {};
  if (service.status === "running") {
    systemCountdownText.textContent = t("system.countdownRunning");
    renderMetrics(systemStatusMetrics, buildSystemMetrics(systemStatusPayload));
    return;
  }

  if (!service.running || !service.next_run_at) {
    systemCountdownText.textContent = t("system.countdownInactive");
    renderMetrics(systemStatusMetrics, buildSystemMetrics(systemStatusPayload));
    return;
  }

  const nextRunAt = new Date(service.next_run_at);
  const seconds = Number.isNaN(nextRunAt.getTime())
    ? null
    : Math.max(0, Math.floor((nextRunAt.getTime() - Date.now()) / 1000));

  systemStatusPayload.service.countdown_seconds = seconds;
  systemCountdownText.textContent = t("system.countdownTemplate", {
    value: formatCountdown(seconds),
    at: formatDate(service.next_run_at),
  });
  renderMetrics(systemStatusMetrics, buildSystemMetrics(systemStatusPayload));
}

function renderSystemStatus(payload) {
  systemStatusPayload = payload;

  const service = payload.service || {};
  const lastRun = payload.last_run || {};
  const controls = payload.controls || {};
  let summaryKey = `system.summary.${service.status || "idle"}`;
  if (
    !TRANSLATIONS[currentLanguage]?.[summaryKey] &&
    !TRANSLATIONS.en?.[summaryKey]
  ) {
    summaryKey = "system.summary.idle";
  }
  systemStatusSummary.textContent = t(summaryKey);

  if (service.status === "running") {
    systemCountdownText.textContent = t("system.countdownRunning");
  } else if (!service.running || !service.next_run_at) {
    systemCountdownText.textContent = t("system.countdownInactive");
  } else {
    systemCountdownText.textContent = t("system.countdownTemplate", {
      value: formatCountdown(service.countdown_seconds),
      at: formatDate(service.next_run_at),
    });
  }

  if (lastRun.finished_at) {
    systemLastRunText.textContent = t("system.lastRunTemplate", {
      status: getSystemStatusLabel(
        lastRun.error_kind === "db_connection"
          ? "db_connection_error"
          : lastRun.status,
      ),
      trigger: getSystemTriggerLabel(lastRun.trigger),
      finished: formatDate(lastRun.finished_at),
      duration: formatDuration(lastRun.duration_seconds),
    });
  } else {
    systemLastRunText.textContent = t("system.lastRunNever");
  }

  systemQueueSummary.textContent = t("system.queueSummaryTemplate", {
    pending: lastRun.pending_job_count || 0,
    attempted: (lastRun.attempted_job_ids || []).length,
    running: lastRun.running_job_count || 0,
  });

  renderMetrics(systemStatusMetrics, buildSystemMetrics(payload));

  manualMaxLaunchedJobsInput.placeholder = String(
    controls.default_max_launched_jobs ?? "",
  );
  runSchedulerNowButton.disabled =
    !controls.can_run_now || service.status === "running";
  runSchedulerNowButton.title = controls.can_run_now
    ? ""
    : t("system.manualRunUnavailable");

  const attemptedEntries = (lastRun.pending_jobs || []).filter(
    (entry) => entry.was_attempted,
  );
  systemQueueList.innerHTML = "";
  if (attemptedEntries.length === 0) {
    systemQueueList.innerHTML = `<div class="empty-state">${escapeHtml(t("system.queueEmpty"))}</div>`;
  } else {
    attemptedEntries.forEach((entry) => {
      const item = document.createElement("article");
      item.className = "queue-entry";
      if (entry.was_attempted) {
        item.classList.add("attempted");
      }
      if (entry.in_failed_attempt_pool) {
        item.classList.add("blocked");
      }
      item.innerHTML = `
        <div class="queue-entry-header">
          <div class="queue-entry-title">
            <strong>${escapeHtml(t("job.label", { id: entry.job_id || "unknown" }))}</strong>
            <div class="queue-entry-meta">${escapeHtml(entry.job_name || "Unnamed job")}</div>
          </div>
          <span class="log-level ${escapeHtml(entry.status || "PENDING")}">${escapeHtml(getQueueStatusLabel(entry.status || "PENDING"))}</span>
        </div>
        <div class="chips" style="margin-bottom: 10px;">
          <span class="chip">${escapeHtml(t("queue.partition", { value: entry.partition || t("label.none") }))}</span>
          <span class="chip">${escapeHtml(
            t("queue.resources", {
              cpus: entry.requested_cpus || 0,
              gpus: entry.requested_gpus || 0,
              nodes: entry.requested_nodes || 0,
              timelimit: entry.timelimit_minutes || 0,
            }),
          )}</span>
          <span class="chip">${escapeHtml(t("queue.constraints", { value: entry.constraints || t("label.none") }))}</span>
        </div>
      `;
      systemQueueList.appendChild(item);
    });
  }

  const hasError = Boolean(lastRun.error_message || lastRun.error_traceback);
  systemErrorDetails.hidden = !hasError;
  if (hasError) {
    systemErrorDetails.open = true;
    systemErrorLog.textContent =
      lastRun.error_traceback || lastRun.error_message || "";
  } else {
    systemErrorDetails.open = false;
    systemErrorLog.textContent = "";
  }

  refreshSystemCountdownView();
}

async function loadSystemStatus({ silent = false } = {}) {
  if (!silent) {
    setStatus(t("status.loadingSystemStatus"));
  }

  const payload = await fetchJson("/api/system-status");
  renderSystemStatus(payload);

  if (!silent) {
    setStatus(t("status.loadedSystemStatus"));
  }
}

async function runSchedulerNow() {
  setStatus(t("status.requestingManualRun"));
  try {
    const rawValue = manualMaxLaunchedJobsInput.value.trim();
    await fetchJson("/api/system-status/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        max_launched_jobs: rawValue ? Number(rawValue) : null,
      }),
    });
    setStatus(t("status.manualRunRequested"));
    showToast(t("status.manualRunRequested"), "success");
    window.setTimeout(() => {
      loadSystemStatus({ silent: true }).catch(() => {});
    }, 300);
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}
