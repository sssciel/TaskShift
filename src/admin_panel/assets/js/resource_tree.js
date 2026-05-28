function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return t("label.none");
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value);
  }
  return `${numeric.toFixed(1)}%`;
}

function renderResourceBar(usedPercent) {
  const safePercent = Math.max(0, Math.min(100, Number(usedPercent || 0)));
  return `
    <div class="resource-bar">
      <span style="width: ${safePercent}%"></span>
    </div>
  `;
}

function renderResourceMetricPair(label, used, available, total, usedPercent) {
  return `
    <div class="resource-meter">
      <div class="resource-meter-header">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(formatPercent(usedPercent))}</span>
      </div>
      ${renderResourceBar(usedPercent)}
      <div class="resource-meter-values">
        <span>${t("resources.used", { value: used })}</span>
        <span>${t("resources.free", { value: available })}</span>
        <span>${t("resources.total", { value: total })}</span>
      </div>
    </div>
  `;
}

function renderResourceTree(payload) {
  resourceTreePayload = payload;
  resourceSnapshotText.textContent = t("resources.snapshotTemplate", {
    generated: formatDate(payload.generated_at),
    running: payload.running_job_count,
  });
  resourceTotalsText.textContent = t("resources.totalsTemplate", {
    features: payload.feature_count,
    nodes: payload.node_count,
  });
  renderMetrics(resourceMetrics, [
    t("metric.runningJobs", { count: payload.running_job_count }),
    t("metric.features", { count: payload.feature_count }),
    t("metric.totalNodes", { count: payload.node_count }),
  ]);
  renderMetrics(resourceTotalMetrics, [
    t("resources.cpuSummary", {
      free: payload.available_cpu,
      used: payload.used_cpu,
      total: payload.total_cpu,
      percent: formatPercent(payload.cpu_used_percent),
    }),
    t("resources.gpuSummary", {
      free: payload.available_gpu,
      used: payload.used_gpu,
      total: payload.total_gpu,
      percent: formatPercent(payload.gpu_used_percent),
    }),
  ]);

  resourceTree.innerHTML = "";
  if (!payload.features || payload.features.length === 0) {
    resourceTree.innerHTML = `<div class="empty-state">${t("resources.empty")}</div>`;
    return;
  }

  payload.features.forEach((feature) => {
    const featureDetails = document.createElement("details");
    featureDetails.open = true;
    const featureSummary = document.createElement("summary");
    const featureMain = document.createElement("div");
    featureMain.className = "summary-main";
    featureMain.innerHTML = `
      <span class="summary-title">${escapeHtml(feature.name)}</span>
      <span class="summary-subtitle">${escapeHtml(t("resources.featureSubtitle", {
        nodes: feature.node_count,
        cpu: `${feature.available_cpu}/${feature.total_cpu}`,
        gpu: `${feature.available_gpu}/${feature.total_gpu}`,
      }))}</span>
    `;
    const featureTags = document.createElement("div");
    featureTags.className = "summary-tags";
    [
      t("resources.cpuUsedPercent", { value: formatPercent(feature.cpu_used_percent) }),
      t("resources.gpuUsedPercent", { value: formatPercent(feature.gpu_used_percent) }),
    ].forEach((text) => featureTags.appendChild(createChip(text)));
    featureSummary.appendChild(featureMain);
    featureSummary.appendChild(featureTags);
    featureDetails.appendChild(featureSummary);

    const featureBody = document.createElement("div");
    featureBody.className = "detail-body";
    const featureMeters = document.createElement("div");
    featureMeters.className = "resource-meter-grid";
    featureMeters.innerHTML = [
      renderResourceMetricPair("CPU", feature.used_cpu, feature.available_cpu, feature.total_cpu, feature.cpu_used_percent),
      renderResourceMetricPair("GPU", feature.used_gpu, feature.available_gpu, feature.total_gpu, feature.gpu_used_percent),
    ].join("");
    featureBody.appendChild(featureMeters);

    const table = document.createElement("table");
    table.className = "node-table resource-node-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>${t("table.node")}</th>
          <th>CPU</th>
          <th>GPU</th>
        </tr>
      </thead>
      <tbody>
        ${feature.nodes.map((node) => `
          <tr>
            <td><strong>${escapeHtml(node.name)}</strong></td>
            <td>
              ${renderResourceMetricPair("CPU", node.used_cpu, node.available_cpu, node.total_cpu, node.cpu_used_percent)}
            </td>
            <td>
              ${renderResourceMetricPair("GPU", node.used_gpu, node.available_gpu, node.total_gpu, node.gpu_used_percent)}
            </td>
          </tr>
        `).join("")}
      </tbody>
    `;
    featureBody.appendChild(table);
    featureDetails.appendChild(featureBody);
    resourceTree.appendChild(featureDetails);
  });
}

async function loadResourceTree() {
  setStatus(t("status.loadingResources"));
  const payload = await fetchJson("/api/resource-tree");
  renderResourceTree(payload);
  setStatus(t("status.loadedResources"));
}
