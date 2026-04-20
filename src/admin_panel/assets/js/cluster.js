function renderClusterTreeInto(payload, sourceTextElement, overviewTextElement, metricsElement, overviewMetricsElement, treeElement) {
  treeElement.innerHTML = '';
  sourceTextElement.textContent = t('cluster.sourceTemplate', {
    selected: payload.selected_file,
    created: formatDate(payload.selected_created_at),
    active: payload.scheduler_active_file,
  });
  overviewTextElement.textContent = t('cluster.overviewTemplate', {
    features: payload.feature_count,
    partitions: payload.partition_count,
    nodes: payload.total_nodes,
  });

  renderMetrics(metricsElement, [
    t('metric.features', { count: payload.feature_count }),
    t('metric.partitions', { count: payload.partition_count }),
    t('metric.latestBackup', { value: payload.latest_backup_file || t('label.none') }),
  ]);
  renderMetrics(overviewMetricsElement, [
    t('metric.totalNodes', { count: payload.total_nodes }),
    t('metric.totalCpu', { count: payload.total_cpu_cores }),
    t('metric.totalGpu', { count: payload.total_gpus }),
  ]);

  payload.features.forEach((feature) => {
    const featureDetails = document.createElement('details');
    featureDetails.open = true;

    const featureSummary = document.createElement('summary');
    const featureMain = document.createElement('div');
    featureMain.className = 'summary-main';
    featureMain.innerHTML = `
      <span class="summary-title">${feature.name}</span>
      <span class="summary-subtitle">${t('feature.summarySubtitle', { groups: feature.node_group_count, nodes: feature.nodes.length, partitions: feature.partitions.length })}</span>
    `;
    const featureTags = document.createElement('div');
    featureTags.className = 'summary-tags';
    [
      t('metric.totalCpu', { count: feature.total_cpu_cores }),
      t('metric.totalGpu', { count: feature.total_gpus }),
      t('feature.partitionChip', { value: feature.partitions.join(', ') || t('label.none') }),
    ].forEach((text) => featureTags.appendChild(createChip(text)));
    featureSummary.appendChild(featureMain);
    featureSummary.appendChild(featureTags);
    featureDetails.appendChild(featureSummary);

    const featureBody = document.createElement('div');
    featureBody.className = 'detail-body';

    feature.node_groups.forEach((group) => {
      const groupDetails = document.createElement('details');
      const groupSummary = document.createElement('summary');
      const groupMain = document.createElement('div');
      groupMain.className = 'summary-main';
      groupMain.innerHTML = `
        <span class="summary-title">${group.name_pattern}</span>
        <span class="summary-subtitle">${t('group.summarySubtitle', { count: group.node_count, weight: group.weight })}</span>
      `;
      const groupTags = document.createElement('div');
      groupTags.className = 'summary-tags';
      [
        t('group.cpuPerNode', { count: group.resources.cpu_cores }),
        t('group.gpuPerNode', { count: group.resources.gpus }),
        t('feature.partitionChip', { value: group.partitions.join(', ') || t('label.none') }),
      ].forEach((text) => groupTags.appendChild(createChip(text)));
      groupSummary.appendChild(groupMain);
      groupSummary.appendChild(groupTags);
      groupDetails.appendChild(groupSummary);

      const groupBody = document.createElement('div');
      groupBody.className = 'detail-body';

      if (group.history.length > 0) {
        const historyCard = document.createElement('div');
        historyCard.className = 'card';
        historyCard.innerHTML = `
          <h3>${t('group.historyTitle')}</h3>
          <div class="chips">${group.history.map((item) => `<span class="chip">${t('group.historyChip', { count: item.node_count, start: item.start || 'start', end: item.end || 'open' })}</span>`).join('')}</div>
        `;
        groupBody.appendChild(historyCard);
      }

      const table = document.createElement('table');
      table.className = 'node-table';
      table.innerHTML = `
        <thead>
          <tr>
            <th>${t('table.node')}</th>
            <th>${t('table.partitions')}</th>
            <th>${t('table.resources')}</th>
          </tr>
        </thead>
        <tbody>
          ${group.nodes.map((node) => `
            <tr>
              <td><strong>${node.name}</strong></td>
              <td>${node.partitions.join(', ') || `<span class="muted">${t('label.none')}</span>`}</td>
              <td>${node.cpu_cores} CPU, ${node.gpus} GPU</td>
            </tr>
          `).join('')}
        </tbody>
      `;
      groupBody.appendChild(table);
      groupDetails.appendChild(groupBody);
      featureBody.appendChild(groupDetails);
    });

    featureDetails.appendChild(featureBody);
    treeElement.appendChild(featureDetails);
  });
}

function renderClusterTree(payload) {
  renderClusterTreeInto(payload, clusterSourceText, clusterOverviewText, clusterMetrics, clusterOverviewMetrics, clusterTree);
}

function buildClusterSourceOptionLabel(source) {
  const dateLabel = source.created_at ? formatDate(source.created_at) : t('common.dateUnknown');
  const label = source.kind === 'current'
    ? t('cluster.sourceLabel.current')
    : t('cluster.sourceLabel.backup', { name: source.label.replace(/^Backup\s*/, '') });
  const activeSuffix = source.is_scheduler_active ? ` | ${t('cluster.sourceActive')}` : '';
  return `${label} | ${dateLabel}${activeSuffix}`;
}

function renderClusterSourceControls() {
  const sources = clusterSourcesCatalog?.sources || [];
  clusterSourceSelect.innerHTML = sources.map((source) => `
    <option value="${escapeHtml(source.path)}" ${source.path === selectedClusterSourcePath ? 'selected' : ''}>
      ${escapeHtml(buildClusterSourceOptionLabel(source))}
    </option>
  `).join('');
}

async function loadClusterTree() {
  const params = new URLSearchParams();
  if (selectedClusterSourcePath) {
    params.set('path', selectedClusterSourcePath);
  }
  const payload = await fetchJson(`/api/cluster-tree?${params.toString()}`);
  clusterPayload = payload;
  renderClusterTree(payload);
}

async function loadClusterSources() {
  const payload = await fetchJson('/api/cluster-sources');
  clusterSourcesCatalog = payload;
  if (!selectedClusterSourcePath) {
    selectedClusterSourcePath = payload.default_path;
  }
  renderClusterSourceControls();
}
