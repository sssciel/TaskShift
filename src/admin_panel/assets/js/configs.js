function renderFileButtons() {
  fileToolbar.innerHTML = '';
  configTargets.forEach((target) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'file-button';
    button.classList.toggle('active', target.id === selectedConfigId);
    button.textContent = configTargetLabel(target);
    button.addEventListener('click', () => loadConfigFile(target.id));
    fileToolbar.appendChild(button);
  });
}

async function loadConfigTargets() {
  const payload = await fetchJson('/api/config-targets');
  configTargets = payload.targets;
  if (!selectedConfigId && configTargets.length > 0) {
    selectedConfigId = configTargets[0].id;
  }
  renderFileButtons();
  if (selectedConfigId) {
    await loadConfigFile(selectedConfigId);
  }
}

async function loadConfigFile(targetId) {
  selectedConfigId = targetId;
  renderFileButtons();
  setStatus(t('status.loadingConfig', { target: targetId }));
  const payload = await fetchJson(`/api/config-targets/${encodeURIComponent(targetId)}`);
  editorPath.textContent = payload.path;
  configEditor.value = payload.content;
  setStatus(t('status.loadedConfig', { label: configTargetLabel(payload) }));
}

async function saveConfigFile() {
  if (!selectedConfigId) {
    setStatus('No config file selected.', true);
    return;
  }

  setStatus(t('status.savingConfig', { target: selectedConfigId }));
  const payload = await fetchJson(`/api/config-targets/${encodeURIComponent(selectedConfigId)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content: configEditor.value }),
  });
  editorPath.textContent = payload.path;
  setStatus(t('status.savedConfig', { label: configTargetLabel(payload) }));

  if (selectedConfigId === 'cluster_active') {
    await loadClusterSources();
    await loadClusterTree();
  }
  if (selectedConfigId === 'server') {
    await loadConfigTargets();
  }
}
