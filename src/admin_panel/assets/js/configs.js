function renderFileButtons() {
  fileToolbar.innerHTML = "";
  configTargets.forEach((target) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "file-button";
    button.classList.toggle("active", target.id === selectedConfigId);
    button.textContent = configTargetLabel(target);
    button.addEventListener("click", () => loadConfigFile(target.id));
    fileToolbar.appendChild(button);
  });
}

async function loadConfigTargets() {
  const payload = await fetchJson("/api/config-targets");
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
  setStatus(t("status.loadingConfig", { target: targetId }));
  try {
    const payload = await fetchJson(
      `/api/config-targets/${encodeURIComponent(targetId)}`,
    );
    editorPath.textContent = payload.path;
    configEditor.value = payload.content;
    setStatus(t("status.loadedConfig", { label: configTargetLabel(payload) }));
    showToast(
      t("status.loadedConfig", { label: configTargetLabel(payload) }),
      "info",
    );
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

async function saveConfigFile() {
  if (!selectedConfigId) {
    setStatus("No configuration file selected.", true);
    showToast("No configuration file selected.", "error");
    return;
  }

  setStatus(t("status.savingConfig", { target: selectedConfigId }));
  try {
    const payload = await fetchJson(
      `/api/config-targets/${encodeURIComponent(selectedConfigId)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content: configEditor.value }),
      },
    );
    editorPath.textContent = payload.path;
    setStatus(t("status.savedConfig", { label: configTargetLabel(payload) }));
    showToast(
      t("status.savedConfig", { label: configTargetLabel(payload) }),
      "success",
    );
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }

  if (selectedConfigId === "server") {
    await loadConfigTargets();
  }
}
