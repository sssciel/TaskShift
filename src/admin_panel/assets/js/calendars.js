function renderCalendarYearSelectors() {
  const yearOptions = (calendarCatalog?.years || []).map((entry) => entry.year);
  const buildOptions = (includeEmptyLabel) => {
    const options = [];
    if (includeEmptyLabel) {
      options.push(
        `<option value="">${escapeHtml(t("cal.noSourceYear"))}</option>`,
      );
    }
    return options
      .concat(
        yearOptions.map((year) => `<option value="${year}">${year}</option>`),
      )
      .join("");
  };

  copyCalendarYearSelect.innerHTML = buildOptions(true);
  templateCalendarYearSelect.innerHTML = buildOptions(true);

  if (!copyCalendarYearSelect.value && yearOptions.length > 0) {
    copyCalendarYearSelect.value = yearOptions[yearOptions.length - 1];
  }
  if (selectedCalendarYear) {
    templateCalendarYearSelect.value = selectedCalendarYear;
  }
}

function renderCalendarYears() {
  const years = calendarCatalog?.years || [];
  calendarYearList.innerHTML = "";
  years.forEach((entry) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "year-button";
    button.classList.toggle("active", entry.year === selectedCalendarYear);
    button.innerHTML = `
      <div class="year-header">
        <span>${entry.year}</span>
        <span class="chip">${entry.files.length}</span>
      </div>
      <div class="year-meta">
        ${entry.files.join(", ") || t("cal.noFilesYet")}<br>
        ${entry.missing_known_files.length > 0 ? t("cal.missingKnownFiles", { files: entry.missing_known_files.join(", ") }) : t("cal.noMissingKnownFiles")}
      </div>
    `;
    button.addEventListener("click", () => {
      selectedCalendarYear = entry.year;
      if (!entry.files.includes(selectedCalendarFile)) {
        selectedCalendarFile = entry.files[0] || null;
      }
      renderCalendarYearSelectors();
      renderCalendarYears();
      renderCalendarFiles();
      if (selectedCalendarYear && selectedCalendarFile) {
        loadCalendarFile(selectedCalendarYear, selectedCalendarFile).catch(
          (error) => setStatus(error.message, true),
        );
      } else {
        calendarEditorMeta.textContent = t("cal.yearNoFiles", {
          year: selectedCalendarYear,
        });
        calendarEditor.value = "";
      }
    });
    calendarYearList.appendChild(button);
  });
}

function renderCalendarFiles() {
  const yearEntry = (calendarCatalog?.years || []).find(
    (entry) => entry.year === selectedCalendarYear,
  );
  calendarFileList.innerHTML = "";
  if (!yearEntry) {
    return;
  }

  const allFiles = [...yearEntry.files];
  yearEntry.missing_known_files.forEach((filename) => {
    if (!allFiles.includes(filename)) {
      allFiles.push(filename);
    }
  });
  allFiles.sort();

  allFiles.forEach((filename) => {
    const exists = yearEntry.files.includes(filename);
    const button = document.createElement("button");
    button.type = "button";
    button.className = "calendar-file-button";
    button.classList.toggle("active", filename === selectedCalendarFile);
    button.innerHTML = `
      <div class="year-header">
        <span>${filename}</span>
        <span class="chip">${exists ? t("cal.filePresent") : t("cal.fileMissing")}</span>
      </div>
      <div class="year-meta">${exists ? t("cal.filePresentMeta") : t("cal.fileMissingMeta")}</div>
    `;
    if (exists) {
      button.addEventListener("click", () => {
        selectedCalendarFile = filename;
        renderCalendarFiles();
        loadCalendarFile(selectedCalendarYear, selectedCalendarFile).catch(
          (error) => setStatus(error.message, true),
        );
      });
    }
    calendarFileList.appendChild(button);
  });
}

function renderCalendarOverview() {
  const years = calendarCatalog?.years || [];
  renderMetrics(calendarOverviewMetrics, [
    t("metric.years", { count: years.length }),
    t("metric.knownFiles", {
      count: (calendarCatalog?.known_files || []).length,
    }),
    t("metric.root", { value: calendarCatalog?.root || "n/a" }),
  ]);
  renderCalendarYearSelectors();
  renderCalendarYears();
  renderCalendarFiles();
}

async function loadCalendarCatalog() {
  const payload = await fetchJson("/api/calendar-years");
  calendarCatalog = payload;
  if (!selectedCalendarYear && payload.years.length > 0) {
    selectedCalendarYear = payload.years[payload.years.length - 1].year;
  }
  const selectedYearEntry = payload.years.find(
    (entry) => entry.year === selectedCalendarYear,
  );
  if (
    selectedYearEntry &&
    (!selectedCalendarFile ||
      !selectedYearEntry.files.includes(selectedCalendarFile))
  ) {
    selectedCalendarFile = selectedYearEntry.files[0] || null;
  }
  renderCalendarOverview();
  if (selectedCalendarYear && selectedCalendarFile) {
    await loadCalendarFile(selectedCalendarYear, selectedCalendarFile);
  } else {
    calendarEditorMeta.textContent = payload.root;
    calendarEditor.value = "";
  }
}

async function loadCalendarFile(year, filename) {
  setStatus(t("status.loadingCalendar", { year, filename }));
  const payload = await fetchJson(
    `/api/calendar-years/${encodeURIComponent(year)}/files/${encodeURIComponent(filename)}`,
  );
  selectedCalendarYear = payload.year;
  selectedCalendarFile = payload.filename;
  renderCalendarYearSelectors();
  renderCalendarYears();
  renderCalendarFiles();
  calendarEditorMeta.textContent = payload.path;
  calendarEditor.value = payload.content;
  setStatus(
    t("status.loadedCalendar", {
      year: payload.year,
      filename: payload.filename,
    }),
  );
}

async function saveCalendarFile() {
  if (!selectedCalendarYear || !selectedCalendarFile) {
    setStatus(t("error.noCalendarSelected"), true);
    showToast(t("error.noCalendarSelected"), "error");
    return;
  }

  setStatus(
    t("status.savingCalendar", {
      year: selectedCalendarYear,
      filename: selectedCalendarFile,
    }),
  );
  try {
    const payload = await fetchJson(
      `/api/calendar-years/${encodeURIComponent(selectedCalendarYear)}/files/${encodeURIComponent(selectedCalendarFile)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content: calendarEditor.value }),
      },
    );
    calendarEditorMeta.textContent = payload.path;
    setStatus(
      t("status.savedCalendar", {
        year: payload.year,
        filename: payload.filename,
      }),
    );
    showToast(
      t("status.savedCalendar", {
        year: payload.year,
        filename: payload.filename,
      }),
      "success",
    );
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
  await loadCalendarCatalog();
}

async function createCalendarYear() {
  const year = newCalendarYearInput.value.trim();
  if (!year) {
    setStatus(t("error.enterCalendarYear"), true);
    showToast(t("error.enterCalendarYear"), "error");
    return;
  }

  setStatus(t("status.creatingCalendarYear", { year }));
  try {
    await fetchJson("/api/calendar-years", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        year,
        copy_from_year: copyCalendarYearSelect.value || null,
      }),
    });
    newCalendarYearInput.value = "";
    selectedCalendarYear = year;
    selectedCalendarFile = null;
    await loadCalendarCatalog();
    setStatus(t("status.createdCalendarYear", { year }));
    showToast(t("status.createdCalendarYear", { year }), "success");
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}

async function createCalendarFile() {
  if (!selectedCalendarYear) {
    setStatus(t("error.selectYearBeforeFile"), true);
    showToast(t("error.selectYearBeforeFile"), "error");
    return;
  }

  const filename = newCalendarFileInput.value.trim();
  if (!filename) {
    setStatus(t("error.enterCalendarFilename"), true);
    showToast(t("error.enterCalendarFilename"), "error");
    return;
  }

  setStatus(
    t("status.creatingCalendarFile", { year: selectedCalendarYear, filename }),
  );
  try {
    await fetchJson(
      `/api/calendar-years/${encodeURIComponent(selectedCalendarYear)}/files`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filename,
          copy_from_year: templateCalendarYearSelect.value || null,
        }),
      },
    );
    newCalendarFileInput.value = "";
    selectedCalendarFile = filename;
    await loadCalendarCatalog();
    setStatus(
      t("status.createdCalendarFile", { year: selectedCalendarYear, filename }),
    );
    showToast(
      t("status.createdCalendarFile", { year: selectedCalendarYear, filename }),
      "success",
    );
  } catch (error) {
    showToast(error.message, "error");
    throw error;
  }
}
