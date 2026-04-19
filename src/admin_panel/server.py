import json
import logging
import re
import secrets
import shutil
import threading
from copy import deepcopy
from datetime import datetime
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import (
    DBConfigFile,
    ClusterConfig,
    clusterConfigFile,
    getAdminPanelAccessConfig,
    getClusterConfig,
    getLatestClusterConfigBackupFile,
    getLatestClusterConfigFile,
    getSchedulerConfig,
    getServerConfig,
    schedulerConfigFile,
    serverConfigFile,
)
from config.models import get_yaml_module
from config.parsing import expand_hostlist
from config.paths import academicCalendarRoot
from scheduler.cron import SCHEDULER_INTERVAL_MINUTES, get_scheduler_service_status
from scheduler.runtime_state import SchedulerRuntimeStateStore


PANEL_COOKIE_NAME = "taskshift_admin_token"
TASKSHIFT_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} [0-9:.,]+) \| (?P<level>[A-Z]+) \| (?P<source>[^|]+) \| (?P<message>.*)$"
)


LOGIN_PAGE_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TaskShift Admin</title>
  <style>
    :root {
      --bg: #f6faff;
      --panel: rgba(255, 255, 255, 0.96);
      --panel-strong: #ffffff;
      --ink: #0d1117;
      --muted: #4b5b70;
      --accent: #1976d2;
      --accent-strong: #0f4fa8;
      --line: rgba(25, 118, 210, 0.18);
      --shadow: 0 20px 52px rgba(15, 35, 65, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background:
        radial-gradient(circle at top left, rgba(25, 118, 210, 0.1), transparent 34%),
        radial-gradient(circle at bottom right, rgba(64, 156, 255, 0.1), transparent 30%),
        linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
    }
    .shell {
      width: min(460px, calc(100vw - 32px));
      padding: 32px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }
    .eyebrow {
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(25, 118, 210, 0.1);
      color: var(--accent-strong);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-weight: 700;
    }
    h1 {
      margin: 18px 0 12px;
      font-size: clamp(30px, 4vw, 42px);
      line-height: 1.02;
      letter-spacing: -0.04em;
    }
    p {
      margin: 0 0 22px;
      color: var(--muted);
      line-height: 1.55;
      font-size: 15px;
    }
    form {
      display: grid;
      gap: 14px;
    }
    label {
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    input {
      width: 100%;
      padding: 16px 18px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
      color: var(--ink);
      font-size: 16px;
      outline: none;
    }
    input:focus {
      border-color: rgba(25, 118, 210, 0.58);
      box-shadow: 0 0 0 4px rgba(25, 118, 210, 0.14);
    }
    button {
      border: 0;
      border-radius: 16px;
      padding: 15px 18px;
      background: linear-gradient(135deg, var(--accent) 0%, #49a3ff 100%);
      color: white;
      font-size: 15px;
      font-weight: 800;
      letter-spacing: 0.02em;
      cursor: pointer;
      box-shadow: 0 14px 28px rgba(25, 118, 210, 0.22);
    }
    .error {
      margin-top: 14px;
      min-height: 20px;
      color: #8d2d1f;
      font-size: 14px;
    }
    .lang-row {
      display: flex;
      justify-content: flex-end;
      margin-bottom: 12px;
    }
    .lang-row select {
      width: auto;
      min-width: 120px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
      color: var(--ink);
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="lang-row">
      <select id="loginLanguageSelect">
        <option value="ru">Русский</option>
        <option value="en">English</option>
      </select>
    </div>
    <div class="eyebrow" id="loginBadge">TaskShift Admin</div>
    <h1 id="loginTitle">Cluster control room</h1>
    <p id="loginDescription">Enter the admin access token from <code>configs/.env</code> to inspect the active cluster snapshot and edit config files.</p>
    <form method="post" action="/login">
      <div>
        <label for="token" id="loginTokenLabel">Access token</label>
        <input id="token" name="token" type="password" autocomplete="current-password" placeholder="ADMIN_PANEL_TOKEN" required>
      </div>
      <button type="submit" id="loginSubmitButton">Open panel</button>
    </form>
    <div class="error" id="loginError">__ERROR_MESSAGE__</div>
  </div>
  <script>
    const loginLanguageSelect = document.getElementById('loginLanguageSelect');
    const loginTranslations = {
      ru: {
        title: 'Панель управления кластером',
        description: 'Введите токен администратора из <code>configs/.env</code>, чтобы просматривать активный снимок кластера и редактировать конфиги.',
        tokenLabel: 'Токен доступа',
        submit: 'Открыть панель',
        invalidToken: 'Неверный токен.',
      },
      en: {
        title: 'Cluster control room',
        description: 'Enter the admin access token from <code>configs/.env</code> to inspect the active cluster snapshot and edit config files.',
        tokenLabel: 'Access token',
        submit: 'Open panel',
        invalidToken: 'Invalid token.',
      },
    };

    function getLoginLanguage() {
      const stored = localStorage.getItem('taskshift-admin-language');
      if (stored === 'ru' || stored === 'en') {
        return stored;
      }
      return (navigator.language || '').toLowerCase().startsWith('ru') ? 'ru' : 'en';
    }

    function applyLoginLanguage(language) {
      const tr = loginTranslations[language] || loginTranslations.en;
      document.documentElement.lang = language;
      document.getElementById('loginTitle').textContent = tr.title;
      document.getElementById('loginDescription').innerHTML = tr.description;
      document.getElementById('loginTokenLabel').textContent = tr.tokenLabel;
      document.getElementById('loginSubmitButton').textContent = tr.submit;
      const errorNode = document.getElementById('loginError');
      if ((errorNode.textContent || '').trim() === loginTranslations.en.invalidToken) {
        errorNode.textContent = tr.invalidToken;
      }
      loginLanguageSelect.value = language;
    }

    const loginLanguage = getLoginLanguage();
    applyLoginLanguage(loginLanguage);
    loginLanguageSelect.addEventListener('change', () => {
      localStorage.setItem('taskshift-admin-language', loginLanguageSelect.value);
      applyLoginLanguage(loginLanguageSelect.value);
    });
  </script>
</body>
</html>
"""


APP_PAGE_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TaskShift Admin</title>
  <style>
    :root {
      --bg: #f6faff;
      --paper: rgba(255, 255, 255, 0.96);
      --paper-strong: #ffffff;
      --ink: #0d1117;
      --muted: #4f6075;
      --line: rgba(21, 101, 192, 0.16);
      --accent: #1976d2;
      --accent-strong: #0f4fa8;
      --accent-soft: rgba(25, 118, 210, 0.08);
      --steel: #1658a6;
      --shadow: 0 22px 54px rgba(15, 35, 65, 0.1);
      --mono: "SFMono-Regular", "IBM Plex Mono", "Menlo", monospace;
      --sans: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(25, 118, 210, 0.08), transparent 24%),
        radial-gradient(circle at right 20%, rgba(73, 163, 255, 0.09), transparent 28%),
        linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
      font-family: var(--sans);
    }
    .layout {
      min-height: 100%;
      display: grid;
      grid-template-columns: 280px 1fr;
    }
    .sidebar {
      padding: 28px 22px;
      border-right: 1px solid var(--line);
      background: rgba(250, 252, 255, 0.92);
      backdrop-filter: blur(12px);
    }
    .brand {
      display: grid;
      gap: 12px;
      margin-bottom: 28px;
    }
    .brand-badge {
      width: fit-content;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent-strong);
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .brand h1 {
      margin: 0;
      font-size: 34px;
      line-height: 0.98;
      letter-spacing: -0.05em;
    }
    .brand p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.55;
    }
    .tab-list {
      display: grid;
      gap: 10px;
      margin-bottom: 22px;
    }
    .language-switch {
      display: flex;
      justify-content: flex-end;
      margin-bottom: 18px;
    }
    .language-switch select {
      width: auto;
      min-width: 128px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
      color: var(--ink);
      font-size: 14px;
      outline: none;
    }
    .tab-button {
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.88);
      border-radius: 16px;
      padding: 14px 14px 14px 16px;
      text-align: left;
      cursor: pointer;
      transition: transform 160ms ease, border-color 160ms ease, background 160ms ease;
    }
    .tab-button:hover {
      transform: translateY(-1px);
      border-color: rgba(25, 118, 210, 0.36);
      background: rgba(244, 249, 255, 0.96);
    }
    .tab-button.active {
      background: linear-gradient(135deg, rgba(25, 118, 210, 0.14), rgba(73, 163, 255, 0.08));
      border-color: rgba(25, 118, 210, 0.44);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
    }
    .tab-label {
      display: block;
      font-weight: 800;
      color: var(--ink);
      margin-bottom: 4px;
    }
    .tab-caption {
      display: block;
      font-size: 13px;
      color: var(--muted);
      line-height: 1.4;
    }
    .side-card {
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.9);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.45);
      margin-bottom: 14px;
    }
    .side-card h3 {
      margin: 0 0 8px;
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    .side-card p, .side-card code {
      margin: 0;
      font-size: 13px;
      line-height: 1.5;
    }
    .side-card code { font-family: var(--mono); }
    .logout {
      width: 100%;
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: white;
      padding: 12px 14px;
      font-weight: 700;
      cursor: pointer;
    }
    .content {
      padding: 24px;
      display: grid;
      gap: 18px;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .topbar h2 {
      margin: 0;
      font-size: clamp(28px, 4vw, 44px);
      letter-spacing: -0.05em;
      line-height: 0.96;
    }
    .topbar p {
      margin: 0;
      color: var(--muted);
    }
    .status {
      padding: 12px 16px;
      border-radius: 14px;
      border: 1px solid rgba(21, 101, 192, 0.14);
      background: rgba(247, 251, 255, 0.95);
      color: var(--steel);
      font-size: 13px;
      min-width: 220px;
    }
    .panel {
      display: none;
      padding: 22px;
      border-radius: 26px;
      border: 1px solid var(--line);
      background: var(--paper);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }
    .panel.active { display: block; }
    .grid {
      display: grid;
      gap: 18px;
    }
    .grid.two {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .card {
      padding: 18px;
      border-radius: 20px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.96);
    }
    .card h3 {
      margin: 0 0 8px;
      font-size: 18px;
      letter-spacing: -0.03em;
    }
    .card p {
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
    }
    .metrics {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 14px;
    }
    .metric {
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(25, 118, 210, 0.08);
      color: var(--accent-strong);
      font-size: 13px;
      font-weight: 700;
    }
    .tree {
      display: grid;
      gap: 12px;
      margin-top: 18px;
    }
    details {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255,255,255,0.98);
      overflow: hidden;
    }
    summary {
      list-style: none;
      cursor: pointer;
      padding: 16px 18px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    summary::-webkit-details-marker { display: none; }
    .summary-main {
      display: grid;
      gap: 4px;
    }
    .summary-title {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: -0.03em;
    }
    .summary-subtitle {
      font-size: 13px;
      color: var(--muted);
    }
    .summary-tags, .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .chip {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(25, 118, 210, 0.08);
      color: var(--steel);
      font-size: 12px;
      font-weight: 700;
    }
    .detail-body {
      padding: 0 18px 18px;
      display: grid;
      gap: 12px;
    }
    .node-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    .node-table th,
    .node-table td {
      padding: 10px 8px;
      border-bottom: 1px solid rgba(24, 33, 43, 0.08);
      text-align: left;
      vertical-align: top;
    }
    .node-table th {
      color: var(--muted);
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 11px;
    }
    .muted { color: var(--muted); }
    .file-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 16px;
    }
    .file-button {
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: white;
      cursor: pointer;
      font-weight: 700;
    }
    .file-button.active {
      border-color: rgba(25, 118, 210, 0.42);
      background: rgba(25, 118, 210, 0.08);
      color: var(--accent-strong);
    }
    .editor-meta {
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 13px;
      font-family: var(--mono);
      word-break: break-all;
    }
    textarea {
      width: 100%;
      min-height: 440px;
      resize: vertical;
      padding: 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: #ffffff;
      color: var(--ink);
      font-family: var(--mono);
      font-size: 13px;
      line-height: 1.55;
      outline: none;
    }
    textarea:focus {
      border-color: rgba(25, 118, 210, 0.42);
      box-shadow: 0 0 0 4px rgba(25, 118, 210, 0.1);
    }
    .actions {
      display: flex;
      gap: 10px;
      margin-top: 14px;
    }
    .stack {
      display: grid;
      gap: 16px;
    }
    .inline-form {
      display: grid;
      gap: 10px;
    }
    .inline-form input,
    .inline-form select {
      width: 100%;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
      color: var(--ink);
      font-size: 14px;
      outline: none;
    }
    .calendar-layout {
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 18px;
      align-items: start;
    }
    .calendar-sidebar {
      display: grid;
      gap: 14px;
    }
    .year-list, .calendar-file-list {
      display: grid;
      gap: 10px;
    }
    .year-button, .calendar-file-button {
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.76);
      border-radius: 16px;
      padding: 12px 14px;
      text-align: left;
      cursor: pointer;
    }
    .year-button.active, .calendar-file-button.active {
      border-color: rgba(25, 118, 210, 0.42);
      background: rgba(25, 118, 210, 0.08);
      color: var(--accent-strong);
    }
    .year-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      font-weight: 800;
      margin-bottom: 4px;
    }
    .year-meta {
      font-size: 12px;
      color: var(--muted);
      line-height: 1.45;
    }
    .calendar-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 12px;
    }
    .calendar-meta {
      display: grid;
      gap: 8px;
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 13px;
      font-family: var(--mono);
      word-break: break-all;
    }
    .snapshot-layout {
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 18px;
      align-items: start;
    }
    .snapshot-list {
      display: grid;
      gap: 10px;
    }
    .snapshot-button {
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.76);
      border-radius: 16px;
      padding: 12px 14px;
      text-align: left;
      cursor: pointer;
    }
    .snapshot-button.active {
      border-color: rgba(25, 118, 210, 0.42);
      background: rgba(25, 118, 210, 0.08);
      color: var(--accent-strong);
    }
    .toolbar {
      display: grid;
      gap: 14px;
      margin-bottom: 16px;
    }
    .toolbar-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }
    .toolbar-row.compact {
      justify-content: space-between;
      align-items: center;
    }
    .toolbar-row input,
    .toolbar-row select {
      min-width: 180px;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
      color: var(--ink);
      font-size: 14px;
      outline: none;
    }
    .status-filter-group {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .status-toggle {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .status-toggle input {
      margin: 0;
      min-width: auto;
    }
    .log-list {
      display: grid;
      gap: 12px;
    }
    .log-entry {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255,255,255,0.8);
      padding: 16px 18px;
      box-shadow: 0 10px 22px rgba(17, 43, 82, 0.05);
    }
    .log-entry-header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }
    .log-entry-title {
      display: grid;
      gap: 4px;
    }
    .log-entry-meta {
      color: var(--muted);
      font-size: 12px;
      font-family: var(--mono);
      word-break: break-word;
    }
    .log-entry-message {
      margin: 0;
      white-space: pre-wrap;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.6;
      color: var(--ink);
    }
    .log-level {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      border: 1px solid var(--line);
      background: rgba(56, 84, 107, 0.08);
      color: var(--steel);
    }
    .log-level.DEBUG { background: rgba(56, 84, 107, 0.08); color: var(--steel); }
    .log-level.INFO { background: rgba(19, 109, 74, 0.08); color: #146345; }
    .log-level.SUCCESS { background: rgba(19, 109, 74, 0.14); color: #146345; }
    .log-level.WARNING { background: rgba(191, 133, 44, 0.16); color: #8f5b00; }
    .log-level.ERROR { background: rgba(159, 43, 43, 0.12); color: #9f2b2b; }
    .log-level.CRITICAL { background: rgba(111, 25, 25, 0.14); color: #6f1919; }
    .log-level.FAILED { background: rgba(159, 43, 43, 0.12); color: #9f2b2b; }
    .log-level.ATTEMPTED { background: rgba(56, 84, 107, 0.08); color: var(--steel); }
    .log-level.SUCCEEDED { background: rgba(19, 109, 74, 0.14); color: #146345; }
    .log-level.LAUNCH_ATTEMPTED { background: rgba(56, 84, 107, 0.08); color: var(--steel); }
    .log-level.LAUNCH_FAILED { background: rgba(159, 43, 43, 0.12); color: #9f2b2b; }
    .log-level.LEFT_PENDING_QUEUE { background: rgba(19, 109, 74, 0.14); color: #146345; }
    .log-level.PENDING { background: rgba(56, 84, 107, 0.08); color: var(--steel); }
    .log-level.SKIPPED_RESOURCES { background: rgba(191, 133, 44, 0.16); color: #8f5b00; }
    .log-level.SKIPPED_TIMELIMIT { background: rgba(111, 25, 25, 0.14); color: #6f1919; }
    .log-level.BLOCKED_FAILED_POOL { background: rgba(159, 43, 43, 0.12); color: #9f2b2b; }
    .system-card {
      display: grid;
      gap: 16px;
      margin-bottom: 18px;
    }
    .system-header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 16px;
      align-items: start;
    }
    .system-header h3 {
      margin-bottom: 6px;
    }
    .system-header p,
    .system-queue-header p,
    .system-pane p {
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
    }
    .system-controls {
      min-width: min(320px, 100%);
      align-content: start;
    }
    .system-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .system-pane {
      padding: 14px 16px;
      border-radius: 18px;
      border: 1px solid rgba(24, 33, 43, 0.08);
      background: rgba(255,255,255,0.6);
    }
    .system-pane h4,
    .system-queue-header h4 {
      margin: 0 0 6px;
      font-size: 13px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .queue-list {
      display: grid;
      gap: 10px;
    }
    .queue-entry {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255,255,255,0.8);
      padding: 14px 16px;
    }
    .queue-entry.attempted {
      border-color: rgba(25, 118, 210, 0.32);
      background: rgba(25, 118, 210, 0.08);
      box-shadow: 0 10px 24px rgba(25, 118, 210, 0.08);
    }
    .queue-entry.blocked {
      border-color: rgba(159, 43, 43, 0.18);
      background: rgba(159, 43, 43, 0.06);
    }
    .queue-entry-header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }
    .queue-entry-title {
      display: grid;
      gap: 4px;
    }
    .queue-entry-meta {
      color: var(--muted);
      font-size: 12px;
      font-family: var(--mono);
      word-break: break-word;
    }
    .runtime-error {
      border: 1px solid rgba(159, 43, 43, 0.14);
      border-radius: 18px;
      background: rgba(159, 43, 43, 0.05);
      padding: 14px 16px;
    }
    .runtime-error summary {
      cursor: pointer;
      font-weight: 800;
      color: #8d2d1f;
    }
    .runtime-error pre {
      margin: 12px 0 0;
      white-space: pre-wrap;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.6;
      color: #6f1919;
    }
    .empty-state {
      padding: 24px;
      border: 1px dashed var(--line);
      border-radius: 18px;
      color: var(--muted);
      text-align: center;
      background: rgba(255,255,255,0.56);
    }
    .primary, .secondary {
      border: 0;
      border-radius: 16px;
      padding: 13px 16px;
      font-weight: 800;
      cursor: pointer;
    }
    .primary {
      background: linear-gradient(135deg, var(--accent) 0%, #49a3ff 100%);
      color: white;
      box-shadow: 0 14px 28px rgba(25, 118, 210, 0.18);
    }
    .secondary {
      background: rgba(25, 118, 210, 0.08);
      color: var(--steel);
      border: 1px solid rgba(25, 118, 210, 0.14);
    }
    .placeholder {
      min-height: 240px;
      display: grid;
      place-items: center;
      text-align: center;
      color: var(--muted);
      font-size: 16px;
      line-height: 1.6;
    }
    @media (max-width: 1040px) {
      .layout { grid-template-columns: 1fr; }
      .sidebar { border-right: 0; border-bottom: 1px solid var(--line); }
      .grid.two { grid-template-columns: 1fr; }
      .system-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-badge">TaskShift Admin</div>
        <h1>Cluster panel</h1>
        <p>Inspect the active cluster snapshot, edit live config files, and keep room for future tabs without changing the operating model.</p>
      </div>

      <div class="language-switch">
        <select id="languageSelect">
          <option value="ru">Русский</option>
          <option value="en">English</option>
        </select>
      </div>

      <div class="tab-list">
        <button class="tab-button active" data-tab="system">
          <span class="tab-label">System</span>
          <span class="tab-caption">Scheduler runtime state, manual trigger, and the latest launch attempts.</span>
        </button>
        <button class="tab-button" data-tab="cluster">
          <span class="tab-label">Cluster</span>
          <span class="tab-caption">Node type tree, node groups, partitions, active snapshot source.</span>
        </button>
        <button class="tab-button" data-tab="configs">
          <span class="tab-label">Configs</span>
          <span class="tab-caption">Edit `scheduler.yaml`, `server.yaml`, active cluster config and `.env`.</span>
        </button>
        <button class="tab-button" data-tab="calendars">
          <span class="tab-label">Calendars</span>
          <span class="tab-caption">Edit calendar configs by year, add new years, and discover new calendar files.</span>
        </button>
        <button class="tab-button" data-tab="logs">
          <span class="tab-label">Logs</span>
          <span class="tab-caption">Structured view for `taskshift.log` with search and level filters.</span>
        </button>
        <button class="tab-button" data-tab="jobs">
          <span class="tab-label">Jobs</span>
          <span class="tab-caption">Launch-event journal with search by job id and status.</span>
        </button>
      </div>

      <div class="side-card">
        <h3>Auth</h3>
        <p>Admin access is protected by <code>ADMIN_PANEL_TOKEN</code> from <code>configs/.env</code>.</p>
      </div>

      <div class="side-card">
        <h3>Editing</h3>
        <p>Cluster edits target the currently active cluster config file used by the scheduler.</p>
      </div>

      <form method="post" action="/logout">
        <button class="logout" type="submit">Log out</button>
      </form>
    </aside>

    <main class="content">
      <div class="topbar">
        <div>
          <h2 id="panelTitle">System status</h2>
          <p id="panelSubtitle">Loading scheduler runtime state.</p>
        </div>
        <div class="status" id="statusBox">Ready.</div>
      </div>

      <section class="panel active" data-panel="system">
        <div class="card system-card">
          <div class="system-header">
            <div>
              <h3>System status</h3>
              <p id="systemStatusSummary">Loading scheduler runtime state.</p>
            </div>
            <div class="inline-form system-controls">
              <input id="manualMaxLaunchedJobsInput" type="number" min="1" step="1" placeholder="Max jobs">
              <button class="primary" id="runSchedulerNowButton" type="button">Run scheduler now</button>
            </div>
          </div>
          <div class="metrics" id="systemStatusMetrics"></div>
          <div class="system-grid">
            <div class="system-pane">
              <h4>Next run</h4>
              <p id="systemCountdownText">Loading next scheduler tick.</p>
            </div>
            <div class="system-pane">
              <h4>Last run</h4>
              <p id="systemLastRunText">Loading last scheduler pass.</p>
            </div>
          </div>
          <div class="system-queue-header">
            <h4>Last launch attempts</h4>
            <p id="systemQueueSummary">Loading jobs attempted on the latest scheduler pass.</p>
          </div>
          <div class="queue-list" id="systemQueueList"></div>
          <details class="runtime-error" id="systemErrorDetails">
            <summary id="systemErrorSummary">Last error log</summary>
            <pre id="systemErrorLog"></pre>
          </details>
        </div>
      </section>

      <section class="panel" data-panel="cluster">
        <div class="card">
          <h3 id="clusterTopologyHeading">Cluster topology</h3>
          <p id="clusterTopologyIntro">Inspect the active cluster snapshot, switch the parsed source file, and browse the feature and partition tree.</p>
        </div>
        <div class="grid two">
          <div class="card">
            <h3>Active snapshot</h3>
            <div class="toolbar-row compact" style="margin-bottom: 12px;">
              <select id="clusterSourceSelect"></select>
              <button class="secondary" id="reloadClusterSourceButton" type="button">Reload source</button>
            </div>
            <p id="clusterSourceText">Loading source metadata.</p>
            <div class="metrics" id="clusterMetrics"></div>
          </div>
          <div class="card">
            <h3>Overview</h3>
            <p id="clusterOverviewText">Loading feature and partition counts.</p>
            <div class="metrics" id="clusterOverviewMetrics"></div>
          </div>
        </div>
        <div class="tree" id="clusterTree"></div>
      </section>

      <section class="panel" data-panel="configs">
        <div class="card">
          <h3>Editable config files</h3>
          <p>Select a file, edit its contents, and save it back to disk. YAML files are syntax-checked before write.</p>
        </div>
        <div class="file-toolbar" id="fileToolbar"></div>
        <div class="editor-meta" id="editorPath">Loading file target.</div>
        <textarea id="configEditor" spellcheck="false"></textarea>
        <div class="actions">
          <button class="primary" id="saveButton" type="button">Save file</button>
          <button class="secondary" id="reloadButton" type="button">Reload from disk</button>
        </div>
      </section>

      <section class="panel" data-panel="calendars">
        <div class="grid two">
          <div class="card">
            <h3>Calendar discovery</h3>
            <p>Calendar files are discovered dynamically by scanning <code>configs/calendar/&lt;year&gt;/*.yaml</code>. If a new file appears in any year, it becomes a known calendar type here automatically.</p>
            <div class="metrics" id="calendarOverviewMetrics"></div>
          </div>
          <div class="card stack">
            <div>
              <h3>Create year</h3>
              <p>Add a new calendar year and optionally copy files from an existing year.</p>
              <div class="inline-form">
                <input id="newCalendarYearInput" type="text" inputmode="numeric" placeholder="2027">
                <select id="copyCalendarYearSelect"></select>
                <button class="primary" id="createCalendarYearButton" type="button">Create year</button>
              </div>
            </div>
            <div>
              <h3>Add calendar file</h3>
              <p>Create a new calendar YAML for the selected year. This is how the panel learns about new calendar types over time.</p>
              <div class="inline-form">
                <input id="newCalendarFileInput" type="text" placeholder="event_calendar.yaml">
                <select id="templateCalendarYearSelect"></select>
                <button class="secondary" id="createCalendarFileButton" type="button">Add file</button>
              </div>
            </div>
          </div>
        </div>

        <div class="calendar-layout">
          <aside class="calendar-sidebar">
            <div class="card">
              <h3>Years</h3>
              <div class="year-list" id="calendarYearList"></div>
            </div>
            <div class="card">
              <h3>Files in year</h3>
              <div class="calendar-file-list" id="calendarFileList"></div>
            </div>
          </aside>

          <div class="card">
            <h3>Calendar editor</h3>
            <div class="calendar-meta" id="calendarEditorMeta">Loading calendar file target.</div>
            <textarea id="calendarEditor" spellcheck="false"></textarea>
            <div class="actions">
              <button class="primary" id="saveCalendarButton" type="button">Save calendar</button>
              <button class="secondary" id="reloadCalendarButton" type="button">Reload calendar</button>
            </div>
          </div>
        </div>
      </section>

      <section class="panel" data-panel="logs">
        <div class="grid two">
          <div class="card">
            <h3>Service log</h3>
            <p id="taskshiftLogPath">Loading log file path.</p>
            <div class="metrics" id="taskshiftLogMetrics"></div>
          </div>
          <div class="card">
            <h3>Filters</h3>
            <div class="toolbar">
              <div class="toolbar-row">
                <input id="taskshiftLogSearchInput" type="search" placeholder="Search text in source or message">
                <button class="secondary" id="reloadTaskshiftLogButton" type="button">Reload log</button>
              </div>
              <div class="status-filter-group" id="taskshiftStatusFilters"></div>
              <div class="toolbar-row">
                <button class="secondary" id="taskshiftPrevPageButton" type="button">Prev page</button>
                <div class="editor-meta" id="taskshiftLogPageInfo">Page 1 / 1</div>
                <button class="secondary" id="taskshiftNextPageButton" type="button">Next page</button>
              </div>
            </div>
          </div>
        </div>
        <div class="log-list" id="taskshiftLogList"></div>
      </section>

      <section class="panel" data-panel="jobs">
        <div class="grid two">
          <div class="card">
            <h3>Job launch log</h3>
            <p id="jobLogPath">Loading job log file path.</p>
            <div class="metrics" id="jobLogMetrics"></div>
          </div>
          <div class="card">
            <h3>Filters</h3>
            <div class="toolbar">
              <div class="toolbar-row">
                <input id="jobLogIdInput" type="search" inputmode="numeric" placeholder="Filter by job id">
                <input id="jobLogSearchInput" type="search" placeholder="Search name, partition, reason">
                <button class="secondary" id="reloadJobLogButton" type="button">Reload jobs log</button>
              </div>
              <div class="status-filter-group" id="jobStatusFilters"></div>
              <div class="toolbar-row">
                <button class="secondary" id="jobPrevPageButton" type="button">Prev page</button>
                <div class="editor-meta" id="jobLogPageInfo">Page 1 / 1</div>
                <button class="secondary" id="jobNextPageButton" type="button">Next page</button>
              </div>
            </div>
          </div>
        </div>
        <div class="log-list" id="jobLogList"></div>
      </section>
    </main>
  </div>

  <script>
    const statusBox = document.getElementById('statusBox');
    const panelTitle = document.getElementById('panelTitle');
    const panelSubtitle = document.getElementById('panelSubtitle');
    const languageSelect = document.getElementById('languageSelect');
    const clusterSourceText = document.getElementById('clusterSourceText');
    const clusterOverviewText = document.getElementById('clusterOverviewText');
    const clusterMetrics = document.getElementById('clusterMetrics');
    const clusterOverviewMetrics = document.getElementById('clusterOverviewMetrics');
    const clusterTree = document.getElementById('clusterTree');
    const clusterSourceSelect = document.getElementById('clusterSourceSelect');
    const reloadClusterSourceButton = document.getElementById('reloadClusterSourceButton');
    const systemStatusSummary = document.getElementById('systemStatusSummary');
    const systemStatusMetrics = document.getElementById('systemStatusMetrics');
    const systemCountdownText = document.getElementById('systemCountdownText');
    const systemLastRunText = document.getElementById('systemLastRunText');
    const systemQueueSummary = document.getElementById('systemQueueSummary');
    const systemQueueList = document.getElementById('systemQueueList');
    const systemErrorDetails = document.getElementById('systemErrorDetails');
    const systemErrorSummary = document.getElementById('systemErrorSummary');
    const systemErrorLog = document.getElementById('systemErrorLog');
    const manualMaxLaunchedJobsInput = document.getElementById('manualMaxLaunchedJobsInput');
    const runSchedulerNowButton = document.getElementById('runSchedulerNowButton');
    const fileToolbar = document.getElementById('fileToolbar');
    const editorPath = document.getElementById('editorPath');
    const configEditor = document.getElementById('configEditor');
    const saveButton = document.getElementById('saveButton');
    const reloadButton = document.getElementById('reloadButton');
    const calendarOverviewMetrics = document.getElementById('calendarOverviewMetrics');
    const calendarYearList = document.getElementById('calendarYearList');
    const calendarFileList = document.getElementById('calendarFileList');
    const calendarEditorMeta = document.getElementById('calendarEditorMeta');
    const calendarEditor = document.getElementById('calendarEditor');
    const saveCalendarButton = document.getElementById('saveCalendarButton');
    const reloadCalendarButton = document.getElementById('reloadCalendarButton');
    const newCalendarYearInput = document.getElementById('newCalendarYearInput');
    const copyCalendarYearSelect = document.getElementById('copyCalendarYearSelect');
    const newCalendarFileInput = document.getElementById('newCalendarFileInput');
    const templateCalendarYearSelect = document.getElementById('templateCalendarYearSelect');
    const createCalendarYearButton = document.getElementById('createCalendarYearButton');
    const createCalendarFileButton = document.getElementById('createCalendarFileButton');
    const taskshiftLogPath = document.getElementById('taskshiftLogPath');
    const taskshiftLogMetrics = document.getElementById('taskshiftLogMetrics');
    const taskshiftStatusFilters = document.getElementById('taskshiftStatusFilters');
    const taskshiftLogSearchInput = document.getElementById('taskshiftLogSearchInput');
    const reloadTaskshiftLogButton = document.getElementById('reloadTaskshiftLogButton');
    const taskshiftPrevPageButton = document.getElementById('taskshiftPrevPageButton');
    const taskshiftNextPageButton = document.getElementById('taskshiftNextPageButton');
    const taskshiftLogPageInfo = document.getElementById('taskshiftLogPageInfo');
    const taskshiftLogList = document.getElementById('taskshiftLogList');
    const jobLogPath = document.getElementById('jobLogPath');
    const jobLogMetrics = document.getElementById('jobLogMetrics');
    const jobStatusFilters = document.getElementById('jobStatusFilters');
    const jobLogIdInput = document.getElementById('jobLogIdInput');
    const jobLogSearchInput = document.getElementById('jobLogSearchInput');
    const reloadJobLogButton = document.getElementById('reloadJobLogButton');
    const jobPrevPageButton = document.getElementById('jobPrevPageButton');
    const jobNextPageButton = document.getElementById('jobNextPageButton');
    const jobLogPageInfo = document.getElementById('jobLogPageInfo');
    const jobLogList = document.getElementById('jobLogList');

    let clusterPayload = null;
    let clusterSourcesCatalog = null;
    let systemStatusPayload = null;
    let selectedClusterSourcePath = null;
    let configTargets = [];
    let selectedConfigId = null;
    let calendarCatalog = null;
    let selectedCalendarYear = null;
    let selectedCalendarFile = null;
    let taskshiftLogPayload = null;
    let jobLogPayload = null;
    let taskshiftLogPage = 1;
    let jobLogPage = 1;
    let currentLanguage = 'ru';
    let systemStatusPollHandle = null;
    let systemCountdownHandle = null;

    const TRANSLATIONS = {
      ru: {
        'brand.title': 'Панель кластера',
        'brand.description': 'Просматривайте снимок кластера, редактируйте конфиги и управляйте служебными вкладками из одной панели.',
        'tab.system.label': 'Статус',
        'tab.system.caption': 'Состояние шедулера, ручной запуск и последние попытки запуска задач.',
        'tab.cluster.label': 'Топология',
        'tab.cluster.caption': 'Дерево типов узлов, групп узлов, партиций и выбор источника конфигурации.',
        'tab.configs.label': 'Конфиги',
        'tab.configs.caption': 'Редактирование `scheduler.yaml`, `server.yaml`, активного cluster config и `.env`.',
        'tab.calendars.label': 'Календари',
        'tab.calendars.caption': 'Редактирование календарей по годам, добавление новых лет и файлов.',
        'tab.logs.label': 'Логи',
        'tab.logs.caption': 'Структурированный просмотр `taskshift.log` с поиском и фильтрами.',
        'tab.jobs.label': 'Задачи',
        'tab.jobs.caption': 'Журнал запусков задач с поиском по `job_id` и статусу.',
        'side.auth.title': 'Доступ',
        'side.auth.text': 'Панель администратора защищена токеном `ADMIN_PANEL_TOKEN` из `configs/.env`.',
        'side.edit.title': 'Редактирование',
        'side.edit.text': 'Редактирование cluster config затрагивает файл, который сейчас используется планировщиком.',
        'button.logout': 'Выйти',
        'panel.system.title': 'Статус системы',
        'panel.system.subtitle': 'Runtime-состояние планировщика, ручной запуск и последние попытки запуска задач.',
        'panel.cluster.title': 'Топология кластера',
        'panel.cluster.subtitle': 'Активный снимок кластера, источник конфигурации и дерево нод/партиций.',
        'panel.configs.title': 'Редактор конфигов',
        'panel.configs.subtitle': 'Прямое редактирование scheduler, server, active cluster config и environment.',
        'panel.calendars.title': 'Конфиги календарей',
        'panel.calendars.subtitle': 'Редактирование календарей по годам с динамическим обнаружением файлов.',
        'panel.logs.title': 'Служебный лог',
        'panel.logs.subtitle': 'Структурированный просмотр runtime-сообщений из `logs/taskshift.log`.',
        'panel.jobs.title': 'Лог запусков задач',
        'panel.jobs.subtitle': 'История попыток запуска с поиском по номеру задачи и статусу.',
        'status.ready': 'Готово.',
        'status.panelLoaded': 'Панель загружена.',
        'status.loadingServiceLog': 'Загружается служебный лог...',
        'status.loadingJobsLog': 'Загружается лог запусков задач...',
        'status.loadingConfig': 'Загружается {target}...',
        'status.loadedConfig': 'Загружен файл: {label}.',
        'status.loadingCalendar': 'Загружается календарь {year}/{filename}...',
        'status.loadedCalendar': 'Календарь {year}/{filename} загружен.',
        'status.savingConfig': 'Сохраняется {target}...',
        'status.savedConfig': 'Файл сохранён: {label}.',
        'status.savingCalendar': 'Сохраняется календарь {year}/{filename}...',
        'status.savedCalendar': 'Календарь {year}/{filename} сохранён.',
        'status.creatingCalendarYear': 'Создаётся календарный год {year}...',
        'status.createdCalendarYear': 'Календарный год {year} создан.',
        'status.creatingCalendarFile': 'Создаётся файл календаря {year}/{filename}...',
        'status.createdCalendarFile': 'Файл календаря {year}/{filename} создан.',
        'status.loadingCluster': 'Загружается снимок кластера...',
        'status.loadingSystemStatus': 'Загружается runtime-статус шедулера...',
        'status.loadedSystemStatus': 'Runtime-статус шедулера обновлён.',
        'status.requestingManualRun': 'Запрашивается ручной прогон шедулера...',
        'status.manualRunRequested': 'Ручной прогон шедулера поставлен в выполнение.',
        'status.loadedServiceLog': 'Загружено записей служебного лога: {count}.',
        'status.loadedJobsLog': 'Загружено записей лога задач: {count}.',
        'status.unauthorized': 'Требуется повторная авторизация.',
        'system.title': 'Статус системы',
        'system.summary.loading': 'Загрузка runtime-состояния шедулера.',
        'system.next.title': 'Следующий прогон',
        'system.next.loading': 'Загрузка времени следующего тика.',
        'system.last.title': 'Последний прогон',
        'system.last.loading': 'Загрузка данных последнего прохода.',
        'system.queue.title': 'Попытки запуска за последний прогон',
        'system.queue.loading': 'Загрузка задач, для которых была попытка запуска в последнем проходе шедулера.',
        'system.error.summary': 'Лог последней ошибки',
        'cluster.topology.title': 'Топология кластера',
        'cluster.topology.intro': 'Отдельная страница с активным снимком кластера, выбором источника и деревом типов узлов/партиций.',
        'system.status.active': 'активен',
        'system.status.inactive': 'не активен',
        'system.status.running': 'выполняется',
        'system.status.db_connection_error': 'ошибка подключения к БД',
        'system.status.error': 'ошибка',
        'system.status.idle': 'ожидание',
        'system.status.success': 'успешно',
        'system.summary.active': 'Сервис планировщика активен.',
        'system.summary.inactive': 'Сервис планировщика не запущен.',
        'system.summary.running': 'Сейчас выполняется прогон планировщика.',
        'system.summary.db_connection_error': 'Последний прогон завершился ошибкой подключения к БД.',
        'system.summary.error': 'Последний прогон завершился с ошибкой.',
        'system.summary.idle': 'Планировщик ожидает первый прогон.',
        'system.countdownTemplate': 'Следующий автоматический прогон через {value} ({at}).',
        'system.countdownRunning': 'Прогон выполняется прямо сейчас. Следующий запуск будет пересчитан после завершения.',
        'system.countdownInactive': 'Автоматический таймер недоступен, потому что сервис не запущен.',
        'system.lastRunTemplate': 'Статус: {status} | триггер: {trigger} | завершён: {finished} | длительность: {duration}.',
        'system.lastRunNever': 'Завершённых прогонов пока нет.',
        'system.queueSummaryTemplate': 'Попыток запуска: {attempted}, задач в очереди на снимке: {pending}, running snapshot: {running}.',
        'system.queueEmpty': 'В последнем прогоне попыток запуска не было.',
        'system.manualMaxPlaceholder': 'Макс. задач',
        'system.manualRunUnavailable': 'Ручной запуск доступен только когда панель подключена к активному сервису шедулера.',
        'system.trigger.startup': 'старт',
        'system.trigger.manual': 'вручную',
        'system.trigger.scheduled': 'по таймеру',
        'system.trigger.unknown': 'неизвестно',
        'metric.schedulerState': 'Статус: {value}',
        'metric.nextRun': 'Следующий прогон: {value}',
        'metric.countdown': 'Отсчёт: {value}',
        'metric.maxLaunch': 'Лимит запуска: {value}',
        'metric.pendingQueue': 'Очередь: {count}',
        'metric.attemptedQueue': 'Попытки запуска: {count}',
        'metric.runningJobs': 'Running snapshot: {count}',
        'metric.failedPool': 'Failed pool: {count}',
        'button.runSchedulerNow': 'Запустить сейчас',
        'queue.status.PENDING': 'в очереди',
        'queue.status.ATTEMPTED': 'попытались запустить',
        'queue.status.SKIPPED_RESOURCES': 'нет ресурсов',
        'queue.status.SKIPPED_TIMELIMIT': 'превышен timelimit',
        'queue.status.BLOCKED_FAILED_POOL': 'заблокирована после неудачной попытки запуска',
        'job.status.LAUNCH_ATTEMPTED': 'попытались запустить',
        'job.status.LAUNCH_FAILED': 'запуск не удался',
        'job.status.LEFT_PENDING_QUEUE': 'ушла из очереди после попытки запуска',
        'job.status.ATTEMPTED': 'попытались запустить',
        'job.status.FAILED': 'запуск не удался',
        'job.status.SUCCEEDED': 'ушла из очереди после попытки запуска',
        'queue.partition': 'Партиция: {value}',
        'queue.constraints': 'Constraints: {value}',
        'queue.resources': '{cpus} CPU, {gpus} GPU, {nodes} nodes, {timelimit} min',
        'cluster.active.title': 'Текущий снимок',
        'cluster.overview.title': 'Обзор',
        'cluster.reloadSource': 'Обновить источник',
        'cluster.loadingSource': 'Загрузка метаданных источника.',
        'cluster.loadingOverview': 'Загрузка статистики по типам узлов и партициям.',
        'cluster.sourceTemplate': 'Просмотр: {selected} | Создан: {created} | Активный снимок планировщика: {active}',
        'cluster.overviewTemplate': '{features} типов узлов, {partitions} партиций, {nodes} объявленных узлов.',
        'metric.features': 'Типы узлов: {count}',
        'metric.partitions': 'Партиции: {count}',
        'metric.latestBackup': 'Последний бэкап: {value}',
        'metric.totalNodes': 'Всего узлов: {count}',
        'metric.totalCpu': 'CPU-ядер: {count}',
        'metric.totalGpu': 'GPU: {count}',
        'metric.years': 'Лет: {count}',
        'metric.knownFiles': 'Известных файлов: {count}',
        'metric.root': 'Корень: {value}',
        'metric.shown': 'Показано: {count}',
        'metric.filtered': 'После фильтра: {count}',
        'metric.total': 'Всего: {count}',
        'metric.statuses': 'Статусов: {count}',
        'metric.featuresShort': '{count} типов',
        'feature.summarySubtitle': '{groups} групп узлов, {nodes} узлов, {partitions} партиций',
        'feature.partitionChip': 'Партиции: {value}',
        'group.summarySubtitle': '{count} настроенных узлов, вес {weight}',
        'group.cpuPerNode': '{count} CPU / узел',
        'group.gpuPerNode': '{count} GPU / узел',
        'group.historyTitle': 'Исторические окна',
        'group.historyChip': '{count} узлов | {start} -> {end}',
        'table.node': 'Узел',
        'table.partitions': 'Партиции',
        'table.resources': 'Ресурсы',
        'label.none': 'нет',
        'configs.title': 'Редактируемые конфиги',
        'configs.description': 'Выберите файл, измените содержимое и сохраните обратно на диск. YAML-файлы проверяются перед записью.',
        'configs.loadingTarget': 'Загрузка выбранного файла.',
        'button.saveFile': 'Сохранить файл',
        'button.reloadDisk': 'Перечитать с диска',
        'config.cluster_active': 'Активный cluster config',
        'config.scheduler': 'Конфиг планировщика',
        'config.server': 'Конфиг веб-сервера',
        'config.env': 'Окружение',
        'cal.discovery.title': 'Обнаружение календарей',
        'cal.discovery.desc': 'Файлы календарей определяются динамически по `configs/calendar/<year>/*.yaml`. Если в каком-то году появляется новый файл, панель автоматически считает его новым типом календаря.',
        'cal.createYear.title': 'Создать год',
        'cal.createYear.desc': 'Добавьте новый календарный год и при необходимости скопируйте файлы из существующего года.',
        'cal.createYear.button': 'Создать год',
        'cal.addFile.title': 'Добавить файл календаря',
        'cal.addFile.desc': 'Создайте новый YAML-файл для выбранного года. Так панель узнаёт о новых типах календарей.',
        'cal.addFile.button': 'Добавить файл',
        'cal.years.title': 'Годы',
        'cal.files.title': 'Файлы года',
        'cal.editor.title': 'Редактор календаря',
        'cal.editor.loading': 'Загрузка файла календаря.',
        'button.saveCalendar': 'Сохранить календарь',
        'button.reloadCalendar': 'Перечитать календарь',
        'cal.noSourceYear': 'Без исходного года',
        'cal.noFilesYet': 'Файлов пока нет',
        'cal.noMissingKnownFiles': 'Все известные типы календарей присутствуют',
        'cal.missingKnownFiles': 'Отсутствуют: {files}',
        'cal.yearNoFiles': 'В году {year} пока нет файлов календаря. Создайте новый файл выше.',
        'cal.filePresent': 'есть',
        'cal.fileMissing': 'нет',
        'cal.filePresentMeta': 'Файл присутствует и доступен для редактирования.',
        'cal.fileMissingMeta': 'Известный тип календаря отсутствует в этом году. Добавьте его через форму выше.',
        'error.noCalendarSelected': 'Файл календаря не выбран.',
        'error.enterCalendarYear': 'Введите календарный год для создания.',
        'error.selectYearBeforeFile': 'Сначала выберите год, затем добавляйте файл календаря.',
        'error.enterCalendarFilename': 'Введите имя файла календаря.',
        'logs.service.title': 'Служебный лог',
        'logs.loadingPath': 'Загрузка пути к лог-файлу.',
        'logs.filters.title': 'Фильтры',
        'logs.searchPlaceholder': 'Поиск по тексту источника или сообщения',
        'button.reloadLog': 'Обновить лог',
        'button.prevPage': 'Предыдущая',
        'button.nextPage': 'Следующая',
        'logs.pageInfo': 'Страница {page} / {total}',
        'logs.noEntries': 'По текущим фильтрам записи в служебном логе не найдены.',
        'jobs.title': 'Лог запусков задач',
        'jobs.loadingPath': 'Загрузка пути к логу задач.',
        'jobs.filters.title': 'Фильтры',
        'jobs.idPlaceholder': 'Фильтр по номеру задачи',
        'jobs.searchPlaceholder': 'Поиск по имени или причине',
        'button.reloadJobsLog': 'Обновить лог задач',
        'jobs.pageInfo': 'Страница {page} / {total}',
        'jobs.noEntries': 'По текущим фильтрам записи лога задач не найдены.',
        'job.label': 'Задача {id}',
        'job.launched': 'Запущена: {value}',
        'job.type': 'Тип: {value}',
        'cluster.sourceLabel.current': 'Текущий config-файл',
        'cluster.sourceLabel.backup': 'Снимок {name}',
        'cluster.sourceActive': 'активный',
        'common.dateUnknown': 'дата неизвестна',
      },
      en: {
        'brand.title': 'Cluster panel',
        'brand.description': 'Inspect the active cluster snapshot, edit live config files, and keep room for future tabs without changing the operating model.',
        'tab.system.label': 'System',
        'tab.system.caption': 'Scheduler runtime state, manual trigger, and the latest launch attempts.',
        'tab.cluster.label': 'Topology',
        'tab.cluster.caption': 'Node type tree, node groups, partitions, active snapshot source.',
        'tab.configs.label': 'Configs',
        'tab.configs.caption': 'Edit `scheduler.yaml`, `server.yaml`, active cluster config and `.env`.',
        'tab.calendars.label': 'Calendars',
        'tab.calendars.caption': 'Edit calendar configs by year, add new years, and discover new calendar files.',
        'tab.logs.label': 'Logs',
        'tab.logs.caption': 'Structured view for `taskshift.log` with search and level filters.',
        'tab.jobs.label': 'Jobs',
        'tab.jobs.caption': 'Launch-event journal with search by job id and status.',
        'side.auth.title': 'Auth',
        'side.auth.text': 'Admin access is protected by `ADMIN_PANEL_TOKEN` from `configs/.env`.',
        'side.edit.title': 'Editing',
        'side.edit.text': 'Cluster edits target the currently active cluster config file used by the scheduler.',
        'button.logout': 'Log out',
        'panel.system.title': 'System status',
        'panel.system.subtitle': 'Scheduler runtime state, manual trigger, and the latest launch attempts.',
        'panel.cluster.title': 'Cluster topology',
        'panel.cluster.subtitle': 'Active cluster snapshot, config source selection, and node/partition tree.',
        'panel.configs.title': 'Config editor',
        'panel.configs.subtitle': 'Direct file editing for scheduler, server, active cluster config, and environment.',
        'panel.calendars.title': 'Calendar configs',
        'panel.calendars.subtitle': 'Year-aware calendar editing with dynamic file discovery and creation.',
        'panel.logs.title': 'Service log',
        'panel.logs.subtitle': 'Structured view of taskshift runtime messages from `logs/taskshift.log`.',
        'panel.jobs.title': 'Job launch log',
        'panel.jobs.subtitle': 'Launch attempt history with search by job id and status.',
        'status.ready': 'Ready.',
        'status.panelLoaded': 'Panel loaded.',
        'status.loadingServiceLog': 'Loading service log...',
        'status.loadingJobsLog': 'Loading job launch log...',
        'status.loadingConfig': 'Loading {target}...',
        'status.loadedConfig': 'Loaded file: {label}.',
        'status.loadingCalendar': 'Loading calendar {year}/{filename}...',
        'status.loadedCalendar': 'Loaded calendar {year}/{filename}.',
        'status.savingConfig': 'Saving {target}...',
        'status.savedConfig': 'Saved file: {label}.',
        'status.savingCalendar': 'Saving calendar {year}/{filename}...',
        'status.savedCalendar': 'Saved calendar {year}/{filename}.',
        'status.creatingCalendarYear': 'Creating calendar year {year}...',
        'status.createdCalendarYear': 'Created calendar year {year}.',
        'status.creatingCalendarFile': 'Creating calendar file {year}/{filename}...',
        'status.createdCalendarFile': 'Created calendar file {year}/{filename}.',
        'status.loadingCluster': 'Loading cluster snapshot...',
        'status.loadingSystemStatus': 'Loading scheduler runtime status...',
        'status.loadedSystemStatus': 'Scheduler runtime status refreshed.',
        'status.requestingManualRun': 'Requesting a manual scheduler run...',
        'status.manualRunRequested': 'Manual scheduler run was queued.',
        'status.loadedServiceLog': 'Loaded {count} service log entries.',
        'status.loadedJobsLog': 'Loaded {count} job log entries.',
        'status.unauthorized': 'Unauthorized.',
        'system.title': 'System status',
        'system.summary.loading': 'Loading scheduler runtime state.',
        'system.next.title': 'Next run',
        'system.next.loading': 'Loading the next scheduler tick.',
        'system.last.title': 'Last run',
        'system.last.loading': 'Loading the latest scheduler pass.',
        'system.queue.title': 'Launch attempts from the last pass',
        'system.queue.loading': 'Loading jobs that were attempted on the latest scheduler pass.',
        'system.error.summary': 'Last error log',
        'cluster.topology.title': 'Cluster topology',
        'cluster.topology.intro': 'Dedicated page for the active cluster snapshot, source selection, and the node type/partition tree.',
        'system.status.active': 'active',
        'system.status.inactive': 'inactive',
        'system.status.running': 'running',
        'system.status.db_connection_error': 'DB connection error',
        'system.status.error': 'error',
        'system.status.idle': 'idle',
        'system.status.success': 'success',
        'system.summary.active': 'The scheduler service is active.',
        'system.summary.inactive': 'The scheduler service is not running.',
        'system.summary.running': 'A scheduler pass is running now.',
        'system.summary.db_connection_error': 'The last pass ended with a database connection error.',
        'system.summary.error': 'The last pass ended with an error.',
        'system.summary.idle': 'The scheduler is waiting for the first pass.',
        'system.countdownTemplate': 'Next automatic run in {value} ({at}).',
        'system.countdownRunning': 'A pass is running right now. The next run will be recalculated after it finishes.',
        'system.countdownInactive': 'Automatic countdown is unavailable because the service is not running.',
        'system.lastRunTemplate': 'Status: {status} | trigger: {trigger} | finished: {finished} | duration: {duration}.',
        'system.lastRunNever': 'No completed scheduler passes yet.',
        'system.queueSummaryTemplate': 'Launch attempts: {attempted}, queued jobs in snapshot: {pending}, running snapshot: {running}.',
        'system.queueEmpty': 'No launch attempts were made on the last scheduler pass.',
        'system.manualMaxPlaceholder': 'Max jobs',
        'system.manualRunUnavailable': 'Manual run is available only when the panel is attached to the active scheduler service.',
        'system.trigger.startup': 'startup',
        'system.trigger.manual': 'manual',
        'system.trigger.scheduled': 'scheduled',
        'system.trigger.unknown': 'unknown',
        'metric.schedulerState': 'State: {value}',
        'metric.nextRun': 'Next run: {value}',
        'metric.countdown': 'Countdown: {value}',
        'metric.maxLaunch': 'Launch limit: {value}',
        'metric.pendingQueue': 'Queue: {count}',
        'metric.attemptedQueue': 'Launch attempts: {count}',
        'metric.runningJobs': 'Running snapshot: {count}',
        'metric.failedPool': 'Failed pool: {count}',
        'button.runSchedulerNow': 'Run now',
        'queue.status.PENDING': 'pending',
        'queue.status.ATTEMPTED': 'launch attempted',
        'queue.status.SKIPPED_RESOURCES': 'no resources',
        'queue.status.SKIPPED_TIMELIMIT': 'timelimit exceeded',
        'queue.status.BLOCKED_FAILED_POOL': 'blocked after failed launch',
        'job.status.LAUNCH_ATTEMPTED': 'launch attempted',
        'job.status.LAUNCH_FAILED': 'launch failed',
        'job.status.LEFT_PENDING_QUEUE': 'left pending queue after launch attempt',
        'job.status.ATTEMPTED': 'launch attempted',
        'job.status.FAILED': 'launch failed',
        'job.status.SUCCEEDED': 'left pending queue after launch attempt',
        'queue.partition': 'Partition: {value}',
        'queue.constraints': 'Constraints: {value}',
        'queue.resources': '{cpus} CPU, {gpus} GPU, {nodes} nodes, {timelimit} min',
        'cluster.active.title': 'Active snapshot',
        'cluster.overview.title': 'Overview',
        'cluster.reloadSource': 'Reload source',
        'cluster.loadingSource': 'Loading source metadata.',
        'cluster.loadingOverview': 'Loading node type and partition counts.',
        'cluster.sourceTemplate': 'Viewing: {selected} | Created: {created} | Scheduler active snapshot: {active}',
        'cluster.overviewTemplate': '{features} node types, {partitions} partitions, {nodes} declared nodes.',
        'metric.features': 'Node types: {count}',
        'metric.partitions': 'Partitions: {count}',
        'metric.latestBackup': 'Latest backup: {value}',
        'metric.totalNodes': 'Total nodes: {count}',
        'metric.totalCpu': 'CPU cores: {count}',
        'metric.totalGpu': 'GPUs: {count}',
        'metric.years': 'Years: {count}',
        'metric.knownFiles': 'Known files: {count}',
        'metric.root': 'Root: {value}',
        'metric.shown': 'Shown: {count}',
        'metric.filtered': 'Filtered: {count}',
        'metric.total': 'Total: {count}',
        'metric.statuses': 'Statuses: {count}',
        'metric.featuresShort': 'Types: {count}',
        'feature.summarySubtitle': '{groups} node groups, {nodes} nodes, {partitions} partitions',
        'feature.partitionChip': 'Partitions: {value}',
        'group.summarySubtitle': '{count} configured nodes, weight {weight}',
        'group.cpuPerNode': '{count} CPU / node',
        'group.gpuPerNode': '{count} GPU / node',
        'group.historyTitle': 'History windows',
        'group.historyChip': '{count} nodes | {start} -> {end}',
        'table.node': 'Node',
        'table.partitions': 'Partitions',
        'table.resources': 'Resources',
        'label.none': 'none',
        'configs.title': 'Editable config files',
        'configs.description': 'Select a file, edit its contents, and save it back to disk. YAML files are syntax-checked before write.',
        'configs.loadingTarget': 'Loading file target.',
        'button.saveFile': 'Save file',
        'button.reloadDisk': 'Reload from disk',
        'config.cluster_active': 'Active cluster config',
        'config.scheduler': 'Scheduler config',
        'config.server': 'Web server config',
        'config.env': 'Environment',
        'cal.discovery.title': 'Calendar discovery',
        'cal.discovery.desc': 'Calendar files are discovered dynamically by scanning `configs/calendar/<year>/*.yaml`. If a new file appears in any year, it becomes a known calendar type here automatically.',
        'cal.createYear.title': 'Create year',
        'cal.createYear.desc': 'Add a new calendar year and optionally copy files from an existing year.',
        'cal.createYear.button': 'Create year',
        'cal.addFile.title': 'Add calendar file',
        'cal.addFile.desc': 'Create a new calendar YAML for the selected year. This is how the panel learns about new calendar types over time.',
        'cal.addFile.button': 'Add file',
        'cal.years.title': 'Years',
        'cal.files.title': 'Files in year',
        'cal.editor.title': 'Calendar editor',
        'cal.editor.loading': 'Loading calendar file target.',
        'button.saveCalendar': 'Save calendar',
        'button.reloadCalendar': 'Reload calendar',
        'cal.noSourceYear': 'No source year',
        'cal.noFilesYet': 'No files yet',
        'cal.noMissingKnownFiles': 'No missing known calendar types',
        'cal.missingKnownFiles': 'Missing: {files}',
        'cal.yearNoFiles': 'Year {year} has no calendar files yet. Create one above.',
        'cal.filePresent': 'present',
        'cal.fileMissing': 'missing',
        'cal.filePresentMeta': 'Editable file in selected year.',
        'cal.fileMissingMeta': 'Known calendar type missing in this year. Add it from the form above.',
        'error.noCalendarSelected': 'No calendar file selected.',
        'error.enterCalendarYear': 'Enter a calendar year to create.',
        'error.selectYearBeforeFile': 'Select a year before adding a calendar file.',
        'error.enterCalendarFilename': 'Enter a calendar filename to create.',
        'logs.service.title': 'Service log',
        'logs.loadingPath': 'Loading log file path.',
        'logs.filters.title': 'Filters',
        'logs.searchPlaceholder': 'Search text in source or message',
        'button.reloadLog': 'Reload log',
        'button.prevPage': 'Prev page',
        'button.nextPage': 'Next page',
        'logs.pageInfo': 'Page {page} / {total}',
        'logs.noEntries': 'No taskshift log entries matched the current filters.',
        'jobs.title': 'Job launch log',
        'jobs.loadingPath': 'Loading job log file path.',
        'jobs.filters.title': 'Filters',
        'jobs.idPlaceholder': 'Filter by job id',
        'jobs.searchPlaceholder': 'Search name or reason',
        'button.reloadJobsLog': 'Reload jobs log',
        'jobs.pageInfo': 'Page {page} / {total}',
        'jobs.noEntries': 'No job log entries matched the current filters.',
        'job.label': 'Job {id}',
        'job.launched': 'Launched: {value}',
        'job.type': 'Type: {value}',
        'cluster.sourceLabel.current': 'Current config file',
        'cluster.sourceLabel.backup': 'Snapshot {name}',
        'cluster.sourceActive': 'active',
        'common.dateUnknown': 'date unknown',
      },
    };

    function getPreferredLanguage() {
      const stored = localStorage.getItem('taskshift-admin-language');
      if (stored === 'ru' || stored === 'en') {
        return stored;
      }
      return (navigator.language || '').toLowerCase().startsWith('ru') ? 'ru' : 'en';
    }

    function t(key, vars = {}) {
      const table = TRANSLATIONS[currentLanguage] || TRANSLATIONS.en;
      const fallback = TRANSLATIONS.en[key] || key;
      const template = table[key] || fallback;
      return template.replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? ''));
    }

    function setText(selector, key, vars = {}) {
      const node = document.querySelector(selector);
      if (node) {
        node.textContent = t(key, vars);
      }
    }

    function setHtml(selector, key, vars = {}) {
      const node = document.querySelector(selector);
      if (node) {
        node.innerHTML = t(key, vars);
      }
    }

    function setPlaceholder(selector, key) {
      const node = document.querySelector(selector);
      if (node) {
        node.setAttribute('placeholder', t(key));
      }
    }

    function configTargetLabel(target) {
      return t(`config.${target.id}`) || target.label;
    }

    function applyLanguage() {
      document.documentElement.lang = currentLanguage;
      languageSelect.value = currentLanguage;
      setText('.brand h1', 'brand.title');
      setText('.brand p', 'brand.description');
      setText('.tab-button[data-tab="system"] .tab-label', 'tab.system.label');
      setText('.tab-button[data-tab="system"] .tab-caption', 'tab.system.caption');
      setText('.tab-button[data-tab="cluster"] .tab-label', 'tab.cluster.label');
      setText('.tab-button[data-tab="cluster"] .tab-caption', 'tab.cluster.caption');
      setText('.tab-button[data-tab="configs"] .tab-label', 'tab.configs.label');
      setText('.tab-button[data-tab="configs"] .tab-caption', 'tab.configs.caption');
      setText('.tab-button[data-tab="calendars"] .tab-label', 'tab.calendars.label');
      setText('.tab-button[data-tab="calendars"] .tab-caption', 'tab.calendars.caption');
      setText('.tab-button[data-tab="logs"] .tab-label', 'tab.logs.label');
      setText('.tab-button[data-tab="logs"] .tab-caption', 'tab.logs.caption');
      setText('.tab-button[data-tab="jobs"] .tab-label', 'tab.jobs.label');
      setText('.tab-button[data-tab="jobs"] .tab-caption', 'tab.jobs.caption');
      setText('.side-card:nth-of-type(1) h3', 'side.auth.title');
      setHtml('.side-card:nth-of-type(1) p', 'side.auth.text');
      setText('.side-card:nth-of-type(2) h3', 'side.edit.title');
      setText('.side-card:nth-of-type(2) p', 'side.edit.text');
      setText('.logout', 'button.logout');
      setText('[data-panel="system"] .system-card h3', 'system.title');
      setText('[data-panel="system"] .system-grid .system-pane:nth-child(1) h4', 'system.next.title');
      setText('[data-panel="system"] .system-grid .system-pane:nth-child(2) h4', 'system.last.title');
      setText('[data-panel="system"] .system-queue-header h4', 'system.queue.title');
      setText('#systemErrorSummary', 'system.error.summary');
      setPlaceholder('#manualMaxLaunchedJobsInput', 'system.manualMaxPlaceholder');
      setText('#runSchedulerNowButton', 'button.runSchedulerNow');
      if (!systemStatusPayload) {
        setText('#systemStatusSummary', 'system.summary.loading');
        setText('#systemCountdownText', 'system.next.loading');
        setText('#systemLastRunText', 'system.last.loading');
        setText('#systemQueueSummary', 'system.queue.loading');
      }
      setText('#clusterTopologyHeading', 'cluster.topology.title');
      setText('#clusterTopologyIntro', 'cluster.topology.intro');
      setText('[data-panel="cluster"] .grid.two .card:nth-child(1) h3', 'cluster.active.title');
      setText('[data-panel="cluster"] .grid.two .card:nth-child(2) h3', 'cluster.overview.title');
      setText('#reloadClusterSourceButton', 'cluster.reloadSource');
      if (!clusterPayload) {
        setText('#clusterSourceText', 'cluster.loadingSource');
        setText('#clusterOverviewText', 'cluster.loadingOverview');
      }
      setText('[data-panel="configs"] .card h3', 'configs.title');
      setText('[data-panel="configs"] .card p', 'configs.description');
      setText('#editorPath', 'configs.loadingTarget');
      setText('#saveButton', 'button.saveFile');
      setText('#reloadButton', 'button.reloadDisk');
      setText('[data-panel="calendars"] .grid.two .card:nth-child(1) h3', 'cal.discovery.title');
      setHtml('[data-panel="calendars"] .grid.two .card:nth-child(1) p', 'cal.discovery.desc');
      setText('[data-panel="calendars"] .card.stack > div:nth-child(1) h3', 'cal.createYear.title');
      setText('[data-panel="calendars"] .card.stack > div:nth-child(1) p', 'cal.createYear.desc');
      setText('#createCalendarYearButton', 'cal.createYear.button');
      setText('[data-panel="calendars"] .card.stack > div:nth-child(2) h3', 'cal.addFile.title');
      setText('[data-panel="calendars"] .card.stack > div:nth-child(2) p', 'cal.addFile.desc');
      setText('#createCalendarFileButton', 'cal.addFile.button');
      setText('[data-panel="calendars"] .calendar-sidebar .card:nth-child(1) h3', 'cal.years.title');
      setText('[data-panel="calendars"] .calendar-sidebar .card:nth-child(2) h3', 'cal.files.title');
      setText('[data-panel="calendars"] .calendar-layout > .card h3', 'cal.editor.title');
      if (!selectedCalendarYear || !selectedCalendarFile) {
        setText('#calendarEditorMeta', 'cal.editor.loading');
      }
      setText('#saveCalendarButton', 'button.saveCalendar');
      setText('#reloadCalendarButton', 'button.reloadCalendar');
      setText('[data-panel="logs"] .card:nth-child(1) h3', 'logs.service.title');
      setText('[data-panel="logs"] .card:nth-child(2) h3', 'logs.filters.title');
      if (!taskshiftLogPayload) setText('#taskshiftLogPath', 'logs.loadingPath');
      setPlaceholder('#taskshiftLogSearchInput', 'logs.searchPlaceholder');
      setText('#reloadTaskshiftLogButton', 'button.reloadLog');
      setText('#taskshiftPrevPageButton', 'button.prevPage');
      setText('#taskshiftNextPageButton', 'button.nextPage');
      setText('[data-panel="jobs"] .card:nth-child(1) h3', 'jobs.title');
      setText('[data-panel="jobs"] .card:nth-child(2) h3', 'jobs.filters.title');
      if (!jobLogPayload) setText('#jobLogPath', 'jobs.loadingPath');
      setPlaceholder('#jobLogIdInput', 'jobs.idPlaceholder');
      setPlaceholder('#jobLogSearchInput', 'jobs.searchPlaceholder');
      setText('#reloadJobLogButton', 'button.reloadJobsLog');
      setText('#jobPrevPageButton', 'button.prevPage');
      setText('#jobNextPageButton', 'button.nextPage');
      const activeTab = document.querySelector('.tab-button.active')?.dataset.tab || 'system';
      activateTab(activeTab);
      if (configTargets.length > 0) renderFileButtons();
      if (clusterSourcesCatalog) renderClusterSourceControls();
      if (systemStatusPayload) renderSystemStatus(systemStatusPayload);
      if (clusterPayload) renderClusterTree(clusterPayload);
      if (calendarCatalog) renderCalendarOverview();
      if (taskshiftLogPayload) renderTaskshiftLogs(taskshiftLogPayload);
      if (jobLogPayload) renderJobLogs(jobLogPayload);
      if (!statusBox.textContent || statusBox.textContent === 'Ready.') {
        setStatus(t('status.ready'));
      }
    }

    function setStatus(message, isError = false) {
      statusBox.textContent = message;
      statusBox.style.color = isError ? '#8d2d1f' : 'var(--steel)';
      statusBox.style.borderColor = isError ? 'rgba(141,45,31,0.2)' : 'rgba(56,84,107,0.14)';
      statusBox.style.background = isError ? 'rgba(141,45,31,0.08)' : 'rgba(255,255,255,0.66)';
    }

    function activateTab(tabName) {
      document.querySelectorAll('.tab-button').forEach((button) => {
        button.classList.toggle('active', button.dataset.tab === tabName);
      });
      document.querySelectorAll('.panel').forEach((panel) => {
        panel.classList.toggle('active', panel.dataset.panel === tabName);
      });
      panelTitle.textContent = t(`panel.${tabName}.title`);
      panelSubtitle.textContent = t(`panel.${tabName}.subtitle`);
    }

    function escapeHtml(value) {
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function formatDate(value) {
      if (!value) {
        return t('common.dateUnknown');
      }
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
        return value;
      }
      return new Intl.DateTimeFormat(currentLanguage === 'ru' ? 'ru-RU' : 'en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(date);
    }

    function createChip(text) {
      const chip = document.createElement('span');
      chip.className = 'chip';
      chip.textContent = text;
      return chip;
    }

    function renderMetrics(container, values) {
      container.innerHTML = '';
      values.forEach((value) => {
        const metric = document.createElement('span');
        metric.className = 'metric';
        metric.textContent = value;
        container.appendChild(metric);
      });
    }

    function getSystemStatusLabel(status) {
      return t(`system.status.${status || 'idle'}`);
    }

    function getTranslatedStatusLabel(prefix, status, fallbackStatus = 'UNKNOWN') {
      const normalizedStatus = String(status || fallbackStatus);
      const key = `${prefix}.${normalizedStatus}`;
      const table = TRANSLATIONS[currentLanguage] || TRANSLATIONS.en;
      return table[key] || TRANSLATIONS.en[key] || normalizedStatus;
    }

    function getQueueStatusLabel(status) {
      return getTranslatedStatusLabel('queue.status', status, 'PENDING');
    }

    function getJobLogStatusLabel(status) {
      return getTranslatedStatusLabel('job.status', status, 'UNKNOWN');
    }

    function getSystemTriggerLabel(trigger) {
      return t(`system.trigger.${trigger || 'unknown'}`);
    }

    function formatDuration(value) {
      if (value === null || value === undefined || value === '') {
        return t('label.none');
      }
      const seconds = Number(value);
      if (!Number.isFinite(seconds)) {
        return String(value);
      }
      if (seconds < 1) {
        return `${seconds.toFixed(3)}s`;
      }
      if (seconds < 60) {
        return `${seconds.toFixed(1)}s`;
      }
      const minutes = Math.floor(seconds / 60);
      const remainder = Math.round(seconds % 60);
      return `${minutes}m ${remainder}s`;
    }

    function formatCountdown(totalSeconds) {
      if (totalSeconds === null || totalSeconds === undefined || Number.isNaN(Number(totalSeconds))) {
        return t('label.none');
      }
      const safeSeconds = Math.max(0, Number(totalSeconds));
      const hours = Math.floor(safeSeconds / 3600);
      const minutes = Math.floor((safeSeconds % 3600) / 60);
      const seconds = Math.floor(safeSeconds % 60);
      if (hours > 0) {
        return `${hours}h ${minutes}m ${seconds}s`;
      }
      if (minutes > 0) {
        return `${minutes}m ${seconds}s`;
      }
      return `${seconds}s`;
    }

    function buildSystemMetrics(payload) {
      const service = payload.service || {};
      const lastRun = payload.last_run || {};
      const controls = payload.controls || {};
      return [
        t('metric.schedulerState', { value: getSystemStatusLabel(service.status) }),
        t('metric.nextRun', { value: service.running && service.next_run_at ? formatDate(service.next_run_at) : t('label.none') }),
        t('metric.countdown', { value: service.running && service.next_run_at ? formatCountdown(service.countdown_seconds) : t('label.none') }),
        t('metric.maxLaunch', { value: controls.default_max_launched_jobs ?? t('label.none') }),
        t('metric.pendingQueue', { count: lastRun.pending_job_count || 0 }),
        t('metric.attemptedQueue', { count: (lastRun.attempted_job_ids || []).length }),
        t('metric.runningJobs', { count: lastRun.running_job_count || 0 }),
        t('metric.failedPool', { count: lastRun.failed_job_pool_size || 0 }),
      ];
    }

    function refreshSystemCountdownView() {
      if (!systemStatusPayload) {
        return;
      }
      const service = systemStatusPayload.service || {};
      if (service.status === 'running') {
        systemCountdownText.textContent = t('system.countdownRunning');
        renderMetrics(systemStatusMetrics, buildSystemMetrics(systemStatusPayload));
        return;
      }
      if (!service.running || !service.next_run_at) {
        systemCountdownText.textContent = t('system.countdownInactive');
        renderMetrics(systemStatusMetrics, buildSystemMetrics(systemStatusPayload));
        return;
      }
      const nextRunAt = new Date(service.next_run_at);
      const seconds = Number.isNaN(nextRunAt.getTime())
        ? null
        : Math.max(0, Math.floor((nextRunAt.getTime() - Date.now()) / 1000));
      systemStatusPayload.service.countdown_seconds = seconds;
      systemCountdownText.textContent = t('system.countdownTemplate', {
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
      let summaryKey = `system.summary.${service.status || 'idle'}`;
      if (!TRANSLATIONS[currentLanguage][summaryKey] && !TRANSLATIONS.en[summaryKey]) {
        summaryKey = 'system.summary.idle';
      }
      systemStatusSummary.textContent = t(summaryKey);

      if (service.status === 'running') {
        systemCountdownText.textContent = t('system.countdownRunning');
      } else if (!service.running || !service.next_run_at) {
        systemCountdownText.textContent = t('system.countdownInactive');
      } else {
        systemCountdownText.textContent = t('system.countdownTemplate', {
          value: formatCountdown(service.countdown_seconds),
          at: formatDate(service.next_run_at),
        });
      }

      if (lastRun.finished_at) {
        systemLastRunText.textContent = t('system.lastRunTemplate', {
          status: getSystemStatusLabel(lastRun.error_kind === 'db_connection' ? 'db_connection_error' : lastRun.status),
          trigger: getSystemTriggerLabel(lastRun.trigger),
          finished: formatDate(lastRun.finished_at),
          duration: formatDuration(lastRun.duration_seconds),
        });
      } else {
        systemLastRunText.textContent = t('system.lastRunNever');
      }

      systemQueueSummary.textContent = t('system.queueSummaryTemplate', {
        pending: lastRun.pending_job_count || 0,
        attempted: (lastRun.attempted_job_ids || []).length,
        running: lastRun.running_job_count || 0,
      });

      renderMetrics(systemStatusMetrics, buildSystemMetrics(payload));

      manualMaxLaunchedJobsInput.placeholder = String(controls.default_max_launched_jobs ?? '');
      runSchedulerNowButton.disabled = !controls.can_run_now || service.status === 'running';
      runSchedulerNowButton.title = controls.can_run_now ? '' : t('system.manualRunUnavailable');

      const attemptedEntries = (lastRun.pending_jobs || []).filter((entry) => entry.was_attempted);
      systemQueueList.innerHTML = '';
      if (attemptedEntries.length === 0) {
        systemQueueList.innerHTML = `<div class="empty-state">${escapeHtml(t('system.queueEmpty'))}</div>`;
      } else {
        attemptedEntries.forEach((entry) => {
          const item = document.createElement('article');
          item.className = 'queue-entry';
          if (entry.was_attempted) {
            item.classList.add('attempted');
          }
          if (entry.in_failed_attempt_pool) {
            item.classList.add('blocked');
          }
          item.innerHTML = `
            <div class="queue-entry-header">
              <div class="queue-entry-title">
                <strong>${escapeHtml(t('job.label', { id: entry.job_id || 'unknown' }))}</strong>
                <div class="queue-entry-meta">${escapeHtml(entry.job_name || 'Unnamed job')}</div>
              </div>
              <span class="log-level ${escapeHtml(entry.status || 'PENDING')}">${escapeHtml(getQueueStatusLabel(entry.status || 'PENDING'))}</span>
            </div>
            <div class="chips" style="margin-bottom: 10px;">
              <span class="chip">${escapeHtml(t('queue.partition', { value: entry.partition || t('label.none') }))}</span>
              <span class="chip">${escapeHtml(t('queue.resources', {
                cpus: entry.requested_cpus || 0,
                gpus: entry.requested_gpus || 0,
                nodes: entry.requested_nodes || 0,
                timelimit: entry.timelimit_minutes || 0,
              }))}</span>
              <span class="chip">${escapeHtml(t('queue.constraints', { value: entry.constraints || t('label.none') }))}</span>
            </div>
          `;
          systemQueueList.appendChild(item);
        });
      }

      const hasError = Boolean(lastRun.error_message || lastRun.error_traceback);
      systemErrorDetails.hidden = !hasError;
      if (hasError) {
        systemErrorDetails.open = true;
        systemErrorLog.textContent = lastRun.error_traceback || lastRun.error_message || '';
      } else {
        systemErrorDetails.open = false;
        systemErrorLog.textContent = '';
      }
      refreshSystemCountdownView();
    }

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

    function renderCalendarYearSelectors() {
      const yearOptions = (calendarCatalog?.years || []).map((entry) => entry.year);
      const buildOptions = (includeEmptyLabel) => {
        const options = [];
        if (includeEmptyLabel) {
          options.push(`<option value="">${escapeHtml(t('cal.noSourceYear'))}</option>`);
        }
        return options.concat(yearOptions.map((year) => `<option value="${year}">${year}</option>`)).join('');
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
      calendarYearList.innerHTML = '';
      years.forEach((entry) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'year-button';
        button.classList.toggle('active', entry.year === selectedCalendarYear);
        button.innerHTML = `
          <div class="year-header">
            <span>${entry.year}</span>
            <span class="chip">${entry.files.length}</span>
          </div>
          <div class="year-meta">
            ${entry.files.join(', ') || t('cal.noFilesYet')}<br>
            ${entry.missing_known_files.length > 0 ? t('cal.missingKnownFiles', { files: entry.missing_known_files.join(', ') }) : t('cal.noMissingKnownFiles')}
          </div>
        `;
        button.addEventListener('click', () => {
          selectedCalendarYear = entry.year;
          if (!entry.files.includes(selectedCalendarFile)) {
            selectedCalendarFile = entry.files[0] || null;
          }
          renderCalendarYearSelectors();
          renderCalendarYears();
          renderCalendarFiles();
          if (selectedCalendarYear && selectedCalendarFile) {
            loadCalendarFile(selectedCalendarYear, selectedCalendarFile).catch((error) => setStatus(error.message, true));
          } else {
            calendarEditorMeta.textContent = t('cal.yearNoFiles', { year: selectedCalendarYear });
            calendarEditor.value = '';
          }
        });
        calendarYearList.appendChild(button);
      });
    }

    function renderCalendarFiles() {
      const yearEntry = (calendarCatalog?.years || []).find((entry) => entry.year === selectedCalendarYear);
      calendarFileList.innerHTML = '';
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
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'calendar-file-button';
        button.classList.toggle('active', filename === selectedCalendarFile);
        button.innerHTML = `
          <div class="year-header">
            <span>${filename}</span>
            <span class="chip">${exists ? t('cal.filePresent') : t('cal.fileMissing')}</span>
          </div>
          <div class="year-meta">${exists ? t('cal.filePresentMeta') : t('cal.fileMissingMeta')}</div>
        `;
        if (exists) {
          button.addEventListener('click', () => {
            selectedCalendarFile = filename;
            renderCalendarFiles();
            loadCalendarFile(selectedCalendarYear, selectedCalendarFile).catch((error) => setStatus(error.message, true));
          });
        }
        calendarFileList.appendChild(button);
      });
    }

    function renderCalendarOverview() {
      const years = calendarCatalog?.years || [];
      renderMetrics(calendarOverviewMetrics, [
        t('metric.years', { count: years.length }),
        t('metric.knownFiles', { count: (calendarCatalog?.known_files || []).length }),
        t('metric.root', { value: calendarCatalog?.root || 'n/a' }),
      ]);
      renderCalendarYearSelectors();
      renderCalendarYears();
      renderCalendarFiles();
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

    function getSelectedStatuses(container) {
      return Array.from(container.querySelectorAll('input[type="checkbox"]:checked')).map((input) => input.value);
    }

    function renderStatusFilters(container, availableStatuses, selectedStatuses, labelResolver = null) {
      container.innerHTML = '';
      availableStatuses.forEach((status) => {
        const labelText = labelResolver ? labelResolver(status) : status;
        const label = document.createElement('label');
        label.className = 'status-toggle';
        label.innerHTML = `
          <input type="checkbox" value="${escapeHtml(status)}" ${selectedStatuses.includes(status) ? 'checked' : ''}>
          <span>${escapeHtml(labelText)}</span>
        `;
        container.appendChild(label);
      });
    }

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
          <pre class="log-entry-message">${escapeHtml(entry.reason || '')}</pre>
        `;
        jobLogList.appendChild(item);
      });
    }

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, {
        credentials: 'same-origin',
        headers: {
          'Accept': 'application/json',
          ...(options.headers || {}),
        },
        ...options,
      });
      if (response.status === 401) {
        window.location.reload();
        throw new Error(t('status.unauthorized'));
      }
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || 'Request failed');
      }
      return payload;
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

    async function loadSystemStatus({ silent = false } = {}) {
      if (!silent) {
        setStatus(t('status.loadingSystemStatus'));
      }
      const payload = await fetchJson('/api/system-status');
      renderSystemStatus(payload);
      if (!silent) {
        setStatus(t('status.loadedSystemStatus'));
      }
    }

    async function runSchedulerNow() {
      setStatus(t('status.requestingManualRun'));
      const rawValue = manualMaxLaunchedJobsInput.value.trim();
      await fetchJson('/api/system-status/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          max_launched_jobs: rawValue ? Number(rawValue) : null,
        }),
      });
      setStatus(t('status.manualRunRequested'));
      window.setTimeout(() => {
        loadSystemStatus({ silent: true }).catch(() => {});
      }, 300);
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

    async function loadCalendarCatalog() {
      const payload = await fetchJson('/api/calendar-years');
      calendarCatalog = payload;
      if (!selectedCalendarYear && payload.years.length > 0) {
        selectedCalendarYear = payload.years[payload.years.length - 1].year;
      }
      const selectedYearEntry = payload.years.find((entry) => entry.year === selectedCalendarYear);
      if (selectedYearEntry && (!selectedCalendarFile || !selectedYearEntry.files.includes(selectedCalendarFile))) {
        selectedCalendarFile = selectedYearEntry.files[0] || null;
      }
      renderCalendarOverview();
      if (selectedCalendarYear && selectedCalendarFile) {
        await loadCalendarFile(selectedCalendarYear, selectedCalendarFile);
      } else {
        calendarEditorMeta.textContent = payload.root;
        calendarEditor.value = '';
      }
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

    async function loadConfigFile(targetId) {
      selectedConfigId = targetId;
      renderFileButtons();
      setStatus(t('status.loadingConfig', { target: targetId }));
      const payload = await fetchJson(`/api/config-targets/${encodeURIComponent(targetId)}`);
      editorPath.textContent = payload.path;
      configEditor.value = payload.content;
      setStatus(t('status.loadedConfig', { label: configTargetLabel(payload) }));
    }

    async function loadCalendarFile(year, filename) {
      setStatus(t('status.loadingCalendar', { year, filename }));
      const payload = await fetchJson(`/api/calendar-years/${encodeURIComponent(year)}/files/${encodeURIComponent(filename)}`);
      selectedCalendarYear = payload.year;
      selectedCalendarFile = payload.filename;
      renderCalendarYearSelectors();
      renderCalendarYears();
      renderCalendarFiles();
      calendarEditorMeta.textContent = payload.path;
      calendarEditor.value = payload.content;
      setStatus(t('status.loadedCalendar', { year: payload.year, filename: payload.filename }));
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

    async function saveCalendarFile() {
      if (!selectedCalendarYear || !selectedCalendarFile) {
        setStatus(t('error.noCalendarSelected'), true);
        return;
      }
      setStatus(t('status.savingCalendar', { year: selectedCalendarYear, filename: selectedCalendarFile }));
      const payload = await fetchJson(`/api/calendar-years/${encodeURIComponent(selectedCalendarYear)}/files/${encodeURIComponent(selectedCalendarFile)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content: calendarEditor.value }),
      });
      calendarEditorMeta.textContent = payload.path;
      setStatus(t('status.savedCalendar', { year: payload.year, filename: payload.filename }));
      await loadCalendarCatalog();
    }

    async function createCalendarYear() {
      const year = newCalendarYearInput.value.trim();
      if (!year) {
        setStatus(t('error.enterCalendarYear'), true);
        return;
      }
      setStatus(t('status.creatingCalendarYear', { year }));
      await fetchJson('/api/calendar-years', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          year,
          copy_from_year: copyCalendarYearSelect.value || null,
        }),
      });
      newCalendarYearInput.value = '';
      selectedCalendarYear = year;
      selectedCalendarFile = null;
      await loadCalendarCatalog();
      setStatus(t('status.createdCalendarYear', { year }));
    }

    async function createCalendarFile() {
      if (!selectedCalendarYear) {
        setStatus(t('error.selectYearBeforeFile'), true);
        return;
      }
      const filename = newCalendarFileInput.value.trim();
      if (!filename) {
        setStatus(t('error.enterCalendarFilename'), true);
        return;
      }
      setStatus(t('status.creatingCalendarFile', { year: selectedCalendarYear, filename }));
      await fetchJson(`/api/calendar-years/${encodeURIComponent(selectedCalendarYear)}/files`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filename,
          copy_from_year: templateCalendarYearSelect.value || null,
        }),
      });
      newCalendarFileInput.value = '';
      selectedCalendarFile = filename;
      await loadCalendarCatalog();
      setStatus(t('status.createdCalendarFile', { year: selectedCalendarYear, filename }));
    }

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
  </script>
</body>
</html>
"""


def build_cluster_tree_payload(sourcePath: str | Path | None = None):
    selectedPath = resolve_cluster_snapshot_path(sourcePath)
    clusterConfig = ClusterConfig().loadConfig(str(selectedPath))
    activeFile = Path(getLatestClusterConfigFile()).resolve()
    latestBackup = getLatestClusterConfigBackupFile()
    nodeToPartitions = _build_node_partition_map(clusterConfig)
    features = []
    totalCpuCores = 0
    totalGpus = 0
    totalNodes = 0

    for featureName in clusterConfig.getFeatureNames():
        featureGroups = []
        featureNodes = []
        featurePartitions = set()
        featureCpu = 0
        featureGpu = 0

        for nodeGroup in clusterConfig.node_groups:
            if featureName not in nodeGroup.features:
                continue

            expandedNodes = expand_hostlist(nodeGroup.name_pattern)
            nodeEntries = []
            groupPartitions = set()
            for nodeName in expandedNodes:
                partitions = sorted(nodeToPartitions.get(nodeName, []))
                groupPartitions.update(partitions)
                nodeEntries.append(
                    {
                        "name": nodeName,
                        "partitions": partitions,
                        "cpu_cores": nodeGroup.resources.cpu_cores,
                        "gpus": nodeGroup.resources.gpus,
                    }
                )

            featureNodes.extend(nodeEntries)
            featurePartitions.update(groupPartitions)
            featureCpu += nodeGroup.node_count * nodeGroup.resources.cpu_cores
            featureGpu += nodeGroup.node_count * nodeGroup.resources.gpus
            featureGroups.append(
                {
                    "name_pattern": nodeGroup.name_pattern,
                    "node_count": nodeGroup.node_count,
                    "weight": nodeGroup.weight,
                    "features": list(nodeGroup.features),
                    "resources": {
                        "sockets": nodeGroup.resources.sockets,
                        "cores_per_socket": nodeGroup.resources.cores_per_socket,
                        "threads_per_core": nodeGroup.resources.threads_per_core,
                        "cpu_cores": nodeGroup.resources.cpu_cores,
                        "gpus": nodeGroup.resources.gpus,
                    },
                    "partitions": sorted(groupPartitions),
                    "history": [
                        {
                            "node_count": period.node_count,
                            "start": period.start,
                            "end": period.end,
                        }
                        for period in (nodeGroup.history or [])
                    ],
                    "nodes": nodeEntries,
                }
            )

        totalCpuCores += featureCpu
        totalGpus += featureGpu
        totalNodes += sum(group["node_count"] for group in featureGroups)
        features.append(
            {
                "name": featureName,
                "node_group_count": len(featureGroups),
                "nodes": featureNodes,
                "partitions": sorted(featurePartitions),
                "total_cpu_cores": featureCpu,
                "total_gpus": featureGpu,
                "node_groups": featureGroups,
            }
        )

    return {
        "selected_file": str(selectedPath),
        "selected_created_at": _format_timestamp(selectedPath.stat().st_mtime) if selectedPath.exists() else None,
        "scheduler_active_file": str(activeFile),
        "latest_backup_file": str(latestBackup.resolve()) if latestBackup is not None else None,
        "feature_count": len(features),
        "partition_count": len(clusterConfig.partitions),
        "total_nodes": totalNodes,
        "total_cpu_cores": totalCpuCores,
        "total_gpus": totalGpus,
        "features": features,
    }


def _build_node_partition_map(clusterConfig) -> dict[str, list[str]]:
    nodeToPartitions = {}
    for partition in clusterConfig.partitions:
        for nodeName in expand_hostlist(partition.nodes):
            nodeToPartitions.setdefault(nodeName, []).append(partition.name)

    return {
        nodeName: sorted(set(partitions))
        for nodeName, partitions in nodeToPartitions.items()
    }


def get_config_targets():
    return [
        {
            "id": "cluster_active",
            "label": "Active cluster config",
            "path": str(Path(getLatestClusterConfigFile()).resolve()),
            "description": "Cluster snapshot currently used by the scheduler.",
        },
        {
            "id": "scheduler",
            "label": "Scheduler config",
            "path": str(Path(schedulerConfigFile).resolve()),
            "description": "Scheduler limits, forecast path, and snapshot cadence.",
        },
        {
            "id": "server",
            "label": "Web server config",
            "path": str(Path(serverConfigFile).resolve()),
            "description": "Host and port for the admin panel.",
        },
        {
            "id": "env",
            "label": "Environment",
            "path": str(Path(DBConfigFile).resolve()),
            "description": "Database credentials and ADMIN_PANEL_TOKEN.",
        },
    ]


def resolve_config_target(targetId: str) -> dict:
    for target in get_config_targets():
        if target["id"] == targetId:
            return target

    raise KeyError(f"Unknown config target: {targetId}")


def read_config_target(targetId: str) -> dict:
    target = resolve_config_target(targetId)
    path = Path(target["path"])
    if path.exists():
        content = path.read_text(encoding="utf-8")
    elif targetId == "server":
        content = 'host: "127.0.0.1"\\nport: 8000\\n'
    else:
        content = ""

    return {
        **target,
        "content": content,
    }


def write_config_target(targetId: str, content: str) -> dict:
    target = resolve_config_target(targetId)
    _validate_config_content(targetId, content)
    path = Path(target["path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return {
        **target,
        "content": content,
    }


def _validate_config_content(targetId: str, content: str):
    if targetId == "env":
        return

    get_yaml_module().safe_load(content or "")


def get_logs_root() -> Path:
    return Path("logs").resolve()


def _format_timestamp(timestamp: float | int | None) -> str | None:
    if timestamp is None:
        return None

    return datetime.fromtimestamp(float(timestamp)).isoformat(timespec="seconds")


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def build_scheduler_system_status_payload(
    projectRoot: str | Path,
    schedulerController=None,
) -> dict:
    projectPath = Path(projectRoot).resolve()
    stateStore = SchedulerRuntimeStateStore(projectPath, SCHEDULER_INTERVAL_MINUTES)
    runtimeState = stateStore.read()
    serviceStatus = get_scheduler_service_status(projectPath)
    schedulerConfig = getSchedulerConfig()
    payload = deepcopy(runtimeState)

    payload["runtime_file"] = str(stateStore.filePath.resolve())
    payload["service"]["running"] = bool(serviceStatus["running"])
    payload["service"]["pid"] = serviceStatus["pid"]
    payload["service"]["pid_file"] = serviceStatus["pid_file"]
    payload["service"]["log_file"] = serviceStatus["log_file"]
    payload["service"]["interval_minutes"] = SCHEDULER_INTERVAL_MINUTES
    payload["service"]["manual_run_available"] = bool(
        schedulerController is not None and schedulerController.can_run_now()
    )

    if not serviceStatus["running"]:
        payload["service"]["next_run_at"] = None

    nextRunAt = _parse_iso_timestamp(payload["service"].get("next_run_at"))
    if serviceStatus["running"] and nextRunAt is not None:
        payload["service"]["countdown_seconds"] = max(
            0,
            int((nextRunAt - datetime.now()).total_seconds()),
        )
    else:
        payload["service"]["countdown_seconds"] = None

    payload["service"]["status"] = payload["last_run"].get("status") or "idle"
    if not serviceStatus["running"]:
        payload["service"]["status"] = "inactive"
    elif payload["last_run"].get("status") == "running":
        payload["service"]["status"] = "running"
    elif payload["last_run"].get("error_kind") == "db_connection":
        payload["service"]["status"] = "db_connection_error"
    elif payload["last_run"].get("status") == "error":
        payload["service"]["status"] = "error"
    else:
        payload["service"]["status"] = "active"

    payload["controls"] = {
        "default_max_launched_jobs": schedulerConfig.max_launched_jobs,
        "can_run_now": bool(schedulerController is not None and schedulerController.can_run_now()),
    }
    return payload


def get_cluster_snapshot_sources_payload() -> dict:
    currentPath = Path(clusterConfigFile).resolve()
    latestBackup = getLatestClusterConfigBackupFile()
    backupRoot = currentPath.parent / "cluster_backups"
    backupFiles = sorted(
        [
            path.resolve()
            for path in backupRoot.rglob("*.yaml")
            if path.is_file()
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    sources = [
        {
            "path": str(currentPath),
            "label": "Current config file",
            "kind": "current",
            "created_at": _format_timestamp(currentPath.stat().st_mtime) if currentPath.exists() else None,
            "is_scheduler_active": str(currentPath) == str(Path(getLatestClusterConfigFile()).resolve()),
        }
    ]
    for backupPath in backupFiles:
        sources.append(
            {
                "path": str(backupPath),
                "label": f"Backup {backupPath.stem}",
                "kind": "backup",
                "created_at": _format_timestamp(backupPath.stat().st_mtime),
                "is_scheduler_active": latestBackup is not None and backupPath == latestBackup.resolve(),
            }
        )

    return {
        "default_path": str(currentPath),
        "scheduler_active_path": str(Path(getLatestClusterConfigFile()).resolve()),
        "latest_backup_path": str(latestBackup.resolve()) if latestBackup is not None else None,
        "sources": sources,
    }


def resolve_cluster_snapshot_path(pathValue: str | None = None) -> Path:
    catalog = get_cluster_snapshot_sources_payload()
    allowedPaths = {entry["path"] for entry in catalog["sources"]}
    defaultPath = Path(catalog["default_path"]).resolve()
    if not pathValue:
        return defaultPath

    requestedPath = Path(pathValue).resolve()
    if str(requestedPath) not in allowedPaths:
        raise ValueError(f"Cluster snapshot source is not available: {requestedPath}")

    return requestedPath


def _normalize_log_limit(limitValue, default: int = 300, maximum: int = 2000) -> int:
    try:
        limit = int(limitValue)
    except (TypeError, ValueError):
        return default

    return max(1, min(limit, maximum))


def _normalize_page(pageValue, default: int = 1) -> int:
    try:
        page = int(pageValue)
    except (TypeError, ValueError):
        return default

    return max(1, page)


def _normalize_status_filters(statusValues) -> list[str]:
    normalized = []
    for value in statusValues or []:
        if value is None:
            continue
        for part in str(value).split(","):
            token = part.strip().upper()
            if token and token not in normalized:
                normalized.append(token)

    return normalized


def normalize_job_log_status(statusValue) -> str:
    normalized = str(statusValue or "").strip().upper()
    if normalized == "ATTEMPTED":
        return "LAUNCH_ATTEMPTED"
    if normalized == "FAILED":
        return "LAUNCH_FAILED"
    if normalized in {"SUCCEEDED", "SUCCESS"}:
        return "LEFT_PENDING_QUEUE"
    return normalized or "UNKNOWN"


def build_taskshift_log_payload(
    *,
    query: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    logPath = get_logs_root() / "taskshift.log"
    selectedStatuses = _normalize_status_filters(statuses)
    entries = []
    currentEntry = None

    if logPath.exists():
        for rawLine in logPath.read_text(encoding="utf-8", errors="replace").splitlines():
            match = TASKSHIFT_LOG_PATTERN.match(rawLine)
            if match:
                if currentEntry is not None:
                    entries.append(currentEntry)
                currentEntry = {
                    "timestamp": match.group("timestamp"),
                    "level": match.group("level"),
                    "source": match.group("source").strip(),
                    "message": match.group("message"),
                    "raw": rawLine,
                }
                continue

            if currentEntry is None:
                currentEntry = {
                    "timestamp": None,
                    "level": "OTHER",
                    "source": "unstructured",
                    "message": rawLine,
                    "raw": rawLine,
                }
                continue

            currentEntry["message"] += f"\n{rawLine}"
            currentEntry["raw"] += f"\n{rawLine}"

        if currentEntry is not None:
            entries.append(currentEntry)

    availableStatuses = sorted({entry["level"] for entry in entries})
    normalizedQuery = query.strip().lower()
    filteredEntries = []
    for entry in entries:
        if selectedStatuses and entry["level"] not in selectedStatuses:
            continue

        if normalizedQuery:
            haystack = "\n".join(
                [
                    entry.get("timestamp") or "",
                    entry.get("level") or "",
                    entry.get("source") or "",
                    entry.get("message") or "",
                ]
            ).lower()
            if normalizedQuery not in haystack:
                continue

        filteredEntries.append(entry)

    filteredEntries = list(reversed(filteredEntries))
    filteredEntriesTotal = len(filteredEntries)
    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageStart = (currentPage - 1) * pageSize
    pageEntries = filteredEntries[pageStart:pageStart + pageSize]
    statusCounts = {}
    for entry in filteredEntries:
        statusCounts[entry["level"]] = statusCounts.get(entry["level"], 0) + 1

    return {
        "file": str(logPath),
        "query": query,
        "selected_statuses": selectedStatuses,
        "available_statuses": availableStatuses,
        "total_entries": len(entries),
        "filtered_entries": filteredEntriesTotal,
        "shown_entries": len(pageEntries),
        "page": currentPage,
        "page_size": pageSize,
        "total_pages": totalPages,
        "has_prev_page": currentPage > 1,
        "has_next_page": currentPage < totalPages,
        "status_counts": statusCounts,
        "entries": pageEntries,
    }


def build_job_logs_payload(
    *,
    query: str = "",
    jobId: str = "",
    statuses: list[str] | None = None,
    page: int = 1,
    limit: int = 100,
) -> dict:
    logPath = get_logs_root() / "job_launches.jsonl"
    selectedStatuses = []
    for status in _normalize_status_filters(statuses):
        normalizedStatus = normalize_job_log_status(status)
        if normalizedStatus not in selectedStatuses:
            selectedStatuses.append(normalizedStatus)
    normalizedQuery = query.strip().lower()
    normalizedJobId = str(jobId).strip()
    entries = []

    if logPath.exists():
        for rawLine in logPath.read_text(encoding="utf-8", errors="replace").splitlines():
            if not rawLine.strip():
                continue
            try:
                payload = json.loads(rawLine)
            except json.JSONDecodeError:
                continue

            payload["status"] = normalize_job_log_status(payload.get("status"))
            payload["raw"] = rawLine
            entries.append(payload)

    availableStatuses = sorted(
        {
            str(entry.get("status", "")).upper()
            for entry in entries
            if entry.get("status")
        }
    )
    filteredEntries = []
    for entry in entries:
        entryStatus = str(entry.get("status", "")).upper()
        if selectedStatuses and entryStatus not in selectedStatuses:
            continue

        if normalizedJobId and normalizedJobId not in str(entry.get("job_id", "")):
            continue

        if normalizedQuery:
            haystack = "\n".join(
                [
                    str(entry.get("job_id", "")),
                    str(entry.get("job_name", "")),
                    str(entry.get("status", "")),
                    str(entry.get("partition", "")),
                    str(entry.get("feature", "")),
                    ", ".join(entry.get("nodes", []) or []),
                    str(entry.get("reason", "")),
                ]
            ).lower()
            if normalizedQuery not in haystack:
                continue

        filteredEntries.append(entry)

    filteredEntries = list(reversed(filteredEntries))
    filteredEntriesTotal = len(filteredEntries)
    pageSize = max(1, limit)
    totalPages = max(1, (filteredEntriesTotal + pageSize - 1) // pageSize)
    currentPage = min(max(1, page), totalPages)
    pageStart = (currentPage - 1) * pageSize
    pageEntries = filteredEntries[pageStart:pageStart + pageSize]
    statusCounts = {}
    for entry in filteredEntries:
        entryStatus = str(entry.get("status", "")).upper() or "UNKNOWN"
        statusCounts[entryStatus] = statusCounts.get(entryStatus, 0) + 1

    return {
        "file": str(logPath),
        "query": query,
        "job_id": normalizedJobId,
        "selected_statuses": selectedStatuses,
        "available_statuses": availableStatuses,
        "total_entries": len(entries),
        "filtered_entries": filteredEntriesTotal,
        "shown_entries": len(pageEntries),
        "page": currentPage,
        "page_size": pageSize,
        "total_pages": totalPages,
        "has_prev_page": currentPage > 1,
        "has_next_page": currentPage < totalPages,
        "status_counts": statusCounts,
        "entries": pageEntries,
    }


def get_calendar_root() -> Path:
    return Path(academicCalendarRoot).resolve()


def get_calendar_catalog_payload() -> dict:
    calendarRoot = get_calendar_root()
    calendarRoot.mkdir(parents=True, exist_ok=True)
    yearDirs = sorted(
        [
            path
            for path in calendarRoot.iterdir()
            if path.is_dir()
        ],
        key=lambda path: path.name,
    )
    knownFiles = sorted(
        {
            filePath.name
            for yearDir in yearDirs
            for filePath in yearDir.glob("*.yaml")
        }
    )

    years = []
    for yearDir in yearDirs:
        files = sorted(filePath.name for filePath in yearDir.glob("*.yaml"))
        years.append(
            {
                "year": yearDir.name,
                "path": str(yearDir.resolve()),
                "files": files,
                "missing_known_files": sorted(set(knownFiles) - set(files)),
            }
        )

    return {
        "root": str(calendarRoot),
        "known_files": knownFiles,
        "years": years,
    }


def _normalize_calendar_year(yearValue: str) -> str:
    year = str(yearValue).strip()
    if not year.isdigit() or len(year) != 4:
        raise ValueError("Calendar year must be a four-digit year like 2027")

    return year


def _normalize_calendar_filename(filenameValue: str) -> str:
    filename = Path(str(filenameValue).strip()).name
    if not filename or filename in {".", ".."}:
        raise ValueError("Calendar filename is required")
    if "/" in filename or "\\" in filename:
        raise ValueError("Calendar filename must not contain path separators")
    if not filename.endswith((".yaml", ".yml")):
        raise ValueError("Calendar filename must end with .yaml or .yml")

    return filename


def _calendar_file_template(filename: str) -> str:
    if filename == "russian_holidays.yaml":
        return "dates: []\n"
    if filename == "university_calendar.yaml":
        return "session_dates: []\nvacation_dates: []\n"

    return "{}\n"


def read_calendar_file(year: str, filename: str) -> dict:
    normalizedYear = _normalize_calendar_year(year)
    normalizedFilename = _normalize_calendar_filename(filename)
    filePath = get_calendar_root() / normalizedYear / normalizedFilename
    if not filePath.exists():
        raise FileNotFoundError(f"Calendar file not found: {filePath}")

    return {
        "year": normalizedYear,
        "filename": normalizedFilename,
        "path": str(filePath.resolve()),
        "content": filePath.read_text(encoding="utf-8"),
    }


def write_calendar_file(year: str, filename: str, content: str) -> dict:
    normalizedYear = _normalize_calendar_year(year)
    normalizedFilename = _normalize_calendar_filename(filename)
    get_yaml_module().safe_load(content or "")
    filePath = get_calendar_root() / normalizedYear / normalizedFilename
    filePath.parent.mkdir(parents=True, exist_ok=True)
    filePath.write_text(content, encoding="utf-8")
    return {
        "year": normalizedYear,
        "filename": normalizedFilename,
        "path": str(filePath.resolve()),
        "content": content,
    }


def create_calendar_year(year: str, copyFromYear: str | None = None) -> dict:
    normalizedYear = _normalize_calendar_year(year)
    targetDir = get_calendar_root() / normalizedYear
    if targetDir.exists():
        raise ValueError(f"Calendar year already exists: {normalizedYear}")

    targetDir.mkdir(parents=True, exist_ok=False)
    copiedFiles = []
    sourceYear = None

    if copyFromYear:
        sourceYear = _normalize_calendar_year(copyFromYear)
        sourceDir = get_calendar_root() / sourceYear
        if not sourceDir.exists():
            raise FileNotFoundError(f"Source calendar year not found: {sourceYear}")

        for sourceFile in sorted(sourceDir.glob("*.yaml")):
            destinationFile = targetDir / sourceFile.name
            shutil.copyfile(sourceFile, destinationFile)
            copiedFiles.append(sourceFile.name)
    else:
        catalog = get_calendar_catalog_payload()
        for filename in catalog["known_files"]:
            destinationFile = targetDir / filename
            destinationFile.write_text(_calendar_file_template(filename), encoding="utf-8")
            copiedFiles.append(filename)

    return {
        "year": normalizedYear,
        "path": str(targetDir.resolve()),
        "copied_from_year": sourceYear,
        "files": copiedFiles,
    }


def create_calendar_file(year: str, filename: str, copyFromYear: str | None = None) -> dict:
    normalizedYear = _normalize_calendar_year(year)
    normalizedFilename = _normalize_calendar_filename(filename)
    yearDir = get_calendar_root() / normalizedYear
    yearDir.mkdir(parents=True, exist_ok=True)
    filePath = yearDir / normalizedFilename
    if filePath.exists():
        raise ValueError(f"Calendar file already exists: {normalizedFilename}")

    sourceYear = None
    if copyFromYear:
        sourceYear = _normalize_calendar_year(copyFromYear)
        sourceFile = get_calendar_root() / sourceYear / normalizedFilename
        if not sourceFile.exists():
            raise FileNotFoundError(f"Template file not found in year {sourceYear}: {normalizedFilename}")

        shutil.copyfile(sourceFile, filePath)
    else:
        filePath.write_text(_calendar_file_template(normalizedFilename), encoding="utf-8")

    return {
        "year": normalizedYear,
        "filename": normalizedFilename,
        "path": str(filePath.resolve()),
        "copied_from_year": sourceYear,
        "content": filePath.read_text(encoding="utf-8"),
    }


class AdminPanelServer:
    def __init__(self, projectRoot: str | Path, schedulerController=None):
        self.projectRoot = Path(projectRoot).resolve()
        self.schedulerController = schedulerController
        self.serverConfig = getServerConfig()
        self.authToken = getAdminPanelAccessConfig().requireToken()
        handlerClass = self._build_handler_class()
        self.httpd = ThreadingHTTPServer((self.serverConfig.host, self.serverConfig.port), handlerClass)
        self.httpd.daemon_threads = True
        self._thread = None

    @property
    def base_url(self) -> str:
        host = self.serverConfig.host
        if host == "0.0.0.0":
            host = "127.0.0.1"
        return f"http://{host}:{self.httpd.server_port}"

    def start_background(self):
        if self._thread is not None:
            return self

        self._thread = threading.Thread(target=self.serve_forever, daemon=True, name="taskshift-admin-panel")
        self._thread.start()
        return self

    def serve_forever(self):
        logger.info(f"TaskShift admin panel started at {self.base_url}")
        self.httpd.serve_forever(poll_interval=0.5)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _build_handler_class(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                app.handle_get(self)

            def do_POST(self):
                app.handle_post(self)

            def log_message(self, format, *args):
                logger.info("Admin panel | " + format % args)

        return Handler

    def handle_get(self, handler: BaseHTTPRequestHandler):
        parsed = urlparse(handler.path)
        path = parsed.path
        if path == "/":
            if self._is_authenticated(handler):
                self._send_html(handler, APP_PAGE_HTML)
                return

            self._send_html(handler, LOGIN_PAGE_HTML.replace("__ERROR_MESSAGE__", ""))
            return

        if path == "/api/cluster-tree":
            if not self._require_auth(handler):
                return

            params = parse_qs(parsed.query)
            try:
                self._send_json(handler, build_cluster_tree_payload(params.get("path", [""])[0]))
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
            return

        if path == "/api/cluster-sources":
            if not self._require_auth(handler):
                return

            self._send_json(handler, get_cluster_snapshot_sources_payload())
            return

        if path == "/api/system-status":
            if not self._require_auth(handler):
                return

            self._send_json(
                handler,
                build_scheduler_system_status_payload(
                    projectRoot=self.projectRoot,
                    schedulerController=self.schedulerController,
                ),
            )
            return

        if path == "/api/config-targets":
            if not self._require_auth(handler):
                return

            self._send_json(handler, {"targets": get_config_targets()})
            return

        if path == "/api/calendar-years":
            if not self._require_auth(handler):
                return

            self._send_json(handler, get_calendar_catalog_payload())
            return

        if path == "/api/logs/taskshift":
            if not self._require_auth(handler):
                return

            params = parse_qs(parsed.query)
            self._send_json(
                handler,
                build_taskshift_log_payload(
                    query=params.get("q", [""])[0],
                    statuses=params.get("statuses", []),
                    page=_normalize_page(params.get("page", [1])[0]),
                    limit=_normalize_log_limit(params.get("limit", [100])[0], default=100, maximum=500),
                ),
            )
            return

        if path == "/api/logs/jobs":
            if not self._require_auth(handler):
                return

            params = parse_qs(parsed.query)
            self._send_json(
                handler,
                build_job_logs_payload(
                    query=params.get("q", [""])[0],
                    jobId=params.get("job_id", [""])[0],
                    statuses=params.get("statuses", []),
                    page=_normalize_page(params.get("page", [1])[0]),
                    limit=_normalize_log_limit(params.get("limit", [100])[0], default=100, maximum=500),
                ),
            )
            return

        if path.startswith("/api/config-targets/"):
            if not self._require_auth(handler):
                return

            targetId = unquote(path.rsplit("/", maxsplit=1)[-1])
            try:
                self._send_json(handler, read_config_target(targetId))
            except KeyError:
                self._send_json(handler, {"error": "Unknown config target"}, status=HTTPStatus.NOT_FOUND)
            return

        if path.startswith("/api/calendar-years/"):
            if not self._require_auth(handler):
                return

            pathParts = [part for part in path.split("/") if part]
            if len(pathParts) == 5 and pathParts[0] == "api" and pathParts[1] == "calendar-years" and pathParts[3] == "files":
                year = unquote(pathParts[2])
                filename = unquote(pathParts[4])
                try:
                    self._send_json(handler, read_calendar_file(year, filename))
                except FileNotFoundError:
                    self._send_json(handler, {"error": "Calendar file not found"}, status=HTTPStatus.NOT_FOUND)
                except Exception as error:
                    self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                return

        self._send_json(handler, {"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def handle_post(self, handler: BaseHTTPRequestHandler):
        parsed = urlparse(handler.path)
        path = parsed.path

        if path == "/login":
            body = self._read_body(handler)
            form = parse_qs(body.decode("utf-8"))
            token = form.get("token", [""])[0]
            if token and secrets.compare_digest(token, self.authToken):
                self._redirect(
                    handler,
                    "/",
                    cookies=[
                        f"{PANEL_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict"
                    ],
                )
                return

            self._send_html(
                handler,
                LOGIN_PAGE_HTML.replace("__ERROR_MESSAGE__", "Invalid token."),
                status=HTTPStatus.UNAUTHORIZED,
            )
            return

        if path == "/logout":
            self._redirect(
                handler,
                "/",
                cookies=[
                    f"{PANEL_COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
                ],
            )
            return

        if path.startswith("/api/config-targets/"):
            if not self._require_auth(handler):
                return

            body = self._read_body(handler)
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send_json(handler, {"error": "Invalid JSON payload"}, status=HTTPStatus.BAD_REQUEST)
                return

            targetId = unquote(path.rsplit("/", maxsplit=1)[-1])
            try:
                result = write_config_target(targetId, payload.get("content", ""))
            except KeyError:
                self._send_json(handler, {"error": "Unknown config target"}, status=HTTPStatus.NOT_FOUND)
                return
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json(handler, result)
            return

        if path == "/api/system-status/run":
            if not self._require_auth(handler):
                return

            body = self._read_body(handler)
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send_json(handler, {"error": "Invalid JSON payload"}, status=HTTPStatus.BAD_REQUEST)
                return

            if self.schedulerController is None:
                self._send_json(
                    handler,
                    {"error": "Manual scheduler run is unavailable because the panel is not attached to the active scheduler service."},
                    status=HTTPStatus.CONFLICT,
                )
                return

            maxLaunchedJobs = payload.get("max_launched_jobs")
            if maxLaunchedJobs in {"", None}:
                normalizedMaxLaunchedJobs = None
            else:
                try:
                    normalizedMaxLaunchedJobs = int(maxLaunchedJobs)
                except (TypeError, ValueError):
                    self._send_json(handler, {"error": "max_launched_jobs must be an integer"}, status=HTTPStatus.BAD_REQUEST)
                    return

                if normalizedMaxLaunchedJobs <= 0:
                    self._send_json(handler, {"error": "max_launched_jobs must be greater than zero"}, status=HTTPStatus.BAD_REQUEST)
                    return

            try:
                self.schedulerController.request_manual_run(maxLaunchedJobs=normalizedMaxLaunchedJobs)
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.CONFLICT)
                return

            self._send_json(
                handler,
                build_scheduler_system_status_payload(
                    projectRoot=self.projectRoot,
                    schedulerController=self.schedulerController,
                ),
            )
            return

        if path == "/api/calendar-years":
            if not self._require_auth(handler):
                return

            body = self._read_body(handler)
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
                result = create_calendar_year(
                    payload.get("year", ""),
                    copyFromYear=payload.get("copy_from_year"),
                )
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json(handler, result)
            return

        if path.startswith("/api/calendar-years/"):
            if not self._require_auth(handler):
                return

            pathParts = [part for part in path.split("/") if part]
            body = self._read_body(handler)
            try:
                payload = json.loads(body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send_json(handler, {"error": "Invalid JSON payload"}, status=HTTPStatus.BAD_REQUEST)
                return

            if len(pathParts) == 4 and pathParts[0] == "api" and pathParts[1] == "calendar-years" and pathParts[3] == "files":
                year = unquote(pathParts[2])
                try:
                    result = create_calendar_file(
                        year,
                        payload.get("filename", ""),
                        copyFromYear=payload.get("copy_from_year"),
                    )
                except Exception as error:
                    self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                    return

                self._send_json(handler, result)
                return

            if len(pathParts) == 5 and pathParts[0] == "api" and pathParts[1] == "calendar-years" and pathParts[3] == "files":
                year = unquote(pathParts[2])
                filename = unquote(pathParts[4])
                try:
                    result = write_calendar_file(year, filename, payload.get("content", ""))
                except Exception as error:
                    self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                    return

                self._send_json(handler, result)
                return

        self._send_json(handler, {"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _is_authenticated(self, handler: BaseHTTPRequestHandler) -> bool:
        cookieHeader = handler.headers.get("Cookie", "")
        cookie = SimpleCookie()
        cookie.load(cookieHeader)
        cookieValue = cookie.get(PANEL_COOKIE_NAME)
        if cookieValue is not None and secrets.compare_digest(cookieValue.value, self.authToken):
            return True

        headerToken = handler.headers.get("X-Admin-Token")
        return bool(headerToken) and secrets.compare_digest(headerToken, self.authToken)

    def _require_auth(self, handler: BaseHTTPRequestHandler) -> bool:
        if self._is_authenticated(handler):
            return True

        self._send_json(handler, {"error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
        return False

    def _send_html(self, handler: BaseHTTPRequestHandler, html: str, status: HTTPStatus = HTTPStatus.OK):
        payload = html.encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    def _send_json(self, handler: BaseHTTPRequestHandler, payload: dict, status: HTTPStatus = HTTPStatus.OK):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Content-Length", str(len(raw)))
        handler.end_headers()
        handler.wfile.write(raw)

    def _redirect(self, handler: BaseHTTPRequestHandler, location: str, cookies: list[str] | None = None):
        handler.send_response(HTTPStatus.SEE_OTHER)
        handler.send_header("Location", location)
        for cookie in cookies or []:
            handler.send_header("Set-Cookie", cookie)
        handler.end_headers()

    def _read_body(self, handler: BaseHTTPRequestHandler) -> bytes:
        contentLength = int(handler.headers.get("Content-Length", "0"))
        if contentLength <= 0:
            return b""

        return handler.rfile.read(contentLength)
