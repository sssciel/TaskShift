# TaskShift Service Documentation

## 1. Purpose and Scope

TaskShift is a Python service for Slurm-based clusters. It combines:

- a foreground or background scheduler loop;
- a resource-aware placement engine for pending jobs;
- an mserver-based connector for Slurm QoS changes;
- a historical utilization export pipeline;
- an optional forecast subsystem for GPU load prediction;
- an admin web panel for inspection and controlled runtime operations.

The repository contains both the service implementation and its operational tooling: CLI commands, configuration loaders, deployment manifests, tests, fake integration harnesses, and calendar/forecast data helpers.

This document focuses on the actual behavior implemented in the current codebase.

## 2. Repository Structure

- `taskshift`: shell entrypoint that runs `src/main.py` through `.venv/bin/python` when available.
- `src/main.py`: minimal executable wrapper around `cli.main()`.
- `src/cli.py`: command-line interface, command dispatch, scheduler loop wiring, export/training commands.
- `src/scheduler/`: core scheduling logic, resource tree, connector, cron/service runtime state.
- `src/storage/`: Slurm DB access, historical export/rebuild pipeline, job models.
- `src/forecast/`: feature engineering, model training, artifact loading, forecast service.
- `src/admin_panel/`: built-in admin HTTP server and its JSON endpoints.
- `src/config/`: YAML and `.env` loading, cluster snapshot refresh, path resolution, logging.
- `mserver/`: small HTTP service and QoS script used to apply Slurm changes outside TaskShift.
- `configs/`: runtime config examples and academic calendar data.
- `tests/unit/`: unit coverage for config, scheduler, resources, connector, admin panel, forecast training.
- `tests/integration/`: MariaDB-backed end-to-end scenarios with fake Slurm control scripts.
- `deploy/`: Docker deployment artifacts intended for service usage.

## 3. Runtime Architecture

At runtime the service is assembled from four main layers.

### 3.1 CLI Layer

`src/cli.py` parses commands and starts one of:

- one scheduler pass;
- the continuous scheduler loop;
- the admin web panel;
- cluster snapshot refresh;
- historical utilization export or rebuild;
- forecast model training.

### 3.2 Scheduling Layer

`src/scheduler/service.py` implements a single scheduler pass:

1. Load pending jobs from Slurm DB.
2. Reconcile launch attempts from the previous pass.
3. Load currently running jobs.
4. Build a resource availability tree from cluster config plus running jobs.
5. Walk the pending queue in storage order.
6. For each job:
   - enforce pass-level launch limits;
   - enforce scheduler timelimit cap;
   - resolve a runnable placement;
   - reserve the chosen resources in-memory immediately;
   - invoke the connector to request a QoS change through mserver;
   - log the launch attempt.

### 3.3 Data Layer

`src/storage/` talks to `linux_job_table` in Slurm DB and materializes:

- `Job` for pending jobs;
- `HistoricalJob` for running or historical jobs;
- raw historical rows for export and rebuild tasks.

### 3.4 Forecast Layer

`src/forecast/` can export utilization history, train a GPU forecast model, save an artifact, and reuse that artifact from the scheduler pass to gate GPU availability.

## 4. Python and System Dependencies

### 4.1 Required Runtime Packages

`requirements.txt` currently installs:

- `mysql-connector-python`
- `python-dotenv`
- `loguru`
- `pyyaml`
- `pandas`
- `APScheduler`

### 4.2 Test Packages

`requirements-test.txt` adds:

- `pytest`
- `pytest-cov`
- yaml/pandas/dotenv/loguru versions suitable for test-only local setups

### 4.3 Optional Forecast Training Packages

The forecast training code prefers gradient boosting libraries that are not listed in `requirements.txt`:

- `catboost`
- `lightgbm`

Training fails with `ImportError` if neither library is installed. Runtime forecast usage without training can still work if a previously saved model artifact already exists.

### 4.4 External Services and Tools

TaskShift expects:

- a reachable MySQL or MariaDB instance with Slurm accounting tables;
- a valid cluster snapshot source command, usually reading `slurm.conf`;
- a reachable mserver endpoint that performs the actual Slurm action;
- for integration tests, Docker plus the compose stack in `tests/integration/compose.e2e.yaml`.

## 5. Setup

### 5.1 Local Development Setup

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

For tests:

```bash
pip install -r requirements-test.txt
```

For forecast model training, also install at least one of:

```bash
pip install catboost
```

or:

```bash
pip install lightgbm
```

### 5.2 Required Runtime Files

The service expects these files unless overridden by environment variables:

- `configs/.env`
- `configs/scheduler.yaml`
- `configs/server.yaml`
- `configs/cluster.yaml`

Examples are provided as:

- `configs/.env.example`
- `configs/scheduler.example.yaml`
- `configs/server.example.yaml`
- `configs/cluster.example.yaml`

### 5.3 Minimum Environment

`configs/.env` or process environment should provide:

- `DB_HOST`
- `DB_USER`
- `DB_PASSWD`
- `DB_DATABASE`

For the admin panel:

- `ADMIN_PANEL_TOKEN`

Optional DB compatibility tuning:

- `DB_CHARSET`
- `DB_COLLATION`

## 6. Configuration Files and Environment Overrides

### 6.1 Path Overrides

These environment variables override default config paths:

- `TASKSHIFT_DB_CONFIG_FILE`
- `TASKSHIFT_SCHEDULER_CONFIG_FILE`
- `TASKSHIFT_SERVER_CONFIG_FILE`
- `TASKSHIFT_CLUSTER_CONFIG_FILE`
- `TASKSHIFT_CLUSTER_CONFIG_BACKUP_ROOT`
- `TASKSHIFT_ACADEMIC_CALENDAR_ROOT`

The admin panel bind address can also be overridden without editing YAML:

- `TASKSHIFT_SERVER_HOST`
- `TASKSHIFT_SERVER_PORT`

### 6.2 `scheduler.yaml`

`SchedulerConfig` currently supports:

- `timelimit`: maximum job timelimit in minutes allowed for launch.
- `max_launched_jobs`: maximum launches per scheduler pass.
- `forecast_enabled`: enables forecast-aware scheduling.
- `forecast_data_dir`: export root or series directory used by forecast loading/training.
- `forecast_model_dir`: directory with forecast model artifact.
- `forecast_skip_startup_training`: if true, startup reuses stale or missing artifacts instead of training.
- `cluster_config_snapshot_interval_hours`: refresh cadence for live cluster snapshots.
- `web_panel_enabled`: auto-start web panel with `schedule`.
- `hot_reload_enabled`: enables background reloading of safe scheduler fields.
- `cluster_config_refresh_command`: command that prints current Slurm config to stdout.
- `connector.mserver_url`: endpoint that accepts QoS change requests.
- `connector.timeout_seconds`: HTTP request timeout for mserver calls.
- `connector.target_qos`: QoS sent to mserver for each selected job.
The mserver API token is read from `TASKSHIFT_MSERVER_API_TOKEN` in `configs/.env`
and sent in the `API_TOKEN` header.

### 6.3 Hot Reload Behavior

Only a subset of scheduler fields is hot-reloadable:

- `timelimit`
- `max_launched_jobs`
- `forecast_enabled`
- `forecast_data_dir`
- `forecast_model_dir`
- `forecast_skip_startup_training`
- `cluster_config_snapshot_interval_hours`

These fields require restart even when file watching is enabled:

- `web_panel_enabled`
- `hot_reload_enabled`
- `cluster_config_refresh_command`

### 6.4 `cluster.yaml`

`ClusterConfig` contains:

- `gres_types`
- `node_groups`
- `partitions`

Each `node_group` defines:

- hostlist pattern;
- node count;
- weight;
- feature labels;
- per-node sockets, cores, threads, GPUs;
- optional historical node-count timeline.

Each partition defines:

- name;
- hostlist;
- state;
- optional `max_cpus_per_node`;
- optional `max_nodes`.

### 6.5 `server.yaml`

`ServerConfig` exposes:

- `host`
- `port`

Defaults are `127.0.0.1:8000`.

## 7. CLI Commands

All commands are implemented in `src/cli.py`.

### 7.1 `taskshift schedule`

Runs the scheduler loop in foreground every 15 minutes.

Flags:

- `--max-launched-jobs`: overrides the configured per-pass launch limit.
- `--without-forecast`: disables forecast usage for this process.
- `--with-web-panel`: force-start the admin panel.
- `--without-web-panel`: force-disable the admin panel.

Behavior:

- starts config hot reload if enabled;
- starts web panel if enabled or forced;
- runs one scheduler tick immediately on startup;
- schedules regular ticks every 15 minutes;
- optionally schedules forecast training every Tuesday and Friday at 00:00.

### 7.2 `taskshift run-scheduler-once`

Runs one scheduler pass immediately and exits.

Flags:

- `--max-launched-jobs`
- `--without-forecast`

### 7.3 `taskshift serve-web-panel`

Runs only the admin panel server.

### 7.4 `taskshift refresh-cluster-config`

Loads a fresh cluster snapshot from `cluster_config_refresh_command` and writes:

- the current cluster config file;
- a timestamped backup under the backup root.

Flags:

- `--output-file`

### 7.5 `taskshift set-forecast-data-dir`

Updates `forecast_data_dir` in `scheduler.yaml`.

Arguments:

- `path`

### 7.6 `taskshift export`

Incrementally exports historical utilization data.

Flags:

- `--output-dir`
- `--interval-minutes`
- `--history-start`
- `--modified-until`
- `--now-timestamp`

Output contains:

- `raw_job_rows.json`
- `state.json`
- `metadata.json`
- `series/*.json`

### 7.7 `taskshift rebuild-series`

Rebuilds utilization series from local raw cache only, without DB access.

Flags:

- `--output-dir`
- `--interval-minutes`
- `--now-timestamp`

### 7.8 `taskshift train-forecast-model`

Trains or refreshes the forecast model artifact.

Flags:

- `--data-dir`
- `--model-dir`
- `--skip-export`

Behavior:

- refreshes historical export by default;
- builds the training frame from the `overall` utilization series;
- fits a gradient boosting regressor;
- stores model and metadata artifact under `forecast_model_dir`.

## 8. Runtime Files and Logs

TaskShift writes runtime artifacts under `logs/` and `exports/`.

### 8.1 Log Files

- `logs/taskshift.log`: main application log.
- `logs/job_launches.jsonl`: structured launch-attempt events.
- `logs/scheduler_service.log`: service-process log when started as a managed loop.

### 8.2 Runtime State Files

- `logs/scheduler_service.pid`: PID of the active scheduler service process.
- `logs/scheduler_runtime_state.json`: current scheduler state exposed to the admin panel.

### 8.3 Export Files

Typical export root:

- `exports/historical_utilization/current/raw_job_rows.json`
- `exports/historical_utilization/current/state.json`
- `exports/historical_utilization/current/metadata.json`
- `exports/historical_utilization/current/series/*.json`

## 9. Admin Web Panel

The built-in admin panel is served by `src/admin_panel/server.py`.

### 9.1 Authentication

- token-based login using `ADMIN_PANEL_TOKEN`;
- token stored in `taskshift_admin_token` HTTP-only cookie;
- unauthenticated users only see the login page.

### 9.2 Main Capabilities

- inspect cluster topology and available snapshot sources;
- inspect runtime scheduler state and last run summary;
- trigger a manual scheduler run when attached to a live scheduler process;
- read and write selected configuration targets;
- browse and edit academic calendar files;
- inspect application and job launch logs.

### 9.3 Config Targets Exposed in UI

- active cluster config;
- scheduler config;
- web server config;

## 10. Scheduling Pass: Significant Implementation Details

This section describes the most important current semantics of the scheduler, including places where the behavior is subtle.

### 10.1 Pending Queue Order

Pending jobs are loaded from Slurm DB by `GET_JOBS_WITH_STATE_QUERY`.

Current SQL order is:

- `ORDER BY timelimit ASC`

This means the scheduler itself does not currently sort pending jobs by Slurm `priority`. The pass processes jobs in the order returned by storage, which today is shortest timelimit first.

### 10.2 Failed Launch Attempt Pool

The scheduler keeps an in-memory cache of launch attempts and a failed-job pool in `src/scheduler/attempt_cache.py`.

Behavior:

1. A job launched on tick `N` is recorded as `LAUNCH_ATTEMPTED`.
2. On tick `N+1`, if that job is still pending, TaskShift marks the old attempt as `LAUNCH_FAILED` with reason `job_still_pending_on_next_scheduler_tick`.
3. The job ID is added to the failed pool.
4. On the same tick, the job is skipped with summary status `BLOCKED_FAILED_POOL`.

The cache is process-local and is lazily cleared every 12 hours.

### 10.3 Resource Tree Construction

`ResourceAvailabilityTree.fromClusterAndJobs()` builds availability from:

- active cluster nodes from `cluster.yaml`;
- currently running jobs returned by storage.

It does not preload pending jobs into the tree. Pending jobs are applied one-by-one during the pass.

For each active node it creates `NodeResourceState` with:

- `nodeName`
- `featureName`
- `totalCpu`
- `totalGpu`
- `usedCpu`
- `usedGpu`

Each running job is converted into a placement and immediately reserved into the tree.

### 10.4 Placement Strategy

Placement search happens in `_findRunnablePlacement()` plus `ResourceAvailabilityTree.findPlacementOnFeature()`.

High-level algorithm:

1. Resolve partition-level allowed nodes and features.
2. Resolve requested features from job constraints.
3. For each candidate feature:
   - filter nodes to the partition;
   - enforce partition limits like `max_cpus_per_node` and `max_nodes`;
   - try to build a placement;
   - if forecast is enabled, check the forecast gate before accepting.

Node ranking prefers higher GPU capacity first and then higher CPU capacity.

### 10.5 Exact Nodes vs Minimum Nodes

If a job explicitly requests nodes through `tresReq`:

- TaskShift treats that as an exact node-count request.

If a job does not request a node count:

- TaskShift selects the minimum number of nodes whose aggregate available CPU and GPU satisfy the request.

### 10.6 Allocation Order Inside a Placement

For a chosen node set, TaskShift allocates:

1. GPU first;
2. CPU second.

If the job is multi-node, distribution is spread across selected nodes in unit steps (`1.0`) round-robin style.

If the job is effectively single-node or node count is unconstrained, resources are packed into the strongest nodes first.

### 10.7 Partition Enforcement

TaskShift respects:

- partition existence;
- partition state;
- partition node hostlist;
- partition feature availability;
- partition `max_cpus_per_node`;
- partition `max_nodes`.

If the job specifies an unknown partition, placement fails immediately.

### 10.8 Current Semantics Around Weights and Priority

Two important non-features in the current implementation:

- `node_group.weight` from cluster config is parsed and shown in admin views, but it is not used by the scheduler placement logic.
- job `priority` is stored on the job model, but the scheduler pass does not currently rank by it.

### 10.9 Running Job Accounting Nuance

When TaskShift loads running jobs into the resource tree, it currently uses:

- requested CPU from `cpusReq`
- requested GPU from `tresReq`

It does not use `tresAlloc` for placement reservation, even though helper methods for allocated values exist on `HistoricalJob`.

This means a mismatch between requested and actually allocated resources can cause the resource tree to reserve the requested amount rather than the allocated amount.

### 10.10 Forecast Gate Nuance

During placement validation, TaskShift computes both CPU and GPU request percentages for the feature. However, the current forecast gate only uses GPU availability:

- placement is accepted when `requestedGpuPercent <= forecast.availableGpuPercent`

CPU forecast is currently not enforced in the scheduler pass.

### 10.11 Feature Totals Nuance

`ResourceAvailabilityTree.getFeatureTotals()` computes:

- CPU totals from current available CPU;
- GPU totals from `totalGpu`, not `availableGpu`.

This is relevant because forecast percent calculations are based on these totals.

## 11. Forecast Subsystem

### 11.1 Data Source

The forecast model is trained from the `overall.json` utilization series, not per-feature series.

Training frame assembly currently:

- regularizes to a 15-minute grid;
- adds time-based cyclic features;
- adds lag features;
- adds rolling statistics;
- detects maintenance windows from zero-utilization periods;
- joins academic calendar flags and conference deadline pressure;
- builds a future target for median GPU utilization over the next 6 hours.

### 11.2 Model Choice

Training prefers:

1. `CatBoostRegressor`
2. `LightGBMRegressor`

If CatBoost is installed, it is chosen first. If not, LightGBM is used. If neither is installed, training fails.

### 11.3 Target Definition

The trained target is:

- `gpu_target_median_6h`

It represents the median GPU utilization over the next 6 hours at 15-minute resolution.

### 11.4 Artifact Freshness Policy

The artifact is considered fresh if it was trained no earlier than the latest scheduled training slot:

- Tuesday 00:00
- Friday 00:00

Startup behavior:

- fresh artifact: reuse immediately;
- stale artifact and startup training enabled: retrain;
- stale artifact and startup training disabled: reuse stale artifact;
- missing artifact and startup training disabled: fall back to historical average baseline.

### 11.5 Runtime Forecast Semantics

`ForecastService.buildFeatureForecast()` returns:

- feature name;
- horizon minutes;
- max CPU load percent;
- max GPU load percent.

Current implementation:

- CPU forecast is always `0.0`;
- GPU forecast uses the artifact's cached prediction;
- if no artifact exists, it falls back to historical average GPU load from the series;
- for horizons longer than 6 hours, the current code effectively repeats the same cached prediction and takes the median.

## 12. Slurm Connector Contract

TaskShift does not execute Slurm commands directly in Python. It delegates QoS
changes to mserver with an HTTP POST request.

The connector sends:

- method: `POST`
- URL: `connector.mserver_url`
- headers: `API_TOKEN: <TASKSHIFT_MSERVER_API_TOKEN>` and `Content-Type: application/json`
- body: `{"jobs": [JOB_ID], "qos": "TARGET_QOS"}`

Connector behavior:

- missing `connector.target_qos`: warning and no request;
- missing API token: error and no request;
- HTTP/network timeout uses `connector.timeout_seconds`;
- non-2xx HTTP response: logged as connector failure;
- 2xx response with `{"success": false}` or `{'success': False}` body: logged as connector failure.

The scheduler still records a launch attempt after calling the connector. It does not currently branch on the boolean result of `executeJob()`.

### 12.1 `mserver/` Directory

The `mserver/` directory contains a minimal standalone HTTP service for the
cluster-side QoS update path.

- `mserver/server_taskshift.py`: starts a `ThreadingHTTPServer`, checks the
  `API_TOKEN` request header, accepts `POST /slurm_set_job_qos`, parses the JSON
  body, and launches `set_job_qos_taskshift.py`.
- `mserver/set_job_qos_taskshift.py`: validates the incoming `jobs` and `qos`
  payload, checks that every job is in an allowed partition, and runs
  `sudo scontrol update job <jobs> qos=<qos>`.

The bundled server default is `0.0.0.0:9426`. Production deployments can run the
same endpoint elsewhere; TaskShift only needs the final URL in
`connector.mserver_url`. The TaskShift-side token value comes from
`TASKSHIFT_MSERVER_API_TOKEN` in `configs/.env` and is sent as the `API_TOKEN`
header.

## 13. Cluster Snapshot Refresh

TaskShift can refresh parsed cluster config from a command that prints Slurm config text.

Current behavior:

- parses nodes and partitions from marked sections in Slurm config text;
- writes the current YAML snapshot;
- writes a timestamped backup;
- on refresh failure, it can continue using the latest backup;
- if no backup exists but current config exists, it can seed a backup from the current config.

Backups are stored under:

- `configs/cluster_backups/YYYY/MM/DD/HHMMSS.yaml`

The scheduler loads the latest backup if one exists; otherwise it falls back to the current cluster config file.

## 14. Historical Export Pipeline

The export subsystem supports incremental historical utilization rebuilding.

### 14.1 Incremental Sync

`export` does:

- load previous state and raw cache if present;
- request only new or changed rows from DB using `mod_time`;
- merge by logical job ID;
- save merged raw cache;
- materialize logical jobs;
- rebuild series files.

### 14.2 Rebuild Without DB

`rebuild-series` uses only:

- `raw_job_rows.json`
- `state.json`

This is the offline path for rebuilding series from a saved export root.

## 15. Testing

### 15.1 Unit Tests

Run:

```bash
make test-unit
```

or:

```bash
. .venv/bin/activate
pytest tests/unit -v --tb=short --maxfail=10
```

Covered areas include:

- config loading and normalization;
- hostlist parsing and cluster config behavior;
- scheduler service summary and failed-attempt reconciliation;
- resource tree placement and reservation semantics;
- connector environment and subprocess behavior;
- admin panel config, logs, runtime state, calendars, system status;
- forecast artifact freshness and training schedule;
- historical utilization series behavior.

### 15.2 Integration Tests

Run:

```bash
make test-integration
```

This starts `tests/integration/compose.e2e.yaml` and verifies:

- pending GPU job launch;
- transition from pending to running in fake Slurm;
- GPU-capacity-aware skipping of oversized jobs;
- completion of launched jobs and removal from active queue;
- timelimit-based skipping.

Integration tests require a Dockerized MariaDB-backed environment. They do not run against a plain local checkout without that service stack.

### 15.3 Coverage

Run:

```bash
make coverage
```

## 16. Deployment

### 16.1 Recommended Docker Deployment

The deployment flow documented by the repository uses:

- `deploy/compose.yaml`
- `deploy/compose.bind.yaml`
- `deploy/Dockerfile`

Default container behavior:

- runs `taskshift schedule --with-web-panel`;
- exposes the admin panel on port `8000` inside the container;
- publishes host port `${TASKSHIFT_WEB_PORT:-8585}`.

### 16.2 Root-Level Docker Artifacts

The repository also contains root-level `dockerfile` and `compose.yaml`.

Important nuance:

- root `compose.yaml` currently passes `--forecast-data-dir=/app/exports/historical_utilization/current`;
- the current CLI does not define a `--forecast-data-dir` flag for `schedule`.

For that reason, the primary documented deployment path should be treated as `deploy/compose.yaml`, not the root compose file.

## 17. Operational Checklist

Before running TaskShift in a real environment, verify:

1. DB credentials resolve and the Slurm accounting tables are reachable.
2. `cluster_config_refresh_command` returns valid Slurm config text.
3. `connector.mserver_url`, `connector.target_qos`, and `TASKSHIFT_MSERVER_API_TOKEN` are configured.
4. `ADMIN_PANEL_TOKEN` is configured if web UI is enabled.
5. `forecast_enabled` is set only if export data and optional model dependencies are ready.
6. The cluster snapshot and partition definitions reflect actual production topology.

## 18. Known Limitations

- Pending queue order is not currently priority-based.
- Node-group `weight` is not used for placement decisions.
- Running-job resource reservation uses requested values rather than allocated values.
- Forecast gating currently uses only GPU availability.
- Training depends on optional ML libraries not included in base runtime requirements.
- The root `compose.yaml` is out of sync with the current CLI flags.
