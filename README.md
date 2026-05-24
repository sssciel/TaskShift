## TaskShift

TaskShift is a Python scheduler service for Slurm-based clusters with forecast-aware job launching and cluster configuration snapshots. It also includes a small admin web panel for inspecting the cluster state and editing selected runtime configuration files.

Full service documentation is available in [docs/SERVICE.md](/Users/ciel/study/hpc2026/repo/TaskShift/docs/SERVICE.md).

### Requirements

- Python `3.11`
- Installed dependencies from [requirements.txt](/Users/ciel/study/hpc2026/repo/TaskShift/requirements.txt)
- Config files in [configs](/Users/ciel/study/hpc2026/repo/TaskShift/configs): `scheduler.yaml`, `server.yaml`, `cluster.yaml`, `.env`
- `ADMIN_PANEL_TOKEN` set in [configs/.env](/Users/ciel/study/hpc2026/repo/TaskShift/configs/.env) if you want to use the web panel

### Run

Install dependencies:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Run the scheduler with the web panel:

```bash
./taskshift schedule
```

Run the scheduler without the web panel:

```bash
./taskshift schedule --without-web-panel
```

Run only the web panel:

```bash
./taskshift serve-web-panel
```

### Web Panel Login

Open the address from the startup log, usually [http://127.0.0.1:8000](http://127.0.0.1:8000), and sign in with `ADMIN_PANEL_TOKEN` from [configs/.env](/Users/ciel/study/hpc2026/repo/TaskShift/configs/.env). Use the panel to inspect the cluster, edit `scheduler.yaml`, `server.yaml`, the active cluster config, and adjust scheduler-related settings.
