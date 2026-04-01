## TaskShift

Python version: `3.11.12`

### Docker

Build and run the scheduler loop:

```bash
docker compose up --build
```

Run a one-off scheduler pass:

```bash
docker compose run --rm taskshift run-scheduler-once
```

Run historical export:

```bash
docker compose run --rm taskshift export --output-dir=/app/exports/historical_utilization/current
```

The container uses:

- [configs](/Users/ciel/study/hpc2026/repo/TaskShift/configs) for configuration files
- [logs](/Users/ciel/study/hpc2026/repo/TaskShift/logs) for append-only logs
- [exports](/Users/ciel/study/hpc2026/repo/TaskShift/exports) for generated historical series

If a real config file is missing, the container bootstraps it from the corresponding `*.example` file.
