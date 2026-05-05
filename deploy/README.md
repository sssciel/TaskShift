# Docker Deployment

Build and start TaskShift:

```sh
docker compose -f deploy/compose.yaml up -d --build
```

The image is named `taskshift` and the service container is named `taskshift-server`.

Before starting, create `configs/.env` from `configs/.env.example` or pass these variables through the environment:

```dotenv
DB_HOST=""
DB_USER=""
DB_PASSWD=""
DB_DATABASE=""
ADMIN_PANEL_TOKEN=""
```

The compose file exposes the admin panel on host port `8000` by default. Override it with:

```sh
TASKSHIFT_WEB_PORT=8080 docker compose -f deploy/compose.yaml up -d
```

The default compose file persists config, logs, and exports in Docker named volumes. To bind those paths directly to this checkout instead, run with the bind override:

```sh
docker compose -f deploy/compose.yaml -f deploy/compose.bind.yaml up -d --build
```

When using the bind override, also create the runtime YAML files from the tracked examples:

```sh
cp configs/scheduler.example.yaml configs/scheduler.yaml
cp configs/cluster.example.yaml configs/cluster.yaml
cp configs/server.example.yaml configs/server.yaml
```
