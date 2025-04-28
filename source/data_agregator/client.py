import os

import influxdb_client
from configs.config import load_env_config

load_env_config()

token = os.getenv("INFLUXDB_TOKEN", None)
org = os.getenv("INFLUXDB_ORG", "cluster")
host = os.getenv("INFLUXDB_HOST", "http://localhost")
port = os.getenv("INFLUXDB_PORT", 8086)
bucket = os.getenv("INFLUXDB_BUCKET", "cluster_load")

url = f"{host}:{port}"

db_client = None


def get_db_client():
    """
    Returns the InfluxDB client.
    """
    global db_client
    if db_client is None:
        db_client = influxdb_client.InfluxDBClient(
            url=url,
            token=token,
            org=org,
        )
    return db_client
