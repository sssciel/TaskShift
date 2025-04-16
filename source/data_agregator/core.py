from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from .client import db_client, org, bucket


def save_data_db(measurement, df):
    write_api = db_client.write_api(write_options=SYNCHRONOUS)

    points = []
    for index, row in df.iterrows():
        point = (
            Point(measurement).time(row["ds"], WritePrecision.NS).field("y", row["y"])
        )
        points.append(point)

    write_api.write(bucket=bucket, org=org, record=points)

    write_api.close()


def get_full_data_db():
    write_api = db_client.query_api()

    # Collect all the data from the database and combine the cpu and gpu load.
    query = """from(bucket: "cluster_load")
    |> range(start: 1970-01-01T00:00:00Z)
    |> filter(fn: (r) => r._measurement == "gpu_load" or r._measurement == "cpu_load")
    |> pivot(
        rowKey: ["_time"],
        columnKey: ["_measurement"],
        valueColumn: "_value"
    )
    """

    df = write_api.query_data_frame(org=org, query=query)

    return df
