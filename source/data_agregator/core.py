from configs.logging import log
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from .client import bucket, get_db_client, org


def save_data_db(measurement, df):
    """
    Saves the data to the database.
    Args:
        measurement (str): The name of the measurement.
        df (pd.DataFrame): The data to be saved.
    """
    log.info("Saving data to the database")
    write_api = get_db_client().write_api(write_options=SYNCHRONOUS)

    points = []
    for index, row in df.iterrows():
        point = (
            Point(measurement).time(row["ds"], WritePrecision.NS).field("y", row["y"])
        )
        points.append(point)

    write_api.write(bucket=bucket, org=org, record=points)

    write_api.close()

    log.debug("The data has been written to the database.")


def get_full_data_db():
    """
    Gets the data from the database.
    Returns:
        pd.DataFrame: The data from the database.
    """
    log.info("Getting data from the database")

    write_api = get_db_client().query_api()

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
