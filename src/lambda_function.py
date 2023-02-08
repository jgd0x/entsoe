import datetime
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd

from src.loader.entsoe_loader import EntsoeLoader
from src.utils.dictionaries import entsoe_endpoints, entsoe_area_codes
from src.config import NUM_EXECUTORS
from src.tracker.dynamodb import DynamoDBTracker


def fetch_entsoe_data(tracker: DynamoDBTracker, s3_client: boto3.client) -> dict:
    """
    Fetches data from all the defined ENTSOE API endpoints and area codes.
    Fetch is performed from the last date that data was fetched until the
    current date (excluded).

    :param tracker: DynamoDB tracker to check whether data has already been fetched.
    :param s3_client: The S3 client to use.

    :return: 200 Success
    """

    entsoe_loader = EntsoeLoader(tracker, s3_client)

    with ThreadPoolExecutor(max_workers=NUM_EXECUTORS) as pool:
        for area in entsoe_area_codes:
            for endpoint in entsoe_endpoints:

                partition_key = f"DATASOURCE#ENTSOE#DATASET#{endpoint.upper()}#AREA#{area.upper()}"
                last_item = tracker.read_tracking_tbl(partition_key)
                # Check if any data has been fetched for this endpoint and area
                if last_item:
                    start_date = pd.Timestamp(last_item["sort_key"], tz="Europe/Brussels")
                else:
                    # TODO: Define default start date
                    start_date = pd.Timestamp("2023-01-01", tz="Europe/Brussels")

                pool.submit(entsoe_loader.run, endpoint, area, start_date, partition_key, tracker, s3_client)

    return {'statusCode': 200, 'body': 'Success'}


if __name__ == "__main__":
    start_time = datetime.datetime.now()

    tracker = DynamoDBTracker(region_name="eu-west-3")
    s3_client = boto3.resource("s3")
    fetch_entsoe_data(tracker, s3_client)

    end_time = datetime.datetime.now()
    print(f"Total time: {end_time - start_time}")
