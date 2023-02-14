from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd

from src.config import NUM_EXECUTORS, DEFAULT_INIT_DATE
from src.loader.entsoe_loader import EntsoeLoader
from src.tracker.dynamodb import DynamoDBTracker
from src.utils.dictionaries import entsoe_endpoints, entsoe_area_codes


def lambda_handler(event, context) -> dict:
    """
    Lambda function to fetch data from ENTSOE API and upload to S3.

    :param event: The event that triggered the lambda function.
    :param context: The context of the lambda function.

    :return: Success message.
    """

    tracker = DynamoDBTracker(region_name="eu-west-3")
    s3_client = boto3.client("s3")
    entsoe_loader = EntsoeLoader(tracker, s3_client)

    with ThreadPoolExecutor(max_workers=NUM_EXECUTORS) as pool:
        for area in entsoe_area_codes:
            for endpoint in entsoe_endpoints:

                partition_key = f"DATASOURCE#ENTSOE#DATASET#{endpoint.upper()}#AREA#{area.upper()}"
                last_item = tracker.read_tracking_tbl(partition_key)
                # Check if any data has been fetched for this endpoint and area
                if last_item:
                    start_date = pd.Timestamp(last_item["sort_key"], tz="Europe/Brussels")
                    if start_date >= entsoe_loader.end_date:
                        continue
                else:
                    start_date = pd.Timestamp(DEFAULT_INIT_DATE, tz="Europe/Brussels")

                pool.submit(entsoe_loader.run, endpoint, area, start_date, partition_key)

    return {'statusCode': 200, 'body': 'Success'}


if __name__ == "__main__":
    lambda_handler(None, None)
