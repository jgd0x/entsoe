import datetime
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd

from src.loader.entsoe_loader import EntsoeLoader
from src.utils.dictionaries import entsoe_endpoints, entsoe_area_codes
from src.config import NUM_EXECUTORS


def fetch_entsoe_data(start, end, s3_client):
    """
    Fetches data from all the defined ENTSOE API endpoints and area codes.

    :param start: The start date of the data to fetch.
    :param end: The end date of the data to fetch.
    :param s3_client: The S3 client to use.

    :return: None
    """

    entsoe_loader = EntsoeLoader(start, end)

    with ThreadPoolExecutor(max_workers=NUM_EXECUTORS) as pool:
        for area in entsoe_area_codes:
            for endpoint in entsoe_endpoints:
                pool.submit(entsoe_loader.run, endpoint, area, s3_client)

"""
 return {
        'statusCode': 200,
        'body': 'Success'
    }

"""


if __name__ == "__main__":

    start_time = datetime.datetime.now()

    s3_client = boto3.client("s3")
    start = pd.Timestamp('20210101', tz='Europe/Brussels')
    end = pd.Timestamp('20210102', tz='Europe/Brussels')

    fetch_entsoe_data(start, end, s3_client)

    end_time = datetime.datetime.now()
    print(f"Total time: {end_time - start_time}")


