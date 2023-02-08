import os
from datetime import timedelta, date
from tempfile import mkdtemp
import boto3

import pandas as pd
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError, InvalidBusinessParameterError
from requests.exceptions import HTTPError

from src.config import API_KEY, WINDOW_SIZE, LOGGER, ENTSOE_TARGET_S3_BUCKET
from src.utils.dictionaries import entsoe_endpoints, entsoe_area_codes
from src.utils.utils import generate_file_name, upload_file_to_s3
from src.tracker.dynamodb import DynamoDBTracker


class EntsoeLoader:

    def __init__(self, tracker: DynamoDBTracker, s3_client: boto3.client):
        self.entsoe_client = EntsoePandasClient(api_key=API_KEY)
        self.tracker = tracker
        self.s3_client = s3_client
        self.temp_folder = mkdtemp()
        self.current_date = self.current_date()
        self.end_date = self.end_date()
        self.window_size: int = WINDOW_SIZE

    def __repr__(self):
        return f"ENTSOE Loader"

    @staticmethod
    def current_date() -> str:
        return date.today().strftime('%Y-%m-%d')

    @staticmethod
    def end_date() -> pd.Timestamp:
        return pd.Timestamp(date.today() - timedelta(days=1), tz="Europe/Brussels")

    def fetch_from_api(self, endpoint: str, area: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """
        Fetches data from the ENTSOE API and returns a pandas dataframe.

        :param endpoint: The endpoint to fetch data from.
        :param area: The area to fetch data from.
        :param start: The start date of the data to fetch.
        :param end: The end date of the data to fetch.

        :return: A pandas dataframe with the fetched data.
        """
        try:
            fetched_data = getattr(self.entsoe_client, entsoe_endpoints[endpoint])(
                entsoe_area_codes[area], start=start, end=end)

            # Taking care of the special case for day_ahead_prices
            if isinstance(fetched_data, pd.Series):
                if not fetched_data.name:
                    fetched_data = fetched_data.rename("spot_fx")
                fetched_data = fetched_data.to_frame()
            fetched_data = fetched_data.reset_index()
            fetched_data.rename(columns={"index": "value_date_time"}, inplace=True)
            fetched_data["area"] = area
            fetched_data["data_set_name"] = endpoint

            return fetched_data

        except (NoMatchingDataError, InvalidBusinessParameterError):
            LOGGER.exception(f"No {endpoint} data found for {area} between {start} to {end}")
            return pd.DataFrame()
        except HTTPError:
            LOGGER.exception(f"Delivered area EIC: {area} is not valid for {endpoint}")
            return pd.DataFrame()
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"Invalid endpoint or area code: {endpoint} or {area}")

    def run(self, endpoint: str, area: str, start_date: pd.Timestamp, partition_key: str) -> bool:
        """
        Fetches data from the ENTSOE API and uploads it to S3.
        Data is fetched in windows defined by the WINDOW_SIZE

        :param endpoint: The endpoint to fetch data from.
        :param area: The area to fetch data from.
        :param start_date: The start date of the data to fetch.
        :param partition_key: DynamoDB partition key.

        :return: True if the data was fetched and uploaded successfully.
        """
        iteration_start_date = start_date
        sort_key = None

        while iteration_start_date < self.end_date:
            iteration_end_date = iteration_start_date + timedelta(days=self.window_size)

            if iteration_end_date > self.end_date:
                iteration_end_date = self.end_date

            fetched_data = self.fetch_from_api(endpoint, area, iteration_start_date, iteration_end_date)
            file_name = generate_file_name(endpoint, area, self.current_date, start_date, self.end_date)

            if not fetched_data.empty:
                sort_key = fetched_data["value_date_time"].max().strftime("%Y-%m-%d")
                fetched_data.to_csv(os.path.join(self.temp_folder, file_name), index=False)
                upload_file_to_s3(self.s3_client, os.path.join(self.temp_folder, file_name),
                                  ENTSOE_TARGET_S3_BUCKET, os.path.join(endpoint, file_name))

            iteration_start_date = iteration_end_date

        # If data was fetched, update the tracker
        if sort_key is not None:
            self.tracker.update_tracking_tbl(partition_key, sort_key, self.current_date)

        return True
