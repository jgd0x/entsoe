import os
from datetime import timedelta, datetime
from tempfile import mkdtemp

import pandas as pd
import pytz
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError, InvalidBusinessParameterError
from requests.exceptions import HTTPError

from src.config import API_KEY, WINDOW_SIZE, LOGGER, ENTSOE_TARGET_S3_BUCKET
from src.utils.dictionaries import entsoe_endpoints, entsoe_area_codes
from src.utils.utils import generate_file_name, upload_file_to_s3


class EntsoeLoader:

    def __init__(self, start_date, end_date):
        self.client = EntsoePandasClient(api_key=API_KEY)
        self.start_date: pd.Timestamp = start_date
        self.end_date: pd.Timestamp = end_date
        self.window_size: int = WINDOW_SIZE
        self.current_date = self.current_date()
        self.temp_folder = mkdtemp()

    @staticmethod
    def current_date():
        now = datetime.now(pytz.utc).replace(tzinfo=None)
        current_date = now.strftime("%Y-%m-%d")

        return current_date

    def fetch_from_api(self, endpoint: str, area: str, start: pd.Timestamp, end: pd.Timestamp):
        """
        Fetches data from the ENTSOE API and returns a pandas dataframe.

        :param endpoint: The endpoint to fetch data from.
        :param area: The area to fetch data from.
        :param start: The start date of the data to fetch.
        :param end: The end date of the data to fetch.

        :return: A pandas dataframe with the fetched data.
        """
        try:
            fetched_data = getattr(self.client, entsoe_endpoints[endpoint])(
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

        except (NoMatchingDataError, InvalidBusinessParameterError) as e:
            LOGGER.exception(f"No {endpoint} data found for {area} between {self.start_date} to {self.end_date}")
            return e
        except HTTPError as e:
            LOGGER.exception(f"Delivered area EIC: {area} is not valid for {endpoint}")
            return e
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"Invalid endpoint or area code: {endpoint} or {area}")

    def run(self, endpoint: str, area: str, s3_client):
        """
        Fetches data from the ENTSOE API and uploads it to S3.
        Data is fetched in windows defined by the WINDOW_SIZE

        :param endpoint: The endpoint to fetch data from.
        :param area:  The area to fetch data from.
        :param s3_client: The S3 client to use.

        :return: None
        """
        iteration_start_date = self.start_date

        while iteration_start_date < self.end_date:
            iteration_end_date = iteration_start_date + timedelta(days=self.window_size)

            if iteration_end_date > self.end_date:
                iteration_end_date = self.end_date

            fetched_data = self.fetch_from_api(endpoint, area, iteration_start_date, iteration_end_date)
            file_name = generate_file_name(endpoint, area, self.current_date, self.start_date, self.end_date)
            if not (isinstance(fetched_data, (NoMatchingDataError, InvalidBusinessParameterError))
                    or (isinstance(fetched_data, HTTPError))):
                fetched_data.to_csv(os.path.join(self.temp_folder, file_name), index=False)
                upload_file_to_s3(s3_client, os.path.join(self.temp_folder, file_name),
                                  ENTSOE_TARGET_S3_BUCKET, os.path.join(endpoint,file_name))

            iteration_start_date = iteration_end_date
        return True
