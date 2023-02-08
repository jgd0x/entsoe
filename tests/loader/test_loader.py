import boto3
import pandas as pd
import pytest

from src.config import ENTSOE_TARGET_S3_BUCKET
from src.loader.entsoe_loader import EntsoeLoader
from src.tracker.dynamodb import DynamoDBTracker

from tests.fixtures import entsoe_loader, s3_client, table_tracker, start_date
from tests.fixtures import valid_endpoint, valid_area_code, invalid_endpoint, invalid_area_code


####################################################################################################
####################################################################################################
def test_api_valid_request(entsoe_loader: EntsoeLoader, start_date: pd.Timestamp,
                           valid_endpoint: str, valid_area_code: str):
    data = entsoe_loader.fetch_from_api(start=start_date, end=entsoe_loader.end_date,
                                        endpoint=valid_endpoint, area=valid_area_code)
    assert data.empty is False


####################################################################################################
####################################################################################################
def test_api_invalid_endpoint(entsoe_loader: EntsoeLoader, start_date: pd.Timestamp,
                              invalid_endpoint: str, valid_area_code: str):
    with pytest.raises(ValueError):
        entsoe_loader.fetch_from_api(start=start_date, end=entsoe_loader.end_date,
                                     endpoint=invalid_endpoint, area=valid_area_code)


####################################################################################################
####################################################################################################
def test_api_no_data(entsoe_loader: EntsoeLoader, valid_endpoint: str, valid_area_code: str,
                     caplog: pytest.LogCaptureFixture):
    start_date = pd.Timestamp('20100101', tz='Europe/Brussels')
    end_date = pd.Timestamp('20100102', tz='Europe/Brussels')

    result = entsoe_loader.fetch_from_api(start=start_date, end=end_date,
                                          endpoint=valid_endpoint, area=valid_area_code)
    assert f"No {valid_endpoint} data found for {valid_area_code}" in caplog.text
    assert result.empty is True


####################################################################################################
####################################################################################################
def test_api_http_error(entsoe_loader: EntsoeLoader, start_date: pd.Timestamp, valid_endpoint: str,
                        invalid_area_code: str, caplog: pytest.LogCaptureFixture):
    result = entsoe_loader.fetch_from_api(start=start_date, end=entsoe_loader.end_date,
                                          endpoint=valid_endpoint, area=invalid_area_code)

    assert f"Delivered area EIC: {invalid_area_code} is not valid for {valid_endpoint}" in caplog.text
    assert result.empty is True


####################################################################################################
####################################################################################################
def test_fetch_and_upload(entsoe_loader: EntsoeLoader, start_date: pd.Timestamp,
                          valid_endpoint: str, valid_area_code: str):
    partition_key = f"DATASOURCE#ENTSOE#DATASET#{valid_endpoint.upper()}#AREA#{valid_area_code.upper()}"
    result = entsoe_loader.run(endpoint=valid_endpoint, area=valid_area_code, start_date=start_date,
                               partition_key=partition_key)

    assert result is True
