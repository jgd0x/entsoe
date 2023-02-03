import pandas as pd
import pytest
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import HTTPError

from src.config import ENTSOE_TARGET_S3_BUCKET
from src.loader.entsoe_loader import EntsoeLoader

from fixtures import entsoe_loader, s3_client
from fixtures import valid_endpoint, valid_area_code, invalid_endpoint, invalid_area_code


def test_api_valid_request(entsoe_loader: EntsoeLoader, valid_endpoint: str, valid_area_code: str):
    data = entsoe_loader.fetch_from_api(start=entsoe_loader.start_date, end=entsoe_loader.end_date,
                                        endpoint=valid_endpoint, area=valid_area_code)
    assert data.empty is False


def test_api_invalid_endpoint(entsoe_loader: EntsoeLoader, invalid_endpoint: str, valid_area_code: str):
    with pytest.raises(ValueError):
        entsoe_loader.fetch_from_api(start=entsoe_loader.start_date, end=entsoe_loader.end_date,
                                     endpoint=invalid_endpoint, area=valid_area_code)


def test_api_no_data(entsoe_loader: EntsoeLoader, valid_endpoint: str, valid_area_code: str):
    start_date = pd.Timestamp('20100101', tz='Europe/Brussels')
    end_date = pd.Timestamp('20100102', tz='Europe/Brussels')

    result = entsoe_loader.fetch_from_api(start=start_date, end=end_date,
                                          endpoint=valid_endpoint, area=valid_area_code)
    assert isinstance(result, NoMatchingDataError)


def test_api_http_error(entsoe_loader: EntsoeLoader, valid_endpoint: str, invalid_area_code: str):
    result = entsoe_loader.fetch_from_api(start=entsoe_loader.start_date, end=entsoe_loader.end_date,
                                                endpoint=valid_endpoint, area=invalid_area_code)
    assert isinstance(result, HTTPError)


def test_fetch_and_upload(entsoe_loader: EntsoeLoader, valid_endpoint: str, valid_area_code: str, s3_client):
    s3_client.create_bucket(Bucket=ENTSOE_TARGET_S3_BUCKET)
    result = entsoe_loader.run(endpoint=valid_endpoint, area=valid_area_code, s3_client=s3_client)

    assert result is True
