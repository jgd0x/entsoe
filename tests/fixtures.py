import boto3
import pandas as pd
import pytest
from moto import mock_s3

from src.loader.entsoe_loader import EntsoeLoader


@pytest.fixture
def entsoe_loader() -> EntsoeLoader:
    start_date = pd.Timestamp('20210101', tz='Europe/Brussels')
    end_date = pd.Timestamp('20210102', tz='Europe/Brussels')

    loader = EntsoeLoader(start_date=start_date, end_date=end_date)

    return loader


@pytest.fixture
def s3_client():
    with mock_s3():
        yield boto3.client("s3")


@pytest.fixture
def valid_endpoint() -> str:
    return "imbalance_prices"


@pytest.fixture
def valid_area_code() -> str:
    return "AL"


@pytest.fixture
def invalid_endpoint() -> str:
    return "invalid_endpoint"


@pytest.fixture
def invalid_area_code() -> str:
    return "DE_AT_LU"