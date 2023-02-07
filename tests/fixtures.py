import boto3
import pandas as pd
import pytest
from moto import mock_s3, mock_dynamodb

from src.loader.entsoe_loader import EntsoeLoader
from src.tracker.dynamodb import DynamoDBTracker


@pytest.fixture
def entsoe_loader() -> EntsoeLoader:
    return EntsoeLoader()


@pytest.fixture
def table_tracker():
    with mock_dynamodb():
        tracker = DynamoDBTracker("eu-west-1", "tracker")


@pytest.fixture
def start_date() -> EntsoeLoader:
    return pd.Timestamp("2022-12-20", tz="Europe/Brussels")


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
