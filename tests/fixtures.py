import typing

import boto3
import pandas as pd
import pytest
from moto import mock_s3, mock_dynamodb

from src.loader.entsoe_loader import EntsoeLoader
from src.tracker.dynamodb import DynamoDBTracker
from src.config import ENTSOE_TARGET_S3_BUCKET, trackingTblConnection


@pytest.fixture()
def entsoe_loader(table_tracker: DynamoDBTracker, s3_client: boto3.client) -> EntsoeLoader:
    return EntsoeLoader(table_tracker, s3_client)


@pytest.fixture()
def invalid_table_tracker() -> typing.Generator:
    with mock_dynamodb():
        dynamo_db = boto3.resource("dynamodb", region_name="eu-west-1")
        yield DynamoDBTracker("eu-west-1")


@pytest.fixture()
def table_tracker() -> typing.Generator:
    with mock_dynamodb():
        dynamo_db = boto3.resource("dynamodb", region_name="eu-west-1")
        dynamo_db.create_table(
            TableName=trackingTblConnection.DATA_FETCH_TRACKING_TABLE,
            KeySchema=[
                {
                    "AttributeName": trackingTblConnection.PARTITION_KEY,
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": trackingTblConnection.SORT_KEY,
                    "KeyType": "RANGE",
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": trackingTblConnection.PARTITION_KEY,
                    "AttributeType": "S",
                },
                {
                    "AttributeName": trackingTblConnection.SORT_KEY,
                    "AttributeType": "S",
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
        dynamo_db.Table(trackingTblConnection.DATA_FETCH_TRACKING_TABLE).put_item(
            Item={trackingTblConnection.PARTITION_KEY: "DATASOURCE#ENTSOE#DATASET#IMBALANCE_PRICES#AREA#AL",
                  trackingTblConnection.SORT_KEY: "2023-01-01",
                  trackingTblConnection.FETCH_DATE: "2023-01-05"})
        yield DynamoDBTracker("eu-west-1")


@pytest.fixture
def start_date() -> pd.Timestamp:
    return pd.Timestamp("2022-12-20", tz="Europe/Brussels")


@pytest.fixture
def s3_client() -> boto3.client:
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=ENTSOE_TARGET_S3_BUCKET)
        yield s3_client


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
