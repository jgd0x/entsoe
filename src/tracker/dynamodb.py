import boto3
from boto3.dynamodb.conditions import Key

from src.config import LOGGER, trackingTblConnection
from src.utils.utils import dynamodb_exception


class DynamoDBTracker:
    """Gets tracking data from dynamodb"""

    def __init__(self, region_name: str):
        self.resource = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.resource.Table(trackingTblConnection.DATA_FETCH_TRACKING_TABLE)

    def __repr__(self) -> str:
        return "DynamoDBTracker"

    @dynamodb_exception
    def update_tracking_tbl(self, partition_key: str, sort_key: str, fetch_date: str) -> bool:
        """
        Updates the tracking table with the latest fetch date for the given partition key.
        """
        LOGGER.info(f"Updating Tracking Table for {partition_key}")
        item = {
            trackingTblConnection.PARTITION_KEY: partition_key,
            trackingTblConnection.SORT_KEY: sort_key,
            trackingTblConnection.FETCH_DATE: fetch_date}
        self.table.put_item(Item=item)
        return True

    @dynamodb_exception
    def read_tracking_tbl(self, partition_key) -> dict:
        """Gets content from DB for the given partition key."""
        LOGGER.info(f"Reading Tracking Table for {partition_key}")
        response = self.table.query(
            KeyConditionExpression=(Key(trackingTblConnection.PARTITION_KEY).eq(partition_key)),
            ScanIndexForward=False,
            Limit=1)
        if not response['Items']:
            return {}
        LOGGER.info(response)
        return response.get('Items')[0]

