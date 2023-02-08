import pytest
from botocore.exceptions import ClientError

from src.tracker.dynamodb import DynamoDBTracker
from src.config import trackingTblConnection
from tests.fixtures import table_tracker, invalid_table_tracker
from tests.fixtures import valid_endpoint, valid_area_code, invalid_endpoint


####################################################################################################
####################################################################################################
def test_update_tracking_tbl(table_tracker: DynamoDBTracker, valid_endpoint: str, valid_area_code: str):
    partition_key = f"DATASOURCE#ENTSOE#DATASET#{valid_endpoint.upper()}#AREA#{valid_area_code.upper()}"
    sort_key = "2023-01-01"
    fetch_date = "2023-01-02"
    result = table_tracker.update_tracking_tbl(partition_key, sort_key, fetch_date)
    assert result is True


####################################################################################################
####################################################################################################
def test_error_tracking_table(invalid_table_tracker, valid_endpoint: str, valid_area_code: str,
                              caplog: pytest.LogCaptureFixture):
    """ Test that the update_tracking_tbl method raises a ClientError when the table does not exist"""
    partition_key = f"DATASOURCE#ENTSOE#DATASET#{valid_endpoint.upper()}#AREA#{valid_area_code.upper()}"
    with pytest.raises(ClientError) as exc_info:
        # Add a request that is expected to raise a ClientError
        invalid_table_tracker.read_tracking_tbl(partition_key)
    assert f"The table does not exist" in caplog.text
    assert 'ResourceNotFoundException' in str(exc_info.value)


####################################################################################################
####################################################################################################
def test_no_item_found(table_tracker: DynamoDBTracker, invalid_endpoint: str, valid_area_code: str):
    """ Test that the read_tracking_tbl method returns False when no item is found"""
    partition_key = f"DATASOURCE#ENTSOE#DATASET#{invalid_endpoint.upper()}#AREA#{valid_area_code.upper()}"
    result = table_tracker.read_tracking_tbl(partition_key)
    assert not result


####################################################################################################
####################################################################################################
def test_item_found(table_tracker: DynamoDBTracker, valid_endpoint: str, valid_area_code: str):
    """ """
    partition_key = f"DATASOURCE#ENTSOE#DATASET#{valid_endpoint.upper()}#AREA#{valid_area_code.upper()}"
    expected_result = {
        trackingTblConnection.PARTITION_KEY: partition_key,
        trackingTblConnection.SORT_KEY: "2023-01-01",
        trackingTblConnection.FETCH_DATE: "2023-01-05"}
    result = table_tracker.read_tracking_tbl(partition_key)
    assert result == expected_result
