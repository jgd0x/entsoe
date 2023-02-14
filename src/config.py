"""Config file for the entsoe-py package"""

import logging
import os

# ENTSOE API CONFIG
API_KEY = os.getenv("API_KEY")
WINDOW_SIZE = 365
DEFAULT_INIT_DATE = "2020-01-01"

# S3 CONFIG
ENTSOE_TARGET_S3_BUCKET = "entsoe-test"

# CONCURRENT EXECUTION CONFIG
NUM_EXECUTORS = 8


# DYNAMODB CONFIG (TRACKER)
class trackingTblConnection():
    """
    Class to hold the connection details for the DynamoDB table
    used to track the data fetches
    """
    DATA_FETCH_TRACKING_TABLE = "tracker"
    PARTITION_KEY = "partition_key"
    SORT_KEY = "sort_key"
    FETCH_DATE = "fetch_date"


# LOGGING CONFIG
LOGGER = logging.getLogger()
LOGGER.setLevel(level=os.getenv("LOG_LEVEL") if os.getenv("LOG_LEVEL") else logging.INFO)
