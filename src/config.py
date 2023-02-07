import logging
import os

API_CALL_DELAY = 8
API_KEY = os.getenv("API_KEY")
NUM_EXECUTORS = 8
ENTSOE_TARGET_S3_BUCKET = "entsoe-test"
WINDOW_SIZE = 365

# Setup logger
LOGGER = logging.getLogger()
LOGGER.setLevel(level=os.getenv("LOG_LEVEL") if os.getenv("LOG_LEVEL") else logging.INFO)
