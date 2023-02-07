from botocore.exceptions import ClientError
import os

from src.config import LOGGER


def generate_file_name(endpoint, area, current_date, start_date, end_date):
    """
    Generates a file name for the fetched data.
    :param endpoint: The endpoint to fetch data from.
    :param area: The area to fetch data from.
    :param current_date: The current date.
    :param start_date: The start date of the data to fetch.
    :param end_date: The end date of the data to fetch.

    :return: A string with the generated file name.
    """
    file_start_date = start_date.strftime("%Y-%m-%d")
    file_end_date = end_date.strftime("%Y-%m-%d")

    file_name = f"dataset={endpoint}_area={area}_fetch_date={current_date}" \
                f"_start_date={file_start_date}_end_date={file_end_date}.csv"

    return file_name


def upload_file_to_s3(s3_client, file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket
    :param s3_client: The S3 client to use.
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used

    :return: True if file was uploaded, else False.
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={"ServerSideEncryption": "AES256"})
    except ClientError as e:
        LOGGER.exception(e)
        return False
    return True


def dynamodb_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except ClientError as e:
            if e.response['Error']['Code'] == "ResourceNotFoundException":
                LOGGER.info("The table does not exist")
                return None
            else:
                raise e

    return wrapper
