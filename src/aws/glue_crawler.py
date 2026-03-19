import boto3
from botocore.exceptions import ClientError


AWS_REGION = "eu-south-2"
DATABASE_NAME = "trade_data_imat3b10"
ROLE_ARN = "arn:aws:iam::354918392915:role/GlueCrawlerRole"
BUCKET_NAME = "cargadatostradingview"
CRAWLER_NAME = "s3-to-glue-crawler"
S3_TARGET_PATH = f"s3://{BUCKET_NAME}/raw/"


glue_client = boto3.client("glue", region_name=AWS_REGION)


def crawler_exists(crawler_name: str) -> bool:
    """
    Check whether a Glue crawler already exists.

    Args:
        crawler_name (str): Name of the Glue crawler.

    Returns:
        bool: True if the crawler exists, False otherwise.

    Raises:
        ClientError: If an unexpected AWS error occurs.
    """
    try:
        glue_client.get_crawler(Name=crawler_name)
        return True
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code == "EntityNotFoundException":
            return False
        raise


def create_crawler() -> None:
    """
    Create a Glue crawler to scan data stored in S3 and register it
    in the Glue Data Catalog.

    The crawler will:
    - Scan the specified S3 path
    - Infer schema automatically
    - Store metadata in the specified Glue database

    If the crawler already exists, it will not be recreated.
    """
    if crawler_exists(CRAWLER_NAME):
        print(f"Crawler '{CRAWLER_NAME}' already exists.")
        return

    try:
        glue_client.create_crawler(
            Name=CRAWLER_NAME,
            Role=ROLE_ARN,
            DatabaseName=DATABASE_NAME,
            Targets={"S3Targets": [{"Path": S3_TARGET_PATH}]},
        )
        print(f"Crawler '{CRAWLER_NAME}' created successfully.")
    except ClientError as exc:
        print(f"Error creating crawler '{CRAWLER_NAME}': {exc}")


def start_crawler() -> None:
    """
    Start the Glue crawler execution.

    This triggers schema inference and updates the Glue Data Catalog
    with the latest structure of the data stored in S3.
    """
    try:
        glue_client.start_crawler(Name=CRAWLER_NAME)
        print(f"Crawler '{CRAWLER_NAME}' started successfully.")
    except ClientError as exc:
        print(f"Error starting crawler '{CRAWLER_NAME}': {exc}")


def main() -> None:
    """
    Entry point for the crawler script.

    Executes the following steps:
    1. Creates the crawler if it does not exist
    2. Starts the crawler to update the Glue Data Catalog
    """
    create_crawler()
    start_crawler()


if __name__ == "__main__":
    main()