import os
from typing import Dict

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from src.ingestion.TradingViewData import TradingViewClient, Interval


BUCKET_NAME = "mi-bucket-de-cripto"

CRYPTO_SYMBOLS: Dict[str, str] = {
    "Bitcoin": "BTCUSD",
    "Ethereum": "ETHUSD",
    "Ripple": "XRPUSD",
    "Solana": "SOLUSD",
    "Dogecoin": "DOGEUSD",
    "Cardano": "ADAUSD",
    "Shiba Inu": "SHIBUSD",
    "Polkadot": "DOTUSD",
    "Aave": "AAVEUSD",
    "Stellar": "XLMUSD",
}


def create_s3_bucket(bucket_name: str) -> None:
    """
    Create an S3 bucket if it does not already exist.

    Args:
        bucket_name (str): Name of the S3 bucket.

    Raises:
        ClientError: If an unexpected AWS error occurs.
    """
    s3 = boto3.client("s3")

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]

        if error_code == "404":
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Error checking bucket: {exc}")


def upload_to_s3(local_file_path: str, bucket_name: str, s3_key: str) -> None:
    """
    Upload a local file to an S3 bucket.

    Args:
        local_file_path (str): Path to the local file.
        bucket_name (str): Target S3 bucket.
        s3_key (str): Destination key in S3.

    Raises:
        NoCredentialsError: If AWS credentials are not configured.
    """
    s3 = boto3.client("s3")

    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Uploaded {local_file_path} → s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("AWS credentials not found.")
    except Exception as exc:
        print(f"Error uploading file: {exc}")


def fetch_and_store_data() -> None:
    """
    Fetch historical cryptocurrency data from TradingView,
    split it into yearly chunks, save it locally, and upload to S3.

    Workflow:
    1. Retrieve OHLCV data from TradingView
    2. Split data into yearly CSV files
    3. Save files locally
    4. Upload files to S3
    """
    os.makedirs("data/raw", exist_ok=True)

    client = TradingViewClient()

    for crypto_name, symbol in CRYPTO_SYMBOLS.items():
        print(f"Fetching data for {crypto_name}...")

        df = client.get_hist(
            symbol=symbol,
            exchange="COINBASE",
            interval=Interval.daily,
            n_bars=4 * 365,
        )

        for i in range(0, len(df), 365):
            chunk = df.iloc[i:i + 365]

            if chunk.empty:
                continue

            year = chunk.index[0].year

            local_path = f"data/raw/{crypto_name}_{year}.csv"
            s3_key = f"raw/{crypto_name}/{crypto_name}_{year}.csv"

            # Save locally
            chunk.to_csv(local_path)
            print(f"Saved {local_path}")

            # Upload to S3
            upload_to_s3(local_path, BUCKET_NAME, s3_key)


def run_pipeline() -> None:
    """
    Execute the full ingestion pipeline.

    Steps:
    1. Ensure S3 bucket exists
    2. Fetch cryptocurrency data
    3. Store data locally and upload to S3
    """
    create_s3_bucket(BUCKET_NAME)
    fetch_and_store_data()


if __name__ == "__main__":
    run_pipeline()