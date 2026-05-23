"""Create the single travellens-data bucket in MinIO. Idempotent."""

import os
import sys
import time

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def make_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def init_buckets():
    s3 = make_client()

    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        print("  ERROR: S3_BUCKET not set in .env", file=sys.stderr)
        sys.exit(1)

    try:
        s3.create_bucket(Bucket=bucket)
        print(f"  Created : {bucket}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            print(f"  Exists  : {bucket}")
        else:
            print(f"  ERROR creating {bucket}: {e}", file=sys.stderr)
            sys.exit(1)

    print()
    print("Buckets in MinIO:")
    resp = s3.list_buckets()
    for b in resp["Buckets"]:
        print(f"  {b['Name']}")
    print(f"\nTotal: {len(resp['Buckets'])} bucket(s)")


if __name__ == "__main__":
    for attempt in range(2):
        try:
            init_buckets()
            break
        except Exception as exc:
            if attempt == 0:
                print(f"MinIO not ready ({exc}), retrying in 10s ...")
                time.sleep(10)
            else:
                print(f"Failed: {exc}", file=sys.stderr)
                sys.exit(1)
