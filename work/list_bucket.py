import argparse
import os

import boto3
from dotenv import load_dotenv

load_dotenv()


def list_prefix(s3, bucket, prefix):
    """Print all objects below an S3 prefix."""
    print(f"\n{prefix}")
    print("-" * len(prefix))

    found = False
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            found = True
            print(f"{obj['Size']:>12}  {obj['LastModified']}  {obj['Key']}")

    if not found:
        print("(no objects found)")


parser = argparse.ArgumentParser(
    description="List all objects under one or more S3 prefixes."
)
parser.add_argument(
    "prefixes",
    nargs="*",
    default=["silver/", "export/"],
    help="S3 prefixes to list (default: silver/ export/)",
)
args = parser.parse_args()

s3 = boto3.client("s3")
bucket = os.environ["S3_BUCKET"]

for prefix in args.prefixes:
    list_prefix(s3, bucket, prefix.rstrip("/") + "/")
        