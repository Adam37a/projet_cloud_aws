import boto3, json
from datetime import datetime
from decimal import Decimal

REGION = "eu-west-3"
BUCKET = "lyon-s3-raw-dev"
PREFIX = "supervision_trafic_temps_reel"
TABLE_NAME = "TrafficRealtime"

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    return obj

def list_date_prefixes():
    """List all date folders in the S3 prefix supervision_trafic_temps_reel/."""
    paginator = s3.get_paginator("list_objects_v2")
    result = set()
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX + "/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            date_folder = prefix["Prefix"].split("/")[-2]
            result.add(date_folder)
    return sorted(result)

def list_s3_objects(date_prefix):
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{PREFIX}/{date_prefix}/"
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def load_existing_dates():
    """Extract all distinct dates already present in DynamoDB (from timestamp_capture)."""
    existing = set()
    response = table.scan(ProjectionExpression="timestamp_capture")
    for item in response.get("Items", []):
        ts = item.get("timestamp_capture")
        if ts:
            existing.add(str(ts)[:10])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="timestamp_capture",
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        for item in response.get("Items", []):
            ts = item.get("timestamp_capture")
            if ts:
                existing.add(str(ts)[:10])
    return existing

def ingest_date(date_prefix):
    """Ingest all JSONs for a given date."""
    print(f"📦 Ingesting S3 date folder: {date_prefix}")
    for key in list_s3_objects(date_prefix):
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        records = json.loads(obj["Body"].read())

        with table.batch_writer() as batch:
            for rec in records:
                batch.put_item(Item=to_decimal(rec))
        print(f"   ✅ Inserted {len(records)} records from {key}")

def main():
    print("🚀 Checking S3 and DynamoDB consistency...")
    s3_dates = list_date_prefixes()
    existing_dates = load_existing_dates()

    print(f"📅 Dates in S3: {s3_dates}")
    print(f"📊 Dates in DynamoDB: {sorted(existing_dates)}")

    missing_dates = [d for d in s3_dates if d not in existing_dates]

    if not missing_dates:
        print("✅ DynamoDB is up to date. Nothing to ingest.")
        return

    print(f"🆕 Missing dates to ingest: {missing_dates}")
    for d in missing_dates:
        ingest_date(d)

if __name__ == "__main__":
    main()
