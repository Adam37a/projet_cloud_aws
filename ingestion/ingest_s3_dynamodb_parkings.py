import boto3
import json
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

# === CONFIGURATION ===
REGION = "eu-west-3"
BUCKET = "lyon-s3-raw-dev"
PREFIX = "disponibilites_parkings_journalier"
TABLE_NAME = "ParkingsDaily"

# Clients AWS
s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# --- UTILITAIRE ---
def to_decimal(obj):
    """Convertit récursivement les floats en Decimal (pour DynamoDB)."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    return obj


def list_s3_objects_for_today():
    """Liste les fichiers S3 du jour."""
    date_prefix = datetime.utcnow().strftime("%Y-%m-%d")
    prefix = f"{PREFIX}/{date_prefix}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def load_existing_ids():
    """Charge tous les idparking déjà présents dans DynamoDB."""
    existing_ids = set()
    response = table.scan(ProjectionExpression="idparking")
    existing_ids.update(item["idparking"] for item in response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="idparking",
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        existing_ids.update(item["idparking"] for item in response.get("Items", []))
    return existing_ids


def ingest_parking_data_from_s3():
    """Lit les fichiers S3 du jour et insère les nouveaux enregistrements."""
    print("🚀 Démarrage ingestion S3 → DynamoDB (ParkingsDaily)")
    try:
        keys = list_s3_objects_for_today()
        if not keys:
            print("⚠️ Aucun fichier trouvé pour aujourd’hui.")
            return

        existing_ids = load_existing_ids()
        print(f"📊 {len(existing_ids)} enregistrements déjà présents dans DynamoDB.")

        total_inserted = 0
        total_skipped = 0

        for key in keys:
            print(f"📂 Lecture : s3://{BUCKET}/{key}")
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            records = json.loads(obj["Body"].read().decode("utf-8"))

            new_records = [
                r for r in records if r.get("idparking") and r["idparking"] not in existing_ids
            ]

            if not new_records:
                print("   ⏩ Aucun nouveau parking à insérer.")
                total_skipped += len(records)
                continue

            with table.batch_writer() as batch:
                for rec in new_records:
                    batch.put_item(Item=to_decimal(rec))
                    existing_ids.add(rec["idparking"])

            print(f"   ✅ {len(new_records)} nouveaux parkings insérés.")
            total_inserted += len(new_records)

        print(f"\n✅ Ingestion terminée : {total_inserted} insérés, {total_skipped} ignorés.")
    except ClientError as e:
        print(f"❌ Erreur AWS : {e}")
    except Exception as e:
        print(f"❌ Erreur générale : {e}")


if __name__ == "__main__":
    ingest_parking_data_from_s3()
