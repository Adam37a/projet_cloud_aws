import boto3
import json
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

# === CONFIGURATION ===
REGION = "eu-west-3"
BUCKET = "lyon-s3-raw-dev"
PREFIX = "perturbations_travaux_temps_reel"
TABLE_NAME = "PerturbationsRealtime"

# Clients AWS
s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# --- UTILITAIRE ---
def to_decimal(obj):
    """Convertit récursivement les floats en Decimal pour DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    return obj


def list_all_dates_in_s3():
    """Retourne toutes les dates (dossiers) présentes dans S3."""
    paginator = s3.get_paginator("list_objects_v2")
    dates = set()
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX + "/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            folder = prefix["Prefix"].split("/")[-2]  # ex: '2025-11-03'
            if folder:
                dates.add(folder)
    return sorted(list(dates))


def list_s3_objects_for_date(date_prefix):
    """Liste tous les fichiers d'une date spécifique."""
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{PREFIX}/{date_prefix}/"
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def load_existing_gids():
    """Charge tous les gid déjà présents dans DynamoDB."""
    existing = set()
    response = table.scan(ProjectionExpression="gid")
    existing.update(int(item["gid"]) for item in response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="gid",
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        existing.update(int(item["gid"]) for item in response.get("Items", []))
    return existing


def ingest_s3_to_dynamo():
    """Compare toutes les dates du S3 et insère uniquement les nouveaux gid."""
    print("🚧 Vérification S3 ↔️ DynamoDB pour PerturbationsRealtime ...")

    try:
        s3_dates = list_all_dates_in_s3()
        if not s3_dates:
            print("⚠️ Aucun dossier daté trouvé dans S3.")
            return

        existing_gids = load_existing_gids()
        print(f"📊 {len(existing_gids)} enregistrements déjà présents en base.")
        print(f"📅 Dates trouvées dans S3 : {s3_dates}")

        total_inserted = 0
        total_skipped = 0

        for date_prefix in s3_dates:
            print(f"\n📦 Traitement du dossier : {date_prefix}")
            keys = list_s3_objects_for_date(date_prefix)
            if not keys:
                print("   ⚠️ Aucun fichier trouvé pour cette date.")
                continue

            for key in keys:
                print(f"   📂 Lecture : s3://{BUCKET}/{key}")
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                records = json.loads(obj["Body"].read().decode("utf-8"))

                new_records = [
                    r for r in records if "gid" in r and int(r["gid"]) not in existing_gids
                ]

                if not new_records:
                    print("      ⏩ Aucun nouveau chantier à insérer.")
                    total_skipped += len(records)
                    continue

                with table.batch_writer() as batch:
                    for rec in new_records:
                        batch.put_item(Item=to_decimal(rec))
                        existing_gids.add(int(rec["gid"]))

                print(f"      ✅ {len(new_records)} nouveaux chantiers insérés.")
                total_inserted += len(new_records)

        print(f"\n🏁 Ingestion terminée : {total_inserted} insérés, {total_skipped} ignorés.")
    except ClientError as e:
        print(f"❌ Erreur AWS : {e}")
    except Exception as e:
        print(f"❌ Erreur : {e}")


if __name__ == "__main__":
    ingest_s3_to_dynamo()
