import requests
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# === Configuration ===
API_URL = "https://data.grandlyon.com/fr/datapusher/ws/grandlyon/pvo_patrimoine_voirie.pvoparking/all.json?maxfeatures=100000&start=1&filename=parkings-metropole-lyon"
BUCKET_NAME = "lyon-s3-raw-dev"

# Client S3
s3 = boto3.client("s3")

def fetch_parking_data():
    """Récupère les disponibilités parkings du Grand Lyon."""
    r = requests.get(API_URL)
    r.raise_for_status()
    data = r.json()
    return data.get("values", [])

def process_records(data):
    """Ajoute la date d’ingestion et retourne la liste brute."""
    records = []
    for p in data:
        record = {
            **p,
            "timestamp_capture": datetime.utcnow().isoformat()
        }
        records.append(record)
    return records

def upload_to_s3(records):
    """Stocke le JSON dans S3, dossier par jour."""
    date_prefix = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = "parkings.json"
    s3_key = f"disponibilites_parkings_journalier/{date_prefix}/{file_name}"

    try:
        json_data = json.dumps(records, indent=2, ensure_ascii=False)
        s3.put_object(Body=json_data.encode("utf-8"), Bucket=BUCKET_NAME, Key=s3_key)
        print(f"☁️  {len(records)} parkings envoyés vers s3://{BUCKET_NAME}/{s3_key}")
    except ClientError as e:
        print(f"❌ Erreur upload S3 : {e}")

def main():
    print("🚗 Téléchargement quotidien des disponibilités parkings...")
    try:
        raw_data = fetch_parking_data()
        records = process_records(raw_data)
        upload_to_s3(records)
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    main()
