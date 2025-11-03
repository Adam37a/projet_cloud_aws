import requests
import json
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# === Configuration ===
API_URL = "https://data.grandlyon.com/fr/datapusher/ws/grandlyon/pvo_patrimoine_voirie.pvochantierperturbant/all.json?maxfeatures=10000&start=1&filename=chantiers-perturbants-metropole-lyon"
BUCKET_NAME = "lyon-s3-raw-dev"

# Client S3
s3 = boto3.client("s3")

def fetch_perturbations():
    """Appelle l’API Grand Lyon - Perturbations trafic / travaux."""
    r = requests.get(API_URL)
    r.raise_for_status()
    data = r.json()
    return data.get("values", [])

def process_records(data):
    """Formate les données pour stockage S3."""
    records = []
    for p in data:
        record = {
            **p,  # garde toutes les colonnes d’origine
            "timestamp_capture": datetime.utcnow().isoformat()
        }
        records.append(record)
    return records

def upload_to_s3(records):
    """Envoie le batch vers S3 dans un dossier date/heure."""
    date_prefix = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = f"perturbations_{datetime.utcnow().strftime('%H%M')}.json"
    s3_key = f"perturbations_travaux_temps_reel/{date_prefix}/{file_name}"

    try:
        json_data = json.dumps(records, indent=2, ensure_ascii=False)
        s3.put_object(Body=json_data.encode("utf-8"), Bucket=BUCKET_NAME, Key=s3_key)
        print(f"☁️  {len(records)} perturbations envoyées vers s3://{BUCKET_NAME}/{s3_key}")
    except ClientError as e:
        print(f"❌ Erreur upload S3 : {e}")

def stream_batches(interval=300):  # 5 minutes = 300 secondes
    """Récupère les perturbations toutes les 5 minutes."""
    print(f"🚧 Démarrage du streaming Perturbations/Travaux → S3 ({BUCKET_NAME})")
    while True:
        try:
            raw_data = fetch_perturbations()
            records = process_records(raw_data)
            if records:
                upload_to_s3(records)
            else:
                print("⚠️ Aucune donnée reçue.")
        except Exception as e:
            print(f"❌ Erreur pendant la récupération : {e}")
        time.sleep(interval)

if __name__ == "__main__":
    stream_batches()
