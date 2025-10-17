import requests
import json
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# === Configuration ===
API_URL = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/pvo_patrimoine_voirie.pvotrafic/items"
BUCKET_NAME = "lyon-s3-raw-dev"

# Client S3 (utilise le rôle IAM de l’EC2 ou aws configure)
s3 = boto3.client("s3")

# === Fonctions principales ===

def fetch_traffic_data(limit=10000, startindex=0):
    """Appelle l’API publique Grand Lyon (GeoServer OGC)."""
    params = {"f": "json", "limit": limit, "startindex": startindex}
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    data = r.json()
    return data.get("features", [])

def clean_speed(v):
    """Convertit '18 km/h' → 18.0, 'Vitesse réglementaire' → 50.0, sinon 0.0."""
    if isinstance(v, str):
        if "km/h" in v:
            return float(v.replace(" km/h", "").strip())
        elif "réglementaire" in v.lower():
            return 50.0
    return float(v) if isinstance(v, (int, float)) else 0.0

def process_records(features):
    """Extrait tous les champs du flux officiel + calculs supplémentaires."""
    records = []
    for f in features:
        p = f["properties"]

        # Nettoyage vitesse
        speed = clean_speed(p.get("vitesse", 0))
        ref_speed = 50.0
        lost_time_min = round(max(0, ref_speed - speed) / ref_speed * 60, 2)
        lost_time_pct = round(max(0, ref_speed - speed) / ref_speed * 100, 1)

        rec = {
            "twgid": p.get("twgid"),
            "code": p.get("code"),
            "libelle": p.get("libelle"),
            "zoom": p.get("zoom"),
            "nom_zoom": p.get("nom_zoom"),
            "sens": p.get("sens"),
            "longueur": p.get("longueur"),
            "fournisseur": p.get("fournisseur"),
            "id_fournisseur": p.get("id_fournisseur"),
            "etat": p.get("etat"),
            "vitesse": p.get("vitesse"),
            "vitesse_clean": speed,
            "ids_ptm": p.get("ids_ptm"),
            "gid": p.get("gid"),
            "last_update": p.get("last_update"),
            "last_update_fme": p.get("last_update_fme"),
            "est_a_jour": p.get("est_a_jour"),
            "timestamp_capture": datetime.utcnow().isoformat(),
            "ref_speed": ref_speed,
            "lost_time_min": lost_time_min,
            "lost_time_pct": lost_time_pct
        }
        records.append(rec)
    return records

def upload_to_s3(records):
    """Envoie directement le JSON en mémoire vers S3."""
    # Structure du dossier : /année-mois-jour/traffic_HHMM.json
    date_prefix = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = f"traffic_{datetime.utcnow().strftime('%H%M')}.json"
    s3_key = f"{date_prefix}/{file_name}"

    try:
        json_data = json.dumps(records, indent=2, ensure_ascii=False)
        s3.put_object(Body=json_data.encode("utf-8"), Bucket=BUCKET_NAME, Key=s3_key)
        print(f"☁️  Données envoyées vers s3://{BUCKET_NAME}/{s3_key} ({len(records)} tronçons)")
    except ClientError as e:
        print(f"❌ Erreur upload S3 : {e}")

def stream_batches(interval=60):
    """Boucle de collecte minute par minute (direct S3)."""
    print(f"🚦 Démarrage du streaming vers S3 ({BUCKET_NAME}) - Ctrl+C pour arrêter")
    while True:
        try:
            features = fetch_traffic_data()
            records = process_records(features)
            if records:
                upload_to_s3(records)
            else:
                print("⚠️ Aucun enregistrement valide.")
        except Exception as e:
            print(f"❌ Erreur pendant le fetch : {e}")
        time.sleep(interval)

if __name__ == "__main__":
    stream_batches(interval=60)
