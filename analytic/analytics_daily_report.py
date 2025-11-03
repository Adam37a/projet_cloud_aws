import boto3
import pandas as pd
from datetime import datetime
from decimal import Decimal
import json

# === CONFIGURATION ===
REGION = "eu-west-3"
dynamodb = boto3.resource("dynamodb", region_name=REGION)

TABLE_TRAFFIC = "TrafficRealtime"
TABLE_PERTURB = "PerturbationsRealtime"
TABLE_PARKING = "ParkingsDaily"
TABLE_ANALYTICS = "AnalyticsDailyReports"

DATE_PREFIX = datetime.utcnow().strftime("%Y-%m-%d")

# --- UTILITAIRES ---
def decimal_to_float(value):
    if isinstance(value, list):
        return [decimal_to_float(v) for v in value]
    elif isinstance(value, dict):
        return {k: decimal_to_float(v) for k, v in value.items()}
    elif isinstance(value, Decimal):
        return float(value)
    return value

def scan_table(table_name):
    table = dynamodb.Table(table_name)
    data = []
    response = table.scan()
    data.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response.get("Items", []))
    return pd.DataFrame([decimal_to_float(x) for x in data])

def to_decimal(obj):
    """Convertit récursivement les floats en Decimal pour DynamoDB"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [to_decimal(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    return obj

# --- 1️⃣ Lecture des tables DynamoDB ---
print("📥 Lecture des tables DynamoDB ...")

traffic_df = scan_table(TABLE_TRAFFIC)
perturb_df = scan_table(TABLE_PERTURB)
parking_df = scan_table(TABLE_PARKING)

print(f"✅ {len(traffic_df)} lignes trafic, {len(perturb_df)} perturbations, {len(parking_df)} parkings")

# --- 2️⃣ Nettoyage des données ---
if not traffic_df.empty:
    traffic_df["vitesse_clean"] = pd.to_numeric(traffic_df["vitesse_clean"], errors="coerce")
    traffic_df["lost_time_min"] = pd.to_numeric(traffic_df["lost_time_min"], errors="coerce")
    traffic_df["timestamp_capture"] = pd.to_datetime(traffic_df["timestamp_capture"], errors="coerce")
    traffic_df["hour"] = traffic_df["timestamp_capture"].dt.hour

# --- 3️⃣ Calculs analytiques ---
if not traffic_df.empty:
    agg_hourly = (
        traffic_df.groupby("hour", as_index=False)
        .agg({"vitesse_clean": "mean", "lost_time_min": "mean"})
        .rename(columns={"vitesse_clean": "vitesse_moyenne_kmh", "lost_time_min": "temps_perdu_min"})
    )
    avg_speed = round(agg_hourly["vitesse_moyenne_kmh"].mean(), 2)
    avg_loss = round(agg_hourly["temps_perdu_min"].mean(), 2)
else:
    avg_speed = 0
    avg_loss = 0
    agg_hourly = pd.DataFrame()

if not traffic_df.empty:
    slow_segments = (
        traffic_df.groupby("libelle", as_index=False)
        .agg({"vitesse_clean": "mean", "lost_time_min": "mean"})
        .sort_values("vitesse_clean", ascending=True)
        .head(10)
    )
    congested_segments = slow_segments.to_dict(orient="records")
else:
    congested_segments = []

if not traffic_df.empty:
    best_hours = (
        agg_hourly.sort_values("vitesse_moyenne_kmh", ascending=False)
        .head(3)["hour"].tolist()
    )
else:
    best_hours = []

if not traffic_df.empty:
    threshold = traffic_df["vitesse_clean"].mean() * 0.5
    nb_alertes = len(traffic_df[traffic_df["vitesse_clean"] < threshold])
else:
    nb_alertes = 0

nb_perturbations = len(perturb_df)
nb_parkings = len(parking_df)

# --- 4️⃣ Création du rapport ---
report = {
    "date": DATE_PREFIX,
    "vitesse_moyenne_kmh": avg_speed,
    "temps_perdu_moyen_min": avg_loss,
    "nombre_congestions": nb_alertes,
    "nombre_perturbations": nb_perturbations,
    "nombre_parkings": nb_parkings,
    "creneaux_trafic_fluide": best_hours,
    "top_troncons_congestionnes": congested_segments,
}

# --- 5️⃣ Sauvegarde dans DynamoDB ---
table_analytics = dynamodb.Table(TABLE_ANALYTICS)
table_analytics.put_item(Item=to_decimal(report))

print(f"📊 Rapport du {DATE_PREFIX} inséré dans la table {TABLE_ANALYTICS}")
