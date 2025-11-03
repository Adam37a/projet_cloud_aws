#  Projet Métropole de Lyon – Trafic, Parkings et Perturbations

## Objectif du projet
Ce projet collecte, stocke et analyse les données de **trafic routier**, **parkings** et **perturbations de circulation** issues des API ouvertes de la **Métropole de Lyon**.  
Les données sont sauvegardées sur **S3**, synchronisées avec **DynamoDB**, puis exploitées pour produire des **rapports quotidiens**.

L’objectif final est de permettre à la mairie :
- d’identifier les zones les plus congestionnées,  
- d’optimiser les feux tricolores,  
- de planifier la collecte des déchets,  
- et de mieux anticiper les travaux routiers.

---

## Structure du projet

### dossier `data_retrieval/`  
Contient les scripts qui **récupèrent les données depuis les APIs du Grand Lyon** et les stockent sur **S3**.

#### `supervision_trafic.py`
- Récupère les données de **trafic en temps réel** depuis l’API Grand Lyon.  
- Nettoie les vitesses (`vitesse_clean`) et calcule :
  - le temps perdu (`lost_time_min`),  
  - le pourcentage de ralentissement (`lost_time_pct`).  
- Envoie les données toutes les **minutes** vers :  
  `s3://lyon-s3-raw-dev/supervision_trafic_temps_reel/YYYY-MM-DD/traffic_HHMM.json`

#### `realtime_perturbations.py`
- Récupère les **perturbations de circulation et travaux**.  
- Exemples de données : nom du chantier, importance, commune, type de perturbation.  
- Stocke les données toutes les **5 minutes** dans :  
  `s3://lyon-s3-raw-dev/perturbations_travaux_temps_reel/YYYY-MM-DD/perturbations_HHMM.json`

#### `daily_parkings.py`
- Récupère les **données de parkings disponibles**.  
- Ajoute la date et l’heure de capture (`timestamp_capture`).  
- Envoie un seul fichier par jour :  
  `s3://lyon-s3-raw-dev/disponibilites_parkings_journalier/YYYY-MM-DD/parkings.json`

---

### dossier `ingestion/`  
Contient les scripts qui **transfèrent les données de S3 vers DynamoDB**, en évitant les doublons.

#### `ingest_s3_dynamodb_traffic_realtime.py`
- Charge les fichiers S3 du trafic dans la table **`TrafficRealtime`**.  
- Vérifie la présence du champ `gid` avant insertion pour éviter les doublons.  
- Insère uniquement les nouveaux enregistrements (non existants dans DynamoDB).

#### `ingest_s3_dynamodb_perturbations.py`
- Lit les perturbations stockées sur S3.  
- Insère les données dans la table **`PerturbationsRealtime`**.  
- Utilise `gid` comme identifiant unique pour ignorer les doublons.

#### `ingest_s3_dynamodb_parkings.py`
- Récupère les fichiers journaliers de parkings sur S3.  
- Insère les enregistrements dans la table **`ParkingsDaily`**.  
- Vérifie l’unicité du champ `idparking` avant l’ingestion.

---

### dossier `analytic/`  
Contient les scripts d’analyse et de génération de rapports.

#### `analytics_daily_report.py`
- Récupère les données depuis **TrafficRealtime**, **PerturbationsRealtime** et **ParkingsDaily**.  
- Calcule les indicateurs suivants :
  - `avg_speed_kmh` → vitesse moyenne journalière.  
  - `avg_time_loss_min` → temps perdu moyen par tronçon.  
  - `nb_congestions` → nombre de tronçons congestionnés.  
  - `optimal_hours` → créneaux horaires les plus fluides (vitesse moyenne la plus haute).  
  - `nb_parkings` → total de parkings disponibles.  
  - `nb_perturbations` → total de chantiers actifs.  
- Enregistre le rapport :
  - dans **DynamoDB** (`DailyAnalytics`)  
  - et sur **S3** (`analytics/YYYY-MM-DD/daily_report.json`)

---
