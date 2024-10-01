
# CryptoBoat

## Description
CryptoBoat est un projet de prédiction et de surveillance en temps réel du marché des cryptomonnaies, avec un focus sur les paires BTC/USDT et ETH/USDT. Le projet télécharge et analyse les données historiques et en temps réel depuis les plateformes Binance et CoinGecko, entraîne des modèles de prédiction, et fournit des décisions basées sur l'évolution des prix. Le projet utilise Airflow pour orchestrer les tâches, FastAPI pour l'API REST, et Dash pour afficher les données sur un tableau de bord.

## Fonctionnalités
1. **Téléchargement des données historiques** : Les données des paires BTC/USDT et ETH/USDT sont téléchargées depuis Binance pour la période du 01-08-2017 au 01-08-2024, avec un intervalle par défaut de 15 minutes. Ces données sont mises à jour toutes les 15 minutes.
2. **Stream de données en temps réel** : Les données des paires BTC/USDT et ETH/USDT sont récupérées en temps réel toutes les 5 secondes via WebSocket et stockées dans la table `stream_crypto_data`.
3. **Prédiction de la fermeture de la bougie actuelle** : Un modèle de prédiction est entraîné chaque mois à partir des données historiques, et les prédictions sont effectuées en temps réel.
4. **Tableau de bord interactif** : Le tableau de bord Dash affiche les données historiques des 30 derniers jours, les prédictions actuelles, et les caractéristiques des cryptomonnaies.
5. **Gestion des utilisateurs** : Les utilisateurs peuvent être créés, supprimés et authentifiés via l'API FastAPI.
6. **Orchestration avec Airflow** : Toutes les tâches de téléchargement, de mise à jour, d'entraînement de modèles et de prédiction sont orchestrées par Airflow.

## Structure du Projet

```bash
crypto_boat/
|
├── app/                                 # API et tableau de bord
│   ├── app.py                           # API FastAPI
│   ├── dashboard.py                     # Tableau de bord Dash
│   └── utils.py                         # Fonctions utilitaires pour la base de données
│
├── dags/                                # DAGs d'Airflow
│   ├── crypto_data_ingestion_dag.py     # Ingestion des données toutes les 15 minutes
│   ├── train_model_dag.py               # Entraînement des modèles chaque mois
│   └── websocket_stream_dag.py          # Ingestion des données en temps réél
│
├── docker/                              # Fichiers Docker
│   ├── Dockerfile_airflow               # Dockerfile pour Airflow
│   ├── Dockerfile_api                   # Dockerfile pour l'API FastAPI
│   └── Dockerfile_train_model           # Dockerfile pour l'entraînement des modèles
│
├── logs/                                # Répertoire pour stocker les logs d Airflow
│
├── model/                               # Répertoire pour stocker les modèles entraînés (.pkl)
│
├── plugins/                             # Répertoire pour les plugins Airflow
│
├── scripts/                             # Scripts pour la récupération et le stockage des données
│   ├── export_users_to_csv.py           # Exportation des utilisateurs dans un fichier CSV
│   ├── fetch_data.py                    # Téléchargement des données de CoinGecko et Binance
│   ├── lib_sql.py                       # Librairie pour la base de données
│   ├── store_data.py                    # Stockage des données dans PostgreSQL
│   ├── train_model.py                   # Script d'entraînement des modèles
│   └── websocket_stream.py              # Gestion du WebSocket pour les données en temps réel
│
├── sql/                                 # Scripts SQL
│   └── init_db.sql                      # Initialisation de la base de données
|
├── users/                               # Répertoire pour stocker les fichiers CSV des utilisateurs
│
├── docker-compose.yml                   # Fichier de configuration Docker Compose
├── main.py                              # Fichier principal pour orchestrer l'exécution du projet
├── README.md                            # Documentation du projet
├── requirements_airflow.txt             # Dépendances pour Airflow
├── requirements_api.txt                 # Dépendances pour l'API
└── requirements_train_model.txt         # Dépendances pour l'entrainement du modèle
```

## Prérequis

- **Docker** : Version 20.x ou supérieure
- **Docker Compose** : Version 1.29.2 ou supérieure
- **Airflow** : Version 2.8.1
- **Python** : Version 3.8

## Installation

1. Clonez le dépôt du projet :
```bash
git clone https://github.com/your-repo/crypto_boat.git
cd crypto_boat
```

2. Créez et configurez un fichier `.env` pour les clés d'API Binance et CoinGecko :
```bash
echo "BINANCE_API_KEY=your_binance_api_key" > .env
echo "BINANCE_API_SECRET=your_binance_api_secret" >> .env
```

3. Construisez et lancez les conteneurs Docker avec Docker Compose :
```bash
docker-compose up --build
```

4. Initialisez la database
```bash
psql --host 0.0.0.0 --port 5432 --user airflow -f sql/init_db.sql
```

5. Accédez à l'interface Airflow pour vérifier les DAGs : 
   ```
   http://localhost:8080
   ```

6. Le tableau de bord Dash est accessible à l'adresse :
   ```
   http://localhost:8050
   ```

7. L'API FastAPI est accessible à l'adresse :
   ```
   http://localhost:8000
   ```

## Fonctionnement

1. **Ingestion des données** : Les données des paires BTC/USDT et ETH/USDT sont téléchargées via Binance et CoinGecko et stockées dans la base de données PostgreSQL.
2. **Prédiction des prix** : Un modèle de forêt aléatoire est entraîné chaque mois avec les données historiques, et utilisé pour prédire la fermeture de la bougie en cours.
3. **Affichage en temps réel** : Le tableau de bord Dash affiche les données historiques, les prédictions et les informations en temps réel via l'API et les données de la table `stream_crypto_data`.

## Endpoints API

- **/healthcheck** : Vérifie l'état de la base de données.
- **/train** : Lance l'entraînement d'un modèle pour une paire et un intervalle donnés.
- **/predict** : Renvoie la prédiction du prix de fermeture de la bougie actuelle.
- **/add_user** : Ajoute un utilisateur dans le fichier CSV.
- **/import_users** : Importe les utilisateurs depuis un fichier CSV dans la base de données.

## Configuration

Les paramètres de configuration, comme les intervalles de téléchargement des données et les modèles à entraîner, sont ajustables dans les variables Airflow.
        Nom de la variable                  Description                               Valeur
    crypto_data_ingestion_date_end      End date for crypto_data_ingestion DAG      2017-10-06
    crypto_data_ingestion_date_start    Start date for crypto_data_ingestion DAG    2017-09-01
    crypto_data_ingestion_interval      Interval for crypto_data_ingestion DAG          15m
    train_model_date_end                End date for train_model DAG                2017-09-02
    train_model_date_start              Start date for train_model DAG              2017-09-01
    train_model_interval                Interval for train_model DAG                    15m
    websocket_stream_interval           Interval for websocket_stream DAG               15m

## Auteur

- [LAKACHE Khaled](https://github.com/Klakache)
- [PUJOS Sylvain](https://github.com/Sylvain35410)

## Licence

Ce projet est sous licence [MIT](LICENSE).
