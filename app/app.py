from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from utils import check_database_health, load_model, make_prediction, add_user_to_csv, import_users_from_csv, get_stream_data
from apscheduler.schedulers.background import BackgroundScheduler
import psycopg2
import os

app = FastAPI()
security = HTTPBasic()

# Fonction pour lire le fichier users.csv et importer les utilisateurs
def scheduled_import_users():
    try:
        imported_users = import_users_from_csv()
        print(f"{imported_users} nouveaux utilisateurs ont été importés avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'importation des utilisateurs : {e}")

# Planification de la tâche pour exécuter toutes les 2 heures
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_import_users, 'interval', hours=2)
scheduler.start()

# Afficher un message indiquant que les nouveaux utilisateurs seront ajoutés dans 2 heures
@app.get("/next_import")
async def next_import():
    return {"message": "Les nouveaux utilisateurs seront ajoutés dans 2 heures."}

# Fonction d'authentification basique pour sécuriser les endpoints
def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cursor = conn.cursor()
    query = "SELECT username, password_hash FROM users WHERE username = %s"
    cursor.execute(query, (credentials.username,))
    user = cursor.fetchone()
    conn.close()

    if user and user[1] == credentials.password:
        return credentials.username
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Identifiants invalides",
        headers={"WWW-Authenticate": "Basic"},
    )

# Endpoint healthcheck pour vérifier l'état de la base de données
@app.get("/healthcheck")
async def healthcheck():
    if check_database_health():
        return {"status": "healthy", "database": "connected"}
    else:
        raise HTTPException(status_code=503, detail="Base de données déconnectée")

# Endpoint pour faire une prédiction avec le modèle
@app.get("/predict")
async def predict_close_price(symbol: str, interval: str, current_user: str = Depends(get_current_user)):
    """
    Prédiction de la clôture du prix pour la paire de cryptomonnaie donnée.
    """
    try:
        model = load_model(symbol, interval)
        prediction = make_prediction(model)
        return {
            "user": current_user,
            "symbol": symbol,
            "interval": interval,
            "prediction": f"Le prix de clôture prédit est {prediction['prediction']} à {prediction['timestamp']}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la prédiction : {str(e)}")

# Endpoint pour ajouter un utilisateur dans le fichier CSV
@app.post("/add_user")
async def add_user(username: str, email: str, password: str):
    """
    Ajoute un utilisateur dans le fichier CSV. Le mot de passe est haché avant l'enregistrement.
    """
    try:
        add_user_to_csv(username, email, password)
        return {"message": f"L'utilisateur {username} a été ajouté avec succès."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'ajout de l'utilisateur : {str(e)}")

# Endpoint pour importer les utilisateurs depuis un fichier CSV dans la base de données
@app.post("/import_users")
async def import_users():
    """
    Importe les utilisateurs depuis le fichier CSV dans la base de données PostgreSQL.
    """
    try:
        imported_users = import_users_from_csv()
        return {"message": f"{imported_users} utilisateurs importés depuis le fichier CSV dans la base de données."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'importation des utilisateurs : {str(e)}")

# Endpoint pour prendre une décision Buy/Hold/Sell
@app.get("/decision")
async def get_decision(symbol: str, current_user: str = Depends(get_current_user)):
    """
    Prend une décision (Buy, Hold, Sell) basée sur les données en temps réel (WebSocket) par rapport à la dernière bougie.
    """
    try:
        stream_data = get_stream_data(symbol)
        if not stream_data:
            raise HTTPException(status_code=500, detail="Impossible de récupérer les données en temps réel.")

        # Comparaison du prix actuel avec le prix de clôture de la bougie précédente
        current_price = float(stream_data['close_price'])
        previous_price = float(stream_data['previous_close_price'])

        if current_price > previous_price:
            decision = "Buy"
        elif current_price < previous_price:
            decision = "Sell"
        else:
            decision = "Hold"

        return {
            "user": current_user,
            "symbol": symbol,
            "current_price": current_price,
            "previous_price": previous_price,
            "decision": decision
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la prise de décision : {str(e)}")

