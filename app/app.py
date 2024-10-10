import pytz
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from pydantic import EmailStr
from email_validator import validate_email, EmailNotValidError
from scripts.lib_sql import (
    check_database_health, add_user_to_db,
    delete_user_from_db, get_current_user, get_id_crypto_characteristics,
    get_id_interval, get_historical_data
)
from tools_app import make_prediction_and_decision, get_crypto_characteristics, get_current_stream_price

security = HTTPBasic()

app = FastAPI()

# Endpoint healthcheck pour vérifier l'état de la base de données
@app.get("/healthcheck")
async def healthcheck():
    """
    Vérifie l'état de la base de données.

    Retourne:
    - Un message indiquant si la base de données est connectée.
    
    Exemple de commande CURL :
    curl -X 'GET' 'http://localhost:8000/healthcheck' -H 'accept: application/json'
    """
    if check_database_health():
        return {"status": "healthy", "database": "connected"}
    else:
        raise HTTPException(status_code=503, detail="Database disconnected")

# Endpoint pour authentifier un utilisateur
@app.get("/authenticate")
async def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    """
    Authentifie un utilisateur et retourne son nom d'utilisateur si les informations sont valides.

    Arguments:
    - credentials : Les informations d'authentification (nom d'utilisateur et mot de passe).

    Retourne:
    - Un message indiquant si l'authentification a réussi ou échoué.

    Exemple de commande CURL :
    curl -X 'POST' 'http://localhost:8000/authenticate' -u user:password -H 'accept: application/json'
    """
    try:
        user = get_current_user(credentials)
        return {"message": f"Authenticated as {user}"}
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid credentials")

# Endpoint pour ajouter un utilisateur dans la base de données
@app.post("/add_user")
async def add_user(username: str, email: EmailStr, password: str):
    """
    Ajoute un utilisateur dans la base de données. Le mot de passe est haché avant l'enregistrement.

    Arguments:
    - username (str): Le nom d'utilisateur à ajouter.
    - email (EmailStr): L'email de l'utilisateur.
    - password (str): Le mot de passe en clair à hacher et stocker.

    Retourne:
    - Un message confirmant l'ajout de l'utilisateur.
    
    Exemple de commande CURL :
    curl -X 'POST' 'http://localhost:8000/add_user?username=admin&email=opa2024dst@gmail.com&password=adminopa2024' -H 'accept: application/json'
    """
    try:
        # Vérification de l'adresse email
        valid = validate_email(email)
        email = valid.email

        add_user_to_db(username, email, password)
        return {"message": f"User {username} added successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding user: {str(e)}")

# Endpoint pour supprimer un utilisateur de la base de données
@app.delete("/delete_user")
async def delete_user(username: str, credentials: HTTPBasicCredentials = Depends(security)):
    """
    Supprime un utilisateur de la base de données.

    Arguments:
    - username (str): Le nom d'utilisateur à supprimer.

    Retourne:
    - Un message confirmant la suppression de l'utilisateur.

    Exemple de commande CURL :
    curl -X 'DELETE' 'http://localhost:8000/delete_user?username=admin' -H 'accept: application/json' -u user:password
    """
    current_user = get_current_user(credentials)
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    try:
        delete_user_from_db(username)
        return {"message": f"User {username} deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting user: {str(e)}")

# Endpoint pour prédiction et décision
@app.get("/predict_and_decide")
async def predict_and_decide(symbol: str, interval: str = "15m", credentials: HTTPBasicCredentials = Depends(security)):
    """
    Prédit le prix de clôture pour la prochaine période et prend une décision (Buy, Sell ou Hold).

    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').
    - interval (str): L'intervalle de temps pour la prédiction (par défaut '15m').

    Retourne:
    - Un dictionnaire contenant les résultats de la prédiction et la décision.

    Exemple de commande CURL :
    curl -X 'GET' 'http://localhost:8000/predict_and_decide?symbol=BTCUSDT' -H 'accept: application/json' -u user:password
    """
    current_user = get_current_user(credentials)
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    try:
        prediction_data = make_prediction_and_decision(symbol, interval)
        return {
            "symbol": symbol,
            "interval": prediction_data['interval'],
            "actual_time": prediction_data['actual_time'],
            "next_time": prediction_data['next_time'],
            "actual_price": prediction_data['actual_price'],
            "predicted_close_price": prediction_data['predicted_close_price'],
            "decision": prediction_data['decision']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction and decision error: {str(e)}")

# Endpoint pour récupérer le prix actuel
@app.get("/get_current_price")
async def get_current_price(symbol: str):
    """
    Récupère le dernier prix de clôture de la cryptomonnaie en temps réel.

    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').

    Retourne:
    - Un dictionnaire contenant le prix actuel.

    Exemple de commande CURL :
    curl -X 'GET' 'http://localhost:8000/get_current_price?symbol=BTCUSDT' -H 'accept: application/json'
    """
    current_price = get_current_stream_price(symbol)
    if current_price is None:
        raise HTTPException(status_code=404, detail="Current price not found.")
    return {"current_price": current_price}

# Endpoint pour récupérer les caractéristiques de la cryptomonnaie
@app.get("/get_crypto_characteristics")
async def get_crypto_characteristics_endpoint(symbol: str):
    """
    Récupère les caractéristiques d'une cryptomonnaie (nom, symbole, market_cap, circulating_supply, max_supply).
    
    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').
    
    Retourne:
    - Un dictionnaire contenant les caractéristiques de la cryptomonnaie.

    Exemple de commande CURL :
    curl -X 'GET' 'http://localhost:8000/get_crypto_characteristics?symbol=BTCUSDT' -H 'accept: application/json'
    """
    characteristics = get_crypto_characteristics(symbol)
    if characteristics is None:
        raise HTTPException(status_code=404, detail="Crypto characteristics not found.")
    return characteristics

# Endpoint pour récupérer les données historiques
@app.get("/get_historical_data")
async def get_historical_data_endpoint(symbol: str, interval: str):
    """
    Récupère les données historiques (open, high, low, close, volume) pour une cryptomonnaie donnée.
    
    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').
    - interval (str): L'intervalle de temps (ex: '15m').

    Retourne:
    - Une liste de données historiques.
    
    Exemple de commande CURL :
    curl -X 'GET' 'http://localhost:8000/get_historical_data?symbol=BTCUSDT&interval=15m' -H 'accept: application/json'
    """
    try:
        id_symbol = get_id_crypto_characteristics(symbol)
        id_interval = get_id_interval(interval)
        
        if id_symbol is None or id_interval is None:
            raise HTTPException(status_code=404, detail="Invalid symbol or interval.")

        end_time = int(datetime.now().replace(tzinfo=pytz.UTC).timestamp() * 1000 )
        start_time = end_time - 1*365*24*60*60*1000
        historical_data = get_historical_data(id_symbol, id_interval, start_time, end_time)

        if historical_data.empty:
            raise HTTPException(status_code=404, detail="No historical data found for this symbol and interval.")
        
        return historical_data.to_dict(orient='records')  # Retourne les données sous forme de liste de dictionnaires
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving historical data: {str(e)}")
