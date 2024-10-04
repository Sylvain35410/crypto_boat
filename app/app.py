from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from pydantic import EmailStr
from email_validator import validate_email, EmailNotValidError
from scripts.lib_sql import (
    check_database_health, add_user_to_db, 
    delete_user_from_db, get_current_user
)
from app.tools_app import make_prediction_and_decision

security = HTTPBasic()

app = FastAPI()

# Endpoint healthcheck pour vérifier l'état de la base de données
@app.get("/healthcheck")
async def healthcheck():
    if check_database_health():
        return {"status": "healthy", "database": "connected"}
    else:
        raise HTTPException(status_code=503, detail="Database disconnected")

# Endpoint pour ajouter un utilisateur dans la base de données
@app.post("/add_user")
async def add_user(username: str, email: EmailStr, password: str):
    """
    Ajoute un utilisateur dans la base de données. Le mot de passe est haché avant l'enregistrement.

    Requête CURL :
    curl -X 'POST' \
        'http://localhost:8000/add_user?username=admin&email=opa2024dst@gmail.com&password=adminopa2024' \
        -H 'accept: application/json'
    """
    try:
        # Vérification de l'adresse email
        try:
            valid = validate_email(email)
            email = valid.email
        except EmailNotValidError as e:
            raise HTTPException(status_code=400, detail=str(e))

        add_user_to_db(username, email, password)
        return {"message": f"User {username} added successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding user: {str(e)}")

# Endpoint pour supprimer un utilisateur de la base de données
@app.delete("/delete_user")
async def delete_user(username: str, credentials: HTTPBasicCredentials = Depends(security)):
    """
    Supprime un utilisateur de la base de données.

    Requête CURL :
    curl -X 'DELETE' \
        'http://localhost:8000/delete_user?username=admin' \
        -H 'accept: application/json' \
        -u user:password
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

    Requête CURL pour BTCUSDT avec intervalle par défaut :
    curl -X 'GET' \
        'http://localhost:8000/predict_and_decide?symbol=BTCUSDT' \
        -H 'accept: application/json' \
        -u user:password

    Requête CURL pour ETHUSDT avec intervalle 15m :
    curl -X 'GET' \
        'http://localhost:8000/predict_and_decide?symbol=ETHUSDT&interval=15m' \
        -H 'accept: application/json' \
        -u user:password
    """
    current_user = get_current_user(credentials)
    if not current_user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    try:
        prediction_data = make_prediction_and_decision(symbol, interval)
        
        # Réorganiser l'ordre du retour
        return {
            "symbol": symbol,
            "interval": prediction_data['interval'],
            "actual_time": prediction_data['actual_time'],
            "next_time": prediction_data['next_time'],
            "actual_price": prediction_data['actual_price'],
            "predicted_close_price": prediction_data['predicted_close_price'],
            "decision": prediction_data['decision']
        }
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, 
            detail=f"Model for {symbol} with interval {interval} not found. Only 15m models are guaranteed."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction and decision error: {str(e)}")