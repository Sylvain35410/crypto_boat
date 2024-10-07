import dash
from datetime import datetime
from dash import dcc, html
from dash.dependencies import Input, Output
import os
import pandas as pd
import plotly.graph_objs as go
import psycopg2
import pytz
import requests
from requests.auth import HTTPBasicAuth
from tools_app import get_current_stream_price


# Fonction pour se connecter à la base de données PostgreSQL et exécuter une requête
def get_data_from_db(query):
    """
    Fonction pour se connecter à la base de données PostgreSQL, exécuter une requête SQL et retourner les résultats sous forme de DataFrame Pandas.
    
    Arguments:
    - query (str): La requête SQL à exécuter.
    
    Retour:
    - pd.DataFrame: Les résultats de la requête sous forme de DataFrame Pandas.
    """
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@postgres/crypto_db')
    df = pd.read_sql(query, DATABASE_URL)
    return df


# Fonction pour récupérer les données historiques sur 30 jours pour une paire et un intervalle choisis
def get_real_data(symbol, interval):
    """
    Récupère les données historiques (open, high, low, close, volume) pour les 30 derniers jours pour un symbole et un intervalle donnés.
    
    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').
    - interval (str): L'intervalle de temps (ex: '15m').
    
    Retour:
    - pd.DataFrame: Les données historiques sous forme de DataFrame.
    """

    # Récupère les données des 30 derniers jours : 30*24*60*60*1000 = 2592000000
    now_time = int(datetime.now().replace(tzinfo=pytz.UTC).timestamp() * 1000) - 2592000000 

    query = f"""
    SELECT open_time, open_price, high_price, low_price, close_price, volume
    FROM historical_crypto_data
    WHERE id_crypto_characteristics = (SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = '{symbol}')
    AND id_interval = (SELECT id_interval FROM intervals WHERE intervals = '{interval}')
    AND open_time >= {now_time}
    ORDER BY open_time DESC
    """
    return get_data_from_db(query)


# Fonction pour récupérer les caractéristiques de la cryptomonnaie
def get_crypto_characteristics(symbol):
    """
    Récupère les caractéristiques d'une cryptomonnaie (nom, symbol, market_cap, circulating_supply, max_supply).
    
    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').
    
    Retour:
    - pd.DataFrame: Les caractéristiques de la cryptomonnaie sous forme de DataFrame.
    """
    query = f"""
    SELECT name, symbol, market_cap, circulating_supply, max_supply
    FROM crypto_characteristics
    WHERE symbol = '{symbol}'
    """
    return get_data_from_db(query)


# Fonction pour récupérer les données de prédiction via l'API FastAPI
def get_predicted_data(symbol):
    """
    Récupère les données de prédiction via l'API FastAPI.
    
    Arguments:
    - symbol (str): Le symbole de la cryptomonnaie (ex: 'BTCUSDT').
    
    Retour:
    - dict: Les résultats de la prédiction sous forme de dictionnaire.
    """
    API_BASE_URL = 'http://api:8000'
    response = requests.get(f'{API_BASE_URL}/predict_and_decide', params={'symbol': symbol}, auth = HTTPBasicAuth('admin', 'adminopa2024'))
    return response.json()


# Fonction pour construire et démarrer le tableau de bord Dash
def build_dashboard():
    """
    Construit et configure le tableau de bord Dash avec les graphiques pour les données historiques, la prédiction et l'histogramme de volume.
    
    Retour:
    - app (dash.Dash): L'application Dash prête à être exécutée.
    """
    app = dash.Dash(__name__)

    app.layout = html.Div(children=[
        html.H1("CBot Crypto Prediction Dashboard"),

        html.Div([
            html.Label("Select Symbol:"),
            dcc.Dropdown(
                id='symbol-dropdown',
                options=[
                    {'label': 'BTC/USDT', 'value': 'BTCUSDT'},
                    {'label': 'ETH/USDT', 'value': 'ETHUSDT'},
                ],
                value='BTCUSDT'  # Valeur par défaut
            ),
            html.Label("Select Interval:"),
            dcc.Dropdown(
                id='interval-dropdown',
                options=[
                    {'label': '15m', 'value': '15m'},
                    {'label': '1h', 'value': '1h'},
                    {'label': '4h', 'value': '4h'},
                    {'label': '1d', 'value': '1d'},
                    {'label': '1w', 'value': '1w'},
                    {'label': '1M', 'value': '1M'},
                ],
                value='15m'  # Valeur par défaut
            ),
        ]),

        html.H2("Crypto Characteristics"),
        html.Div(id='crypto-characteristics'),

        html.H2("Current Price"),
        html.Div(id='current-price'),

        dcc.Graph(id='real-data-graph'),
        dcc.Graph(id='predicted-data-graph'),
        dcc.Graph(id='volume-histogram')
    ])

    @app.callback(
        [Output('real-data-graph', 'figure'),
        Output('predicted-data-graph', 'figure'),
        Output('volume-histogram', 'figure'),
        Output('crypto-characteristics', 'children'),
        Output('current-price', 'children')],
        [Input('symbol-dropdown', 'value'),
        Input('interval-dropdown', 'value')]
    )
    def update_dashboard(symbol, interval):
        """
        Met à jour les graphiques du tableau de bord lorsque l'utilisateur change le symbole ou l'intervalle.

        Arguments:
        - symbol (str): Le symbole de la cryptomonnaie sélectionnée (ex: 'BTCUSDT').
        - interval (str): L'intervalle sélectionné (ex: '15m').
        
        Retour:
        - tuple: Les objets pour les graphiques et les informations à afficher dans le tableau de bord.
        """
        # Récupérer les données historiques depuis la base de données
        real_data = get_real_data(symbol, interval)
        
        # Récupérer les caractéristiques de la cryptomonnaie
        characteristics = get_crypto_characteristics(symbol).iloc[0]
        
        # Récupérer le prix actuel depuis la table stream
        current_price = get_current_stream_price(symbol)
        
        # Récupérer la prédiction depuis l'API
        predicted_data = get_predicted_data(symbol)

        # Graphique des données historiques
        real_trace = go.Candlestick(
            x=[row['open_time'] for index, row in real_data.iterrows()],
            open=real_data['open_price'],
            high=real_data['high_price'],
            low=real_data['low_price'],
            close=real_data['close_price']
        )

        # Graphique des prédictions
        predicted_trace = go.Scatter(
            x=[predicted_data['next_time']],
            y=[predicted_data['predicted_close_price']],
            mode='lines',
            name='Predicted Data'
        )

        # Histogramme du volume pour les 30 derniers jours
        volume_trace = go.Bar(
            x=[row['open_time'] for index, row in real_data.iterrows()],
            y=real_data['volume'],
            name='Volume'
        )

        # Configuration des graphiques
        real_fig = {
            'data': [real_trace],
            'layout': go.Layout(title=f'Real Data for {symbol}', xaxis_title='Time', yaxis_title='Price')
        }

        predicted_fig = {
            'data': [predicted_trace],
            'layout': go.Layout(title=f'Predicted Data for {symbol}', xaxis_title='Time', yaxis_title='Predicted Price')
        }

        volume_fig = {
            'data': [volume_trace],
            'layout': go.Layout(title='Volume', xaxis_title='Time', yaxis_title='Volume')
        }

        # Affichage des caractéristiques
        characteristics_text = f"""
        Name: {characteristics['name']}\n
        Symbol: {characteristics['symbol']}\n
        Market Cap: ${characteristics['market_cap']}\n
        Circulating Supply: {characteristics['circulating_supply']}\n
        Max Supply: {characteristics['max_supply']}
        """

        return real_fig, predicted_fig, volume_fig, characteristics_text, f"Current Price: ${current_price}"

    return app


if __name__ == '__main__':
    """
    Point d'entrée du fichier, démarre le serveur Dash si le script est exécuté directement.
    """
    app = build_dashboard()
    app.run_server(debug=True, host='0.0.0.0', port=8050)
