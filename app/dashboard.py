import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import psycopg2
import pandas as pd
import os
import requests

# Connexion à la base de données PostgreSQL
def get_data_from_db(query):
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@postgres/crypto_db')
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Récupérer les données historiques depuis la base de données pour la paire et l'intervalle choisis
def get_real_data(symbol, interval):
    query = f"""
    SELECT open_time, open_price, high_price, low_price, close_price, volume
    FROM historical_crypto_data
    WHERE id_crypto_characteristics = (SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = '{symbol}')
    AND id_interval = (SELECT id_interval FROM intervals WHERE intervals = '{interval}')
    ORDER BY open_time DESC
    LIMIT 1000  -- Limite les données à 1 mois
    """
    return get_data_from_db(query)

# Récupérer les caractéristiques de la cryptomonnaie
def get_crypto_characteristics(symbol):
    query = f"""
    SELECT name, symbol, market_cap, circulating_supply, max_supply
    FROM crypto_characteristics
    WHERE symbol = '{symbol}'
    """
    return get_data_from_db(query)

# Récupérer le prix actuel depuis la table stream
def get_current_price(symbol):
    query = f"""
    SELECT close_price
    FROM stream_crypto_data
    WHERE id_crypto_characteristics = (SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = '{symbol}')
    ORDER BY event_time DESC
    LIMIT 1
    """
    return get_data_from_db(query).iloc[0]['close_price']

# Récupérer la prédiction depuis l'API FastAPI
def get_predicted_data(symbol, interval):
    API_BASE_URL = 'http://localhost:8000'
    response = requests.get(f'{API_BASE_URL}/predict', params={'symbol': symbol, 'interval': interval})
    return response.json()

app = dash.Dash(__name__)

app.layout = html.Div(children=[
    html.H1("Crypto Prediction Dashboard"),

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
    # Récupérer les données historiques depuis la base de données
    real_data = get_real_data(symbol, interval)
    
    # Récupérer les caractéristiques de la cryptomonnaie
    characteristics = get_crypto_characteristics(symbol).iloc[0]
    
    # Récupérer le prix actuel depuis la table stream
    current_price = get_current_price(symbol)
    
    # Récupérer la prédiction depuis l'API
    predicted_data = get_predicted_data(symbol, interval)

    # Graphique des données historiques
    real_trace = go.Candlestick(
        x=[datetime.fromtimestamp(data['open_time'] / 1000) for index, data in real_data.iterrows()],
        open=real_data['open_price'],
        high=real_data['high_price'],
        low=real_data['low_price'],
        close=real_data['close_price'],
        name='Real Data'
    )

    # Graphique des prédictions
    predicted_trace = go.Scatter(
        x=[predicted_data['timestamp']],
        y=[predicted_data['prediction']],
        mode='lines',
        name='Predicted Data'
    )

    # Histogramme du volume
    volume_trace = go.Bar(
        x=[datetime.fromtimestamp(data['open_time'] / 1000) for index, data in real_data.iterrows()],
        y=real_data['volume'],
        name='Volume',
        marker_color='blue'
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

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)


