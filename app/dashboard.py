import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import requests
from requests.auth import HTTPBasicAuth

# Fonction pour récupérer les données via l'API
def fetch_data(api_url, endpoint, params=None, credentials=None):
    """
    Récupère les données de l'API en utilisant des requêtes GET.

    Arguments :
    - api_url (str) : L'URL de l'API.
    - endpoint (str) : Le point de terminaison de l'API à appeler.
    - params (dict) : Les paramètres de la requête (facultatif).
    - credentials (HTTPBasicAuth) : Les informations d'authentification (facultatif).

    Retourne :
    - dict : Les données récupérées sous forme de dictionnaire.
    """
    try:
        response = requests.get(f"{api_url}/{endpoint}", params=params, auth=credentials)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {api_url}/{endpoint}: {e}")
        return {}

# Création de l'application Dash
app = dash.Dash(__name__)
app.title = "CBot Crypto Prediction Dashboard"

# Mise en page du tableau de bord
app.layout = html.Div(style={'padding': '20px'}, children=[
    html.H1("CBot Crypto Prediction Dashboard", style={'textAlign': 'center'}),

    html.Div(id='login-page', children=[
        html.Label("Username:"),
        dcc.Input(id='username', type='text', placeholder='Enter username'),
        html.Br(),
        html.Label("Password:"),
        dcc.Input(id='password', type='password', placeholder='Enter password'),
        html.Br(),
        html.Button('Login', id='login-button', n_clicks=0),
        html.Div(id='login-error', style={'color': 'red'})
    ]),

    html.Div(id='dashboard-page', style={'display': 'none'}, children=[
        html.Label("Select Symbol:"),
        dcc.Dropdown(
            id='symbol-dropdown',
            options=[
                {'label': 'BTC/USDT', 'value': 'BTCUSDT'},
                {'label': 'ETH/USDT', 'value': 'ETHUSDT'}
            ],
            value='BTCUSDT'
        ),

        html.Label("Select Interval:"),
        dcc.Dropdown(
            id='interval-dropdown',
            options=[
                {'label': '15m', 'value': '15m'},
                {'label': '1h', 'value': '1h'},
                {'label': '4h', 'value': '4h'},
                {'label': '1d', 'value': '1d'},
                {'label': '1w', 'value': '1w'},  # Ajout de l'intervalle hebdomadaire
                {'label': '1M', 'value': '1M'},  # Ajout de l'intervalle mensuel
            ],
            value='15m'
        ),

        # Intervalle pour rafraîchir toutes les données toutes les 10 secondes
        dcc.Interval(
            id='interval-all-data',
            interval=10 * 1000,  # 10 secondes en millisecondes
            n_intervals=0
        ),

        html.H2("Crypto Characteristics, Current Price, and Predicted Price", style={'textAlign': 'center'}),
        html.Div(id='data-table', style={'display': 'flex', 'flex-direction': 'column', 'alignItems': 'center'}),

        html.H2("Decision", style={'textAlign': 'center'}),
        html.Div(id='decision', style={'font-size': '24px', 'font-weight': 'bold', 'textAlign': 'center'}),

        html.Div(children=[
            dcc.Graph(id='real-data-graph', style={'height': '75vh'}),
            dcc.Graph(id='volume-histogram', style={'height': '25vh'})
        ], style={'display': 'flex', 'flex-direction': 'column'})
    ])
])

# Callback pour gérer la connexion
@app.callback(
    [Output('dashboard-page', 'style'),
     Output('login-page', 'style'),
     Output('login-error', 'children')],
    [Input('login-button', 'n_clicks')],
    [State('username', 'value'), State('password', 'value')]
)
def handle_login(n_clicks, input_username, input_password):
    """
    Gère la connexion de l'utilisateur en vérifiant les informations d'identification.

    Arguments :
    - n_clicks (int) : Le nombre de fois que le bouton de connexion a été cliqué.
    - input_username (str) : Le nom d'utilisateur saisi.
    - input_password (str) : Le mot de passe saisi.

    Retourne :
    - list : Les styles de mise en page pour les pages de connexion et de tableau de bord, ainsi que le message d'erreur.
    """
    if n_clicks > 0:
        credentials = HTTPBasicAuth(input_username, input_password)
        # Appel de l'API pour authentifier l'utilisateur
        api_url = 'http://localhost:8000'
        auth_response = fetch_data(api_url, 'authenticate', credentials=credentials)

        if 'message' in auth_response:
            return [{'display': 'block'}, {'display': 'none'}, '']
        else:
            return [{'display': 'none'}, {'display': 'block'}, 'Invalid username or password.']
    return [{'display': 'none'}, {'display': 'block'}, '']

# Callback pour mettre à jour toutes les données toutes les 10 secondes
@app.callback(
    [Output('real-data-graph', 'figure'),
     Output('volume-histogram', 'figure'),
     Output('data-table', 'children'),
     Output('decision', 'children')],
    [Input('interval-all-data', 'n_intervals')],
    [State('symbol-dropdown', 'value'),
     State('interval-dropdown', 'value'),
     State('username', 'value'),
     State('password', 'value')]
)
def update_dashboard(n_intervals, symbol, interval, username, password):
    """
    Met à jour les graphiques, le tableau de données et la décision à afficher sur le tableau de bord.

    Arguments :
    - n_intervals (int) : Le nombre d'intervalles écoulés pour le rafraîchissement.
    - symbol (str) : Le symbole de la cryptomonnaie sélectionnée.
    - interval (str) : L'intervalle de temps sélectionné.
    - username (str) : Le nom d'utilisateur saisi.
    - password (str) : Le mot de passe saisi.

    Retourne :
    - tuple : Les figures des graphiques, le tableau de données et la décision d'investissement.
    """
    # Authentification
    credentials = HTTPBasicAuth(username, password)
    api_url = 'http://localhost:8000'
    
    # Appels API pour récupérer les données
    historical_data = fetch_data(api_url, 'get_historical_data', {'symbol': symbol, 'interval': interval}, credentials)
    prediction_data = fetch_data(api_url, 'predict_and_decide', {'symbol': symbol, 'interval': interval}, credentials)
    current_price_data = fetch_data(api_url, 'get_current_price', {'symbol': symbol}, credentials)
    characteristics_data = fetch_data(api_url, 'get_crypto_characteristics', {'symbol': symbol}, credentials)

    if not historical_data or not prediction_data or not current_price_data or not characteristics_data:
        return {}, {}, "No data available", ""

    # Préparation des graphiques
    real_trace = go.Candlestick(
        x=[item['open_time'] for item in historical_data],
        open=[item['open_price'] for item in historical_data],
        high=[item['high_price'] for item in historical_data],
        low=[item['low_price'] for item in historical_data],
        close=[item['close_price'] for item in historical_data],
        name='Candlestick Data'
    )

    # Ligne horizontale pour le prix actuel
    current_price_line = go.Scatter(
        x=[item['open_time'] for item in historical_data],
        y=[round(current_price_data['current_price'], 2)] * len(historical_data),
        mode='lines',
        name='Current Price',
        line=dict(color='blue', dash='dash')
    )

    # Ligne horizontale pour le prix prédit (couleur magenta)
    predicted_price_line = go.Scatter(
        x=[item['open_time'] for item in historical_data],
        y=[round(prediction_data['predicted_close_price'], 2)] * len(historical_data),
        mode='lines',
        name='Predicted Price',
        line=dict(color='magenta', dash='dot')
    )

    # Histogramme des volumes
    volume_trace = go.Bar(
        x=[item['open_time'] for item in historical_data],
        y=[item['volume'] for item in historical_data],
        name='Volume',
        marker_color='blue'
    )

    # Configuration des graphiques
    real_fig = {
        'data': [real_trace, current_price_line, predicted_price_line],
        'layout': go.Layout(title=f'Real and Predicted Data for {symbol}', xaxis_title='Time', yaxis_title='Price')
    }

    volume_fig = {
        'data': [volume_trace],
        'layout': go.Layout(title='Volume', xaxis_title='Time', yaxis_title='Volume', barmode='stack')
    }

    # Tableau dynamique pour les caractéristiques et les prix
    characteristics = {
        'Symbol': characteristics_data['symbol'],
        'Market Cap': characteristics_data['market_cap'],
        'Circulating Supply': characteristics_data['circulating_supply'],
        'Max Supply': characteristics_data['max_supply'],
        'Current Price': f"${round(current_price_data['current_price'], 2)}",
        'Predicted Price': f"${round(prediction_data['predicted_close_price'], 2)}"
    }

    # Création du tableau des caractéristiques et prix
    table = html.Table(
        # Ligne d'en-tête
        [html.Tr([html.Th(col) for col in characteristics.keys()])] +
        # Ligne des valeurs
        [html.Tr([html.Td(characteristics[key], style={'border': '1px solid black', 'padding': '10px'}) for key in characteristics])],
        style={
            'width': '80%',
            'margin': 'auto',
            'border-collapse': 'collapse',
            'border': '1px solid black',
            'padding': '10px',
            'text-align': 'center'
        }
    )

    # Décision (Buy/Sell/Hold) avec couleur
    decision = prediction_data['decision']
    decision_color = {'Buy': 'green', 'Sell': 'red', 'Hold': 'orange'}[decision]

    return real_fig, volume_fig, table, html.Span(decision, style={'color': decision_color, 'font-size': '24px'})

# Lancer l'application Dash
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)


