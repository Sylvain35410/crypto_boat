import psycopg2
import pandas as pd

class CryptoDataHandler:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                dbname="cryptoboat_db",
                user="airflow",
                password="airflow",
                host="0.0.0.0",
                port="5432"
            )
            self.curr = self.conn.cursor()
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            raise

    def terminate_connection(self):
        if self.conn:
            self.curr.close()
            self.conn.close()

    # Fonction pour insérer des données historiques dans la base de données
    def insert_historical_data(self, symbol, interval, records):
        try:
            query = '''
                INSERT INTO historical_crypto_data (
                    id_crypto_characteristics, id_interval, open_time, open_price, high_price, low_price, close_price, 
                    volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, 
                    taker_buy_quote_asset_volume
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            self.curr.executemany(query, records)
            self.conn.commit()
            print(f"Inserted historical data for {symbol} at interval {interval}.")
        except Exception as e:
            print(f"Error inserting historical data: {e}")
            raise

    # Fonction pour récupérer la dernière heure d'ouverture stockée
    def fetch_last_open_timestamp(self):
        try:
            query = "SELECT MAX(open_time) FROM historical_crypto_data"
            self.curr.execute(query)
            data = self.curr.fetchone()
            return data[0] if data else None
        except Exception as e:
            print(f"Error fetching last open timestamp: {e}")
            raise

    # Fonction pour récupérer les données historiques sous forme de DataFrame
    def load_historical_data_as_dataframe(self):
        try:
            query = "SELECT * FROM historical_crypto_data ORDER BY open_time ASC LIMIT 90000"
            df = pd.read_sql(query, self.conn)
            
            # Définir les colonnes sur lesquelles on appliquera des décalages
            feature_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 
                               'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 
                               'taker_buy_quote_asset_volume']
            
            # Ajouter des colonnes de décalage pour les dernières valeurs des indicateurs
            lag_steps = 2
            for col in feature_columns:
                for lag in range(1, lag_steps + 1):
                    df[f'{col}_lag-{lag}'] = df[col].shift(lag)
                    
            df['predicted_close'] = df['close_price'].shift(-1)
            df = df.dropna()
            
            # Diviser les données en caractéristiques et cible
            features = df.drop(['open_time', 'close_time', 'predicted_close'], axis=1)
            target = df['predicted_close']
            
            return features, target
        except Exception as e:
            print(f"Error loading historical data as DataFrame: {e}")
            raise

    # Fonction pour stocker les flux de données en temps réel dans la base de données
    def insert_stream_data(self, stream_record):
        try:
            query = '''
                INSERT INTO stream_crypto_data (
                    id_crypto_characteristics, event_time, first_trade_id, last_trade_id, open_time, open_price,
                    high_price, low_price, close_price, close_time, base_asset_volume, number_of_trades, 
                    is_this_kline_closed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            self.curr.execute(query, stream_record)
            self.conn.commit()
            print("Inserted stream data successfully.")
        except Exception as e:
            print(f"Error inserting stream data: {e}")
            raise

    # Fonction pour compter les lignes de chaque table
    def counter(self):
        try:
            query = "SELECT tablename FROM pg_tables WHERE tablename not like 'pg_%' AND tablename not like 'sql_%'"
            self.curr.execute(query)
            tables = [item[0] for item in self.curr.fetchall()]

            for table in sorted(tables):
                query = f"SELECT count(*) FROM {table}"
                self.curr.execute(query)
                data = self.curr.fetchone()
                print(f"Table : {table:25s} Nombre de ligne : {data[0]}")

        except Exception as e:
            print(f"Error count: {e}")
            raise
