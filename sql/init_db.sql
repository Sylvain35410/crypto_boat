-- Initialisation de la base de données pour le projet CryptoBoat
-- ----------------------------------

-- Création de la base de données (si nécessaire)
CREATE DATABASE cryptoboat_db;

-- Connexion à la base de données cryptoboat_db
\c cryptoboat_db;

-- Création des tables
-- ----------------------------------

-- Création de la table users
CREATE TABLE IF NOT EXISTS users (
    id_users SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Création de la table crypto_characteristics
CREATE TABLE IF NOT EXISTS crypto_characteristics (
    id_crypto_characteristics SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    market_cap BIGINT,
    circulating_supply FLOAT,
    max_supply FLOAT,
    CONSTRAINT unique_symbol UNIQUE (symbol)
);

-- Création de la table intervals
CREATE TABLE IF NOT EXISTS intervals (
    id_interval SERIAL PRIMARY KEY,
    intervals VARCHAR(4) NOT NULL
);

-- Création de la table historical_crypto_data
CREATE TABLE IF NOT EXISTS historical_crypto_data (
    id_historical_crypto_data SERIAL PRIMARY KEY,
    id_crypto_characteristics INTEGER NOT NULL,
    id_interval INTEGER NOT NULL,
    open_time BIGINT NOT NULL,
    open_price FLOAT NOT NULL,
    high_price FLOAT NOT NULL,
    low_price FLOAT NOT NULL,
    close_price FLOAT NOT NULL,
    volume FLOAT NOT NULL,
    close_time BIGINT NOT NULL,
    quote_asset_volume FLOAT NOT NULL,
    number_of_trades FLOAT NOT NULL,
    taker_buy_base_asset_volume FLOAT NOT NULL,
    taker_buy_quote_asset_volume FLOAT NOT NULL,
    FOREIGN KEY (id_crypto_characteristics) REFERENCES crypto_characteristics (id_crypto_characteristics),
    FOREIGN KEY (id_interval) REFERENCES intervals (id_interval),
    CONSTRAINT unique_historical_crypto_data UNIQUE (id_crypto_characteristics, id_interval, open_time)
);

-- Création de la table stream_crypto_data
CREATE TABLE IF NOT EXISTS stream_crypto_data (
    id_stream_crypto_data SERIAL PRIMARY KEY,
    id_crypto_characteristics INTEGER NOT NULL,
    event_time BIGINT NOT NULL,
    first_trade_id BIGINT NOT NULL,
    last_trade_id BIGINT NOT NULL,
    open_time BIGINT NOT NULL,
    open_price FLOAT NOT NULL,
    high_price FLOAT NOT NULL,
    low_price FLOAT NOT NULL,
    close_price FLOAT NOT NULL,
    close_time BIGINT NOT NULL,
    base_asset_volume FLOAT NOT NULL,
    number_of_trades FLOAT NOT NULL,
    is_this_kline_closed BOOLEAN NOT NULL,
    quote_asset_volume FLOAT NOT NULL,
    taker_buy_base_asset_volume FLOAT NOT NULL,
    taker_buy_quote_asset_volume FLOAT NOT NULL,
    FOREIGN KEY (id_crypto_characteristics) REFERENCES crypto_characteristics (id_crypto_characteristics),
    CONSTRAINT unique_stream_crypto_data UNIQUE (id_crypto_characteristics)
);

-- Insertion des valeurs pour les intervalles
INSERT INTO intervals (intervals)
VALUES
    ('15m'),   -- scalping
    ('1h'),    -- horaire
    ('4h'),    -- horaire
    ('1d'),    -- journalier
    ('1w'),    -- hebdomadaire
    ('1M');    -- mensuel



