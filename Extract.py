from datetime import datetime, timedelta
import websocket
import json
import threading
import pandas as pd
import csv

# Define the assets to extract
assets = ['BTCUSDT', 'ETHUSDT']
assets = [coin.lower() + '@kline_1m' for coin in assets]
assets_stream = '/'.join(assets)

# Initialize data dictionary
data = {'event_time': [], 'event_date': [], "asset_name": [], 'price': []}

# Load user's portfolio
portfolio_path = 'outfile/my_portfolio.csv'
portfolio_df = pd.read_csv(portfolio_path)
portfolio_df.loc[portfolio_df['status'] == 'sell', "price"] *= -1
if 'initial_value' not in portfolio_df.columns:
    portfolio_df["initial_value"] = portfolio_df["amount"] * portfolio_df["price"]
else:
    portfolio_df["initial_value"] += portfolio_df["amount"] * portfolio_df["price"]

def on_message(ws, message):
    """Handles incoming WebSocket messages."""
    message = json.loads(message)
    manipulate_data(message, portfolio_df)

def manipulate_data(source, portfolio_df):
    """Processes incoming data and updates the portfolio."""
    crypto_name = source['data']['s']
    price = float(source['data']['k']['c'])
    event_time = datetime.fromtimestamp(source['data']['E'] / 1000)

    print(f"{crypto_name} Price: {price} --- at: {event_time}")

    data['price'].append(price)
    data['asset_name'].append(crypto_name)
    data['event_time'].append(event_time.strftime("%H:%M:%S"))
    data['event_date'].append(event_time.strftime('%Y-%m-%d'))

    analyze_portfolio(portfolio_df, price, crypto_name)

def analyze_portfolio(portfolio_df, price, crypto_name):
    """Analyzes and updates the portfolio based on the current price."""
    mask = portfolio_df['asset_name'] == crypto_name
    if not mask.any():
        return

    portfolio_df.loc[mask, 'new_value'] = portfolio_df.loc[mask, 'amount'] * price

def close_connection(ws, data):
    """Closes the WebSocket connection after a specified duration."""
    time_limit = datetime.now() + timedelta(minutes=5)
    while datetime.now() < time_limit:
        pass
    print("Closing WebSocket connection after 5 minutes...\n------------------------------------------------------")
    df = convert_to_dataframe(data)
    print(df)
    print(portfolio_df)
    ws.close()

def convert_to_dataframe(data):
    """Converts the data dictionary into a pandas DataFrame."""
    dataframe = pd.DataFrame(data)
    return dataframe

if __name__ == "__main__":
    socket = f'wss://stream.binance.us:9443/stream?streams={assets_stream}'
    ws = websocket.WebSocketApp(socket, on_message=on_message)

    close_thread = threading.Thread(target=close_connection, args=(ws, data))
    close_thread.start()

    ws.run_forever()
