import os
from TradingViewData import TradingViewData, Interval

CRYPTO_SYMBOLS = {
    "Bitcoin": "BTCUSD",
    "Ethereum": "ETHUSD",
}

def fetch_crypto_data():
    """
    Fetches historical daily price data for specified cryptocurrencies 
    from TradingView and saves it in CSV format.
    """
    os.makedirs("data/raw", exist_ok=True)

    tv = TradingViewData()

    for name, symbol in CRYPTO_SYMBOLS.items():
        data = tv.get_hist(
            symbol,
            exchange="COINBASE",
            interval=Interval.daily,
            n_bars=4 * 365
        )

        for i in range(0, len(data), 365):
            chunk = data[i:i+365]
            year = chunk.index[0].year

            file_path = f"data/raw/{name}_{year}.csv"
            chunk.to_csv(file_path)

            print(f"Saved {file_path}")