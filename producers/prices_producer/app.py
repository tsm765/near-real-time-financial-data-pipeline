import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path  # ✅ NEW: for clean path handling

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# -------------------------------------------------------------------
# ✅ Resolve project root and load configs/.env from there
# app.py is at:   .../finance-kafka-project/producers/prices_producer/app.py
# parents[0] ->   .../prices_producer
# parents[1] ->   .../producers
# parents[2] ->   .../finance-kafka-project  (project root)
# -------------------------------------------------------------------

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
PRICE_TOPIC = os.getenv("PRICE_TOPIC", "prices")

if not ALPHAVANTAGE_API_KEY:
    raise RuntimeError(f"ALPHAVANTAGE_API_KEY is not set in {ENV_PATH}")

TICKERS = ["AAPL", "MSFT", "TSLA"]  # you can adjust this list

# Alpha Vantage endpoint (we'll use GLOBAL_QUOTE for simplicity)
ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"


def get_latest_price(symbol: str) -> float | None:
    """
    Call Alpha Vantage GLOBAL_QUOTE endpoint and return latest price.
    """
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHAVANTAGE_API_KEY,
    }

    try:
        resp = requests.get(ALPHAVANTAGE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        quote = data.get("Global Quote") or data.get("Global_Quote")
        if not quote:
            print(f"[WARN] No 'Global Quote' field for symbol {symbol}: {data}")
            return None

        price_str = quote.get("05. price") or quote.get("05_price")
        if not price_str:
            print(f"[WARN] No price field for symbol {symbol}: {quote}")
            return None

        return float(price_str)
    except Exception as e:
        print(f"[ERROR] Failed to fetch price for {symbol}: {e}")
        return None


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    symbol = "AAPL"
    print(f"[INFO] One-shot mode: fetching 1 price for {symbol}")

    try:
        price = get_latest_price(symbol)
        if price is None:
            print("[ERROR] Did not get a price (rate limit / network / API response).")
            return

        event = {
            "ticker": symbol,
            "price": round(price, 4),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "alpha_vantage",
        }

        producer.send(PRICE_TOPIC, value=event)
        producer.flush()
        print(f"[SENT] {event}")

    finally:
        producer.close()
        print("[INFO] Producer closed.")


if __name__ == "__main__":
    main()
