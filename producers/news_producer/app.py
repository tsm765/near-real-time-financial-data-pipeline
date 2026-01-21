import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path  # ✅ NEW

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# ---- Load env from project_root/configs/.env ----
# app.py is at:   .../finance-kafka-project/producers/news_producer/app.py
# parents[0] ->   .../news_producer
# parents[1] ->   .../producers
# parents[2] ->   .../finance-kafka-project  (project root)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MENTIONS_TOPIC = os.getenv("MENTIONS_TOPIC", "mentions")

NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY")
if not NEWSAPI_API_KEY:
    raise RuntimeError("NEWSAPI_API_KEY is not set (check configs/.env and docker-compose env_file).")



# Tickers we care about
ATTENTION_TICKERS = [
    t.strip().upper()
    for t in os.getenv("ATTENTION_TICKERS", "AAPL,MSFT,TSLA").split(",")
    if t.strip()
]

NEWSAPI_URL = "https://newsapi.org/v2/everything"


def fetch_articles_for_tickers(tickers):
    """
    Call NewsAPI /v2/everything with a combined query like 'AAPL OR MSFT OR TSLA'.
    Returns a list of article dicts (may be empty).
    """
    query = " OR ".join(tickers)

    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 50,  # max articles per call (free dev tier: delayed by 24h, 100 req/day) :contentReference[oaicite:2]{index=2}
        "apiKey": NEWSAPI_API_KEY,
    }

    resp = requests.get(NEWSAPI_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status")
    if status != "ok":
        print(f"[WARN] NewsAPI returned non-ok status: {data}")
        return []

    return data.get("articles", [])


def extract_tickers_from_article(article, tickers):
    """
    Very simple matching: check if ticker appears in title or description.
    Returns a set of tickers mentioned.
    """
    title = (article.get("title") or "").upper()
    desc = (article.get("description") or "").upper()
    text = f"{title} {desc}"

    mentioned = set()
    for t in tickers:
        if t in text:
            mentioned.add(t)

    return mentioned


def main():
    print("[INFO] Starting NewsAPI mentions producer...")
    print(f"[INFO] Tickers: {ATTENTION_TICKERS}")
    print(f"[INFO] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Mentions topic: {MENTIONS_TOPIC}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        # For now: one-shot fetch to test the pipeline.
        # Later, we can loop this every N minutes, respecting 100 req/day. :contentReference[oaicite:3]{index=3}
        articles = fetch_articles_for_tickers(ATTENTION_TICKERS)
        print(f"[INFO] Retrieved {len(articles)} articles from NewsAPI.")

        count_events = 0

        for article in articles:
            mentioned_tickers = extract_tickers_from_article(article, ATTENTION_TICKERS)
            if not mentioned_tickers:
                continue

            pub_time = article.get("publishedAt") or datetime.now(timezone.utc).isoformat()
            source_name = (article.get("source") or {}).get("name", "unknown")

            for ticker in mentioned_tickers:
                event = {
                    "ticker": ticker,
                    "mentions": 1,
                    "timestamp": pub_time,
                    "source": "newsapi",
                    "news_source": source_name,
                }

                producer.send(MENTIONS_TOPIC, value=event)
                count_events += 1
                print(f"[SENT] {event}")

        producer.flush()
        print(f"[INFO] Done. Sent {count_events} mention events.")

    except Exception as e:
        print(f"[ERROR] NewsAPI producer error: {e}")
    finally:
        producer.close()
        print("[INFO] Producer closed.")


if __name__ == "__main__":
    main()
