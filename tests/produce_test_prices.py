import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "prices"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

tickers = ["AAPL", "MSFT", "TSLA"]

print(f"Producing test messages to topic '{TOPIC}' on {BOOTSTRAP_SERVERS} ...")

try:
    for i in range(15):
        event = {
            "ticker": tickers[i % len(tickers)],
            "price": round(100 + i * 0.75, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "test_producer",
        }

        producer.send(TOPIC, value=event)
        producer.flush()

        print(f"Sent: {event}")
        time.sleep(1)

    print("Done. Sent 15 messages.")
finally:
    producer.close()
