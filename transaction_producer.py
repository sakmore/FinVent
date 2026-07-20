import json
import time
import pandas as pd
from kafka import KafkaProducer

from config.settings import KAFKA_BROKER, TRANSACTION_TOPIC, CSV_PATH, STREAM_DELAY, STREAM_LIMIT


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv(CSV_PATH)

print(f"Loaded {len(df)} transactions.\n")

for index, row in df.head(STREAM_LIMIT).iterrows():

    transaction = row.to_dict()

    producer.send(TRANSACTION_TOPIC, transaction)

    print(
        f"Sent Transaction {index+1} | "
        f"Type: {transaction['type']} | "
        f"Amount: ₹{transaction['amount']}"
    )

    time.sleep(STREAM_DELAY)

producer.flush()

print("\nFinished streaming transactions.")