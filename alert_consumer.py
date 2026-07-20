import json

from kafka import KafkaConsumer

from database.db import get_connection

from config.settings import (
    KAFKA_BROKER,
    FRAUD_TOPIC
)

# -----------------------------
# Kafka Consumer
# -----------------------------
consumer = KafkaConsumer(
    FRAUD_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# -----------------------------
# PostgreSQL
# -----------------------------
connection = get_connection()
cursor = connection.cursor()

print("Alert Consumer is running...\n")

# -----------------------------
# Listen Forever
# -----------------------------
for message in consumer:

    fraud = message.value

    cursor.execute("""
        INSERT INTO fraud_transactions (
            step,
            transaction_type,
            amount,
            sender,
            receiver,
            risk_score,
            risk_level,
            reasons
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (

        fraud["step"],

        fraud["transaction_type"],

        fraud["amount"],

        fraud["sender"],

        fraud["receiver"],

        fraud["risk_score"],

        fraud["risk_level"],

        ", ".join(fraud["reasons"])

    ))

    connection.commit()

    print(
        f"🚨 Fraud Stored | "
        f"Amount ₹{fraud['amount']} | "
        f"Score {fraud['risk_score']}"
    )