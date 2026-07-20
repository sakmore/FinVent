import json

from kafka import KafkaConsumer

from database.db import get_connection
from config.settings import (
    KAFKA_BROKER,
    TRANSACTION_TOPIC
)

# Connect to Kafka
consumer = KafkaConsumer(
    TRANSACTION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Connect to PostgreSQL
connection = get_connection()
cursor = connection.cursor()

print("Storage Consumer is running...\n")

# Continuously listen for new transactions
for message in consumer:

    transaction = message.value

    cursor.execute("""
        INSERT INTO transactions (
            step,
            transaction_type,
            amount,
            sender,
            sender_old_balance,
            sender_new_balance,
            receiver,
            receiver_old_balance,
            receiver_new_balance,
            is_fraud,
            is_flagged_fraud
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        transaction["step"],
        transaction["type"],
        transaction["amount"],
        transaction["nameOrig"],
        transaction["oldbalanceOrg"],
        transaction["newbalanceOrig"],
        transaction["nameDest"],
        transaction["oldbalanceDest"],
        transaction["newbalanceDest"],
        transaction["isFraud"],
        transaction["isFlaggedFraud"]
    ))

    connection.commit()

    print(
        f"Stored Transaction | "
        f"Type: {transaction['type']} | "
        f"Amount: ₹{transaction['amount']}"
    )