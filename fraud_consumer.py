import json
from collections import defaultdict, deque

from kafka import KafkaConsumer, KafkaProducer

from config.settings import (
    KAFKA_BROKER,
    TRANSACTION_TOPIC,
    FRAUD_TOPIC,

    LARGE_TRANSACTION,
    VERY_LARGE_TRANSACTION,

    LARGE_TRANSACTION_SCORE,
    VERY_LARGE_TRANSACTION_SCORE,
    HIGH_RISK_TYPE_SCORE,
    SENDER_EMPTY_SCORE,
    ACCOUNT_DRAINED_SCORE,
    NEW_RECEIVER_SCORE,
    ROUND_AMOUNT_SCORE,
    FLAGGED_FRAUD_SCORE,
    FREQUENT_TRANSACTION_SCORE,

    HIGH_RISK_TYPES,

    SENDER_BALANCE_THRESHOLD,

    LOW_RISK_THRESHOLD,
    MEDIUM_RISK_THRESHOLD
)

# =====================================================
# Kafka Consumer
# =====================================================

consumer = KafkaConsumer(
    TRANSACTION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# =====================================================
# Kafka Producer
# =====================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚨 Fraud Detection Consumer Running...\n")

# =====================================================
# Behaviour Tracking
# =====================================================

# Keeps track of recent transactions for every sender.
# We'll only remember the latest 20 transactions.

sender_history = defaultdict(lambda: deque(maxlen=20))

# =====================================================
# Risk Calculation Function
# =====================================================

def calculate_risk(transaction):

    risk_score = 0

    reasons = []

    rule_breakdown = {}

    sender = transaction["nameOrig"]

    # -----------------------------------------
    # Rule 1 : Large Transaction
    # -----------------------------------------

    if transaction["amount"] >= LARGE_TRANSACTION:

        risk_score += LARGE_TRANSACTION_SCORE

        reasons.append("Large Transaction")

        rule_breakdown["Large Transaction"] = LARGE_TRANSACTION_SCORE

    # -----------------------------------------
    # Rule 2 : Very Large Transaction
    # -----------------------------------------

    if transaction["amount"] >= VERY_LARGE_TRANSACTION:

        risk_score += VERY_LARGE_TRANSACTION_SCORE

        reasons.append("Very Large Transaction")

        rule_breakdown["Very Large Transaction"] = VERY_LARGE_TRANSACTION_SCORE

    # -----------------------------------------
    # Rule 3 : High Risk Transaction Type
    # -----------------------------------------

    if transaction["type"] in HIGH_RISK_TYPES:

        risk_score += HIGH_RISK_TYPE_SCORE

        reasons.append("High Risk Transaction Type")

        rule_breakdown["High Risk Transaction Type"] = HIGH_RISK_TYPE_SCORE

    # -----------------------------------------
    # Rule 4 : Sender Balance Nearly Empty
    # -----------------------------------------

    if (
        transaction["oldbalanceOrg"] > 0
        and transaction["newbalanceOrig"]
        <= transaction["oldbalanceOrg"] * SENDER_BALANCE_THRESHOLD
    ):

        risk_score += SENDER_EMPTY_SCORE

        reasons.append("Sender Balance Nearly Empty")

        rule_breakdown["Sender Balance Nearly Empty"] = SENDER_EMPTY_SCORE

    # -----------------------------------------
    # Rule 5 : Sender Account Fully Drained
    # -----------------------------------------

    if (
        transaction["oldbalanceOrg"] > 0
        and transaction["newbalanceOrig"] == 0
    ):

        risk_score += ACCOUNT_DRAINED_SCORE

        reasons.append("Sender Account Fully Drained")

        rule_breakdown["Sender Account Fully Drained"] = ACCOUNT_DRAINED_SCORE

    # -----------------------------------------
    # Rule 6 : Receiver Previously Empty
    # -----------------------------------------

    if transaction["oldbalanceDest"] == 0:

        risk_score += NEW_RECEIVER_SCORE

        reasons.append("Receiver Had Zero Balance")

        rule_breakdown["Receiver Had Zero Balance"] = NEW_RECEIVER_SCORE

    # -----------------------------------------
    # Rule 7 : Round Amount
    # -----------------------------------------

    if transaction["amount"] % 100000 == 0:

        risk_score += ROUND_AMOUNT_SCORE

        reasons.append("Round Amount Transaction")

        rule_breakdown["Round Amount Transaction"] = ROUND_AMOUNT_SCORE

    # -----------------------------------------
    # Rule 8 : Dataset Flagged Fraud
    # -----------------------------------------

    if transaction["isFlaggedFraud"] == 1:

        risk_score += FLAGGED_FRAUD_SCORE

        reasons.append("Dataset Flagged Fraud")

        rule_breakdown["Dataset Flagged Fraud"] = FLAGGED_FRAUD_SCORE

    # -----------------------------------------
    # Rule 9 : Behaviour Rule
    # Frequent Transactions
    # -----------------------------------------

    recent_steps = sender_history[sender]

    recent_steps.append(transaction["step"])
    # Trigger only if 5 transactions happened
    # within the last 10 steps
    if (
    len(recent_steps) >= 5
    and (recent_steps[-1] - recent_steps[0]) <= 10):
        
        risk_score += FREQUENT_TRANSACTION_SCORE

        reasons.append("Frequent Transactions In Short Time")

        rule_breakdown["Frequent Transactions In Short Time"] = FREQUENT_TRANSACTION_SCORE


    # -----------------------------------------
    # Risk Classification
    # -----------------------------------------

    if risk_score >= MEDIUM_RISK_THRESHOLD:

        risk_level = "HIGH"

    elif risk_score >= LOW_RISK_THRESHOLD:

        risk_level = "MEDIUM"

    else:

        risk_level = "LOW"

    return (
        risk_score,
        risk_level,
        reasons,
        rule_breakdown
    )
# =====================================================
# Listen Forever
# =====================================================

for message in consumer:

    transaction = message.value

    (
        risk_score,
        risk_level,
        reasons,
        rule_breakdown
    ) = calculate_risk(transaction)

    # ------------------------------------------
    # Console Output
    # ------------------------------------------

    print("\n" + "=" * 70)

    print(
        f"{transaction['type']} | "
        f"₹{transaction['amount']:,.2f}"
    )

    print(f"Risk Score : {risk_score}")

    print(f"Risk Level : {risk_level}")

    if reasons:

        print("Reasons:")

        for reason in reasons:

            print(f"   • {reason}")

    else:

        print("No suspicious behaviour detected.")

    print("=" * 70)

    # ------------------------------------------
    # Publish Medium & High Risk Events
    # ------------------------------------------

    if risk_level in ["MEDIUM", "HIGH"]:

        fraud_event = {

            "step": transaction["step"],

            "transaction_type": transaction["type"],

            "amount": transaction["amount"],

            "sender": transaction["nameOrig"],

            "receiver": transaction["nameDest"],

            "risk_score": risk_score,

            "risk_level": risk_level,

            "reasons": reasons,

            "rule_breakdown": rule_breakdown

        }

        producer.send(
            FRAUD_TOPIC,
            fraud_event
        )

        producer.flush()

        print("\n🚨 ALERT PUBLISHED TO KAFKA")

        print(f"Sender      : {transaction['nameOrig']}")

        print(f"Receiver    : {transaction['nameDest']}")

        print(f"Amount      : ₹{transaction['amount']:,.2f}")

        print(f"Risk Level  : {risk_level}")

        print(f"Risk Score  : {risk_score}")

        print("-" * 70)