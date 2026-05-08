import json
import os
import pandas as pd
import boto3
from kafka import KafkaConsumer
from datetime import datetime

# -----------------------
# CONFIGURATION
# -----------------------
S3_CONFIG = {
    "endpoint_url": "http://127.0.0.1:9000",
    "aws_access_key_id": "admin",
    "aws_secret_access_key": "admin123"
}
BUCKET = "features"

# In-memory "Materialized View" for fast feature calculation
loan_store = {}
payment_store = {}

# -----------------------
# MINIO STORAGE (PURE STREAMING)
# -----------------------
def upload_to_minio(df, client_id):
    """
    Uploads a single client's features to MinIO immediately.
    Naming convention: client_id=X/YYYYMMDD_HHMMSS_microseconds.parquet
    """
    if df.empty:
        return

    s3 = boto3.client("s3", **S3_CONFIG)
    
    # High-resolution timestamp to prevent collisions during bulk restores
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    
    # Hive-style partitioning (client_id=X/)
    file_name = f"{timestamp}.parquet"
    s3_path = f"client_id={client_id}/{file_name}"
    
    # Save locally temporarily
    df.to_parquet(file_name, index=False)
    
    # Upload to MinIO
    s3.upload_file(file_name, BUCKET, s3_path)
    
    # Cleanup local file
    os.remove(file_name)
    print(f"✅ Real-time Feature Upload: {BUCKET}/{s3_path}")

# -----------------------
# FEATURE COMPUTATION LOGIC
# -----------------------
def compute_features(client_id):
    # Filter memory for this specific client
    loans = [l for l in loan_store.values() if l["client_id"] == client_id]
    if not loans:
        return pd.DataFrame()

    df_loans = pd.DataFrame(loans)
    loan_ids = set(df_loans["id"].tolist())
    
    # Link payments via loan_id
    payments = [p for p in payment_store.values() if p["loan_id"] in loan_ids]
    df_payments = pd.DataFrame(payments) if payments else pd.DataFrame()

    row = {
        "client_id": client_id,
        "computed_at": datetime.now().isoformat()
    }

    # 1. client.paid_loans.count
    row["paid_loans_count"] = len(df_loans[df_loans["status"] == "paid"])

    # 2. client.days_since_last_late_payment
    row["days_since_last_late_payment"] = None
    if not df_payments.empty and "status" in df_payments.columns:
        late = df_payments[df_payments["status"] == "late"]
        if not late.empty:
            last_date = pd.to_datetime(late["created_on"]).max()
            row["days_since_last_late_payment"] = (pd.Timestamp.now() - last_date).days

    # 3. client.profit_in_last_90_days.rate
    ninety_days_ago = pd.Timestamp.now() - pd.Timedelta(days=90)
    df_loans["created_on"] = pd.to_datetime(df_loans["created_on"])
    recent_loans = df_loans[df_loans["created_on"] >= ninety_days_ago]

    row["profit_in_last_90_days_rate"] = None
    if not recent_loans.empty:
        r_ids = set(recent_loans["id"].tolist())
        interest = df_payments[df_payments["loan_id"].isin(r_ids)]["interest"].sum() if not df_payments.empty else 0
        total_amt = recent_loans["amount"].sum()
        row["profit_in_last_90_days_rate"] = interest / total_amt if total_amt > 0 else None

    return pd.DataFrame([row])

# -----------------------
# EVENT PROCESSING (CDC)
# -----------------------
def process_event(msg):
    data = msg.value
    if not data or "payload" not in data or not data["payload"]:
        return None
    
    payload = data["payload"]
    table = msg.topic.split(".")[-1]
    op = payload["op"] 
    
    # Get record body based on operation type
    body = payload["before"] if op == "d" else payload["after"]
    if not body:
        return None

    if table == "loan":
        if op == "d":
            loan_store.pop(body["id"], None)
        else:
            loan_store[body["id"]] = body
        return body["client_id"]

    elif table == "payment":
        if op == "d":
            payment_store.pop(body["id"], None)
        else:
            payment_store[body["id"]] = body
        
        # Resolve client_id through the loan record
        loan = loan_store.get(body["loan_id"])
        return loan["client_id"] if loan else None
    
    return None

# -----------------------
# MAIN RUNNER
# -----------------------
def run():
    consumer = KafkaConsumer(
        "credit.public.loan",
        "credit.public.payment",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="pure-streaming-v1", 
        enable_auto_commit=True
    )

    print("🚀 Pure Streaming Pipeline Started.")
    print("📥 Monitoring Kafka for CDC events... (Direct I/O)")

    try:
        for msg in consumer:
            client_id = process_event(msg)
            if client_id:
                features_df = compute_features(client_id)
                if not features_df.empty:
                    upload_to_minio(features_df, client_id)
                    
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()