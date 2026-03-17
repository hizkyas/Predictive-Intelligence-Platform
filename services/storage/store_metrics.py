from kafka import KafkaConsumer
import json
import psycopg2
from sklearn.ensemble import IsolationForest
import numpy as np

# Kafka consumer setup
consumer = KafkaConsumer(
    "system-metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="metrics_db",
    user="admin",
    password="admin"
)
cursor = conn.cursor()

print("Storage service started...")

# Buffer to keep recent metrics for model training
recent_metrics = []

# Main loop
for message in consumer:
    data = message.value
    recent_metrics.append([
        data["cpu_percent"],
        data["memory_percent"],
        data["disk_percent"],
        data["network_bytes_sent"],
        data["network_bytes_recv"]
    ])

    # Keep only the last 1000 metrics for training
    if len(recent_metrics) > 1000:
        recent_metrics.pop(0)

    # Convert to numpy array
    X = np.array(recent_metrics)

    # Train Isolation Forest on recent metrics
    model = IsolationForest(contamination=0.05, random_state=42)
    model.fit(X)

    # Predict anomaly for the latest metric
    anomaly_pred = model.predict([X[-1]])[0]  # -1 for latest entry
    data["anomaly_flag"] = True if anomaly_pred == -1 else False

    print("Received:", data)

    # Insert into PostgreSQL
    cursor.execute("""
        INSERT INTO system_metrics (
            timestamp,
            cpu_percent,
            memory_percent,
            disk_percent,
            network_bytes_sent,
            network_bytes_recv,
            anomaly_flag
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        data["timestamp"],
        data["cpu_percent"],
        data["memory_percent"],
        data["disk_percent"],
        data["network_bytes_sent"],
        data["network_bytes_recv"],
        data["anomaly_flag"]
    ))

    conn.commit()
    print("Stored metrics! Anomaly:", data["anomaly_flag"])