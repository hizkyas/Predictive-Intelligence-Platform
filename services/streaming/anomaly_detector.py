import json
import time
import pandas as pd
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest

TOPIC = "system-metrics"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Consume Kafka messages
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v)
)

# Store last N metrics for anomaly detection
WINDOW_SIZE = 50
metrics_window = []

model = IsolationForest(contamination=0.05)  # 5% anomalies

print("Anomaly detection engine started...")

for message in consumer:
    data = message.value
    metrics_window.append([
        data["cpu_percent"],
        data["memory_percent"],
        data["disk_percent"],
        data["network_bytes_sent"],
        data["network_bytes_recv"]
    ])

    if len(metrics_window) > WINDOW_SIZE:
        metrics_window.pop(0)

    if len(metrics_window) == WINDOW_SIZE:
        df = pd.DataFrame(metrics_window, columns=[
            "cpu_percent",
            "memory_percent",
            "disk_percent",
            "network_bytes_sent",
            "network_bytes_recv"
        ])
        model.fit(df)
        preds = model.predict(df)
        # -1 is anomaly, 1 is normal
        if preds[-1] == -1:
            print(f"⚠️ Anomaly detected at {data['timestamp']}: {data}")
        else:
            print(f"Normal: {data}")
    else:
        print(f"Collecting metrics... {len(metrics_window)}/{WINDOW_SIZE}")