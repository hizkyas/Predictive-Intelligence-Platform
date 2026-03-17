import json
import time
import psutil
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "system-metrics"


def collect_metrics():
    return {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_percent": psutil.disk_usage("/").percent,
        "network_bytes_sent": psutil.net_io_counters().bytes_sent,
        "network_bytes_recv": psutil.net_io_counters().bytes_recv,
        "timestamp": datetime.utcnow().timestamp()
    }


def main():
    while True:
        metrics = collect_metrics()
        producer.send(TOPIC, metrics)

        print(metrics)

        time.sleep(5)


if __name__ == "__main__":
    main()
