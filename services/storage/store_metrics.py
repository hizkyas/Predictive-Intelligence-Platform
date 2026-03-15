import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Float, BigInteger, Boolean, Integer, Table, MetaData

TOPIC = "system-metrics"
BOOTSTRAP_SERVERS = ["localhost:9092"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v)
)

# PostgreSQL connection
engine = create_engine("postgresql+psycopg2://admin:admin@localhost:5432/metrics_db")
metadata = MetaData()

metrics_table = Table(
    'system_metrics', metadata,
    Column('id', Integer, primary_key=True),
    Column('timestamp', Float),
    Column('cpu_percent', Float),
    Column('memory_percent', Float),
    Column('disk_percent', Float),
    Column('network_bytes_sent', BigInteger),
    Column('network_bytes_recv', BigInteger),
    Column('anomaly_flag', Boolean, default=False)
)

metadata.create_all(engine)

conn = engine.connect()

print("Storage service started...")

for message in consumer:
    data = message.value
    ins = metrics_table.insert().values(
        timestamp=data["timestamp"],
        cpu_percent=data["cpu_percent"],
        memory_percent=data["memory_percent"],
        disk_percent=data["disk_percent"],
        network_bytes_sent=data["network_bytes_sent"],
        network_bytes_recv=data["network_bytes_recv"]
    )
    conn.execute(ins)
    print(f"Stored metrics at {data['timestamp']}")