import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy Engine Setup
DB_URL = "postgresql://admin:admin@localhost:5432/metrics_db"
engine = create_engine(DB_URL)

def init_db():
    """Ensure the ai_anomaly column exists in forecast_metrics."""
    query = text("""
    ALTER TABLE forecast_metrics 
    ADD COLUMN IF NOT EXISTS ai_anomaly BOOLEAN DEFAULT FALSE;
    """)
    with engine.begin() as conn:
        conn.execute(query)
    logger.info("Anomaly detection schema initialized.")

def detect_anomalies():
    # Load recent system metrics
    # We select timestamp and features
    query = """
        SELECT timestamp, cpu_percent, memory_percent, disk_percent, network_bytes_sent
        FROM system_metrics
        ORDER BY timestamp DESC
        LIMIT 1000
    """
    
    df = pd.read_sql(query, engine)

    if df.empty:
        logger.warning("No data to detect anomalies.")
        return

    # Train Isolation Forest
    # contamination=0.05 means we expect roughly 5% of data to be anomalous
    clf = IsolationForest(contamination=0.05, random_state=42)
    features = ['cpu_percent', 'memory_percent', 'disk_percent', 'network_bytes_sent']
    
    # Fit and predict
    df['ai_anomaly_pred'] = clf.fit_predict(df[features])
    # -1 is anomaly, 1 is normal in IsolationForest
    df['is_anomaly'] = df['ai_anomaly_pred'] == -1

    # Update the forecast_metrics table
    # We use to_timestamp() to fix the "timestamp = numeric" error
    update_query = text("""
        UPDATE forecast_metrics
        SET ai_anomaly = :is_anomaly
        WHERE timestamp = to_timestamp(:ts)
    """)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(update_query, {
                'is_anomaly': bool(row['is_anomaly']),
                'ts': row['timestamp']
            })
            
    logger.info(f"AI detection completed. Total anomalies found: {df['is_anomaly'].sum()}")

if __name__ == "__main__":
    logger.info("Starting AI Anomaly Detector...")
    init_db()
    
    while True:
        try:
            detect_anomalies()
            logger.info("Sleeping for 1 hour...")
            time.sleep(3600)
        except Exception as e:
            logger.error(f"Anomaly Detector error: {e}")
            time.sleep(60)