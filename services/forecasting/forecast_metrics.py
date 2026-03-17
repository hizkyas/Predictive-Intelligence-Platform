import pandas as pd
from sqlalchemy import create_engine, text
from prophet import Prophet
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy Engine Setup
DB_URL = "postgresql://admin:admin@localhost:5432/metrics_db"
engine = create_engine(DB_URL)

def init_db():
    """Ensure the table and all necessary columns exist."""
    # 1. Create base table if it doesn't exist
    base_query = text("""
    CREATE TABLE IF NOT EXISTS forecast_metrics (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        lower_bound DOUBLE PRECISION,
        upper_bound DOUBLE PRECISION,
        future_anomaly BOOLEAN DEFAULT FALSE
    );
    """)
    
    with engine.begin() as conn:
        conn.execute(base_query)
        
        # 2. Automatically add missing metric columns if they aren't there
        required_columns = [
            "cpu_predicted", 
            "memory_predicted", 
            "disk_predicted", 
            "network_predicted"
        ]
        
        for col in required_columns:
            try:
                conn.execute(text(f"ALTER TABLE forecast_metrics ADD COLUMN {col} DOUBLE PRECISION;"))
                logger.info(f"Added missing column: {col}")
            except Exception:
                # Column already exists, PostgreSQL will throw an error we can safely ignore
                pass
                
    logger.info("Database schema is up to date.")

def forecast_metric(metric_name, threshold):
    """Forecast a metric using Prophet and insert predictions into the DB."""
    query = f"""
        SELECT 
            to_timestamp(timestamp) AS ds, 
            {metric_name} AS y 
        FROM system_metrics 
        ORDER BY timestamp
    """
    
    df = pd.read_sql(query, engine)

    if df.empty:
        logger.warning(f"No historical data for {metric_name}. Skipping forecast.")
        return

    # Fix Timezone ValueError
    df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)

    # Daily and Weekly seasonality enabled for system metrics
    model = Prophet(daily_seasonality=True, weekly_seasonality=True)
    model.fit(df)

    # Predict next 24 hours (using 'h' for future compatibility)
    future = model.make_future_dataframe(periods=24, freq='h')
    forecast = model.predict(future)

    forecast['future_anomaly'] = forecast['yhat'] > threshold

    # Precise mapping to DB columns
    if 'cpu' in metric_name:
        target_col = "cpu_predicted"
    elif 'memory' in metric_name:
        target_col = "memory_predicted"
    elif 'disk' in metric_name:
        target_col = "disk_predicted"
    elif 'network' in metric_name:
        target_col = "network_predicted"
    else:
        logger.error(f"Unknown mapping for {metric_name}")
        return
    
    insert_query = text(f"""
        INSERT INTO forecast_metrics (
            timestamp, 
            {target_col}, 
            lower_bound, 
            upper_bound, 
            future_anomaly
        ) VALUES (:ds, :yhat, :yhat_lower, :yhat_upper, :future_anomaly)
    """)

    with engine.begin() as conn:
        for _, row in forecast.iterrows():
            conn.execute(insert_query, {
                'ds': row['ds'],
                'yhat': row['yhat'],
                'yhat_lower': row['yhat_lower'],
                'yhat_upper': row['yhat_upper'],
                'future_anomaly': bool(row['future_anomaly'])
            })
    
    logger.info(f"Forecast for {metric_name} successfully saved.")

def forecast_all_metrics():
    metrics_thresholds = {
        "cpu_percent": 85,
        "memory_percent": 90,
        "disk_percent": 90,
        "network_bytes_sent": None
    }

    for metric, threshold in metrics_thresholds.items():
        limit = threshold if threshold is not None else float('inf')
        forecast_metric(metric, limit)

if __name__ == "__main__":
    logger.info("Starting Forecast Service...")
    init_db()
    
    while True:
        try:
            forecast_all_metrics()
            logger.info("Forecasting cycle complete. Waiting 1 hour...")
            time.sleep(3600)
        except Exception as e:
            logger.error(f"Service error: {e}")
            time.sleep(60)