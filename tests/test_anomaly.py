import pandas as pd
from sklearn.ensemble import IsolationForest

def test_isolation_forest_logic():
    # Create some dummy data: mostly normal values with one clear outlier
    data = {
        'cpu_percent': [20.0, 21.5, 19.8, 22.1, 95.0], # 95 is the anomaly
        'memory_percent': [40.0, 41.0, 39.5, 42.0, 99.0],
        'disk_percent': [50.0, 51.0, 50.5, 52.0, 50.0],
        'network_bytes_sent': [1000, 1100, 1050, 1200, 1000000]
    }
    df = pd.DataFrame(data)
    
    clf = IsolationForest(contamination=0.2, random_state=42)
    df['anomaly'] = clf.fit_predict(df)
    
    # IsolationForest returns -1 for anomalies
    # The 5th element (index 4) should be detected as an anomaly
    assert df['anomaly'].iloc[4] == -1
    # The 1st element should be normal (1)
    assert df['anomaly'].iloc[0] == 1