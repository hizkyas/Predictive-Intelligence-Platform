# 💸 Predictive Intelligence & Infrastructure Platform

A production-ready, end-to-end Machine Learning and Data Engineering pipeline for real-time infrastructure monitoring, resource forecasting, and automated anomaly detection. This platform transforms raw system metrics into actionable operational intelligence.

## 🚀 Project Overview

The **Predictive Intelligence Platform** is designed to automate the health management of distributed systems. It follows a modular microservice architecture, separating high-speed data ingestion (Kafka), time-series forecasting (Prophet), and statistical anomaly detection (Isolation Forest).

### Key Features
* **Automated Data Pipeline**: Real-time ETL pipeline that ingests system metrics via Kafka and persists them to a structured PostgreSQL time-series schema.
* **Advanced ML Models**: 
    * **Forecasting**: Utilizes `Facebook Prophet` to predict future resource utilization (CPU/Memory) trends.
    * **Anomaly Detection**: Implements `IsolationForest` to identify multi-variate outliers in system performance.
* **Streamlined Storage**: Optimized database interaction using `SQLAlchemy` for handling high-frequency metric logs.
* **Production-Ready**: Fully containerized with Docker and orchestrated via Docker Compose, supported by a multi-workflow CI/CD pipeline via GitHub Actions.

## 🛠️ Tech Stack

* **Language**: Python 3.11+
* **Machine Learning**: `scikit-learn` (Isolation Forest), `Prophet` (Time-series)
* **Data Engineering**: `Pandas`, `NumPy`, `Apache Kafka`
* **Database**: `PostgreSQL`, `SQLAlchemy`, `psycopg2`
* **DevOps**: `Docker`, `Docker Compose`, `GitHub Actions`
* **Testing & Linting**: `Pytest`, `Ruff`

---

## 📁 Project Structure

```text
predictive-intelligence-platform/
├── services/
│   ├── streaming/          # Kafka consumers & real-time anomaly detection
│   ├── forecasting/        # Prophet model training & future trend prediction
│   └── storage/            # Database schema & metric persistence logic
├── data/                   # SQL scripts and sample metric exports
├── tests/
│   ├── test_anomaly.py     # Unit tests for ML detection logic
│   └── test_pipeline.py    # Infrastructure integration tests
├── .github/workflows/      # CI/CD: Linting, Unit Tests, & Docker Builds
├── Dockerfile              # Containerization setup
├── docker-compose.yml      # Multi-service orchestration
├── requirements.txt        # Project dependencies
└── README.md               # Documentation
🛠️ Installation & Setup
1. Clone the Repository
Bash
git clone [https://github.com/hizkyas/Predictive-Intelligence-Platform.git](https://github.com/hizkyas/Predictive-Intelligence-Platform.git)
cd Predictive-Intelligence-Platform
2. Install Dependencies
Bash
python -m venv .venv
source .venv/bin/activate  # Or .venv\Scripts\activate on Windows
pip install -r requirements.txt
3. Launch Infrastructure (Docker)
Bash
docker-compose up --build -d
📊 Model & Metrics
The platform analyzes core system telemetry:

Resource Usage: CPU Percent, Memory Usage, Disk I/O.

Network Traffic: Bytes sent/received, packet loss.

Predictions: Generates 24-hour look-ahead forecasts for capacity planning.

🐳 Docker Deployment
To build and run the core intelligence service standalone:

Bash
docker build -t predictive-intelligence-app .
docker run --env-file .env predictive-intelligence-app
👨‍💻 Author
Hizkyas Tadele
AI/Data Engineer & CyberSecurity Analyst
LinkedIn https://www.linkedin.com/in/hizkyas-tadele-b689b6249/
