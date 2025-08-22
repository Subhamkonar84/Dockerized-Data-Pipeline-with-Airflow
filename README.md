# 📊 Stock Data ETL with Airflow & PostgreSQL  

This project implements a robust **ETL pipeline** to fetch stock price data from **Yahoo Finance**, transform it into structured records, and load it into a **PostgreSQL database** — all orchestrated via **Apache Airflow**.  

---

## 🚀 Features
- Extracts intraday stock data (default interval: 60m) using [yfinance](https://pypi.org/project/yfinance/).  
- Transforms raw stock data into a validated structured format.  
- Loads data into **Postgres** with automatic **table creation** + **UPSERT (conflict resolution)**.  
- Fully containerized using **Docker Compose**.  
- Robust logging & error handling in Airflow tasks.  
- Easily configurable through environment variables.  

---

## 🏗️ Project Structure

```bash
.
├── dags/
│   └── stock_data_etl.py        # Airflow DAG (ETL definition)
├── scripts/
│   └── fetch_stock_data.py      # StockETL class (extract, transform, load)
├── docker-compose.yml           # Services (Airflow + Postgres stack)
├── requirements.txt             # Python dependencies
├── .env                         # Sensitive config (Postgres + Airflow secrets)
└── README.md                    # Documentation

```

# ⚙️ Setup & Installation

## 1. Clone the repository
```bash
git clone https://github.com/your-username/stock-etl-airflow.git
cd stock-etl-airflow
```
## 2. Create an `.env` file
```text
# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=stocks
DB_PORT=5433

# Airflow
AIRFLOW_FERNET_KEY=your_random_fernet_key
AIRFLOW_SECRET_KEY=your_webserver_secret
STOCK_SYMBOL=IBM
SCHEDULE_INTERVAL=@hourly
```

To generate a **Fernet key**:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
### 3. Launch with Docker
```bash
docker-compose up --build
```

# 🎛️ Services Overview

### **Postgres**
- Runs on `localhost:${DB_PORT}` (default mapped to `5433`).  
- Stores stock prices in table **`stock_prices`**.  

### **Airflow Webserver**
- UI available at: [http://localhost:8080](http://localhost:8080)  
- Default login:
  ```text
  Username: admin
  Password: admin
```

### **Airflow Scheduler**
- Executes DAGs periodically based on **SCHEDULE_INTERVAL**.  

---

# 📑 DAG: `stock_data_etl`

- **extract** → Downloads stock data from Yahoo Finance (via `yfinance`).  
- **transform** → Validates & converts raw data into a clean record.  
- **load** → Inserts/updates record in Postgres (**stock_prices**).  

# 🛠️ Customization

### To track a different stock:
Set environment variable `STOCK_SYMBOL` in the `.env` file.  
Example:
```text
STOCK_SYMBOL=AAPL
```

### To modify scheduling:
Update `SCHEDULE_INTERVAL` (Airflow cron or presets like `@daily`, `@hourly`).  

---

# 🔍 Example Workflow

1. Airflow triggers the DAG (`stock_data_etl`).  
2. **Extract task** fetches the latest 60-min candle for the given stock.  
3. **Transform task** builds a structured record.  
4. **Load task** upserts the record into Postgres.  
5. **Data** ready for analysis in Postgres 🎉  


# ✅ Tech Stack
- **Apache Airflow** (DAG orchestration)  
- **PostgreSQL** (data warehouse)  
- **yfinance** (data extraction)  
- **Docker Compose** (containerization)  

---

# 🧩 Future Improvements
- Add alerting (Slack/Email on DAG failures).  
- Support multiple stock symbols in parallel (batch processing).  
- Store historical OHLCV data instead of just latest records.  
- Integration with BI tools (e.g., Metabase, Superset).  

---

# 👤 Author
Built with ❤️ by **[subham ranjan konar]**.  
