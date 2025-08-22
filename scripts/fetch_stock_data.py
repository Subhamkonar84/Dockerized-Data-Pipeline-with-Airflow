import logging
from datetime import datetime
from typing import Optional
import yfinance as yf
from psycopg2 import DatabaseError, OperationalError, IntegrityError


class StockETL:
    def __init__(self, symbol: str):
        if not symbol or not isinstance(symbol, str):
            logging.critical("❌ Initialization failed: Invalid stock symbol provided -> %s", symbol)
            raise ValueError("Invalid stock symbol provided.")
        self.symbol = symbol
        logging.info("✅ StockETL initialized for symbol: %s", self.symbol)

    # ---------------------- Extract ---------------------- #
    def extract(self) -> dict:
        """
        Fetch latest raw stock data using yfinance.
        Potential failures: API issues, ticker not found, empty dataframe.
        """
        logging.info("🚀 Starting extract task for symbol: %s", self.symbol)
        try:
            ticker = yf.Ticker(self.symbol)
            df = ticker.history(period="1d", interval="60m")

            if df.empty:
                logging.warning("⚠️ No stock data returned for %s", self.symbol)
                raise ValueError(f"No data returned for {self.symbol}")

            latest_row = df.iloc[-1].to_dict()
            latest_time = df.index[-1]

            logging.debug("📊 Extract raw row for %s -> %s", self.symbol, latest_row)
            logging.info("✅ Extracted raw data for %s at %s", self.symbol, latest_time)

            return {
                "timestamp": str(latest_time),
                "raw_open": latest_row.get("Open"),
                "raw_high": latest_row.get("High"),
                "raw_low": latest_row.get("Low"),
                "raw_close": latest_row.get("Close"),
                "raw_volume": latest_row.get("Volume"),
            }

        except Exception as e:
            logging.error("❌ Extraction failed for %s: %s", self.symbol, str(e), exc_info=True)
            raise

    # ---------------------- Transform ---------------------- #
    def transform(self, raw_data: dict) -> dict:
        """
        Transform raw stock data into structured validated record.
        Potential failures: Missing fields, incorrect types, timestamp parsing issues.
        """
        logging.info("🚀 Starting transform task for %s", self.symbol)
        try:
            if not raw_data:
                logging.error("❌ Empty raw_data provided to transform() for symbol: %s", self.symbol)
                raise ValueError("Empty raw_data provided to transform().")

            # --- Validate & normalize timestamp ---
            ts = raw_data.get("timestamp")
            if not ts:
                logging.error("❌ Missing timestamp field in raw_data for %s", self.symbol)
                raise ValueError("Missing timestamp in raw_data.")
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    logging.debug("⚠️ Fallback: parsing timestamp with strptime for %s", self.symbol)
                    ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")

            # --- Validate all required fields before transformation ---
            required_fields = ["raw_open", "raw_high", "raw_low", "raw_close", "raw_volume"]
            for field in required_fields:
                if raw_data.get(field) is None:
                    logging.error("❌ Missing value for %s in raw_data for %s", field, self.symbol)
                    raise ValueError(f"Missing value for {field} in raw_data.")

            # --- Build standardized clean record ---
            record = {
                "symbol": self.symbol,
                "timestamp": ts.isoformat(),
                "open": float(raw_data["raw_open"]),
                "high": float(raw_data["raw_high"]),
                "low": float(raw_data["raw_low"]),
                "close": float(raw_data["raw_close"]),
                "volume": int(raw_data["raw_volume"]),
            }

            logging.debug("📝 Transformed record for %s -> %s", self.symbol, record)
            logging.info("✅ Transformation successful for %s at %s", self.symbol, record["timestamp"])
            return record

        except (KeyError, ValueError, TypeError) as e:
            logging.error("❌ Transformation failed for %s: %s", self.symbol, str(e), exc_info=True)
            raise

    # ---------------------- Load ---------------------- #
    def load(self, record: dict, pg_hook) -> None:
        """
        Load transformed record into Postgres with UPSERT.
        Potential failures: DB connectivity, constraint violations, SQL errors.
        """
        logging.info("🚀 Starting load task for symbol: %s", record.get("symbol"))
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol VARCHAR(10),
            timestamp TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            PRIMARY KEY (symbol, timestamp)
        );
        """
        upsert_sql = """
        INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """

        conn, cur = None, None
        try:
            logging.debug("🔌 Establishing connection to Postgres for %s", record["symbol"])
            conn = pg_hook.get_conn()
            cur = conn.cursor()

            logging.debug("📂 Ensuring stock_prices table exists")
            cur.execute(create_table_sql)

            logging.debug("⬆️ Performing UPSERT for record -> %s", record)
            cur.execute(
                upsert_sql,
                (
                    record["symbol"],
                    record["timestamp"],  
                    float(record["open"]),
                    float(record["high"]),
                    float(record["low"]),
                    float(record["close"]),
                    int(record["volume"]),
                ),
            )

            conn.commit()
            logging.info("✅ Upsert successful for %s at %s", record["symbol"], record["timestamp"])

        except (DatabaseError, OperationalError, IntegrityError) as db_err:
            if conn:
                conn.rollback()
            logging.critical("❌ Database error during load for %s: %s", record["symbol"], str(db_err), exc_info=True)
            raise
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error("❌ Unexpected error in load for %s: %s", record["symbol"], str(e), exc_info=True)
            raise
        finally:
            if cur:
                cur.close()
                logging.debug("🔒 Cursor closed for %s", record.get("symbol"))
            if conn:
                conn.close()
                logging.debug("🔌 Connection closed for %s", record.get("symbol"))