import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import os
import logging
import sys

class GrafanaDBLoader:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def create_database(self):
        """Create the database and tables if they don't exist"""
        try:
            # Connect to MySQL server
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()

            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}")

            # Create metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS llm_metrics (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    server VARCHAR(50),
                    date DATE,
                    request_rate INT,
                    model_type VARCHAR(50),
                    dataset VARCHAR(50),
                    input_tokens INT,
                    output_tokens INT,
                    output_tokens_retokenized INT,
                    mean_latency FLOAT,
                    median_latency FLOAT,
                    median_ttft FLOAT,
                    median_itl FLOAT,
                    throughput FLOAT,
                    duration FLOAT,
                    completed_requests INT,
                    tokens_per_second FLOAT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_date (date),
                    INDEX idx_server (server),
                    INDEX idx_model (model_type)
                )
            """)
            
            conn.commit()
            self.logger.info("Database and table created successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create database: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def load_data(self, csv_path):
        """Load data from CSV into the database"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Create SQLAlchemy engine
            engine = create_engine(
                f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
            )
            
            # Load data into database
            df.to_sql('llm_metrics', engine, if_exists='append', index=False)
            
            self.logger.info(f"Successfully loaded {len(df)} records into database")
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            raise

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python grafana_db_loader.py <csv_file> [host] [user] [password] [database]")
        sys.exit(1)

    csv_file = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    user = sys.argv[3] if len(sys.argv) > 3 else 'grafana'
    password = sys.argv[4] if len(sys.argv) > 4 else 'your_password'
    database = sys.argv[5] if len(sys.argv) > 5 else 'llm_metrics'

    loader = GrafanaDBLoader(host, user, password, database)
    loader.create_database()
    loader.load_data(csv_file)
