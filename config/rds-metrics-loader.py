import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
import sys
from urllib.parse import quote_plus

class RDSMetricsLoader:
    def __init__(self, host, user, password, database='llm_metrics'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Create SQLAlchemy engine
        self.engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{quote_plus(self.password)}@{self.host}/{self.database}",
            pool_recycle=3600
        )

    def initialize_database(self):
        """Create database and tables"""
        try:
            # Create database if it doesn't exist
            engine_no_db = create_engine(
                f"mysql+mysqlconnector://{self.user}:{quote_plus(self.password)}@{self.host}"
            )
            with engine_no_db.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.database}"))
                
            # Create metrics table
            create_table_sql = """
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
                INDEX idx_server_model (server, model_type),
                INDEX idx_request_rate (request_rate)
            )
            """
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            
            self.logger.info("Database and table initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {str(e)}")
            raise

    def load_metrics(self, csv_path):
        """Load metrics from CSV into RDS"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            self.logger.info(f"Loading {len(df)} records from {csv_path}")
            
            # Load data into database
            df.to_sql('llm_metrics', self.engine, if_exists='append', index=False)
            
            # Verify the load
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        server,
                        model_type,
                        COUNT(*) as count,
                        AVG(median_latency) as avg_latency,
                        AVG(throughput) as avg_throughput
                    FROM llm_metrics
                    GROUP BY server, model_type
                """))
                
                self.logger.info("\nData load summary:")
                for row in result:
                    print(f"Server: {row.server}, Model: {row.model_type}")
                    print(f"Records: {row.count}, Avg Latency: {row.avg_latency:.2f}, Avg Throughput: {row.avg_throughput:.2f}")
                    
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            raise

def main():
    if len(sys.argv) < 5:
        print("Usage: python rds_loader.py <csv_file> <rds_host> <user> <password>")
        sys.exit(1)

    csv_file = sys.argv[1]
    host = sys.argv[2]
    user = sys.argv[3]
    password = sys.argv[4]

    loader = RDSMetricsLoader(host, user, password)
    loader.initialize_database()
    loader.load_metrics(csv_file)

if __name__ == "__main__":
    main()
