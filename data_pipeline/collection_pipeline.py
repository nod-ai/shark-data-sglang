import subprocess
import os
import logging
import schedule
import time
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import sys

class LLMMetricsPipeline:
    def __init__(self, rds_host, rds_user, rds_password, rds_database):
        self.setup_logging()
        self.setup_database_connection(rds_host, rds_user, rds_password, rds_database)
        self.benchmark_dir = "./benchmark_files"
        self.processed_dir = "./processed_data"

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('llm_pipeline.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_database_connection(self, host, user, password, database):
        self.db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.engine = create_engine(
            f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
        )

    def run_benchmark(self):
        """Run the benchmark collection"""
        try:
            self.logger.info("Starting benchmark collection")
            
            # Create directories if they don't exist
            os.makedirs(self.benchmark_dir, exist_ok=True)
            
            # Run benchmark command for each configuration
            for request_rate in [1, 2, 4, 8, 16, 32]:
                # SGLang benchmarks
                cmd = f"python benchmark-collector.py --server sglang --rate {request_rate}"
                subprocess.run(cmd, shell=True, check=True)
                
                # Shortfin benchmarks (none and trie)
                for model_type in ['none', 'trie']:
                    cmd = f"python benchmark-collector.py --server shortfin --rate {request_rate} --model {model_type}"
                    subprocess.run(cmd, shell=True, check=True)
            
            self.logger.info("Benchmark collection completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error running benchmarks: {str(e)}")
            return False

    def process_metrics(self):
        """Process collected metrics"""
        try:
            self.logger.info("Processing metrics")
            
            # Create output directory
            os.makedirs(self.processed_dir, exist_ok=True)
            
            # Process metrics using existing processor
            cmd = f"python metrics-processor.py"
            subprocess.run(cmd, shell=True, check=True)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing metrics: {str(e)}")
            return False

    def load_to_database(self):
        """Load processed metrics to RDS"""
        try:
            self.logger.info("Loading data to RDS")
            
            # Find latest processed CSV file
            csv_files = [f for f in os.listdir(self.processed_dir) if f.endswith('.csv')]
            if not csv_files:
                raise Exception("No processed CSV files found")
                
            latest_csv = max(csv_files, key=lambda x: os.path.getctime(os.path.join(self.processed_dir, x)))
            csv_path = os.path.join(self.processed_dir, latest_csv)
            
            # Load data
            df = pd.read_csv(csv_path)
            df.to_sql('llm_metrics', self.engine, if_exists='append', index=False)
            
            self.logger.info(f"Loaded {len(df)} records to database")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading to database: {str(e)}")
            return False

    def run_pipeline(self):
        """Run the complete pipeline"""
        try:
            self.logger.info("Starting pipeline run")
            
            # Run each step
            if not self.run_benchmark():
                raise Exception("Benchmark collection failed")
                
            if not self.process_metrics():
                raise Exception("Metrics processing failed")
                
            if not self.load_to_database():
                raise Exception("Database loading failed")
                
            self.logger.info("Pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

def run_scheduled_pipeline(pipeline):
    """Wrapper function for scheduled execution"""
    try:
        pipeline.run_pipeline()
    except Exception as e:
        logging.error(f"Scheduled pipeline run failed: {str(e)}")

if __name__ == "__main__":
    # Database configuration
    DB_HOST = "llm-metrics.c3kwuosg6kjs.us-east-2.rds.amazonaws.com"
    DB_USER = "admin"
    DB_PASSWORD = "LLMMetrics1733778864#"
    DB_NAME = "llm_metrics"

    # Initialize pipeline
    pipeline = LLMMetricsPipeline(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)

    # Schedule the pipeline
    schedule.every().day.at("00:00").do(run_scheduled_pipeline, pipeline)

    # Run immediately for first time
    pipeline.run_pipeline()

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)
