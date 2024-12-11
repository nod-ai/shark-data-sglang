import py7zr
import json
import pandas as pd
import glob
import os
from datetime import datetime
import logging

class LLMMetricsProcessor:
    def __init__(self, archive_path, extract_dir, output_dir):
        self.archive_path = archive_path
        self.extract_dir = extract_dir
        self.output_dir = output_dir
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def extract_archive(self):
        try:
            with py7zr.SevenZipFile(self.archive_path, mode='r') as z:
                z.extractall(self.extract_dir)
            self.logger.info(f"Successfully extracted archive to {self.extract_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to extract archive: {str(e)}")
            raise

    def process_jsonl_file(self, filepath):
        records = []
        
        try:
            # Parse filename components
            filename = os.path.basename(filepath)
            parts = filename.replace('.jsonl', '').split('_')
            
            server = parts[0]    # shortfin or sglang
            date = parts[1]      # date component
            rate = parts[2]      # request rate
            model_type = parts[3] if len(parts) > 3 else 'default'  # none/trie for shortfin
            
            with open(filepath, 'r') as f:
                for line in f:
                    data = json.loads(line)
                    record = {
                        # Metadata
                        'server': server,
                        'date': datetime.strptime(date, '%d').strftime('%Y-%m-%d'),
                        'request_rate': int(rate),
                        'model_type': model_type,
                        'dataset': data.get('dataset_name'),
                        
                        # Token metrics
                        'input_tokens': data.get('total_input_tokens'),
                        'output_tokens': data.get('total_output_tokens'),
                        'output_tokens_retokenized': data.get('total_output_tokens_retokenized'),
                        
                        # Latency metrics
                        'mean_latency': data.get('mean_e2e_latency_ms'),
                        'median_latency': data.get('median_e2e_latency_ms'),
                        'median_ttft': data.get('median_ttft_ms'),
                        'median_itl': data.get('median_itl_ms'),
                        
                        # Performance metrics
                        'throughput': data.get('output_throughput'),
                        'duration': data.get('duration'),
                        'completed_requests': data.get('completed')
                    }
                    records.append(record)
                    
            df = pd.DataFrame(records)
            self.logger.info(f"Successfully processed {filepath} - {len(records)} records")
            return df
        except Exception as e:
            self.logger.error(f"Failed to process file {filepath}: {str(e)}")
            return None

    def process_all_files(self):
        all_data = []
        
        # Look for .jsonl files in the benchmark_files subdirectory
        search_path = os.path.join(self.extract_dir, 'benchmark_files', '*.jsonl')
        files = glob.glob(search_path)
        self.logger.info(f"Found {len(files)} .jsonl files")
        
        for filepath in files:
            try:
                df = self.process_jsonl_file(filepath)
                if df is not None and not df.empty:
                    all_data.append(df)
            except Exception as e:
                self.logger.error(f"Error processing {filepath}: {str(e)}")
                continue
                
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            self.logger.info(f"Successfully combined data from {len(all_data)} files")
            
            # Add some useful derived metrics
            combined_df['tokens_per_second'] = combined_df['output_tokens'] / combined_df['duration']
            
            return combined_df
        else:
            raise ValueError("No data was successfully processed")

    def save_data(self, df, format='csv'):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == 'csv':
            output_path = os.path.join(self.output_dir, f'llm_metrics_{timestamp}.csv')
            df.to_csv(output_path, index=False)
        elif format == 'parquet':
            output_path = os.path.join(self.output_dir, f'llm_metrics_{timestamp}.parquet')
            df.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {format}")
            
        self.logger.info(f"Saved processed data to {output_path}")
        
        # Print summary statistics
        summary = df.groupby(['server', 'model_type']).agg({
            'median_latency': 'mean',
            'throughput': 'mean',
            'tokens_per_second': 'mean'
        }).round(2)
        
        self.logger.info("\nPerformance Summary:")
        print(summary)

    def run(self, output_format='csv'):
        try:
            os.makedirs(self.extract_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            
            self.extract_archive()
            df = self.process_all_files()
            if df is not None:
                self.save_data(df, format=output_format)
                self.logger.info("Processing completed successfully")
                return df
            
        except Exception as e:
            self.logger.error(f"Processing pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    processor = LLMMetricsProcessor(
        archive_path="benchmark_files.7z",
        extract_dir="./extracted_files",
        output_dir="./processed_data"
    )
    
    df = processor.run(output_format='csv')
