# LLM Metrics Automation Pipeline Guide

## Overview
This pipeline automatically collects, processes, and stores performance metrics for comparing Shortfin LLM Server with SGLang Server. The pipeline runs daily and handles:
- Benchmark collection for both servers
- Metrics processing and standardization
- Data storage in RDS
- Automatic dashboard updates in Grafana

## Prerequisites

### Software Requirements
```bash
# Install required Python packages
pip install schedule mysql-connector-python sqlalchemy pandas py7zr

# Clone your benchmark repository (if applicable)
git clone <your-benchmark-repo>
cd <repo-directory>
```

### Required Files
Ensure these files are in your working directory:
- `benchmark-collector.py` - Your benchmark collection script
- `metrics-processor.py` - Metrics processing script
- `metrics_pipeline.py` - Main pipeline script

### Customer Intake (WIP)
- Setup a github issue with the set of actions they need to run at their end

### Database Configuration
The pipeline uses Amazon RDS MySQL. Your database connection details:
```
Host: llm-metrics.c3kwuosg6kjs.us-east-2.rds.amazonaws.com
Database: llm_metrics(for example)
Username: [user-name]
Password: [password]
```

## Pipeline Components

### 1. Benchmark Collection
- Runs benchmarks for multiple configurations:
  - Request rates: 1, 2, 4, 8, 16, 32
  - Servers: sglang, shortfin
  - Model types: none, trie (for shortfin)
- Metrics collected:
  - Median E2E Latency
  - Median TTFT (Time to First Token)
  - Median ITL (Inter-Token Latency)
  - Request Throughput
  - Duration

### 2. Metrics Processing
- Processes raw benchmark data
- Standardizes metrics format
- Creates timestamped CSV files
- Stores processed data in `./processed_data/`

### 3. Database Loading
- Automatically loads processed metrics to RDS
- Maintains historical data
- Enables time-series analysis
- Supports Grafana visualization

## Running the Pipeline

### Manual Execution
```bash
# Run the complete pipeline once
python metrics_pipeline.py

# Run individual components
python metrics_pipeline.py --collect-only  # Only collect benchmarks
python metrics_pipeline.py --process-only  # Only process existing data
python metrics_pipeline.py --load-only     # Only load to database
```

### Automated Scheduling
The pipeline is configured to run daily at midnight using cron:

1. Set up the cron job:
```bash
# Create the runner script
cat << 'EOF' > run_pipeline.sh
#!/bin/bash
cd /home/cloudshell-user
source ~/.bashrc
python metrics_pipeline.py >> pipeline.log 2>&1
EOF

# Make executable
chmod +x run_pipeline.sh

# Add to crontab
crontab -e
# Add: 0 0 * * * /home/cloudshell-user/run_pipeline.sh
```

### Monitoring and Logs
- Pipeline logs: `llm_pipeline.log`
- Cron execution logs: `pipeline.log`
- RDS metrics available in CloudWatch
- Pipeline status viewable in Grafana

## Data Structure

### Database Schema
```sql
CREATE TABLE llm_metrics (
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
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Grafana Integration
- Data source: MySQL
- Default dashboard provided
- Key visualizations:
  - Latency comparison
  - Throughput analysis
  - Request rate impact
  - Token processing efficiency

## Troubleshooting

### Common Issues
1. **Benchmark Collection Fails**
   - Check server accessibility
   - Verify benchmark script parameters
   - Check disk space for outputs

2. **Database Connection Issues**
   - Verify RDS security group settings
   - Check credentials
   - Confirm network connectivity

3. **Missing Data**
   - Check pipeline logs
   - Verify file permissions
   - Ensure sufficient disk space

### Pipeline Status Check
```bash
# Check pipeline status
tail -f llm_pipeline.log

# Check database connectivity
python -c "import mysql.connector; mysql.connector.connect(host='your-rds-endpoint', user='admin', password='your-password', database='llm_metrics')"

# Verify cron job
crontab -l
```

## Maintenance Tasks

### Regular Maintenance
1. Log rotation:
```bash
# Compress old logs
find . -name "*.log" -mtime +7 -exec gzip {} \;
```

2. Data cleanup:
```bash
# Remove processed files older than 7 days
find ./processed_data -mtime +7 -exec rm {} \;
```

### Backup Procedures
1. Database backup:
```bash
# Create RDS snapshot
aws rds create-db-snapshot \
    --db-instance-identifier llm-metrics \
    --db-snapshot-identifier metrics-backup-$(date +%Y%m%d)
```

## TODO: Run a scheduled maintenance every week, automate it
