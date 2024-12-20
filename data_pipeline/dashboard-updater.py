import json
import glob
import pandas as pd
import requests
from datetime import datetime
import os

class GrafanaDashboardUpdater:
    def __init__(self, api_key, grafana_url):
        self.api_key = api_key
        self.grafana_url = grafana_url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
    def process_jsonl_file(self, filename):
        """Process a single JSONL file and extract metrics."""
        data = []
        with open(filename, 'r') as f:
            for line in f:
                data.append(json.loads(line))
                
        # Calculate metrics from the data
        metrics = {
            'e2e_latency': pd.DataFrame(data)['e2e_latency'].median(),
            'ttft': pd.DataFrame(data)['time_to_first_token'].median(),
            'itl': pd.DataFrame(data)['inter_token_latency'].median(),
            'throughput': len(data) / (data[-1]['timestamp'] - data[0]['timestamp']),
            'duration': data[-1]['timestamp'] - data[0]['timestamp']
        }
        
        return metrics
        
    def collect_metrics(self, data_dir):
        """Collect metrics from all JSONL files."""
        metrics_data = []
        
        # Process all files
        for pattern in ['shortfin_10_*_none.jsonl', 'shortfin_10_*_trie.jsonl', 'sglang_10_*.jsonl']:
            files = glob.glob(os.path.join(data_dir, pattern))
            for file in files:
                # Extract concurrent requests from filename
                concurrent = int(file.split('_')[2])
                # Determine system and cache type
                if 'shortfin' in file:
                    system = 'Shortfin'
                    cache_type = 'Trie' if 'trie' in file else 'Base'
                else:
                    system = 'SGLang'
                    cache_type = 'N/A'
                    
                metrics = self.process_jsonl_file(file)
                metrics_data.append({
                    'system': system,
                    'cache_type': cache_type,
                    'concurrent_requests': concurrent,
                    **metrics,
                    'timestamp': datetime.now().isoformat()
                })
        
        return metrics_data

    def create_dashboard_panels(self, metrics_data):
        """Create Grafana dashboard panels configuration."""
        panels = []
        metrics_map = {
            'e2e_latency': 'Median E2E Latency',
            'ttft': 'Median TTFT',
            'itl': 'Median ITL',
            'throughput': 'Request Throughput',
            'duration': 'Benchmark Duration'
        }
        
        for idx, (metric_key, metric_name) in enumerate(metrics_map.items()):
            panels.append({
                "id": idx + 1,
                "gridPos": {
                    "h": 8,
                    "w": 12,
                    "x": (idx % 2) * 12,
                    "y": (idx // 2) * 8
                },
                "type": "timeseries",
                "title": metric_name,
                "targets": [{
                    "expr": f"${metric_key}",
                    "refId": "A"
                }],
                "fieldConfig": {
                    "defaults": {
                        "color": {
                            "mode": "palette-classic"
                        },
                        "custom": {
                            "axisCenteredZero": False,
                            "axisColorMode": "text",
                            "axisLabel": "",
                            "axisPlacement": "auto",
                            "barAlignment": 0,
                            "drawStyle": "line",
                            "fillOpacity": 10,
                            "gradientMode": "none",
                            "hideFrom": {
                                "legend": False,
                                "tooltip": False,
                                "viz": False
                            },
                            "lineInterpolation": "linear",
                            "lineWidth": 1,
                            "pointSize": 5,
                            "scaleDistribution": {
                                "type": "linear"
                            },
                            "showPoints": "auto",
                            "spanNulls": False,
                            "stacking": {
                                "group": "A",
                                "mode": "none"
                            },
                            "thresholdsStyle": {
                                "mode": "off"
                            }
                        }
                    }
                }
            })
        
        return panels

    def update_dashboard(self, metrics_data):
        """Update the Grafana dashboard with new data."""
        dashboard_config = {
            "dashboard": {
                "id": None,
                "uid": "cluster-metrics",
                "title": "Cluster Details",
                "tags": ["kubernetes", "cluster"],
                "timezone": "browser",
                "panels": self.create_dashboard_panels(metrics_data),
                "refresh": "5m",
                "schemaVersion": 36,
                "version": 1
            },
            "message": f"Dashboard updated at {datetime.now().isoformat()}",
            "overwrite": True
        }
        
        response = requests.post(
            f"{self.grafana_url}/api/dashboards/db",
            headers=self.headers,
            json=dashboard_config
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to update dashboard: {response.text}")
        
        return response.json()

def main():
    # Configuration
    API_KEY = os.getenv('GRAFANA_API_KEY')
    GRAFANA_URL = os.getenv('GRAFANA_URL')
    DATA_DIR = os.getenv('DATA_DIR', '/data/metrics')
    
    if not all([API_KEY, GRAFANA_URL]):
        raise ValueError("Missing required environment variables")
    
    updater = GrafanaDashboardUpdater(API_KEY, GRAFANA_URL)
    
    try:
        # Collect and process metrics
        metrics_data = updater.collect_metrics(DATA_DIR)
        
        # Update dashboard
        result = updater.update_dashboard(metrics_data)
        print(f"Dashboard updated successfully: {result}")
        
    except Exception as e:
        print(f"Error updating dashboard: {str(e)}")
        raise

if __name__ == "__main__":
    main()
