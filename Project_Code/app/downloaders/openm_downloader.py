#openm_downloader.py
import requests
import os
from hdfs import InsecureClient
from datetime import datetime, timedelta
import time
import io
import json

class OpenmeasuresDownloader:
    def __init__(self, hdfs_url="http://namenode:9870", max_retries=5):
        self.max_retries = max_retries
        self.hdfs_client = None
        self.hdfs_url = hdfs_url
        self._init_hdfs_client()
        
    # Inizializzazione HDFS client    
    def _init_hdfs_client(self):
        retries = 0
        while retries < self.max_retries:
            try:
                self.hdfs_client = InsecureClient(self.hdfs_url, user='root')
                self.hdfs_client.status('/')
                print("Successfully connected to HDFS")
                return
            except Exception as e:
                print(f"Attempt {retries + 1}/{self.max_retries} to connect to HDFS failed: {e}")
                retries += 1
                if retries < self.max_retries:
                    time.sleep(10)
        raise Exception("Failed to connect to HDFS after maximum retries")
    
    # Download file da Openmeasures
    def download_openmeasures_file(self, social, start_date, finish_date, search_term="*"):
        jwt_token = '<JWT_TOKEN>'
        pro_api_host_url = 'https://api.openmeasures.io'
        
        json_name = f"{social}_{start_date.strftime('%Y%m%d')}_{finish_date.strftime('%Y%m%d')}.json"
        hdfs_json_path = f"/openmeasures/json/{json_name}"
        
        try:
            self.hdfs_client.makedirs(os.path.dirname(hdfs_json_path), permission=755)
            
            print(f"Downloading data for {social} from {start_date.strftime('%Y-%m-%d')} to {finish_date.strftime('%Y-%m-%d')}")
            response = requests.get(
                f"{pro_api_host_url}/content",
                params={
                    "site": social,
                    "term": search_term,
                    "since": start_date.strftime('%Y-%m-%d'),
                    "until": finish_date.strftime('%Y-%m-%d'),
                    "limit": 10000,
                    "esquery": True
                },
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                total_hits = data.get('hits', {}).get('total', {}).get('value', 0)
                print(f"Found {total_hits} documents for period {start_date.strftime('%Y-%m-%d')} to {finish_date.strftime('%Y-%m-%d')}")
                
                # Scrivi il file JSON in HDFS
                with self.hdfs_client.write(hdfs_json_path, overwrite=True) as hdfs_file:
                    json_str = json.dumps(data)
                    hdfs_file.write(json_str.encode('utf-8'))
                    
                print(f"Successfully uploaded {json_name} to HDFS")
                return hdfs_json_path
            else:
                raise Exception(f"Failed to download data: {response.status_code}")
        except Exception as e:
            print(f"Error downloading/uploading data: {e}")
            raise

    def download_date_range(self, social, start_date, end_date, keywords_dict=None):
        """
        Download data di un periodo di due giorni alla volta 
        """
        paths = []
        current_date = start_date
        
        # Costruisci il search term dalle keywords se disponibili
        if keywords_dict and isinstance(keywords_dict, dict):
            top_keywords = list(keywords_dict.keys())[:3]
            search_term = " OR ".join(f'"{kw}"' for kw in top_keywords) if top_keywords else "*"
        else:
            search_term = "*"
        
        while current_date < end_date:
            try:
                next_period_end = min(current_date + timedelta(days=3), end_date)
                
                print(f"\nProcessing period: {current_date.strftime('%Y-%m-%d')} to {next_period_end.strftime('%Y-%m-%d')}")
                path = self.download_openmeasures_file(social, current_date, next_period_end, search_term=search_term)
                
                if path:
                    paths.append(path)
                    print(f"Successfully downloaded data for period")
                
                current_date = next_period_end
                
            except Exception as e:
                print(f"Error downloading data for period starting {current_date.strftime('%Y-%m-%d')}: {e}")
                current_date = current_date + timedelta(days=2)
        
        print(f"\nDownload summary for {social}:")
        print(f"Total files downloaded: {len(paths)}")
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        return paths
