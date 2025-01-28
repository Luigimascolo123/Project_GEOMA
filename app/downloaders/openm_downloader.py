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
        jwt_token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InBjaDVnZnBVRE9MYmg4OWtELUNOZSJ9.eyJpc3MiOiJodHRwczovL2xvZ2luLnNtYXQtYXBwLmNvbS8iLCJzdWIiOiJhdXRoMHw2NmU4MjFlODUyYzQ1NDQ5N2M3OTZkMzIiLCJhdWQiOlsiaHR0cDovL2dvb2dsZV9hcGkiLCJodHRwczovL3Byai1iLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3MzYzNTE4ODcsImV4cCI6MTczODk0Mzg4Nywic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBxdWVyeTpwcm9fYXBpIHF1ZXJ5OmdyYXBocWwgYWNjZXNzOmNyYXdsX3JlcXVlc3RzIGFjY2VzczptZWRpYSBhY2Nlc3M6a2liYW5hIGFjY2Vzczpwcml2YXRldWkgb2ZmbGluZV9hY2Nlc3MiLCJhenAiOiJHb292aDhCTjQwV2RYRzMwQmtaZWJIMkhVWjNsZTJlNSIsInBlcm1pc3Npb25zIjpbImFjY2VzczpjcmF3bF9yZXF1ZXN0cyIsImFjY2VzczpraWJhbmEiLCJhY2Nlc3M6bWVkaWEiLCJhY2Nlc3M6cHJpdmF0ZXVpIiwicXVlcnk6Z3JhcGhxbCIsInF1ZXJ5OnByb19hcGkiXX0.pcWXZAO98xp-BH_kUjajrTnfsFZWhqgszovqujhnNOyiebve9PHfWAOdnhUU_9TgrxS5ZFHUXuTJJhWcnM8NqdlASm7atbvsp-OSYSdCB10AIIutSF6vZBlG1KGa406HGGiaMU9lPNQO788IAAbv0nJfW82vt7cHbDXYxVm9kqgzfsVUUCCgNYHMJLfhsEtfSItIJDge7NrtQDTznZVHIaVdLcwpx4IwfWdnJ8JkTdA2NGRp9RFATs77MhyVyFwJ2XBll2t0H2b8UdZ-7tmm_XLkxNzAJginW4USDwUWPjxUtsJ5zKhWo86s36ZqOfXO-lNFXBL3g_oIsBauRI5W-w'
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