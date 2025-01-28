#gdelt_downloader.py
import requests
import os
from hdfs import InsecureClient
from datetime import datetime, timedelta
import time
import zipfile
import io

class GDELTDownloader:
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
    
    # Download file da GDELT
    def download_gdelt_file(self, date):
        base_url = "http://data.gdeltproject.org/events/"
        zip_name = f"{date.strftime('%Y%m%d')}.export.CSV.zip"
        csv_name = f"{date.strftime('%Y%m%d')}.export.CSV"
        url = base_url + zip_name
        hdfs_csv_path = f"/gdelt/csv/{csv_name}"
        
        try:
            # Creazione cartella HDFS
            self.hdfs_client.makedirs(os.path.dirname(hdfs_csv_path), permission=755)
            
            # Download ZIP file
            print(f"Downloading {url}")
            response = requests.get(url)
            
            if response.status_code == 200:
                # Decompress ZIP e scrittura del file csv in HDFS
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                    csv_filename = zip_ref.namelist()[0]
                    with zip_ref.open(csv_filename) as csv_file:
                        csv_content = csv_file.read()
                        
                        with self.hdfs_client.write(hdfs_csv_path, overwrite=True) as hdfs_file:
                            hdfs_file.write(csv_content)
                
                print(f"Successfully uploaded {csv_name} to HDFS")
                return hdfs_csv_path
            else:
                raise Exception(f"Failed to download file: {response.status_code}")
        except Exception as e:
            print(f"Error downloading/uploading file {zip_name}: {e}")
            raise

    # Download di un range di date
    def download_date_range(self, start_date, end_date):
        current_date = start_date
        paths = []
        while current_date <= end_date:
            try:
                path = self.download_gdelt_file(current_date)
                paths.append(path)
            except Exception as e:
                print(f"Error downloading data for {current_date}: {e}")
            current_date += timedelta(days=1)
        return paths
    