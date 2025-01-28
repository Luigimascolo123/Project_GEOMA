# main.py
from datetime import datetime, timedelta
from downloaders.gdelt_downloader import GDELTDownloader
from processors.gdelt_spark_processor import SparkProcessorGDELT
from downloaders.openm_downloader import OpenmeasuresDownloader
from processors.openm_spark_processor import SparkProcessorOpenMeasures
from utils.spark_singleton import SparkManager
from aggregations.aggregation_collections import AggregationsManager
import logging
import os
import time

def main():
    try:
        # Inizializzazione HDFS url
        hdfs_url = f"http://{os.getenv('HDFS_HOST', 'namenode')}:{os.getenv('HDFS_PORT', '9870')}"
        
        # Settiamo il range temporale
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)  #col parametro days scegliamo quanti giorni analizzare
        
        # Openmeasures socials
        openmeasures_social = ["tiktok_video", "truth_social", "vk"]
        
        #Inizializzazione Downloader/Processor per Openmeasures
        downloader_openmeasures = OpenmeasuresDownloader()
        processor_openmeasures = SparkProcessorOpenMeasures()
        
        #Inizializzazione Downloader/Processor per GDELT
        downloader_gdelt = GDELTDownloader(hdfs_url=hdfs_url)
        processor_gdelt = SparkProcessorGDELT()
        
        # GDELT Download files
        print("\nDownloading GDELT files...")
        paths_gdelt = downloader_gdelt.download_date_range(start_date, end_date)
        
        # GDELT Process files
        print("Processing GDELT files...")
        for path in paths_gdelt:
            if processor_gdelt.process_file(path):
                print(f"Successfully processed {path}")
            else:
                print(f"Failed to process {path}")
        
        # Creazione collections GDELT
        processor_gdelt.process_all_aggregations(len(paths_gdelt)) 
        
        # Estrai le keywords dai dai di GDELT
        aggregator = AggregationsManager(processor_gdelt.mongodb_host, processor_gdelt.mongodb_port)
        keywords_dict = aggregator.get_top_mentions_keywords(processor_gdelt.keyword_extractor)
        
        
        # Openmeasures Download files
        print("\nDownloading Openmeasures files...")
        for social in openmeasures_social:
            try:
                print(f"\nDownloading {social}...")
                paths_openm = downloader_openmeasures.download_date_range(
                    social, 
                    start_date, 
                    end_date,
                    keywords_dict=keywords_dict
                )
                
                # Openmeasures Process files
                for path in paths_openm:
                    if social in path.lower():
                        if processor_openmeasures.process_file(path, social):
                            print(f"Successfully processed {path} for {social}")
                        else:
                            print(f"Failed to process {path} for {social}")
                            
                #Creazione collections Openmeasures           
                processor_openmeasures.process_all_aggregations_openm(len(paths_openm))
            except Exception as e:
                print(f"Error processing {social}: {str(e)}")
                continue
            
    except Exception as e:
        print(f"Error in main processing: {str(e)}")
        raise

    finally:
        SparkManager.stop()

if __name__ == "__main__":
    main()