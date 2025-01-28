# gdelt_spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pymongo import MongoClient
import logging
import os
from hdfs import InsecureClient
from datetime import datetime, date
import re
import spacy
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from utils.spark_singleton import SparkManager
from aggregations.aggregation_collections import AggregationsManager
from utils.keyword_extractor import KeywordExtractor


# Classe per processare i dati GDELT
class SparkProcessorGDELT:
    def __init__(self):
       logging.basicConfig(level=logging.INFO)
       self.logger = logging.getLogger(__name__)
       
       self.keyword_extractor = KeywordExtractor()
       
       # Get environment variables
       self.hdfs_host = os.getenv('HDFS_HOST', 'namenode')
       self.hdfs_port = os.getenv('HDFS_PORT', '9000')
       self.hdfs_webhdfs_port = os.getenv('HDFS_WEBHDFS_PORT', '9870')
       self.mongodb_host = os.getenv('MONGODB_HOST', 'mongodb')
       self.mongodb_port = os.getenv('MONGODB_PORT', '27017')
       
       # Inizializzazione HDFS client
       self.hdfs_client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_webhdfs_port}')
       
       # Get shared Spark instance
       self.spark = SparkManager.get_instance(self.hdfs_host)
       
       # Inizializzazione MongoDB client
       self.mongo_client = MongoClient(f"mongodb://{self.mongodb_host}:{self.mongodb_port}/")
       self.db = self.mongo_client.gdelt_db
       
       self.logger.info("SparkProcessor initialized successfully")

    # Processa il file GDELT
    def process_file(self, hdfs_path):
       try:
           self.logger.info(f"\n{'='*50}")
           self.logger.info(f"Processing file: {hdfs_path}")
           
           # Costruiamo il path HDFS completo
           hdfs_uri = f"hdfs://{self.hdfs_host}:9000{hdfs_path}"
           
           # Read CSV directly from HDFS
           df = self.spark.read.format("csv") \
               .option("sep", "\t") \
               .option("header", "false") \
               .option("maxColumns", "60") \
               .schema(self._get_schema()) \
               .load(hdfs_uri)
           
           self.logger.info("Successfully loaded CSV from HDFS")
           
           # Count raw data
           raw_count = df.count()
           self.logger.info(f"\nDATA COUNTS:")
           self.logger.info(f"Raw records in HDFS: {raw_count:,}")
           
           # Process the data
           processed_df = self._transform_data(df)
           processed_count = processed_df.count()
           self.logger.info(f"Records after processing: {processed_count:,}")
           
           # Convertiamo il df processato in una lista di dizionari
           rows = processed_df.collect()
           total_written = 0
           chunk_size = 100000
           
           # Convertiamo i dati in una lista di dizonari e formattiamo la data
           all_documents = []
           for row in rows:
               doc = row.asDict()
               for k, v in doc.items():
                   if v is not None:
                       if isinstance(v, (datetime, date)):
                           doc[k] = v.isoformat()
               all_documents.append(doc)
           
           # Processiamo i dati a chunk
           total_docs = len(all_documents)
           for i in range(0, total_docs, chunk_size):
               chunk = all_documents[i:i + chunk_size]
               
               # Scrittura in MongoDB
               if chunk:
                   result = self.db.events.insert_many(chunk)
                   written = len(result.inserted_ids)
                   total_written += written
           
           # Final summary
           self.logger.info(f"\nFINAL SUMMARY for {os.path.basename(hdfs_path)}:")
           self.logger.info(f"{'Raw records in HDFS:':<30} {raw_count:,}")
           self.logger.info(f"{'Processed records:':<30} {processed_count:,}")
           self.logger.info(f"{'Written to MongoDB:':<30} {total_written:,}")
           
           return True
           
       except Exception as e:
           self.logger.error(f"Error processing file {hdfs_path}: {str(e)}", exc_info=True)
           # Nel caso di errori, riproviamo ad inizializzare di nuovo
           try:
               self.__init__()
           except:
               pass
           return False

    # Funzione per definire schema GDELT
    def _get_schema(self):
       
       return StructType([
           StructField("GlobalEventID", IntegerType(), True),       # 0
           StructField("SQLDATE", StringType(), True),              # 1
           StructField("MonthYear", IntegerType(), True),           # 2
           StructField("Year", IntegerType(), True),                # 3
           StructField("FractionDate", DoubleType(), True),         # 4
           StructField("Actor1Code", StringType(), True),           # 5
           StructField("Actor1Name", StringType(), True),           # 6
           StructField("Actor1CountryCode", StringType(), True),    # 7
           StructField("Actor1KnownGroupCode", StringType(), True), # 8
           StructField("Actor1EthnicCode", StringType(), True),     # 9
           StructField("Actor1Religion1Code", StringType(), True),  # 10
           StructField("Actor1Religion2Code", StringType(), True),  # 11
           StructField("Actor1Type1Code", StringType(), True),      # 12
           StructField("Actor1Type2Code", StringType(), True),      # 13
           StructField("Actor1Type3Code", StringType(), True),      # 14
           StructField("Actor2Code", StringType(), True),           # 15
           StructField("Actor2Name", StringType(), True),           # 16
           StructField("Actor2CountryCode", StringType(), True),    # 17
           StructField("Actor2KnownGroupCode", StringType(), True), # 18
           StructField("Actor2EthnicCode", StringType(), True),     # 19
           StructField("Actor2Religion1Code", StringType(), True),  # 20
           StructField("Actor2Religion2Code", StringType(), True),  # 21
           StructField("Actor2Type1Code", StringType(), True),      # 22
           StructField("Actor2Type2Code", StringType(), True),      # 23
           StructField("Actor2Type3Code", StringType(), True),      # 24
           StructField("IsRootEvent", IntegerType(), True),         # 25
           StructField("EventCode", StringType(), True),            # 26
           StructField("EventBaseCode", StringType(), True),        # 27
           StructField("EventRootCode", StringType(), True),        # 28
           StructField("QuadClass", IntegerType(), True),           # 29
           StructField("GoldsteinScale", DoubleType(), True),       # 30
           StructField("NumMentions", IntegerType(), True),         # 31
           StructField("NumSources", IntegerType(), True),          # 32
           StructField("NumArticles", IntegerType(), True),         # 33
           StructField("AvgTone", DoubleType(), True),              # 34
           StructField("Actor1Geo_Type", IntegerType(), True),      # 35
           StructField("Actor1Geo_FullName", StringType(), True),   # 36
           StructField("Actor1Geo_CountryCode", StringType(), True),# 37
           StructField("Actor1Geo_ADM1Code", StringType(), True),   # 38
           StructField("Actor1Geo_Lat", DoubleType(), True),        # 39
           StructField("Actor1Geo_Long", DoubleType(), True),       # 40
           StructField("Actor1Geo_FeatureID", StringType(), True),  # 41
           StructField("Actor2Geo_Type", IntegerType(), True),      # 42
           StructField("Actor2Geo_FullName", StringType(), True),   # 43
           StructField("Actor2Geo_CountryCode", StringType(), True),# 44
           StructField("Actor2Geo_ADM1Code", StringType(), True),   # 45
           StructField("Actor2Geo_Lat", DoubleType(), True),        # 46
           StructField("Actor2Geo_Long", DoubleType(), True),       # 47
           StructField("Actor2Geo_FeatureID", StringType(), True),  # 48
           StructField("ActionGeo_Type", IntegerType(), True),      # 49
           StructField("ActionGeo_FullName", StringType(), True),   # 50
           StructField("ActionGeo_CountryCode", StringType(), True),# 51
           StructField("ActionGeo_ADM1Code", StringType(), True),   # 52
           StructField("ActionGeo_Lat", DoubleType(), True),        # 53
           StructField("ActionGeo_Long", DoubleType(), True),       # 54
           StructField("ActionGeo_FeatureID", StringType(), True),  # 55
           StructField("DATEADDED", IntegerType(), True),           # 56
           StructField("SOURCEURL", StringType(), True),            # 57
       ])

    def _transform_data(self, df):
        """Applichiamo transformazione al Dataframe"""
        df = df.withColumn("ProcessedDate", current_timestamp()) \
            .withColumn("ProcessingDate", current_date())
        return df
            
    def process_all_aggregations(self, total_files):
        """
        Processa tutte le aggregazioni una volta che tutti i file sono stati processati
        """
        try:   
                # Usa AggregationsManager
                aggregator = AggregationsManager(self.mongodb_host, self.mongodb_port)
                keyword_extractor = KeywordExtractor()
                
                # Crea le collections di aggregazione
                aggregator.create_daily_insights()
                aggregator.create_geo_analysis()
                aggregator.create_trend_analysis()
                aggregator.create_top_mentions(self.keyword_extractor)
                
                # Ottieni le keywords
                keywords = aggregator.get_top_mentions_keywords(self.keyword_extractor)
                self.logger.info(f"Extracted keywords: {keywords}")
            
                aggregator.close()
                
                self.processing_complete = True
                return True
        except Exception as e:
            self.logger.error(f"Error in aggregation process: {str(e)}", exc_info=True)
            return False

    def close(self):
       """Clean up resources"""
       try:
           if self.mongo_client:
               self.mongo_client.close()
           if self.spark:
               self.spark.stop()
       except:
           pass
       