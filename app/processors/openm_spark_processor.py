from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row  
from pymongo import MongoClient
import logging
import os
from hdfs import InsecureClient
from datetime import datetime, date
from utils.spark_singleton import SparkManager
from utils.schema_registry import SchemaRegistry
from aggregations.aggregation_collections import AggregationsManager

class SparkProcessorOpenMeasures:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
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
        self.db = self.mongo_client.openmeasures_db
        
        self.logger.info("SparkProcessor initialized successfully")

    # Processa un file JSON da HDFS
    def process_file(self, hdfs_path: str, social: str) -> bool:
        try:
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"Processing file: {hdfs_path} for platform: {social}")
            
            # Usa lo schema dati appropriato per il social
            try:
                schema = SchemaRegistry.get_schema(social)
            except ValueError as e:
                self.logger.error(str(e))
                return False
            
            # Costriusce l'URI HDFS
            hdfs_uri = f"hdfs://{self.hdfs_host}:9000{hdfs_path}"
            
            # Leggi il JSON con lo schema appropriato 
            df = self.spark.read \
                .option("multiline", "true") \
                .schema(schema) \
                .json(hdfs_uri)
            
            # Estraiamo i dati 
            if social.lower() == "tiktok_video":
                df = df.select(explode("hits.hits").alias("hit")) \
                    .select("hit._source.*")
                    
            elif social.lower() == "truth_social":
                df = df.select(explode("hits.hits._source").alias("data")) \
                    .select("data.*")
                    
            elif social.lower() == "vk":
                df = df.select(explode("hits.hits").alias("hit")) \
                    .select("hit._source.*")
            
            self.logger.info("Successfully loaded JSON from HDFS")
            
            # Count raw data
            raw_count = df.count()
            self.logger.info(f"\nDATA COUNTS:")
            self.logger.info(f"Raw records in HDFS: {raw_count:,}")
            
            # Process the data
            processed_df = self._transform_data(df, social)
            processed_count = processed_df.count()
            self.logger.info(f"Records after processing: {processed_count:,}")
            
            rows = processed_df.collect()
            total_written = 0
            chunk_size = 100000
            
            # Convertiamo i dati in una lista di dizonari e 
            all_documents = []
            for row in rows:
                doc = {}
                for field in row.__fields__:
                    value = getattr(row, field)
                    if value is not None:
                        if isinstance(value, (datetime, date)):
                            doc[field] = value.isoformat()
                        elif isinstance(value, Row):  # Per oggetti strutturati come stats
                            doc[field] = value.asDict()
                        elif isinstance(value, list) and value and isinstance(value[0], Row):  # Per array di oggetti strutturati
                            doc[field] = [v.asDict() for v in value]
                        else:
                            doc[field] = value
                all_documents.append(doc)
            
            # Process in chunks
            total_docs = len(all_documents)
            for i in range(0, total_docs, chunk_size):
                chunk = all_documents[i:i + chunk_size]
                self.logger.info(f"Processing chunk {i//chunk_size + 1} of {(total_docs + chunk_size - 1)//chunk_size}")
                
                # Write to MongoDB
                collection_name = f"Openmeasures_data"
                if chunk:
                    result = self.db[collection_name].insert_many(chunk)
                    written = len(result.inserted_ids)
                    total_written += written
                    self.logger.info(f"Written {written} documents in this chunk")
            
            # Final summary
            self.logger.info(f"\nFINAL SUMMARY for {os.path.basename(hdfs_path)}:")
            self.logger.info(f"{'Raw records in HDFS:':<30} {raw_count:,}")
            self.logger.info(f"{'Processed records:':<30} {processed_count:,}")
            self.logger.info(f"{'Written to MongoDB:':<30} {total_written:,}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing file {hdfs_path}: {str(e)}", exc_info=True)
            # Proviamo ad inizializzare di nuovo Spark in caso di errore
            try:
                self.__init__()
            except:
                pass
            return False

    def _transform_data(self, df, social: str):
        
        # Aggiungi campi di data di processamento e piattaforma
        df = df.withColumn("ProcessedDate", current_timestamp()) \
            .withColumn("ProcessingDate", current_date()) \
            .withColumn("Platform", lit(social.lower()))
        
        # Definiamo un mapping per i campi comuni dei diversi social
        
        #TIKTOK_VIDEO
        if social.lower() == "tiktok_video":
            #Estraiamo gli hashtags 
            df = df.withColumn("hashtags",
                          when(size(col("challenges")) > 0,
                               transform("challenges", lambda x: x.title))
                          .otherwise(array()))
            
            # Convertiamo il timestamp Unix di TikTok in timestamp
            df = df.withColumn("created_at_ts", from_unixtime(col("createTime")).cast("timestamp"))
            
            # Convertiamo lastseents in timestamp
            df = df.withColumn("collected_at_ts", to_timestamp(col("lastseents")))
            
            # Rinominiamo e selezioniamo i campi per TikTok
            df = df.select(
                col("Platform"),
                col("id").alias("post_id"),
                col("desc").alias("content"),
                col("author").alias("author_name"),
                col("author_id"),
                col("created_at_ts").alias("created_at"),
                col("stats.diggCount").alias("likes_count"),
                col("stats.shareCount").alias("shares_count"),
                col("stats.commentCount").alias("comments_count"),
                col("challenges.title").alias("hashtags"),
                col("datatype"),
                col("lastseents"),
                col("ProcessedDate"),
                col("ProcessingDate"),
                col("collected_by"),
                col("isAd").alias("is_sponsored"),
                
                col("stats.playCount").alias("views_count"),    #Colonne solo di tiktok_video
                col("music.musicName").alias("music_name"),
                col("music.musicAuthor").alias("music_author"),
                col("duration"),
                col("definition").alias("video_quality"),
                
                lit(None).cast("string").alias("provider_name"),    #Colonne solo di truth_social
                lit(None).cast("string").alias("provider_url"),
                lit(None).cast("string").alias("provider_title"),
                lit(None).cast("string").alias("provider_description"),
            )
            
        #TRUTH_SOCIAL
        elif social.lower() == "truth_social":
            # Estrai gli hashtags da tags.name
            df = df.withColumn("hashtags",
                      col("tags.name"))
            
            # Convertiamo le stringhe di data in timestamp
            df = df.withColumn("created_at_ts", to_timestamp(col("created_at")))
            
            # Ultima visita del post
            df = df.withColumn("collected_at_ts", to_timestamp(col("lastseents")))
            
            # Rinominiamo e selezioniamo i campi per Truth Social
            df = df.select(
                col("Platform"),
                col("id").alias("post_id"),
                col("content_cleaned").alias("content"),
                col("account.acct").alias("author_name"),
                col("account.id").alias("author_id"),
                col("created_at_ts").alias("created_at"),
                col("favourites_count").alias("likes_count"),
                col("reblogs_count").alias("shares_count"),
                col("replies_count").alias("comments_count"),
                col("tags.name").alias("hashtags"),
                col("datatype"),
                col("lastseents"),
                col("ProcessedDate"),
                col("ProcessingDate"),
                col("collected_by"),
                col("sponsored").alias("is_sponsored"),
                
                lit(None).cast("long").alias("views_count"),    #Colonne solo di tiktok_video
                lit(None).cast("string").alias("music_name"),
                lit(None).cast("string").alias("music_author"),
                lit(None).cast("integer").alias("duration"),
                lit(None).cast("string").alias("video_quality"),
                
                col("card.provider_name").alias("provider_name"),   #Colonne solo di truth_social
                col("card.url").alias("provider_url"),
                col("card.title").alias("provider_title"),
                col("card.description").alias("provider_description"),
            )
        
        elif social.lower() == "vk":
            # Convertiamo il timestamp Unix di VK in timestamp
            df = df.withColumn("created_at_ts", 
                from_unixtime(col("date")).cast("timestamp"))
            
            # Convertiamo lastseents in timestamp
            df = df.withColumn("collected_at_ts", 
                to_timestamp(col("lastseents")))
            
            # Estrai hashtag dal testo (parole che iniziano con #)
            df = df.withColumn("hashtags",
                expr("array_distinct(filter(split(text, ' '), x -> startswith(x, '#')))"))

            # Rinominiamo e selezioniamo i campi per VK
            df = df.select(
                col("Platform"),
                col("id").alias("post_id"),
                col("text").alias("content"),
                col("author").alias("author_name"),
                col("from_id").alias("author_id"),  
                col("created_at_ts").alias("created_at"),
                col("likes.count").alias("likes_count"),
                col("reposts.count").alias("shares_count"),
                col("comments.count").alias("comments_count"),
                col("hashtags"),
                col("datatype"),
                col("lastseents"),
                col("ProcessedDate"),
                col("ProcessingDate"),
                col("collected_by"),
                col("marked_as_ads").alias("is_sponsored"),
                
                col("views.count").alias("views_count"),  # Campi di VK
                col("post_source").alias("post_source"),
                
                lit(None).cast("string").alias("music_name"),   #Campi di tiktok_video
                lit(None).cast("string").alias("music_author"),
                lit(None).cast("integer").alias("duration"),
                lit(None).cast("string").alias("video_quality"),
                
                lit(None).cast("string").alias("provider_name"), #Campi di truth_social
                lit(None).cast("string").alias("provider_url"),
                lit(None).cast("string").alias("provider_title"),
                lit(None).cast("string").alias("provider_description")
            )
        
        # Calcola l'engagement totale
        df = df.withColumn(
            "total_engagement",
            coalesce(col("likes_count"), lit(0)) + 
            coalesce(col("shares_count"), lit(0)) + 
            coalesce(col("comments_count"), lit(0))
        )
        
        return df

    # Creaiamo le collezioni per Openmeasures
    def process_all_aggregations_openm(self, total_files):
        try:
                # Usa AggregationsManager
                aggregator = AggregationsManager(self.mongodb_host, self.mongodb_port)
                
                # Crea le collections di aggregazione
                aggregator.create_social_temporal_metrics()
                aggregator.create_top_authors_engagement()
                aggregator.create_gdelt_social_correlation()

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