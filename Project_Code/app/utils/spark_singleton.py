# spark_singleton.py
from pyspark.sql import SparkSession
import os

class SparkManager:
    _instance = None
    
    @classmethod
    def get_instance(cls, hdfs_host='namenode'):
        if cls._instance is None:
            spark = SparkSession.builder \
                .appName("Data Processing") \
                .master("local[*]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.sql.shuffle.partitions", "10") \
                .config("spark.default.parallelism", "10") \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.hadoop.fs.defaultFS", f"hdfs://{hdfs_host}:9000") \
                .config("spark.hadoop.dfs.replication", "1") \
                .getOrCreate()
            cls._instance = spark
            
        return cls._instance

    @classmethod
    def stop(cls):
        if cls._instance:
            cls._instance.stop()
            cls._instance = None