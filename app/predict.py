from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time
from cassandra.cluster import Cluster
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, FMClassifier
from pyspark.ml.classification import RandomForestClassifier,  LogisticRegression , NaiveBayes, MultilayerPerceptronClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer, VectorIndexer, MinMaxScaler, IndexToString, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

kafka_topic_name = "demo22"
kafka_bootstrap_servers = 'localhost:9092'
cluster = Cluster()

session = cluster.connect('k1')

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    persistedModel = PipelineModel.load("D:\\kafka-demo\\model")

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

        
    orders_schema_string = '''ID STRING, QUARTER INT, MONTH INT, DAY_OF_MONTH INT, DAY_OF_WEEK INT,
        OP_UNIQUE_CARRIER STRING,
        ORIGIN STRING,
        DEST STRING, DISTANCE DOUBLE,
        CRS_DEP_TIME INT,
        DEP_TIME DOUBLE,
        DEP_DELAY DOUBLE,
        DEP_DELAY_NEW DOUBLE,
        DEP_DEL15 DOUBLE,
        ARR_DEL15 DOUBLE'''

    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string)\
        .alias("orders"), "timestamp")
    
    # Chuẩn hóa tên cột để tương thích với model
    orders_df3 = orders_df2.select("orders.*", "timestamp")
        
    orders_df3.printSchema()

    
    print("Chua transform")
    prediction1 = persistedModel.transform(orders_df3)
    print("Da transform")
    
    # Tạo DataFrame với các cột cần thiết
    predicted1 = prediction1.select('ARR_DEL15', "prediction", 'timestamp')
    print("Khong van de 1")
    
    # Tạo DataFrame với tất cả các cột
    predicted2 = prediction1.select('ID', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
            'OP_UNIQUE_CARRIER',
            'ORIGIN',
            'DEST', 'DISTANCE',
            'CRS_DEP_TIME',
            'DEP_TIME', 'DEP_DELAY',
            'DEP_DELAY_NEW','DEP_DEL15',
            'ARR_DEL15', "prediction")
    print("Khong van de 2")
    
    orders_agg_write_stream1 = predicted2 \
        .writeStream \
        .trigger(processingTime = "5 seconds")\
        .outputMode("append") \
        .option("path", "D:\\kafka-demo\\output\\")\
        .option("checkpointLocation", "file:///D:/kafka-demo/checkpoint-stream/") \
        .format("csv") \
        .start()
    print("Khong  van de 3")
    orders_agg_write_stream = predicted1 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    print("Khong  van de 4") 
    
    orders_agg_write_stream1.awaitTermination()  
    orders_agg_write_stream.awaitTermination()
    # orders_agg_write_stream2.awaitTermination()
    print("Stream Data Processing Application Completed.")