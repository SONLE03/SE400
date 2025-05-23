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

kafka_topic_name = "demo20"
kafka_bootstrap_servers = 'localhost:9092'
# persistedModel = PipelineModel.load("C:\kafka-demo\model")
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
    print("Printing Schema of orders_df: ")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

        
    orders_schema_string = '''id STRING ,quarter INT, month INT, day_of_month INT, day_of_week INT ,
            op_unique_carrier STRING,
            origin STRING,
            dest STRING, distance DOUBLE,
            crs_dep_time INT,
            dep_time DOUBLE,
            dep_delay DOUBLE,
            dep_delay_new DOUBLE,
            dep_del15 DOUBLE,
            arr_del15 DOUBLE'''

    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    
    
    orders_df3.printSchema()

    # Simple aggregate - find total_order_amount by grouping country, city
    # orders_df4 = orders_df3.groupBy("order_country_name", "order_city_name") \
    #     .agg({'order_amount': 'sum'}) \
    #     .select("order_country_name", "order_city_name", col("sum(order_amount)") \
    #     .alias("total_order_amount"))

    # print("Printing Schema of orders_df4: ")
    # orders_df4.printSchema()

    
    

    

    print("Chua transform")
    # prediction1 = persistedModel.transform(orders_df3)
    print("Da transform")
    # predicted1 = prediction1.select('LABEL', "prediction",'timestamp')
    print("Khong  van de 1")
    predicted2 = orders_df3.select('id' ,'quarter' , 'month','day_of_month' , 'day_of_week' ,
            'op_unique_carrier', 
            'origin',
            'dest', 'distance',
            'crs_dep_time',
            'dep_time',
            'dep_delay',
            'dep_delay_new',
            'dep_del15',
            'arr_del15')
    print("Khong  van de 2")
    
    orders_agg_write_stream1 = predicted2 \
        .writeStream \
        .trigger(processingTime = "5 seconds")\
        .outputMode("append") \
        .option("path", "file:///D:/kafka-demo/output-train/")\
        .option("checkpointLocation", "file:///D:/kafka-demo/checkpoint/") \
        .format("csv") \
        .start()
    print("Khong  van de 3")
    orders_agg_write_stream = predicted2 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    print("Khong  van de 4")
    # orders_agg_write_stream2 = predicted1 \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .options(table="test", keyspace="k1")\
    #     .format("org.apache.spark.sql.cassandra") \
    #     .start()
    orders_agg_write_stream_cassandra = predicted2 \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="train_data", keyspace="k1") \
    .option("checkpointLocation", "file:///D:/kafka-demo/checkpoint-cassandra/") \
    .start()    
    # prediction1.toPandas()
    
    # df_check = spark.read \
    # .format("org.apache.spark.sql.cassandra") \
    # .options(table="train_data", keyspace="k1") \
    # .load()
    # df_check.show(10)

    orders_agg_write_stream1.awaitTermination()  
    orders_agg_write_stream.awaitTermination()
    orders_agg_write_stream_cassandra.awaitTermination()
    # orders_agg_write_stream2.awaitTermination()
    print("Stream Data Processing Application Completed.")