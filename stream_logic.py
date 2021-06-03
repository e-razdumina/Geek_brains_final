#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

kafka_brokers = "10.0.0.6:6667"

#читаем кафку по одной записи, но можем и по 1000 за раз
test_user = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "test_users"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "1"). \
    load()

schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("ts", StringType())

user_id = test_user.select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

user_id_flat = user_id.select(F.col("value.*"), "offset")

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

s = console_output(user_id_flat, 5)
s.stop()

#подготавливаем DataFrame для запросов к касандре со скорами
cassandra_recs = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_recs", keyspace="keyspace1" ) \
    .load()

cassandra_recs.show()

def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new request--------")
    print("This is what I've got from Kafka:")
    df.show()
    
    users_list_df = df.select("user_id").distinct()
    users_list_rows = users_list_df.collect()
    users_list = map( lambda x: str(x.__getattr__("user_id")) , users_list_rows )
    where_string = " user_id = " + " or user_id = ".join(users_list)
    print("I'm gonna select this from Cassandra:")
    print(where_string)
    features_from_cassandra = cassandra_recs.where(where_string).na.fill(0)
    features_from_cassandra.persist()
    print("Here is what I've got from Cassandra:")
    features_from_cassandra.show()
    
    if features_from_cassandra is null:
        cassandra_recs.where(" user_id = 10000000000")
        
    df.unpersist()


stream = user_id_flat \
    .writeStream \
    .trigger(processingTime='1 seconds') \
    .foreachBatch(writer_logic) \
    .option("checkpointLocation", "checkpoints/test_users_checkpoint")

s = stream.start()

s.stop()
