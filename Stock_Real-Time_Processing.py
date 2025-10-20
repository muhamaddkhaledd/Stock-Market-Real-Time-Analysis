from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, count, stddev, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

spark = SparkSession.builder \
    .appName("Binance-Realtime") \
         .config("spark.sql.caseSensitive", "true")\
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0")\
         .config("spark.sql.shuffle.partitions", "4") \
         .getOrCreate()

schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("t", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("T", LongType()),
    StructField("m", BooleanType()),
    StructField("M", BooleanType())
])


df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "broker:9092")\
    .option("subscribe", "binance_trades").load()

df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("p").cast("double")) \
    .withColumn("quantity", col("q").cast("double")) \
    .withColumn("timestamp", (col("T") / 1000).cast("timestamp"))

final = df.groupBy(window(col("timestamp"), "10 seconds", "5 seconds"), col("s")) \
    .agg(
        avg("price").alias("avg_price"),
        sum("quantity").alias("total_quantity"),
        sum(col("price") * col("quantity")).alias("traded_value"),
        avg("quantity").alias("avg_quantity"),
        stddev("price").alias("volatility"),
        count(when(col("m") == False, True)).alias("buy_count"),
        count(when(col("m") == True, True)).alias("sell_count"),
        (count(when(col("m") == False, True)) / count(when(col("m") == True, True))).alias("buy_sell_ratio"),
        count("*").alias("trade_count")
    )

# Flatten window struct into separate columns
final_flat = final.withColumn("window_start", col("window.start")) \
                  .withColumn("window_end", col("window.end")) \
                  .drop("window")


#send datas to database
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        print("No data in this batch")
        return
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/stock_data") \
        .option("dbtable", "stock_metrics") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


query = final_flat.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
