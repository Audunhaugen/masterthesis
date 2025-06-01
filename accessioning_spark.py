import builtins
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, to_timestamp, col, expr, lit, first, lag, min, max, last, regexp_extract, regexp_replace, when, lead, count, mean, sum as spark_sum
from pyspark.sql.types import TimestampType, IntegerType, DecimalType, StringType, StructField, StructType
from pyspark.sql.window import Window
import time
from datetime import datetime, timedelta

start_time = time.time()

# spark session initialization
spark = SparkSession.builder.appName("combineDateAndTime").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Function to combine date and time columns into a timestamp column
def combine_date_and_time_in_df(
    df: DataFrame,
    date_column_name: str,
    time_column_name: str,
    timestamp_column_name: str
) -> DataFrame:
    """
    Combines separate date and time columns into a single timestamp column.
    """
    return df.withColumn(
        timestamp_column_name,
        to_timestamp(concat_ws(" ", col(date_column_name), col(time_column_name)), "yyyy-MM-dd HH:mm:ss")
    ).drop(date_column_name, time_column_name)

# Read parquet files and select relevant columns
events_df = spark.read.parquet("converted_events.parquet").select("eventid", "eventdate", "eventtime", "requisitionid", "eventcategory", "tokenid", "info", "machine", "status", "username", "workstation")
access_df = spark.read.parquet("converted_access.parquet").select("requisitionid", "eventdate", "eventtime", "username")

# Fix eventtime by converting from nanoseconds to HH:mm:ss format
events_df = events_df.withColumn("eventtime_fix", expr("from_unixtime(eventtime / 1e9, 'HH:mm:ss')")).drop("eventtime")
access_df = access_df.withColumn("eventtime_fix", expr("from_unixtime(eventtime / 1e9, 'HH:mm:ss')")).drop("eventtime")

# Combine date and time columns into a single timestamp column
events_df = combine_date_and_time_in_df(events_df, "eventdate", "eventtime_fix", "happened_at").drop("eventdate", "eventtime_fix")
access_df = combine_date_and_time_in_df(access_df, "eventdate", "eventtime_fix", "happened_at").drop("eventdate", "eventtime_fix")

events_df.show(10)
print("above is 10 first lines of events dataset of total length " + str(events_df.count()))
access_df.show(10)
print("above is 10 first lines of access dataset of total length " + str(access_df.count()))

base = events_df

to_append_fin = base.filter(
    (col('eventid') == 'AO') | ((col('eventid') == 'ERF') & (col('eventcategory') == '2'))
).select(
    lit(20).alias('event_name'),
    lit(2).alias('event_type'),
    col('happened_at'),
    col('requisitionid'),
    col('requisitionid').alias('token_id'),
    lit(0).alias('token_type'),
    lit(2).alias('revision'),
    col('username'),
    col('workstation'),
    lit(None).cast(IntegerType()).alias('lab_ref')
)

finished_df = to_append_fin

finished_df.show(10)
print("Above is 10 first lines of finished accession added to finished_df with total length " + str(finished_df.count()))

access_df = access_df.withColumnRenamed("happened_at", "happened_at_right").withColumnRenamed("username", "username_right")
base = base.repartition("requisitionid")


a = (
    base.filter(col('eventid') == 'AO')
    .join(access_df, 'requisitionid')
    .filter(col('happened_at') > col('happened_at_right'))
    .groupBy('requisitionid')
    .agg(
        min(col('happened_at_right')).alias('happened_at'),
        first(col('username_right')).alias('username')
    )
    .withColumn('type', lit('A'))
)

b = (
    base.filter(col('eventid') == 'AO')
    .select(
        col('requisitionid'),
        col('happened_at'),
        col('username'),
        lit('B').alias('type')
    )
)

c = (
    base.filter(
        (col('eventid') == 'ERF') & (col('eventcategory') == '2')
    )
    .select(
        col('requisitionid'),
        col('happened_at'),
        col('username'),
        lit('C').alias('type')
    )
)

together = a.unionByName(b).unionByName(c)

together.show(10)
print("Above is accessioning type A, B and C, with length " + str(together.count()))

together = together.repartition("username")
window_spec = Window.partitionBy("username").orderBy("happened_at")

d = (
    together.select("requisitionid", "happened_at", "username", "type")
    .sort("username", "happened_at")
    .withColumn("n", lag("happened_at", 1).over(window_spec))
    .withColumn("a", lag("username", 1).over(window_spec))
    .withColumn("t", lag("type", 1).over(window_spec))
    .filter((col("type") == "C") & (col("username") == col("a")))
    .select(
        col("requisitionid"),
        col("n").alias("happened_at"),
        col("username"),
        lit("D").alias("type")
    )
)
together = together.unionByName(d)

together.show(10)
print("Above is accessioning type D added, with length " + str(together.count()))

together = together.repartition("type")
to_append_start = (
    together.filter(col("type").isin(["A", "D"]))
    .select(
        lit(20).alias("event_name"),
        lit(1).alias("event_type"),
        col("happened_at"),
        col("requisitionid"),
        col("requisitionid").alias("token_id"),
        lit(0).alias("token_type"),
        lit(2).alias("revision"),
        col("username"),
        lit(None).cast("string").alias("workstation"),
        lit(None).cast("int").alias("lab_ref")
    )
)

to_append_start.show(10)
print("Showed accessioning start events, with length " + str(to_append_start.count()))

finished_df = finished_df.repartition("requisitionid")
finished_df = finished_df.unionByName(to_append_start).orderBy("happened_at")

finished_df.show()
print("Above is 10 first lines of accession (start + end) added to finished df with total length " + str(finished_df.count()))

print(f"Execution time: {time.time() - start_time}")