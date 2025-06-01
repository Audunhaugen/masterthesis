import pyarrow.parquet as pq
import pyarrow as pa
from pyspark.sql import SparkSession

# To adjust for spark compatible typing, this script reads Parquet files, fixes time64[ns] columns, 
# converts them to Spark DataFrames, repartitions them based on specific columns, and writes the optimized Parquet 
# files back to disk.

# Initialize Spark
spark = SparkSession.builder.appName("ParquetRepartition").getOrCreate()

# Define input and output file paths
input_files = [
    "events.parquet",
    "access.parquet"
]

output_files = [
    "converted_access.parquet",
    "converted_access.parquet"
]

# Loop through the files and process them
for input_parquet, output_parquet in zip(input_files, output_files):
    print(f"Processing: {input_parquet}")

    table = pq.read_table(input_parquet)

    # Fix time64[ns] columns
    fixed_columns = []
    for col in table.schema:
        if pa.types.is_time64(col.type):  # Detect time64[ns]
            print(f"Converting {col.name} from time64[ns] to STRING...")
            fixed_columns.append(table[col.name].cast(pa.int64()).cast(pa.string()))
        else:
            fixed_columns.append(table[col.name])

    new_schema = pa.schema([(col.name, pa.string() if pa.types.is_time64(col.type) else col.type) for col in table.schema])

    fixed_table = pa.Table.from_arrays(fixed_columns, schema=new_schema)

    
    print("Converting PyArrow table to Spark DataFrame...")
    df_spark = spark.createDataFrame(fixed_table)

    
    if "eventid" in df_spark.columns:
        print(f"Repartitioning {input_parquet} by 'eventid'...")
        df_spark = df_spark.repartition("eventid") 
    elif "requisitionid" in df_spark.columns:
        print(f"Repartitioning {input_parquet} by 'requisitionid'...")
        df_spark = df_spark.repartition("requisitionid")

    print(f"Writing the final optimized Parquet file for {input_parquet}...")
    df_spark.write.mode("overwrite").parquet(output_parquet)

    print(f"Fixed & Repartitioned Parquet file saved at: {output_parquet}")

print("All files processed successfully!")
