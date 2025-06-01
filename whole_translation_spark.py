import builtins
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, to_timestamp, col, expr, lit, first, lag, min, max, last, regexp_extract, regexp_replace, when, lead, count, mean, sum as spark_sum
from pyspark.sql.types import TimestampType, IntegerType, DecimalType, StringType, StructField, StructType
from pyspark.sql.window import Window
import time as timemodule
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("combineDateAndTime").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
start_time = timemodule.time()

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

base = events_df

##################################### 20 : Accessioning #####################################

to_append_fin = base.filter(
    (col('eventid') == 'AO') | ((col('eventid') == 'ERF') & (col('eventcategory') == '2'))
).select(
    lit(20).alias('event_name'), # event_name 20 for accessioning
    lit(2).alias('event_type'), # event_type 2 for finished event
    col('happened_at'), # happened_at is the timestamp of the event
    col('requisitionid'), # requisitionid is the unique identifier for the accession
    col('requisitionid').alias('token_id'), # token_id is the unique identifier of a token, requisitionid as placeholder
    lit(0).alias('token_type'), # token_type 0 for case
    lit(2).alias('revision'),
    col('username'), # username is the user who performed the event
    col('workstation'), # workstation is the workstation where the event was performed
    lit(None).cast(IntegerType()).alias('lab_ref') # lab_ref is unique identifier of the laboratory
)

finished_df = to_append_fin

access_df = access_df.withColumnRenamed("happened_at", "happened_at_right").withColumnRenamed("username", "username_right")
base = base.repartition("requisitionid")

# join with access_df and find the earliest accession per requisition
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

# preserve original timestamps and usernames
b = (
    base.filter(col('eventid') == 'AO')
    .select(
        col('requisitionid'),
        col('happened_at'),
        col('username'),
        lit('B').alias('type')
    )
)

# manual requisitions
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

together = together.repartition("username")
window_spec = Window.partitionBy("username").orderBy("happened_at")

# Create a new dataframe with the previous event's timestamp, for the same username
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

finished_df = finished_df.repartition("requisitionid")
finished_df = finished_df.unionByName(to_append_start).orderBy("happened_at")

##################################### 30 : Grossing #####################################

makros = base.filter(col("eventcategory") == "934")
first_makros = makros.filter((col("eventid") == 'RE') & (col("info") == "MAKRO"))
nymakros_base = makros.filter((col("eventid") == "RE") & (col("info") == "NYMAKRO")).select("requisitionid", "username", "workstation")

nymakros = nymakros_base.join(
    makros.groupBy(["requisitionid", "username"]).agg(max("happened_at").alias("happened_at")),
    on=["requisitionid", "username"]
)

grossing_fin = nymakros.unionByName(
    first_makros.select("requisitionid", "username", "workstation", "happened_at")
)

to_append_fin = grossing_fin.select(
    lit(30).alias("event_name"),
    lit(2).alias("event_type"),
    col("happened_at"),
    col("requisitionid"),
    col("requisitionid").alias("token_id"),
    lit(0).alias("token_type"),
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")
)

access_renamed = access_df.withColumnRenamed("username_right", "username")

joined = access_renamed.join(
    grossing_fin,
    on=["requisitionid", "username"]
).filter(
    col("happened_at_right") > col("happened_at")
)

grouped = joined.groupBy("requisitionid").agg(
    first("username").alias("username"),
    min("happened_at").alias("happened_at")
)

to_append_start = grouped.select(
    lit(30).alias("event_name"),        
    lit(1).alias("event_type"),          
    col("happened_at"),
    col("requisitionid"),
    col("requisitionid").alias("token_id"),
    lit(0).alias("token_type"),       
    lit(2).alias("revision"),
    col("username"),
    lit(None).cast("string").alias("workstation"),
    lit(0).cast("int").alias("lab_ref")
)

to_append = to_append_start.unionByName(to_append_fin)

finished_df = finished_df.unionByName(to_append)


### 31 : SpecimenContainerArchived

# Finding the latest event of type 'ARCH', marking as 'SpecimenContainerArchived'
archived = base.filter(
    (col("eventid") == "ARCH") &
    (col("info").rlike(r"\S+ \S+ \d+ RESTMAT"))
).groupBy("requisitionid", "username").agg(
    max("happened_at").alias("happened_at")
)

to_append_archived = archived.select(
    lit(31).alias("event_name"),           
    lit(0).alias("event_type"),          
    col("happened_at"),
    col("requisitionid"),
    col("requisitionid").alias("token_id"),
    lit(0).alias("token_type"),            
    lit(2).alias("revision"),
    col("username"),
    lit(None).cast("string").alias("workstation"),
    lit(None).cast("int").alias("lab_ref") 
)

finished_df = finished_df.unionByName(to_append_archived).orderBy("happened_at")

### 32 : SpecimenContainerRetrieved

# Finding the earliest event of type 'DELA', marking as 'SpecimenContainerRetrieved'
retrieved = base.filter(
    col("eventid") == "DELA"
).groupBy("requisitionid", "username").agg(
    min("happened_at").alias("happened_at")
)

to_append_retrieved = retrieved.select(
    lit(32).alias("event_name"),          
    lit(0).alias("event_type"),          
    col("happened_at"),
    col("requisitionid"),
    col("requisitionid").alias("token_id"),
    lit(0).alias("token_type"),           
    lit(2).alias("revision"),
    col("username"),
    lit(None).cast("string").alias("workstation"),
    lit(None).cast("int").alias("lab_ref")
)

finished_df = finished_df.unionByName(to_append_retrieved).orderBy("happened_at")

##################################### 40 : Processing #####################################

proc = base.filter(
    (col("eventcategory") == "FREM") & (col("eventid") == "START")
)

# extracting value of timer from status to determine stop events
proc_base = proc.groupBy("requisitionid", "tokenid").agg(
    last("happened_at", ignorenulls=True).alias("happened_at"),
    last("status", ignorenulls=True).alias("status"),
    last("username", ignorenulls=True).alias("username"),
    last("workstation", ignorenulls=True).alias("workstation")
).withColumn(
    "to_add",
    (
        regexp_replace(
            regexp_extract(col("status"), r"(\d+(,\d+)?) timer.*", 1),
            ",", "."
        ).cast("double") * 60
    ).cast("long")
)

to_append_start = proc_base.select(
    lit(40).alias("event_name"),           
    lit(1).alias("event_type"),          
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),           
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")   
)

to_append_fin = proc_base.select(
    lit(40).alias("event_name"),           
    lit(2).alias("event_type"),            
    (col("happened_at") + expr("CAST(to_add AS INT) * INTERVAL 1 SECOND")).alias("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),            
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")    
)

to_append = to_append_start.union(to_append_fin)

finished_df = finished_df.unionByName(to_append).orderBy("happened_at")

### 41 : Decalcination
dekal = base.filter(
    col("eventcategory") == "DEKAL"
).select(
    "requisitionid",
    "tokenid",
    "eventid",
    "username",
    "workstation",
    "happened_at"
).select(
    lit(41).alias("event_name"), 
    when(col("eventid") == "START", lit(1)).otherwise(lit(2)).cast("int").alias("event_type"),
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),  
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")  
)

to_append = dekal.select(
    lit(41).alias("event_name"),             
    lit(2).alias("event_type"),            
    col("happened_at"),
    col("requisitionid"),
    col("token_id").alias("token_id"),
    lit(2).alias("token_type"),             
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")      
)

finished_df = finished_df.unionByName(to_append).orderBy("happened_at")

dekal = base.filter(col('eventcategory') == "DEKAL").filter(col('eventid') == 'START').select(
    'requisitionid',
    'tokenid',
    'username',
    'workstation',
    'happened_at'
)

to_append = dekal.select(
    lit(41).alias("event_name"),           
    lit(1).alias("event_type"),             
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),              
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")      
)

finished_df = finished_df.unionByName(to_append).orderBy("happened_at")

##################################### 50 (51, 59) : Embedding #####################################
emb_base = base.filter(
    col("eventcategory").isin(["STØP", "KOORD", "IN_ATEK2_HBE"])
)

### 59 : koordinering
# Finding the latest event of type 'KOORD' with eventid 'STOP', marking as instant activity
to_add_coord = emb_base.filter(
    (col("eventid") == "STOP") & (col("eventcategory") == "KOORD")
).groupBy("tokenid").agg(
    last("requisitionid", ignorenulls=True).alias("requisitionid"),
    last("username", ignorenulls=True).alias("username"),
    last("workstation", ignorenulls=True).alias("workstation"),
    last("happened_at", ignorenulls=True).alias("happened_at")
).select(
    lit(59).alias("event_name"),     
    lit(0).alias("event_type"),           
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),          
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")   
)

finished_df = finished_df.unionByName(to_add_coord).orderBy("happened_at")

### 50 : manualembedding
man_embd = emb_base.filter(
    col("eventcategory").isin(["STØP", "IN_ATEK2_HBE"]) & (col("status") == "STØP")
).select(
    lit(50).alias("event_name"), 
    when(col("eventid") == "START", lit(1)).otherwise(lit(2)).cast("int").alias("event_type"),
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref") 
)

finished_df = finished_df.unionByName(man_embd).orderBy("happened_at")

### 51 : automaticembedding
filtered_emb_base = emb_base.filter(
    col("eventcategory").isin(["STØP", "IN_ATEK2_HBE"]) &
    (~col("status").isin(["STØP"]))
)

aut_embd = emb_base.filter(
    col("eventcategory").isin(["STØP", "IN_ATEK2_HBE"]) &
    (~col("status").isin(["STØP"]))
)

aut_embd_base = emb_base.filter(
    col("eventcategory").isin(["STØP", "IN_ATEK2_HBE"]) &
    (~col("status").isin(["STØP"]))
)

aut_embd_starts = aut_embd_base.filter(
    col("eventid") == "START"
).groupBy("requisitionid", "tokenid").agg(
    min("happened_at").alias("happened_at"),
    first("username", ignorenulls=True).alias("username"),
    first("workstation", ignorenulls=True).alias("workstation")
).select(
    lit(51).alias("event_name"),            
    lit(1).cast("int").alias("event_type"),  
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),             
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")     
)

finished_df = finished_df.unionByName(aut_embd_starts).orderBy("happened_at")

aut_embd_fins = aut_embd_base.filter(
    col("eventid") == "STOP"
).groupBy("requisitionid", "tokenid").agg(
    max("happened_at").alias("happened_at"),
    last("username", ignorenulls=True).alias("username"),
    last("workstation", ignorenulls=True).alias("workstation")
).select(
    lit(51).alias("event_name"),             
    lit(2).cast("int").alias("event_type"), 
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(2).alias("token_type"),             
    lit(2).alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")     
)

finished_df = finished_df.unionByName(aut_embd_fins).orderBy("happened_at")

##################################### 60: sectioning #####################################

def _make_sectioning_group_mappings(actor_ref: int, two_sigma: timedelta, work: DataFrame, open_start: bool) -> DataFrame:
    # Define a window over each technician's timeline
    w = Window.partitionBy("username").orderBy("happened_at")

    two_sigma_seconds = two_sigma.total_seconds()

    # Filter STOP events for a given user and calculate time difference between each STOP and the next one
    grouped = work.filter(
        (col("username") == actor_ref) & (col("eventid") == "STOP")
    ).withColumn(
        "happened_at_next", lead("happened_at").over(w)
    ).withColumn(
        "delta", col("happened_at_next").cast("long") - col("happened_at").cast("long")
    ).withColumn(
        "is_new_group", col("delta") > lit(two_sigma_seconds)  # Flag time gaps larger than threshold
    )

    # Convert boolean new-group flag to int, then sum to get group ids
    grouped = grouped.withColumn("is_new_group_int", when(col("is_new_group"), lit(1)).otherwise(lit(0)))
    grouped = grouped.withColumn("group", spark_sum("is_new_group_int").over(w))

    # Aggregate per-group statistics (start, stop, count)
    prelim = grouped.groupBy("group").agg(
        min("happened_at").alias("start"),
        max("happened_at").alias("stop"),
        count("tokenid").alias("count")
    ).withColumn(
        "time_taken", col("stop").cast("long") - col("start").cast("long")
    ).withColumn(
        "per_item", (col("time_taken") / col("count")).cast("double")
    ).orderBy("group")

    # Adjust timing if the start of a session is ambiguous or missing
    if open_start:
        prelim = prelim.withColumn("start_next", lead("start").over(Window.partitionBy("group").orderBy("start")))
        prelim = prelim.withColumn("delta", col("start_next").cast("long") - col("stop").cast("long"))
        prelim = prelim.withColumn("delta_prev", lag("delta").over(Window.partitionBy("group").orderBy("delta")))
        prelim = prelim.withColumn("time_taken_new", col("time_taken") + col("delta_prev"))
        prelim = prelim.withColumn("per_item_new", (col("time_taken_new") / col("count")).cast("double"))
    else:
        prelim = prelim.withColumn("stop_prev", lag("stop").over(Window.partitionBy("group").orderBy("stop")))
        prelim = prelim.withColumn("delta", col("start").cast("long") - col("stop_prev").cast("long"))
        prelim = prelim.withColumn("delta_next", lead("delta").over(Window.partitionBy("group").orderBy("delta")))
        prelim = prelim.withColumn("time_taken_new", col("time_taken") + col("delta_next"))
        prelim = prelim.withColumn("per_item_new", (col("time_taken_new") / col("count")).cast("double"))

    # Fallback for small datasets (only one group)
    if prelim.count() == 1:
        final_groups = prelim.select("group", "start", "stop")
    else:
        # Estimate typical per-item time across groups, capped at 30 minutes
        if "per_item_new" in prelim.columns:
            x_vals = prelim.approxQuantile("per_item_new", [0.5], 0.01)
            if x_vals:
                x = round(builtins.min(x_vals[0], 1800))
            else:
                print("WARN: approxQuantile returned empty list — falling back to default")
                x = 60
        else:
            print("WARN: Column per_item_new missing — falling back to default")
            x = 60
            x = round(builtins.min(x, 1800))

        # Adjust start or stop times using estimated duration
        if open_start:
            final_groups = prelim.withColumn(
                "start_next", lead("stop").over(Window.partitionBy("group").orderBy("stop"))
            ).withColumn(
                "secs", lit(x) * col("count")
            ).withColumn(
                "alt_start", (col("stop").cast("long") - col("secs")).cast("timestamp")
            ).withColumn(
                "start", when(col("start_next").isNotNull(), col("start_next")).otherwise(col("alt_start"))
            ).select("group", "start", "stop")
        else:
            final_groups = prelim.withColumn(
                "stop_new", lead("stop").over(Window.partitionBy("group").orderBy("stop"))
            ).withColumn(
                "secs", lit(x) * col("count")
            ).withColumn(
                "alt_stop", (col("start").cast("long") + col("secs")).cast("timestamp")
            ).withColumn(
                "stop", when(col("stop_new").isNotNull(), col("stop_new")).otherwise(col("alt_stop"))
            ).select("group", "start", "stop")

    # Join adjusted group times back to the original stop events
    enriched = grouped.join(final_groups, on="group")

    # Helper to create synthetic event records
    def make_event_df(enriched, time_col: str, event_type: int) -> DataFrame:
        return enriched.select(
            lit(60).alias("event_name"),
            lit(event_type).alias("event_type"),
            col(time_col).alias("happened_at"),
            col("requisitionid"),
            col("tokenid").alias("token_id"),
            lit(2).alias("token_type"),
            lit(2).alias("revision"),
            col("username"),
            col("workstation"),
            when(col("status") == "SNIMM", lit(2))
            .when(col("status") == "SNNYRE", lit(3))
            .when(col("status") == "SNNEVRO", lit(4))
            .when(col("status") == "SNMOLP", lit(5))
            .otherwise(lit(0)).cast("int").alias("lab_ref")
        )

    # Create start and stop events from enriched timeline
    starts = make_event_df(enriched, "start", 1)
    stops = make_event_df(enriched, "stop", 2)

    return starts.unionByName(stops) if starts.count() > 0 and stops.count() > 0 else starts

def _translate_too_quick(work: DataFrame, act: int) -> DataFrame:
    # Create synthetic start events from raw stop events for fast operators
    new_events = work.filter(
        (col("username") == act) & (col("eventid") == "STOP")
    ).select(
        lit(60).alias("event_name"),
        lit(1).cast("int").alias("event_type"),
        col("happened_at"),
        col("requisitionid"),
        col("tokenid").alias("token_id"),
        lit(2).alias("token_type"),
        lit(2).alias("revision"),
        col("username"),
        col("workstation"),
        when(col("status") == "SNIMM", lit(2))
        .when(col("status") == "SNNYRE", lit(3))
        .when(col("status") == "SNNEVRO", lit(4))
        .when(col("status") == "SNMOLP", lit(5))
        .otherwise(lit(0)).cast("int").alias("lab_ref")
    )
    return new_events

sect_base = base.filter(col('eventcategory') == 'SECT')

work = sect_base.select(
    'tokenid',
    'eventid',
    'requisitionid',
    'status',
    'happened_at',
    'username',
    'workstation',
)

# Build per-user sigma_table with average time between stops
w = Window.partitionBy("username").orderBy("happened_at")
sigma_table = work.filter(col("eventid") == "STOP").withColumn(
    "happened_at_next", lead("happened_at").over(w)
).withColumn(
    "username_next", lead("username").over(w)
).filter(
    col("username_next").isNotNull() & (col("username_next") == col("username"))
).withColumn(
    "duration", (col("happened_at_next").cast("long") - col("happened_at").cast("long"))
).groupBy("username").agg(
    mean("duration").alias("duration_mean"),
    first("status", ignorenulls=True).alias("status")
).withColumn("row_index", col("username").cast("int"))

# Convert table to list of rows
rows = sigma_table.collect()

# Collect translated events from each user
to_append = []

for r in rows:
   act = r["username"]
   delta = r["duration_mean"]
   seksjon = r["status"]

   if delta is None or seksjon is None:
       continue

   delta_td = timedelta(seconds=delta)

   # If execution is fast but not unrealistic, double the estimate
   if timedelta(seconds=30) <= delta_td < timedelta(minutes=15):
       delta_td *= 2

   # Molpat is excluded from translation
   if seksjon == "SNMOLP":
       continue
   elif delta_td < timedelta(seconds=30):
       # Very fast work — assume all items are scanned, not cut yet
       new_events = _translate_too_quick(work, act)
       if new_events.count() > 0:
           to_append.append(new_events)
   elif seksjon in {"SNHIST", "SNHISTSTOR"}:
       # Histology — start might be ambiguous
       new_events = _make_sectioning_group_mappings(act, delta_td, work, True)
       if new_events.count() > 0:
           to_append.append(new_events)
   else:
       # Other departments
       new_events = _make_sectioning_group_mappings(act, delta_td, work, False)
       if new_events.count() > 0:
           to_append.append(new_events)

# Merge all translated event blocks into one dataframe
if len(to_append) > 1:
    new_events = to_append[0]
    for df in to_append[1:]:
        new_events = new_events.unionByName(df)
elif len(to_append) == 1:
    new_events = to_append[0]
else:
    print("WARN: there were no sectioning events translated!")
    new_events = None

# Append to final output
finished_df = finished_df.unionByName(new_events)


##################################### 70: staining #####################################

farge_base = base.filter(col('eventcategory') == 'FARG')

aut_stain_fin = farge_base.filter(
    col("status") == "IN-TTHIST"
).select(
    lit(70).alias("event_name"),           
    lit(2).alias("event_type"),            
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(3).cast("int").alias("token_type"), 
    lit(2).cast("int").alias("revision"),
    col("username"),
    col("workstation").alias("workstation"),
    lit(0).cast("int").alias("lab_ref")      
)

finished_df = finished_df.unionByName(aut_stain_fin)

# automatic staining start events created 25 minutes before stop
aut_stain_starts = farge_base.filter(
    col("status") == "IN-TTHIST"
).select(
        lit(70).alias("event_name"), 
        lit(1).cast(IntegerType()).alias("event_type"),  
        (col("happened_at") - expr("INTERVAL 25 MINUTES")).cast(TimestampType()).alias("happened_at"),
        col("requisitionid"),
        col("requisitionid").alias("token_id"),
        lit(3).cast(IntegerType()).alias("token_type"),  
        lit(2).cast(IntegerType()).alias("revision"),
        col("username"),
        col("workstation"),
        lit(0).cast(IntegerType()).alias("lab_ref") 
)

finished_df = finished_df.unionByName(aut_stain_starts)

### 71 : manual staining
man_stains = farge_base.filter(
    col("status") == "FARGE"
).select(
    lit(71).alias("event_name"),           
    lit(2).alias("event_type"),          
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(3).cast("int").alias("token_type"),  
    lit(2).cast("int").alias("revision"),
    col("username"),
    col("workstation"),
    lit(0).cast("int").alias("lab_ref")      
)

finished_df = finished_df.unionByName(man_stains)


### 72 : IHC staining
ihc_stains = farge_base.filter(
    col("status").isin(["IN_DAKO_HBE", "IN_BENCH"])
).select(
    lit(72).alias("event_name"),  
    when(col("eventid") == "START", lit(1)).otherwise(lit(2)).cast("int").alias("event_type"),
    col("happened_at"),
    col("requisitionid"),
    col("tokenid").alias("token_id"),
    lit(3).cast("int").alias("token_type"),  
    lit(2).cast("int").alias("revision"),
    col("username"),
    col("workstation").alias("workstation"),
    lit(2).cast("int").alias("lab_ref")     
)

finished_df = finished_df.unionByName(ihc_stains)

##################################### 85 : Scanning #####################################
scan_base = base.filter(col("eventcategory") == "SKAN")

scan_finished = (
    scan_base.select(
        lit(85).alias("event_name"),  # scanning
        lit(2).cast(IntegerType()).alias("event_type"),  # activity stop
        col("happened_at"),
        col("requisitionid"),
        col("requisitionid").alias("token_id"),
        lit(3).cast(IntegerType()).alias("token_type"),  # slide
        lit(2).cast(IntegerType()).alias("revision"),
        col("username"),
        col("workstation"),
        lit(0).cast(IntegerType()).alias("lab_ref")  # histology
    )
)

finished_df = finished_df.unionByName(scan_finished)

# duration of scanning set to 30 minutes
scan_starts = (
    scan_base.select(
        lit(85).alias("event_name"), 
        lit(1).cast(IntegerType()).alias("event_type"), 
        (col("happened_at") - expr("INTERVAL 30 MINUTES")).cast(TimestampType()).alias("happened_at"),
        col("requisitionid"),
        col("requisitionid").alias("token_id"),
        lit(3).cast(IntegerType()).alias("token_type"), 
        lit(2).cast(IntegerType()).alias("revision"),
        col("username"),
        col("workstation"),
        lit(0).cast(IntegerType()).alias("lab_ref")  
    )
)

finished_df = finished_df.unionByName(scan_starts)

##################################### 80(81) : Case assignment #####################################
disp_base = base.filter(
    col("eventid").isin(["PAT_DAN", "PAT_EAN"])
)

disp_base = disp_base.select(
    col("requisitionid"),
    col("happened_at"),
    col("username").alias("dispatcher"), # actor who assigned the case
    col("info").alias("dispatchee") # To who the case was assigned
)

# Group by requisition and dispatchee to get the earliest dispatch per recipient
disp_base = disp_base.groupBy("requisitionid", "dispatchee").agg(
    min("happened_at").alias("happened_at"),
    first("dispatcher", ignorenulls=True).alias("dispatcher")
)

# event log entry for first dispatch per requisition
to_append = disp_base.groupBy("requisitionid").agg(
    first("happened_at", ignorenulls=True).alias("happened_at"),
    first("dispatcher", ignorenulls=True).alias("dispatcher")
).select(
    lit(80).alias("event_name"),
    lit(0).alias("event_type"),
    col("happened_at"),
    col("requisitionid"),
    col("requisitionid").alias("token_id"),
    lit(0).cast("int").alias("token_type"),
    lit(2).cast("int").alias("revision"),
    col("dispatcher").alias("username"),
    lit(None).cast("int").alias("workstation"),
    lit(None).cast("int").alias("lab_ref")
)

finished_df = finished_df.unionByName(to_append).orderBy("happened_at")

reassignments = disp_base

to_append = reassignments.select(
    lit(81).alias('event_name'),
    lit(0).alias('event_type'),
    col('happened_at'),
    col('requisitionid'),
    col('requisitionid').alias('token_id'),
    lit(0).alias('token_type'), 
    lit(2).alias('revision'),
    col('dispatcher').alias('username'),
    lit(None).alias('workstation'),
    lit(None).alias('lab_ref')
)

to_append = to_append.withColumn("event_type", col("event_type").cast("int"))

finished_df = finished_df.unionByName(to_append).orderBy("happened_at")

# fix type issue specific to spark
reassignments_fixed = reassignments.select(
    lit(81).cast("int").alias('event_name'),
    lit(0).cast("int").alias('event_type'),
    col('happened_at').cast("timestamp"),
    col('requisitionid').cast("decimal(20,0)"),
    col('requisitionid').cast("decimal(20,0)").alias('token_id'),
    lit(0).cast("int").alias('token_type'),
    lit(2).cast("int").alias('revision'),
    col('dispatcher').cast("decimal(20,0)").alias('username'),
    lit(None).cast("void").alias('workstation'),
    lit(None).cast("void").alias('lab_ref')
)

to_append = to_append.select(
    col("event_name").cast("int"),
    col("event_type").cast("int"),
    col("happened_at").cast("timestamp"),
    col("requisitionid").cast("decimal(20,0)"),
    col("token_id").cast("decimal(20,0)"),
    col("token_type").cast("int"),
    col("revision").cast("int"),
    col("username").cast("decimal(20,0)"),
    lit(None).cast("void").alias("workstation"),
    lit(None).cast("void").alias("lab_ref") 
)

finished_df = finished_df.unionByName(to_append).orderBy("happened_at")

finished_df.show()
finished_df = finished_df.repartition("requisitionid")

print(f"Full translation execution time: {timemodule.time() - start_time}")

### Query
time = timemodule.time()

finished_df.filter(col("requisitionid") == "17060286647797133986").show()

print(f"Query execution time: {timemodule.time() - time}")