import polars as pl
from datetime import datetime
from pathlib import Path
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

start_time = datetime.now()

pl.Config.set_tbl_rows(18)

TZ = ZoneInfo("Europe/Oslo")
# Function to combine date and time columns into a timestamp column
def combine_date_and_time_in_df(
                                df: pl.DataFrame,
                                date_column_name: str,
                                time_column_name: str,
                                timestamp_column_name: str,
                                source_time_zone: str = "Europe/Oslo",
                                target_time_zone: str = "UTC") -> pl.DataFrame:
    return df.with_columns(
        pl.col(date_column_name).dt.combine(pl.col(time_column_name), "ns").alias(timestamp_column_name)
    ).with_columns(
        pl.col(timestamp_column_name).dt.replace_time_zone(source_time_zone)
    ).with_columns(
        pl.col(timestamp_column_name).dt.convert_time_zone(target_time_zone)
    )

# Read parquet files
events_df = pl.scan_parquet("events.parquet").collect()
access_df = pl.scan_parquet("events.parquet").collect()
# Combine date and time columns into a single timestamp column
events_df = combine_date_and_time_in_df(events_df, 'eventdate', 'eventtime', 'happened_at')
access_df = combine_date_and_time_in_df(access_df, 'eventdate', 'eventtime', 'happened_at')

print(events_df)
print("above is 10 first lines of events dataset of total length " + str(events_df.height))
print(access_df)
print("above is 10 first lines of access dataset of total length " + str(access_df.height))

base = events_df

to_append_fin = base.filter(
    (pl.col('eventid') == 'AO') | ((pl.col('eventid') == 'ERF') & (pl.col('eventcategory') == '2'))
).select(
    pl.lit(20).alias('event_name'), # event_name 20 for accessioning
    pl.lit(2).alias('event_type'), # event_type 2 for finished event
    pl.col('happened_at'), # happened_at is the timestamp of the event
    pl.col('requisitionid'), # requisitionid is the unique identifier for the accession
    pl.col('requisitionid').alias('token_id'), # token_id is the unique identifier of a token, requisitionid as placeholder
    pl.lit(0).alias('token_type'), # token_type 0 for case
    pl.lit(2).alias('revision'), 
    pl.col('username'), # username is the user who performed the event
    pl.col('workstation'), # workstation is the workstation where the event was performed
    pl.lit(None).cast(pl.Int32).alias('lab_ref') # lab_ref is unique identifier of the laboratory
)

finished_df = to_append_fin

print(finished_df)
print("Above is 10 first lines of finished accession added to finished_df with total length " + str(finished_df.height))


a = base.filter(pl.col('eventid') == 'AO').join(access_df, on='requisitionid').filter(
    (pl.col('happened_at') > pl.col('happened_at_right'))
).group_by('requisitionid').agg(
    pl.col('happened_at_right').min().alias('happened_at'),
    pl.col('username_right').first().alias('username')
).with_columns(pl.lit('A').alias('type'))

b = base.filter(pl.col('eventid') == 'AO').select(pl.col('requisitionid'), pl.col('happened_at'), pl.col('username'), pl.lit('B').alias('type'))

c = base.filter(
    (pl.col('eventid') == 'ERF') & (pl.col('eventcategory') == '2')
).select(
    pl.col('requisitionid'),
    pl.col('happened_at'),
    pl.col('username'),
    pl.lit('C').alias('type')
)

together = pl.concat([a, b, c])

print(together)
print("Above is accessioning type A, B and C, with length " + str(together.height))


d = together.sort(['username', 'happened_at']).with_columns(
    pl.col('happened_at').shift(1).alias('n'),
    pl.col('username').shift(1).alias('a'),
    pl.col('type').shift(1).alias('t')
).filter(
    (pl.col('type') == 'C') & (pl.col('username') == pl.col('a'))
).select(
    pl.col('requisitionid'),
    pl.col('n').alias('happened_at'),
    pl.col('username'),
    pl.lit('D').alias('type')
)
together = pl.concat([together, d])

print(together)
print("Above is accessioning type D added, with length " + str(together.height))

to_append_start = together.filter(pl.col('type').is_in(['A', 'D'])).select(
    pl.lit(20).alias('event_name'),
    pl.lit(1).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'),
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.lit(None).cast(pl.String).alias('workstation'),
    pl.lit(None).cast(pl.Int32).alias('lab_ref')
)

print(to_append_start)
print("Showed accessioning start events, with length " + str(to_append_start.height))

finished_df = pl.concat([finished_df, to_append_fin]).sort("happened_at")

print(finished_df)
print("Above is 10 first lines of accession (start + end) added to finished df with total length " + str(finished_df.height))

print(f"Execution time: {datetime.now() - start_time}")