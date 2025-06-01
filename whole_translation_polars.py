import polars as pl
from pathlib import Path
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from datetime import datetime

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
access_df = pl.scan_parquet("access.parquet").collect()

# Combine date and time columns into a single timestamp column
events_df = combine_date_and_time_in_df(events_df, 'eventdate', 'eventtime', 'happened_at')
access_df = combine_date_and_time_in_df(access_df, 'eventdate', 'eventtime', 'happened_at')

base = events_df

##################################### 20 : Accessioning #####################################

to_append_fin = base.filter((pl.col('eventid') == 'AO') | ((pl.col('eventid') == 'ERF') & (pl.col('eventcategory') == '2'))).select(
    pl.lit(20).alias('event_name'),
    pl.lit(2).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'),
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(None).cast(pl.Int32).alias('lab_ref')
)

finished_df = to_append_fin

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

finished_df = pl.concat([finished_df, to_append_start]).sort("happened_at")

##################################### 30 : Grossing #####################################

makros = base.filter(pl.col('eventcategory') == '934')
first_makros = makros.filter((pl.col('eventid') == 'RE') & (pl.col('info') == 'MAKRO'))
nymakros_base = makros.filter((pl.col('eventid') == 'RE') & (pl.col('info') == 'NYMAKRO')).select('requisitionid', 'username', 'workstation')

nymakros = nymakros_base.join(
    makros.group_by(['requisitionid', 'username']).agg(pl.col('happened_at').max()),
    on=['requisitionid', 'username']
)

grossing_fin = pl.concat(
    [nymakros,  first_makros.select('requisitionid', 'username', 'workstation', 'happened_at')]
)

to_append_fin = grossing_fin.select(
    pl.lit(30).alias('event_name'),
    pl.lit(2).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, to_append_fin])

to_append_start = access_df.join(grossing_fin, on=['requisitionid', 'username']).filter(
    (pl.col('happened_at_right') > pl.col('happened_at'))
).group_by('requisitionid').agg(
    pl.col('username').first(),
    pl.col('happened_at').min()
).select(
    pl.lit(30).alias('event_name'), 
    pl.lit(1).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.lit(None).cast(pl.String).alias('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref')
)


finished_df = pl.concat([finished_df, to_append_start])

### Restmaterial - Archive (31+32)

to_append_archived = base.filter(pl.col('eventid') == 'ARCH').filter(pl.col('info').str.contains(r"\S+ \S+ \d+ RESTMAT")).group_by(['requisitionid', 'username']).agg(pl.col('happened_at').max()).select(
    pl.lit(31).alias('event_name'),
    pl.lit(0).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'),
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.lit(None).cast(pl.String).alias('workstation'),
    pl.lit(None).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, to_append_archived])

to_append_retrieved = base.filter(pl.col('eventid') == 'DELA').group_by(['requisitionid', 'username']).agg(pl.col('happened_at').min()).select(
    pl.lit(32).alias('event_name'),
    pl.lit(0).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.lit(None).cast(pl.String).alias('workstation'),
    pl.lit(None).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, to_append_retrieved])

##################################### 40 : Processing #####################################

proc = base.filter((pl.col('eventcategory') == 'FREM') & (pl.col('eventid') == 'START'))

proc_base = proc.sort('happened_at').group_by(['requisitionid', 'tokenid']).agg(
    pl.col('happened_at').last(),
    pl.col('status').last(),
    pl.col('username').last(),
    pl.col('workstation').last(),
).with_columns(
    (((pl.col('status').str.extract(r"(\d+(,\d+)?) timer.*", 1)).str.replace_all(',', '.')).str.to_decimal() * 60).cast(pl.Int64).alias('to_add')
)

to_append_start = proc_base.select(
    pl.lit(40).alias('event_name'), 
    pl.lit(1).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, to_append_start])

to_append_fin = proc_base.select(
    pl.lit(40).alias('event_name'), 
    pl.lit(2).alias('event_type'), 
    pl.col('happened_at') + pl.duration(minutes="to_add"),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
).with_columns(
    pl.col('happened_at').dt.cast_time_unit('ns')
)


finished_df = pl.concat([finished_df, to_append_fin])

### 41 : Decalcination
dekal = base.filter(pl.col('eventcategory') == "DEKAL").select(
    'requisitionid',
    'eventid',
    'username',
    'workstation',
    'happened_at'
).select(
    pl.lit(41).alias('event_name'),
    pl.when(pl.col('eventid') == 'START').then(pl.lit(1)).otherwise(pl.lit(2)).cast(pl.Int32).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

to_append = dekal.select(
    pl.lit(41).alias('event_name'), 
    pl.lit(2).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref')
)

finished_df = pl.concat([finished_df, to_append])

dekal = base.filter(pl.col('eventcategory') == "DEKAL").filter(pl.col('eventid') == 'START').select(
    'requisitionid',
    'username',
    'workstation',
    'happened_at',
)

to_append = dekal.select(
    pl.lit(41).alias('event_name'),
    pl.lit(1).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, to_append])

##################################### 50 (51, 59) : Embedding #####################################
emb_base = base.filter(pl.col('eventcategory').is_in({"STØP", 'KOORD',"IN_ATEK2_HBE" }))

unknown_blocks = emb_base.select(
    pl.col('requisitionid').alias('element_id'),
    pl.lit(24).cast(pl.Int32).alias('issue_id'), 
    pl.lit(2).cast(pl.Int32).alias('stage_no'),
    pl.lit(datetime.now()).dt.cast_time_unit('ns').alias('trafo_ts'),
    pl.col('happened_at').dt.replace_time_zone(None).alias('event_ts'),
    pl.concat_str([pl.lit('Activity: 50/51/59; Token: '), pl.col('tokenid')]).alias('details')
)

### 59 : koordinering
to_add_coord = emb_base.filter(
    (pl.col('eventid') == 'STOP') & (pl.col('eventcategory') == 'KOORD')
).sort('happened_at').group_by('tokenid').agg(
    pl.col('requisitionid').last(),
    pl.col('username').last(),
    pl.col('workstation').last(),
    pl.col('happened_at').last(),
).select(
    pl.lit(59).alias('event_name'), 
    pl.lit(0).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, to_add_coord])

### 50 : manualembedding
man_embd =  emb_base.filter((pl.col('eventcategory').is_in({'STØP', 'IN_ATEK2_HBE'})) & (pl.col('status') == 'STØP')).select(
    pl.lit(50).alias('event_name'),
    pl.when(pl.col('eventid') == 'START').then(pl.lit(1)).otherwise(pl.lit(2)).cast(pl.Int32).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, man_embd])

### 51 : automaticembedding
emb_base.filter(
    (pl.col('eventcategory').is_in({'STØP', 'IN_ATEK2_HBE'})) & (~(pl.col('status').is_in({'STØP'})))
)
aut_embd = emb_base.filter(
    (pl.col('eventcategory').is_in({'STØP', 'IN_ATEK2_HBE'})) & (~(pl.col('status').is_in({'STØP'})))
)

aut_embd_base = emb_base.filter((pl.col('eventcategory').is_in({'STØP', 'IN_ATEK2_HBE'})) & (~(pl.col('status').is_in({'STØP'}))))

aut_embd_starts = aut_embd_base.filter(pl.col('eventid') == 'START').group_by(['requisitionid', 'tokenid']).agg(
    pl.col('happened_at').min(),
    pl.col('username').first(),
    pl.col('workstation').first(),
).select(
    pl.lit(51).alias('event_name'),
    pl.lit(1).cast(pl.Int32).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'),
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref')
)
aut_embd_fins = aut_embd_base.filter(pl.col('eventid') == 'STOP').group_by(['requisitionid', 'tokenid']).agg(
    pl.col('happened_at').max(),
    pl.col('username').last(),
    pl.col('workstation').last(),
).select(
    pl.lit(51).alias('event_name'), 
    pl.lit(2).cast(pl.Int32).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(2).alias('token_type'),
    pl.lit(2).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref')
)
aut_embd = pl.concat([aut_embd_starts, aut_embd_fins])

finished_df = pl.concat([finished_df, aut_embd])

##################################### 60: sectioning #####################################

def _make_sectioning_group_mappings(actor_ref: int, two_sigma: timedelta, work: pl.DataFrame, open_start: bool) -> pl.DataFrame:
    grouped = work.filter(pl.col('username') == actor_ref).filter(pl.col('eventid') == 'STOP').sort('happened_at').with_columns(
        pl.col('happened_at').shift(-1).alias('happened_at_next'),
    ).with_columns(
        (pl.col('happened_at_next') - pl.col('happened_at')).alias('delta')
    ).with_columns(
        (pl.col("delta") > two_sigma).alias("is_new_group")
    ).with_columns(
        pl.col('is_new_group').cum_sum().alias('group')
    ).with_columns(
        pl.col('group').shift(1)
    ).with_columns(
        pl.col('group').fill_null(0)
    )
    if open_start:
        prelim = grouped.group_by('group').agg(
            pl.col('happened_at').min().alias('start'),
            pl.col('happened_at').max().alias('stop'),
            pl.col('requisitionid').count().alias('count')
        ).with_columns(
            (pl.col('stop') - pl.col('start')).alias('time_taken')
        ).with_columns(
            (pl.col('time_taken') / pl.col('count')).alias('per_item')
        ).sort('group').with_columns(
            pl.col('start').shift(-1).alias('start_next'),
        ).with_columns(
            (pl.col('start_next') - pl.col('stop')).alias('delta')
        ).with_columns(
            pl.col('delta').shift(1)
        ).with_columns(
            pl.col('time_taken') + pl.col('delta')
        ).with_columns(
            (pl.col('time_taken') / pl.col('count')).alias('per_item_new')
        )
    else:
        prelim = grouped.group_by('group').agg(
            pl.col('happened_at').min().alias('start'),
            pl.col('happened_at').max().alias('stop'),
            pl.col('requisitionid').count().alias('count')
        ).with_columns(
            (pl.col('stop') - pl.col('start')).alias('time_taken')
        ).with_columns(
            (pl.col('time_taken') / pl.col('count')).alias('per_item')
        ).sort('group').with_columns(
            pl.col('stop').shift(1).alias('stop_prev'),
        ).with_columns(
            (pl.col('start') -  pl.col('stop_prev')).alias('delta')
        ).with_columns(
            pl.col('delta').shift(-1)
        ).with_columns(
            pl.col('time_taken') + pl.col('delta')
        ).with_columns(
            (pl.col('time_taken') / pl.col('count')).alias('per_item_new')
        )

    if len(prelim) == 1:
        final_groups = prelim.select('group', 'start', 'stop')
    else:
        x = prelim.select(pl.col('per_item_new').quantile(0.5, 'lower')).to_series().to_list()[0]
        x = x.total_seconds()
        x = round(x)
        x = min(x, 1800) # setting a cut-off at 1800s (i.e. 30min --> one should not spend more than half an hour on a block)
        if open_start:
            final_groups = prelim.with_columns(
                pl.col('stop').shift(1).alias('start_next'),
                (pl.lit(x) * pl.col('count')).alias('secs')
            ).with_columns(
                (pl.col('stop') - pl.duration(seconds=pl.col('secs'))).dt.cast_time_unit('ns').alias('alt_start')
            ).select(
                'group',
                pl.when(pl.col('start_next').is_not_null()).then(pl.col('start_next')).otherwise(pl.col('alt_start')).alias('start'),
                'stop'
            )
        else:
            final_groups = prelim.with_columns(
                pl.col('stop').shift(-1).alias('stop_new'),
                (pl.lit(x) * pl.col('count')).alias('secs')
            ).with_columns(
                (pl.col('start') + pl.duration(seconds=pl.col('secs'))).dt.cast_time_unit('ns').alias('alt_stop')
            ).select(
                'group',
                 'start',
                pl.when(pl.col('stop_new').is_not_null()).then(pl.col('stop_new')).otherwise(pl.col('alt_stop')).alias('stop'),
            )
    starts = grouped.join(final_groups, on='group').select(
        pl.lit(60).alias('event_name'), 
        pl.lit(1).cast(pl.Int32).alias('event_type'),
        pl.col('start').alias('happened_at'),
        pl.col('requisitionid'),
        pl.col('requisitionid').alias('token_id'),
        pl.lit(2).alias('token_type'),
        pl.lit(2).alias('revision'),
        pl.col('username'),
        pl.col('workstation'),
        pl.when(
            pl.col('status') == 'SNIMM'
        ).then(pl.lit(2)).when(
            pl.col('status') == 'SNNYRE'
        ).then(pl.lit(3)).when(
            pl.col('status') == 'SNNEVRO'
        ).then(pl.lit(4)).when(
            pl.col('status') == 'SNMOLP'
        ).then(pl.lit(5)).otherwise(pl.lit(0)).cast(pl.Int32).alias('lab_ref')
    )
    stops = grouped.join(final_groups, on='group').select(
        pl.lit(60).alias('event_name'),
        pl.lit(2).cast(pl.Int32).alias('event_type'),
        pl.col('stop').alias('happened_at'),
        pl.col('requisitionid'),
        pl.col('requisitionid').alias('token_id'),
        pl.lit(2).alias('token_type'),
        pl.lit(2).alias('revision'),
        pl.col('username'),
        pl.col('workstation'),
        pl.when(
            pl.col('status') == 'SNIMM'
        ).then(pl.lit(2)).when(
            pl.col('status') == 'SNNYRE'
        ).then(pl.lit(3)).when(
            pl.col('status') == 'SNNEVRO'
        ).then(pl.lit(4)).when(
            pl.col('status') == 'SNMOLP'
        ).then(pl.lit(5)).otherwise(pl.lit(0)).cast(pl.Int32).alias('lab_ref')
    )
    if len(starts) > 0 and len(stops) > 0:
        return pl.concat([starts, stops])
    else:
        return starts # otherwise it is empty

def _translate_too_quick(work: pl.DataFrame, act: int) -> pl.DataFrame:
    new_events = work.filter(pl.col('username') == act).filter(pl.col('eventid') == 'STOP').select(
        pl.lit(60).alias('event_name'),
        pl.lit(1).cast(pl.Int32).alias('event_type'),
        pl.col('happened_at'),
        pl.col('requisitionid'),
        pl.col('requisitionid').alias('token_id'),
        pl.lit(2).alias('token_type'), 
        pl.lit(2).alias('revision'),
        pl.col('username'),
        pl.col('workstation'),
        pl.when(
            pl.col('status') == 'SNIMM'
        ).then(pl.lit(2)).when(
            pl.col('status') == 'SNNYRE'
        ).then(pl.lit(3)).when(
            pl.col('status') == 'SNNEVRO'
        ).then(pl.lit(4)).when(
            pl.col('status') == 'SNMOLP'
        ).then(pl.lit(5)).otherwise(pl.lit(0)).cast(pl.Int32).alias('lab_ref')
    )
    return new_events

sect_base = base.filter(pl.col('eventcategory') == 'SECT')

# should be empty
unknown = sect_base.unique('tokenid').select(
                pl.col('requisitionid').alias('element_id'),
                pl.lit(24).cast(pl.Int32).alias('issue_id'),
                pl.lit(2).cast(pl.Int32).alias('stage_no'),
                pl.lit(datetime.now()).dt.cast_time_unit('ns').alias('trafo_ts'),
                pl.col('happened_at').dt.replace_time_zone(None).alias('event_ts'),
                pl.concat_str([pl.lit('Activity: 60; Token: '), pl.col('requisitionid')]).alias('details')
)

work = sect_base.select(
    'eventid',
    'requisitionid',
    'status',
    'happened_at',
    'username',
    'workstation',
)

# sigma table is the central part of the algorithm
sigma_table = work.filter(pl.col('eventid') == 'STOP').sort(['username', 'happened_at']).with_columns(
    pl.col('happened_at').shift(-1).alias('happened_at_next'),
    pl.col('username').shift(-1).alias('username_next')
).filter((pl.col('username_next').is_not_null()) & (pl.col('username_next') == pl.col('username'))).with_columns(
    (pl.col('happened_at_next') - pl.col('happened_at')).alias('duration')
).group_by('username').agg(
    pl.col('duration').mean(),
    pl.col('status').first()
)
sigma_table.with_row_index()

# act, delta, work are from the sigma_table
n = 2
act = sigma_table.select('username').to_series().to_list()[n]
delta = sigma_table.select('duration').to_series().to_list()[n]
department = sigma_table.select('status').to_series().to_list()[n]

# histology (SNHIST, SNHISTSTOR)
new_events = _make_sectioning_group_mappings(act, delta, work, True)

# other lab divisions (SNIMM, SNNYRE, ...)
new_events = _make_sectioning_group_mappings(act, delta, work, False)

# if is too quick (i.e. delta smaller than 30 seconds)
new_events = _translate_too_quick(work, act)

to_append = []
for r in sigma_table.iter_rows():
    act = r[0]
    delta = r[1]
    seksjon = r[2]
    if delta >= timedelta(seconds=30) and delta < timedelta(minutes=15):
        delta *= 2
    if seksjon == 'SNMOLP':
        continue # Skip molpat
    elif delta < timedelta(seconds=30):
        # those who are too quick, scan all the blocks and cut them later
        new_events = _translate_too_quick(work, act)
        if len(new_events) > 0:
            to_append.append(new_events)
    elif seksjon in {'SNHIST', 'SNHISTSTOR'}:
        new_events = _make_sectioning_group_mappings(act, delta, work, True)
        if len(new_events) > 0:
            to_append.append(new_events)
    else:
        new_events = _make_sectioning_group_mappings(act, delta, work, False)
        if len(new_events) > 0:
            to_append.append(new_events)


if len(to_append) > 1:
    new_events = pl.concat(to_append)
elif len(to_append) == 1:
    new_events = to_append[0]
else:
    print("WARN there were not sectioning events translated ?!")

finished_df = pl.concat([finished_df, new_events])

##################################### 70: staining #####################################

farge_base = base.filter(pl.col('eventcategory') == 'FARG')

# hopefully empty
missing = farge_base.select(
    pl.col('requisitionid').alias('element_id'),
    pl.lit(25).cast(pl.Int32).alias('issue_id'), 
    pl.lit(2).cast(pl.Int32).alias('stage_no'),
    pl.lit(datetime.now()).dt.cast_time_unit('ns').alias('trafo_ts'),
    pl.col('happened_at').dt.replace_time_zone(None).alias('event_ts'),
    pl.concat_str([pl.lit('Activity: 70; Token: '), pl.col('tokenid')]).alias('details')
)

aut_stain_fin = farge_base.filter(pl.col('status') == 'IN-TTHIST').select(
    pl.lit(70).alias('event_name'), 
    pl.lit(2).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(3).cast(pl.Int32).alias('token_type'), 
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, aut_stain_fin])

aut_stain_starts = farge_base.filter(pl.col('status') == 'IN-TTHIST').select(
    pl.lit(70).alias('event_name'), 
    pl.lit(1).alias('event_type'), 
    ((pl.col('happened_at') - pl.duration(minutes=25)).cast(pl.Datetime("ns", "UTC"))).alias('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(3).cast(pl.Int32).alias('token_type'),
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)
finished_df = pl.concat([finished_df, aut_stain_starts])

### 71 : manual staining
man_stains = farge_base.filter(pl.col('status') == 'FARGE').select(
    pl.lit(71).alias('event_name'), 
    pl.lit(2).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(3).cast(pl.Int32).alias('token_type'), 
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref')
)

finished_df = pl.concat([finished_df, man_stains])

### 72 : IHC staining
ihc_stains = farge_base.filter(pl.col('status').is_in({'IN_DAKO_HBE', 'IN_BENCH'})).select(
    pl.lit(72).alias('event_name'), 
    pl.when(pl.col('eventid') == 'START').then(pl.lit(1)).otherwise(pl.lit(2)).cast(pl.Int32).alias('event_type'), # activity stop
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(3).cast(pl.Int32).alias('token_type'), 
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(2).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, ihc_stains])

##################################### 85 : Scanning #####################################

scan_base = base.filter(pl.col('eventcategory') == 'SKAN')

scan_finished = scan_base.select(
    pl.lit(85).alias('event_name'),
    pl.lit(2).alias('event_type'), 
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(3).cast(pl.Int32).alias('token_type'),
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref') 
)

finished_df = pl.concat([finished_df, scan_finished])

scan_starts = scan_base.select(
    pl.lit(85).alias('event_name'), 
    pl.lit(1).alias('event_type'), 
    ((pl.col('happened_at') - pl.duration(minutes=30)).cast(pl.Datetime("ns", "UTC"))).alias('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(3).cast(pl.Int32).alias('token_type'), 
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('username'),
    pl.col('workstation'),
    pl.lit(0).cast(pl.Int32).alias('lab_ref')  
)

finished_df = pl.concat([finished_df, scan_starts])

##################################### 80(81) : Case assignment #####################################

disp_base = base.filter(
    pl.col('eventid').is_in({'PAT_DAN', 'PAT_EAN'})
)

disp_base = disp_base.select(
    pl.col('requisitionid'),
    pl.col('happened_at'),
    pl.col('username').alias('dispatcher'),
    pl.col('info').alias('dispatchee'),
)

disp_base = disp_base.sort('happened_at').group_by(['requisitionid', 'dispatchee']).agg(
    pl.col('happened_at').min(),
    pl.col('dispatcher').first(),
)

to_append = disp_base.sort('happened_at').group_by('requisitionid').agg(
    pl.col('happened_at').first(),
    pl.col('dispatcher').first(),
).select(
    pl.lit(80).alias('event_name'),
    pl.lit(0).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).cast(pl.Int32).alias('token_type'), # case
    pl.lit(2).cast(pl.Int32).alias('revision'),
    pl.col('dispatcher').alias('username'),
    pl.lit(None).alias("workstation"),
    pl.lit(None).cast(pl.Int32).alias('lab_ref')
).with_columns(pl.col('happened_at').dt.cast_time_unit('ns'))

finished_df = pl.concat([finished_df, to_append])

to_append_worklist = disp_base.select(
    pl.col('requisitionid'),
    pl.col('dispatchee').alias('username'),
    pl.col('happened_at').alias('valid_from'),
    pl.lit(None).cast(pl.Datetime).alias('valid_until'),
    pl.lit(0).cast(pl.Int32).alias('in_role') # Lege
)

combined_base = disp_base

reassignments = combined_base

to_append = reassignments.select(
    pl.lit(81).alias('event_name'),
    pl.lit(0).alias('event_type'),
    pl.col('happened_at'),
    pl.col('requisitionid'),
    pl.col('requisitionid').alias('token_id'),
    pl.lit(0).alias('token_type'), 
    pl.lit(2).alias('revision'),
    pl.col('dispatcher').alias('username'),
    pl.lit(None).alias('workstation'),
    pl.lit(None).alias('lab_ref')
)

finished_df = pl.concat([finished_df, to_append])

to_append = pl.concat([
    to_append.join(reassignments, on=["requisitionid", "happened_at"], how="anti"),
    reassignments.select(
        pl.lit(81).alias('event_name'),
        pl.lit(0).alias('event_type'),
        pl.col('happened_at'),
        pl.col('requisitionid'),
        pl.col('requisitionid').alias('token_id'),
        pl.lit(0).alias('token_type'), 
        pl.lit(2).alias('revision'),
        pl.col('dispatcher').alias('username'),
        pl.lit(None).alias('workstation'),
        pl.lit(None).alias('lab_ref')
    )
])

finished_df = pl.concat([finished_df, to_append])

print(f"Full translation execution time: {datetime.now() - start_time}")

### Query
time = datetime.now()

df = finished_df.filter(pl.col("requisitionid") == 17060286647797133986).sort('happened_at')

print(f"Query execution time: {datetime.now() - time}")