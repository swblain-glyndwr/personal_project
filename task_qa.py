import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (
    JobParser, assert_pk, get_table_pk_cols, map_tbl, post_to_webhook)
from pyspark.sql import functions as F
from pyspark.sql import Window


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

LOCATIONS = cfg["locations"]

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][job_env]
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}
ASSIGNMENTS_TABLE_LATEST = map_tbl(tbls["assignments_latest"], **tbl_args)
CELLS_TABLE_LATEST = map_tbl(tbls["customer_cells_latest"], **tbl_args)

FALLOW_TRUE = cfg["fallow_control"]["true_label"]
FIXED_CELLS = cfg["fixed_cells"]

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

df_assigned = get_spark().table(ASSIGNMENTS_TABLE_LATEST)
df_cells = get_spark().table(CELLS_TABLE_LATEST)


log.info('Checking for invalid Homepage Teaser assignments')

teaser_locs = ['PH3', 'PH4', 'PH5']
teaser_locs_fmt = ["'" + tl + "'" for tl in teaser_locs]
w_acc = Window.partitionBy('AccountNumber')

df_invalid_teasers = (
    df_assigned
    .where(F.col('Location').isin(teaser_locs))
    .withColumn(
        'TeaserAssigned',
        F.when(F.col('MASID').endswith('_Z'), F.lit(0)).otherwise(F.lit(1))
        )
    .withColumn('TeasersAssigned', F.sum('TeaserAssigned').over(w_acc))
    .drop('TeaserAssigned')
    .withColumn('MASIDToken', F.split('MASID', '_')[1])
    .withColumn('TokenSet', F.collect_set(F.col('MASIDToken')).over(w_acc))
    .withColumn('UniqueTokens', F.array_size('TokenSet'))
    .where(
        (F.col('TeasersAssigned') < len(teaser_locs))
        | (F.col('UniqueTokens') < len(teaser_locs))
        )
    .where(F.col('TokenSet') != F.array(F.lit('Z')))
)

if df_invalid_teasers.count() > 0:

    df_invalid_teaser_accounts = (
        df_invalid_teasers
        .select('AccountNumber')
        .distinct()
    )

    n_it = df_invalid_teaser_accounts.count()
    msg_it = f'{n_it:,} accounts found with invalid HomePage Teasers'
    log.warning(msg_it)
    if job_env == "prod":
        post_to_webhook(WEBHOOK_URL, msg_it)

    df_invalid_teaser_accounts.createOrReplaceTempView("df_it_accs")
    sql_del_invalid = f'''
    delete from {ASSIGNMENTS_TABLE_LATEST}
    where AccountNumber in (select AccountNumber from df_it_accs)
    and Location in ({', '.join(teaser_locs_fmt)})
    '''
    msg_it_rm = (
        'Removing Teaser assignments for affected accounts ' +
        f'from table read by PF: {ASSIGNMENTS_TABLE_LATEST}')
    log.warning(msg_it_rm)
    if job_env == "prod":
        post_to_webhook(WEBHOOK_URL, msg_it_rm)
    get_spark().sql(sql_del_invalid)


df_assigned_dt = (df_assigned.select("rundate").distinct())
df_cells_dt = (df_cells.select("rundate").distinct())
assigned_dts = [x[0] for x in df_assigned_dt.collect()]
cells_dts = [x[0] for x in df_cells_dt.collect()]

assert len(assigned_dts) == 1, f"Multiple dates in {ASSIGNMENTS_TABLE_LATEST}"
assert len(cells_dts) == 1, f"Multiple dates in {CELLS_TABLE_LATEST}"
assert assigned_dts == cells_dts


log.info("Checking integrity of Fallow Control")
df_assignments_w_cells = (
    df_assigned.join(df_cells, on="AccountNumber", how="inner")
    )

df_fallow_with_ads = (
    df_assignments_w_cells
    .where(F.col("FallowControl") == FALLOW_TRUE)
    .where(F.col("UniqueAdIDAssigned") != "NoAd")
)

ads_in_control = df_fallow_with_ads.count()

assert ads_in_control == 0, "Ads assigned to Fallow Control customers"


log.info("Checking integrity of Local Controls")
local_control_labels = dict()
for fc in FIXED_CELLS:
    for i in FIXED_CELLS[fc]['cells']:
        if 'control' in i['then']['lit'].lower():
            local_control_labels[fc] = i['then']['lit']

lc_to_location = dict()
for local_control in local_control_labels:
    lc_to_location[local_control] = []

for lc, lc_val in local_control_labels.items():
    for location in LOCATIONS:
        for m in LOCATIONS[location]['map']:
            for i in m['when']:
                if i['col'] == lc and i['val'] == lc_val:
                    lc_to_location[lc].append(location)

for lc in lc_to_location:
    for location in lc_to_location[lc]:
        log.info(f'Checking {lc} local control for location {location}')
        df_lc_with_ads = (
            df_assignments_w_cells
            .where(F.col("Location") == location)
            .where(F.col(lc) == local_control_labels[lc])
            .where(F.col("UniqueAdIDAssigned") != "NoAd")
            )
        ads_in_lc = df_lc_with_ads.count()
        assert ads_in_lc == 0, f'Ads assigned to {lc} at location: {location}'


log.info("Checking that all NoAd assignments map to MASID ending _Z")
df_noad_nonz = (
    df_assignments_w_cells
    .where(F.col("UniqueAdIDAssigned") == "NoAd")
    .where(~F.col("MASID").endswith("_Z"))
)
df_noad_nonz_n = df_noad_nonz.count()
assert df_noad_nonz_n == 0, "Non _Z-ending MASIDs found for NoAd assignments"


log.info('Checking Primary Key validity of latest process tables')
# Checking history tables too would progressively increase process runtime
for tbl in tbls:
    if not tbl.endswith('_latest'):
        continue
    tbl_mapped = map_tbl(tbls[tbl], **tbl_args)
    pk_cols = get_table_pk_cols(tbl_mapped)
    log.info(f'Asserting {pk_cols} as PK for {tbl_mapped}')
    df_tbl_pk = get_spark().table(tbl_mapped)
    assert_pk(df_tbl_pk, pk_cols)
