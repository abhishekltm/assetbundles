from pyspark import pipelines as dp
from procurement.lib import config_loader
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# env = spark.conf.get("env", "dev")
env_cfg, table_cfg, dq_cfg, schema_cfg = config_loader.load_entity_metadata("lfa1", env="dev")

catalog = table_cfg["catalog"]
bronze_schema = table_cfg["schemas"]["bronze"]
bronze_table = f"{catalog}.{bronze_schema}.{table_cfg['tables']['bronze_table']}"

@dp.table(
    name=bronze_table,
    comment="Bronze LFA1 - Vendor raw ingestion"
)
def bronze_lfa1():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", table_cfg["format"])
            .option("header", True)
            .load(f'{env_cfg["source"]["path"]}/lfa1')
            .withColumn("_ingest_ts", current_timestamp())
    )