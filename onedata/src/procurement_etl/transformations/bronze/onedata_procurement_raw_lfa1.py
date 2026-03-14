from procurement_etl.lib.config_loader import load_entity_metadata
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

ENV = spark.conf.get("env")
source_path = spark.conf.get("volume_path")
catalog = spark.conf.get("catalog")
configs = load_entity_metadata("lfa1", ENV)
table_cfg = configs["table"]
bronze_schema = table_cfg["schemas"]["bronze"]
bronze_table = f"{catalog}.{bronze_schema}.{table_cfg['tables']['bronze_table']}"

@dp.table(
    name= bronze_table,
    comment="Bronze LFA1 - Vendor raw ingestion"
)
def bronze_lfa1():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", table_cfg["format"])
            .option("cloudFiles.schemaLocation", f"{source_path}/_schemas/lfa1")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            # .option("cloudFiles.inferColumnTypes", True)
            .option("header", True)
            .option("rescuedDataColumn", "_rescued_data")
            .load(f'{source_path}/lfa1')
            .withColumn("_ingest_ts", current_timestamp())
    )