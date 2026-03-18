from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from procurement_etl.lib.metadata_loader import load_metadata, load_active_entities

# Load configuration values from Spark config and metadata
source_path = spark.conf.get("volume_path")
catalog = spark.conf.get("catalog")

def _register_bronze(entity):
    bronze_cfg = load_metadata(entity, "bronze")
    bronze_table = f"{catalog}.{bronze_cfg["table"]["schema"]}.{bronze_cfg["table"]["name"]}"

    @dp.table(
        name= bronze_table,
        comment="Bronze table ingestion"
    )
    def bronze_table():
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", bronze_cfg["source"]["format"])
                .option("cloudFiles.schemaLocation", f"{source_path}/_schemas/{bronze_cfg["source"]["entity"]}")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("header", True)
                .load(f'{source_path}/{bronze_cfg["source"]["entity"]}')
                .withColumn("_ingest_ts", current_timestamp())
            )
for _entity in load_active_entities("bronze"):
    _register_bronze(_entity)