from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit
from procurement_etl.lib.utilities import load_metadata, load_active_entities

# Load configuration values from Spark config and metadata
source_path = spark.conf.get("volume_path")
catalog = spark.conf.get("catalog")

def _register_bronze(entity):
    bronze_cfg = load_metadata(entity, "bronze")
    bronze_table = f"{catalog}.{bronze_cfg["target"]["schema"]}.{bronze_cfg["target"]["table"]}"
    schema_location = f"{source_path}/_schemas/{bronze_cfg["source"]["entity"]}"
    schema_evolution_mode = bronze_cfg["options"]["schema_evolution_mode"]
    infer_column_types = bronze_cfg["options"]["infer_column_types"]
    header = bronze_cfg["source"]["header"]

    @dp.table(
        name= bronze_table,
        comment="Bronze table ingestion",
        table_properties = {
            "quality": "bronze",
            "layer": "bronze",
            "pipelines.autoOptimize.managed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }
    )

    def bronze_table():
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", bronze_cfg["source"]["format"])
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
                .option("cloudFiles.inferColumnTypes", infer_column_types)
                .option("header", header)
                .load(f'{source_path}/{bronze_cfg["source"]["sub_folder"]}')
                .withColumn("bronze_ingest_ts", current_timestamp())
                .withColumn("_entity", lit(bronze_cfg["source"]["entity"]))
            )
for _entity in load_active_entities("bronze"):
    _register_bronze(_entity)