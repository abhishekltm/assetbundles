from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from procurement_etl.lib.metadata_loader import load_metadata, load_active_entities
from procurement_etl.lib.column_mapper import apply_column_mapping
from procurement_etl.lib.apply_dq_rules import dq_split

catalog = spark.conf.get("catalog")

def _register_silver(entity):
    silver_cfg       = load_metadata(entity, "silver")
    bronze_table     = f"{catalog}.bronze.{silver_cfg["table"]["bronze_table"]}"
    silver_table     = f"{catalog}.silver.{silver_cfg["table"]["silver_table"]}"
    quarantine_table = f"{catalog}.silver.{silver_cfg["table"]["quarantine_table"]}"
    columns_dict     = silver_cfg["mapping"]["columns"]
    dq_rules         = silver_cfg["dq_rules"]

    def silver_transform():
        df = spark.read.table(bronze_table)
        df = apply_column_mapping(df, columns_dict)
        valid_df, invalid_df = dq_split(df, dq_rules)
        return valid_df, invalid_df
    
    @dp.table(
        name = silver_table,
        comment = f"Cleansed Silver table for {entity}"
    )
    def cleansed():
        valid_df, _ = silver_transform()
        return valid_df

    @dp.table(
        name = quarantine_table,
        comment = f"Quarantine table for {entity}"
    )
    def quarantine():
        _, invalid_df = silver_transform()
        return invalid_df
for _entity in load_active_entities("silver"):
   _register_silver(_entity)
