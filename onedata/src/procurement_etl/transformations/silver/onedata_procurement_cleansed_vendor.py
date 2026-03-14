from procurement_etl.lib.config_loader import load_entity_metadata
from procurement_etl.lib.column_mapper import apply_column_mapping
from procurement_etl.lib.apply_dq_rules import apply_dq_rules
from procurement_etl.lib.schema_enforcer import enforce_schema
from pyspark import pipelines as dp

ENV = spark.conf.get("env")
catalog = spark.conf.get("catalog")
configs = load_entity_metadata("lfa1", ENV)
table_cfg = configs["table"]
dq_cfg = configs["dq"]
schema_cfg = configs["schema"]
mapping_cfg = configs["mapping"]
column_mappings = mapping_cfg["columns"]

silver_schema = table_cfg["schemas"]["silver"]
silver_table = f"{catalog}.{silver_schema}.{table_cfg['tables']['silver_table']}"
bronze_schema = table_cfg["schemas"]["bronze"]
bronze_table = f"{catalog}.{bronze_schema}.{table_cfg['tables']['bronze_table']}"
@dp.table(
    name=silver_table,
    comment="Silver LFA1 - cleansed vendor"
)
def silver_lfa1():
    df = spark.read.table(bronze_table)
    df = apply_column_mapping(df, column_mappings)
    df = enforce_schema(df, schema_cfg)
    dq_rules = dq_cfg.get("dq_rules", [])
    valid_df, invalid_df = apply_dq_rules(df, dq_rules)
    return valid_df

quarantine_table = silver_table + "_quarantine"
@dp.table(
    name=quarantine_table,
    comment="Quarantine LFA1"
)
def quarantine_lfa1():
    df = spark.read.table(bronze_table)
    df = apply_column_mapping(df, column_mappings)
    dq_rules = dq_cfg.get("dq_rules", [])
    valid_df, invalid_df = apply_dq_rules(df, dq_rules)
    return invalid_df

@dp.view(name = "lfa1_cleansed_view")
def lfa1_cleansed_view():
    return spark.readStream.table(silver_table)

scd_cfg = table_cfg["scd"]
cdm_table = f"{catalog}.{silver_schema}.{table_cfg['tables']['cdm_table']}"
dp.create_streaming_table(name = cdm_table)
dp.apply_changes(
    target = cdm_table,
    source = "lfa1_cleansed_view",
    keys = [table_cfg["keys"]["primary"]],
    sequence_by = scd_cfg["sequence_column"],
    stored_as_scd_type = scd_cfg["type"],
    track_history_column_list = scd_cfg["track_history_columns"]
)


