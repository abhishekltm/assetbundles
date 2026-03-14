from procurement.lib import config_loader
import dlt

env_cfg, table_cfg, dq_cfg, schema_cfg = config_loader.load_entity_metadata("lfa1", env="dev")

catalog = table_cfg["catalog"]
bronze_schema = table_cfg["schemas"]["bronze"]
bronze_table = f"{catalog}.{bronze_schema}.{table_cfg['tables']['bronze_table']}"
silver_schema = table_cfg["schemas"]["silver"]
silver_table = f"{catalog}.{silver_schema}.{table_cfg['tables']['silver_table']}"

@dlt.expect_all({r["name"]: r["rule"] for r in dq_cfg["dq_rules"]})
#We will have to quarantine the data if any of the rules fail.

@dlt.table(
    name=silver_table,
    comment="Silver LFA1 - cleaned vendor master"
)

def silver_lfa1():
    df = spark.read.table(bronze_table)
    for src, tgt in table_cfg["columns"].items():
        df = df.withColumnRenamed(src, tgt)

    return df