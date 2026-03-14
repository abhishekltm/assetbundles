import yaml
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

def load_entity_metadata(entity, env="dev"):
    base_path = spark.conf.get("base_path") + "/src/procurement_etl"

    with open(f"{base_path}/metadata/table_config/onedata_procurement_{entity}_config.yaml") as f:
        table_cfg = yaml.safe_load(f)
    with open(f"{base_path}/metadata/dq_rules/onedata_procurement_{entity}_dq_rules.yaml") as f:
        dq_cfg = yaml.safe_load(f)
    with open(f"{base_path}/metadata/schema/onedata_procurement_{entity}_schema.yaml") as f:
        schema_cfg = yaml.safe_load(f)
    with open(f"{base_path}/metadata/mappings/onedata_procurement_{entity}_mappings.yaml") as f:
        mapping_cfg = yaml.safe_load(f)
    return {
        "table": table_cfg,
        "dq": dq_cfg,
        "schema": schema_cfg,
        "mapping": mapping_cfg
    }