import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit

spark = SparkSession.getActiveSession()
base_path = spark.conf.get("base_path") + "/src/procurement_etl"

# Load metadata -------
def load_metadata(entity, layer):
    file_path = f"{base_path}/metadata/{layer}/onedata_procurement_{entity}_{layer}_config.yaml"
    with open(file_path) as f:
        cfg = yaml.safe_load(f)
    return cfg

# Load entities -------
def load_active_entities(layer):
    file_path = f"{base_path}/entities/entity.yaml"
    with open(file_path) as f:
        registry = yaml.safe_load(f)
    table_name = spark.conf.get("table_name", "")
    requested = [t.strip() for t in table_name.split(",")] if table_name.strip() else []
    active = []
    for item in registry.get("entities", []):
        is_enabled = item.get("enabled", False)
        has_layer  = layer in item.get("layers", [])
        matches_filter = (
            not requested or         
            item["name"] in requested    
        )
        if is_enabled and has_layer and matches_filter:
            active.append(item["name"])
    return active

# Apply Mapping ------
def apply_column_mapping(df, columns_dict):
    if columns_dict is None:
        print("No mapping provided")
        return df
    for src_col, tgt_col in columns_dict.items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)
    return df

# Apply DQ rules ------
def dq_split(df, dq_rules, reason_col="reject_reason"):
    if not dq_rules:
        return df, None
    reject_exprs = None
    for rule in dq_rules:
        rule_condition = f"NOT ({rule['rule']})"
        if reject_exprs is None:
            reject_exprs = when(expr(rule_condition), lit(rule["name"]))
        else:
            reject_exprs = reject_exprs.when(expr(rule_condition), lit(rule["name"]))
    df = df.withColumn(reason_col, reject_exprs)
    valid_df = df.filter(col(reason_col).isNull()).drop(reason_col)
    invalid_df = df.filter(col(reason_col).isNotNull())
    return valid_df, invalid_df

# Apply schema enforcement -----
def enforce_schema(df, schema_dict, drop_extra_cols=True):
    if not schema_dict:
        print("No schema config provided")
        return df
    for column_name, dtype in schema_dict.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(dtype))
        else:
            df = df.withColumn(column_name, lit(None).cast(dtype))
    if drop_extra_cols:
        df = df.select(*schema_dict.keys())
    return df
 
 
 

    




 