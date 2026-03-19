import yaml
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
base_path = spark.conf.get("base_path") + "/src/procurement_etl"

def load_metadata(entity, layer):
    file_path = f"{base_path}/metadata/{layer}/onedata_procurement_{entity}_{layer}_config.yaml"
    with open(file_path) as f:
        cfg = yaml.safe_load(f)
    return cfg

def load_active_entities(layer):
    file_path = f"{base_path}/entities/entity.yaml"
    with open(file_path) as f:
        registry = yaml.safe_load(f)
    active = []
    entity_list = registry.get("entities", [])
    for item in entity_list:
       is_enabled = item.get("enabled", False)
       has_layer  = layer in item.get("layers", [])
       if is_enabled and has_layer:
           active.append(item["name"])
    return active
 

    




 