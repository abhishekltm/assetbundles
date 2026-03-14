import yaml
import os

BASE = os.getcwd()

def load_entity_metadata(entity, env="dev"):
    metadata_path = os.path.join(BASE, "metadata")
    with open(os.path.join(metadata_path, "table_config",
                           f"onedata_procurement_{entity}_config.yaml")) as f:
        table_cfg = yaml.safe_load(f)
    with open(os.path.join(metadata_path, "dq_rules",
                           f"onedata_procurement_{entity}_dq_rules.yaml")) as f:
        dq_cfg = yaml.safe_load(f)

    env_cfg = {}

    return env_cfg, table_cfg, dq_cfg
 