import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(BASE_DIR, "config", "study_protocol.yaml"), "r") as f:
    STUDY_PROTOCOL = yaml.safe_load(f)

with open(os.path.join(BASE_DIR, "config", "schemas.yaml"), "r") as f:
    SCHEMAS = yaml.safe_load(f)

with open(os.path.join(BASE_DIR, "config", "validation_rules.yaml"), "r") as f:
    VALIDATION_RULES = yaml.safe_load(f)
