from triallock.config import SCHEMAS

def validate_entity(df, entity):
    schema = SCHEMAS.get(entity, [])
    required = [item["name"] for item in schema if item.get("required")]
    return {"required": required, "missing": []}
