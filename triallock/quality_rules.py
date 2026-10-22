from triallock.config import VALIDATION_RULES

def evaluate_rules(df, entity, rules_config):
    issues = []
    for rule in rules_config.get("rules", []):
        if rule.get("entity") != entity:
            continue
        # Simplified evaluation
        severity = rule.get("severity")
        if rule.get("logic") == "missing" and rule.get("field") in ["subject_id"]:
            # Placeholder - real evaluation against Spark DataFrames would use filter
            issues.append({
                "rule_id": rule.get("rule_id"),
                "description": rule.get("description"),
                "severity": severity,
                "entity": entity,
                "field": rule.get("field"),
                "message": f"Missing required field: {rule.get('field')}"
            })
    return issues
