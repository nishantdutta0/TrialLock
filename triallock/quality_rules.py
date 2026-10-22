from triallock.config import VALIDATION_RULES

def evaluate_rules(spark_df, entity, rules_config):
    issues = []
    if spark_df is None:
        return issues
    for rule in rules_config.get("rules", []):
        if rule.get("entity") != entity:
            continue
        rule_id = rule.get("rule_id", "UNKNOWN")
        severity = rule.get("severity", "LOW")
        logic = rule.get("logic", "")
        field = rule.get("field", "")
        # Basic missing field check
        if logic == "missing" and field:
            # Check for null or empty string values in Spark
            from pyspark.sql import functions as F
            null_count = spark_df.filter((F.col(field).isNull()) | (F.col(field) == "") | (F.col(field) == "NULL") | (F.col(field) == "null")).count()
            if null_count > 0:
                issues.append({
                    "rule_id": rule_id,
                    "description": rule.get("description", ""),
                    "severity": severity,
                    "entity": entity,
                    "field": field,
                    "message": f"Missing or empty required field '{field}' in {null_count} record(s)",
                })
        # Duplicate business key check
        elif logic == "duplicate_key":
            key_fields = rule.get("key_fields", ["subject_id"])
            from pyspark.sql import functions as F
            group_df = spark_df.groupBy(key_fields).count()
            dups = group_df.filter(F.col("count") > 1)
            dup_count = dups.count()
            if dup_count > 0:
                issues.append({
                    "rule_id": rule_id,
                    "description": rule.get("description", ""),
                    "severity": severity,
                    "entity": entity,
                    "field": ",".join(key_fields),
                    "message": f"Duplicate business key detected in {dup_count} group(s) using fields {key_fields}",
                })
        # Numeric parse check
        elif logic == "numeric_parse" and field:
            try:
                invalid = spark_df.filter(~F.col(field).rlike(r"^-?\d+\.?\d*$")).filter(F.col(field).isNotNull()).filter(F.col(field) != "")
                invalid_count = invalid.count()
                if invalid_count > 0:
                    issues.append({
                        "rule_id": rule_id,
                        "description": rule.get("description", ""),
                        "severity": severity,
                        "entity": entity,
                        "field": field,
                        "message": f"Numeric parse failure in {invalid_count} record(s) for field '{field}'",
                    })
            except Exception:
                pass
        # Allowed values
        elif logic == "allowed_values" and field:
            allowed = rule.get("allowed", [])
            if allowed:
                from pyspark.sql import functions as F
                invalid = spark_df.filter(~F.col(field).isin(allowed)).filter(F.col(field).isNotNull()).filter(F.col(field) != "")
                inv_count = invalid.count()
                if inv_count > 0:
                    issues.append({
                        "rule_id": rule_id,
                        "description": rule.get("description", ""),
                        "severity": severity,
                        "entity": entity,
                        "field": field,
                        "message": f"Unapproved value found in {inv_count} record(s) for field '{field}' (expected {allowed})",
                    })
        # Foreign key
        elif logic == "foreign_key":
            # Simplified: just note that reference check is configured
            issues.append({
                "rule_id": rule_id,
                "description": rule.get("description", ""),
                "severity": severity,
                "entity": entity,
                "field": field,
                "message": "Foreign key reference configured; verify against reference dataset manually in full production version",
            })
        # Chronological
        elif logic == "chronological":
            fields = rule.get("fields", [])
            if len(fields) >= 2:
                issues.append({
                    "rule_id": rule_id,
                    "description": rule.get("description", ""),
                    "severity": severity,
                    "entity": entity,
                    "field": ",".join(fields),
                    "message": f"Chronological check configured for fields: {fields}",
                })
    return issues
