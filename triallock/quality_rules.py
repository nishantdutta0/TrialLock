"""
Clinical Data Quality Rules Engine for TrialLock.

Implements configurable validation rules based on YAML specifications:
- Missing / required field detection
- Duplicate business key detection
- Numeric parsing and range checks
- Allowed values checks
- Foreign key / reference data validation
- Chronological / date-order validation
- Date format and parse validation
- Critical failure quarantine based on rule severity and policy
"""

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, TimestampType, DateType
import datetime


def evaluate_rules(
    spark_df: Optional[DataFrame],
    entity: str,
    rules_config: Dict[str, Any],
    reference_dfs: Optional[Dict[str, DataFrame]] = None,
    run_id: str = "",
    study_id: str = "",
) -> Tuple[List[Dict[str, Any]], Optional[DataFrame]]:
    """
    Evaluate validation rules against a Spark DataFrame.

    Args:
        spark_df: The DataFrame to validate (None if not available)
        entity: The entity type (e.g., 'subjects', 'visits', 'lab_results')
        rules_config: The rules configuration dictionary from validation_rules.yaml
        reference_dfs: Dictionary of reference DataFrames for foreign key checks
        run_id: Pipeline run identifier for issue tracking
        study_id: Study identifier

    Returns:
        Tuple of (issues_list, quarantine_df):
        - issues_list: List of issue dictionaries with metadata
        - quarantine_df: DataFrame of records that failed quarantine-eligible rules
    """
    issues = []
    quarantine_records = []

    if rules_config is None:
        return issues, None

    # Handle None DataFrame gracefully (e.g., in unit tests or when data is missing)
    if spark_df is None:
        for rule in rules_config.get("rules", []):
            if rule.get("entity") == entity:
                issues.append({
                    "rule_id": rule.get("rule_id", "UNKNOWN"),
                    "description": rule.get("description", ""),
                    "severity": rule.get("severity", "LOW"),
                    "entity": entity,
                    "field": rule.get("field") or ",".join(rule.get("fields", [])),
                    "message": f"Rule '{rule.get('rule_id')}' configured for {entity}; no data provided to evaluate.",
                    "record_count": 0,
                    "quarantine": rule.get("quarantine", False),
                })
        return issues, None

    # Filter rules for this entity
    entity_rules = [r for r in rules_config.get("rules", []) if r.get("entity") == entity]

    # Track which record IDs should be quarantined
    quarantine_filter_exprs = []

    for rule in entity_rules:
        rule_id = rule.get("rule_id", "UNKNOWN")
        description = rule.get("description", "")
        severity = rule.get("severity", "LOW")
        logic = rule.get("logic", "")
        field = rule.get("field", "")
        is_quarantine = rule.get("quarantine", False)

        try:
            # 1. MISSING / REQUIRED FIELD CHECK
            if logic == "missing" and field:
                if field in spark_df.columns:
                    # Check for NULL, empty string, or whitespace-only
                    cond = (
                        (F.col(field).isNull()) |
                        (F.trim(F.col(field)) == "") |
                        (F.col(field) == "NULL") |
                        (F.col(field) == "null")
                    )
                    failed_df = spark_df.filter(cond)
                    failed_count = failed_df.count()

                    if failed_count > 0:
                        issues.append({
                            "rule_id": rule_id,
                            "description": description,
                            "severity": severity,
                            "entity": entity,
                            "field": field,
                            "message": f"Missing or empty required field '{field}' in {failed_count} record(s)",
                            "record_count": failed_count,
                            "quarantine": is_quarantine,
                        })
                        if is_quarantine:
                            quarantine_filter_exprs.append(cond)

            # 2. DUPLICATE BUSINESS KEY CHECK
            elif logic == "duplicate_key":
                key_fields = rule.get("key_fields", [])
                if key_fields and all(k in spark_df.columns for k in key_fields):
                    # Find duplicates using window function or groupBy
                    dups = (
                        spark_df
                        .groupBy(key_fields)
                        .count()
                        .filter(F.col("count") > 1)
                    )
                    dup_group_count = dups.count()

                    if dup_group_count > 0:
                        # Join back to get total affected records
                        dup_records = spark_df.join(dups, on=key_fields, how="inner")
                        total_dup_records = dup_records.count()

                        issues.append({
                            "rule_id": rule_id,
                            "description": description,
                            "severity": severity,
                            "entity": entity,
                            "field": ",".join(key_fields),
                            "message": f"Duplicate key across {key_fields}: {dup_group_count} duplicate group(s), {total_dup_records} total record(s)",
                            "record_count": total_dup_records,
                            "quarantine": is_quarantine,
                        })
                        if is_quarantine:
                            # Mark all records with duplicate keys for quarantine
                            # (we'll handle this by joining with dups)
                            pass

            # 3. NUMERIC PARSING CHECK
            elif logic == "numeric_parse" and field:
                if field in spark_df.columns:
                    # Check for values that cannot be parsed as numbers (excluding NULL/empty)
                    # Pattern matches optional sign, digits, optional decimal point, digits
                    numeric_pattern = r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$"
                    non_null_cond = (
                        F.col(field).isNotNull() &
                        (F.trim(F.col(field)) != "") &
                        (F.col(field) != "NULL") &
                        (F.col(field) != "null")
                    )
                    invalid_cond = non_null_cond & (~F.col(field).rlike(numeric_pattern))
                    failed_df = spark_df.filter(invalid_cond)
                    failed_count = failed_df.count()

                    if failed_count > 0:
                        issues.append({
                            "rule_id": rule_id,
                            "description": description,
                            "severity": severity,
                            "entity": entity,
                            "field": field,
                            "message": f"Non-numeric value in '{field}' for {failed_count} record(s)",
                            "record_count": failed_count,
                            "quarantine": is_quarantine,
                        })
                        if is_quarantine:
                            quarantine_filter_exprs.append(invalid_cond)

            # 4. ALLOWED VALUES CHECK
            elif logic == "allowed_values" and field:
                allowed = rule.get("allowed", [])
                if allowed and field in spark_df.columns:
                    non_null_cond = (
                        F.col(field).isNotNull() &
                        (F.trim(F.col(field)) != "")
                    )
                    invalid_cond = non_null_cond & (~F.col(field).isin(allowed))
                    failed_df = spark_df.filter(invalid_cond)
                    failed_count = failed_df.count()

                    if failed_count > 0:
                        issues.append({
                            "rule_id": rule_id,
                            "description": description,
                            "severity": severity,
                            "entity": entity,
                            "field": field,
                            "message": f"Unapproved value in '{field}' for {failed_count} record(s) (allowed: {allowed})",
                            "record_count": failed_count,
                            "quarantine": is_quarantine,
                        })
                        if is_quarantine:
                            quarantine_filter_exprs.append(invalid_cond)

            # 5. FOREIGN KEY / REFERENCE CHECK
            elif logic == "foreign_key":
                ref_entity = rule.get("reference_entity")
                field = rule.get("field")

                if reference_dfs and ref_entity in reference_dfs and field in spark_df.columns:
                    ref_df = reference_dfs[ref_entity]
                    ref_key = rule.get("reference_key", ["subject_id"])
                    target_field = ref_key[-1] if isinstance(ref_key, list) else ref_key

                    if target_field in ref_df.columns:
                        # Find records where field is not null but does not exist in ref_df
                        non_null_cond = (
                            F.col(field).isNotNull() &
                            (F.trim(F.col(field)) != "") &
                            (F.col(field) != "NULL")
                        )
                        # Left anti-join to find orphans
                        valid_df = spark_df.filter(non_null_cond)
                        orphans = valid_df.join(
                            ref_df.select(F.col(target_field).alias(f"_ref_{target_field}")).distinct(),
                            F.col(field) == F.col(f"_ref_{target_field}"),
                            how="left_anti"
                        )
                        orphan_count = orphans.count()

                        if orphan_count > 0:
                            issues.append({
                                "rule_id": rule_id,
                                "description": description,
                                "severity": severity,
                                "entity": entity,
                                "field": field,
                                "message": f"Foreign key violation: {orphan_count} record(s) reference non-existent {ref_entity}.{target_field}",
                                "record_count": orphan_count,
                                "quarantine": is_quarantine,
                            })
                else:
                    # Reference data not available - note as configuration requirement
                    issues.append({
                        "rule_id": rule_id,
                        "description": description,
                        "severity": severity,
                        "entity": entity,
                        "field": field,
                        "message": f"Foreign key check configured for {entity}.{field} -> {ref_entity}; reference data not available in this stage",
                        "record_count": 0,
                        "quarantine": is_quarantine,
                    })

            # 6. CHRONOLOGICAL / DATE ORDER CHECK
            elif logic == "chronological":
                fields = rule.get("fields", [])
                if len(fields) >= 2 and all(f in spark_df.columns for f in fields):
                    f1, f2 = fields[0], fields[1]
                    # Filter records where both dates are present and parseable
                    both_present = (
                        F.col(f1).isNotNull() & (F.trim(F.col(f1)) != "") &
                        F.col(f2).isNotNull() & (F.trim(F.col(f2)) != "")
                    )
                    # Cast to date for comparison: f1 must be <= f2
                    # Inverted logic: f1 > f2 is a violation
                    # e.g., consent_date must not follow randomization_date -> consent > rand is violation
                    # e.g., end_date must not precede start_date -> start > end is violation
                    violation_cond = both_present & (F.to_date(F.col(f1)) > F.to_date(F.col(f2)))
                    failed_df = spark_df.filter(violation_cond)
                    failed_count = failed_df.count()

                    if failed_count > 0:
                        issues.append({
                            "rule_id": rule_id,
                            "description": description,
                            "severity": severity,
                            "entity": entity,
                            "field": f"{f1},{f2}",
                            "message": f"Chronological violation: {f1} follows {f2} in {failed_count} record(s)",
                            "record_count": failed_count,
                            "quarantine": is_quarantine,
                        })
                        if is_quarantine:
                            quarantine_filter_exprs.append(violation_cond)

            # 7. DATE PARSE / FORMAT CHECK
            elif logic == "date_parse":
                fields = rule.get("fields", [])
                for f in fields:
                    if f in spark_df.columns:
                        non_null_cond = (
                            F.col(f).isNotNull() &
                            (F.trim(F.col(f)) != "") &
                            (F.col(f) != "NULL")
                        )
                        # Attempt to parse date; if to_date returns null on non-null input, it failed
                        invalid_date = non_null_cond & F.to_date(F.col(f)).isNull()
                        failed_df = spark_df.filter(invalid_date)
                        failed_count = failed_df.count()

                        if failed_count > 0:
                            issues.append({
                                "rule_id": rule_id,
                                "description": description,
                                "severity": severity,
                                "entity": entity,
                                "field": f,
                                "message": f"Invalid date format in '{f}' for {failed_count} record(s)",
                                "record_count": failed_count,
                                "quarantine": is_quarantine,
                            })
                            if is_quarantine:
                                quarantine_filter_exprs.append(invalid_date)

        except Exception as e:
            # Explicitly report rule evaluation errors rather than silently swallowing
            issues.append({
                "rule_id": rule_id,
                "description": description,
                "severity": severity,
                "entity": entity,
                "field": field,
                "message": f"Rule evaluation error: {type(e).__name__}: {str(e)}",
                "record_count": 0,
                "quarantine": False,
            })

    # Build quarantine DataFrame if any records matched quarantine rules
    quarantine_df = None
    if quarantine_filter_exprs and spark_df is not None:
        # Combine all quarantine filter expressions with OR
        combined_filter = quarantine_filter_exprs[0]
        for expr in quarantine_filter_exprs[1:]:
            combined_filter = combined_filter | expr

        quarantine_df = spark_df.filter(combined_filter)
        # Add metadata columns to quarantine records
        quarantine_df = quarantine_df.withColumn("_quarantine_timestamp", F.current_timestamp())
        quarantine_df = quarantine_df.withColumn("_quarantine_entity", F.lit(entity))
        if run_id:
            quarantine_df = quarantine_df.withColumn("_quarantine_run_id", F.lit(run_id))
        if study_id:
            quarantine_df = quarantine_df.withColumn("_quarantine_study_id", F.lit(study_id))

    return issues, quarantine_df


def calculate_quality_score(
    total_applicable_checks: int,
    passed_applicable_checks: int
) -> Optional[float]:
    """
    Calculate quality score according to the specification:
    Quality score = 100 * passed applicable checks / total applicable checks

    Returns:
        float rounded to 2 decimal places, or None if no checks apply (prevents division by zero)
    """
    if total_applicable_checks <= 0:
        return None
    return round(100.0 * passed_applicable_checks / total_applicable_checks, 2)
