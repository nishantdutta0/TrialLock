"""
Entity-level validation orchestrator for TrialLock.
Combines schema conformance checking with quality rule evaluation.
"""

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame
from triallock.schemas import validate_schema_conformance, get_required_fields
from triallock.quality_rules import evaluate_rules
from triallock.config import VALIDATION_RULES


def validate_entity(
    spark_df: Optional[DataFrame],
    entity: str,
    reference_dfs: Optional[Dict[str, DataFrame]] = None,
    run_id: str = "",
    study_id: str = "",
) -> Dict[str, Any]:
    """
    Run complete validation on an entity DataFrame:
    1. Schema conformance (required/extra columns)
    2. Quality rules evaluation (record-level checks)

    Returns:
        Dict with comprehensive validation results:
        - entity: str
        - schema_conformance: dict
        - issues: list of quality issues found
        - quarantine_df: DataFrame of records to quarantine (if any)
        - passed_checks: count of checks that passed
        - failed_checks: count of checks that failed
        - total_checks: total applicable checks
    """
    # 1. Schema conformance check
    schema_result = validate_schema_conformance(spark_df, entity)

    # 2. Quality rules evaluation
    issues, quarantine_df = evaluate_rules(
        spark_df=spark_df,
        entity=entity,
        rules_config=VALIDATION_RULES,
        reference_dfs=reference_dfs,
        run_id=run_id,
        study_id=study_id,
    )

    # Count checks: each configured rule for this entity counts as 1 check
    entity_rules = [r for r in VALIDATION_RULES.get("rules", []) if r.get("entity") == entity]
    total_checks = len(entity_rules)

    # A rule failed if it generated an issue with record_count > 0
    failed_rule_ids = {i["rule_id"] for i in issues if i.get("record_count", 0) > 0}
    failed_checks = len(failed_rule_ids)
    passed_checks = max(0, total_checks - failed_checks)

    return {
        "entity": entity,
        "schema_conformance": schema_result,
        "issues": issues,
        "quarantine_df": quarantine_df,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
    }
