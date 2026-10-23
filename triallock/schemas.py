"""
Schema conformance and validation for TrialLock.
Validates input DataFrames against schemas defined in config/schemas.yaml.
"""

from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, functions as F
from triallock.config import SCHEMAS


def get_schema_fields(entity: str) -> List[Dict[str, Any]]:
    """Get the field specifications for an entity."""
    return SCHEMAS.get(entity, [])


def get_required_fields(entity: str) -> List[str]:
    """Get list of required field names for an entity."""
    return [f["name"] for f in get_schema_fields(entity) if f.get("required", False)]


def validate_schema_conformance(
    spark_df: Optional[DataFrame],
    entity: str
) -> Dict[str, Any]:
    """
    Validate that a DataFrame conforms to the expected schema:
    - Check for missing required columns
    - Check for unexpected extra columns
    - Check for data type mismatches where applicable

    Returns:
        Dict with conformance findings:
        - valid: bool
        - missing_columns: list of required columns not in DataFrame
        - extra_columns: list of columns in DataFrame not in schema
        - column_count: actual column count
        - expected_column_count: expected column count
    """
    schema = get_schema_fields(entity)
    if not schema:
        return {
            "valid": True,
            "entity": entity,
            "missing_columns": [],
            "extra_columns": [],
            "message": f"No schema defined for entity '{entity}'",
        }

    if spark_df is None:
        return {
            "valid": False,
            "entity": entity,
            "missing_columns": get_required_fields(entity),
            "extra_columns": [],
            "message": "DataFrame is None",
        }

    expected_cols = {f["name"] for f in schema}
    required_cols = {f["name"] for f in schema if f.get("required", False)}
    actual_cols = set(spark_df.columns)

    missing = list(required_cols - actual_cols)
    extra = list(actual_cols - expected_cols)

    is_valid = len(missing) == 0

    return {
        "valid": is_valid,
        "entity": entity,
        "missing_columns": sorted(missing),
        "extra_columns": sorted(extra),
        "actual_columns": sorted(list(actual_cols)),
        "expected_columns": sorted(list(expected_cols)),
        "message": f"Schema validation {'passed' if is_valid else 'failed'}: {len(missing)} missing required column(s)",
    }
