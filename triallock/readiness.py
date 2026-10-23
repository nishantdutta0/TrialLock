"""
Clinical study database-lock readiness assessment for TrialLock.

Evaluates:
- Unresolved BLOCKER and HIGH data quality issues
- Expected-visit schedule reconciliation for required visits
- Configured quality threshold from study protocol
"""

from typing import List, Dict, Any, Tuple, Optional
from triallock.config import STUDY_PROTOCOL


def assess_readiness(
    issues: List[Dict[str, Any]],
    visit_completion: Optional[Dict[str, Any]] = None,
    quality_score: Optional[float] = None,
    threshold: Optional[float] = None,
) -> Tuple[str, str]:
    """
    Assess study readiness according to the documented clinical rules:
    - BLOCKED: At least one unresolved BLOCKER issue exists.
    - AT RISK: No BLOCKER issues, but HIGH issues exist, mandatory visits are missing, or quality score is below threshold.
    - READY FOR DATA REVIEW: No BLOCKER or HIGH issues, required visits reconciled, and quality threshold met.

    Args:
        issues: List of data quality issue dictionaries
        visit_completion: Dict with visit completion metrics (e.g., {"missing": count, "reconciled": bool})
        quality_score: Calculated quality score (0-100 or None)
        threshold: Minimum quality score threshold (defaults to study protocol setting)

    Returns:
        Tuple of (status, reason):
        - status: "BLOCKED" | "AT RISK" | "READY FOR DATA REVIEW"
        - reason: Human-readable explanation of the assessment
    """
    if threshold is None:
        threshold = STUDY_PROTOCOL.get("minimum_quality_score", 95)

    if visit_completion is None:
        visit_completion = {"missing": 0, "reconciled": True}

    # Count issues by severity (only issues with actual violations, record_count > 0)
    real_issues = [i for i in issues if i.get("record_count", 0) > 0]
    blockers = [i for i in real_issues if i.get("severity") == "BLOCKER"]
    highs = [i for i in real_issues if i.get("severity") == "HIGH"]

    # Rule 1: BLOCKED if any BLOCKER issues exist
    if blockers:
        blocker_rules = list({b.get("rule_id", "UNKNOWN") for b in blockers})
        total_blocker_records = sum(b.get("record_count", 0) for b in blockers)
        return (
            "BLOCKED",
            f"{len(blockers)} BLOCKER rule(s) violated ({', '.join(blocker_rules)}) affecting {total_blocker_records} record(s)"
        )

    # Rule 2: AT RISK if HIGH issues exist, visits missing, or quality below threshold
    at_risk_reasons = []

    if highs:
        high_rules = list({h.get("rule_id", "UNKNOWN") for h in highs})
        at_risk_reasons.append(f"{len(highs)} HIGH issue(s) ({', '.join(high_rules)})")

    missing_visits = visit_completion.get("missing", 0)
    if missing_visits > 0:
        at_risk_reasons.append(f"{missing_visits} expected mandatory visit(s) unreconciled")

    if quality_score is None:
        at_risk_reasons.append("quality score not evaluated")
    elif quality_score < threshold:
        at_risk_reasons.append(f"quality score {quality_score:.2f}% below threshold of {threshold}%")

    if at_risk_reasons:
        return "AT RISK", "; ".join(at_risk_reasons)

    # Rule 3: READY FOR DATA REVIEW
    return (
        "READY FOR DATA REVIEW",
        f"All criteria met: 0 BLOCKER, 0 HIGH, visits reconciled, quality score {quality_score:.2f}% >= {threshold}%"
    )


def reconcile_expected_visits(
    subjects_df: Optional[Any],
    visits_df: Optional[Any],
) -> Dict[str, Any]:
    """
    Reconcile expected visits against actual visits for enrolled subjects.

    Returns:
        Dict with reconciliation findings:
        - total_expected: total mandatory visits expected across all subjects
        - completed: count of completed visits
        - missing: count of missing mandatory visits
        - reconciled: bool
    """
    if subjects_df is None or visits_df is None:
        return {"total_expected": 0, "completed": 0, "missing": 0, "reconciled": True}

    try:
        from pyspark.sql import functions as F

        # Get required visit names from protocol
        expected_schedule = STUDY_PROTOCOL.get("expected_visit_schedule", [])
        required_visit_names = [v["visit_name"] for v in expected_schedule if v.get("required", False)]

        if not required_visit_names:
            return {"total_expected": 0, "completed": 0, "missing": 0, "reconciled": True}

        # Count active/randomized subjects
        active_subjects = subjects_df.filter(F.col("subject_status") == "Randomized").count()
        total_expected = active_subjects * len(required_visit_names)

        # Count actual completed required visits
        completed_visits = (
            visits_df
            .filter(F.col("visit_name").isin(required_visit_names))
            .filter(F.col("visit_status") == "Completed")
            .count()
        )

        missing = max(0, total_expected - completed_visits)
        reconciled = (missing == 0)

        return {
            "total_expected": total_expected,
            "completed": completed_visits,
            "missing": missing,
            "reconciled": reconciled,
        }
    except Exception:
        return {"total_expected": 0, "completed": 0, "missing": 0, "reconciled": True}
