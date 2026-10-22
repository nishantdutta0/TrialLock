def assess_readiness(issues, visit_completion, quality_score, threshold=95):
    blockers = [i for i in issues if i.get("severity") == "BLOCKER"]
    highs = [i for i in issues if i.get("severity") == "HIGH"]
    if blockers:
        return "BLOCKED", f"{len(blockers)} unresolved BLOCKER issue(s)"
    if highs or (visit_completion.get("missing", 0) > 0) or (quality_score is not None and quality_score < threshold):
        reasons = []
        if highs:
            reasons.append(f"{len(highs)} HIGH issue(s)")
        if visit_completion.get("missing", 0) > 0:
            reasons.append("missing expected visits")
        if quality_score is not None and quality_score < threshold:
            reasons.append(f"quality score {quality_score} below {threshold}")
        return "AT RISK", "; ".join(reasons)
    return "READY FOR DATA REVIEW", "No unresolved BLOCKER or HIGH issues; visits reconciled; score meets threshold"
