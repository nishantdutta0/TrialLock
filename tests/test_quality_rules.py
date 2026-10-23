from triallock.quality_rules import evaluate_rules

def test_severity_filter():
    rules = [{"rule_id": "TEST", "entity": "subjects", "severity": "BLOCKER", "logic": "missing", "field": "subject_id"}]
    issues, quarantine_df = evaluate_rules(None, "subjects", {"rules": rules})
    assert any(i["severity"] == "BLOCKER" for i in issues)
