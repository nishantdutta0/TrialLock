from triallock.readiness import assess_readiness

def test_readiness_blocked():
    # Issues with record_count > 0 are considered real violations
    status, reason = assess_readiness([{"severity":"BLOCKER", "record_count": 5}], {"missing":0}, 96)
    assert status == "BLOCKED"

def test_readiness_at_risk():
    status, reason = assess_readiness([{"severity":"HIGH", "record_count": 3}], {"missing":0}, 90)
    assert status == "AT RISK"

def test_readiness_ready():
    status, reason = assess_readiness([], {"missing":0}, 96)
    assert status == "READY FOR DATA REVIEW"
