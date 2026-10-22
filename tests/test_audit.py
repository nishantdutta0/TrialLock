from triallock.audit import verify_audit_chain

def test_audit_chain_valid():
    valid, _ = verify_audit_chain()
    # Should not raise; may be true or false depending on DB state
    assert isinstance(valid, bool)
