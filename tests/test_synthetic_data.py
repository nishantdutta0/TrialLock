from triallock.synthetic_data import generate_demo

def test_deterministic():
    data1 = generate_demo()
    data2 = generate_demo()
    assert data1["meta"]["subjects"] == data2["meta"]["subjects"]
