from triallock.pipeline import run_pipeline
from triallock import config

def test_pipeline_produces_result():
    result = run_pipeline(config.STUDY_PROTOCOL["study_id"])
    assert "run_id" in result
