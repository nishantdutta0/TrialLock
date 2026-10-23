"""
End-to-end integration test for TrialLock pipeline.
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from triallock import synthetic_data, pipeline, metrics, storage

print("=" * 70)
print("Pipeline Integration Test")
print("=" * 70)

# Step 1: Generate synthetic demo data
print("\n[1] Generating synthetic demo data...")
data = synthetic_data.generate_demo("IBD-DEMO-001")
print(f"  Generated: {data['meta']}")

# Step 2: Run pipeline
print("\n[2] Running medallion pipeline...")
result = pipeline.run_pipeline("IBD-DEMO-001")

print(f"  Run ID: {result.get('run_id')}")
print(f"  Status: {result.get('status')}")
print(f"  Duration: {result.get('duration_sec')}s")
print(f"  Quality Score: {result.get('quality_score')}%")
print(f"  Readiness: {result.get('readiness_status')}")
print(f"  Readiness Reason: {result.get('readiness_reason')}")
print(f"  Input counts: {result.get('input_counts')}")
print(f"  Silver counts: {result.get('silver_counts')}")
print(f"  Gold counts: {result.get('gold_counts')}")
print(f"  Quarantined counts: {result.get('quarantined_counts')}")
print(f"  Total checks: {result.get('total_checks')}")
print(f"  Passed checks: {result.get('passed_checks')}")
print(f"  Failed checks: {result.get('failed_checks')}")

if result.get("status") != "COMPLETED":
    print(f"\nERROR: Pipeline failed: {result.get('error')}")
    sys.exit(1)

# Step 3: Verify DQ issues were recorded
print("\n[3] Checking recorded DQ issues...")
issues = storage.get_dq_issues(run_id=result.get("run_id"))
print(f"  Total recorded issues: {len(issues)}")
for issue in issues[:5]:
    print(f"    - [{issue['severity']}] {issue['rule_id']}: {issue['message']}")

# Step 4: Verify metrics retrieval
print("\n[4] Checking metrics retrieval...")
latest = metrics.get_latest_metrics()
print(f"  Latest run: {latest.get('run_id')}, Status: {latest.get('status')}, Score: {latest.get('quality_score')}%")

print("\n" + "=" * 70)
print("INTEGRATION TEST PASSED!")
print("=" * 70)
