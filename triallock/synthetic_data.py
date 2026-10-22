import random
import datetime
from pathlib import Path
import pandas as pd
import numpy as np

def generate_demo(study_id="IBD-DEMO-001"):
    random.seed(42)
    np.random.seed(42)
    subjects = []
    for i in range(1, 101):
        site = f"SITE-{random.randint(1, 5):02d}"
        screening = datetime.date(2024, 1, 15) + datetime.timedelta(days=random.randint(0, 60))
        consent = screening + datetime.timedelta(days=random.randint(1, 14))
        rand = consent + datetime.timedelta(days=random.randint(0, 21))
        arm = random.choice(["Active", "Placebo"])
        status = random.choice(["Randomized", "Screen Failure", "Withdrawn"])
        subjects.append({
            "study_id": study_id,
            "subject_id": f"S-{i:03d}",
            "site_id": site,
            "screening_date": screening.strftime("%Y-%m-%d"),
            "consent_date": consent.strftime("%Y-%m-%d"),
            "randomization_date": rand.strftime("%Y-%m-%d") if status == "Randomized" else "",
            "treatment_arm": arm if status == "Randomized" else "",
            "subject_status": status,
        })
    visits = []
    visit_names = ["Baseline", "Week 4", "Week 8", "Week 12"]
    visit_statuses = ["Scheduled", "Completed", "Missed", "Cancelled"]
    for s in subjects:
        sid = s["subject_id"]
        visit_dates = {}
        for vname in visit_names:
            scheduled = datetime.datetime.strptime(s["randomization_date"], "%Y-%m-%d").date() + datetime.timedelta(days=visit_names.index(vname)*28) if s["randomization_date"] else (datetime.date(2024, 2, 1) + datetime.timedelta(days=random.randint(0, 120)))
            actual = scheduled + datetime.timedelta(days=random.randint(-3, 7)) if random.random() > 0.15 else None
            status = random.choice(visit_statuses) if random.random() > 0.8 else "Completed"
            visits.append({
                "study_id": study_id,
                "subject_id": sid,
                "visit_id": f"{sid}-V{visit_names.index(vname)+1}",
                "visit_name": vname,
                "scheduled_date": scheduled.strftime("%Y-%m-%d"),
                "visit_date": actual.strftime("%Y-%m-%d") if actual else "",
                "visit_status": status,
            })
    labs = []
    test_codes = {"CRP": "mg/L", "WBC": "counts/uL", "ALB": "g/L", "ALT": "U/L"}
    for s in subjects:
        sid = s["subject_id"]
        for _ in range(random.randint(6, 14)):
            test = random.choice(list(test_codes.keys()))
            labs.append({
                "study_id": study_id,
                "subject_id": sid,
                "visit_id": f"{sid}-V{random.randint(1,4)}",
                "sample_id": f"{sid}-LAB-{random.randint(1,999):03d}",
                "collection_datetime": (datetime.datetime(2024, 1, 15) + datetime.timedelta(days=random.randint(0, 150), hours=random.randint(0, 23))).strftime("%Y-%m-%d %H:%M:%S"),
                "test_code": test,
                "result_value": str(round(random.uniform(0.5, 120.0), 2)),
                "unit": test_codes[test],
                "source_lab": f"LAB-{random.randint(1,3)}",
            })
    # Introduce some defects
    # Duplicate business key
    for i in range(3):
        subjects.append(subjects[i].copy())
        subjects[-1]["subject_id"] = subjects[i]["subject_id"]  # duplicate
    # Invalid date
    for i in range(5):
        subjects[i]["screening_date"] = "not-a-date"
    # Missing required field
    for i in range(5, 10):
        subjects[i]["subject_id"] = ""
    # Invalid visit reference
    for v in visits[:10]:
        v["subject_id"] = "S-NONEXIST"
    # Invalid lab numeric
    labs[0]["result_value"] = "N/A"
    # AE records
    aes = []
    for s in subjects[:20]:
        sid = s["subject_id"]
        for _ in range(random.randint(0, 3)):
            start = datetime.date(2024, 3, 1) + datetime.timedelta(days=random.randint(0, 100))
            end = start + datetime.timedelta(days=random.randint(-5, 21))
            aes.append({
                "study_id": study_id,
                "subject_id": sid,
                "ae_id": f"{sid}-AE-{random.randint(1,99):02d}",
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "event_term": random.choice(["Headache", "Nausea", "Fatigue", "Abdominal Pain"]),
                "severity": random.choice(["Mild", "Moderate", "Severe", "InvalidSev"]),
                "serious": random.choice(["Yes", "No", True, False]),
                "outcome": random.choice(["Recovered", "Recovering", "Ongoing"]),
            })
    # PRO records
    pros = []
    instruments = ["IBDQ", "SF-36"]
    for s in subjects:
        sid = s["subject_id"]
        for _ in range(random.randint(3, 7)):
            pros.append({
                "study_id": study_id,
                "subject_id": sid,
                "visit_id": f"{sid}-V{random.randint(1,4)}",
                "instrument": random.choice(instruments),
                "question_code": f"Q{random.randint(1,10):02d}",
                "response_value": str(random.randint(1, 5)),
                "response_date": (datetime.date(2024, 1, 15) + datetime.timedelta(days=random.randint(0, 150))).strftime("%Y-%m-%d"),
            })
    meta = {
        "subjects": len(subjects),
        "visits": len(visits),
        "lab_results": len(labs),
        "adverse_events": len(aes),
        "pro_responses": len(pros),
    }
    import csv
    bronze_dir = Path(__file__).resolve().parent.parent / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    for entity, records in [("subjects", subjects), ("visits", visits), ("lab_results", labs), ("adverse_events", aes), ("pro_responses", pros)]:
        path = bronze_dir / f"{entity}_{study_id}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys() if records else [])
            writer.writeheader()
            for r in records:
                writer.writerow(r)
    return {"subjects": subjects, "visits": visits, "lab_results": labs, "adverse_events": aes, "pro_responses": pros, "meta": meta}
