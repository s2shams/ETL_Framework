import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_job_owners():
    path = os.path.join(BASE_DIR, "jobs", "config", "job_owners.json")
    with open(path) as f:
        return json.load(f)


def get_owner_email(job_name):
    owners = get_job_owners()
    return owners.get(job_name)


def send_email(job_name, step, email):
    print(f"[EMAIL] Job {job_name} failed at step {step} → {email}")