import json
import subprocess
import argparse
import os
import shutil

REGION = "us-central1"
IMAGE_NAME = "etl-runner"

def get_gcloud():
    gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd")

    if not gcloud:
        raise RuntimeError("gcloud not found in PATH")

    return gcloud

def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    config_path = os.path.join(
        base_dir,
        "..",
        "config",
        "jobs_config.json"
    )

    with open(config_path) as f:
        return json.load(f)

def deploy_job(job_name, config, project_id, target):
    gcloud = get_gcloud()

    cpu = config.get("cpu", "1")
    memory = config.get("memory", "1Gi")
    schedule = config.get("schedule", None)
    timeout = config.get("timeout", "10m") # 10 minutes is the default timeout
    retries = config.get("retries", "0") # 0 retries is the default max
    sa_email = os.getenv("SA_EMAIL")

    if sa_email is None:
        print("Error: SA_EMAIL env variable not correctly set.")
        exit(1)

    image = f"gcr.io/{project_id}/{IMAGE_NAME}"

    update_cmd = [
        gcloud, "run", "jobs", "update", job_name,
        "--image", image,
        "--region", REGION,
        "--cpu", cpu,
        "--memory", memory,
        "--args", job_name,
        "--set-env-vars", f"TARGET={target}",
        "--service-account", sa_email,
        "--task-timeout", timeout,
        "--max-retries", retries
    ]

    create_cmd = [
        gcloud, "run", "jobs", "create", job_name,
        "--image", image,
        "--region", REGION,
        "--cpu", cpu,
        "--memory", memory,
        "--args", job_name,
        "--set-env-vars", f"TARGET={target}",
        "--service-account", sa_email,
        "--task-timeout", timeout,
        "--max-retries", retries
    ]

    print(f"\nDeploying job: {job_name}", flush=True)

    result = subprocess.run(update_cmd)

    if result.returncode != 0:
        print(f"Job {job_name} not found, creating...", flush=True)
        subprocess.run(create_cmd, check=True)

    # create/update scheduler now
    if schedule:
        uri = f"https://run.googleapis.com/v2/projects/{project_id}/locations/{REGION}/jobs/{job_name}:run"
        
        update_scheduler_cmd = [
            gcloud, "scheduler", "jobs", "update", "http",
            f"{job_name}-schedule",
            "--location", REGION,
            "--schedule", schedule,
            "--uri", uri,
            "--http-method", "POST",
            "--oauth-service-account-email", sa_email,
            "--time-zone", "America/Toronto"
        ]

        create_scheduler_cmd = [
            gcloud, "scheduler", "jobs", "create", "http",
            f"{job_name}-schedule",
            "--location", REGION,
            "--schedule", schedule,
            "--uri", uri,
            "--http-method", "POST",
            "--oauth-service-account-email", sa_email,
            "--time-zone", "America/Toronto"
        ]

        print(f"Deploying scheduler for {job_name}", flush=True)

        result = subprocess.run(update_scheduler_cmd)

        if result.returncode != 0:
            print(f"Scheduler for {job_name} not found, creating...",flush=True)
            subprocess.run(create_scheduler_cmd, check=True)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--project", required=True)
    parser.add_argument("--target", required=True)

    args = parser.parse_args()

    project_id = args.project
    target = args.target

    config = load_config()

    for job_name, job_cfg in config.items():
        deploy_job(job_name, job_cfg, project_id, target)

if __name__ == "__main__":
    main()