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
    config_path = os.path.join(base_dir, "jobs", "config", "jobs_config.json")

    with open(config_path) as f:
        return json.load(f)


def deploy_job(job_name, config, project_id):
    gcloud = get_gcloud()

    cpu = config.get("cpu", "1")
    memory = config.get("memory", "1Gi")

    image = f"gcr.io/{project_id}/{IMAGE_NAME}"

    update_cmd = [
        gcloud, "run", "jobs", "update", job_name,
        "--image", image,
        "--region", REGION,
        "--cpu", cpu,
        "--memory", memory,
        "--args", job_name
    ]

    create_cmd = [
        gcloud, "run", "jobs", "create", job_name,
        "--image", image,
        "--region", REGION,
        "--cpu", cpu,
        "--memory", memory,
        "--args", job_name
    ]

    print(f"\nDeploying job: {job_name}")

    result = subprocess.run(update_cmd)

    if result.returncode != 0:
        print(f"Job {job_name} not found, creating...")
        subprocess.run(create_cmd, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    args = parser.parse_args()

    config = load_config()

    for job_name, job_cfg in config.items():
        deploy_job(job_name, job_cfg, args.project)


if __name__ == "__main__":
    main()