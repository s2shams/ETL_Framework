import sys
import subprocess
import os
from jobs.runtime.etl_utils import send_email, get_owner_email

def get_target():
    return os.getenv("TARGET", "dev")

def parse_line(line: str):
    if ":" not in line:
        raise ValueError(f"Invalid step format: {line}")

    prefix, rest = line.split(":", 1)
    prefix = prefix.strip().lower()

    parts = rest.strip().split()

    command = parts[0]
    args = parts[1:] if len(parts) > 1 else []

    return prefix, command, args

def run_python(script_path, args):
    cmd = [
        "python",
        script_path,
        "--target",
        get_target()
    ] + args

    print("Running:", " ".join(cmd), flush=True)
    return subprocess.run(cmd).returncode

def run_dbt(model, args):
    cmd = [
        "dbt",
        "run",
        "--select",
        model,
        "--target",
        get_target()
    ] + args

    print("Running:", " ".join(cmd), flush=True)
    return subprocess.run(cmd).returncode

def run_job(job_name, file_path):
    email = get_owner_email(job_name)

    with open(file_path) as f:
        steps = [line.strip() for line in f if line.strip()]

    print(f"Starting job: {job_name} | TARGET={get_target()}", flush=True)

    for step in steps:
        prefix, command, args = parse_line(step)

        if prefix == "python":
            exit_code = run_python(command, args)

        elif prefix == "dbt":
            exit_code = run_dbt(command, args)

        else:
            raise ValueError(f"Unknown step type: {prefix}")

        if exit_code != 0:
            print(f"\nFAILED STEP: {step}", flush=True)

            if email:
                send_email(job_name, step, email)

            sys.exit(exit_code)

    print(f"Job {job_name} completed successfully", flush=True)