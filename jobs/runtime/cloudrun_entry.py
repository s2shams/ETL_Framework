import sys
from jobs.runtime.job_runner import run_job

def main():
    if len(sys.argv) < 2:
        raise ValueError("Missing job_name argument")

    job_name = sys.argv[1]

    file_path = f"jobs/definitions/{job_name}.txt"

    run_job(job_name, file_path)

if __name__ == "__main__":
    main()