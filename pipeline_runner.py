import subprocess
import datetime

def run_script(script_name):
    print(f"\nRunning {script_name}...")
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)


def main():
    print("\n=== WebinX Automated Pipeline Started ===")
    print("Time:", datetime.datetime.now())

    # Order matters (structured → semi-structured)
    run_script("auto_fetch.py")        # Eventbrite / Meetup
    run_script("rss_ingestion.py")     # RSS sources

    print("\n=== Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()