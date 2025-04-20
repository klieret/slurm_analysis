import subprocess
import csv

def get_finger_info(usernames: list[str], output_path: str) -> None:
    """Fetch finger info for a list of usernames and save to CSV."""
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["username", "finger_output"])
        for user in usernames:
            try:
                output = subprocess.check_output(["finger", user], text=True).strip()
            except subprocess.CalledProcessError as e:
                output = f"Error: {e}"
            writer.writerow([user, output])
