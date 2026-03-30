import csv
import os


def export_to_csv(mismatches, filename="report.csv"):
    if not mismatches:
        print("No mismatches to export")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    keys = set()
    for mismatch in mismatches:
        keys.update(mismatch.keys())

    with open(filename, "w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=list(keys))
        writer.writeheader()
        writer.writerows(mismatches)

    print(f"Report exported to {filename}")
