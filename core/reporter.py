import csv
import os

fieldnames = [
    "status",
    "category",
    "severity",
    "path",
    "sql_value",
    "mongo_value",
    "error"
]

def export_to_csv(mismatches, filename="report.csv"):
    if not mismatches:
        print("No mismatches to export")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    keys = set()
    for m in mismatches:
        keys.update(m.keys())

    keys = list(keys)

    with open(filename, "w", newline="", encoding="utf-8") as f:

        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(mismatches)


    print(f"📁 Report exported to {filename}")