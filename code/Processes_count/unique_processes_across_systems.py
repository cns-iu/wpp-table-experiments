#!/usr/bin/env python3
import os
import glob
import pandas as pd
import re

# ---------------------------
# CONFIG
# ---------------------------
input_folder = "./output/temporal_spatial_output/v3/"
output_summary_file = "./output/unique_processes/unique_process_counts_final_summary.csv"

ENTRY_SEPARATOR = ";"
FUNC_PROC_SEPARATOR = "@"

# Order to display spatial columns
PREFERRED_ORDER = ["Organ", "AS", "FTU", "CT", "B"]

# Detect only these
SPATIAL_PATTERN = re.compile(r"^(Organ|AS|FTU|CT|B)$", re.IGNORECASE)


# ---------------------------
# HELPERS
# ---------------------------
def extract_processes(cell):
    """Extract list of valid processes; empty list means missing/invalid."""
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []

    processes = []
    for item in s.split(ENTRY_SEPARATOR):
        item = item.strip()
        if FUNC_PROC_SEPARATOR in item:
            _, proc = item.split(FUNC_PROC_SEPARATOR, 1)
            proc = proc.strip()
            if proc:
                processes.append(proc)
    return processes


def detect_spatial_columns(df):
    return [c for c in df.columns if SPATIAL_PATTERN.match(c)]


def order_spatial_columns(cols):
    low2orig = {c.lower(): c for c in cols}
    ordered = []

    for pref in PREFERRED_ORDER:
        key = pref.lower()
        if key in low2orig:
            ordered.append(low2orig[key])
            del low2orig[key]

    ordered.extend(sorted(low2orig.values(), key=str.lower))
    return ordered


# ---------------------------
# MAIN
# ---------------------------
summary_rows = []
csv_files = glob.glob(os.path.join(input_folder, "*.csv"))

for file_path in csv_files:
    organ_system = os.path.splitext(os.path.basename(file_path))[0]
    df = pd.read_csv(file_path)

    spatial_cols = detect_spatial_columns(df)
    if not spatial_cols:
        summary_rows.append({"organ_system": organ_system})
        continue

    ordered_spatials = order_spatial_columns(spatial_cols)

    row = {"organ_system": organ_system}

    # Global accumulators
    all_extracted_processes = []
    all_unique_global = set()
    total_missing_all = 0

    # Per-column unique counting
    for col in ordered_spatials:
        unique_procs = set()
        missing_count = 0

        for cell in df[col].astype(object):
            procs = extract_processes(cell)

            if not procs:
                missing_count += 1
                continue

            unique_procs.update(procs)
            all_extracted_processes.extend(procs)
            all_unique_global.update(procs)

        # KEEP ONLY THIS COLUMN (drop extracted_count, drop missing_entries)
        row[f"{col}_unique_processes"] = len(unique_procs)

        # Still needed for global missing
        total_missing_all += missing_count

    # -------- FINAL AGGREGATES --------

    # sum of all <col>_unique_processes
    row["sum_per_column_unique"] = sum(
        row.get(f"{col}_unique_processes", 0) for col in ordered_spatials
    )

    # extracted + missing
    total_extracted = len(all_extracted_processes)
    row["total_processes_including_duplicates_and_missing"] = (
        total_extracted + total_missing_all
    )

    # unique across all spatial columns
    row["total_unique_across_all_spatial"] = len(all_unique_global)

    # total missing across all spatial columns
    row["total_missing_all_spatial"] = total_missing_all

    summary_rows.append(row)


summary_df = pd.DataFrame(summary_rows).fillna(0).sort_values("organ_system")
summary_df.to_csv(output_summary_file, index=False)

print("Done. Output saved to:", output_summary_file)
