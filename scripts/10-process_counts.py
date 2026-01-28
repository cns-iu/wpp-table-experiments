#!/usr/bin/env python3
import os
import glob
import pandas as pd
import re

input_folder = "./temporal_spatial_output/"   # folder with CSVs
output_summary = "./unique_processes/process_counts.csv"
# output_details_dir = "./output/unique_processes/per_file_details/"  # per-file detail lists
os.makedirs(os.path.dirname(output_summary), exist_ok=True)
# os.makedirs(output_details_dir, exist_ok=True)

ENTRY_SEPARATOR = "?"          
SPATIAL_COLUMNS = ["Organ", "AS", "FTU", "CT", "B"]
SPATIAL_PATTERN = re.compile(r"^(Organ|AS|FTU|CT|B)$", re.IGNORECASE)

# ---------- HELPERS ----------
def items_from_cell(cell):
    """Return list of semicolon-separated non-empty items (no '@' logic)."""
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []
    return [it.strip() for it in s.split(ENTRY_SEPARATOR) if it.strip()]

def find_spatial_cols(df):
    """Return the actual column names in df that match our spatial columns (case-insensitive)."""
    cols = {}
    for desired in SPATIAL_COLUMNS:
        matched = next((c for c in df.columns if SPATIAL_PATTERN.match(c) and c.strip().lower() == desired.lower()), None)
        cols[desired] = matched  # matched may be None if column absent
    return cols

# ---------- MAIN ----------
summary_rows = []

csv_files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
if not csv_files:
    print("No CSV files found in", input_folder)
    raise SystemExit(1)

for path in csv_files:
    df = pd.read_csv(path, dtype=object)
    df.columns = [c.strip() for c in df.columns]  # normalize headers
    fname = os.path.splitext(os.path.basename(path))[0]

    # mapping: item_str -> set of spatial columns where it was seen
    item_to_spatials = {}

    spatial_col_map = find_spatial_cols(df)

    # Walk each spatial column and collect items
    for spatial_key, actual_col in spatial_col_map.items():
        if actual_col is None:
            continue
        for cell in df[actual_col].astype(object):
            for item in items_from_cell(cell):
                # record that `item` was seen in spatial_key
                if item not in item_to_spatials:
                    item_to_spatials[item] = set()
                item_to_spatials[item].add(spatial_key)

    # Build per-file detail DataFrame: one row per unique item and boolean flags for each spatial
    detail_rows = []
    for item, spatials in item_to_spatials.items():
        row = {"item": item}
        for sc in SPATIAL_COLUMNS:
            row[sc] = (sc in spatials)
        detail_rows.append(row)

    detail_df = pd.DataFrame(detail_rows).sort_values("item").reset_index(drop=True)

    # Save per-file detail CSV (so you can inspect exact mapping)
    # detail_out = os.path.join(output_details_dir, f"{fname}_unique_items_by_spatial.csv")
    # detail_df.to_csv(detail_out, index=False, encoding="utf-8-sig")

    # Compute summary counts for this file
    per_spatial_counts = {sc: int(detail_df[sc].sum()) if not detail_df.empty else 0 for sc in SPATIAL_COLUMNS}
    total_per_spatial_sum = sum(per_spatial_counts.values())
    global_unique = len(detail_df)

    # Prepare summary row
    summary_row = {"file": fname}
    for sc in SPATIAL_COLUMNS:
        summary_row[f"{sc}_unique_count"] = per_spatial_counts[sc]
    summary_row["Total_per_spatial_sum"] = total_per_spatial_sum
    summary_row["Global_unique_across_spatials"] = global_unique

    summary_rows.append(summary_row)

    print(f"Processed {fname}: global_unique={global_unique}, per_spatial={per_spatial_counts}")

# Save summary CSV
summary_df = pd.DataFrame(summary_rows).sort_values("file")
cols = ["file"] + [f"{c}_unique_count" for c in SPATIAL_COLUMNS] + ["Total_per_spatial_sum", "Global_unique_across_spatials"]
summary_df = summary_df[cols]
summary_df.to_csv(output_summary, index=False, encoding="utf-8-sig")

print("Saved summary to:", output_summary)
# print("Saved per-file details to:", output_details_dir)
