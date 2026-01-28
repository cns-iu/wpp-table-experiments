#!/usr/bin/env python3
import pandas as pd
import glob
import os
import re

INPUT_FOLDER = "./data/WPP Input Tables/"
OUT_FOLDER = "./unique_effectors/"
os.makedirs(OUT_FOLDER, exist_ok=True)

# Spatial types we report (keeps column order)
DESIRED_SPATIAL = ["Organ", "AS", "FTU", "CT", "B"]

# TIME_MAPPING kept for compatibility with "TimeScale" normalization (not used in aggregation)
TIME_MAPPING = {
    "milliseconds": ["<1 second"], "seconds": ["1s - < 1min"],
    "secondsminutes": ["1s - < 1min", "1min - < 1hr"],
    "minuteshours": ["1min - < 1hr", "1hr - < 1day"], "hoursdays": ["1hr - < 1day", "1day - < 1week"],
    "daysweeks": ["1day - < 1week", "1 week - < 1 year"], "hours": ["1hr - < 1day"],
    "minutes": ["1min - < 1hr"], "days": ["1day - < 1week"], "nan": ["Unknown"],
    "weeks": ["1 week - < 1 year"], "months": ["1 week - < 1 year"], "years": ["1 year or longer"],
    "weeksmonths": ["1 week - < 1 year"], "minuteshoursdays": ["1min - < 1hr", "1hr - < 1day", "1day - < 1week"],
    "hoursdaysweeksmonths": ["1hr - < 1day", "1day - < 1week", "1 week - < 1 year"],
    "secondsminuteshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "milisecondsseconds": ["<1 second", "1s - < 1min"], "secondshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "continuous": ["continuous"], "variable": ["variable"],
}

SPATIAL_MAPPING = {
    "tissue": "AS", "tissueftu": "FTU", "cell": "CT",
    "organ": "Organ", "organsystem": "Organ",
    "biomolecule": "B", "molecule": "B",
    "subcellular": "Unknown", "organism": "Unknown",
    "nan": "Unknown", "": "Unknown"
}

# If you have your ftu_ids set from before, place them here for FTU detection:
ftu_ids = {
    "UBERON:0004203","UBERON:0001289","UBERON:0004205","UBERON:0004193",
    "UBERON:0001285","UBERON:0004204","UBERON:0001229","UBERON:0001291",
    "UBERON:0004647","UBERON:0002299","UBERON:8410043","UBERON:0000006",
    "UBERON:0001263","UBERON:0014725","UBERON:0004179","UBERON:0001983",
    "UBERON:0000412","UBERON:0002073","UBERON:0013487","UBERON:0001213",
    "UBERON:0001250","UBERON:0001959","UBERON:0002125","UBERON:0001831",
    "UBERON:0001832","UBERON:0001736"
}

def normalize_spatial(val, effector_id=None):
    if pd.isna(val) or str(val).strip() == "":
        return SPATIAL_MAPPING.get("nan", "Unknown")
    v = re.sub(r"[^a-z0-9]", "", str(val).strip().lower())
    if v == "tissueftu":
        return "FTU"
    if v.startswith("tissue"):
        if effector_id is not None and pd.notna(effector_id):
            eff_id_str = str(effector_id).strip()
            if eff_id_str in ftu_ids:
                return "FTU"
        return "AS"
    return SPATIAL_MAPPING.get(v, "Unknown")

def get_lowest_function(row):
    function_cols = [col for col in row.index if re.match(r"Function/\d+$", col.strip())]
    function_cols.sort(key=lambda c: int(re.search(r"\d+", c).group()))
    for col in function_cols:
        val = row.get(col, "")
        if pd.notna(val) and str(val).strip().lower() not in {"", "nan", "none", "null"}:
            return str(val).strip()
    return "Unknown"

def build_combined_process(row):
    proc = row.get("Process", "")
    if pd.isna(proc) or str(proc).strip().lower() in {"", "nan", "none", "null"}:
        return None
    proc = str(proc).strip()
    lf = row.get("Lowest_Function", "")
    if lf and lf != "Unknown":
        return f"{lf}@{proc}"
    return proc

def find_label_column(df):
    candidates = ["Effector/Label", "Effector/LABEL", "Effector Label", "EffectorLabel", "Effector/label"]
    lc = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None

def safe_label_set(series):
    out = set()
    for v in series.dropna().astype(str):
        s = v.strip()
        if s and s.lower() not in {"nan", "none", "null"}:
            out.add(s)
    return out

def process_file_aggregate(path, header_row):
    df = pd.read_csv(path, header=header_row, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()

    # Build columns
    df["Lowest_Function"] = df.apply(get_lowest_function, axis=1)
    df["Combined_Process"] = df.apply(build_combined_process, axis=1)
    df["Spatial_Type"] = df.apply(lambda r: normalize_spatial(r.get("EffectorScale", ""), r.get("Effector/ID", "")), axis=1)

    # Keep only rows with a Combined_Process
    df = df[df["Combined_Process"].notna()].copy()

    # find label column (if none, produce empty)
    label_col = find_label_column(df)
    if label_col is None:
        df["__LABEL_TEMP__"] = pd.NA
        label_col = "__LABEL_TEMP__"

    # Group by Spatial_Type and collect unique labels (ACROSS ALL TIME)
    grouped = df.groupby("Spatial_Type")[label_col].apply(lambda s: safe_label_set(s)).to_dict()

    # Build spatial counts ensuring desired spatials present
    spatial_counts = {s: len(grouped.get(s, set())) for s in DESIRED_SPATIAL}

    # Compute union across spatial types (unique labels across all spatials)
    union_all = set()
    for st_set in grouped.values():
        union_all.update(st_set)
    total_union = len(union_all)

    return spatial_counts, total_union

files = sorted(glob.glob(os.path.join(INPUT_FOLDER, "**", "*.csv"), recursive=True))
summary_rows = []

if not files:
    print("No CSV files found in", INPUT_FOLDER)
    raise SystemExit(1)

for file_path in files:
    fname = os.path.basename(file_path)
    header_row = 12 if "endocrine" in fname.lower() else 11

    # prefix for naming
    base_noext = os.path.splitext(fname)[0]
    words = re.findall(r"\w+", base_noext)
    if len(words) >= 2:
        prefix = f"{words[0]}_{words[1]}"
    elif len(words) == 1:
        prefix = words[0]
    else:
        prefix = base_noext

    try:
        counts, total_union = process_file_aggregate(file_path, header_row)
        # per-file dataframe (single-row)
        perfile_df = pd.DataFrame([{
            "file": fname,
            **{k: counts[k] for k in DESIRED_SPATIAL},
            "Total_unique_labels_across_spatial": total_union
        }])
        out_per_file = os.path.join(OUT_FOLDER, f"{prefix}_label_counts_agg.csv")
        perfile_df.to_csv(out_per_file, index=False, encoding="utf-8-sig")

        # add to combined summary
        summary_rows.append({
            "file": fname,
            **{k: counts[k] for k in DESIRED_SPATIAL},
            "Total_unique_labels_across_spatial": total_union
        })

        print(f"Saved aggregated label counts for {fname} -> {out_per_file}")

    except Exception as e:
        print(f"Failed processing {fname}: {e}")
        continue

# write combined summary CSV
if summary_rows:
    summary_df = pd.DataFrame(summary_rows)
    # optional: reorder columns
    cols = ["file"] + DESIRED_SPATIAL + ["Total_unique_labels_across_spatial"]
    summary_df = summary_df[cols]
    summary_out = os.path.join(OUT_FOLDER, "all_organ_system_label_counts.csv")
    summary_df.to_csv(summary_out, index=False, encoding="utf-8-sig")
    print("Saved combined summary:", summary_out)

print("Done.")