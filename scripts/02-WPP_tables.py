#!/usr/bin/env python3
import pandas as pd
import glob
import os
import re
import sys

INPUT_FOLDER = "./data/WPP Input Tables/"   # root folder containing CSV files (will search recursively)
OUTPUT_FOLDER = "./temporal_spatial_output/"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

TIME_COLUMNS = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day", "1day - < 1week",
    "1 week - < 1 year", "1 year or longer",
]

TIME_MAPPING = {
    "milliseconds": ["<1 second"], "seconds": ["1s - < 1min"], "secondsminutes": ["1s - < 1min", "1min - < 1hr"],
    "minuteshours": ["1min - < 1hr", "1hr - < 1day"], "hoursdays": ["1hr - < 1day", "1day - < 1week"],
    "daysweeks": ["1day - < 1week", "1 week - < 1 year"], "hours": ["1hr - < 1day"], "minutes": ["1min - < 1hr"],
    "days": ["1day - < 1week"], "nan": ["Unknown"],
    "weeks": ["1 week - < 1 year"], "months": ["1 week - < 1 year"], "years": ["1 year or longer"],
    "weeksmonths": ["1 week - < 1 year"], "minuteshoursdays": ["1min - < 1hr", "1hr - < 1day", "1day - < 1week"],
    "hoursdaysweeksmonths": ["1hr - < 1day", "1day - < 1week", "1 week - < 1 year"],
    "secondsminuteshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "milisecondsseconds": ["<1 second", "1s - < 1min"], "secondshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "continuous": ["continuous"], "variable": ["variable"],
}

SPATIAL_MAPPING = {
    "tissue": "AS",
    "tissueftu": "FTU",
    "cell": "CT",
    "organ": "Organ",
    "organsystem": "Organ",
    "biomolecule": "B",
    "molecule": "B",
    "subcellular": "Unknown",
    "organism": "Unknown",
    "nan": "Unknown",
    "": "Unknown"
}

time_category_order = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day",
    "1day - < 1week", "1 week - < 1 year", "1 year or longer",
    "continuous", "variable"
]

ftu_ids = {
    "UBERON:0004203",
    "UBERON:0001289",
    "UBERON:0004205",
    "UBERON:0004193",
    "UBERON:0001285",
    "UBERON:0004204",
    "UBERON:0001229",
    "UBERON:0001291",
    "UBERON:0004647",
    "UBERON:0002299",
    "UBERON:8410043",
    "UBERON:0000006",
    "UBERON:0001263",
    "UBERON:0014725",
    "UBERON:0004179",
    "UBERON:0001983",
    "UBERON:0000412",
    "UBERON:0002073",
    "UBERON:0013487",
    "UBERON:0001213",
    "UBERON:0001250",
    "UBERON:0001959",
    "UBERON:0002125",
    "UBERON:0001831",
    "UBERON:0001832",
    "UBERON:0001736",
}

def find_col_case_insensitive(columns, candidates):
    """
    Return first matching column name from 'columns' for any candidate (case-insensitive), or None.
    """
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand in columns:
            return cand
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None

def clean_effector_id(eff_id):
    """
    Normalize an Effector/ID value so it can be matched to ftu_ids.
    - extracts a 'UBERON:NNNN' token if present (case-insensitive),
    - strips common wrappers, removes URLs, then uppercases fallback string.
    - returns None if nothing meaningful.
    """
    if pd.isna(eff_id):
        return None
    s = str(eff_id).strip()
    if not s:
        return None
    # extract UBERON token if present
    m = re.search(r"(UBERON:\d+)", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # remove urls and surrounding punctuation then uppercase
    s2 = re.sub(r"https?://\S+", "", s)
    s2 = re.sub(r"[<>()\[\]{}\"']", "", s2)
    s2 = s2.strip()
    return s2.upper() if s2 else None

def normalize_time(val):
    if pd.isna(val):
        return "nan"
    s = str(val).lower()
    s = re.sub(r"[–—\-\s,]+", "", s)
    return s

def normalize_spatial(val, effector_id=None):
    val_str = str(val).strip() if pd.notna(val) else ""
    if val_str == "":
        return SPATIAL_MAPPING.get("nan", "Unknown")

    v = re.sub(r"[^a-z0-9]", "", val_str.lower())

    if v == "tissueftu":
        return "FTU"
    if v.startswith("tissue"):
        eff_id_str = str(effector_id).strip() if pd.notna(effector_id) else ""
        if eff_id_str.upper() in ftu_ids or clean_effector_id(eff_id_str) in ftu_ids:
            return "FTU"
        return "AS"

    return SPATIAL_MAPPING.get(v, "Unknown")

def get_lowest_function(row):
    lowest_func = ""
    function_cols = [col for col in row.index if re.match(r"Function/\d+$", col.strip())]
    if not function_cols:
        # heuristics: look for "Lowest Function" column or "Lowest_Function"
        for cand in ["Lowest Function", "Lowest_Function", "LowestFunction"]:
            if cand in row.index:
                val = str(row.get(cand, "")).strip()
                if pd.notna(val) and val != "" and val.lower() != "nan":
                    return val
        return "Unknown"

    function_cols.sort(key=lambda c: int(re.search(r"\d+", c).group()))
    # iterate and keep the last non-empty (matches your previous behavior)
    last_val = ""
    for col in function_cols:
        val = row.get(col, "")
        if pd.notna(val) and str(val).strip().lower() not in {"", "nan"}:
            last_val = str(val).strip()
    return last_val if last_val else "Unknown"

# split Process cell on ';' into fragments
def split_processes_cell(proc_cell):
    """
    Safely split the Process cell (string or float) on semicolons.
    """
    if pd.isna(proc_cell):
        return []

    s = str(proc_cell).strip()
    if s.lower() in {"", "nan", "none", "null"}:
        return []

    parts = re.split(r"\s*;\s*", s)
    return [
        p.strip()
        for p in parts
        if p and p.strip() and p.strip().lower() not in {"nan", "none", "null"}
    ]

def make_function_at_process(lowest_function, process_fragment):
    if process_fragment is None:
        return None

    pf = str(process_fragment).strip()
    if pf == "":
        return None

    lf = str(lowest_function).strip() if pd.notna(lowest_function) else ""

    if lf.lower() != "unknown" and lf != "":
        return f"{lf}@{pf}"
    else:
        return pf

def process_and_save_single(MAIN_CSV_PATH, OUTPUT_PATH, header_row=11):
    # read with given header row (0-indexed)
    main = pd.read_csv(MAIN_CSV_PATH, header=header_row, encoding="utf-8-sig")
    # strip whitespace from column names
    main.columns = main.columns.str.strip()

    # compute Lowest_Function
    main["Lowest_Function"] = main.apply(get_lowest_function, axis=1)

    # split Process into list fragments
    main["Process_List"] = main.get("Process", pd.Series([""] * len(main))).apply(split_processes_cell)

    # explode so each process fragment gets its own row
    exploded = main.explode("Process_List").copy()

    # build Function@Process
    exploded["Function@Process"] = exploded.apply(
    lambda r: make_function_at_process(
        str(r.get("Lowest_Function", "")),
        str(r.get("Process_List", "")) if pd.notna(r.get("Process_List", "")) else ""
    ),
    axis=1
    )

    # drop rows where Function@Process is None or empty (missing processes)
    exploded = exploded[exploded["Function@Process"].notna() & (exploded["Function@Process"].astype(str).str.strip() != "")]

    # normalize TimeScale and Spatial_Type on exploded rows
    # find effector id column case-insensitively once per file
    effector_id_col = find_col_case_insensitive(exploded.columns, ["Effector/ID","Effector ID","Effector_ID","Effector/Id","Effector/identifier","EffectorID"])
    # apply TimeScale normalization
    if "TimeScale" in exploded.columns:
        exploded["TimeScale_norm"] = exploded["TimeScale"].apply(normalize_time)
    else:
        exploded["TimeScale_norm"] = "nan"

    # compute Spatial_Type - try common columns
    # prefer explicit 'EffectorScale' column, otherwise check candidate names
    effector_scale_col = find_col_case_insensitive(exploded.columns, ["EffectorScale","Effector Scale","Effector_Scale","Scale"])
    # If no explicit effector scale column, set as nan to map to Unknown
    if effector_scale_col is None:
        exploded["Spatial_Type"] = SPATIAL_MAPPING.get("nan", "Unknown")
    else:
        if effector_id_col:
            exploded["Spatial_Type"] = exploded.apply(lambda r: normalize_spatial(r.get(effector_scale_col, ""), r.get(effector_id_col, "")), axis=1)
        else:
            exploded["Spatial_Type"] = exploded[effector_scale_col].apply(lambda v: normalize_spatial(v, None))

    # Map time scales that expand to multiple TIME_MAPPING entries
    exploded["Time Range"] = exploded["TimeScale_norm"].apply(lambda x: TIME_MAPPING.get(x, ["Unknown"]))
    exploded = exploded.explode("Time Range")

    # Now group by Time Range + Spatial_Type and collect unique Function@Process entries
    grouped = (
        exploded.groupby(["Time Range", "Spatial_Type"])["Function@Process"]
        .apply(lambda s: "? ".join(sorted(set(ss.strip() for ss in s.dropna() if str(ss).strip()))))
        .reset_index(name="Function@Process")
    )

    # drop empty/Unknown groups (same as before)
    grouped = grouped[grouped["Function@Process"] != ""]
    grouped = grouped[grouped["Function@Process"] != "Unknown"]

    # pivot into spatial x temporal table
    pivot = grouped.pivot(
        index="Time Range",
        columns="Spatial_Type",
        values="Function@Process"
    ).fillna("").reset_index()

    # remove Unknown column if present
    if "Unknown" in pivot.columns:
        pivot = pivot.drop(columns=["Unknown"])

    desired_spatial_types = ["Organ", "AS", "FTU", "CT", "B"]
    for t in desired_spatial_types:
        if t not in pivot.columns:
            pivot[t] = ""

    # Ensure desired ordering of time rows
    pivot = pivot.set_index("Time Range").reindex(time_category_order).fillna("").reset_index()
    pivot["Time Range"] = pd.Categorical(pivot["Time Range"], categories=time_category_order, ordered=True)
    pivot = pivot.sort_values("Time Range").reset_index(drop=True)

    final_pivot = pivot[["Time Range"] + desired_spatial_types]

    # Save to CSV
    final_pivot.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

def main_run():
    csv_files = sorted(glob.glob(os.path.join(INPUT_FOLDER, "**", "*.csv"), recursive=True))
    if not csv_files:
        print("No CSV files found in", INPUT_FOLDER)
        sys.exit(1)

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        # header row heuristic (your previous rule)
        if "endocrine" in file_name.lower():
            header_row = 12
        else:
            header_row = 11

        base_noext = os.path.splitext(file_name)[0]
        words = re.findall(r"\w+", base_noext)
        if len(words) >= 2:
            prefix = f"{words[0]}_{words[1]}"
        elif len(words) == 1:
            prefix = words[0]
        else:
            prefix = base_noext

        out_name = f"{prefix}_spatial_temporal_table.csv"
        out_path = os.path.join(OUTPUT_FOLDER, out_name)

        try:
            process_and_save_single(file_path, out_path, header_row=header_row)
            print(f"Saved: {out_path}")
        except Exception as e:
            print(f"Failed processing {file_name}: {e}")
            continue

    print("Done processing all folders.")

if __name__ == "__main__":
    main_run()
