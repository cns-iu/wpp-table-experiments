import pandas as pd
import re
import json

# --- LOAD MAIN CSV ---
main = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8-sig")
main.columns = main.columns.str.strip()

# --- LOAD STRUCTURE JSON ---
with open("data/kidney.json", "r", encoding="utf-8") as f:
    data_json = json.load(f)

# Collect label → type mapping
label_type_map = {}

# Handle anatomical_structures
for item in data_json.get("data", {}).get("anatomical_structures", []):
    label = str(item.get("ccf_pref_label", "")).strip().lower()
    typ = item.get("ccf_asctb_type", "").strip().upper()
    if label:
        label_type_map[label] = typ

# Handle cell_types
for item in data_json.get("data", {}).get("cell_types", []):
    label = str(item.get("ccf_pref_label", "")).strip().lower()
    typ = item.get("ccf_asctb_type", "").strip().upper()
    if label:
        label_type_map[label] = typ

# Handle functional tissue units (FTUs) if present
for item in data_json.get("data", {}).get("functional_tissue_units", []):
    label = str(item.get("ccf_pref_label", "")).strip().lower()
    typ = item.get("ccf_asctb_type", "").strip().upper()
    if label:
        label_type_map[label] = typ

# --- NORMALIZE TIME SCALE ---
def normalize(val):
    if pd.isna(val):
        return "nan"
    val = str(val).lower().replace("–", "").replace("-", "").replace(" ", "")
    return val.strip()

main["TimeScale_norm"] = main["TimeScale"].apply(normalize)

# --- MAPPING FOR TIME SCALE RANGES ---
mapping = {
    "milliseconds": ["<1 second"],
    "seconds": ["1s - 1min"],
    "secondsminutes": ["1s - 1min", "1min - 1hr"],
    "minuteshours": ["1min - 1hr", "1hr - 1day"],
    "hoursdays": ["1hr - 1day", "1day - 1week"],
    "daysweeks": ["1day - 1week", "1 week - 1 year"],
    "hours": ["1hr - 1day"],
    "minutes": ["1min - 1hr"],
    "days": ["1day - 1week"],
    "nan": ["nan"],
}

# --- STRUCTURE LABEL COLUMNS ---
structure_label_cols = [c for c in main.columns if re.search(r"Structure/\d+/LABEL", c)]

# --- FIND LOWEST STRUCTURE PER ROW ---
def get_lowest_structure(row):
    vals = [str(row[c]).strip() for c in structure_label_cols if pd.notna(row[c]) and str(row[c]).strip()]
    return vals[-1] if vals else ""

def get_lowest_type(label):
    if not label:
        return ""
    return label_type_map.get(label.lower(), "Unknown")

# --- ADD LOWEST STRUCTURE + TYPE ---
main["Lowest Structure"] = main.apply(get_lowest_structure, axis=1)
main["Lowest Type"] = main["Lowest Structure"].apply(get_lowest_type)

# --- BUILD OUTPUT TABLE ---
cols = [
    "Human Organ System",
    "Major Organs",
    "Lowest Structure",
    "Lowest Type",
    "<1 second",
    "1s - 1min",
    "1min - 1hr",
    "1hr - 1day",
    "1day - 1week",
    "1 week - 1 year",
    "1 year or longer",
    "nan",
]

output = pd.DataFrame(columns=cols)

for _, row in main.iterrows():
    system = row.get("Function/1", "")
    organ = row.get("Structure/1", "")
    process = row.get("Process triple", "")
    scale = row["TimeScale_norm"]

    new_row = {col: "" for col in cols}
    new_row["Human Organ System"] = system
    new_row["Major Organs"] = organ
    new_row["Lowest Structure"] = row.get("Lowest Structure", "")
    new_row["Lowest Type"] = row.get("Lowest Type", "")

    # Combine Function/4 and Process triple
    func4 = str(row.get("Function/4", "")).strip()
    combined_process = f"{func4}@{process}" if func4 and process else process or func4

    for t in mapping.get(scale, []):
        new_row[t] = combined_process

    output.loc[len(output)] = new_row

output.to_csv("output/lowest_type.csv", index=False, encoding="utf-8-sig")

print("Created successfully with Lowest Structure + Type columns!")
