import pandas as pd
import re

# --- LOAD MAIN CSV ---
main = pd.read_csv(
    "data/Urinary-system_v1.0_DRAFT_250923 - urinary-system_v1.0_DRAFT.csv",
    header=11,
    encoding="utf-8-sig"
)
main.columns = main.columns.str.strip()

# --- NORMALIZE TIME SCALE ---
def normalize(val):
    if pd.isna(val):
        return "nan"
    val = str(val).lower()
    val = re.sub(r"[–,\-\s]", "", val)
    return val.strip()

main["TimeScale_norm"] = main["TimeScale"].apply(normalize)

# --- MAPPING FOR TIME SCALE RANGES ---
mapping = {
    "milliseconds": ["<1 second"],
    "seconds": ["1s - < 1min"],
    "secondsminutes": ["1s - < 1min", "1min - < 1hr"],
    "minuteshours": ["1min - < 1hr", "1hr - < 1day"],
    "hoursdays": ["1hr - < 1day", "1day - < 1week"],
    "daysweeks": ["1day - < 1week", "1 week - < 1 year"],
    "hours": ["1hr - < 1day"],
    "minutes": ["1min - < 1hr"],
    "days": ["1day - < 1week"],
    "nan": ["nan"],
}

# --- FIND STRUCTURE + CELL LABEL/ID COLUMNS ---
structure_label_cols = sorted(
    [c for c in main.columns if re.search(r"Structure/\d+/LABEL", c)],
    key=lambda x: int(re.search(r"/(\d+)/", x).group(1))
)
cell_label_cols = sorted(
    [c for c in main.columns if re.search(r"Cell/\d+/LABEL", c)],
    key=lambda x: int(re.search(r"/(\d+)/", x).group(1))
)

# --- FUNCTION COLUMNS ---
func_cols = [c for c in main.columns if re.search(r"^Function\s*/?\s*\d+$", c, re.IGNORECASE)]
func_cols.sort(key=lambda c: int(re.search(r"\d+", c).group()))

def get_lowest_function(row):
    """Return deepest non-empty Function/x."""
    lowest_func = ""
    function_cols = []
    for col in row.index:
        match = re.match(r"Function/(\d+)$", col.strip())
        if match:
            idx = int(match.group(1))
            function_cols.append((idx, col))
    function_cols.sort(key=lambda x: x[0])
    for _, col in function_cols:
        val = str(row.get(col, "")).strip()
        if pd.notna(val) and val.lower() != "nan" and val != "":
            lowest_func = val
    return lowest_func if lowest_func else "Unknown"

# --- BUILD OUTPUT ---
time_cols = [
    "<1 second",
    "1s - < 1min",
    "1min - < 1hr",
    "1hr - < 1day",
    "1day - < 1week",
    "1 week - < 1 year",
    "1 year or longer",
    "Unkown",
]

base_cols = [
    "Human Organ System",
    "Major Organs"
]

# dynamically add Structure/Cell columns
structure_cols = []
for i, label_col in enumerate(structure_label_cols, start=1):
    structure_cols += [f"Structure_{i}_Label", f"Structure_{i}_ID"]
cell_cols = []
for i, label_col in enumerate(cell_label_cols, start=1):
    cell_cols += [f"Cell_{i}_Label", f"Cell_{i}_ID"]

cols = base_cols + structure_cols + cell_cols + time_cols
output = pd.DataFrame(columns=cols)

# --- MAIN LOOP ---
for _, row in main.iterrows():
    system = row.get("Function/1", "")
    organ = row.get("Structure/1", "")
    process = str(row.get("Process triple", "")).strip()
    scale = row["TimeScale_norm"]

    lowest_func = get_lowest_function(row)
    combined_process = (
        f"{lowest_func}@{process}" if lowest_func != "Unknown" and process else process or lowest_func
    )

    new_row = {col: "" for col in cols}
    new_row["Human Organ System"] = system
    new_row["Major Organs"] = organ

    # fill structures
    for i, label_col in enumerate(structure_label_cols, start=1):
        id_col = re.sub("LABEL", "ID", label_col)
        label_val = str(row.get(label_col, "")).strip()
        id_val = str(row.get(id_col, "")).strip().upper() if id_col in row else ""
        if label_val and label_val.lower() != "nan":
            new_row[f"Structure_{i}_Label"] = label_val
            new_row[f"Structure_{i}_ID"] = id_val

    # fill cells
    for i, label_col in enumerate(cell_label_cols, start=1):
        id_col = re.sub("LABEL", "ID", label_col)
        label_val = str(row.get(label_col, "")).strip()
        id_val = str(row.get(id_col, "")).strip().upper() if id_col in row else ""
        if label_val and label_val.lower() != "nan":
            new_row[f"Cell_{i}_Label"] = label_val
            new_row[f"Cell_{i}_ID"] = id_val

    # assign combined process string to corresponding time columns
    for t in mapping.get(scale, []):
        new_row[t] = combined_process

    output.loc[len(output)] = new_row

# --- SAVE OUTPUT ---
output.to_csv(
    "data/temporal_spatial_data/urinary_system_all_structures_cells_v2.csv",
    index=False,
    encoding="utf-8-sig"
)
print("Created successfully with all Structure/Cell columns + Function@Process in timescale columns!")
print("Total columns:", len(output.columns))
print(output.head(5))
