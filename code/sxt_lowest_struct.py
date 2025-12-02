import pandas as pd
import re
import json

# --- LOAD MAIN CSV ---
main = pd.read_csv("data/Male-reproductive-system_v1.0_DRAFT - Sheet1.csv", header=11, encoding="utf-8-sig")
main.columns = main.columns.str.strip()

# --- LOAD STRUCTURE JSON ---
with open("data/kidney.json", "r", encoding="utf-8") as f:
    data_json = json.load(f)

# --- Build both ID→Type and Label→Type maps ---
id_type_map = {}
label_type_map = {}

for section in ["anatomical_structures", "cell_types", "functional_tissue_units"]:
    for item in data_json.get("data", {}).get(section, []):
        label = str(item.get("ccf_pref_label", "")).strip().lower()
        typ = str(item.get("ccf_asctb_type", "")).strip().upper()
        cid = str(item.get("id", "")).strip().upper()  # ← JSON ID field
        if cid:
            id_type_map[cid] = typ
        if label:
            label_type_map[label] = typ

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

# --- STRUCTURE LABEL + ID COLUMNS ---
structure_label_cols = [c for c in main.columns if re.search(r"Structure/\d+/LABEL", c)]
structure_id_cols = [c for c in main.columns if re.search(r"Structure/\d+/ID", c)]

# --- FIND LOWEST STRUCTURE + ID ---
# def get_lowest_structure_and_id(row):
#     labels = []
#     ids = []
#     for label_col in structure_label_cols:
#         val = str(row[label_col]).strip() if pd.notna(row[label_col]) else ""
#         if val:
#             labels.append(val)
#     for id_col in structure_id_cols:
#         val = str(row[id_col]).strip().upper() if pd.notna(row[id_col]) else ""
#         if val:
#             ids.append(val)
#     if labels:
#         lowest_structure = labels[-1]
#         lowest_id = ids[-1] if len(ids) >= len(labels) else ""
#     else:
#         lowest_structure = ""
#         lowest_id = ""
#     return lowest_structure, lowest_id
def get_lowest_structure_and_id(row):
    """
    Finds the deepest non-empty label+ID among all Structure/x and Cell/x columns.
    Works even if some intermediate columns like 'Structure/6' exist without LABEL/ID.
    """
    lowest_label = ""
    lowest_id = ""

    # Gather all Structure and Cell label-ID pairs (sorted by numeric index)
    structure_pairs = []
    cell_pairs = []

    # Find all Structure/x/LABEL columns dynamically
    for col in row.index:
        match = re.match(r"(Structure|Cell)/(\d+)/LABEL", col)
        if match:
            prefix = match.group(1)
            idx = int(match.group(2))
            id_col = f"{prefix}/{idx}/ID"
            if prefix == "Structure":
                structure_pairs.append((col, id_col))
            else:
                cell_pairs.append((col, id_col))

    # Sort them numerically
    structure_pairs.sort(key=lambda x: int(re.search(r"/(\d+)/", x[0]).group(1)))
    cell_pairs.sort(key=lambda x: int(re.search(r"/(\d+)/", x[0]).group(1)))

    # Combine both (structures first, then cells)
    all_pairs = structure_pairs + cell_pairs

    for label_col, id_col in all_pairs:
        label_val = str(row[label_col]).strip() if pd.notna(row[label_col]) else ""
        if label_val:
            lowest_label = label_val
            id_val = str(row.get(id_col, "")).strip().upper() if pd.notna(row.get(id_col, "")) else ""
            lowest_id = id_val

    return lowest_label, lowest_id




main[["Lowest Structure", "Lowest ID"]] = main.apply(
    lambda row: pd.Series(get_lowest_structure_and_id(row)), axis=1
)

# --- FIND LOWEST TYPE (now based on ID first) ---
def get_lowest_type(row):
    low_id = str(row.get("Lowest ID", "")).strip().upper()
    low_label = str(row.get("Lowest Structure", "")).strip().lower()
    if low_id in id_type_map:
        return id_type_map[low_id]
    elif low_label in label_type_map:
        return label_type_map[low_label]
    return "Unknown"

main["Lowest Type"] = main.apply(get_lowest_type, axis=1)

# --- BUILD OUTPUT TABLE ---
cols = [
    "Human Organ System",
    "Major Organs",
    "Lowest Structure",
    "Lowest ID",
    "Lowest Type",
    "<1 second",
    "1s - < 1min",
    "1min - < 1hr",
    "1hr - < 1day",
    "1day - < 1week",
    "1 week - < 1 year",
    "1 year or longer",
    "Unkown",
]

output = pd.DataFrame(columns=cols)

func_cols = [
    c for c in main.columns
    if re.search(r"^Function\s*/?\s*\d+$", c, re.IGNORECASE)
]
func_cols.sort(key=lambda c: int(re.search(r"\d+", c).group()))


def get_lowest_function(row):
    """
    Finds the deepest non-empty Function/x value.
    Works even if some intermediate levels like 'Function/5' are empty.
    """
    lowest_func = ""

    # Gather all Function/x columns dynamically (sorted by numeric index)
    function_cols = []
    for col in row.index:
        match = re.match(r"Function/(\d+)$", col.strip())
        if match:
            idx = int(match.group(1))
            function_cols.append((idx, col))

    # Sort by numeric index
    function_cols.sort(key=lambda x: x[0])

    # Iterate from top → bottom (you could reverse for bottom-first)
    for _, col in function_cols:
        val = str(row.get(col, "")).strip()
        if pd.notna(val) and val.lower() != "nan" and val != "":
            lowest_func = val

    return lowest_func if lowest_func else "Unknown"


for _, row in main.iterrows():
    system = row.get("Function/1", "")
    organ = row.get("Structure/1", "")
    process = row.get("Process triple", "")
    scale = row["TimeScale_norm"]

    new_row = {col: "" for col in cols}
    new_row["Human Organ System"] = system
    new_row["Major Organs"] = organ
    new_row["Lowest Structure"] = row.get("Lowest Structure", "")
    new_row["Lowest ID"] = row.get("Lowest ID", "")
    new_row["Lowest Type"] = row.get("Lowest Type", "")

    lowest_func = get_lowest_function(row)
    combined_process = f"{lowest_func}@{process}" if lowest_func != "Unknown" and process else process or lowest_func

    for t in mapping.get(scale, []):
        new_row[t] = combined_process

    output.loc[len(output)] = new_row

# --- SAVE OUTPUT ---
output.to_csv("data/temporal_spatial_data/male_reproductive_lowest_type_with_id.csv", index=False, encoding="utf-8-sig")
print("Created successfully with Lowest Structure + ID + Type (matched by ID)!")
# print(main[["Function/1", "Function/2", "Function/3", "Function/4", "Function/5"]].head(10))


# print(main["TimeScale_norm"].value_counts().head(20))
# print("Detected function columns:", func_cols)
