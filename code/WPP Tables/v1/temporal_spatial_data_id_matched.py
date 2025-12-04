import pandas as pd
import re

# --- LOAD FILES ---
main = pd.read_csv("data/temporal_spatial_data/male_reproductive_all_structures_cells_v2.csv", encoding="utf-8-sig")
asctb = pd.read_csv("data/all_asctb_ids_and_types.csv", encoding="utf-8-sig")

# --- NORMALIZE ID COLUMNS ---
asctb["id"] = asctb["id"].astype(str).str.strip().str.upper()
id_type_map = dict(zip(asctb["id"], asctb["cf_asctb_type"]))
id_label_map = dict(zip(asctb["id"], asctb["label"]))

# --- DETECT STRUCTURE & CELL ID COLUMNS ---
structure_id_cols = [c for c in main.columns if re.search(r"Structure_\d+_ID", c)]
cell_id_cols = [c for c in main.columns if re.search(r"Cell_\d+_ID", c)]
all_id_cols = structure_id_cols + cell_id_cols

structure_label_cols = [c for c in main.columns if re.search(r"Structure_\d+_Label", c)]
cell_label_cols = [c for c in main.columns if re.search(r"Cell_\d+_Label", c)]
all_label_cols = structure_label_cols + cell_label_cols

# --- LOAD FTU IDS ---
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
    "UBERON:0001736"
}

# --- FIND LOWEST MATCHED STRUCTURE OR FTU ---
def find_lowest_in_asctb(row):
    """
    From all structure/cell IDs (lowest to highest), find the deepest one
    that exists in ASCT+B or FTU list.
    """
    lowest_id = ""
    lowest_label = ""
    lowest_type = "Unknown"

    # Combine structure + cell pairs in order (Structure_1 → ..., Cell_1 → ...)
    pairs = []
    for i in range(max(len(all_id_cols), len(all_label_cols))):
        if i < len(structure_id_cols):
            pairs.append((structure_label_cols[i], structure_id_cols[i]))
        if i < len(cell_id_cols):
            pairs.append((cell_label_cols[i], cell_id_cols[i]))

    # Iterate bottom-up (lowest first)
    for label_col, id_col in reversed(pairs):
        val = str(row.get(id_col, "")).strip().upper()
        if not val:
            continue

        # --- 1️⃣ Check FTU first ---
        if val in ftu_ids:
            lowest_id = val
            lowest_label = str(row.get(label_col, "")).strip()
            lowest_type = "FTU"
            break

        # --- 2️⃣ Otherwise check ASCT+B ---
        if val in id_type_map:
            lowest_id = val
            lowest_label = str(row.get(label_col, "")).strip()
            lowest_type = id_type_map[val]
            break

    return pd.Series({
        "Lowest Structure ASCTB Matched": lowest_label,
        "Lowest ID ASCTB Matched": lowest_id,
        "Lowest Type ASCTB Matched": lowest_type
    })

# --- APPLY TO MAIN DATAFRAME ---
lowest_info = main.apply(find_lowest_in_asctb, axis=1)
main = pd.concat([main, lowest_info], axis=1)

# --- BUILD FINAL OUTPUT ---
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

final_cols = [
    "Human Organ System",
    "Major Organs",
    "Lowest Structure ASCTB Matched",
    "Lowest ID ASCTB Matched",
    "Lowest Type ASCTB Matched",
] + time_cols

output = main[final_cols]

# --- SAVE OUTPUT ---
output.to_csv("data/temporal_spatial_data/male_reproductive_lowest_type_from_asctb_matched_v3.csv", index=False, encoding="utf-8-sig")
print("Created successfully using ASCT+B match for deepest available structure/cell!")
print("Matched entries:", (output['Lowest Type ASCTB Matched'] != 'Unknown').sum(), "of", len(output))
print(output.head(10))
