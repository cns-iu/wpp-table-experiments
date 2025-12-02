import pandas as pd
import re

main = pd.read_csv("data/Female-reproductive-system_v1.0_DRAFT - Sheet1.csv", header=11, encoding="utf-8-sig")
main.columns = main.columns.str.strip()

# Columns: organ, id, cf_asctb_type, label
asctb = pd.read_csv("data/all_asctb_ids_and_types.csv", encoding="utf-8-sig")
asctb.columns = asctb.columns.str.strip()

# Normalize ASCTB IDs
asctb["id_norm"] = asctb["id"].astype(str).str.strip().str.upper()
asctb_ids = set(asctb["id_norm"])

# --- Step 3: Identify Structure and Cell columns separately ---
structure_label_cols = [c for c in main.columns if re.search(r"^Structure/\d+/LABEL$", c, re.IGNORECASE)]
structure_id_cols    = [c for c in main.columns if re.search(r"^Structure/\d+/ID$", c, re.IGNORECASE)]
cell_label_cols      = [c for c in main.columns if re.search(r"^Cell/\d+/LABEL$", c, re.IGNORECASE)]
cell_id_cols         = [c for c in main.columns if re.search(r"^Cell/\d+/ID$", c, re.IGNORECASE)]

def extract_index(c):
    m = re.search(r"/(\d+)/", c)
    return int(m.group(1)) if m else 0

# Sort descending → check deepest first
structure_label_cols = sorted(structure_label_cols, key=extract_index, reverse=True)
structure_id_cols    = sorted(structure_id_cols, key=extract_index, reverse=True)
cell_label_cols      = sorted(cell_label_cols, key=extract_index, reverse=True)
cell_id_cols         = sorted(cell_id_cols, key=extract_index, reverse=True)

# --- Step 4: Helper to find lowest non-empty label–ID in a group ---
def find_lowest_non_empty(row, label_cols, id_cols):
    for l_col, i_col in zip(label_cols, id_cols):
        label = str(row.get(l_col, "")).strip()
        id_ = str(row.get(i_col, "")).strip().upper()
        if (
            label
            and id_
            and id_.lower() not in ["nan", "none", ""]
            and not id_.startswith("GO:")
        ):
            return label, id_
    return None, None

# --- Step 5: Apply hierarchy — Cells first, else Structures ---
def get_lowest_cell_or_structure(row):
    label, id_ = find_lowest_non_empty(row, cell_label_cols, cell_id_cols)
    if label and id_:
        return label, id_, "Cell"
    # fallback to Structure if no cell found
    label, id_ = find_lowest_non_empty(row, structure_label_cols, structure_id_cols)
    if label and id_:
        return label, id_, "Structure"
    return None, None, "Unknown"

main[["Label", "ID", "Level_Type"]] = main.apply(
    lambda r: pd.Series(get_lowest_cell_or_structure(r)), axis=1
)

# --- Step 6: Drop empty and duplicate IDs ---
main = main.dropna(subset=["ID"])
main = main[main["ID"] != ""]
main = main.drop_duplicates(subset=["ID"])

# --- Step 7: Check in ASCT+B ---
main["Found_in_ASCTB"] = main["ID"].apply(
    lambda x: "Found" if x.strip().upper() in asctb_ids else "Not found"
)

# --- Step 8: Fill blanks and save ---
main["Label"] = main["Label"].fillna("Unknown")
main["ID"] = main["ID"].fillna("Unknown")
main["Level_Type"] = main["Level_Type"].fillna("Unknown")

final = main[["Label", "ID", "Level_Type", "Found_in_ASCTB"]].reset_index(drop=True)

final.to_csv("data/lowest_missing_female_reproductive_data.csv", index=False, encoding="utf-8-sig")
print(final.head(10))
print(f"\nTotal rows: {len(final)} | Found: {(final['Found_in_ASCTB'] == 'Found').sum()} | Not found: {(final['Found_in_ASCTB'] == 'Not found').sum()}")