import os
import glob
import re
import pandas as pd

# ---------- CONFIG ----------
input_folder = "./data/WPP Tables/"
output_file = "./output/analysis/all_CT_statistics/all_CL_ids_in_WPP.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Column name guesses (case-insensitive)
EFF_ID_CANDIDATES = [
    "Effector/ID", "Effector ID", "effector/id", "effector id",
    "EffectorLocation/ID", "EffectorLocation ID"
]

EFF_LABEL_CANDIDATES = [
    "Effector/LABEL", "Effector Label", "effector/label",
    "EffectorLocation/LABEL", "EffectorLocation Label"
]

# ---------- HELPERS ----------
def find_column(df, candidates):
    """Case-insensitive column finder."""
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None

def split_cells(cell):
    """Split on ';' and clean whitespace."""
    if pd.isna(cell):
        return []
    return [p.strip() for p in str(cell).split(";") if p.strip()]

# ---------- MAIN ----------
records = []
files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
if not files:
    raise RuntimeError(f"No CSV files found in {input_folder}")

for fpath in files:
    fname_l = os.path.basename(fpath).lower()
    
    # your rule:
    header_row = 12 if "endocrine" in fname_l else 11
    
    # read CSV with appropriate header row
    try:
        df = pd.read_csv(fpath, dtype=str, header=header_row)
    except Exception as e:
        print(f"⚠️ Could not read {fpath} with header={header_row}: {e}")
        continue

    # find effector columns
    id_col = find_column(df, EFF_ID_CANDIDATES)
    label_col = find_column(df, EFF_LABEL_CANDIDATES)

    if id_col is None:
        continue

    # iterate rows
    for _, row in df.iterrows():
        ids = split_cells(row.get(id_col))
        labels = split_cells(row.get(label_col)) if label_col else []

        for idx, eff_id in enumerate(ids):
            if not eff_id.upper().startswith("CL:"):
                continue

            # match label
            if labels and len(labels) == len(ids):
                eff_label = labels[idx]
            elif labels:
                eff_label = labels[0]
            else:
                eff_label = ""

            records.append({
                "CL_LABELS": eff_label,
                "CL_IDs": eff_id
            })

# ---------- OUTPUT ----------
if records:
    out_df = pd.DataFrame(records)

    # remove duplicate (label, id) pairs
    out_df = out_df.drop_duplicates(subset=["CL_LABELS", "CL_IDs"]).reset_index(drop=True)

    # sort for consistency
    out_df = out_df.sort_values("CL_IDs").reset_index(drop=True)

    out_df.to_csv(output_file, index=False)
    print(f"✓ Saved {len(out_df)} unique CL IDs with labels")
    print("Output:", output_file)
else:
    print("No CL: IDs found.")