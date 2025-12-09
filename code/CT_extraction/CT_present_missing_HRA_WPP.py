#!/usr/bin/env python3
import os
import pandas as pd

# -----------------------
# USER CONFIG
# -----------------------
cl_ids_file = "./output/analysis/all_CT_statistics/all_CL_ids_in_WPP.csv"              # your extracted CL file
astcb_master_file = "./data/all_asctb_ids_and_types.csv"        # master file
output_missing = "./output/analysis/all_CT_statistics/cl_ids_missing_in_astcb.csv"
output_present = "./output/analysis/all_CT_statistics/cl_ids_present_in_astcb.csv"

ASTCB_ID_COL_CANDIDATES = ["id", "ID", "asctb_id"]

# ---------- HELPERS ----------
def find_column(df, candidates):
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None

def split_semicolons(cell):
    """Split semicolon-separated values safely and strip whitespace."""
    if pd.isna(cell):
        return []
    s = str(cell)
    parts = [p.strip() for p in s.split(";") if p.strip() != ""]
    return parts

# ---------- MAIN ----------
def main():
    # check inputs
    if not os.path.exists(cl_ids_file):
        print(f"[ERROR] CL file not found: {cl_ids_file}")
        return
    if not os.path.exists(astcb_master_file):
        print(f"[ERROR] ASTCB master not found: {astcb_master_file}")
        return

    # read CL file (expects columns CL_LABELS and CL_IDs, but robust if multiple IDs in a cell)
    cl_df = pd.read_csv(cl_ids_file, dtype=str)

    # build mapping CL_ID -> CL_LABEL (if multiple IDs in a row, use the same label for each)
    id_to_label = {}
    if "CL_IDs" not in cl_df.columns and "CL_ID" in cl_df.columns:
        cl_df = cl_df.rename(columns={"CL_ID": "CL_IDs"})
    if "CL_LABELS" not in cl_df.columns and "CL_LABEL" in cl_df.columns:
        cl_df = cl_df.rename(columns={"CL_LABEL": "CL_LABELS"})

    if "CL_IDs" not in cl_df.columns:
        print("[ERROR] Input CL file lacks CL_IDs column.")
        return

    for _, row in cl_df.iterrows():
        label = row.get("CL_LABELS", "") or ""
        ids = split_semicolons(row.get("CL_IDs", ""))
        for cid in ids:
            key = cid.strip()
            if not key:
                continue
            # prefer first-seen label for an ID
            if key not in id_to_label:
                id_to_label[key] = label

    all_cl_ids = set(id_to_label.keys())
    print(f"Total CL IDs loaded from CL file: {len(all_cl_ids)}")

    # read ASTCB master and find ID column
    astcb_df = pd.read_csv(astcb_master_file, dtype=str)
    astcb_col = find_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
    if astcb_col is None:
        # fallback: try any column with 'id' in name
        astcb_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
        if astcb_col is None:
            print("[ERROR] Could not find an ID column in ASTCB master.")
            return
        print(f"[INFO] Using guessed ASTCB ID column: {astcb_col}")

    astcb_ids = {str(x).strip() for x in astcb_df[astcb_col].dropna() if str(x).strip()}
    print(f"Total unique IDs in ASTCB ({astcb_col}): {len(astcb_ids)}")

    # compare
    present_ids = sorted(all_cl_ids & astcb_ids)
    missing_ids = sorted(all_cl_ids - astcb_ids)

    print(f"Present in ASTCB: {len(present_ids)}")
    print(f"Missing from ASTCB: {len(missing_ids)}")

    # Build output DataFrames (label first)
    present_rows = []
    for pid in present_ids:
        present_rows.append({
            "CL_LABELS": id_to_label.get(pid, ""),
            "CL_IDs": pid
        })

    missing_rows = []
    for mid in missing_ids:
        missing_rows.append({
            "CL_LABELS": id_to_label.get(mid, ""),
            "CL_IDs": mid
        })

    if present_rows:
        pd.DataFrame(present_rows).drop_duplicates(subset=["CL_LABELS", "CL_IDs"]).to_csv(output_present, index=False)
        print(f"Saved present CL IDs with labels → {output_present}")
    else:
        print("No present CL IDs to save.")

    if missing_rows:
        pd.DataFrame(missing_rows).drop_duplicates(subset=["CL_LABELS", "CL_IDs"]).to_csv(output_missing, index=False)
        print(f"Saved missing CL IDs with labels → {output_missing}")
    else:
        print("No missing CL IDs to save.")

    # summary
    print("\n=== SUMMARY ===")
    print(f"Total CL IDs checked: {len(all_cl_ids)}")
    print(f"Present in ASTCB: {len(present_ids)}")
    print(f"Missing in ASTCB: {len(missing_ids)}")

if __name__ == "__main__":
    main()
