#!/usr/bin/env python3
import os
import pandas as pd

# -----------------------
# USER CONFIG
# -----------------------
cl_ids_file = "./output/analysis/all_CT_statistics/all_CL_ids_in_WPP_by_id.csv"  # your extracted CL file
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

def is_cl_id(x):
    return isinstance(x, str) and x.strip().upper().startswith("CL:")

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

    # tolerate alternate column names
    if "CL_IDs" not in cl_df.columns and "CL_ID" in cl_df.columns:
        cl_df = cl_df.rename(columns={"CL_ID": "CL_IDs"})
    if "CL_LABELS" not in cl_df.columns and "CL_LABEL" in cl_df.columns:
        cl_df = cl_df.rename(columns={"CL_LABEL": "CL_LABELS"})

    if "CL_IDs" not in cl_df.columns:
        print("[ERROR] Input CL file lacks CL_IDs column.")
        return

    # build mapping CL_ID -> CL_LABEL (prefer first-seen label)
    id_to_label = {}
    for _, row in cl_df.iterrows():
        label = (row.get("CL_LABELS") or "") or ""
        ids = split_semicolons(row.get("CL_IDs", ""))
        for cid in ids:
            key = cid.strip()
            if not key:
                continue
            if key not in id_to_label:
                id_to_label[key] = label

    all_cl_ids = set(id_to_label.keys())
    print(f"Total CL IDs loaded from WPP file: {len(all_cl_ids)}")

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

    # collect unique ASTCB IDs (raw)
    astcb_ids_raw = {str(x).strip() for x in astcb_df[astcb_col].dropna() if str(x).strip()}
    print(f"Total unique raw IDs in ASTCB ({astcb_col}): {len(astcb_ids_raw)}")

    # restrict to CL: IDs in ASTCB (HRA)
    astcb_cl_ids = {x for x in astcb_ids_raw if is_cl_id(x)}
    total_cl_in_hra = len(astcb_cl_ids)
    print(f"Total CL-type IDs in ASTCB (HRA): {total_cl_in_hra}")

    # compute intersections and differences (CL-only)
    present_cl_ids = sorted(all_cl_ids & astcb_cl_ids)           # in both WPP and ASTCB (CL only)
    missing_cl_ids = sorted(all_cl_ids - astcb_cl_ids)           # in WPP but not in ASTCB
    only_in_hra_cl_ids = sorted(astcb_cl_ids - all_cl_ids)       # in ASTCB but not in WPP

    # save present (intersection) and missing (WPP-only) as before, attaching labels
    present_rows = []
    for pid in present_cl_ids:
        present_rows.append({
            "CL_LABELS": id_to_label.get(pid, ""),
            "CL_IDs": pid
        })

    missing_rows = []
    for mid in missing_cl_ids:
        missing_rows.append({
            "CL_LABELS": id_to_label.get(mid, ""),
            "CL_IDs": mid
        })

    # write outputs (dedup again by label+id just in case)
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

    # final summary: counts and relationships
    print("\n=== SUMMARY ===")
    print(f"Total CL IDs checked (WPP): {len(all_cl_ids)}")
    print(f"HRA intersection (present in both) => {len(present_cl_ids)}")
    print(f"Only in WPP => {len(missing_cl_ids)}")
    print(f"Total CL IDs in HRA (ASTCB): {total_cl_in_hra}")
    # Only in HRA = total_cl_in_hra - intersection
    only_in_hra_count = len(only_in_hra_cl_ids)
    print(f"Only in HRA => {only_in_hra_count}")

    # optionally show small samples (uncomment if you want)
    # print("Sample present CL IDs:", present_cl_ids[:10])
    # print("Sample only-in-HRA CL IDs:", only_in_hra_cl_ids[:10])

if __name__ == "__main__":
    main()
