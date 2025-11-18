# #!/usr/bin/env python3
# """
# collect_tissue_effectors.py

# Extract unique tissue effectors from CSV files.

# - For rows where effector scale == "tissue" (exact, case-insensitive),
#   collects Effector/LABEL (as EffectorLabel) and Effector/ID (as EffectorID).
# - Deduplicates labels and IDs across all files.
# - Uses header=12 for filenames containing "endocrine", otherwise header=11.
# """

# import os
# import glob
# import pandas as pd

# # -----------------------
# # USER CONFIG - edit these paths
# # -----------------------
# input_folder = "./data/WPP Tables/"
# output_tissue_file = "./output/analysis/AS_UBERON_in_WPP.csv"

# # Candidate column name variants (extend if your files use other names)
# EFFECTOR_SCALE_COLS = ["effector scale", "Effector Scale", "effector_scale", "EffectorScale"]
# TISSUE_LABEL_COLS = ["Effector/LABEL", "Effector LABEL", "EffectorLabel", "Effector Label", "LABEL", "label"]
# TISSUE_ID_COLS = ["Effector/ID", "Effector ID", "EffectorID", "effector_id", "ID", "id"]

# # -----------------------
# # Helpers
# # -----------------------
# def find_column(df, candidates):
#     """Return the first matching column name from df (case-insensitive), or None."""
#     lowered = {c.lower(): c for c in df.columns}
#     for cand in candidates:
#         if cand in df.columns:
#             return cand
#         lc = cand.lower()
#         if lc in lowered:
#             return lowered[lc]
#     return None

# def clean_text(val):
#     """Normalize text for deduplication; return None for empty/NaN."""
#     if pd.isna(val):
#         return None
#     s = str(val).strip()
#     if s == "":
#         return None
#     return " ".join(s.split())

# # -----------------------
# # Main
# # -----------------------
# def collect_tissue_only(input_folder, output_tissue_file):
#     files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
#     if not files:
#         print(f"No CSV files found in: {input_folder}")
#         return

#     tissue_map = {}  # EffectorLabel -> set(EffectorID)
#     per_file_counts = {}

#     for fp in files:
#         fname = os.path.basename(fp)
#         fname_l = fname.lower()
#         header_row = 12 if "endocrine" in fname_l else 11

#         try:
#             df = pd.read_csv(fp, dtype=str, header=header_row)
#         except Exception as e:
#             # try utf-8-sig fallback then skip
#             try:
#                 df = pd.read_csv(fp, dtype=str, header=header_row, encoding="utf-8-sig")
#             except Exception:
#                 print(f"[ERROR] Could not read {fname}: {e} -- skipping.")
#                 per_file_counts[fname] = 0
#                 continue

#         esc_col = find_column(df, EFFECTOR_SCALE_COLS)
#         tissue_label_col = find_column(df, TISSUE_LABEL_COLS)
#         tissue_id_col = find_column(df, TISSUE_ID_COLS)

#         if esc_col is None:
#             print(f"[WARN] File {fname} has no 'effector scale' column. Skipping file.")
#             per_file_counts[fname] = 0
#             continue

#         esc_series = df[esc_col].astype(str).str.strip().str.lower()
#         tissue_mask = esc_series == "tissue"

#         tissue_count = 0
#         if tissue_mask.any():
#             if tissue_label_col is None:
#                 print(f"[WARN] {fname} has tissue rows but no tissue label column found; tissue rows ignored.")
#             else:
#                 for _, row in df.loc[tissue_mask].iterrows():
#                     label = clean_text(row.get(tissue_label_col))
#                     if not label:
#                         continue
#                     tid = clean_text(row.get(tissue_id_col)) if tissue_id_col else None
#                     tissue_map.setdefault(label, set())
#                     if tid:
#                         tissue_map[label].add(tid)
#                     tissue_count += 1

#         per_file_counts[fname] = tissue_count

#     # Build output DataFrame
#     rows = []
#     for label, ids in sorted(tissue_map.items()):
#         id_str = ";".join(sorted(ids)) if ids else ""
#         rows.append({"EffectorLabel": label, "EffectorID": id_str})

#     out_df = pd.DataFrame(rows, columns=["EffectorLabel", "EffectorID"])
#     os.makedirs(os.path.dirname(output_tissue_file) or ".", exist_ok=True)
#     out_df.to_csv(output_tissue_file, index=False)

#     # Summary
#     total_tissue_rows = sum(per_file_counts.values())
#     print("\n=== Summary ===")
#     print(f"Files scanned: {len(files)}")
#     for fn, ct in per_file_counts.items():
#         print(f"  {fn}: tissue_matches={ct}")
#     print(f"Total tissue-matched rows: {total_tissue_rows}")
#     print(f"Unique tissue EffectorLabel values: {len(out_df)} -> saved to: {output_tissue_file}")

# # -----------------------
# # Run
# # -----------------------
# if __name__ == "__main__":
#     collect_tissue_only(input_folder, output_tissue_file)


################### ASCTB Matched########################
#!/usr/bin/env python3
"""
match_tissue_ids_to_astcb_with_present.py

Compare tissue IDs (from WPP tissue output) against ASTCB master list.

✅ Features:
- Ignores CL-prefixed IDs (CL*, case-insensitive)
- Counts how many tissue IDs are present vs missing in ASTCB
- Saves two output CSVs:
    1. Missing IDs (not found in ASTCB)
    2. Present IDs (found in ASTCB)
- Each output file lists the WPP EffectorLabel(s) referencing that ID.
"""

import os
import pandas as pd

# -----------------------
# USER CONFIG
# -----------------------
tissue_output_file = "./output/analysis/AS_UBERON_in_WPP.csv"        # your WPP tissue file
astcb_master_file = "./data/all_asctb_ids_and_types.csv"                    # ASTCB master file
output_missing_file = "./output/analysis/AS_ids_missing_in_asctb.csv"
output_present_file = "./output/analysis/as_ids_present_in_astcb.csv"
filtered_cl_output = "./output/analysis/tissue_ids_filtered_out_CL.csv"

ID_SEPARATOR = ";"

ASTCB_ID_COL_CANDIDATES = ["id"]

# -----------------------
# Helpers
# -----------------------
def find_column(df, candidates):
    """Return first matching column name from df (case-insensitive)."""
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None

def split_ids_field(id_field, sep=ID_SEPARATOR):
    """Split and normalize semicolon-separated IDs."""
    if pd.isna(id_field) or id_field is None or str(id_field).strip() == "":
        return []
    parts = [p.strip() for p in str(id_field).split(sep)]
    return [p for p in parts if p]

def is_cl_id(idstr):
    return idstr and str(idstr).strip().upper().startswith("CL")

# -----------------------
# Main logic
# -----------------------
def main():
    # ---- Load tissue output ----
    if not os.path.exists(tissue_output_file):
        print(f"[ERROR] Tissue file not found: {tissue_output_file}")
        return
    tissue_df = pd.read_csv(tissue_output_file, dtype=str)

    id_col = "EffectorID" if "EffectorID" in tissue_df.columns else next((c for c in tissue_df.columns if "id" in c.lower()), None)
    if id_col is None:
        print("[ERROR] No ID column found in tissue file.")
        return

    # Build mapping: ID -> set(WPP labels)
    id_to_labels = {}
    cl_ids_set = set()
    total_id_occurrences = 0

    for _, row in tissue_df.iterrows():
        label = row.get("EffectorLabel") or ""
        ids = split_ids_field(row.get(id_col))
        for i in ids:
            total_id_occurrences += 1
            if is_cl_id(i):
                cl_ids_set.add(i)
                continue
            id_to_labels.setdefault(i, set()).add(label)

    print(f"Unique non-CL tissue IDs: {len(id_to_labels)}")
    print(f"Filtered out CL IDs: {len(cl_ids_set)}")

    # Save filtered CL IDs for inspection
    if cl_ids_set:
        pd.DataFrame(sorted(cl_ids_set), columns=["CL_ID"]).to_csv(filtered_cl_output, index=False)
        print(f"Saved filtered CL IDs to: {filtered_cl_output}")

    # ---- Load ASTCB master ----
    if not os.path.exists(astcb_master_file):
        print(f"[ERROR] ASTCB master file not found: {astcb_master_file}")
        return
    astcb_df = pd.read_csv(astcb_master_file, dtype=str)

    astcb_id_col = find_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
    if astcb_id_col is None:
        astcb_id_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
        if astcb_id_col is None:
            print("[ERROR] Could not detect ID column in ASTCB file.")
            return
        print(f"[INFO] Using guessed ASTCB ID column: {astcb_id_col}")

    astcb_ids = {str(v).strip() for v in astcb_df[astcb_id_col].dropna().astype(str) if str(v).strip()}
    print(f"Total unique IDs in ASTCB ({astcb_id_col}): {len(astcb_ids)}")

    # ---- Compare ----
    tissue_ids = set(id_to_labels.keys())
    present_ids = sorted(tissue_ids & astcb_ids)
    missing_ids = sorted(tissue_ids - astcb_ids)

    # ---- Save Missing IDs ----
    missing_rows = [{"MissingID": mid, "WPP_ReferencingLabels": ";".join(sorted(id_to_labels[mid]))} for mid in missing_ids]
    if missing_rows:
        pd.DataFrame(missing_rows).to_csv(output_missing_file, index=False)
        print(f"✅ Saved {len(missing_rows)} missing IDs to: {output_missing_file}")
    else:
        print("✅ No missing tissue IDs — all found in ASTCB.")

    # ---- Save Present IDs ----
    present_rows = [{"PresentID": pid, "WPP_ReferencingLabels": ";".join(sorted(id_to_labels[pid]))} for pid in present_ids]
    if present_rows:
        pd.DataFrame(present_rows).to_csv(output_present_file, index=False)
        print(f"✅ Saved {len(present_rows)} present IDs to: {output_present_file}")
    else:
        print("⚠️ No tissue IDs matched ASTCB entries.")

    # ---- Summary ----
    print("\n=== Summary ===")
    print(f"Total WPP tissue IDs (non-CL): {len(tissue_ids)}")
    print(f"Present in ASTCB: {len(present_ids)}")
    print(f"Missing from ASTCB: {len(missing_ids)}")
    print(f"Filtered out CL IDs: {len(cl_ids_set)}")

if __name__ == "__main__":
    main()
