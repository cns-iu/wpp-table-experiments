################### ASCTB Matched########################


# Compare tissue IDs (from WPP tissue output) against ASTCB master list.


# import os
# import pandas as pd

# # -----------------------
# # USER CONFIG
# # -----------------------
# tissue_output_file = "./output/analysis/AS_UBERON_in_WPP.csv"        # your WPP tissue file
# astcb_master_file = "./data/all_asctb_ids_and_types.csv"                    # ASTCB master file
# output_missing_file = "./output/analysis/AS_ids_missing_in_asctb.csv"
# output_present_file = "./output/analysis/as_ids_present_in_astcb.csv"
# filtered_cl_output = "./output/analysis/tissue_ids_filtered_out_CL.csv"

# ID_SEPARATOR = ";"

# ASTCB_ID_COL_CANDIDATES = ["id"]

# # -----------------------
# # Helpers
# # -----------------------
# def find_column(df, candidates):
#     """Return first matching column name from df (case-insensitive)."""
#     lowered = {c.lower(): c for c in df.columns}
#     for cand in candidates:
#         if cand in df.columns:
#             return cand
#         if cand.lower() in lowered:
#             return lowered[cand.lower()]
#     return None

# def split_ids_field(id_field, sep=ID_SEPARATOR):
#     """Split and normalize semicolon-separated IDs."""
#     if pd.isna(id_field) or id_field is None or str(id_field).strip() == "":
#         return []
#     parts = [p.strip() for p in str(id_field).split(sep)]
#     return [p for p in parts if p]

# def is_cl_id(idstr):
#     return idstr and str(idstr).strip().upper().startswith("CL")

# # -----------------------
# # Main logic
# # -----------------------
# def main():
#     # ---- Load tissue output ----
#     if not os.path.exists(tissue_output_file):
#         print(f"[ERROR] Tissue file not found: {tissue_output_file}")
#         return
#     tissue_df = pd.read_csv(tissue_output_file, dtype=str)

#     id_col = "EffectorID" if "EffectorID" in tissue_df.columns else next((c for c in tissue_df.columns if "id" in c.lower()), None)
#     if id_col is None:
#         print("[ERROR] No ID column found in tissue file.")
#         return

#     # Build mapping: ID -> set(WPP labels)
#     id_to_labels = {}
#     cl_ids_set = set()
#     total_id_occurrences = 0

#     for _, row in tissue_df.iterrows():
#         label = row.get("EffectorLabel") or ""
#         ids = split_ids_field(row.get(id_col))
#         for i in ids:
#             total_id_occurrences += 1
#             if is_cl_id(i):
#                 cl_ids_set.add(i)
#                 continue
#             id_to_labels.setdefault(i, set()).add(label)

#     print(f"Unique non-CL tissue IDs: {len(id_to_labels)}")
#     print(f"Filtered out CL IDs: {len(cl_ids_set)}")

#     # Save filtered CL IDs for inspection
#     if cl_ids_set:
#         pd.DataFrame(sorted(cl_ids_set), columns=["CL_ID"]).to_csv(filtered_cl_output, index=False)
#         print(f"Saved filtered CL IDs to: {filtered_cl_output}")

#     # ---- Load ASTCB master ----
#     if not os.path.exists(astcb_master_file):
#         print(f"[ERROR] ASTCB master file not found: {astcb_master_file}")
#         return
#     astcb_df = pd.read_csv(astcb_master_file, dtype=str)

#     astcb_id_col = find_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
#     if astcb_id_col is None:
#         astcb_id_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
#         if astcb_id_col is None:
#             print("[ERROR] Could not detect ID column in ASTCB file.")
#             return
#         print(f"[INFO] Using guessed ASTCB ID column: {astcb_id_col}")

#     astcb_ids = {str(v).strip() for v in astcb_df[astcb_id_col].dropna().astype(str) if str(v).strip()}
#     print(f"Total unique IDs in ASTCB ({astcb_id_col}): {len(astcb_ids)}")

#     # ---- Compare ----
#     tissue_ids = set(id_to_labels.keys())
#     present_ids = sorted(tissue_ids & astcb_ids)
#     missing_ids = sorted(tissue_ids - astcb_ids)

#     # ---- Save Missing IDs ----
#     missing_rows = [{"MissingID": mid, "WPP_ReferencingLabels": ";".join(sorted(id_to_labels[mid]))} for mid in missing_ids]
#     if missing_rows:
#         pd.DataFrame(missing_rows).to_csv(output_missing_file, index=False)
#         print(f"✅ Saved {len(missing_rows)} missing IDs to: {output_missing_file}")
#     else:
#         print("✅ No missing tissue IDs — all found in ASTCB.")

#     # ---- Save Present IDs ----
#     present_rows = [{"PresentID": pid, "WPP_ReferencingLabels": ";".join(sorted(id_to_labels[pid]))} for pid in present_ids]
#     if present_rows:
#         pd.DataFrame(present_rows).to_csv(output_present_file, index=False)
#         print(f"✅ Saved {len(present_rows)} present IDs to: {output_present_file}")
#     else:
#         print("⚠️ No tissue IDs matched ASTCB entries.")

#     # ---- Summary ----
#     print("\n=== Summary ===")
#     print(f"Total WPP tissue IDs (non-CL): {len(tissue_ids)}")
#     print(f"Present in ASTCB: {len(present_ids)}")
#     print(f"Missing from ASTCB: {len(missing_ids)}")
#     print(f"Filtered out CL IDs: {len(cl_ids_set)}")

# if __name__ == "__main__":
#     main()

import os
import pandas as pd

# -----------------------
# USER CONFIG
# -----------------------
tissue_output_file = "./output/analysis/all_Uberon_statistics/all_Uberon_ids_in_WPP.csv"        # your WPP tissue file
astcb_master_file = "./data/all_asctb_ids_and_types.csv"                    # ASTCB master file
output_missing_file = "./output/analysis/all_Uberon_statistics/uberon_ids_missing_in_asctb.csv"
output_present_file = "./output/analysis/all_Uberon_statistics/uberon_ids_present_in_astcb.csv"
# filtered_cl_output = "./output/analysis/all_Uberon_statistics/tissue_ids_filtered_out_CL.csv"

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

    id_col = "AS_ID" if "AS_ID" in tissue_df.columns else next((c for c in tissue_df.columns if "id" in c.lower()), None)
    if id_col is None:
        print("[ERROR] No ID column found in tissue file.")
        return

    # Build mapping: ID -> set(WPP labels)
    id_to_labels = {}
    cl_ids_set = set()
    total_id_occurrences = 0

    for _, row in tissue_df.iterrows():
        label = row.get("AS") or ""
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
    # if cl_ids_set:
    #     pd.DataFrame(sorted(cl_ids_set), columns=["CL_ID"]).to_csv(filtered_cl_output, index=False)
    #     print(f"Saved filtered CL IDs to: {filtered_cl_output}")

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
    missing_rows = [{"Missing_AS_ID": mid, "AS": ";".join(sorted(id_to_labels[mid]))} for mid in missing_ids]
    if missing_rows:
        pd.DataFrame(missing_rows).to_csv(output_missing_file, index=False)
        print(f"✅ Saved {len(missing_rows)} missing IDs to: {output_missing_file}")
    else:
        print("✅ No missing tissue IDs — all found in ASTCB.")

    # ---- Save Present IDs ----
    present_rows = [{"Present_AS_ID": pid, "AS": ";".join(sorted(id_to_labels[pid]))} for pid in present_ids]
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