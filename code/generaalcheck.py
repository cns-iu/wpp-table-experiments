# import os
# import glob
# import pandas as pd

# # --- CONFIGURATION ---
# INPUT_DIR = "./data/WPP Tables"  # 👈 change this to your directory path
# ID_COLUMN = "EffectorLocation/ID"        # The column name to check

# # --- FTU IDs ---
# ftu_ids = {
#     "UBERON:0004203", "UBERON:0001289", "UBERON:0004205", "UBERON:0004193",
#     "UBERON:0001285", "UBERON:0004204", "UBERON:0001229", "UBERON:0001291",
#     "UBERON:0004647", "UBERON:0002299", "UBERON:8410043", "UBERON:0000006",
#     "UBERON:0001263", "UBERON:0014725", "UBERON:0004179", "UBERON:0001983",
#     "UBERON:0000412", "UBERON:0002073", "UBERON:0013487", "UBERON:0001213",
#     "UBERON:0001250", "UBERON:0001959", "UBERON:0002125", "UBERON:0001831",
#     "UBERON:0001832", "UBERON:0001736",
# }

# # --- MAIN SCRIPT ---
# def count_ftu_entries_in_csvs(input_dir: str):
#     csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
#     if not csv_files:
#         print(f"⚠️ No CSV files found in directory: {input_dir}")
#         return

#     print(f"📂 Found {len(csv_files)} CSV files in {input_dir}")
#     print("--------------------------------------------------")

#     summary = []

#     for file_path in csv_files:
#         try:
#             df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
#         except Exception as e:
#             print(f"❌ Error reading {os.path.basename(file_path)}: {e}")
#             continue

#         # Check if the column exists
#         if ID_COLUMN not in df.columns:
#             print(f"⚠️ '{ID_COLUMN}' not found in {os.path.basename(file_path)}")
#             summary.append((os.path.basename(file_path), 0, len(df)))
#             continue

#         # Normalize the column to string & strip whitespace
#         df[ID_COLUMN] = df[ID_COLUMN].astype(str).str.strip()

#         # Count matches
#         count = df[ID_COLUMN].isin(ftu_ids).sum()
#         total = len(df)

#         print(f"✅ {os.path.basename(file_path)} — {count} FTU entries out of {total} rows")

#         summary.append((os.path.basename(file_path), count, total))

#     # --- Summary table ---
#     print("\n📊 Summary:")
#     print("File Name".ljust(40), "FTU Count".rjust(12), "Total Rows".rjust(12))
#     print("-" * 70)
#     for fname, count, total in summary:
#         print(f"{fname.ljust(40)} {str(count).rjust(12)} {str(total).rjust(12)}")

# # --- RUN ---
# if __name__ == "__main__":
#     count_ftu_entries_in_csvs(INPUT_DIR)

#!/usr/bin/env python3
import os
import glob
import pandas as pd

# ----------------------------------------
# 🔧 USER CONFIGURATION
# ----------------------------------------
input_folder = "./data/WPP Tables"
output_path = "./output/analysis/unique_effector_locations_with_ids.csv"

# Candidate column name variants (case-insensitive matching)
EFFECTOR_SCALE_COL_NAMES = ["effector scale", "Effector Scale", "effector_scale", "EffectorScale"]
EFFECTOR_LOCATION_COL_NAMES = ["EffectorLocation", "effector location", "Effector Location", "effector_location"]
EFFECTOR_ID_COL_NAMES = ["EffectorLocation/ID"]

# ----------------------------------------
# Helpers
# ----------------------------------------
def find_column(df, candidates):
    """Return matching column name from df (case-insensitive), or None."""
    cols = list(df.columns)
    lowered = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None

def normalize_value(v):
    """Normalize a value for deduplication: str, strip, collapse whitespace. Return None for empty/NaN."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "":
        return None
    return " ".join(s.split())

# ----------------------------------------
# Main logic
# ----------------------------------------
def collect_effector_locations_with_ids(input_folder, output_path):
    files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not files:
        print(f"No CSV files found in: {input_folder}")
        return

    # mapping: location -> set(ids)
    loc_to_ids = {}
    per_file_counts = {}

    for file_path in files:
        fname = os.path.basename(file_path)
        fname_l = fname.lower()

        # choose header dynamically
        header_row = 12 if "endocrine" in fname_l else 11

        try:
            df = pd.read_csv(file_path, dtype=str, header=header_row)
        except Exception as e:
            # try a fallback encoding
            try:
                df = pd.read_csv(file_path, dtype=str, header=header_row, encoding="utf-8-sig")
            except Exception:
                print(f"[ERROR] Could not read {fname}: {e}")
                continue

        esc_col = find_column(df, EFFECTOR_SCALE_COL_NAMES)
        loc_col = find_column(df, EFFECTOR_LOCATION_COL_NAMES)
        id_col = find_column(df, EFFECTOR_ID_COL_NAMES)

        if esc_col is None:
            print(f"[WARN] Skipping {fname} — 'effector scale' column not found. Columns: {list(df.columns)[:8]}")
            per_file_counts[fname] = 0
            continue
        if loc_col is None:
            print(f"[WARN] Skipping {fname} — 'EffectorLocation' column not found. Columns: {list(df.columns)[:8]}")
            per_file_counts[fname] = 0
            continue
        if id_col is None:
            print(f"[INFO] File {fname} has no ID column (will still collect locations, ID column left blank).")

        # filter rows where effector scale contains 'organ' (case-insensitive)
        mask = df[esc_col].astype(str).str.lower() == "organ"
        matched = df.loc[mask, [loc_col] + ([id_col] if id_col else [])]

        count = 0
        for _, row in matched.iterrows():
            loc_raw = row.get(loc_col, None)
            loc = normalize_value(loc_raw)
            if not loc:
                continue
            id_raw = row.get(id_col, None) if id_col else None
            eid = normalize_value(id_raw)

            # add to mapping
            if loc not in loc_to_ids:
                loc_to_ids[loc] = set()
            if eid:
                loc_to_ids[loc].add(eid)
            count += 1

        per_file_counts[fname] = count

    # Prepare DataFrame: EffectorLocation | EffectorID (semicolon-separated unique IDs)
    rows = []
    for loc, ids in sorted(loc_to_ids.items()):
        id_str = ";".join(sorted(ids)) if ids else ""
        rows.append({"EffectorLocation": loc, "EffectorID": id_str})

    out_df = pd.DataFrame(rows, columns=["EffectorLocation", "EffectorID"])
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    out_df.to_csv(output_path, index=False)

    # Summary
    total_locations = len(out_df)
    total_matched_rows = sum(per_file_counts.values())
    print("\n=== Summary ===")
    print(f"Files scanned: {len(files)}")
    for fn, ct in per_file_counts.items():
        print(f"  {fn}: {ct} matched rows")
    print(f"Total matched rows across files: {total_matched_rows}")
    print(f"Unique EffectorLocation values: {total_locations}")
    print(f"aved to: {output_path}")

# ----------------------------------------
# Run
# ----------------------------------------
if __name__ == "__main__":
    collect_effector_locations_with_ids(input_folder, output_path)
