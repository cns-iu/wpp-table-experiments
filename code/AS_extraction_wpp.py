#!/usr/bin/env python3
"""
Extract unique tissue effectors from CSV files.

- For rows where effector scale == "tissue" (exact, case-insensitive),
  collects Effector/LABEL (as EffectorLabel) and Effector/ID (as EffectorID).
- Deduplicates labels and IDs across all files.
"""

import os
import glob
import pandas as pd

input_folder = "./data/WPP Tables/"
output_tissue_file = "./output/analysis/AS_UBERON_in_WPP.csv"


EFFECTOR_SCALE_COLS = ["effector scale", "Effector Scale", "effector_scale", "EffectorScale"]
TISSUE_LABEL_COLS = ["Effector/LABEL", "Effector LABEL", "EffectorLabel", "Effector Label", "LABEL", "label"]
TISSUE_ID_COLS = ["Effector/ID", "Effector ID", "EffectorID", "effector_id", "ID", "id"]

# -----------------------
# Helpers
# -----------------------
def find_column(df, candidates):
    """Return the first matching column name from df (case-insensitive), or None."""
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        lc = cand.lower()
        if lc in lowered:
            return lowered[lc]
    return None

def clean_text(val):
    """Normalize text for deduplication; return None for empty/NaN."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    return " ".join(s.split())

# -----------------------
# Main
# -----------------------
def collect_tissue_only(input_folder, output_tissue_file):
    files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not files:
        print(f"No CSV files found in: {input_folder}")
        return

    tissue_map = {}  # EffectorLabel -> set(EffectorID)
    per_file_counts = {}

    for fp in files:
        fname = os.path.basename(fp)
        fname_l = fname.lower()
        header_row = 12 if "endocrine" in fname_l else 11

        try:
            df = pd.read_csv(fp, dtype=str, header=header_row)
        except Exception as e:
            # try utf-8-sig fallback then skip
            try:
                df = pd.read_csv(fp, dtype=str, header=header_row, encoding="utf-8-sig")
            except Exception:
                print(f"[ERROR] Could not read {fname}: {e} -- skipping.")
                per_file_counts[fname] = 0
                continue

        esc_col = find_column(df, EFFECTOR_SCALE_COLS)
        tissue_label_col = find_column(df, TISSUE_LABEL_COLS)
        tissue_id_col = find_column(df, TISSUE_ID_COLS)

        if esc_col is None:
            print(f"[WARN] File {fname} has no 'effector scale' column. Skipping file.")
            per_file_counts[fname] = 0
            continue

        esc_series = df[esc_col].astype(str).str.strip().str.lower()
        tissue_mask = esc_series == "tissue"

        tissue_count = 0
        if tissue_mask.any():
            if tissue_label_col is None:
                print(f"[WARN] {fname} has tissue rows but no tissue label column found; tissue rows ignored.")
            else:
                for _, row in df.loc[tissue_mask].iterrows():
                    label = clean_text(row.get(tissue_label_col))
                    if not label:
                        continue
                    tid = clean_text(row.get(tissue_id_col)) if tissue_id_col else None
                    tissue_map.setdefault(label, set())
                    if tid:
                        tissue_map[label].add(tid)
                    tissue_count += 1

        per_file_counts[fname] = tissue_count

    # Build output DataFrame
    rows = []
    for label, ids in sorted(tissue_map.items()):
        id_str = ";".join(sorted(ids)) if ids else ""
        rows.append({"AS": label, "AS_ID": id_str})

    out_df = pd.DataFrame(rows, columns=["AS", "AS_ID"])
    os.makedirs(os.path.dirname(output_tissue_file) or ".", exist_ok=True)
    out_df.to_csv(output_tissue_file, index=False)

    # Summary
    total_tissue_rows = sum(per_file_counts.values())
    print("\n=== Summary ===")
    print(f"Files scanned: {len(files)}")
    for fn, ct in per_file_counts.items():
        print(f"  {fn}: tissue_matches={ct}")
    print(f"Total tissue-matched rows: {total_tissue_rows}")
    print(f"Unique tissue EffectorLabel values: {len(out_df)} -> saved to: {output_tissue_file}")

if __name__ == "__main__":
    collect_tissue_only(input_folder, output_tissue_file)