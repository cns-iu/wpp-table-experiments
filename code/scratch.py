#!/usr/bin/env python3
"""
Extract UBERON IDs and their corresponding labels from ASCTB CSVs.

- Looks for two label/id "pairs":
    1) EffectorLocation label <-> EffectorLocation/ID
    2) Effector label         <-> Effector/ID
  Candidate column names are fuzzy/case-insensitive (see config lists).
- If either ID column contains one or more UBERON IDs (pattern UBERON:digits),
  the script extracts those IDs and associates them with the corresponding label.
- Deduplicates labels and IDs across all files.
- Uses header=12 for filenames that contain "endocrine", otherwise header=11.
- Outputs CSV with columns: AS (label), AS_ID (semi-colon separated UBERON ids).
"""

import os
import glob
import re
import pandas as pd

# -----------------------
# USER CONFIG - edit these paths
# -----------------------
input_folder = "./data/WPP Tables/"
output_tissue_file = "./output/analysis/all_Uberon_ids_in_WPP.csv"

# -----------------------
# Candidate column names (case-insensitive matching)
# -----------------------
# EffectorLocation pair
EFFLOC_LABEL_COLS = [
    "EffectorLocation/LABEL", "EffectorLocation LABEL", "EffectorLocationLabel",
    "EffectorLocation Label", "EffectorLocation_LABEL", "Effector Location LABEL"
]
EFFLOC_ID_COLS = [
    "EffectorLocation/ID", "EffectorLocation ID", "EffectorLocationID",
    "EffectorLocation_ID", "Effector Location ID"
]

# Effector pair
EFF_LABEL_COLS = ["Effector/LABEL", "Effector LABEL", "EffectorLabel", "Effector Label", "LABEL", "label"]
EFF_ID_COLS = ["Effector/ID", "Effector ID", "EffectorID", "effector_id", "ID", "id"]

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

UBERON_RE = re.compile(r"(UBERON:\d+)", re.IGNORECASE)

def extract_uberon_ids(text):
    """Return a list of unique normalized UBERON IDs found in text (upper-case prefix)."""
    if pd.isna(text):
        return []
    s = str(text)
    found = UBERON_RE.findall(s)
    # normalize prefix to exact case "UBERON:digits"
    normalized = []
    for f in found:
        # ensure prefix uppercase and remove stray spaces
        norm = f.upper().replace(" ", "")
        normalized.append(norm)
    return sorted(set(normalized))

# -----------------------
# Main
# -----------------------
def collect_uberon_ids(input_folder, output_tissue_file):
    files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not files:
        print(f"No CSV files found in: {input_folder}")
        return

    tissue_map = {}  # label -> set(UBERON IDs)
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

        # find candidate columns
        effloc_label_col = find_column(df, EFFLOC_LABEL_COLS)
        effloc_id_col = find_column(df, EFFLOC_ID_COLS)
        eff_label_col = find_column(df, EFF_LABEL_COLS)
        eff_id_col = find_column(df, EFF_ID_COLS)

        # We'll scan each row and check both pairs:
        # - if EffectorLocation ID contains UBERON -> use EffectorLocation label (if present)
        # - if Effector ID contains UBERON -> use Effector label (if present)
        file_matches = 0

        # If none of the relevant id columns exist, warn and continue
        if effloc_id_col is None and eff_id_col is None:
            print(f"[WARN] File {fname} has no EffectorLocation/ID or Effector/ID columns. Skipping file.")
            per_file_counts[fname] = 0
            continue

        # iterate rows
        for _, row in df.iterrows():
            # 1) check EffectorLocation ID column
            if effloc_id_col:
                raw_ids = row.get(effloc_id_col)
                ids = extract_uberon_ids(raw_ids)
                if ids:
                    # label from effloc_label_col if available, else try eff_label_col, else use first id as fallback
                    label = clean_text(row.get(effloc_label_col)) if effloc_label_col else None
                    if not label and eff_label_col:
                        label = clean_text(row.get(eff_label_col))
                    if not label:
                        # fallback: use id as label to preserve the association
                        label = ids[0]
                    tissue_map.setdefault(label, set())
                    tissue_map[label].update(ids)
                    file_matches += 1

            # 2) check Effector ID column
            if eff_id_col:
                raw_ids = row.get(eff_id_col)
                ids = extract_uberon_ids(raw_ids)
                if ids:
                    label = clean_text(row.get(eff_label_col)) if eff_label_col else None
                    if not label and effloc_label_col:
                        label = clean_text(row.get(effloc_label_col))
                    if not label:
                        label = ids[0]
                    tissue_map.setdefault(label, set())
                    tissue_map[label].update(ids)
                    file_matches += 1

        per_file_counts[fname] = file_matches

    # Build output DataFrame
    rows = []
    for label, ids in sorted(tissue_map.items(), key=lambda x: (x[0].lower() if x[0] else "")):
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
        print(f"  {fn}: uberon_matches_found={ct}")
    print(f"Total UBERON matches (row-level contributions): {total_tissue_rows}")
    print(f"Unique label values extracted: {len(out_df)} -> saved to: {output_tissue_file}")

# -----------------------
# Run
# -----------------------
if __name__ == "__main__":
    collect_uberon_ids(input_folder, output_tissue_file)
