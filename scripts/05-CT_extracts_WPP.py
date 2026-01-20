#!/usr/bin/env python3
"""
Extract unique CL IDs from WPP CSVs and deduplicate by CL ID.

Output:
 - ./output/analysis/all_CT_statistics/all_CL_ids_in_WPP_by_id1.csv
Columns:
 - CL_ID         (e.g. "CL:0000001")
 - LABELS        (all distinct labels that referenced that CL ID, joined by " | ")
 - SOURCE_TABLES (canonical filenames where the CL ID was found, joined by " | ")
"""

import os
import glob
import pandas as pd

# ---------- CONFIG ----------
input_folder = "./data/WPP Input Tables/"
output_file = "./output/analysis/all_CT_statistics/all_CL_ids_in_WPP_by_id.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

ID_LABEL_PAIRS_CANDIDATES = [
    # Effector pair variants
    (["Effector/ID", "Effector ID", "EffectorID", "effector_id", "ID", "id", "AS_ID"],
     ["Effector/LABEL", "Effector LABEL", "EffectorLabel", "Effector Label", "LABEL", "label", "AS"]),
    # EffectorLocation pair variants
    (["EffectorLocation/ID", "EffectorLocation ID", "EffectorLocationID", "effectorlocation_id", "effectorlocationid"],
     ["EffectorLocation/LABEL", "EffectorLocation LABEL", "EffectorLocationLabel", "Effector Location Label"])
]

# -----------------------
# Helpers
# -----------------------
def find_column(df, candidates):
    """Return first matching column name from df (case-insensitive), or None."""
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        lc = cand.lower()
        if lc in lowered:
            return lowered[lc]
    return None

def split_cells(cell, sep=";"):
    """Split on sep and clean whitespace. Returns list of strings (no empty)."""
    if pd.isna(cell) or cell is None:
        return []
    s = str(cell).strip()
    if s == "":
        return []
    parts = [p.strip() for p in s.split(sep)]
    return [p for p in parts if p]

def is_cl_id(idstr):
    """True if idstr (string) starts with CL: (case-insensitive)."""
    if idstr is None:
        return False
    return str(idstr).strip().upper().startswith("CL:")

def normalize_source_name(fname):
    """
    Normalize input table names so the same table is only listed once per ID.
    - lowercases
    - strips extension
    - removes trailing ' - ...' suffix if present
    - converts underscores to hyphens
    - strips surrounding whitespace
    """
    s = fname.strip().lower()
    if " - " in s:
        s = s.split(" - ")[0]
    s = os.path.splitext(s)[0]
    s = s.replace("_", "-")
    s = s.strip()
    return s

# -----------------------
# Main
# -----------------------
def collect_cl_ids_dedupe_by_id(input_folder, output_file):
    files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not files:
        print(f"[ERROR] No CSV files found in: {input_folder}")
        return

    cl_to_labels = {}   # map CL_ID -> set(labels)
    cl_to_sources = {}  # map CL_ID -> set(normalized source names)
    per_file_counts = {}

    for fp in files:
        fname = os.path.basename(fp)
        fname_l = fname.lower()
        # heuristic used in your other scripts
        header_row = 12 if "endocrine" in fname_l else 11

        # robust read with utf-8-sig fallback
        try:
            df = pd.read_csv(fp, dtype=str, header=header_row)
        except Exception:
            try:
                df = pd.read_csv(fp, dtype=str, header=header_row, encoding="utf-8-sig")
            except Exception as e:
                print(f"[WARN] Could not read {fname} (header={header_row}): {e} -- skipping.")
                per_file_counts[fname] = 0
                continue

        # For each candidate pair, detect actual column names present in this file
        found_pairs = []
        for id_cands, label_cands in ID_LABEL_PAIRS_CANDIDATES:
            id_col = find_column(df, id_cands)
            label_col = find_column(df, label_cands)
            # we will process the pair even if label_col is None (we'll use empty label)
            if id_col:
                found_pairs.append((id_col, label_col))

        if not found_pairs:
            # nothing to extract in this file
            per_file_counts[fname] = 0
            continue

        row_count_with_ids = 0
        canonical_fname = normalize_source_name(fname)

        # iterate rows
        for _, row in df.iterrows():
            row_had_id = False
            for id_col, label_col in found_pairs:
                raw_ids = split_cells(row.get(id_col))
                if not raw_ids:
                    continue

                # labels from matching label column if present
                raw_labels = split_cells(row.get(label_col)) if label_col else []

                # process each id with positional mapping if possible
                for idx, raw_id in enumerate(raw_ids):
                    if not is_cl_id(raw_id):
                        continue
                    row_had_id = True
                    # determine label for this id
                    label_for_id = ""
                    if raw_labels:
                        if len(raw_labels) == len(raw_ids):
                            # positional mapping
                            label_for_id = raw_labels[idx]
                        else:
                            # fallback: use first label if available
                            label_for_id = raw_labels[0]

                    # store label (if non-empty) for this CL id
                    cl_key = raw_id.strip()
                    if cl_key not in cl_to_labels:
                        cl_to_labels[cl_key] = set()
                    if label_for_id:
                        cl_to_labels[cl_key].add(label_for_id)

                    # store canonical source filename for this CL id (set prevents duplicates)
                    if cl_key not in cl_to_sources:
                        cl_to_sources[cl_key] = set()
                    cl_to_sources[cl_key].add(canonical_fname)

            if row_had_id:
                row_count_with_ids += 1

        per_file_counts[fname] = row_count_with_ids

    # Build output rows: one row per unique CL ID, labels joined by " | ", sources joined by " | "
    rows = []
    for cl_id in sorted(cl_to_labels.keys(), key=lambda x: x):
        labels = sorted(cl_to_labels[cl_id])
        label_str = " | ".join(labels) if labels else ""
        sources = sorted(cl_to_sources.get(cl_id, set()))
        source_str = " | ".join(sources) if sources else ""
        # produce row with CL_ID first for readability
        rows.append({"LABELS": label_str, "CL_ID": cl_id, "SOURCE_TABLES": source_str})

    out_df = pd.DataFrame(rows, columns=["LABELS", "CL_ID", "SOURCE_TABLES"])
    out_df.to_csv(output_file, index=False)

    # Summary
    total_rows = sum(per_file_counts.values())
    print("\n=== Summary ===")
    print(f"Files scanned: {len(files)}")
    for fn, ct in per_file_counts.items():
        print(f"  {fn}: rows_with_CL_ids={ct}")
    print(f"Total unique CL IDs collected: {len(out_df)} -> saved to: {output_file}")

if __name__ == "__main__":
    collect_cl_ids_dedupe_by_id(input_folder, output_file)
