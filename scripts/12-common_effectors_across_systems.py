#!/usr/bin/env python3
import pandas as pd
import glob
import os
import re

INPUT_FOLDER = "./data/WPP Input Tables/"
OUT_FOLDER = "./common_effectors_across_systems/"
os.makedirs(OUT_FOLDER, exist_ok=True)

# If you use the same header-row heuristic as before:
def header_row_for_filename(fname):
    return 12 if "endocrine" in fname.lower() else 11

# helper: produce short file prefix (first two words from filename without extension)
def file_prefix_from_name(fname):
    base_noext = os.path.splitext(os.path.basename(fname))[0]
    words = re.findall(r"\w+", base_noext)
    if len(words) >= 2:
        return f"{words[0]}_{words[1]}"
    elif len(words) == 1:
        return words[0]
    else:
        # fallback: safe short name
        return re.sub(r'\W+', '_', base_noext)[:40]

# find label column (tries multiple common alternatives)
def find_label_column(df):
    candidates = ["Effector/Label", "Effector/LABEL", "Effector Label", "EffectorLabel", "Effector/label"]
    lc = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None

# find id column (common alternatives)
def find_id_column(df):
    candidates = ["Effector/ID", "Effector/Id", "Effector_ID", "EffectorId"]
    lc = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None

# normalize label for matching (lowercase + collapse whitespace + strip)
def label_key(s):
    if pd.isna(s):
        return None
    t = str(s).strip()
    if t == "" or t.lower() in {"nan", "none", "null"}:
        return None
    # collapse whitespace and normalize unicode-ish spacing
    t2 = re.sub(r"\s+", " ", t)
    return t2.lower()

# split multi-values like "A; B" or "A | B"
def split_multi_values(cell):
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return []
    parts = re.split(r"\s*;\s*|\s*\|\s*|\s*,\s*", s)  # semicolon, pipe, comma
    out = []
    for p in parts:
        p = p.strip()
        if p and p.lower() not in {"nan", "none", "null"}:
            out.append(p)
    return out

files = sorted(glob.glob(os.path.join(INPUT_FOLDER, "**", "*.csv"), recursive=True))
if not files:
    raise SystemExit(f"No CSV files found in {INPUT_FOLDER}")

# maps keyed by normalized label:
# label_to_display[label_key] = first-seen original label (for nicer output)
label_to_display = {}
# label_to_files[label_key] = set of file prefixes where it appears
label_to_files = {}
# label_to_ids[label_key] = set of effector IDs seen for that label across files
label_to_ids = {}

for file_path in files:
    fname = os.path.basename(file_path)
    header_row = header_row_for_filename(fname)

    try:
        df = pd.read_csv(file_path, header=header_row, encoding="utf-8-sig")
    except Exception as e:
        print(f"Skipping {fname}: failed to read CSV ({e})")
        continue

    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    label_col = find_label_column(df)
    id_col = find_id_column(df)

    if label_col is None:
        # nothing to match in this file
        print(f"Skipping {fname}: no label column found (searched common names).")
        continue

    prefix = file_prefix_from_name(fname)

    # iterate rows
    for _, row in df.iterrows():
        label_cell = row.get(label_col, pd.NA)
        label_values = split_multi_values(label_cell)
        if not label_values:
            continue

        # extract ids for this row (could be multi)
        ids_here = []
        if id_col is not None:
            ids_here = split_multi_values(row.get(id_col, pd.NA))

        # For each label value in this row, add file prefix and ids
        for raw_label in label_values:
            k = label_key(raw_label)
            if k is None:
                continue

            # store display version (first seen)
            if k not in label_to_display:
                label_to_display[k] = raw_label  # keep first-seen original casing

            label_to_files.setdefault(k, set()).add(prefix)
            if ids_here:
                label_to_ids.setdefault(k, set()).update(ids_here)
            else:
                # keep an explicit empty set if not present yet (so it's easier later)
                label_to_ids.setdefault(k, set())

rows = []
for k, fileset in label_to_files.items():
    if len(fileset) >= 2:
        display_label = label_to_display.get(k, k)
        ids = sorted(label_to_ids.get(k, []))
        rows.append({
            "Effector/LABEL": display_label,
            "Effector/ID(s)": ";".join(ids) if ids else "",
            "Files": ";".join(sorted(fileset)),
            "Count_files": len(fileset)
        })

# write results
out_path = os.path.join(OUT_FOLDER, "labels_present_in_multiple_files.csv")
if rows:
    out_df = pd.DataFrame(rows)
    # optional: sort by number of files desc, then label
    out_df = out_df.sort_values(by=["Count_files", "Effector/LABEL"], ascending=[False, True])
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(out_df)} labels (present in 2+ files) -> {out_path}")
else:
    # still write an empty template for convenience
    pd.DataFrame(columns=["Effector/LABEL", "Effector/ID(s)", "Files", "Count_files"]).to_csv(out_path, index=False, encoding="utf-8-sig")
    print("No labels found in 2 or more input files. Wrote empty template to", out_path)

print("Done.")
