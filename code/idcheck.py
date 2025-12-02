import pandas as pd
import re

# load CSVs
main = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8-sig")
file2 = pd.read_csv("data/CT_ids_labels.csv")
file3 = pd.read_csv("data/UBERON_ids_labels.csv")

# detect ID and LABEL columns
id_cols = [col for col in main.columns if re.search(r"(Structure|Cell)/\d+/ID", col, re.IGNORECASE)]
label_cols = [col for col in main.columns if re.search(r"(Structure|Cell)/\d+/LABEL", col, re.IGNORECASE)]

print("Detected ID columns:", id_cols)
print("Detected LABEL columns:", label_cols)

# normalize helper
def normalize_id(x):
    if pd.isna(x):
        return None
    s = str(x).strip().upper().replace("_", ":").replace(" ", "")
    return s

# ID→Label mapping
id_label_pairs = []
for id_col, label_col in zip(id_cols, label_cols):
    for id_val, lbl_val in zip(main[id_col], main[label_col]):
        nid = normalize_id(id_val)
        if nid:
            id_label_pairs.append((nid, str(lbl_val).strip() if pd.notna(lbl_val) else None))

main_map = pd.DataFrame(id_label_pairs, columns=["id", "label"]).drop_duplicates(subset=["id"])
main_ids = set(main_map["id"])

# normalize and merge the ID–label pairs
def extract_id_label(df):
    id_col = [c for c in df.columns if 'id' in c.lower()][0]
    label_col = [c for c in df.columns if 'label' in c.lower()][0]
    temp = df[[id_col, label_col]].copy()
    temp.columns = ["id", "label"]
    temp["id"] = temp["id"].apply(normalize_id)
    temp["label"] = temp["label"].astype(str).str.strip()
    return temp.dropna(subset=["id"]).drop_duplicates(subset=["id"])

file2_map = extract_id_label(file2)
file3_map = extract_id_label(file3)
combined_map = pd.concat([file2_map, file3_map], ignore_index=True).drop_duplicates(subset=["id"])

known_ids = set(combined_map["id"])

missing_ids = sorted(main_ids - known_ids)
missing_df = main_map[main_map["id"].isin(missing_ids)].reset_index(drop=True)

print(f"Total Structure/Cell IDs in main: {len(main_ids)}")
print(f"Missing IDs: {len(missing_ids)}")

if not missing_ids:
    print("No missing IDs found!")
else:
    print("\nMissing IDs and labels:")
    print(missing_df)
    missing_df.to_csv("output/missing_ids_labels.csv", index=False)
    print("Saved to data/missing_ids_labels.csv")