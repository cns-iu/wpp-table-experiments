import pandas as pd
import re

main = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8")
file2 = pd.read_csv("data/CT_ids.csv")
file3 = pd.read_csv("data/UBERON_ids.csv")

id_cols = [
    col for col in main.columns
    if re.search(r"(Structure|Cell)/\d+/ID", col, re.IGNORECASE)
]
print("Detected ID columns:", id_cols)

main_ids = pd.unique(main[id_cols].values.ravel()) if id_cols else []

id_cols_2 = [col for col in file2.columns if 'id' in col.lower()]
id_cols_3 = [col for col in file3.columns if 'id' in col.lower()]

file2_ids = pd.unique(file2[id_cols_2].values.ravel()) if id_cols_2 else []
file3_ids = pd.unique(file3[id_cols_3].values.ravel()) if id_cols_3 else []

# Normalize IDs
def normalize_id(x):
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    s = s.replace("_", ":")  
    s = s.replace(" ", "")
    return s

main_ids = {normalize_id(x) for x in main_ids if pd.notna(x)}
file2_ids = {normalize_id(x) for x in file2_ids if pd.notna(x)}
file3_ids = {normalize_id(x) for x in file3_ids if pd.notna(x)}

all_other_ids = file2_ids | file3_ids
missing_ids = sorted(main_ids - all_other_ids)

print(f"Total Structure/Cell IDs: {len(main_ids)}")
print(f"Missing IDs: {len(missing_ids)}")

if missing_ids:
    print("\n".join(missing_ids))
else:
    print("No missing IDs found!")

pd.DataFrame({"missing_id": missing_ids}).to_csv("data/missing_ids.csv", index=False)
print("Saved successfully!")
