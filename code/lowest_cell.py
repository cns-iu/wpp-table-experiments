import pandas as pd
import re

main = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8-sig")
main.columns = main.columns.str.strip()

# normalize timescale
def normalize(val):
    if pd.isna(val):
        return "nan"
    val = str(val).lower().replace("–", "").replace("-", "").replace(" ", "")
    return val.strip()

main["TimeScale_norm"] = main["TimeScale"].apply(normalize)

# mapping
mapping = {
    "milliseconds": ["<1 second"],
    "seconds": ["1s - 1min"],
    "secondsminutes": ["1s - 1min", "1min - 1hr"],
    "minuteshours": ["1min - 1hr", "1hr - 1day"],
    "hoursdays": ["1hr - 1day", "1day - 1week"],
    "daysweeks": ["1day - 1week", "1 week - 1 year"],
    "hours": ["1hr - 1day"],
    "minutes": ["1min - 1hr"],
    "days": ["1day - 1week"],
    "nan": ["nan"],
}

# find label columns
structure_label_cols = [c for c in main.columns if re.search(r"Structure/\d+/LABEL", c)]
cell_label_cols = [c for c in main.columns if re.search(r"Cell/\d+/LABEL", c)]

# determine lowest structure or cell per row
def get_lowest_structure_or_cell(row):
    # check for any cell label first (they're the lowest level)
    cell_vals = [str(row[c]).strip() for c in cell_label_cols if pd.notna(row[c]) and str(row[c]).strip()]
    if cell_vals:
        return f"{cell_vals[-1]}"
    # otherwise fall back to lowest structure label
    struct_vals = [str(row[c]).strip() for c in structure_label_cols if pd.notna(row[c]) and str(row[c]).strip()]
    return struct_vals[-1] if struct_vals else ""

main["Lowest Structure or Cell"] = main.apply(get_lowest_structure_or_cell, axis=1)

# define output columns
cols = [
    "Human Organ System",
    "Major Organs",
    "Lowest Structure or Cell",
    "<1 second",
    "1s - 1min",
    "1min - 1hr",
    "1hr - 1day",
    "1day - 1week",
    "1 week - 1 year",
    "1 year or longer",
    "nan",
]

output = pd.DataFrame(columns=cols)

# build output
for _, row in main.iterrows():
    system = row.get("Function/1", "")
    organ = row.get("Structure/1", "")
    process = row.get("Process triple", "")
    scale = row["TimeScale_norm"]

    new_row = {col: "" for col in cols}
    new_row["Human Organ System"] = system
    new_row["Major Organs"] = organ
    new_row["Lowest Structure or Cell"] = row.get("Lowest Structure or Cell", "")

    # combine Function/4 with Process triple
    func4 = str(row.get("Function/4", "")).strip()
    combined_process = f"{func4}@{process}" if func4 and process else process or func4

    for t in mapping.get(scale, []):
        new_row[t] = combined_process

    output.loc[len(output)] = new_row

output.to_csv("data/lowest_cell_lvl.csv", index=False, encoding="utf-8-sig")

print("Created successfully with lowest structure/cell included!")
