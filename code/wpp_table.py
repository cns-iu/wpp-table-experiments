import pandas as pd
import numpy as np

main = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8")  # adjust header row index if needed

# Normalize TimeScale
def normalize(val):
    if pd.isna(val):
        return "nan"
    val = str(val).lower().replace("–", "").replace("-", "").replace(" ", "")
    return val.strip()

main["TimeScale_norm"] = main["TimeScale"].apply(normalize)
main.columns = main.columns.str.strip()

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
    "nan": ["nan"]
}

cols = [
    "Human Organ System",
    "Major Organs",
    "<1 second",
    "1s - 1min",
    "1min - 1hr",
    "1hr - 1day",
    "1day - 1week",
    "1 week - 1 year",
    "1 year or longer",
    "nan"
]
output = pd.DataFrame(columns=cols)

for _, row in main.iterrows():
    system = row.get("Function/1", "")
    organ = row.get("Structure/1", "")
    process = row.get("Process triple", "")
    scale = row["TimeScale_norm"]

    targets = mapping.get(scale, [])
    new_row = {col: "" for col in cols}
    new_row["Human Organ System"] = system
    new_row["Major Organs"] = organ

    for t in targets:
        new_row[t] = process

    # Append row
    output.loc[len(output)] = new_row

output.to_csv("data/wpp_table.csv", encoding="utf-8", index=False)
print("Created successfully!")