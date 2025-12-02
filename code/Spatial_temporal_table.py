import pandas as pd

# --- LOAD FINAL CSV ---
df = pd.read_csv("data/temporal_spatial_data/urinary_system_lowest_type_from_asctb_matched_v3.csv", encoding="utf-8-sig")

# --- DEFINE TIME COLUMNS ---
time_cols = [
    "<1 second",
    "1s - < 1min",
    "1min - < 1hr",
    "1hr - < 1day",
    "1day - < 1week",
    "1 week - < 1 year",
    "1 year or longer",
]

# --- MELT ---
melted = df.melt(
    id_vars=["Major Organs", "Lowest Type ASCTB Matched"],
    value_vars=time_cols,
    var_name="Time Range",
    value_name="Function@Process"
)
melted = melted.dropna(subset=["Function@Process"])
melted = melted[melted["Function@Process"].astype(str).str.strip() != ""]

# --- GROUP & MERGE BY TIME RANGE + TYPE (collapse across organs) ---
grouped = (
    melted.groupby(["Time Range", "Lowest Type ASCTB Matched"])["Function@Process"]
    .apply(lambda x: "; ".join(sorted(set("; ".join(x).split("; ")))))
    .reset_index()
)

# --- PIVOT TO WIDE TABLE ---
pivot = grouped.pivot(
    index="Time Range",
    columns="Lowest Type ASCTB Matched",
    values="Function@Process"
).fillna("").reset_index()

# --- ENSURE ALL TIME RANGES APPEAR ---
# Reindex so even missing ranges show up as blank rows
pivot = pivot.set_index("Time Range").reindex(time_cols).fillna("").reset_index()
pivot["Time Range"] = pd.Categorical(pivot["Time Range"], categories=time_cols, ordered=True)
pivot = pivot.sort_values("Time Range").reset_index(drop=True)
# --- ENSURE COLUMN ORDER ---
desired_types = ["AS", "FTU", "CT", "B", "Unknown"]
for t in desired_types:
    if t not in pivot.columns:
        pivot[t] = ""

pivot = pivot[["Time Range"] + desired_types]

# --- SAVE OUTPUT ---
pivot.to_csv("output/temporal_spatial_output/urinary_system_spatial_temporal_v2.csv", index=False, encoding="utf-8-sig")

print("Created summary table by Time Range × Type (all time ranges retained)!")
print("Total rows:", len(pivot))
print(pivot.head(10))
