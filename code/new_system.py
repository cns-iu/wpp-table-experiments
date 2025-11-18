import pandas as pd
import re

# --- CONFIGURATION ---
MAIN_CSV_PATH = "./data/WPP Tables/Muscular-system_v1.0_DRAFT_20251112 - Sheet1.csv"
# Updated output path to reflect the new mapping
OUTPUT_PATH = "./output/temporal_spatial_output/v3/muscular_system_effector_scale_spatial_temporal_v3.csv"

# --- TIME SCALE RANGES ---
TIME_COLUMNS = [
    "<1 second",
    "1s - < 1min",
    "1min - < 1hr",
    "1hr - < 1day",
    "1day - < 1week",
    "1 week - < 1 year",
    "1 year or longer",
    "Unknown",
]

# --- MAPPING FOR TIME SCALE RANGES ---
TIME_MAPPING = {
    "milliseconds": ["<1 second"],
    "seconds": ["1s - < 1min"],
    "secondsminutes": ["1s - < 1min", "1min - < 1hr"],
    "minuteshours": ["1min - < 1hr", "1hr - < 1day"],
    "hoursdays": ["1hr - < 1day", "1day - < 1week"],
    "daysweeks": ["1day - < 1week", "1 week - < 1 year"],
    "hours": ["1hr - < 1day"],
    "minutes": ["1min - < 1hr"],
    "days": ["1day - < 1week"],
    "nan": ["Unknown"],
}

# -------------------------------------------------------------------------
# --- SPATIAL MAPPING (UPDATED FOR CASE-SENSITIVE KEYS) ---
# We'll use the exact strings found in the data as keys.
# Note: "tissue(ftu)" is likely lowercase/special character, so we keep that pattern.
# We map all 'Tissue' variants to AS/FTU, Cell to CT, Organ/Organ System to Organ.
SPATIAL_MAPPING = {
    "Tissue": "AS", 
    "Tissue(FTU)": "FTU", # Assuming this one might remain capitalized or use caps
    "tissue": "FTU", # For safety, keep the lowercase version
    "Cell": "CT",
    "cell": "CT",
    "Organ": "Organ",
    "organ": "Organ",
    "Organ system": "Organ", 
    "Biomolecule": "B",
    "Molecule": "B",
    "Subcellular": "Unknown",
    "Organism": "Unknown",
    "nan": "Unknown",
    "": "Unknown"
}
# -------------------------------------------------------------------------

# --- DATA LOADING AND INITIAL CLEANING ---
try:
    main = pd.read_csv(
        MAIN_CSV_PATH,
        header=11,
        encoding="utf-8-sig"
    )
except FileNotFoundError:
    print(f"Error: The file '{MAIN_CSV_PATH}' was not found.")
    exit()

main.columns = main.columns.str.strip()

# --- HELPER FUNCTIONS ---

def normalize_time(val):
    """Normalize TimeScale value for mapping."""
    if pd.isna(val):
        return "nan"
    val = str(val).lower()
    val = re.sub(r"[–,\-\s]", "", val)
    return val.strip()

def normalize_spatial(val):
    """
    Normalize and map EffectorScale to a spatial type.
    We strip whitespace and rely on the exact keys in SPATIAL_MAPPING, 
    since the user confirmed capitalization is important.
    """
    if pd.isna(val):
        return SPATIAL_MAPPING.get("nan")
        
    val = str(val).strip()
    
    # Check for direct match first
    if val in SPATIAL_MAPPING:
        return SPATIAL_MAPPING[val]
    
    # Handle the normalized/mixed case scenarios for better robustness
    normalized_key = val.replace('(', '').replace(')', '').replace('/', '').replace(' ', '')
    
    # Specific check for Tissue(FTU) where capitalization might be tricky
    if normalized_key.upper() == "TISSUEFTU":
        return "FTU"
    
    # Specific check for Organ System
    if normalized_key.upper() == "ORGANSYSTEM":
        return "Organ"
        
    # Default fallback
    return SPATIAL_MAPPING.get(val, "Unknown")


def get_lowest_function(row):
    """Return deepest non-empty Function/x."""
    lowest_func = ""
    function_cols = [col for col in row.index if re.match(r"Function/\d+$", col.strip())]
    function_cols.sort(key=lambda c: int(re.search(r"\d+", c).group()))

    for col in function_cols:
        val = str(row.get(col, "")).strip()
        if pd.notna(val) and val.lower() != "nan" and val != "":
            lowest_func = val
    return lowest_func if lowest_func else "Unknown"

# -----------------------------------------------------------
## STEP 1: Prepare Main Data
# -----------------------------------------------------------

main["TimeScale_norm"] = main["TimeScale"].apply(normalize_time)
main["Lowest_Function"] = main.apply(get_lowest_function, axis=1)

# Create the combined process string (Function@Process)
main["Combined_Process"] = main.apply(
    lambda row: f"{row['Lowest_Function']}@{str(row.get('Process', '')).strip()}"
    if row["Lowest_Function"] != "Unknown" and str(row.get("Process", "")).strip()
    else str(row.get("Process", "")).strip() or row["Lowest_Function"],
    axis=1
)

# Determine the Spatial Type from EffectorScale
main["Spatial_Type"] = main["EffectorScale"].apply(normalize_spatial)

# -----------------------------------------------------------
## STEP 2: Aggregate Processes by Spatial Type and Time Range
# -----------------------------------------------------------

# Melt the prepared data into a long format.
melted_df = main.copy()
melted_df["Time Range"] = melted_df["TimeScale_norm"].apply(lambda x: TIME_MAPPING.get(x, ["Unknown"]))
melted_df = melted_df.explode("Time Range")

# Group by Spatial_Type and Time Range to aggregate the processes (collapsing across all organs/rows).
grouped = (
    melted_df.groupby(["Time Range", "Spatial_Type"])["Combined_Process"]
    .apply(
        # Aggregate all unique processes within the group
        lambda x: "; ".join(
            sorted(
                set(
                    p.strip() for process_str in x.dropna() 
                    for p in process_str.split(';') if p.strip()
                )
            )
        )
    )
    .reset_index(name="Function@Process")
)

# Filter out rows where the aggregated process list is empty or 'Unknown'
grouped = grouped[grouped["Function@Process"] != ""]
grouped = grouped[grouped["Function@Process"] != "Unknown"]


# -----------------------------------------------------------
## STEP 3: Pivot to Final Wide Table and Order Columns
# -----------------------------------------------------------

# Pivot to wide table (Time Range vs Spatial Type)
pivot = grouped.pivot(
    index="Time Range",
    columns="Spatial_Type",
    values="Function@Process"
).fillna("").reset_index()

# Final cleanup and column ordering (REQUIRED SEQUENCE)
# The requested sequence is: Organ, AS, FTU, CT, B, Unknown
desired_types = ["Organ", "AS", "FTU", "CT", "B", "Unknown"]

# Ensure all desired columns exist, even if empty in the data
for t in desired_types:
    if t not in pivot.columns:
        pivot[t] = ""

# Ensure all time ranges appear and are ordered
pivot = pivot.set_index("Time Range").reindex(TIME_COLUMNS).fillna("").reset_index()
pivot["Time Range"] = pd.Categorical(pivot["Time Range"], categories=TIME_COLUMNS, ordered=True)
pivot = pivot.sort_values("Time Range").reset_index(drop=True)

# Select and order the final columns
final_pivot = pivot[["Time Range"] + desired_types]

# --- SAVE OUTPUT ---
final_pivot.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

print(f"✅ Created summary table by Time Range × EffectorScale Type successfully at: {OUTPUT_PATH}!")
print("---")
print(f"Final Spatial Column Order: {final_pivot.columns.tolist()[1:]}")
print("Total rows:", len(final_pivot))
print(final_pivot.head(10))