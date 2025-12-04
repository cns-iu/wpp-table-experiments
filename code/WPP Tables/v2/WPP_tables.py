import pandas as pd
import re

# --- CONFIGURATION ---
MAIN_CSV_PATH = "./data/WPP Tables/Endocrine-System_v1.0_DRAFT_20251105 - Sheet1.csv"
OUTPUT_PATH = "./output/temporal_spatial_output/v3/endocrine_system_final_spatial_temporal_v3.csv"

# --- TIME SCALE RANGES ---
TIME_COLUMNS = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day", "1day - < 1week", 
    "1 week - < 1 year", "1 year or longer",
]

# --- MAPPING FOR TIME SCALE RANGES ---
TIME_MAPPING = {
    # Existing base mappings
    "milliseconds": ["<1 second"], "seconds": ["1s - < 1min"], "secondsminutes": ["1s - < 1min", "1min - < 1hr"],
    "minuteshours": ["1min - < 1hr", "1hr - < 1day"], "hoursdays": ["1hr - < 1day", "1day - < 1week"],
    "daysweeks": ["1day - < 1week", "1 week - < 1 year"], "hours": ["1hr - < 1day"], "minutes": ["1min - < 1hr"],
    "days": ["1day - < 1week"], "nan": ["Unknown"],

    # Complex/Missing mappings (normalized keys)
    "weeks": ["1 week - < 1 year"], "months": ["1 week - < 1 year"], "years": ["1 year or longer"],
    "weeksmonths": ["1 week - < 1 year"], "minuteshoursdays": ["1min - < 1hr", "1hr - < 1day", "1day - < 1week"],
    "hoursdaysweeksmonths": ["1hr - < 1day", "1day - < 1week", "1 week - < 1 year"],
    "secondsminuteshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "milisecondsseconds": ["<1 second", "1s - < 1min"], "secondshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "continuous": ["continuous"], "variable": ["variable"],
}

# --- SPATIAL MAPPING (KEYS ARE NOW NORMALIZED, LOWERCASE, NO SPACES/PUNCTUATION) ---
# The normalize_spatial function will convert the input string to match these keys.
SPATIAL_MAPPING = {
    "tissue": "AS",
    "tissueftu": "FTU", # Matches "Tissue(FTU)", "tissue(ftu)"
    "cell": "CT",
    "organ": "Organ",
    "organsystem": "Organ", # Matches "Organ system"
    "biomolecule": "B",
    "molecule": "B",
    "subcellular": "Unknown",
    "organism": "Unknown",
    "nan": "Unknown",
    "": "Unknown"
}
# -------------------------------------------------------------------------

ftu_ids = {
    "UBERON:0004203",
    "UBERON:0001289",
    "UBERON:0004205",
    "UBERON:0004193",
    "UBERON:0001285",
    "UBERON:0004204",
    "UBERON:0001229",
    "UBERON:0001291",
    "UBERON:0004647",
    "UBERON:0002299",
    "UBERON:8410043",
    "UBERON:0000006",
    "UBERON:0001263",
    "UBERON:0014725",
    "UBERON:0004179",
    "UBERON:0001983",
    "UBERON:0000412",
    "UBERON:0002073",
    "UBERON:0013487",
    "UBERON:0001213",
    "UBERON:0001250",
    "UBERON:0001959",
    "UBERON:0002125",
    "UBERON:0001831",
    "UBERON:0001832",
    "UBERON:0001736",
}

# --- DATA LOADING AND INITIAL CLEANING ---
try:
    main = pd.read_csv(
        MAIN_CSV_PATH,
        header=12,
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
    val = re.sub(r"[–,\-\s]", "", val) # Remove all separators
    return val.strip()

def normalize_spatial(val, effector_id=None):
    """
    Normalizes the EffectorScale value and uses effector_id to decide
    whether 'tissue' maps to FTU or AS.

    Parameters
    ----------
    val : str or NaN
        The EffectorScale value (e.g., "Tissue(FTU)", "cell", "organ", etc.)
    effector_id : str or None
        The Effector/ID value to check against ftu_ids when val indicates tissue.

    Returns
    -------
    str
        One of the SPATIAL_MAPPING values (e.g., "FTU", "AS", "CT", "Organ", "B", "Unknown").
    """
    # Handle missing
    if pd.isna(val) or str(val).strip() == "":
        return SPATIAL_MAPPING.get("nan", "Unknown")
    
    val_str = str(val).strip().lower()

    # Aggressively clean to normalized key (e.g., "tissue(ftu)" -> "tissueftu")
    normalized_key = re.sub(r"[^a-z0-9]", "", val_str)

    # If the explicit key already encodes FTU (e.g., 'tissueftu') prefer that
    if normalized_key in SPATIAL_MAPPING:
        # If this is explicitly tissueftu, map directly
        if normalized_key == "tissueftu":
            return "FTU"
        return SPATIAL_MAPPING[normalized_key]

    # If it's some form of tissue (starts with 'tissue' or equals 'tissue'),
    # decide based on effector_id membership in ftu_ids.
    if normalized_key.startswith("tissue"):
        # normalize effector_id string
        if effector_id is not None and pd.notna(effector_id):
            eff_id_str = str(effector_id).strip()
            # If the effector id (exact match) is in ftu list -> FTU, else AS
            if eff_id_str in ftu_ids:
                return "FTU"
        # default for tissue -> AS
        return "AS"

    # Fallback: try to map by normalized_key; otherwise Unknown
    return SPATIAL_MAPPING.get(normalized_key, "Unknown")


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

# Create the combined process string (Function@Process), using 'nan' for missing Process
main["Combined_Process"] = main.apply(
    lambda row: (
        process_val := str(row.get('Process', '')).strip(),
        final_process := process_val if process_val else "nan",
        (
            f"{row['Lowest_Function']}@{final_process}"
            if row["Lowest_Function"] != "Unknown"
            else final_process
        )
    )[-1],
    axis=1
)

# Determine the Spatial Type from EffectorScale using normalized values
main["Spatial_Type"] = main.apply(
    lambda row: normalize_spatial(row.get("EffectorScale", ""), row.get("Effector/ID", "")),
    axis=1
)

# -----------------------------------------------------------
## STEP 2: Aggregate Processes by Spatial Type and Time Range
# -----------------------------------------------------------

# Melt the prepared data into a long format.
melted_df = main.copy()
melted_df["Time Range"] = melted_df["TimeScale_norm"].apply(lambda x: TIME_MAPPING.get(x, ["Unknown"]))
melted_df = melted_df.explode("Time Range")

# Group by Spatial_Type and Time Range to aggregate the processes.
grouped = (
    melted_df.groupby(["Time Range", "Spatial_Type"])["Combined_Process"]
    .apply(
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

# Filter out rows where the aggregated process list is empty or strictly 'Unknown'
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

# Final cleanup and column ordering (Organ, AS, FTU, CT, B, UNKNOWN)
# Note: We keep "Unknown" here for sorting, but drop the column before final save.
desired_spatial_types = ["Organ", "AS", "FTU", "CT", "B"]

# Ensure 'Unknown' spatial column is dropped from the final output table
if "Unknown" in pivot.columns:
    pivot = pivot.drop(columns=["Unknown"])

# Ensure all desired columns exist, even if empty in the data
for t in desired_spatial_types:
    if t not in pivot.columns:
        pivot[t] = ""

# Define custom categorical order for time ranges (includes new entries)
time_category_order = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day",
    "1day - < 1week", "1 week - < 1 year", "1 year or longer",
    "continuous", "variable"
]

# Ensure all time ranges appear and are ordered
pivot = pivot.set_index("Time Range").reindex(time_category_order).fillna("").reset_index()
pivot["Time Range"] = pd.Categorical(pivot["Time Range"], categories=time_category_order, ordered=True)
pivot = pivot.sort_values("Time Range").reset_index(drop=True)

# Select and order the final columns
final_pivot = pivot[["Time Range"] + desired_spatial_types]

# --- SAVE OUTPUT ---
final_pivot.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

print(f"✅ Created summary table by Time Range × EffectorScale Type successfully at: {OUTPUT_PATH}!")
print("---")
print(f"Spatial Mapping Method: Aggressive Lowercase/Punctuation removal on input and keys.")
print(f"Final Spatial Column Order: {final_pivot.columns.tolist()[1:]}")
print("Total rows:", len(final_pivot))
print(final_pivot.head(15))