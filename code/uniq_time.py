import pandas as pd

df = pd.read_csv("./data/WPP Tables/Endocrine-System_v1.0_DRAFT_20251105 - Sheet1.csv", header=12, encoding="utf-8")  # change header index if needed

# Get unique values from a specific column
unique_cities = df["TimeScale"].unique()

print(unique_cities)