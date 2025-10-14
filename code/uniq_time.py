import pandas as pd

df = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8")  # change header index if needed

# Get unique values from a specific column
unique_cities = df["TimeScale"].unique()

print(unique_cities)