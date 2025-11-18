import pandas as pd
import glob
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import os
import re
import matplotlib.colors as mcolors

# --------------------------
# Paths
# --------------------------
input_folder = "./output/temporal_spatial_output/v3/"
output_folder = "./output/3d_scatter_plots/"
os.makedirs(output_folder, exist_ok=True)
files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))

# --------------------------
# Configuration
# --------------------------
spatial_order = ["Organ", "AS", "FTU", "CT", "B"][::-1]
time_order = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day",
    "1day - < 1week", "1 week - < 1 year", "1 year or longer"
][::-1]

# Choose a sequential colormap (change as desired)
cmap_choice = "summer"   # e.g. "viridis", "inferno", "plasma", "magma", "cividis", "YlGnBu"
# If you want log scaling of colors for wide ranges, set use_log_norm = True
use_log_norm = False

# --------------------------
# Helpers
# --------------------------
def process_count(x):
    if pd.isna(x) or str(x).strip() == "":
        return 0
    return len([p for p in str(x).split(";") if p.strip()])

def extract_organ_system_name(filename):
    """
    Extract first two words from filename (before '_final...' or '.csv').
    e.g. 'male_reproductive_system_xyz.csv' -> 'male reproductive'
    """
    base = os.path.basename(filename)
    base = re.sub(r"_final_spatial_temporal_v3\.csv$", "", base)
    base = re.sub(r"\.csv$", "", base)
    parts = base.split("_")
    if len(parts) >= 2:
        return " ".join(parts[:2])
    return parts[0]

# --------------------------
# Load & combine all CSVs into one dataframe with an 'Organ System' column
# --------------------------
combined_list = []
organ_system_labels = []

for f in files:
    label = extract_organ_system_name(f)
    organ_system_labels.append(label)
    df = pd.read_csv(f)
    df["Organ System"] = label
    combined_list.append(df)

if not combined_list:
    raise RuntimeError(f"No CSV files found in {input_folder}")

combined = pd.concat(combined_list, ignore_index=True)

# --------------------------
# Count processes on each spatial column
# --------------------------
for col in ["Organ", "AS", "FTU", "CT", "B"]:
    if col in combined.columns:
        combined[col + "_count"] = combined[col].apply(process_count)
    else:
        # If the column is missing, create a zero column to avoid errors downstream
        combined[col + "_count"] = 0

# --------------------------
# Flatten to long format
# --------------------------
long_df = combined.melt(
    id_vars=["Time Range", "Organ System"],
    value_vars=["Organ_count", "AS_count", "FTU_count", "CT_count", "B_count"],
    var_name="Spatial Scale",
    value_name="Count"
)

long_df["Spatial Scale"] = long_df["Spatial Scale"].str.replace("_count", "")
long_df = long_df[long_df["Count"] > 0].copy()

# --------------------------
# Build z-axis categories (organ systems) from filenames (preserve order if you like)
# --------------------------
# We'll use the order of first appearance in 'files' for z-order:
organ_system_order = []
seen = set()
for f in files:
    lab = extract_organ_system_name(f)
    if lab not in seen:
        seen.add(lab)
        organ_system_order.append(lab)

# Fallback: if something odd happened, use unique sorted
if not organ_system_order:
    organ_system_order = sorted(long_df["Organ System"].unique())

# --------------------------
# Encode categorical axes
# --------------------------
long_df["z"] = long_df["Organ System"].astype("category").cat.set_categories(organ_system_order).cat.codes
long_df["x"] = long_df["Spatial Scale"].astype("category").cat.set_categories(spatial_order).cat.codes
long_df["y"] = long_df["Time Range"].astype("category").cat.set_categories(time_order).cat.codes

# Remove rows that got -1 codes because their category wasn't present in the category map
long_df = long_df[(long_df["x"] >= 0) & (long_df["y"] >= 0) & (long_df["z"] >= 0)].copy()

# --------------------------
# Plot combined 3D scatter
# --------------------------
fig = plt.figure(figsize=(24, 16))
ax = fig.add_subplot(111, projection="3d")

xs = long_df["x"].values
ys = long_df["y"].values
zs = long_df["z"].values
sizes = (long_df["Count"].values.astype(float) ** 0.9) * 30  # gentle transform so large counts don't explode
colors = long_df["Count"].values

# Optional: Log normalization (helps when counts span orders of magnitude)
norm = None
if use_log_norm and colors.min() > 0:
    norm = mcolors.LogNorm(vmin=colors.min(), vmax=colors.max())

# Clip the color range slightly above min to make small counts visible
vmin = max(1, colors.min())  # avoid 0
vmax = colors.max()
vmin_adjusted = vmin + (vmax - vmin) * 0.01  # start 5% into the colormap

p = ax.scatter(
    xs, ys, zs, s=sizes, c=colors,
    cmap=cmap_choice, alpha=0.9, edgecolors="w", linewidths=0.3,
    vmin=vmin_adjusted, vmax=vmax
)


# Ticks & labels
ax.set_xticks(range(len(spatial_order)))
ax.set_xticklabels(spatial_order, rotation=45, ha="right", fontsize=11)
ax.set_xlabel("Spatial Scale", fontsize=14, labelpad=18)

ax.set_yticks(range(len(time_order)))
ax.set_yticklabels(time_order, rotation=10, fontsize=10)
ax.set_ylabel("Time Scale", fontsize=14, labelpad=18)

ax.set_zticks(range(len(organ_system_order)))
# show nicer z tick labels (title case)
ztick_labels = [s.replace("_", " ").title() for s in organ_system_order]
ax.set_zticklabels(ztick_labels, fontsize=11)
ax.set_zlabel("Organ System", fontsize=14, labelpad=50)

# Expand axes limits so end labels aren't crammed
ax.set_xlim(-0.6, len(spatial_order)-0.4)
ax.set_ylim(-0.6, len(time_order)-0.4)
ax.set_zlim(-0.6, len(organ_system_order)-0.4)

# Title, colorbar and layout tweaks
ax.set_title("Combined Temporal–Spatial Distribution — All Organ Systems", fontsize=18, pad=30)

cbar = fig.colorbar(p, ax=ax, shrink=0.6, pad=0.08)
cbar.set_label("Number of Processes", rotation=270, labelpad=20, fontsize=12)

# Subplot adjustments to create breathing room
plt.subplots_adjust(left=0.12, right=0.92, bottom=0.12, top=0.9)

# Improve 3D view angle (tweak these if necessary)
ax.view_init(elev=25, azim=130)

# Save
combined_output_path = os.path.join(output_folder, "combined_all_systems_3D_scatter.png")
plt.savefig(combined_output_path, dpi=300, bbox_inches="tight")
plt.close(fig)

print(f"Saved combined plot to: {combined_output_path}")
