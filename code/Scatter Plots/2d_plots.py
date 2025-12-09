#!/usr/bin/env python3
import os
import glob
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# --------------------------
# Paths (adjust if needed)
# --------------------------
input_folder = "./output/temporal_spatial_output/v3/"
output_folder = "./output/2d_plots/v3/"
os.makedirs(output_folder, exist_ok=True)

# --------------------------
# Configuration (orders + visuals)
# --------------------------
spatial_order = ["Organ", "AS", "FTU", "CT", "B"]
time_order = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day",
    "1day - < 1week", "1 week - < 1 year", "1 year or longer"
]  

# plotting defaults
cmap_choice = "summer"
figsize = (10, 6)
dpi = 300

# --------------------------
# Helpers
# --------------------------
def process_count(x):
    """Return number of semicolon-separated IDs in a cell; treat empty/NaN as 0."""
    if pd.isna(x) or str(x).strip() == "":
        return 0
    return len([p for p in str(x).split(";") if p.strip()])

def extract_organ_system_name(filename):
    """
    Extract first two words from filename (before '_final...' or '.csv').
    e.g. 'male_reproductive_system_final_spatial_temporal_v3.csv' -> 'male reproductive'
    """
    base = os.path.basename(filename)
    base = re.sub(r"_final_spatial_temporal_v3\.csv$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"\.csv$", "", base, flags=re.IGNORECASE)
    parts = base.split("_")
    if len(parts) >= 2:
        return " ".join(parts[:2])
    return parts[0]

# --------------------------
# Read CSVs and build combined dataframe
# --------------------------
files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
if not files:
    raise RuntimeError(f"No CSV files found in {input_folder}")

combined_list = []
organ_system_labels = []
for f in files:
    label = extract_organ_system_name(f)
    organ_system_labels.append(label)
    # read file
    df = pd.read_csv(f)
    df["Organ System"] = label
    combined_list.append(df)

combined = pd.concat(combined_list, ignore_index=True)

for col in ["Organ", "AS", "FTU", "CT", "B"]:
    if col in combined.columns:
        combined[col + "_count"] = combined[col].apply(process_count)
    else:
        # create zero-count column if missing
        combined[col + "_count"] = 0


long_df = combined.melt(
    id_vars=["Time Range", "Organ System"],
    value_vars=["Organ_count", "AS_count", "FTU_count", "CT_count", "B_count"],
    var_name="Spatial Scale",
    value_name="Count"
)

long_df["Spatial Scale"] = long_df["Spatial Scale"].str.replace("_count", "")
long_df = long_df[long_df["Count"] > 0].copy()

# --------------------------
# Build organ system order (preserve file order)
# --------------------------
organ_system_order = []
seen = set()
for f in files:
    lab = extract_organ_system_name(f)
    if lab not in seen:
        seen.add(lab)
        organ_system_order.append(lab)

if not organ_system_order:
    organ_system_order = sorted(long_df["Organ System"].unique())

# --------------------------
# Encode categorical axes & filter invalid categories
# --------------------------
long_df["z"] = long_df["Organ System"].astype("category").cat.set_categories(organ_system_order).cat.codes
long_df["x"] = long_df["Spatial Scale"].astype("category").cat.set_categories(spatial_order).cat.codes
long_df["y"] = long_df["Time Range"].astype("category").cat.set_categories(time_order).cat.codes

long_df = long_df[(long_df["x"] >= 0) & (long_df["y"] >= 0) & (long_df["z"] >= 0)].copy()

x_categories = spatial_order[:]
y_categories = time_order[:]
x_map = {cat: i for i, cat in enumerate(x_categories)}
y_map = {cat: i for i, cat in enumerate(y_categories)}

organ_systems = [s for s in organ_system_order if s in long_df["Organ System"].unique()]

# --------------------------
# CALCULATE GLOBAL COLORBAR RANGE (same as 3D plot)
# --------------------------
all_counts = long_df["Count"].values.astype(float)
global_vmin = max(1, all_counts.min())  # avoid 0
global_vmax = all_counts.max()
# Apply same adjustment as 3D plot
global_vmin_adjusted = global_vmin + (global_vmax - global_vmin) * 0.01

print(f"Global colorbar range: {global_vmin_adjusted:.2f} to {global_vmax:.2f}")

# --------------------------
# Plot settings
# --------------------------
make_heatmaps = False  
make_bubbles = True     

for organ in organ_systems:
    df_os = long_df[long_df["Organ System"] == organ].copy()
    if df_os.empty:
        continue

    # Numeric positions for bubble plot
    df_os["xpos"] = df_os["Spatial Scale"].map(x_map)
    df_os["ypos"] = df_os["Time Range"].map(y_map)

    # --- BUBBLE PLOT ---
    if make_bubbles:
        counts = df_os["Count"].astype(float).values
        if counts.size > 0:
            sizes = (counts ** 0.9) * 30  
            colors = counts

            fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
            
            # Use GLOBAL colorbar range
            sc = ax.scatter(
                df_os["xpos"].values, df_os["ypos"].values,
                s=sizes, c=colors, cmap=cmap_choice,
                alpha=0.9, edgecolors="#808080", linewidths=0.8,
                vmin=global_vmin_adjusted, vmax=global_vmax  # GLOBAL RANGE
            )

            # Set ALL axis labels (even if no data)
            ax.set_xticks(range(len(x_categories)))
            ax.set_xticklabels(x_categories, rotation=45, ha="right", fontsize=10)
            ax.set_xlim(-0.5, len(x_categories) - 0.5)
            
            ax.set_yticks(range(len(y_categories)))
            ax.set_yticklabels(y_categories, fontsize=9)
            ax.set_ylim(-0.5, len(y_categories) - 0.5)

            ax.set_xlabel("Spatial Scale", fontsize=12, labelpad=8)
            ax.set_ylabel("Time Range", fontsize=12, labelpad=8)

            # colorbar with GLOBAL range
            cbar = fig.colorbar(sc, ax=ax, pad=0.05, shrink=0.8)
            cbar.set_label("Number of Processes", rotation=90, labelpad=12)
            cbar.ax.yaxis.set_label_position("left")
            
            plt.tight_layout()
            safe_name = organ.replace(" ", "_")
            out_bubble = os.path.join(output_folder, f"{safe_name}_plot.png")
            plt.savefig(out_bubble, bbox_inches="tight")
            plt.close(fig)
            print(f"Saved {out_bubble}")

    if make_heatmaps:
        # --- HEATMAP ---
        pivot = df_os.pivot_table(index="Time Range", columns="Spatial Scale", values="Count", aggfunc="sum", fill_value=0)
        pivot = pivot.reindex(index=y_categories, columns=x_categories, fill_value=0)

        fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
        
        # Use GLOBAL colorbar range for heatmap too
        im = ax.imshow(
            pivot.values, aspect="auto", origin="lower", 
            interpolation="none", cmap=cmap_choice,
            vmin=global_vmin_adjusted, vmax=global_vmax  # GLOBAL RANGE
        )

        # Set ALL axis labels (even if no data)
        ax.set_xticks(range(len(x_categories)))
        ax.set_xticklabels(x_categories, rotation=45, ha="right", fontsize=10)
        ax.set_yticks(range(len(y_categories)))
        ax.set_yticklabels(y_categories, fontsize=9)

        ax.set_xlabel("Spatial Scale")
        ax.set_ylabel("Time Range")
        
        # Create vertical colorbar
        cbar = fig.colorbar(im, ax=ax, pad=0.02, shrink=0.8)
        cbar.set_label("")
        cbar.ax.set_title("Number of Processes", rotation=90, fontsize=12, pad=20)        
        
        annotate_cells = True
        if annotate_cells:
            for i in range(pivot.shape[0]):
                for j in range(pivot.shape[1]):
                    val = pivot.iat[i, j]
                    if val > 0:
                        ax.text(j, i, int(val), ha="center", va="center", fontsize=8)

        plt.tight_layout()
        safe_name = organ.replace(" ", "_")
        out_heatmap = os.path.join(output_folder, f"{safe_name}_heatmap.png")
        plt.savefig(out_heatmap, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved {out_heatmap}")

print("All done — 2D plots saved to:", output_folder)