# wpp-table-experiments
Experimental repo for WPP tables

All ids and types from asctb and HRA kg are extracted in this table
> ./data/all_asctb_ids_with_types.py

## Spatial Temporal tables using EffectorScale 

> This script will create spatial temporal tables for all organ systems using "EffectorScale" to identify the spatial scale and "TimeScale" to identify time scale
Run - new_system.py

Output - output/temporal_spatial_output/v3

## Scatter Plots

### 3D Scatter plots for all organ systems

> This script will plot a 3D scatter plot with dots size and color representing number of processes at that scale where -
> - X axis is Spatial Scale => Organs, AS, FTU, CT, B
> - Y axis is Temporal Scale -> <1 second, 1s - < 1min, 1min - < 1hr, 1hr - < 1day, 1day - < 1week, 1 week - < 1 year, 1 year or longer
> - Z axis is Organ Systems -> CardioVascular, Digestive, Endocrine, Female Reproductive, Male Reproductive, Muscular, Pulmonary, Skeletal, Urinary System

Run = code/3d_scatter_plot.py

Output - output/3d_scatter_plots/

### 2D bubble plots for each organ systems

> This script will plot a 2D scatter plot for each Organ System with dots size and color representing number of processes at that scale where -
> - X axis is Spatial Scale => Organs, AS, FTU, CT, B
> - Y axis is Temporal Scale -> <1 second, 1s - < 1min, 1min - < 1hr, 1hr - < 1day, 1day - < 1week, 1 week - < 1 year, 1 year or longer

Run = code/2d_plots.py

Output - output/2d_plots/

## Analysis

In this directory there are csv files containing Organs, AS present in WPP and also these AS are then categorized in two files -

- AS present in WPP and HRA kg
- AS present in WPP but missing in HRA kg

> If we consider only Where EffectorScale == "Tissue"
> Unique AS IDs: 177

> Unique AS IDs present in ASTCB: 121

> Unique AS IDs NOT present in ASTCB: 56

Output - ./output/analysis

- output\analysis\Organs_with_ids_WPP.csv (Organs present in WPP tables)

- output\analysis\AS_UBERON_in_WPP.csv (ALL AS present in WPP tables)

- output\analysis\AS_ids_missing_in_asctb.csv (AS missin in ASTCB but present in WPP tables)

- output\analysis\AS_ids_present_in_asctb.csv (AS present in ASTCB and WPP tables)

> Now in HRA there are total 4955 AS entries, where organs are also considered as AS
> In WPP these Organs are considered as Organs and are separated from AS hence the preiouvs results were in consideration.
> If we consider these Organs separated in WPP as AS as well just to know the number UBERON ids present in WPP and HRA - 

In below file there are total 283 Uberon entries in WPP across 9 organ systems.
Total Uberon ids in WPP => 283
WPP intersection HRA => 164
Only in WPP => 119
Total in HRA => 4955
Only in HRA => 4791

- output\analysis\all_Uberon_statistics\all_Uberon_ids_in_WPP.csv

### Challenges

