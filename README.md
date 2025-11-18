# wpp-table-experiments
Experimental repo for WPP tables

## Outputs

- missing ids are present in output/missing_ids_labels.csv

- WPP table consisting of time scales is in output/wpp_table.csv

- got all ids and types from asctb table using all_asctb_ids_with_types.py

### Spatial Temporal Tables

Run in this sequence ->

- process_triples_withall_struc.py   (All structure for each entry with Fucntion@Process Triples)

- temporal_spatial_data_id_matched.py (Lowest sturcture/cell matched with ASCTB for each entry with Fucntion@Process Triples)

- Spatial_temporal_table.py (Final spatial temporal table)

## Newly created tables using EffectorScale 

Run - new_system.py

Output - output/temporal_spatial_output/v3

### Scatter Plots

Run = code/3d_scatter_plot.py

Output - output/3d_scatter_plots/

### Analysis

Output - ./output/analysis

- output\analysis\Organs_in_WPP.csv (Organs present in WPP tables)

- output\analysis\AS_UBERON_in_WPP.csv (ALL AS present in WPP tables)

- output\analysis\AS_ids_missing_in_asctb.csv (AS missin in ASTCB but present in WPP tables)

- output\analysis\AS_ids_present_in_asctb.csv (AS present in ASTCB and WPP tables)

### Challenges

Cannot find IDs of majority lower structures in asctb tables hence matched with the ones as lower as possible in hierarchy. Refer this code at code/lowest_missing.py

- Urinary System = Total rows: 38 | Found: 18 | Not found: 20 (data/lowest_missing_endocrine_system_data.csv)

- Endcorine System = Total rows: 45 | Found: 8 | Not found: 37 (data/lowest_missing_endocrine_system_data.csv)

- Male Reproductive System = Total rows: 34 | Found: 12 | Not found: 22 (data/lowest_missing_male_reproductive_data.csv)

- Female Reproductive System = Total rows: 46 | Found: 20 | Not found: 26 (data/lowest_missing_female_reproductive_data.csv)