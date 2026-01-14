# wpp-table-experiments
Experimental repo for WPP tables
Descriptions according to the script number

## 01 - All ids and types from asctb and HRA kg are extracted in this table
> Output - data/all_asctb_ids_with_types.csv

## 02 - Spatial Temporal tables using EffectorScale 

This script will create spatial temporal tables for all organ systems using "EffectorScale" to identify the spatial scale and "TimeScale" to identify time scale

> Output - output/temporal_spatial_output/v7/

## 03 & 04 Analysis

Now in HRA there are total 4955 AS entries, where organs are also considered as AS
In WPP these Organs are considered as Organs and are separated from AS 
In below file there are total 283 Uberon entries in WPP across 9 organ systems. Also, statistics for present in WPP but missing in HRA and present in both

> - Total Uberon ids in WPP => 290
> - WPP intersection HRA => 167
> - Only in WPP => 123
> - Total in HRA => 1805
> - Only in HRA => 1638

> Output - output\analysis\all_Uberon_statistics\ 

## 05 & 06 Analysis

Similarly, for CT the statistics are in files as total CT, present in WPP but missing in HRA, present in Both

 === SUMMARY For CT in WPP and HRA===

> - Total CL IDs checked: 332
> - Present in ASTCB: 117
> - Missing in ASTCB: 215


> Output - output/analysis/all_CT_statistics

## 07 - 2D bubble plots for each organ systems

This script will plot a 2D scatter plot for each Organ System with dots size and color representing number of processes at that scale where -
- X axis is Spatial Scale => Organs, AS, FTU, CT, B
- Y axis is Temporal Scale -> <1 second, 1s - < 1min, 1min - < 1hr, 1hr - < 1day, 1day - < 1week, 1 week - < 1 year, 1 year or longer

> Output - output/2d_plots/v7

## 08 - 3D Scatter plots for all organ systems

This script will plot a 3D scatter plot with dots size and color representing number of processes at that scale where -
- X axis is Spatial Scale => Organs, AS, FTU, CT, B
- Y axis is Temporal Scale -> <1 second, 1s - < 1min, 1min - < 1hr, 1hr - < 1day, 1day - < 1week, 1 week - < 1 year, 1 year or longer
- Z axis is Organ Systems -> CardioVascular, Digestive, Endocrine, Female Reproductive, Male Reproductive, Muscular, Pulmonary, Skeletal, Urinary System

> Output - output/3d_scatter_plots/v7/

## 10 - Process Counts

This script gets the total number of processes across each spatial scale and time across organ systems including unique counts.

> Output - output\unique_processes\process_counts6.csv

## 11 - Unique Effectors

This script gets the total unique effectors in each organ systems which have the processes going inside, differentiated by spatial ranges.

> Output - output\unique_effectors\all_organ_system_label_counts.csv

## 12 - Common effectors across Organ Systems

We observed that some of the Anatomical structures, CT or FTUs which are present across 2 or more WPP Organ System tables. This script gives us a list of such effectors with their ids and list of organ systems they are part of.

> Output - output\common_effectors_across_systems\labels_present_in_multiple_files.csv

## 13 - FTUs occurring in WPP tables and the processes happening in them

1. ftu_global_process_summary_1.csv
High-level summary showing total unique process count for each FTU across all tables. Only includes FTUs from Effector/ID columns. Sorted by process count (highest to lowest) to identify FTUs involved in the most diverse set of biological processes.
2. ftu_id_matches_summary_1.csv
Detailed breakdown of FTU matches found in each input table. Shows which FTUs appear in which tables, their labels, and associated processes. For Effector/ID columns, includes all unique processes found in that specific table and a count of those processes. EffectorLocation/ID matches have empty process fields.

> Output - output\unique_ftus\ftu_global_process_summary_1.csv
>        - output\unique_ftus\ftu_id_matches_summary_1.csv

### Challenges

