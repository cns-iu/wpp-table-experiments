#!/usr/bin/env python3
import pandas as pd
import requests


def fetch_json(purl):
    response = requests.get(purl, headers={"Accept": "application/json"})
    response.raise_for_status()
    return response.json()


def is_asctb_table(purl):
    return (
        purl.startswith("https://purl.humanatlas.io/asct-b/")
        and "crosswalk" not in purl
    )

def get_latest_asctb_data():
    hra_collection = fetch_json("https://purl.humanatlas.io/collection/hra")
    digital_objects = hra_collection["metadata"]["had_member"]
    tables = {}
    for purl in sorted(filter(is_asctb_table, digital_objects)):
        table_name = purl.split("/")[-2].replace('-', '_')
        table_data = fetch_json(purl)
        table_rows = table_data["data"]["asctb_record"]
        tables[table_name] = table_rows
    return tables

def format_term(s):
    return s.replace("https://purl.org/ccf/ASCTB-TEMP_", "ASCTB-TEMP:")

def extract_all_ids_and_types(tables):
    records = []

    for organ_name, rows in tables.items():
        for record in rows:
            # --- Anatomical Structures ---
            for item in record.get("anatomical_structure_list", []):
                records.append({
                    "organ": organ_name,
                    "id": format_term(item["source_concept"]),
                    "cf_asctb_type": "AS",
                    "label": item["ccf_pref_label"]
                })
            # --- Cell Types ---
            for item in record.get("cell_type_list", []):
                records.append({
                    "organ": organ_name,
                    "id": format_term(item["source_concept"]),
                    "cf_asctb_type": "CT",
                    "label": item["ccf_pref_label"]
                })
            # --- Gene Biomarkers ---
            for item in record.get("gene_marker_list", []):
                records.append({
                    "organ": organ_name,
                    "id": format_term(item["source_concept"]),
                    "cf_asctb_type": "B (gene)",
                    "label": item["ccf_pref_label"]
                })
            # --- Protein Biomarkers ---
            for item in record.get("protein_marker_list", []):
                records.append({
                    "organ": organ_name,
                    "id": format_term(item["source_concept"]),
                    "cf_asctb_type": "B (protein)",
                    "label": item["ccf_pref_label"]
                })

    df = pd.DataFrame(records).drop_duplicates().reset_index(drop=True)
    return df


# --- Usage ---
tables = get_latest_asctb_data()
print("Fetched", len(tables), "ASCT+B tables")

df_all_ids = extract_all_ids_and_types(tables)
print("Extracted", len(df_all_ids), "unique entries across all organs.")
print(df_all_ids.head())

# Optional: Save to CSV
df_all_ids.to_csv("./data/all_asctb_ids_and_types.csv", index=False)
