# metal_lci2bw

This repository provides a Python utility to import **Life Cycle Inventory (LCI) datasets from Excel files into Brightway**. It is intended for structured, reproducible transfer of foreground LCI data into an existing Brightway environment, with explicit linking to technosphere and biosphere databases.

The tool is designed for research and project-based LCAs where inventories are maintained in Excel format and need to be converted into Brightway activities in a controlled and transparent manner.

---

## What this tool does

The script reads Excel-based LCI datasets and creates corresponding activities and exchanges in a target Brightway database. It links technosphere exchanges to existing activities and biosphere exchanges to `biosphere3`, using a conservative mapping approach. Where biosphere flow names differ between source data and Brightway, explicit corrections are applied via an external mapping file.

The importer prioritizes correctness and traceability. Missing or unresolved links are not guessed and must be resolved explicitly (see the mapping excel file), ensuring that all mappings remain auditable.

---

## Input data

The Excel LCI files used by this tool are taken from the following dataset:

Lai, F. (2025). *LCI datasets associated with the "Life cycle inventories of global metal and mineral supply chains: a comprehensive data review, analysis and processing" article* [Data set]. *Resources, Conservation and Recycling* (Version 0). Zenodo.
[https://doi.org/10.5281/zenodo.15075067](https://doi.org/10.5281/zenodo.15075067)

The user must copy the required Excel files from this dataset into the folder:

```
lci_excels/
```

---

## Requirements

* A working **Brightway2** installation
  or
* **Activity Browser ≤ 2.11** (newer versions may rely on Brightway 2.5 and are not supported)
* Access to **ecoinvent 3.10 cut-off**, with valid credentials
  The importer relies on this database for technosphere and biosphere mapping.
* A Python environment where **Brightway or Activity Browser is installed**

---

## How to use

1. Copy the Excel LCI files from the Zenodo dataset into the folder:

   ```
   lci_excels/
   ```
2. Open `import_lci_excels.py` and provide your **ecoinvent 3.10 cut-off credentials** in the designated variables within the python file.
3. Ensure that the `Biosphere mapping fix.xlsx` file is present in the repository. This file contains explicit name corrections for biosphere flows where needed.
4. Run the Python script:
   ```
   python import_lci_excels.py
   ```
5. The script must be executed **within the same Python environment** where Brightway or Activity Browser is installed.

---

## Notes

* Biosphere flow name differences are resolved only through the mapping Excel file; users are encouraged to review and extend this file where necessary.
* The script does not modify background databases.
* All mappings remain explicit and version-controlled.

---

## Intended audience

This tool is intended for LCA practitioners, researchers, and database developers who work with Excel-based inventories and require a reliable pathway to Brightway without opaque or heuristic-based matching.

---

