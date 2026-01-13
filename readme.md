# metal_lci2bw

This repository provides a Python utility to import **Life Cycle Inventory (LCI) datasets from Excel files into Brightway**. It supports both **legacy Brightway2** and **Brightway 2.5** workflows and is intended for structured, reproducible transfer of foreground LCI data into an existing Brightway project.

The tool is designed for research and project-based LCAs where inventories are maintained in Excel format and must be converted into Brightway activities in a controlled, transparent, and auditable manner.

---

## What this tool does

The script reads Excel-based LCI datasets and creates corresponding activities and exchanges in a target Brightway project. Technosphere exchanges are linked to an existing ecoinvent database, while biosphere exchanges are linked to a configured biosphere database (by default `biosphere3`).

Biosphere flow name differences between source Excel files and Brightway are handled explicitly through a dedicated mapping file. The importer follows a conservative strategy: unresolved links are not guessed and must be resolved explicitly, ensuring traceability and scientific robustness.

---

## Input data

The Excel LCI files used by this tool are taken from the following dataset:

Lai, F. (2025). *LCI datasets associated with the "Life cycle inventories of global metal and mineral supply chains: a comprehensive data review, analysis and processing" article* [Data set]. *Resources, Conservation and Recycling* (Version 0). Zenodo.
[https://doi.org/10.5281/zenodo.15075067](https://doi.org/10.5281/zenodo.15075067)

The required Excel files must be copied into the following folder:

```
lci_excels/
```

Excel files are intentionally excluded from version control and must be obtained separately from the original data source.

---

## Requirements

* A working **Brightway2** installation
  or

* **Brightway 2.5** (including Activity Browser beta environments)

* Access to **ecoinvent 3.10 cut-off**, with valid credentials
  The importer relies on this database for technosphere linking and LCIA method import.

* A Python environment where **Brightway or Activity Browser** is installed
  The script must be run inside this environment.

---

## How to use

1. Copy the Excel LCI files from the Zenodo dataset into:

   ```
   lci_excels/
   ```

2. Open the relevant import script (`import_lci_excels.py` for Brightway2 or `import_lci_bw25.py` for Brightway 2.5) and provide your **ecoinvent 3.10 cut-off credentials** in the configuration section, or via environment variables.

3. Ensure that the file `Biosphere mapping fix.xlsx` is present in the repository root. This file defines explicit name replacements for biosphere flows where required.

4. Run the script:

   ```
   python import_lci_bw25.py
   ```

   or

   ```
   python import_lci_bw2.py
   ```

5. The script must be executed **within the same Python environment** where Brightway or Activity Browser is installed.

---

## Notes

* Biosphere flow name differences are resolved only through the mapping Excel file; users are encouraged to review and extend this file as needed.
* The script does not modify background databases beyond linking and importing required data.
* All mappings remain explicit, reproducible, and version-controlled.
* LCIA methods from ecoinvent are imported automatically when enabled.

---

## Intended audience

This tool is intended for LCA practitioners, researchers, and database developers who work with Excel-based inventories and require a reliable and transparent pathway to Brightway, without heuristic or opaque matching procedures.

---
