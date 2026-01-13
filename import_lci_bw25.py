# Excel-to-Brightway LCI Importer (BW2.5): import Excel-based life cycle inventories into Brightway 2.5.
# Copyright (C) 2026 Anish KOYAMPARAMBATH (WeLOOP)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.
# If not, see <https://www.gnu.org/licenses/>.


from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import hashlib
import os

import bw2data as bd
import bw2io as bi
from bw2io.importers import ExcelImporter


# =============================================================================
# Configuration (BW2.5)
# =============================================================================

@dataclass(frozen=True)
class Config:
    """
    Central configuration for this import script (Brightway 2.5).

    Notes
    -----
    - In Brightway 2.5, avoid calling bw2setup(); use explicit creation functions.
    - ecoinvent import can create a namespaced biosphere database (e.g., ecoinvent-3.10-biosphere).
    """

    # Brightway project name (BW2.5 project; do not migrate old BW2 projects here)
    project_name: str = "lci_metals"

    # Folder containing BW-formatted Excel LCIs
    excel_folder: Path = Path(__file__).resolve().parent / "lci_excels"

    # Background DB label used inside the Excel files
    excel_background_db_name: str = "ecoinvent 3.10 cutoff"

    # ecoinvent import settings (use only ecoinvent 3.10 cutoff)
    ecoinvent_version: str = "3.10"
    ecoinvent_system_model: str = "cutoff"

    # ecoinvent credentials (leave empty to use env vars)
    username_override: Optional[str] = ""
    password_override: Optional[str] = ""

    # Environment variable fallback
    env_user: str = "EI_USERNAME"
    env_pass: str = "EI_PASSWORD"

    # Overwrite metal databases on reruns
    overwrite_metal_databases: bool = True

    # Which biosphere DB to link foreground biosphere exchanges to.
    # Default is Brightway's configured biosphere (usually "biosphere3").
    # You may also set this to your ecoinvent namespaced biosphere if you want.
    biosphere_db_name: Optional[str] = None  # if None, use bd.config.biosphere

    # Custom biosphere handling
    custom_biosphere_db_name: str = "biosphere_custom"
    allow_create_missing_biosphere_flows: bool = False

    # Mapping file for biosphere fixes
    biosphere_mapping_fix_file: Path = (
        Path(__file__).resolve().parent / "Biosphere mapping fix.xlsx"
    )


CFG = Config()


# =============================================================================
# Types and small constants
# =============================================================================

_EIKey = Tuple[str, str, str]  # (name, reference product, location)
_BioExactKey = Tuple[str, Tuple[str, ...], str]  # (norm name, norm categories tuple, norm unit)

_BIO_NAME_ALIASES: Dict[str, list[str]] = {}


# =============================================================================
# Project and background setup (BW2.5)
# =============================================================================

def _configured_biosphere_name() -> str:
    """
    Return the biosphere DB name configured in Brightway preferences, defaulting to 'biosphere3'.
    """
    # Brightway docs: bd.config.biosphere gives the configured biosphere database name.
    # Default is 'biosphere3'. :contentReference[oaicite:4]{index=4}
    return CFG.biosphere_db_name or bd.config.biosphere


def _ensure_biosphere_db(db_name: str) -> None:
    """
    Ensure the biosphere database exists.

    Brightway 2.5 guidance: avoid bw2setup(); use explicit creation functions. :contentReference[oaicite:5]{index=5}
    """
    if db_name in bd.databases:
        return

    if db_name == "biosphere3":
        # Create the default biosphere flow list explicitly (no LCIA methods needed here).
        bi.create_default_biosphere3(overwrite=False)
        if "biosphere3" not in bd.databases:
            raise RuntimeError("Failed to create biosphere3 database.")
        return

    raise RuntimeError(
        f"Biosphere database '{db_name}' not found. "
        "Either set CFG.biosphere_db_name to an existing biosphere DB, or use 'biosphere3'."
    )


def _credentials() -> Tuple[str, str]:
    """
    Resolve ecoinvent credentials.

    Priority:
    1) CFG.username_override / CFG.password_override
    2) EI_USERNAME / EI_PASSWORD (or CFG.env_user / CFG.env_pass)
    """
    if CFG.username_override and CFG.password_override:
        return CFG.username_override, CFG.password_override

    user = os.getenv(CFG.env_user)
    pwd = os.getenv(CFG.env_pass)
    if not user or not pwd:
        raise RuntimeError(
            "Missing ecoinvent credentials. Provide username_override/password_override in CONFIG "
            "or set EI_USERNAME/EI_PASSWORD."
        )
    return user, pwd


def _find_ecoinvent_db() -> Optional[str]:
    """
    Find an already-imported ecoinvent DB matching the expected version/system model.

    BW2.5 common patterns:
    - 'ecoinvent-3.10-cutoff'
    - 'ecoinvent 3.10 cutoff'
    - user-defined labels
    """
    candidates = [
        f"ecoinvent-{CFG.ecoinvent_version}-{CFG.ecoinvent_system_model}",
        f"ecoinvent {CFG.ecoinvent_version} {CFG.ecoinvent_system_model}",
        f"ecoinvent {CFG.ecoinvent_version} cut-off",
        CFG.excel_background_db_name,
    ]
    for c in candidates:
        if c in bd.databases:
            return c

    for name in bd.databases:
        low = name.lower()
        if "ecoinvent" in low and CFG.ecoinvent_version in low and "cut" in low:
            return name

    return None


def _ensure_ecoinvent() -> str:
    """
    Ensure ecoinvent exists in the current project, importing if missing.

    Note:
    - Brightway docs explicitly warn not to run bw2setup before import_ecoinvent_release. :contentReference[oaicite:6]{index=6}
    """
    existing = _find_ecoinvent_db()
    if existing:
        print(f"[Background] Using existing ecoinvent database: '{existing}'")
        return existing

    user, pwd = _credentials()
    print(f"[Background] Importing ecoinvent {CFG.ecoinvent_version} ({CFG.ecoinvent_system_model}) ...")

    # Prefer explicit biosphere choice:
    # - If you want ecoinvent to create and use its namespaced biosphere, do not force biosphere_name.
    # - If you want to force e.g. 'biosphere3', set biosphere_name explicitly (may require patching).
    # Brightway docs: import_ecoinvent_release creates a namespaced biosphere DB. :contentReference[oaicite:7]{index=7}
    bi.import_ecoinvent_release(
        version=CFG.ecoinvent_version,
        system_model=CFG.ecoinvent_system_model,
        username=user,
        password=pwd,
        lcia=True,  # <-- import ecoinvent LCIA methods
    )


    created = _find_ecoinvent_db()
    if not created:
        raise RuntimeError("ecoinvent import finished, but no ecoinvent database was detected.")

    print(f"[Background] ecoinvent imported as: '{created}'")
    return created


# =============================================================================
# Custom biosphere flow creation
# =============================================================================

def _ensure_custom_biosphere_db(db_name: str) -> None:
    """Create an empty custom biosphere database container if missing."""
    if db_name in bd.databases:
        return
    bd.Database(db_name).write({})


def _custom_flow_code(name: str, categories: Tuple[str, ...], unit: str) -> str:
    """Deterministic code for a custom biosphere flow."""
    key = f"{name}|{categories}|{unit}".encode("utf-8")
    return hashlib.md5(key).hexdigest()


def _get_or_create_custom_biosphere_flow(
    *,
    db_name: str,
    flow_name: str,
    categories: Tuple[str, ...],
    unit: str,
) -> Tuple[str, str]:
    """Return (db_name, code) for a flow in the custom biosphere DB, creating it if missing."""
    _ensure_custom_biosphere_db(db_name)

    code = _custom_flow_code(flow_name, categories, unit)
    key = (db_name, code)

    db = bd.Database(db_name)
    existing = db.load()
    if key in existing:
        return key

    existing[key] = {
        "name": flow_name,
        "categories": categories,
        "unit": unit,
        "type": "emission",
        "code": code,
    }
    db.write(existing)
    return key


# =============================================================================
# Excel IO and importer utilities
# =============================================================================

def _iter_excels(folder: Path) -> Iterable[Path]:
    """Yield Excel files, skipping temporary Office files."""
    for p in sorted(folder.glob("*.xlsx")):
        if p.name.startswith("~$"):
            continue
        yield p


def _activities_view(importer: ExcelImporter) -> Iterable[Dict[str, Any]]:
    """Uniform iterable view of activities in an ExcelImporter."""
    data = importer.data
    if isinstance(data, dict):
        return data.values()
    if isinstance(data, list):
        return data
    return list(data)


def _databases_in_importer(importer: ExcelImporter) -> Set[str]:
    """Extract all database names referenced by activities in the importer payload."""
    if isinstance(importer.data, dict):
        return {k[0] for k in importer.data.keys()}

    out: Set[str] = set()
    for act in _activities_view(importer):
        dbname = act.get("database")
        if isinstance(dbname, str) and dbname:
            out.add(dbname)
    return out


def _normalize_exchange_inputs_to_tuples(importer: ExcelImporter) -> int:
    """Convert any exchange input 'db::code' strings to (db, code) tuples."""
    changed = 0
    for act in _activities_view(importer):
        for exc in act.get("exchanges", []) or []:
            if not isinstance(exc, dict):
                continue
            inp = exc.get("input")
            if isinstance(inp, str) and "::" in inp:
                db_part, code_part = inp.split("::", 1)
                exc["input"] = (db_part, code_part)
                changed += 1
    return changed


def _rewrite_background_db_label(importer: ExcelImporter, old_bg: str, new_bg: str) -> int:
    """Rewrite technosphere references from Excel background label to actual ecoinvent DB name."""
    if old_bg == new_bg:
        return 0

    changed = 0
    for act in _activities_view(importer):
        for exc in act.get("exchanges", []) or []:
            if not isinstance(exc, dict):
                continue

            if exc.get("database") == old_bg:
                exc["database"] = new_bg
                changed += 1

            inp = exc.get("input")
            if isinstance(inp, tuple) and len(inp) == 2 and inp[0] == old_bg:
                exc["input"] = (new_bg, inp[1])
                changed += 1

    return changed


# =============================================================================
# Technosphere relinking (ecoinvent)
# =============================================================================

def _build_ecoinvent_index(db_name: str) -> Dict[_EIKey, Tuple[str, str]]:
    """Build an in-memory index for ecoinvent activities."""
    idx: Dict[_EIKey, Tuple[str, str]] = {}
    for act in bd.Database(db_name):
        name = act.get("name")
        ref = act.get("reference product")
        loc = act.get("location")
        code = act.get("code")
        if all(isinstance(x, str) and x for x in (name, ref, loc, code)):
            idx[(name, ref, loc)] = (db_name, code)
    return idx


def _fill_missing_technosphere_inputs(importer: ExcelImporter) -> int:
    """Fill missing technosphere exchange inputs using (database, name, reference product, location)."""
    indices: Dict[str, Dict[_EIKey, Tuple[str, str]]] = {}
    fixed = 0

    for act in _activities_view(importer):
        for exc in act.get("exchanges", []) or []:
            if not isinstance(exc, dict):
                continue
            if exc.get("type") != "technosphere":
                continue
            if "input" in exc:
                continue

            db_name = exc.get("database")
            name = exc.get("name")
            ref = exc.get("reference product")
            loc = exc.get("location")

            if not all(isinstance(x, str) and x for x in (db_name, name, ref, loc)):
                continue
            if db_name not in bd.databases:
                continue

            if db_name not in indices:
                indices[db_name] = _build_ecoinvent_index(db_name)

            hit = indices[db_name].get((name, ref, loc))
            if hit:
                exc["input"] = hit
                fixed += 1

    return fixed


# =============================================================================
# Biosphere relinking (configured biosphere DB or custom)
# =============================================================================

def _norm(s: str) -> str:
    """Normalize strings for robust matching."""
    return " ".join(s.strip().lower().split())


def _build_biosphere_exact_index(db_name: str) -> Dict[_BioExactKey, Tuple[str, str]]:
    """Build an exact-match index for biosphere flows."""
    idx: Dict[_BioExactKey, Tuple[str, str]] = {}
    for flow in bd.Database(db_name):
        name = flow.get("name")
        cats = flow.get("categories")
        unit = flow.get("unit")
        code = flow.get("code")

        if not (isinstance(name, str) and isinstance(unit, str) and isinstance(code, str) and code):
            continue

        if isinstance(cats, (list, tuple)):
            cats_t = tuple(_norm(str(x)) for x in cats)
        elif isinstance(cats, str) and cats:
            cats_t = (_norm(cats),)
        else:
            cats_t = tuple()

        idx[(_norm(name), cats_t, _norm(unit))] = (db_name, code)

    return idx


def load_biosphere_mapping_fix(xlsx_path: Path) -> Dict[str, str]:
    """Load the biosphere mapping fix file: columns 'Error' -> 'To replace'."""
    import pandas as pd

    xlsx_path = xlsx_path.expanduser().resolve()
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Biosphere mapping fix file not found: {xlsx_path}")

    df = pd.read_excel(xlsx_path, sheet_name=0)
    cols = {c.strip().lower(): c for c in df.columns}

    if "error" not in cols or "to replace" not in cols:
        raise ValueError(
            "Mapping fix file must contain columns named 'Error' and 'To replace'. "
            f"Found columns: {list(df.columns)}"
        )

    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        src = row[cols["error"]]
        dst = row[cols["to replace"]]
        if not isinstance(src, str) or not src.strip():
            continue
        if not isinstance(dst, str) or not dst.strip():
            continue
        out[_norm(src)] = dst.strip()

    return out


def _fill_missing_biosphere_inputs(
    importer: ExcelImporter,
    biosphere_db: str,
    name_map: Optional[Dict[str, str]] = None,
) -> int:
    """Resolve missing biosphere inputs using exact match, mapping file, name-only, or custom flows."""
    name_map = name_map or {}

    exact_idx = _build_biosphere_exact_index(biosphere_db)

    name_idx: Dict[str, list[tuple[str, str, tuple[str, ...], str]]] = {}
    for flow in bd.Database(biosphere_db):
        name = flow.get("name")
        cats = flow.get("categories")
        unit = flow.get("unit")
        code = flow.get("code")
        if not (isinstance(name, str) and isinstance(unit, str) and isinstance(code, str) and code):
            continue

        if isinstance(cats, (list, tuple)):
            cats_t = tuple(_norm(str(x)) for x in cats)
        elif isinstance(cats, str) and cats:
            cats_t = (_norm(cats),)
        else:
            cats_t = tuple()

        name_idx.setdefault(_norm(name), []).append((biosphere_db, code, cats_t, _norm(unit)))

    def candidates_for_name(name: str) -> list[tuple[str, str, tuple[str, ...], str]]:
        n0 = _norm(name)
        out = list(name_idx.get(n0, []))
        for a in _BIO_NAME_ALIASES.get(n0, []):
            out.extend(name_idx.get(_norm(a), []))

        seen = set()
        uniq = []
        for item in out:
            k = (item[0], item[1])
            if k in seen:
                continue
            seen.add(k)
            uniq.append(item)
        return uniq

    def choose_best_candidate(
        cands: list[tuple[str, str, tuple[str, ...], str]],
        unit_n: str,
        top_compartment: Optional[str],
    ) -> Optional[Tuple[str, str]]:
        if not cands:
            return None

        c_unit = [c for c in cands if c[3] == unit_n] or cands

        if top_compartment:
            tc = _norm(top_compartment)
            c_tc = [c for c in c_unit if len(c[2]) >= 1 and c[2][0] == tc]
            if len(c_tc) == 1:
                return (c_tc[0][0], c_tc[0][1])
            if len(c_tc) > 1:
                return None

        if len(c_unit) == 1:
            return (c_unit[0][0], c_unit[0][1])

        return None

    fixed = 0

    for act in _activities_view(importer):
        for exc in act.get("exchanges", []) or []:
            if not isinstance(exc, dict):
                continue
            if exc.get("type") != "biosphere":
                continue
            if "input" in exc:
                continue

            raw_name = exc.get("name")
            cats = exc.get("categories")
            unit = exc.get("unit")

            if not (isinstance(raw_name, str) and isinstance(unit, str)):
                continue

            if isinstance(cats, (list, tuple)):
                cats_t = tuple(_norm(str(x)) for x in cats)
            elif isinstance(cats, str) and cats:
                cats_t = (_norm(cats),)
            else:
                cats_t = tuple()

            top_comp = cats_t[0] if cats_t else None
            unit_n = _norm(unit)

            name_n = _norm(raw_name)
            hit = exact_idx.get((name_n, cats_t, unit_n))
            if hit:
                exc["input"] = hit
                fixed += 1
                continue

            mapped_name = name_map.get(name_n, raw_name)
            mapped_n = _norm(mapped_name)

            hit = exact_idx.get((mapped_n, cats_t, unit_n))
            if hit:
                exc["input"] = hit
                fixed += 1
                continue

            cands = candidates_for_name(mapped_name)
            chosen = choose_best_candidate(cands, unit_n, top_compartment=top_comp)
            if chosen:
                exc["input"] = chosen
                fixed += 1
                continue

            if CFG.allow_create_missing_biosphere_flows:
                new_input = _get_or_create_custom_biosphere_flow(
                    db_name=CFG.custom_biosphere_db_name,
                    flow_name=raw_name,
                    categories=cats_t,
                    unit=unit_n,
                )
                exc["input"] = new_input
                fixed += 1
                continue

            raise ValueError(
                "Could not resolve biosphere exchange and custom flow creation is disabled.\n"
                f"Exchange: name={raw_name!r}, mapped_to={mapped_name!r}, categories={cats!r}, unit={unit!r}"
            )

    return fixed


def _sanitize_dataset_text_fields(importer: ExcelImporter) -> int:
    """Convert None -> '' for dataset and exchange text fields."""
    changed = 0
    for act in _activities_view(importer):
        if not isinstance(act, dict):
            continue

        for k in ("comment", "description"):
            if k in act and act[k] is None:
                act[k] = ""
                changed += 1

        for exc in act.get("exchanges", []) or []:
            if isinstance(exc, dict) and "comment" in exc and exc["comment"] is None:
                exc["comment"] = ""
                changed += 1

    return changed


# =============================================================================
# Validation
# =============================================================================

def _is_number(x: Any) -> bool:
    """Return True for int/float values excluding NaN."""
    return isinstance(x, (int, float)) and not (x != x)


def _validate_importer_payload(importer: ExcelImporter) -> None:
    """Fail early if any exchange is invalid for bw2data write."""
    for act in _activities_view(importer):
        adb = act.get("database")
        acode = act.get("code")
        ctx = (adb, acode)

        for exc in act.get("exchanges", []) or []:
            if not isinstance(exc, dict):
                raise ValueError(f"Non-dict exchange in activity {ctx}: {exc!r}")

            missing = [k for k in ("type", "amount", "input") if k not in exc]
            if missing:
                raise ValueError(f"Missing keys {missing} in exchange for activity {ctx}: {exc!r}")

            if exc["type"] not in {"production", "technosphere", "biosphere"}:
                raise ValueError(f"Invalid exchange type in activity {ctx}: {exc!r}")

            if not _is_number(exc["amount"]):
                raise ValueError(f"Non-numeric amount in activity {ctx}: {exc!r}")

            inp = exc["input"]
            if not (isinstance(inp, tuple) and len(inp) == 2 and all(isinstance(i, str) and i for i in inp)):
                raise ValueError(f"Invalid input format in activity {ctx}: {exc!r}")

            if exc["type"] == "production":
                if isinstance(adb, str) and isinstance(acode, str) and adb and acode:
                    if inp != (adb, acode):
                        raise ValueError(
                            f"Production exchange must point to {(adb, acode)} but got {inp!r}: {exc!r}"
                        )


# =============================================================================
# Orchestration
# =============================================================================

def _prepare_project() -> tuple[str, str]:
    """
    Prepare project prerequisites.

    Returns
    -------
    (ecoinvent_db_name, biosphere_db_name)
    """
    bd.projects.set_current(CFG.project_name)
    print(f"[Project] Current project: {bd.projects.current}")

    biosphere_db = _configured_biosphere_name()
    _ensure_biosphere_db(biosphere_db)
    _ensure_custom_biosphere_db(CFG.custom_biosphere_db_name)

    ecoinvent_db = _ensure_ecoinvent()
    return ecoinvent_db, biosphere_db


def _process_excel(
    *,
    xlsx: Path,
    actual_ecoinvent_db: str,
    biosphere_db: str,
    biosphere_name_map: Dict[str, str],
) -> None:
    """Process one BW-formatted Excel file."""
    print(f"\n[Excel] {xlsx.name}")

    importer = ExcelImporter(str(xlsx))
    importer.apply_strategies()

    n_norm = _normalize_exchange_inputs_to_tuples(importer)
    if n_norm:
        print(f"[Link] Normalised {n_norm} exchange input(s) to tuples")

    n_bg = _rewrite_background_db_label(importer, CFG.excel_background_db_name, actual_ecoinvent_db)
    if n_bg:
        print(f"[Link] Rewired {n_bg} background DB label reference(s)")

    n_tech = _fill_missing_technosphere_inputs(importer)
    if n_tech:
        print(f"[Link] Filled {n_tech} missing technosphere input(s) by lookup")

    n_bio = _fill_missing_biosphere_inputs(importer, biosphere_db, name_map=biosphere_name_map)
    if n_bio:
        print(f"[Link] Filled {n_bio} missing biosphere input(s) (mapped or custom)")

    n_txt = _sanitize_dataset_text_fields(importer)
    if n_txt:
        print(f"[Clean] Sanitized {n_txt} None text field(s) to empty strings")

    stats = importer.statistics()
    print(f"[Excel] Stats (datasets, exchanges, unlinked): {stats}")

    metal_dbs = sorted(_databases_in_importer(importer))
    print(f"[Excel] Metal database name(s): {metal_dbs}")

    if CFG.overwrite_metal_databases:
        for db_name in metal_dbs:
            if db_name in bd.databases:
                del bd.databases[db_name]
                print(f"[Write] Deleted existing database: {db_name}")

    _validate_importer_payload(importer)
    importer.write_database()
    print(f"[Write] Completed: {xlsx.name}")


def run() -> None:
    """Main entry point."""
    actual_ecoinvent_db, biosphere_db = _prepare_project()

    folder = CFG.excel_folder.expanduser().resolve()
    if not folder.exists():
        raise FileNotFoundError(f"Excel folder not found: {folder}")

    biosphere_name_map = load_biosphere_mapping_fix(CFG.biosphere_mapping_fix_file)
    print(f"[Biosphere map] Loaded {len(biosphere_name_map)} name replacements")

    for xlsx in _iter_excels(folder):
        _process_excel(
            xlsx=xlsx,
            actual_ecoinvent_db=actual_ecoinvent_db,
            biosphere_db=biosphere_db,
            biosphere_name_map=biosphere_name_map,
        )

    print("\n[Done] Databases in this project:")
    for name in sorted(bd.databases):
        print(f" - {name}")


if __name__ == "__main__":
    run()
