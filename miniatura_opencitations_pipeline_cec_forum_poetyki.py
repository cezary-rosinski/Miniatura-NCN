"""
Miniatura NCN: complete OAI-PMH/PDF -> CEC/GROBID -> OpenCitations pipeline.

The script keeps the analytical flow of the original
``2.2. miniatura_opencitations_pipeline.py``, but makes the stages explicit,
reproducible and independently runnable.

The PDF pilot uses the OpenCitations Citation Extraction Service (CEC). CEC
internally runs GROBID ``processFulltextDocument``. The pilot is deterministic
and stratified by publication period and OAI-PMH reference-list length.

Recommended order on Windows:

1. Start the CEC Docker Compose stack described at
   https://github.com/opencitations/cec
2. Run only the PDF pilot:

   python miniatura_opencitations_pipeline_cec.py --stage pdf

3. Inspect:
   data/final_pipeline_output/01_pdf_extraction_evaluation/pdf_vs_oai_summary.csv
4. Run the complete OAI-PMH pipeline:

   python miniatura_opencitations_pipeline_cec.py --stage oai

Use ``--refresh-cec`` to ignore locally cached TEI files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import math
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit

import pandas as pd
import requests

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - convenience fallback
    def tqdm(iterable=None, *args, **kwargs):
        return iterable if iterable is not None else []

try:
    from Levenshtein import distance as levenshtein_distance
except ImportError:  # pragma: no cover - slower but dependency-free fallback
    levenshtein_distance = None

from miniatura_parsing_anystyle import map_anystyle_to_oc, normalize_text
from miniatura_parsing_from_pdf import (
    build_pdf_jobs,
    load_inputs,
    merge_manifest_and_pickle,
    parse_tei_references,
    resolve_pdf_path,
)


# %%
# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

XLSX_PATH = DATA_DIR / "forum_poetyki_harvesting_info.xlsx"
PICKLE_PATH = DATA_DIR / "Forum_Poetyki_harvested.pkl"
PDF_BASE_DIR = PROJECT_ROOT

OUTPUT_DIR = DATA_DIR / "final_pipeline_output"
PDF_EVAL_DIR = OUTPUT_DIR / "01_pdf_extraction_evaluation"
OAI_PIPELINE_DIR = OUTPUT_DIR / "02_oai_to_opencitations"
CACHE_DIR = OUTPUT_DIR / "cache"
CEC_TEI_DIR = CACHE_DIR / "cec_tei"
CEC_DIAGNOSTICS_DIR = CACHE_DIR / "cec_diagnostics"

LANG_MODE = "en"

# The size remains comparable with the original pilot. Representativeness is
# improved by deterministic stratification rather than by taking head(20).
PDF_SAMPLE_SIZE = 50
PDF_SAMPLE_RANDOM_STATE = 20260730
PDF_YEAR_PERIODS = 3
PDF_REFERENCE_BANDS = 3

# OpenCitations CEC API, as specified in opencitations/cec.
CEC_BASE_URL = os.environ.get("CEC_BASE_URL", "http://127.0.0.1:5001").rstrip("/")
CEC_EXTRACTOR_PATH = "/cex/api/extractor"
CEC_TIMEOUT_SECONDS = 1800
CEC_CONSOLIDATE = False
CEC_MAX_WORKERS = 1

# Optional health check for the GROBID service exposed by the official CEC
# Docker Compose file. Failure here is reported as a warning because CEC may
# still reach GROBID over Docker's internal network.
GROBID_HEALTH_URL = os.environ.get(
    "GROBID_HEALTH_URL",
    "http://127.0.0.1:8070/api/isalive",
)

# AnyStyle. Set ANYSTYLE_CMD in the environment if the executable is elsewhere.
ANYSTYLE_CMD = os.environ.get(
    "ANYSTYLE_CMD",
    r"C:\Ruby34-x64\bin\anystyle.bat",
)

# Crossref. Replace the placeholder or set CROSSREF_MAILTO in the environment.
CROSSREF_MAILTO = os.environ.get("CROSSREF_MAILTO", "your.email@example.org")
CROSSREF_USER_AGENT = (
    "MiniaturaOpenCitationsPipeline/0.2 "
    f"(mailto:{CROSSREF_MAILTO})"
)
CROSSREF_ROWS = 3
CROSSREF_MAX_WORKERS = 3
CROSSREF_SLEEP_SECONDS = 0.10
CROSSREF_CACHE_PATH = CACHE_DIR / "crossref_cache.json"
CROSSREF_TITLE_THRESHOLD = 0.85
CROSSREF_LOOSE_THRESHOLD = 0.78

DEDUPLICATION_THRESHOLD = 30.0

# oc_validator >= 0.3.3 is required for the closure command. Prefer setting
# OC_VALIDATOR_CMD to the oc_validator executable. OC_VALIDATOR_PYTHON may
# instead point to the Python executable in the validator virtual environment.
OC_VALIDATOR_CMD = os.environ.get("OC_VALIDATOR_CMD", "")
OC_VALIDATOR_PYTHON = os.environ.get("OC_VALIDATOR_PYTHON", "")
VALIDATOR_SKIP_ID_EXISTENCE = True

# Image versions stated by the current CEC root README. They are recorded as
# expected versions; the API does not expose a version endpoint.
EXPECTED_CEC_EXTRACTOR_IMAGE = "opencitations/oc_cec_extractor:1.1.0"
EXPECTED_CEC_GROBID_IMAGE = "opencitations/oc_cec_grobid:1.2.0"

TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}
cache_lock = Lock()


PDF_SUMMARY_FIELDS = [
    "identifier",
    "publisher_id",
    "doi",
    "article_title",
    "year",
    "issue",
    "lang",
    "pdf_filename",
    "pdf_path",
    "pdf_sha256",
    "sampling_stratum",
    "sampling_probability",
    "sample_weight",
    "extractor_backend",
    "grobid_success",
    "error_type",
    "error",
    "tei_bibl_struct_count",
    "pdf_reference_count",
    "gold_reference_count",
    "difference_pdf_minus_gold",
    "reference_count_ratio",
]

PDF_REFERENCE_FIELDS = [
    "identifier",
    "publisher_id",
    "doi",
    "article_title",
    "year",
    "issue",
    "lang",
    "pdf_filename",
    "pdf_path",
    "extractor_backend",
    "grobid_success",
    "error_type",
    "error",
    "reference_index",
    "raw_reference",
    "raw_reference_source",
    "parsed_title",
    "parsed_author",
    "parsed_pub_date",
    "parsed_venue",
    "parsed_volume",
    "parsed_issue",
    "parsed_page",
    "parsed_type",
    "parsed_doi",
]

GOLD_REFERENCE_FIELDS = [
    "identifier",
    "publisher_id",
    "doi",
    "article_title",
    "year",
    "issue",
    "lang",
    "pdf_filename",
    "reference_index",
    "raw_reference",
    "gold_ref_id",
]

PDF_SAMPLE_MANIFEST_FIELDS = [
    "source_row",
    "identifier",
    "publisher_id",
    "doi",
    "article_title",
    "year",
    "issue",
    "lang",
    "pdf_filename",
    "pdf_rel_path",
    "gold_reference_count",
    "year_period",
    "reference_band",
    "sampling_stratum",
    "population_n",
    "sample_n",
    "sampling_probability",
    "sample_weight",
]

ANYSTYLE_FIELDS = [
    "_row_order",
    "reference_key",
    "identifier",
    "publisher_id",
    "article_doi",
    "article_title",
    "lang",
    "citing_id",
    "reference_index",
    "gold_ref_id",
    "raw_reference",
    "parsed_json",
    "title",
    "author",
    "pub_date",
    "venue",
    "volume",
    "issue",
    "page",
    "type",
    "doi",
]

ANYSTYLE_ERROR_FIELDS = [
    "identifier",
    "publisher_id",
    "article_doi",
    "article_title",
    "lang",
    "citing_id",
    "reference_index",
    "raw_reference",
    "error_type",
    "error",
]

OC_METADATA_FIELDS = [
    "id",
    "title",
    "author",
    "pub_date",
    "venue",
    "volume",
    "issue",
    "page",
    "type",
    "publisher",
    "editor",
]

OC_CITATION_FIELDS = ["citing_id", "cited_id"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("miniatura_pipeline")


# %%
# =============================================================================
# BASIC HELPERS
# =============================================================================

def ensure_output_dirs() -> None:
    for path in [
        OUTPUT_DIR,
        PDF_EVAL_DIR,
        OAI_PIPELINE_DIR,
        CACHE_DIR,
        CEC_TEI_DIR,
        CEC_DIAGNOSTICS_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def clean_str(value) -> str:
    if value is None:
        return ""
    if not isinstance(value, (list, tuple, dict, set)):
        try:
            if bool(pd.isna(value)):
                return ""
        except (TypeError, ValueError):
            pass
    return normalize_text(value)


def safe_filename(value: str) -> str:
    value = clean_str(value)
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value.strip("._") or "unknown"


def sha256_text(text: str, length: int | None = None) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return digest[:length] if length else digest


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        while True:
            chunk = file_handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def stable_temp_id(*parts, prefix: str = "temp") -> str:
    key = " | ".join(clean_str(part) for part in parts if clean_str(part))
    if not key:
        key = "empty"
    return f"{prefix}:{sha256_text(key, 24)}"


def normalize_doi(value) -> str:
    doi = clean_str(value).lower()
    doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi)
    return doi.strip().rstrip(".,;")


def id_from_doi_or_temp(doi, *parts) -> str:
    normalized = normalize_doi(doi)
    if normalized:
        return f"doi:{normalized}"
    return stable_temp_id(*parts)


def extract_year(value) -> str:
    match = re.search(r"(1[6-9]\d{2}|20\d{2}|21\d{2})", clean_str(value))
    return match.group(1) if match else ""


def write_csv(rows, path: Path, fieldnames: list[str]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    records = list(rows)

    with path.open("w", encoding="utf-8", newline="") as file_handle:
        writer = csv.DictWriter(
            file_handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in records:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(value, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_json_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON cache: {path}") from exc


def save_json_cache(cache: dict, path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    write_json(cache, temporary)
    temporary.replace(path)


def load_jobs() -> pd.DataFrame:
    missing = [path for path in [XLSX_PATH, PICKLE_PATH] if not path.exists()]
    if missing:
        missing_text = "\n".join(f"- {path}" for path in missing)
        raise FileNotFoundError(
            "Missing pipeline input files:\n"
            f"{missing_text}\n"
            "Place them in the repository's data directory."
        )

    manifest, harvested = load_inputs(XLSX_PATH, PICKLE_PATH)
    merged = merge_manifest_and_pickle(manifest, harvested)
    jobs = build_pdf_jobs(
        merged,
        lang_mode=LANG_MODE,
        only_ok_pdfs=True,
    )

    if jobs.empty:
        raise ValueError(
            "No PDF jobs were built. Check LANG_MODE and the *_status/*_path "
            "columns in the harvesting manifest."
        )

    return jobs.reset_index(drop=True)


# %%
# =============================================================================
# OAI-PMH GOLD REFERENCES
# =============================================================================

def extract_gold_refs(job: dict) -> list[dict]:
    output = []

    for index, reference in enumerate(
        job.get("references_gold", []) or [],
        start=1,
    ):
        if isinstance(reference, dict):
            citation = clean_str(reference.get("citation", ""))
            reference_id = clean_str(reference.get("ref_id", ""))
        else:
            citation = clean_str(reference)
            reference_id = ""

        if citation:
            output.append(
                {
                    "reference_index": index,
                    "raw_reference": citation,
                    "gold_ref_id": reference_id,
                }
            )

    return output


# %%
# =============================================================================
# REPRESENTATIVE PDF PILOT SAMPLE
# =============================================================================

def _assign_year_periods(frame: pd.DataFrame) -> pd.Series:
    labels = pd.Series("year_unknown", index=frame.index, dtype="object")
    years = pd.to_numeric(frame["year"], errors="coerce")
    valid = years.notna()

    if not valid.any():
        return labels

    unique_years = years[valid].nunique()
    if unique_years == 1:
        labels.loc[valid] = "period_1"
        return labels

    bins = min(PDF_YEAR_PERIODS, unique_years)
    codes = pd.cut(
        years.loc[valid],
        bins=bins,
        labels=False,
        include_lowest=True,
        duplicates="drop",
    )
    labels.loc[valid] = codes.map(lambda value: f"period_{int(value) + 1}")
    return labels


def _assign_reference_bands(frame: pd.DataFrame) -> pd.Series:
    labels = pd.Series("zero", index=frame.index, dtype="object")
    positive = frame["gold_reference_count"] > 0

    if not positive.any():
        return labels

    counts = frame.loc[positive, "gold_reference_count"]
    unique_counts = counts.nunique()

    if unique_counts == 1:
        labels.loc[positive] = "positive_1"
        return labels

    bands = min(PDF_REFERENCE_BANDS, unique_counts)
    codes = pd.qcut(
        counts,
        q=bands,
        labels=False,
        duplicates="drop",
    )
    labels.loc[positive] = codes.map(lambda value: f"ref_q{int(value) + 1}")
    return labels


def _allocate_sample(
    stratum_sizes: pd.Series,
    sample_size: int,
) -> pd.Series:
    if sample_size < len(stratum_sizes):
        raise ValueError(
            f"PDF sample size {sample_size} is smaller than the number of "
            f"non-empty strata ({len(stratum_sizes)}). Increase "
            "PDF_SAMPLE_SIZE or reduce the number of bands."
        )

    raw = sample_size * stratum_sizes / stratum_sizes.sum()
    allocation = (
        raw.apply(math.floor)
        .astype(int)
        .clip(lower=1)
        .clip(upper=stratum_sizes)
    )

    while int(allocation.sum()) < sample_size:
        can_add = allocation < stratum_sizes
        priorities = (raw - allocation).where(can_add, -math.inf)
        allocation.loc[priorities.idxmax()] += 1

    while int(allocation.sum()) > sample_size:
        can_remove = allocation > 1
        priorities = (allocation - raw).where(can_remove, -math.inf)
        if not can_remove.any():
            break
        allocation.loc[priorities.idxmax()] -= 1

    return allocation


def select_pdf_pilot_sample(
    jobs: pd.DataFrame,
    sample_size: int = PDF_SAMPLE_SIZE,
    random_state: int = PDF_SAMPLE_RANDOM_STATE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = (
        jobs.copy()
        .reset_index()
        .rename(columns={"index": "source_row"})
    )

    sample_size = min(int(sample_size), len(frame))
    frame["gold_reference_count"] = frame.apply(
        lambda row: len(extract_gold_refs(row.to_dict())),
        axis=1,
    )
    frame["year_period"] = _assign_year_periods(frame)
    frame["reference_band"] = _assign_reference_bands(frame)
    frame["sampling_stratum"] = (
        frame["year_period"] + "|" + frame["reference_band"]
    )

    if frame["lang"].nunique(dropna=True) > 1:
        frame["sampling_stratum"] += (
            "|lang=" + frame["lang"].fillna("unknown").astype(str)
        )

    stratum_sizes = frame.groupby(
        "sampling_stratum",
        sort=True,
        observed=True,
    ).size()
    allocation = _allocate_sample(stratum_sizes, sample_size)

    sample_parts = []
    for stratum, number in allocation.items():
        group = frame[frame["sampling_stratum"] == stratum]
        group_seed = int(
            sha256_text(f"{random_state}|{stratum}", 8),
            16,
        )
        sample_parts.append(
            group.sample(n=int(number), random_state=group_seed)
        )

    sample = pd.concat(sample_parts, ignore_index=True)
    allocation_frame = pd.DataFrame(
        {
            "sampling_stratum": stratum_sizes.index,
            "population_n": stratum_sizes.values,
            "sample_n": allocation.reindex(stratum_sizes.index).values,
        }
    )
    allocation_frame["population_share"] = (
        allocation_frame["population_n"] / len(frame)
    )
    allocation_frame["sample_share"] = (
        allocation_frame["sample_n"] / len(sample)
    )
    allocation_frame["sampling_probability"] = (
        allocation_frame["sample_n"]
        / allocation_frame["population_n"]
    )
    allocation_frame["sample_weight"] = (
        1 / allocation_frame["sampling_probability"]
    )

    sample = sample.merge(
        allocation_frame,
        on="sampling_stratum",
        how="left",
    )
    sample = sample.sort_values(
        ["year", "issue", "identifier", "lang"],
        kind="stable",
    ).reset_index(drop=True)

    return sample, allocation_frame


# %%
# =============================================================================
# CEC/GROBID EXTRACTION
# =============================================================================

def check_cec_connection() -> None:
    parsed = urlsplit(CEC_BASE_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as exc:
        raise ConnectionError(
            f"CEC is not reachable at {CEC_BASE_URL}. Start the official "
            "Docker Compose stack with `docker compose up -d` and verify "
            "that the extractor is exposed on port 5001."
        ) from exc

    try:
        response = requests.get(GROBID_HEALTH_URL, timeout=10)
        if response.status_code != 200:
            LOGGER.warning(
                "GROBID health endpoint returned HTTP %s: %s",
                response.status_code,
                GROBID_HEALTH_URL,
            )
    except requests.RequestException:
        LOGGER.warning(
            "Could not query %s. CEC may still reach GROBID internally.",
            GROBID_HEALTH_URL,
        )


def _normalize_cec_download_url(download_url: str) -> str:
    marker = "/cex/api/download/"
    if marker not in download_url:
        return download_url
    suffix = download_url.split(marker, 1)[1]
    return f"{CEC_BASE_URL}{marker}{suffix}"


def _read_json_zip_members(
    archive: zipfile.ZipFile,
    suffix: str,
) -> list[dict | list]:
    payloads = []
    for member in archive.namelist():
        if member.lower().endswith(suffix.lower()):
            try:
                payloads.append(
                    json.loads(archive.read(member).decode("utf-8-sig"))
                )
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
    return payloads


def cec_pdf_to_tei(
    pdf_path: Path,
    session: requests.Session,
) -> tuple[str, dict]:
    endpoint = f"{CEC_BASE_URL}{CEC_EXTRACTOR_PATH}"
    fields = {
        "perform_alignment": "false",
        "create_rdf": "false",
        "consolidate": "true" if CEC_CONSOLIDATE else "false",
        "max_workers": str(CEC_MAX_WORKERS),
    }

    with pdf_path.open("rb") as file_handle:
        response = session.post(
            endpoint,
            files={
                "input_files_or_archives": (
                    pdf_path.name,
                    file_handle,
                    "application/pdf",
                )
            },
            data=fields,
            timeout=(30, CEC_TIMEOUT_SECONDS),
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text[:1000]
        raise RuntimeError(
            f"CEC upload failed with HTTP {response.status_code}: {body}"
        ) from exc

    try:
        payload = response.json()
    except requests.JSONDecodeError as exc:
        raise RuntimeError(
            f"CEC returned non-JSON response: {response.text[:1000]}"
        ) from exc

    download_url = clean_str(payload.get("download_url", ""))
    if not download_url:
        raise RuntimeError(f"CEC response has no download_url: {payload}")

    download_url = _normalize_cec_download_url(download_url)
    archive_response = session.get(
        download_url,
        timeout=(30, CEC_TIMEOUT_SECONDS),
    )
    archive_response.raise_for_status()

    try:
        with zipfile.ZipFile(io.BytesIO(archive_response.content)) as archive:
            members = [
                member
                for member in archive.namelist()
                if not member.endswith("/")
            ]
            tei_members = [
                member
                for member in members
                if member.lower().endswith((".tei.xml", ".grobid.xml"))
            ]
            manifests = _read_json_zip_members(archive, "manifest.json")

            if not tei_members:
                raise RuntimeError(
                    "CEC archive contains no TEI XML. "
                    f"Members: {members}. Manifests: {manifests}"
                )

            if len(tei_members) > 1:
                matching = [
                    member
                    for member in tei_members
                    if pdf_path.stem.lower() in Path(member).name.lower()
                ]
                if len(matching) == 1:
                    tei_member = matching[0]
                else:
                    raise RuntimeError(
                        "CEC returned more than one TEI file for a single PDF: "
                        f"{tei_members}"
                    )
            else:
                tei_member = tei_members[0]

            tei_xml = archive.read(tei_member).decode(
                "utf-8-sig",
                errors="replace",
            )
    except zipfile.BadZipFile as exc:
        raise RuntimeError("CEC download is not a valid ZIP archive.") from exc

    diagnostics = {
        "cec_response": payload,
        "download_url_path": urlsplit(download_url).path,
        "archive_members": members,
        "tei_member": tei_member,
        "manifests": manifests,
        "consolidate": CEC_CONSOLIDATE,
        "max_workers": CEC_MAX_WORKERS,
    }
    return tei_xml, diagnostics


def _bibl_struct_text(node: ET.Element) -> str:
    return " ".join(
        fragment.strip()
        for fragment in node.itertext()
        if fragment and fragment.strip()
    )


def parse_tei_references_resilient(tei_xml: str) -> tuple[list[dict], int]:
    """
    Parse GROBID references without requiring ``note[@type=raw_reference]``.

    CEC's GROBID client currently calls processFulltextDocument without
    ``include_raw_citations=True``. In that output, ``biblStruct`` elements may
    be complete while the raw-reference note is absent. The original pipeline
    filtered those records out and could therefore create an empty CSV.
    """
    try:
        root = ET.fromstring(tei_xml)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid TEI XML: {exc}") from exc

    nodes = root.findall(".//tei:listBibl/tei:biblStruct", TEI_NS)
    parsed = parse_tei_references(tei_xml)
    output = []

    for index, node in enumerate(nodes, start=1):
        reference = parsed[index - 1] if index - 1 < len(parsed) else {}
        raw_note = clean_str(reference.get("raw_reference", ""))
        fallback_text = _bibl_struct_text(node)
        raw_reference = raw_note or fallback_text
        raw_source = (
            "grobid_raw_note"
            if raw_note
            else "biblStruct_text_fallback"
        )

        row = {
            "reference_index": reference.get("tei_ref_position", index),
            "raw_reference": raw_reference,
            "raw_reference_source": raw_source,
            "parsed_title": clean_str(reference.get("title", "")),
            "parsed_author": clean_str(reference.get("author", "")),
            "parsed_pub_date": clean_str(reference.get("pub_date", "")),
            "parsed_venue": clean_str(reference.get("venue", "")),
            "parsed_volume": clean_str(reference.get("volume", "")),
            "parsed_issue": clean_str(reference.get("issue", "")),
            "parsed_page": clean_str(reference.get("page", "")),
            "parsed_type": clean_str(reference.get("type", "")),
            "parsed_doi": (
                normalize_doi(reference.get("id", ""))
                if clean_str(reference.get("id", "")).lower().startswith(
                    ("doi:", "10.")
                )
                else ""
            ),
        }

        if any(
            row[field]
            for field in [
                "raw_reference",
                "parsed_title",
                "parsed_author",
                "parsed_pub_date",
                "parsed_venue",
            ]
        ):
            output.append(row)

    return output, len(nodes)


def extract_pdf_refs(
    job: dict,
    session: requests.Session,
    refresh_cec: bool = False,
) -> dict:
    pdf_path = resolve_pdf_path(PDF_BASE_DIR, job["pdf_rel_path"])
    cache_stem = safe_filename(
        f"{job.get('publisher_id', '')}_{job.get('lang', '')}"
    )
    tei_path = CEC_TEI_DIR / f"{cache_stem}.grobid.tei.xml"
    diagnostics_path = CEC_DIAGNOSTICS_DIR / f"{cache_stem}.json"

    result = {
        "success": False,
        "error_type": "",
        "error": "",
        "pdf_path": str(pdf_path),
        "pdf_sha256": "",
        "tei_bibl_struct_count": 0,
        "refs": [],
        "tei_cache_path": str(tei_path),
    }

    try:
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        result["pdf_sha256"] = sha256_file(pdf_path)

        if tei_path.exists() and not refresh_cec:
            tei_xml = tei_path.read_text(
                encoding="utf-8-sig",
                errors="replace",
            )
            diagnostics = {
                "source": "local_tei_cache",
                "tei_path": str(tei_path),
            }
        else:
            tei_xml, diagnostics = cec_pdf_to_tei(pdf_path, session)
            tei_path.write_text(tei_xml, encoding="utf-8")

        refs, bibl_count = parse_tei_references_resilient(tei_xml)
        result["tei_bibl_struct_count"] = bibl_count
        result["refs"] = refs

        diagnostics.update(
            {
                "pdf_path": str(pdf_path),
                "pdf_sha256": result["pdf_sha256"],
                "tei_cache_path": str(tei_path),
                "tei_bibl_struct_count": bibl_count,
                "parsed_reference_count": len(refs),
            }
        )
        write_json(diagnostics, diagnostics_path)

        if not refs:
            result["error_type"] = "no_references_in_tei"
            result["error"] = (
                "CEC/GROBID returned TEI, but no usable bibliography "
                f"references were found (biblStruct count: {bibl_count})."
            )
            return result

        result["success"] = True
        return result

    except FileNotFoundError as exc:
        result["error_type"] = "pdf_not_found"
        result["error"] = str(exc)
    except (requests.RequestException, ConnectionError) as exc:
        result["error_type"] = "cec_connection_error"
        result["error"] = str(exc)
    except (ET.ParseError, ValueError) as exc:
        result["error_type"] = "tei_parse_error"
        result["error"] = str(exc)
    except Exception as exc:  # keep per-PDF diagnostics without hiding failure
        result["error_type"] = "cec_processing_error"
        result["error"] = str(exc)

    return result


def compare_reference_counts(
    pdf_refs: list[dict],
    gold_refs: list[dict],
) -> dict:
    pdf_count = len(pdf_refs)
    gold_count = len(gold_refs)
    return {
        "pdf_reference_count": pdf_count,
        "gold_reference_count": gold_count,
        "difference_pdf_minus_gold": pdf_count - gold_count,
        "reference_count_ratio": (
            pdf_count / gold_count if gold_count else ""
        ),
    }


def run_pdf_extraction_evaluation(
    jobs: pd.DataFrame,
    sample_size: int = PDF_SAMPLE_SIZE,
    random_state: int = PDF_SAMPLE_RANDOM_STATE,
    refresh_cec: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sample, allocation = select_pdf_pilot_sample(
        jobs,
        sample_size=sample_size,
        random_state=random_state,
    )

    write_csv(
        sample.to_dict("records"),
        PDF_EVAL_DIR / "pdf_sample_manifest.csv",
        PDF_SAMPLE_MANIFEST_FIELDS,
    )
    write_csv(
        allocation.to_dict("records"),
        PDF_EVAL_DIR / "pdf_sample_allocation.csv",
        [
            "sampling_stratum",
            "population_n",
            "sample_n",
            "population_share",
            "sample_share",
            "sampling_probability",
            "sample_weight",
        ],
    )

    summary_rows = []
    pdf_reference_rows = []
    gold_reference_rows = []

    with requests.Session() as session:
        for _, job_row in tqdm(
            sample.iterrows(),
            total=len(sample),
            desc="CEC/GROBID PDF pilot",
        ):
            job = job_row.to_dict()
            pdf_result = extract_pdf_refs(
                job,
                session=session,
                refresh_cec=refresh_cec,
            )
            gold_refs = extract_gold_refs(job)

            if gold_refs and not pdf_result["refs"] and not pdf_result["error"]:
                pdf_result["success"] = False
                pdf_result["error_type"] = "empty_extraction"
                pdf_result["error"] = (
                    "The OAI-PMH record contains references, but CEC/GROBID "
                    "returned none."
                )

            base = {
                "identifier": job.get("identifier", ""),
                "publisher_id": job.get("publisher_id", ""),
                "doi": job.get("doi", ""),
                "article_title": job.get("article_title", ""),
                "year": job.get("year", ""),
                "issue": job.get("issue", ""),
                "lang": job.get("lang", ""),
                "pdf_filename": job.get("pdf_filename", ""),
                "pdf_path": pdf_result["pdf_path"],
                "pdf_sha256": pdf_result["pdf_sha256"],
                "sampling_stratum": job.get("sampling_stratum", ""),
                "sampling_probability": job.get(
                    "sampling_probability",
                    "",
                ),
                "sample_weight": job.get("sample_weight", ""),
                "extractor_backend": "CEC/GROBID",
                "grobid_success": pdf_result["success"],
                "error_type": pdf_result["error_type"],
                "error": pdf_result["error"],
                "tei_bibl_struct_count": pdf_result[
                    "tei_bibl_struct_count"
                ],
            }
            summary_rows.append(
                {
                    **base,
                    **compare_reference_counts(
                        pdf_result["refs"],
                        gold_refs,
                    ),
                }
            )

            for reference in pdf_result["refs"]:
                pdf_reference_rows.append({**base, **reference})

            gold_base = {
                key: base[key]
                for key in [
                    "identifier",
                    "publisher_id",
                    "doi",
                    "article_title",
                    "year",
                    "issue",
                    "lang",
                    "pdf_filename",
                ]
            }
            for reference in gold_refs:
                gold_reference_rows.append({**gold_base, **reference})

    write_csv(
        summary_rows,
        PDF_EVAL_DIR / "pdf_vs_oai_summary.csv",
        PDF_SUMMARY_FIELDS,
    )
    write_csv(
        pdf_reference_rows,
        PDF_EVAL_DIR / "pdf_extracted_references.csv",
        PDF_REFERENCE_FIELDS,
    )
    write_csv(
        gold_reference_rows,
        PDF_EVAL_DIR / "oai_gold_references_for_pdf_sample.csv",
        GOLD_REFERENCE_FIELDS,
    )

    failures = [
        row for row in summary_rows if not bool(row["grobid_success"])
    ]
    write_csv(
        failures,
        PDF_EVAL_DIR / "pdf_extraction_failures.csv",
        PDF_SUMMARY_FIELDS,
    )

    if not pdf_reference_rows:
        error_counts = pd.Series(
            [row["error_type"] or "unknown" for row in summary_rows]
        ).value_counts().to_dict()
        raise RuntimeError(
            "The PDF pilot produced zero extracted references. "
            "The CSV files contain headers and diagnostics. Inspect "
            f"{PDF_EVAL_DIR / 'pdf_vs_oai_summary.csv'}. "
            f"Error counts: {error_counts}"
        )

    return pd.DataFrame(summary_rows), allocation


# %%
# =============================================================================
# ANYSTYLE PARSING OF THE FULL OAI-PMH CORPUS
# =============================================================================

def resolve_anystyle_command() -> str:
    direct_path = Path(ANYSTYLE_CMD)
    if direct_path.exists():
        return str(direct_path)

    discovered = shutil.which(ANYSTYLE_CMD)
    if discovered:
        return discovered

    raise FileNotFoundError(
        f"AnyStyle executable not found: {ANYSTYLE_CMD}. "
        "Set the ANYSTYLE_CMD environment variable."
    )


def parse_with_anystyle_batch(
    references: list[str],
    anystyle_cmd: str,
) -> list[dict]:
    normalized = [
        clean_str(reference)
        for reference in references
        if clean_str(reference)
    ]
    if not normalized:
        return []

    with tempfile.TemporaryDirectory() as temporary_dir:
        input_path = Path(temporary_dir) / "references.txt"
        input_path.write_text("\n".join(normalized), encoding="utf-8")

        result = subprocess.run(
            [
                anystyle_cmd,
                "--stdout",
                "-f",
                "json",
                "parse",
                str(input_path),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    if result.returncode != 0:
        raise RuntimeError(
            f"AnyStyle returned {result.returncode}\n"
            f"STDERR:\n{result.stderr}\n"
            f"STDOUT:\n{result.stdout}"
        )

    stdout = result.stdout.strip()
    if not stdout:
        return []

    parsed = json.loads(stdout)
    if not isinstance(parsed, list):
        raise TypeError("AnyStyle JSON output is not a list.")
    return parsed


def _parse_anystyle_aligned(
    gold_refs: list[dict],
    anystyle_cmd: str,
) -> tuple[list[dict | None], list[dict]]:
    raw_refs = [reference["raw_reference"] for reference in gold_refs]
    if not raw_refs:
        return [], []

    errors = []
    parsed_batch = parse_with_anystyle_batch(raw_refs, anystyle_cmd)
    if len(parsed_batch) == len(raw_refs):
        return parsed_batch, errors

    # A length mismatch makes zip-based alignment unsafe. Retry one reference
    # at a time so every result remains attached to the correct source row.
    errors.append(
        {
            "error_type": "batch_length_mismatch",
            "error": (
                f"AnyStyle returned {len(parsed_batch)} records for "
                f"{len(raw_refs)} input references; retried individually."
            ),
        }
    )
    aligned: list[dict | None] = []

    for reference in raw_refs:
        try:
            single = parse_with_anystyle_batch(
                [reference],
                anystyle_cmd,
            )
            aligned.append(single[0] if single else None)
        except Exception as exc:
            aligned.append(None)
            errors.append(
                {
                    "error_type": "single_reference_parse_error",
                    "error": str(exc),
                    "raw_reference": reference,
                }
            )

    return aligned, errors


def parse_all_oai_references(
    jobs: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    anystyle_cmd = resolve_anystyle_command()
    parsed_rows = []
    error_rows = []
    row_order = 0

    for _, job_row in tqdm(
        jobs.iterrows(),
        total=len(jobs),
        desc="AnyStyle OAI-PMH parsing",
    ):
        job = job_row.to_dict()
        gold_refs = extract_gold_refs(job)
        if not gold_refs:
            continue

        citing_id = id_from_doi_or_temp(
            job.get("doi", ""),
            job.get("identifier", ""),
            job.get("publisher_id", ""),
            job.get("article_title", ""),
            job.get("lang", ""),
        )

        error_base = {
            "identifier": job.get("identifier", ""),
            "publisher_id": job.get("publisher_id", ""),
            "article_doi": normalize_doi(job.get("doi", "")),
            "article_title": clean_str(job.get("article_title", "")),
            "lang": job.get("lang", ""),
            "citing_id": citing_id,
        }

        try:
            parsed_aligned, batch_errors = _parse_anystyle_aligned(
                gold_refs,
                anystyle_cmd,
            )
        except Exception as exc:
            for reference in gold_refs:
                error_rows.append(
                    {
                        **error_base,
                        "reference_index": reference["reference_index"],
                        "raw_reference": reference["raw_reference"],
                        "error_type": "article_batch_parse_error",
                        "error": str(exc),
                    }
                )
            continue

        for batch_error in batch_errors:
            error_rows.append(
                {
                    **error_base,
                    "reference_index": "",
                    "raw_reference": batch_error.get(
                        "raw_reference",
                        "",
                    ),
                    **batch_error,
                }
            )

        for reference, parsed in zip(gold_refs, parsed_aligned):
            if parsed is None:
                error_rows.append(
                    {
                        **error_base,
                        "reference_index": reference["reference_index"],
                        "raw_reference": reference["raw_reference"],
                        "error_type": "empty_anystyle_result",
                        "error": "AnyStyle returned no aligned record.",
                    }
                )
                continue

            mapped = map_anystyle_to_oc(parsed)
            mapped.setdefault("doi", "")
            reference_key = (
                f"{citing_id}|{reference['reference_index']}"
            )

            parsed_rows.append(
                {
                    "_row_order": row_order,
                    "reference_key": reference_key,
                    **error_base,
                    "reference_index": reference["reference_index"],
                    "gold_ref_id": reference.get("gold_ref_id", ""),
                    "raw_reference": reference["raw_reference"],
                    "parsed_json": json.dumps(
                        parsed,
                        ensure_ascii=False,
                    ),
                    **mapped,
                }
            )
            row_order += 1

    write_csv(
        parsed_rows,
        OAI_PIPELINE_DIR / "oai_references_parsed_anystyle.csv",
        ANYSTYLE_FIELDS,
    )
    write_csv(
        error_rows,
        OAI_PIPELINE_DIR / "oai_references_parsing_errors.csv",
        ANYSTYLE_ERROR_FIELDS,
    )

    return pd.DataFrame(parsed_rows), pd.DataFrame(error_rows)


# %%
# =============================================================================
# CROSSREF DOI ENRICHMENT
# =============================================================================

def string_similarity(first, second) -> float:
    first = clean_str(first).casefold()
    second = clean_str(second).casefold()
    if not first or not second:
        return 0.0

    if levenshtein_distance is not None:
        return 1 - (
            levenshtein_distance(first, second)
            / max(len(first), len(second))
        )
    return SequenceMatcher(None, first, second).ratio()


def crossref_candidate_to_record(item: dict) -> dict:
    title = clean_str((item.get("title") or [""])[0])
    container = clean_str((item.get("container-title") or [""])[0])

    year = ""
    for key in [
        "published-print",
        "published-online",
        "published",
        "issued",
    ]:
        date_parts = item.get(key, {}).get("date-parts", [])
        if date_parts and date_parts[0]:
            year = str(date_parts[0][0])
            break

    authors = []
    for author in item.get("author", []) or []:
        name = " ".join(
            part
            for part in [
                clean_str(author.get("given", "")),
                clean_str(author.get("family", "")),
            ]
            if part
        )
        if name:
            authors.append(name)

    return {
        "crossref_doi": normalize_doi(item.get("DOI", "")),
        "crossref_title": title,
        "crossref_author": "; ".join(authors),
        "crossref_pub_date": year,
        "crossref_venue": container,
        "crossref_type": clean_str(item.get("type", "")),
        "crossref_score_raw": item.get("score", ""),
    }


def build_crossref_query(row: dict) -> str:
    parts = []

    for value in [
        row.get("title", ""),
        clean_str(row.get("author", "")).split(";")[0],
        extract_year(row.get("pub_date", "")),
        row.get("venue", ""),
    ]:
        value = clean_str(value)
        if value:
            parts.append(value)

    if not parts:
        parts.append(clean_str(row.get("raw_reference", "")))

    return " ".join(parts)


def query_crossref_threadsafe(
    query: str,
    cache: dict,
) -> dict:
    query = clean_str(query)
    cache_key = sha256_text(query)

    with cache_lock:
        if cache_key in cache:
            return cache[cache_key]

    try:
        response = requests.get(
            "https://api.crossref.org/works",
            params={
                "query.bibliographic": query,
                "rows": CROSSREF_ROWS,
                "mailto": CROSSREF_MAILTO,
            },
            headers={"User-Agent": CROSSREF_USER_AGENT},
            timeout=30,
        )

        if response.status_code in {429, 503}:
            retry_after = int(response.headers.get("Retry-After", "3"))
            time.sleep(max(3, retry_after))
            response = requests.get(
                "https://api.crossref.org/works",
                params={
                    "query.bibliographic": query,
                    "rows": CROSSREF_ROWS,
                    "mailto": CROSSREF_MAILTO,
                },
                headers={"User-Agent": CROSSREF_USER_AGENT},
                timeout=30,
            )

        response.raise_for_status()
        items = response.json().get("message", {}).get("items", [])
        result = {
            "query": query,
            "candidates": [
                crossref_candidate_to_record(item)
                for item in items
            ],
            "error": "",
        }
        time.sleep(CROSSREF_SLEEP_SECONDS)
    except Exception as exc:
        result = {
            "query": query,
            "candidates": [],
            "error": str(exc),
        }

    with cache_lock:
        cache[cache_key] = result
    return result


def score_crossref_candidate(row: dict, candidate: dict) -> dict:
    title_similarity = string_similarity(
        row.get("title", ""),
        candidate.get("crossref_title", ""),
    )
    author_similarity = string_similarity(
        clean_str(row.get("author", "")).split(";")[0],
        clean_str(candidate.get("crossref_author", "")).split(";")[0],
    )
    venue_similarity = string_similarity(
        row.get("venue", ""),
        candidate.get("crossref_venue", ""),
    )

    local_year = extract_year(row.get("pub_date", ""))
    candidate_year = extract_year(
        candidate.get("crossref_pub_date", "")
    )
    year_match = int(
        bool(
            local_year
            and candidate_year
            and local_year == candidate_year
        )
    )
    final_score = (
        0.60 * title_similarity
        + 0.20 * author_similarity
        + 0.10 * venue_similarity
        + 0.10 * year_match
    )

    return {
        "crossref_match_score": final_score,
        "crossref_title_similarity": title_similarity,
        "crossref_author_similarity": author_similarity,
        "crossref_venue_similarity": venue_similarity,
        "crossref_year_match": year_match,
    }


def accept_crossref_candidate(
    candidate: dict,
    score: dict,
) -> bool:
    if not candidate.get("crossref_doi"):
        return False

    if (
        score["crossref_title_similarity"]
        >= CROSSREF_TITLE_THRESHOLD
        and score["crossref_year_match"] == 1
    ):
        return True

    return (
        score["crossref_match_score"]
        >= CROSSREF_LOOSE_THRESHOLD
        and score["crossref_title_similarity"] >= 0.80
    )


def enrich_one_crossref_row(
    row: dict,
    cache: dict,
) -> tuple[dict, list[dict]]:
    output = dict(row)
    existing_doi = normalize_doi(row.get("doi", ""))

    if existing_doi:
        output.update(
            {
                "doi_enriched": existing_doi,
                "doi_source": "existing",
                "crossref_match_score": "",
                "crossref_type": "",
            }
        )
        return output, []

    query = build_crossref_query(row)
    result = query_crossref_threadsafe(query, cache)
    best_accepted_candidate = None
    best_accepted_score = None
    diagnostics = []

    for rank, candidate in enumerate(result["candidates"], start=1):
        score = score_crossref_candidate(row, candidate)
        accepted = accept_crossref_candidate(candidate, score)
        diagnostics.append(
            {
                "reference_key": row.get("reference_key", ""),
                "identifier": row.get("identifier", ""),
                "publisher_id": row.get("publisher_id", ""),
                "reference_index": row.get("reference_index", ""),
                "raw_reference": row.get("raw_reference", ""),
                "local_title": row.get("title", ""),
                "local_author": row.get("author", ""),
                "local_pub_date": row.get("pub_date", ""),
                "local_venue": row.get("venue", ""),
                "crossref_query": query,
                "crossref_candidate_rank": rank,
                **candidate,
                **score,
                "accepted_automatically": accepted,
                "crossref_error": result.get("error", ""),
            }
        )

        if (
            accepted
            and (
                best_accepted_score is None
                or score["crossref_match_score"]
                > best_accepted_score["crossref_match_score"]
            )
        ):
            best_accepted_candidate = candidate
            best_accepted_score = score

    if best_accepted_candidate:
        output.update(
            {
                "doi_enriched": best_accepted_candidate["crossref_doi"],
                "doi_source": "crossref",
                "crossref_match_score": best_accepted_score[
                    "crossref_match_score"
                ],
                "crossref_type": best_accepted_candidate[
                    "crossref_type"
                ],
            }
        )
    else:
        output.update(
            {
                "doi_enriched": "",
                "doi_source": "none",
                "crossref_match_score": "",
                "crossref_type": "",
            }
        )

    return output, diagnostics


def validate_crossref_configuration() -> None:
    if (
        not CROSSREF_MAILTO
        or CROSSREF_MAILTO == "cezary.rosinski@gmail.com"
        or "@" not in CROSSREF_MAILTO
    ):
        raise ValueError(
            "Set a real CROSSREF_MAILTO address before Crossref enrichment, "
            "or run with --skip-crossref."
        )


def enrich_with_crossref(
    parsed: pd.DataFrame,
    max_workers: int = CROSSREF_MAX_WORKERS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    validate_crossref_configuration()
    cache = load_json_cache(CROSSREF_CACHE_PATH)
    enriched_rows = []
    diagnostics_rows = []

    records = parsed.to_dict("records")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(enrich_one_crossref_row, row, cache)
            for row in records
        ]

        for number, future in enumerate(
            tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Crossref enrichment",
            ),
            start=1,
        ):
            row, diagnostics = future.result()
            enriched_rows.append(row)
            diagnostics_rows.extend(diagnostics)

            if number % 50 == 0:
                with cache_lock:
                    save_json_cache(cache, CROSSREF_CACHE_PATH)

    with cache_lock:
        save_json_cache(cache, CROSSREF_CACHE_PATH)

    enriched = pd.DataFrame(enriched_rows)
    if not enriched.empty and "_row_order" in enriched.columns:
        enriched = enriched.sort_values(
            "_row_order",
            kind="stable",
        ).reset_index(drop=True)

    diagnostics = pd.DataFrame(diagnostics_rows)
    write_csv(
        enriched.to_dict("records"),
        OAI_PIPELINE_DIR / "oai_references_crossref_enriched.csv",
        list(enriched.columns),
    )
    write_csv(
        diagnostics.to_dict("records"),
        OAI_PIPELINE_DIR / "crossref_diagnostics.csv",
        list(diagnostics.columns) if not diagnostics.empty else [
            "reference_key",
            "identifier",
            "publisher_id",
            "reference_index",
            "raw_reference",
            "crossref_query",
            "crossref_error",
        ],
    )

    return enriched, diagnostics


def skip_crossref_enrichment(
    parsed: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    enriched = parsed.copy()
    enriched["doi_enriched"] = enriched["doi"].map(normalize_doi)
    enriched["doi_source"] = enriched["doi_enriched"].map(
        lambda value: "existing" if value else "none"
    )
    enriched["crossref_match_score"] = ""
    enriched["crossref_type"] = ""

    write_csv(
        enriched.to_dict("records"),
        OAI_PIPELINE_DIR / "oai_references_crossref_enriched.csv",
        list(enriched.columns),
    )
    write_csv(
        [],
        OAI_PIPELINE_DIR / "crossref_diagnostics.csv",
        [
            "reference_key",
            "identifier",
            "publisher_id",
            "reference_index",
            "raw_reference",
            "crossref_query",
            "crossref_error",
        ],
    )
    return enriched, pd.DataFrame()


# %%
# =============================================================================
# MATCHING SCORE AND DEDUPLICATION
# =============================================================================

def record_doi(record: dict) -> str:
    return normalize_doi(
        record.get("doi_enriched", "")
        or record.get("doi", "")
    )


def matching_score(first: dict, second: dict) -> float:
    doi_match = int(
        bool(
            record_doi(first)
            and record_doi(first) == record_doi(second)
        )
    )
    title_match = string_similarity(
        first.get("title", ""),
        second.get("title", ""),
    )
    author_match = string_similarity(
        clean_str(first.get("author", "")).split(";")[0],
        clean_str(second.get("author", "")).split(";")[0],
    )
    source_match = string_similarity(
        first.get("venue", ""),
        second.get("venue", ""),
    )

    first_year = extract_year(first.get("pub_date", ""))
    second_year = extract_year(second.get("pub_date", ""))
    other_match = int(
        bool(
            first_year
            and second_year
            and first_year == second_year
            and clean_str(first.get("volume", ""))
            == clean_str(second.get("volume", ""))
            and clean_str(first.get("issue", ""))
            == clean_str(second.get("issue", ""))
            and clean_str(first.get("page", ""))
            == clean_str(second.get("page", ""))
        )
    )

    return (
        50 * doi_match
        + 25 * title_match
        + 10 * author_match
        + 10 * source_match
        + 5 * other_match
    )


def normalized_title_key(value) -> str:
    return re.sub(r"\W+", "", clean_str(value).casefold())


def deduplication_block_keys(record: dict) -> set[str]:
    keys = set()
    doi = record_doi(record)
    year = extract_year(record.get("pub_date", ""))
    title_key = normalized_title_key(record.get("title", ""))
    first_author = normalized_title_key(
        clean_str(record.get("author", "")).split(";")[0]
    )

    if doi:
        keys.add(f"doi:{doi}")
    if year:
        keys.add(f"year:{year}")
    if title_key:
        keys.add(f"title:{title_key[:16]}")
    if year and first_author:
        keys.add(f"author_year:{first_author[:16]}:{year}")
    return keys


def deduplicate_records(
    enriched: pd.DataFrame,
    threshold: float = DEDUPLICATION_THRESHOLD,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if enriched.empty:
        raise ValueError("There are no enriched references to deduplicate.")

    records = enriched.to_dict("records")
    records.sort(
        key=lambda record: (
            -int(bool(record_doi(record))),
            int(record.get("_row_order", 0)),
        )
    )

    canonical: list[dict] = []
    block_index: dict[str, set[int]] = {}
    mappings = []

    for record in tqdm(records, desc="Reference deduplication"):
        original_id = id_from_doi_or_temp(
            record_doi(record),
            record.get("title", ""),
            record.get("author", ""),
            record.get("pub_date", ""),
            record.get("venue", ""),
            record.get("raw_reference", ""),
        )
        block_keys = deduplication_block_keys(record)
        candidate_indices = set()
        for key in block_keys:
            candidate_indices.update(block_index.get(key, set()))

        best_index = None
        best_score = -1.0
        for candidate_index in candidate_indices:
            score = matching_score(
                record,
                canonical[candidate_index],
            )
            if score > best_score:
                best_index = candidate_index
                best_score = score

        if best_index is not None and best_score >= threshold:
            best_record = canonical[best_index]
            canonical_id = best_record["_canonical_id"]
            for key in block_keys:
                block_index.setdefault(key, set()).add(best_index)
            decision = "matched_existing"
        else:
            canonical_id = original_id
            new_record = dict(record)
            new_record["_canonical_id"] = canonical_id
            canonical.append(new_record)
            new_index = len(canonical) - 1
            for key in block_keys:
                block_index.setdefault(key, set()).add(new_index)
            decision = "new_canonical"

        best_record = (
            canonical[best_index]
            if best_index is not None
            else None
        )
        mappings.append(
            {
                "reference_key": record.get("reference_key", ""),
                "original_temp_id": original_id,
                "canonical_id": canonical_id,
                "matching_score": (
                    best_score if best_index is not None else ""
                ),
                "dedup_status": decision,
                "title": record.get("title", ""),
                "best_candidate_id": (
                    best_record.get("_canonical_id", "")
                    if best_record
                    else ""
                ),
                "best_candidate_title": (
                    best_record.get("title", "")
                    if best_record
                    else ""
                ),
                "threshold": threshold,
            }
        )

    canonical_frame = pd.DataFrame(canonical)
    mapping_frame = (
        pd.DataFrame(mappings)
        .sort_values("reference_key", kind="stable")
        .reset_index(drop=True)
    )

    write_csv(
        canonical_frame.to_dict("records"),
        OAI_PIPELINE_DIR / "deduplicated_metadata_records.csv",
        list(canonical_frame.columns),
    )
    write_csv(
        mapping_frame.to_dict("records"),
        OAI_PIPELINE_DIR / "deduplication_mapping.csv",
        list(mapping_frame.columns),
    )
    return canonical_frame, mapping_frame


# %%
# =============================================================================
# OPENCITATIONS NORMALIZATION AND EXPORT
# =============================================================================

TYPE_MAP = {
    "article": "journal article",
    "article-journal": "journal article",
    "journal-article": "journal article",
    "journal article": "journal article",
    "journal": "journal",
    "book": "book",
    "monograph": "monograph",
    "chapter": "book chapter",
    "book-chapter": "book chapter",
    "book chapter": "book chapter",
    "book-section": "book section",
    "book-part": "book part",
    "proceedings-article": "proceedings article",
    "paper-conference": "proceedings article",
    "proceedings": "proceedings",
    "report": "report",
    "report-series": "report series",
    "posted-content": "posted content",
    "webpage": "web content",
    "web": "web content",
    "web-content": "web content",
    "dataset": "dataset",
    "data-file": "data file",
    "dissertation": "dissertation",
    "thesis": "dissertation",
    "editorial": "editorial",
    "peer-review": "peer review",
    "reference-book": "reference book",
    "reference-entry": "reference entry",
    "component": "component",
    "standard": "standard",
    "other": "other",
}


def normalize_type(value) -> str:
    normalized = clean_str(value).casefold().replace("_", "-")
    return TYPE_MAP.get(normalized, "other")


def normalize_page(value) -> tuple[str, str]:
    original = clean_str(value)
    if not original:
        return "", ""

    text = original.replace("–", "-").replace("—", "-")
    text = re.sub(r"(?i)\b(?:pp?|s)\.?\s*", "", text)
    text = re.sub(r"\s+", "", text)

    token = r"(?:[A-Za-z]?\d+[A-Za-z]?|[ivxlcdmIVXLCDM]+)"
    single = re.fullmatch(token, text)
    if single:
        return f"{text}-{text}", "single_page_expanded"

    interval = re.fullmatch(fr"({token})-({token})", text)
    if not interval:
        return "", f"unrepresentable_page_value:{original}"

    start, end = interval.groups()
    if start.isdigit() and end.isdigit() and len(end) < len(start):
        end = start[: len(start) - len(end)] + end

    return f"{start}-{end}", (
        "normalized_page_interval"
        if f"{start}-{end}" != original
        else ""
    )


def sanitize_text_field(value) -> str:
    return (
        clean_str(value)
        .replace("[", "(")
        .replace("]", ")")
    )


def normalize_cited_metadata(
    row: dict,
) -> tuple[dict, list[dict]]:
    crossref_type = clean_str(row.get("crossref_type", ""))
    raw_type = crossref_type or row.get("type", "")
    page, page_note = normalize_page(row.get("page", ""))
    canonical_id = clean_str(row.get("_canonical_id", ""))

    metadata = {
        "id": canonical_id,
        "title": sanitize_text_field(row.get("title", "")),
        "author": sanitize_text_field(row.get("author", "")),
        "pub_date": extract_year(row.get("pub_date", "")),
        "venue": sanitize_text_field(row.get("venue", "")),
        "volume": sanitize_text_field(row.get("volume", "")),
        "issue": sanitize_text_field(row.get("issue", "")),
        "page": page,
        "type": normalize_type(raw_type),
        "publisher": sanitize_text_field(row.get("publisher", "")),
        "editor": sanitize_text_field(row.get("editor", "")),
    }

    log_rows = []
    if page_note:
        log_rows.append(
            {
                "id": canonical_id,
                "field": "page",
                "original_value": clean_str(row.get("page", "")),
                "normalized_value": page,
                "note": page_note,
            }
        )
    if clean_str(raw_type) != metadata["type"]:
        log_rows.append(
            {
                "id": canonical_id,
                "field": "type",
                "original_value": clean_str(raw_type),
                "normalized_value": metadata["type"],
                "note": (
                    "crossref_type_mapping"
                    if crossref_type
                    else "anystyle_type_mapping"
                ),
            }
        )
    original_date = clean_str(row.get("pub_date", ""))
    if original_date and original_date != metadata["pub_date"]:
        log_rows.append(
            {
                "id": canonical_id,
                "field": "pub_date",
                "original_value": original_date,
                "normalized_value": metadata["pub_date"],
                "note": "year_extracted",
            }
        )

    return metadata, log_rows


def missing_required_fields(metadata: dict) -> list[str]:
    identifiers = metadata["id"].split()
    internal_only = bool(identifiers) and all(
        identifier.startswith(("temp:", "local:"))
        for identifier in identifiers
    )
    if not internal_only:
        return []

    resource_type = metadata["type"]
    missing = []

    work_types = {
        "book",
        "dataset",
        "data file",
        "dissertation",
        "edited book",
        "editorial",
        "journal article",
        "monograph",
        "other",
        "peer review",
        "posted content",
        "web content",
        "proceedings article",
        "reference book",
        "report",
    }
    part_types = {
        "book chapter",
        "book part",
        "book section",
        "book track",
        "component",
        "reference entry",
    }
    series_types = {
        "book series",
        "book set",
        "journal",
        "proceedings",
        "proceedings series",
        "report series",
        "standard",
        "standard series",
    }

    if resource_type in work_types:
        if not metadata["title"]:
            missing.append("title")
        if not metadata["pub_date"]:
            missing.append("pub_date")
        if not metadata["author"] and not metadata["editor"]:
            missing.append("author_or_editor")
    elif resource_type in part_types:
        if not metadata["title"]:
            missing.append("title")
        if not metadata["venue"]:
            missing.append("venue")
    elif resource_type in series_types:
        if not metadata["title"]:
            missing.append("title")

    if metadata["volume"] and not metadata["venue"]:
        missing.append("venue_required_by_volume")
    if metadata["issue"] and not metadata["venue"]:
        missing.append("venue_required_by_issue")

    return sorted(set(missing))


def build_citing_metadata_from_job(job: dict) -> dict:
    citing_id = id_from_doi_or_temp(
        job.get("doi", ""),
        job.get("identifier", ""),
        job.get("publisher_id", ""),
        job.get("article_title", ""),
        job.get("lang", ""),
    )
    authors = []
    for author in job.get("authors", []) or []:
        if isinstance(author, dict):
            full_name = clean_str(author.get("full_name", ""))
            if not full_name:
                full_name = " ".join(
                    part
                    for part in [
                        clean_str(author.get("given_names", "")),
                        clean_str(author.get("surname", "")),
                    ]
                    if part
                )
            if full_name:
                authors.append(full_name)

    first_page = clean_str(job.get("fpage", ""))
    last_page = clean_str(job.get("lpage", ""))
    source_page = (
        f"{first_page}-{last_page}"
        if first_page and last_page
        else first_page
    )
    page, _ = normalize_page(source_page)

    return {
        "id": citing_id,
        "title": sanitize_text_field(job.get("article_title", "")),
        "author": sanitize_text_field("; ".join(authors)),
        "pub_date": extract_year(job.get("year", "")),
        "venue": sanitize_text_field(job.get("journal_title", "")),
        "volume": "",
        "issue": sanitize_text_field(job.get("issue", "")),
        "page": page,
        "type": "journal article",
        "publisher": "",
        "editor": "",
    }


def build_oc_export(
    jobs: pd.DataFrame,
    enriched: pd.DataFrame,
    canonical: pd.DataFrame,
    mappings: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    mapping_lookup = dict(
        zip(mappings["reference_key"], mappings["canonical_id"])
    )

    normalized_canonical = {}
    eligible_canonical = {}
    normalization_log = []

    for _, canonical_row in canonical.iterrows():
        metadata, log_rows = normalize_cited_metadata(
            canonical_row.to_dict()
        )
        normalized_canonical[metadata["id"]] = metadata
        missing = missing_required_fields(metadata)
        eligible_canonical[metadata["id"]] = not missing
        normalization_log.extend(log_rows)

        if missing:
            normalization_log.append(
                {
                    "id": metadata["id"],
                    "field": "required_fields",
                    "original_value": "",
                    "normalized_value": "",
                    "note": "missing:" + ",".join(missing),
                }
            )

    citation_rows = []
    exclusions = []

    for _, enriched_row in enriched.iterrows():
        row = enriched_row.to_dict()
        reference_key = row.get("reference_key", "")
        cited_id = mapping_lookup.get(reference_key, "")
        citing_id = clean_str(row.get("citing_id", ""))

        if not cited_id:
            exclusions.append(
                {
                    "reference_key": reference_key,
                    "citing_id": citing_id,
                    "cited_id": "",
                    "raw_reference": row.get("raw_reference", ""),
                    "reason": "missing_deduplication_mapping",
                }
            )
            continue

        if not eligible_canonical.get(cited_id, False):
            metadata = normalized_canonical.get(cited_id, {})
            missing = missing_required_fields(metadata) if metadata else []
            exclusions.append(
                {
                    "reference_key": reference_key,
                    "citing_id": citing_id,
                    "cited_id": cited_id,
                    "raw_reference": row.get("raw_reference", ""),
                    "reason": (
                        "incomplete_temp_metadata:"
                        + ",".join(missing)
                    ),
                }
            )
            continue

        citation_rows.append(
            {"citing_id": citing_id, "cited_id": cited_id}
        )

    seen_pairs = set()
    citation_unique = []
    for row in citation_rows:
        pair = (row["citing_id"], row["cited_id"])
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            citation_unique.append(row)

    if not citation_unique:
        write_csv(
            exclusions,
            OAI_PIPELINE_DIR / "oc_export_exclusions.csv",
            [
                "reference_key",
                "citing_id",
                "cited_id",
                "raw_reference",
                "reason",
            ],
        )
        raise RuntimeError(
            "No citation survived OpenCitations normalization. Inspect "
            f"{OAI_PIPELINE_DIR / 'oc_export_exclusions.csv'}."
        )

    participating_ids = {
        identifier
        for row in citation_unique
        for identifier in [row["citing_id"], row["cited_id"]]
    }
    citing_ids = {row["citing_id"] for row in citation_unique}
    citing_lookup = {}
    for _, job_row in jobs.iterrows():
        metadata = build_citing_metadata_from_job(job_row.to_dict())
        if metadata["id"] in citing_ids:
            missing = missing_required_fields(metadata)
            if missing:
                raise ValueError(
                    f"Citing record {metadata['id']} is missing fields "
                    f"required by META-CSV: {missing}"
                )
        citing_lookup[metadata["id"]] = metadata

    metadata_rows = []
    for identifier in sorted(participating_ids):
        if identifier in citing_lookup:
            metadata_rows.append(citing_lookup[identifier])
        elif identifier in normalized_canonical:
            metadata_rows.append(normalized_canonical[identifier])
        else:
            raise KeyError(
                f"No metadata record was built for citation ID: {identifier}"
            )

    seen_ids = set()
    metadata_unique = []
    for row in metadata_rows:
        if row["id"] not in seen_ids:
            seen_ids.add(row["id"])
            metadata_unique.append(row)

    citation_ids = {
        identifier
        for row in citation_unique
        for identifier in [row["citing_id"], row["cited_id"]]
    }
    metadata_ids = {row["id"] for row in metadata_unique}
    if citation_ids != metadata_ids:
        raise AssertionError(
            "OpenCitations export is not closed: "
            f"citation-only IDs={sorted(citation_ids - metadata_ids)}, "
            f"metadata-only IDs={sorted(metadata_ids - citation_ids)}"
        )

    write_csv(
        metadata_unique,
        OAI_PIPELINE_DIR / "example_metadata.csv",
        OC_METADATA_FIELDS,
    )
    write_csv(
        citation_unique,
        OAI_PIPELINE_DIR / "example_citations.csv",
        OC_CITATION_FIELDS,
    )
    write_csv(
        exclusions,
        OAI_PIPELINE_DIR / "oc_export_exclusions.csv",
        [
            "reference_key",
            "citing_id",
            "cited_id",
            "raw_reference",
            "reason",
        ],
    )
    write_csv(
        normalization_log,
        OAI_PIPELINE_DIR / "oc_normalization_log.csv",
        [
            "id",
            "field",
            "original_value",
            "normalized_value",
            "note",
        ],
    )

    summary = {
        "metadata_rows": len(metadata_unique),
        "citation_rows": len(citation_unique),
        "excluded_reference_instances": len(exclusions),
        "normalization_events": len(normalization_log),
        "closure_precheck_passed": citation_ids == metadata_ids,
    }
    write_json(summary, OAI_PIPELINE_DIR / "oc_export_summary.json")

    return (
        pd.DataFrame(metadata_unique),
        pd.DataFrame(citation_unique),
        pd.DataFrame(exclusions),
    )


# %%
# =============================================================================
# OPEN CITATIONS VALIDATION
# =============================================================================

def _validator_prefix() -> list[str]:
    if OC_VALIDATOR_CMD:
        return shlex.split(OC_VALIDATOR_CMD, posix=os.name != "nt")

    executable_candidates = [
        PROJECT_ROOT / "oc_validator_env" / "Scripts" / "oc_validator.exe",
        PROJECT_ROOT.parent / "oc_validator_env" / "Scripts" / "oc_validator.exe",
        PROJECT_ROOT / "oc_validator_env" / "bin" / "oc_validator",
        PROJECT_ROOT.parent / "oc_validator_env" / "bin" / "oc_validator",
    ]
    for candidate in executable_candidates:
        if candidate.exists():
            return [str(candidate)]

    python_candidates = []
    if OC_VALIDATOR_PYTHON:
        python_candidates.append(Path(OC_VALIDATOR_PYTHON))
    python_candidates.extend(
        [
            PROJECT_ROOT
            / "oc_validator_env"
            / "Scripts"
            / "python.exe",
            PROJECT_ROOT.parent
            / "oc_validator_env"
            / "Scripts"
            / "python.exe",
            PROJECT_ROOT / "oc_validator_env" / "bin" / "python",
            PROJECT_ROOT.parent
            / "oc_validator_env"
            / "bin"
            / "python",
        ]
    )

    for candidate in python_candidates:
        if candidate.exists():
            return [
                str(candidate),
                "-c",
                "from oc_validator.cli import main; main()",
            ]

    raise FileNotFoundError(
        "Could not find oc_validator. Set OC_VALIDATOR_CMD to its "
        "executable or OC_VALIDATOR_PYTHON to the Python executable in the "
        "validator virtual environment."
    )


def _count_validation_issues(path: Path) -> dict:
    counts = {"error": 0, "warning": 0, "other": 0}
    if not path.exists() or path.stat().st_size == 0:
        return counts

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        issue = json.loads(line)
        issue_type = issue.get("error_type", "other")
        counts[issue_type if issue_type in counts else "other"] += 1
    return counts


def run_oc_validator(
    metadata_path: Path,
    citations_path: Path,
) -> dict:
    validation_dir = OAI_PIPELINE_DIR / "validation"
    metadata_out = validation_dir / "metadata"
    citations_out = validation_dir / "citations"
    metadata_out.mkdir(parents=True, exist_ok=True)
    citations_out.mkdir(parents=True, exist_ok=True)

    for generated_file in [
        metadata_out / "out_validate_meta.jsonl",
        metadata_out / "meta_validation_summary.txt",
        citations_out / "out_validate_cits.jsonl",
        citations_out / "cits_validation_summary.txt",
    ]:
        generated_file.unlink(missing_ok=True)

    prefix = _validator_prefix()
    help_result = subprocess.run(
        prefix + ["--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    help_text = help_result.stdout + help_result.stderr
    if "closure" not in help_text:
        raise RuntimeError(
            "The installed oc_validator has no closure subcommand. "
            "Upgrade it to the current release (at least 0.3.3)."
        )

    command = prefix + [
        "closure",
        "--meta",
        str(metadata_path),
        "--meta-out",
        str(metadata_out),
        "--cits",
        str(citations_path),
        "--cits-out",
        str(citations_out),
    ]
    if VALIDATOR_SKIP_ID_EXISTENCE:
        command.append("-s")

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    metadata_issues = _count_validation_issues(
        metadata_out / "out_validate_meta.jsonl"
    )
    citation_issues = _count_validation_issues(
        citations_out / "out_validate_cits.jsonl"
    )
    report = {
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "metadata_issues": metadata_issues,
        "citation_issues": citation_issues,
        "skip_id_existence": VALIDATOR_SKIP_ID_EXISTENCE,
    }
    write_json(report, validation_dir / "oc_validator_report.json")

    total_errors = (
        metadata_issues["error"] + citation_issues["error"]
    )
    if result.returncode != 0:
        raise RuntimeError(
            "oc_validator failed technically. Inspect "
            f"{validation_dir / 'oc_validator_report.json'}."
        )
    if total_errors:
        raise RuntimeError(
            f"oc_validator found {total_errors} data errors. Inspect "
            f"{validation_dir}."
        )

    return report


# %%
# =============================================================================
# PIPELINE STAGES AND RUN MANIFEST
# =============================================================================

def base_run_manifest(stage: str) -> dict:
    manifest = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "python": sys.version,
        "inputs": {
            "xlsx": str(XLSX_PATH),
            "pickle": str(PICKLE_PATH),
            "xlsx_sha256": (
                sha256_file(XLSX_PATH) if XLSX_PATH.exists() else ""
            ),
            "pickle_sha256": (
                sha256_file(PICKLE_PATH) if PICKLE_PATH.exists() else ""
            ),
        },
        "pdf_pilot": {
            "language": LANG_MODE,
            "sample_size": PDF_SAMPLE_SIZE,
            "random_state": PDF_SAMPLE_RANDOM_STATE,
            "year_periods": PDF_YEAR_PERIODS,
            "reference_bands": PDF_REFERENCE_BANDS,
            "cec_base_url": CEC_BASE_URL,
            "cec_consolidate": CEC_CONSOLIDATE,
            "cec_max_workers": CEC_MAX_WORKERS,
            "expected_cec_extractor_image": (
                EXPECTED_CEC_EXTRACTOR_IMAGE
            ),
            "expected_cec_grobid_image": EXPECTED_CEC_GROBID_IMAGE,
        },
        "crossref": {
            "rows": CROSSREF_ROWS,
            "max_workers": CROSSREF_MAX_WORKERS,
            "mailto_configured": (
                CROSSREF_MAILTO != "your.email@example.org"
            ),
        },
        "deduplication_threshold": DEDUPLICATION_THRESHOLD,
        "counts": {},
    }
    return manifest


def run_pdf_stage(
    jobs: pd.DataFrame,
    sample_size: int,
    refresh_cec: bool,
) -> dict:
    check_cec_connection()
    summary, allocation = run_pdf_extraction_evaluation(
        jobs,
        sample_size=sample_size,
        random_state=PDF_SAMPLE_RANDOM_STATE,
        refresh_cec=refresh_cec,
    )
    return {
        "pdf_sample_articles": len(summary),
        "pdf_successes": int(summary["grobid_success"].astype(bool).sum()),
        "pdf_failures": int((~summary["grobid_success"].astype(bool)).sum()),
        "pdf_extracted_references": int(
            summary["pdf_reference_count"].sum()
        ),
        "pdf_gold_references": int(
            summary["gold_reference_count"].sum()
        ),
        "pdf_sampling_strata": len(allocation),
    }


def run_oai_stage(
    jobs: pd.DataFrame,
    skip_crossref: bool,
    run_validation: bool,
) -> dict:
    parsed, parse_errors = parse_all_oai_references(jobs)
    if parsed.empty:
        raise RuntimeError(
            "AnyStyle produced no parsed OAI-PMH references. Inspect "
            f"{OAI_PIPELINE_DIR / 'oai_references_parsing_errors.csv'}."
        )

    if skip_crossref:
        enriched, crossref_diagnostics = skip_crossref_enrichment(
            parsed
        )
    else:
        enriched, crossref_diagnostics = enrich_with_crossref(parsed)

    canonical, mappings = deduplicate_records(enriched)
    metadata, citations, exclusions = build_oc_export(
        jobs=jobs,
        enriched=enriched,
        canonical=canonical,
        mappings=mappings,
    )

    validation_report = None
    if run_validation:
        validation_report = run_oc_validator(
            metadata_path=OAI_PIPELINE_DIR / "example_metadata.csv",
            citations_path=OAI_PIPELINE_DIR / "example_citations.csv",
        )

    return {
        "jobs": len(jobs),
        "parsed_oai_references": len(parsed),
        "anystyle_error_rows": len(parse_errors),
        "crossref_diagnostic_candidates": len(
            crossref_diagnostics
        ),
        "canonical_cited_records": len(canonical),
        "oc_metadata_rows": len(metadata),
        "oc_citation_rows": len(citations),
        "oc_excluded_reference_instances": len(exclusions),
        "validation_completed": validation_report is not None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=["pdf", "oai", "validate", "all"],
        default="all",
        help=(
            "pdf: only CEC/GROBID pilot; oai: OAI-PMH to OpenCitations; "
            "validate: validate existing exports; all: pdf and oai"
        ),
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=PDF_SAMPLE_SIZE,
        help=f"PDF pilot size (default: {PDF_SAMPLE_SIZE})",
    )
    parser.add_argument(
        "--refresh-cec",
        action="store_true",
        help="Ignore cached TEI and call CEC again.",
    )
    parser.add_argument(
        "--skip-crossref",
        action="store_true",
        help="Skip Crossref DOI enrichment.",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Build OpenCitations CSV files without running oc_validator.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_output_dirs()
    manifest = base_run_manifest(args.stage)
    manifest["pdf_pilot"]["sample_size"] = args.sample_size

    try:
        if args.stage == "validate":
            report = run_oc_validator(
                metadata_path=OAI_PIPELINE_DIR / "example_metadata.csv",
                citations_path=OAI_PIPELINE_DIR / "example_citations.csv",
            )
            manifest["counts"]["validation_completed"] = True
            manifest["validation"] = report
        else:
            jobs = load_jobs()
            if args.stage in {"pdf", "all"}:
                manifest["counts"].update(
                    run_pdf_stage(
                        jobs,
                        sample_size=args.sample_size,
                        refresh_cec=args.refresh_cec,
                    )
                )
            if args.stage in {"oai", "all"}:
                manifest["counts"].update(
                    run_oai_stage(
                        jobs,
                        skip_crossref=args.skip_crossref,
                        run_validation=not args.no_validate,
                    )
                )

        manifest["status"] = "completed"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        manifest["finished_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()
        write_json(manifest, OUTPUT_DIR / "run_manifest.json")

    LOGGER.info("Pipeline completed.")
    LOGGER.info("Output directory: %s", OUTPUT_DIR)
    LOGGER.info("Counts: %s", manifest["counts"])


if __name__ == "__main__":
    main()
