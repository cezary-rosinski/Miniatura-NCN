import csv
import hashlib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pickle
import requests


#%%
# =========================================================
# CONFIG
# =========================================================

# Input files prepared in your previous harvesting step
XLSX_PATH = Path(r"data/forum_poetyki_harvesting_info.xlsx")
PICKLE_PATH = Path(r"data/Forum_Poetyki_harvested.pkl")

# Base directory for local PDFs.
# In your original manifest the paths are relative, e.g.:
# data\forum_poetyki_pdfs\pl\20652_2019_17_pl.pdf
#
# Set this to the project root that contains the "data" folder.
PDF_BASE_DIR = Path(r"C:\Users\pracownik\Documents\Miniatura-NCN")

# Output directory
OUTPUT_DIR = Path(r"data/forum_poetyki_pdf_grobid_pipeline_output")

# GROBID endpoint
GROBID_FULLTEXT_URL = "http://localhost:8070/api/processFulltextDocument"

# Which PDFs to process:
# "pl" -> only Polish PDFs
# "en" -> only English PDFs
# "both" -> both, one row per available PDF version
LANG_MODE = "en"

# Whether to keep only rows with status == "ok"
ONLY_OK_PDFS = True

# GROBID parameters
GROBID_TIMEOUT = 300
MAX_RETRIES_503 = 5
RETRY_SLEEP_SECONDS = 5

#%%
# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

NS = {"tei": "http://www.tei-c.org/ns/1.0"}


# =========================================================
# BASIC HELPERS
# =========================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_ws(text: Optional[str]) -> str:
    if text is None:
        return ""
    return " ".join(str(text).split()).strip()


def safe_text(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    return normalize_ws("".join(el.itertext()))


def first_text(parent: ET.Element, xpath: str) -> str:
    el = parent.find(xpath, NS)
    return safe_text(el)


def coerce_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def extract_year(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(1[6-9]\d{2}|20\d{2}|21\d{2})", text)
    return m.group(1) if m else ""


def md5_short(text: str, n: int = 16) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:n]


def normalize_orcid(orcid: str) -> str:
    orcid = normalize_ws(orcid)
    if not orcid:
        return ""
    return orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "").strip("/")


def build_temp_id(parts: List[str], prefix: str = "temp") -> str:
    key = " | ".join(normalize_ws(x) for x in parts if normalize_ws(x))
    return f"{prefix}:{md5_short(key)}"


def write_csv(rows: List[Dict], output_path: Path, fieldnames: List[str]) -> None:
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

#%%
# =========================================================
# LOAD AND MERGE FORUM POETYKI INPUTS
# =========================================================

def load_inputs(xlsx_path: Path, pickle_path: Path) -> Tuple[pd.DataFrame, List[Dict]]:
    df_manifest = pd.read_excel(xlsx_path)

    with open(pickle_path, "rb") as f:
        harvested = pickle.load(f)

    if not isinstance(harvested, list):
        raise TypeError("Expected pickle to contain a list of article dictionaries.")

    required_manifest_cols = {
        "identifier", "publisher_id", "doi", "article_title",
        "pl_status", "pl_path", "en_status", "en_path"
    }
    missing_manifest = required_manifest_cols - set(df_manifest.columns)
    if missing_manifest:
        raise ValueError(f"Missing expected columns in manifest: {sorted(missing_manifest)}")

    return df_manifest, harvested


def merge_manifest_and_pickle(df_manifest: pd.DataFrame, harvested: List[Dict]) -> pd.DataFrame:
    df_pickle = pd.DataFrame(harvested)

    # Normalize join keys
    for col in ["identifier", "doi", "article_title"]:
        df_manifest[col] = df_manifest[col].fillna("").astype(str).str.strip()
        df_pickle[col] = df_pickle[col].fillna("").astype(str).str.strip()

    df_manifest["publisher_id"] = df_manifest["publisher_id"].astype(str).str.strip()
    df_pickle["publisher_id"] = df_pickle["publisher_id"].fillna("").astype(str).str.strip()

    # Keep pickle columns distinct
    rename_map = {
        "journal_title": "pickle_journal_title",
        "year": "pickle_year",
        "issue": "pickle_issue",
        "fpage": "pickle_fpage",
        "lpage": "pickle_lpage",
        "abstract": "pickle_abstract",
        "keywords": "pickle_keywords",
        "authors": "pickle_authors",
        "pdf_urls": "pickle_pdf_urls",
        "article_urls": "pickle_article_urls",
        "references": "pickle_references",
    }
    df_pickle = df_pickle.rename(columns=rename_map)

    df = df_manifest.merge(
        df_pickle,
        on=["identifier", "publisher_id", "doi", "article_title"],
        how="left",
        validate="one_to_one"
    )

    return df


def build_pdf_jobs(df_merged: pd.DataFrame, lang_mode: str = "pl", only_ok_pdfs: bool = True) -> pd.DataFrame:
    jobs = []

    def add_job(row: pd.Series, lang: str):
        status_col = f"{lang}_status"
        path_col = f"{lang}_path"
        filename_col = f"{lang}_filename"
        view_url_col = f"{lang}_view_url"
        direct_pdf_col = f"{lang}_direct_pdf_url"

        status = coerce_str(row.get(status_col, ""))
        rel_path = coerce_str(row.get(path_col, ""))
        if not rel_path:
            return

        if only_ok_pdfs and status.lower() != "ok":
            return

        jobs.append({
            "identifier": coerce_str(row["identifier"]),
            "publisher_id": coerce_str(row["publisher_id"]),
            "doi": coerce_str(row["doi"]),
            "article_title": coerce_str(row["article_title"]),
            "lang": lang,
            "pdf_status": status,
            "pdf_rel_path": rel_path,
            "pdf_filename": coerce_str(row.get(filename_col, "")),
            "view_url": coerce_str(row.get(view_url_col, "")),
            "direct_pdf_url": coerce_str(row.get(direct_pdf_col, "")),
            "journal_title": coerce_str(row.get("pickle_journal_title", "")),
            "year": coerce_str(row.get("pickle_year", "")),
            "issue": coerce_str(row.get("pickle_issue", "")),
            "fpage": coerce_str(row.get("pickle_fpage", "")),
            "lpage": coerce_str(row.get("pickle_lpage", "")),
            "abstract": row.get("pickle_abstract", ""),
            "keywords": row.get("pickle_keywords", []),
            "authors": row.get("pickle_authors", []),
            "references_gold": row.get("pickle_references", []),
            "article_urls": row.get("pickle_article_urls", []),
            "pdf_urls_pickle": row.get("pickle_pdf_urls", []),
        })

    for _, row in df_merged.iterrows():
        if lang_mode in ("pl", "both"):
            add_job(row, "pl")
        if lang_mode in ("en", "both"):
            add_job(row, "en")

    jobs_df = pd.DataFrame(jobs)
    return jobs_df


def resolve_pdf_path(pdf_base_dir: Path, rel_path: str) -> Path:
    # Manifest uses Windows-style relative paths: data\forum_poetyki_pdfs\pl\...
    rel_norm = rel_path.replace("\\", "/")
    return pdf_base_dir / Path(rel_norm)

#%%
# =========================================================
# GROBID
# =========================================================

def grobid_fulltext_to_tei(pdf_path: Path) -> str:
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    for attempt in range(1, MAX_RETRIES_503 + 1):
        with open(pdf_path, "rb") as f:
            files = {"input": (pdf_path.name, f, "application/pdf")}
            data = {
                "consolidateHeader": "0",
                "consolidateCitations": "0",
                "includeRawCitations": "1",
                "includeRawAffiliations": "0",
                "teiCoordinates": "",
            }
            response = requests.post(
                GROBID_FULLTEXT_URL,
                files=files,
                data=data,
                timeout=GROBID_TIMEOUT
            )

        if response.status_code == 503:
            logging.warning("GROBID returned 503 for %s (attempt %s/%s). Retrying...", pdf_path.name, attempt, MAX_RETRIES_503)
            time.sleep(RETRY_SLEEP_SECONDS)
            continue

        response.raise_for_status()

        tei = response.text.strip()
        if not tei:
            raise ValueError(f"Empty TEI returned by GROBID for {pdf_path}")

        return tei

    raise RuntimeError(f"GROBID repeatedly returned 503 for {pdf_path}")


# =========================================================
# TEI PARSING - HEADER
# =========================================================

def parse_header_metadata_from_tei(tei_xml: str) -> Dict:
    root = ET.fromstring(tei_xml.encode("utf-8"))

    title = first_text(root, ".//tei:titleStmt/tei:title")
    doi = ""
    for idno in root.findall(".//tei:fileDesc/tei:sourceDesc//tei:idno", NS):
        if idno.attrib.get("type", "").lower() == "doi":
            doi = safe_text(idno)
            if doi:
                break

    authors = []
    for author in root.findall(".//tei:titleStmt/tei:author", NS):
        surname = first_text(author, "./tei:persName/tei:surname")
        forename = first_text(author, "./tei:persName/tei:forename")
        name = ", ".join([x for x in [surname, forename] if x]).strip(", ")
        if not name:
            name = safe_text(author)
        if name:
            authors.append(name)

    pub_date = ""
    date_el = root.find(".//tei:publicationStmt/tei:date", NS)
    if date_el is not None:
        pub_date = date_el.attrib.get("when", "") or safe_text(date_el)

    return {
        "tei_title": title,
        "tei_doi": doi.lower(),
        "tei_author": "; ".join(authors),
        "tei_pub_date": pub_date,
    }


# =========================================================
# TEI PARSING - REFERENCES
# =========================================================

def guess_type_from_tei(bibl: ET.Element) -> str:
    if bibl.find("./tei:analytic", NS) is not None and bibl.find("./tei:monogr/tei:title[@level='j']", NS) is not None:
        return "journal article"
    if bibl.find("./tei:analytic", NS) is not None and bibl.find("./tei:monogr", NS) is not None:
        return "book chapter"
    if bibl.find("./tei:monogr", NS) is not None:
        return "book"
    return "other"


def extract_editors(bibl: ET.Element) -> str:
    editors = []
    for editor in bibl.findall(".//tei:editor", NS):
        surname = first_text(editor, "./tei:persName/tei:surname")
        forename = first_text(editor, "./tei:persName/tei:forename")
        name = ", ".join([x for x in [surname, forename] if x]).strip(", ")
        if not name:
            name = safe_text(editor)
        if name:
            editors.append(name)
    return "; ".join(editors)


def extract_publisher(bibl: ET.Element) -> str:
    return first_text(bibl, ".//tei:imprint/tei:publisher")


def parse_tei_references(tei_xml: str) -> List[Dict]:
    root = ET.fromstring(tei_xml.encode("utf-8"))
    refs = []

    for idx, bibl in enumerate(root.findall(".//tei:listBibl/tei:biblStruct", NS), start=1):
        analytic_title = first_text(bibl, "./tei:analytic/tei:title")
        journal_title = first_text(bibl, "./tei:monogr/tei:title[@level='j']")
        monogr_title = first_text(bibl, "./tei:monogr/tei:title")
        venue = journal_title or monogr_title

        authors = []
        for author in bibl.findall("./tei:analytic/tei:author", NS):
            surname = first_text(author, "./tei:persName/tei:surname")
            forename = first_text(author, "./tei:persName/tei:forename")
            name = ", ".join([x for x in [surname, forename] if x]).strip(", ")
            if not name:
                name = safe_text(author)
            if name:
                authors.append(name)

        if not authors:
            for author in bibl.findall("./tei:monogr/tei:author", NS):
                surname = first_text(author, "./tei:persName/tei:surname")
                forename = first_text(author, "./tei:persName/tei:forename")
                name = ", ".join([x for x in [surname, forename] if x]).strip(", ")
                if not name:
                    name = safe_text(author)
                if name:
                    authors.append(name)

        date_when = ""
        date_el = bibl.find(".//tei:imprint/tei:date", NS)
        if date_el is not None:
            date_when = date_el.attrib.get("when", "") or safe_text(date_el)

        volume = ""
        issue = ""
        page = ""

        for scope in bibl.findall(".//tei:biblScope", NS):
            unit = scope.attrib.get("unit", "")
            if unit == "volume":
                volume = safe_text(scope)
            elif unit == "issue":
                issue = safe_text(scope)
            elif unit == "page":
                page_from = scope.attrib.get("from", "")
                page_to = scope.attrib.get("to", "")
                if page_from and page_to:
                    page = f"{page_from}-{page_to}"
                elif page_from:
                    page = page_from
                else:
                    page = safe_text(scope)

        doi = ""
        for idno in bibl.findall(".//tei:idno", NS):
            if idno.attrib.get("type", "").lower() == "doi":
                doi = safe_text(idno).lower()
                if doi:
                    break

        raw_ref = ""
        note = bibl.find(".//tei:note[@type='raw_reference']", NS)
        if note is not None:
            raw_ref = safe_text(note)

        title = analytic_title or monogr_title
        pub_date = normalize_ws(date_when) or extract_year(raw_ref)

        ref = {
            "id": f"doi:{doi}" if doi else "",
            "title": normalize_ws(title),
            "author": "; ".join(a for a in authors if a),
            "pub_date": pub_date,
            "venue": normalize_ws(venue),
            "volume": normalize_ws(volume),
            "issue": normalize_ws(issue),
            "page": normalize_ws(page),
            "type": guess_type_from_tei(bibl),
            "publisher": extract_publisher(bibl),
            "editor": extract_editors(bibl),
            "raw_reference": raw_ref,
            "tei_ref_position": idx,
        }

        if not ref["id"]:
            ref["id"] = build_temp_id(
                [
                    ref["title"], ref["author"], ref["pub_date"],
                    ref["venue"], ref["page"], raw_ref
                ],
                prefix="temp"
            )

        refs.append(ref)

    return refs

#%%
# =========================================================
# MAPPING TO OPENCITATIONS-LIKE TABLES
# =========================================================

def authors_to_oc_author(authors: List[Dict]) -> str:
    names = []
    for a in authors or []:
        if not isinstance(a, dict):
            continue
        full_name = normalize_ws(a.get("full_name", ""))
        if full_name:
            if a.get("orcid"):
                orcid = normalize_orcid(a["orcid"])
                names.append(f"{full_name} [orcid:{orcid}]")
            else:
                names.append(full_name)
    return "; ".join(names)


def build_citing_metadata_row(job: Dict, tei_header: Dict) -> Dict:
    doi = normalize_ws(job.get("doi", "")).lower()
    article_id = f"doi:{doi}" if doi else build_temp_id(
        [job.get("identifier", ""), job.get("publisher_id", ""), job.get("article_title", ""), job.get("lang", "")],
        prefix="temp"
    )

    page = ""
    fpage = normalize_ws(job.get("fpage", ""))
    lpage = normalize_ws(job.get("lpage", ""))
    if fpage and lpage:
        page = f"{fpage}-{lpage}"
    elif fpage:
        page = fpage

    venue = normalize_ws(job.get("journal_title", ""))
    issue = normalize_ws(job.get("issue", ""))
    if venue and issue:
        venue = f"{venue} [issue:{issue}]"

    row = {
        "id": article_id,
        "title": normalize_ws(job.get("article_title", "")),
        "author": authors_to_oc_author(job.get("authors", [])),
        "pub_date": normalize_ws(job.get("year", "")),
        "venue": venue,
        "volume": "",
        "issue": normalize_ws(job.get("issue", "")),
        "page": page,
        "type": "journal article",
        "publisher": "",
        "editor": "",
    }

    # Optional TEI diagnostics, kept outside final OC columns but useful in a diagnostics table
    row["_tei_title"] = tei_header.get("tei_title", "")
    row["_tei_doi"] = tei_header.get("tei_doi", "")
    row["_tei_author"] = tei_header.get("tei_author", "")
    row["_tei_pub_date"] = tei_header.get("tei_pub_date", "")
    return row


def deduplicate_metadata_rows(rows: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for row in rows:
        rid = row.get("id", "")
        if rid and rid not in seen:
            out.append(row)
            seen.add(rid)
    return out


def build_oc_rows_for_job(job: Dict, refs: List[Dict], tei_header: Dict) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    citing = build_citing_metadata_row(job, tei_header)

    metadata_rows = [citing]
    citations_rows = []
    diagnostics_rows = []

    for ref in refs:
        metadata_rows.append({
            "id": ref["id"],
            "title": ref["title"],
            "author": ref["author"],
            "pub_date": ref["pub_date"],
            "venue": ref["venue"],
            "volume": ref["volume"],
            "issue": ref["issue"],
            "page": ref["page"],
            "type": ref["type"],
            "publisher": ref["publisher"],
            "editor": ref["editor"],
        })

        citations_rows.append({
            "citing_id": citing["id"],
            "citing_publication_date": citing["pub_date"],
            "cited_id": ref["id"],
            "cited_publication_date": ref["pub_date"],
        })

        diagnostics_rows.append({
            "identifier": job["identifier"],
            "publisher_id": job["publisher_id"],
            "doi": job["doi"],
            "lang": job["lang"],
            "pdf_filename": job["pdf_filename"],
            "citing_title": job["article_title"],
            "tei_ref_position": ref["tei_ref_position"],
            "raw_reference": ref["raw_reference"],
            "parsed_id": ref["id"],
            "parsed_title": ref["title"],
            "parsed_author": ref["author"],
            "parsed_pub_date": ref["pub_date"],
            "parsed_venue": ref["venue"],
            "parsed_volume": ref["volume"],
            "parsed_issue": ref["issue"],
            "parsed_page": ref["page"],
            "parsed_type": ref["type"],
        })

    metadata_rows = deduplicate_metadata_rows(metadata_rows)
    return metadata_rows, citations_rows, diagnostics_rows

#%%
# =========================================================
# EVALUATION AGAINST PICKLE REFERENCES
# =========================================================

def build_gold_reference_rows(job: Dict) -> List[Dict]:
    rows = []
    for ref in job.get("references_gold", []) or []:
        if isinstance(ref, dict):
            rows.append({
                "identifier": job["identifier"],
                "publisher_id": job["publisher_id"],
                "doi": job["doi"],
                "lang": job["lang"],
                "pdf_filename": job["pdf_filename"],
                "gold_ref_id": normalize_ws(ref.get("ref_id", "")),
                "gold_citation": normalize_ws(ref.get("citation", "")),
            })
    return rows


def build_job_summary(job: Dict, refs: List[Dict], tei_header: Dict, pdf_path: Path, success: bool, error: str = "") -> Dict:
    gold_refs = job.get("references_gold", []) or []
    return {
        "identifier": job["identifier"],
        "publisher_id": job["publisher_id"],
        "doi": job["doi"],
        "article_title": job["article_title"],
        "lang": job["lang"],
        "pdf_filename": job["pdf_filename"],
        "pdf_rel_path": job["pdf_rel_path"],
        "pdf_abs_path": str(pdf_path),
        "pdf_exists": pdf_path.exists(),
        "grobid_success": success,
        "error": error,
        "gold_reference_count_pickle": len(gold_refs),
        "parsed_reference_count_grobid": len(refs),
        "difference_grobid_minus_pickle": len(refs) - len(gold_refs),
        "tei_title": tei_header.get("tei_title", ""),
        "tei_doi": tei_header.get("tei_doi", ""),
        "tei_author": tei_header.get("tei_author", ""),
        "tei_pub_date": tei_header.get("tei_pub_date", ""),
    }

#%%
# =========================================================
# MAIN PIPELINE
# =========================================================

def run_pipeline() -> None:
    ensure_dir(OUTPUT_DIR)
    tei_dir = OUTPUT_DIR / "tei"
    ensure_dir(tei_dir)

    df_manifest, harvested = load_inputs(XLSX_PATH, PICKLE_PATH)
    df_merged = merge_manifest_and_pickle(df_manifest, harvested)
    jobs_df = build_pdf_jobs(df_merged, lang_mode=LANG_MODE, only_ok_pdfs=ONLY_OK_PDFS)

    logging.info("Manifest rows: %s", len(df_manifest))
    logging.info("Harvested pickle records: %s", len(harvested))
    logging.info("PDF jobs to process: %s", len(jobs_df))

    all_metadata = []
    all_citations = []
    all_diagnostics = []
    all_gold = []
    all_summary = []

    for _, job_row in jobs_df.iterrows():
        # job_row = jobs_df.loc[0]
        job = job_row.to_dict()
        pdf_path = resolve_pdf_path(PDF_BASE_DIR, job["pdf_rel_path"])

        refs = []
        tei_header = {}
        success = False
        error = ""

        try:
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF not found: {pdf_path}")

            logging.info("Processing %s | %s | %s", job["lang"], job["publisher_id"], pdf_path.name)

            tei_xml = grobid_fulltext_to_tei(pdf_path)
            tei_out = tei_dir / f"{job['publisher_id']}_{job['lang']}.tei.xml"
            tei_out.write_text(tei_xml, encoding="utf-8")

            tei_header = parse_header_metadata_from_tei(tei_xml)
            refs = parse_tei_references(tei_xml)

            metadata_rows, citations_rows, diagnostics_rows = build_oc_rows_for_job(job, refs, tei_header)

            all_metadata.extend(metadata_rows)
            all_citations.extend(citations_rows)
            all_diagnostics.extend(diagnostics_rows)
            all_gold.extend(build_gold_reference_rows(job))

            success = True

        except Exception as e:
            error = str(e)
            logging.exception("Failed on %s | %s", job.get("publisher_id", ""), job.get("pdf_filename", ""))

        finally:
            all_summary.append(build_job_summary(job, refs, tei_header, pdf_path, success, error))

    all_metadata = deduplicate_metadata_rows(all_metadata)

    metadata_fields = [
        "id", "title", "author", "pub_date", "venue",
        "volume", "issue", "page", "type", "publisher", "editor"
    ]
    citations_fields = [
        "citing_id", "citing_publication_date", "cited_id", "cited_publication_date"
    ]

    diagnostics_fields = [
        "identifier", "publisher_id", "doi", "lang", "pdf_filename", "citing_title",
        "tei_ref_position", "raw_reference", "parsed_id", "parsed_title",
        "parsed_author", "parsed_pub_date", "parsed_venue", "parsed_volume",
        "parsed_issue", "parsed_page", "parsed_type"
    ]

    gold_fields = [
        "identifier", "publisher_id", "doi", "lang", "pdf_filename",
        "gold_ref_id", "gold_citation"
    ]

    summary_fields = [
        "identifier", "publisher_id", "doi", "article_title", "lang",
        "pdf_filename", "pdf_rel_path", "pdf_abs_path", "pdf_exists",
        "grobid_success", "error",
        "gold_reference_count_pickle", "parsed_reference_count_grobid",
        "difference_grobid_minus_pickle",
        "tei_title", "tei_doi", "tei_author", "tei_pub_date"
    ]

    write_csv(all_metadata, OUTPUT_DIR / "example_metadata.csv", metadata_fields)
    write_csv(all_citations, OUTPUT_DIR / "example_citations.csv", citations_fields)
    write_csv(all_diagnostics, OUTPUT_DIR / "parsed_references_diagnostics.csv", diagnostics_fields)
    write_csv(all_gold, OUTPUT_DIR / "pickle_gold_references.csv", gold_fields)
    write_csv(all_summary, OUTPUT_DIR / "job_summary.csv", summary_fields)

    config_dump = {
        "XLSX_PATH": str(XLSX_PATH),
        "PICKLE_PATH": str(PICKLE_PATH),
        "PDF_BASE_DIR": str(PDF_BASE_DIR),
        "OUTPUT_DIR": str(OUTPUT_DIR),
        "GROBID_FULLTEXT_URL": GROBID_FULLTEXT_URL,
        "LANG_MODE": LANG_MODE,
        "ONLY_OK_PDFS": ONLY_OK_PDFS,
    }
    (OUTPUT_DIR / "run_config.json").write_text(json.dumps(config_dump, ensure_ascii=False, indent=2), encoding="utf-8")

    logging.info("Done.")
    logging.info("Metadata rows: %s", len(all_metadata))
    logging.info("Citation rows: %s", len(all_citations))
    logging.info("Diagnostics rows: %s", len(all_diagnostics))
    logging.info("Gold reference rows: %s", len(all_gold))
    logging.info("Summary rows: %s", len(all_summary))
    logging.info("Output dir: %s", OUTPUT_DIR)


if __name__ == "__main__":
    run_pipeline()
