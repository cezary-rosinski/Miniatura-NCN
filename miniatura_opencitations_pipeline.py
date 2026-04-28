import csv
import json
import hashlib
import time
import re
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from tqdm import tqdm
from Levenshtein import distance as lev

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import json
import subprocess
import tempfile
from pathlib import Path

import oc_ds_converter

# IMPORTY Z TWOICH PLIKÓW
from miniatura_parsing_from_pdf import (
    load_inputs,
    merge_manifest_and_pickle,
    build_pdf_jobs,
    resolve_pdf_path,
    grobid_fulltext_to_tei,
    parse_tei_references,
)

from miniatura_parsing_anystyle import (
    parse_with_anystyle_single_reference,
    map_anystyle_to_oc,
    normalize_text,
)

#%%
# =========================================================
# CONFIG
# =========================================================

OC_VALIDATOR_PYTHON = r"C:\Users\pracownik\Documents\Miniatura-NCN\oc_validator_env\Scripts\python.exe"

ANYSTYLE_CMD = r"C:\Ruby34-x64\bin\anystyle.bat"

XLSX_PATH = Path(r"data/forum_poetyki_harvesting_info.xlsx")
PICKLE_PATH = Path(r"data/Forum_Poetyki_harvested.pkl")

PDF_BASE_DIR = Path(r"C:\Users\pracownik\Documents\Miniatura-NCN")

OUTPUT_DIR = Path(r"data/final_pipeline_output")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

PDF_EVAL_DIR = OUTPUT_DIR / "01_pdf_extraction_evaluation"
OAI_PIPELINE_DIR = OUTPUT_DIR / "02_oai_to_opencitations"
CACHE_DIR = OUTPUT_DIR / "cache"

PDF_EVAL_DIR.mkdir(exist_ok=True, parents=True)
OAI_PIPELINE_DIR.mkdir(exist_ok=True, parents=True)
CACHE_DIR.mkdir(exist_ok=True, parents=True)

LANG_MODE = "en"

# ustaw np. 20, 50, 100 albo None
PDF_SAMPLE_SIZE = 20

# Crossref
CROSSREF_MAILTO = "your.email@example.org"  # <- zmień
CROSSREF_USER_AGENT = (
    f"MiniaturaOpenCitationsPipeline/0.1 "
    f"(mailto:{CROSSREF_MAILTO})"
)
CROSSREF_ROWS = 3
CROSSREF_SLEEP = 0.0
CROSSREF_CACHE_PATH = CACHE_DIR / "crossref_cache.json"

DOI_ACCEPT_TITLE_THRESHOLD = 0.85
DOI_ACCEPT_LOOSE_THRESHOLD = 0.78

CROSSREF_MAX_WORKERS = 3  # bezpiecznie; możesz testowo podnieść do 5
cache_lock = Lock()

#%%
# =========================================================
# BASIC HELPERS
# =========================================================

def clean_str(x):
    return normalize_text(x)


def md5_short(text, n=16):
    return hashlib.md5(str(text).encode("utf-8")).hexdigest()[:n]


def stable_temp_id(*parts, prefix="temp"):
    key = " | ".join(clean_str(p) for p in parts if clean_str(p))
    if not key:
        key = "empty"
    return f"{prefix}:{md5_short(key)}"


def write_csv(rows, path, fieldnames=None):
    rows = list(rows)

    if fieldnames is None:
        keys = []
        seen = set()
        for row in rows:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
        fieldnames = keys

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def load_json_cache(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_json_cache(cache, path):
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_doi(doi):
    doi = clean_str(doi).lower()
    doi = doi.replace("https://doi.org/", "")
    doi = doi.replace("http://doi.org/", "")
    doi = doi.replace("doi:", "")
    return doi.strip()


def id_from_doi_or_temp(doi, *parts):
    doi = normalize_doi(doi)
    if doi:
        return f"doi:{doi}"
    return stable_temp_id(*parts)


def extract_year(text):
    text = clean_str(text)
    m = re.search(r"(1[6-9]\d{2}|20\d{2}|21\d{2})", text)
    return m.group(1) if m else ""


# =========================================================
# LOAD INPUTS
# =========================================================

df_manifest, harvested = load_inputs(XLSX_PATH, PICKLE_PATH)
df_merged = merge_manifest_and_pickle(df_manifest, harvested)
jobs_df = build_pdf_jobs(df_merged, lang_mode=LANG_MODE)


# =========================================================
# OAI-PMH GOLD REFERENCES
# =========================================================

def extract_gold_refs(job):
    refs = job.get("references_gold", [])
    out = []

    for idx, r in enumerate(refs or [], start=1):
        if isinstance(r, dict):
            citation = clean_str(r.get("citation", ""))
            if citation:
                out.append({
                    "reference_index": idx,
                    "raw_reference": citation,
                    "gold_ref_id": clean_str(r.get("ref_id", "")),
                })
        else:
            citation = clean_str(r)
            if citation:
                out.append({
                    "reference_index": idx,
                    "raw_reference": citation,
                    "gold_ref_id": "",
                })

    return out


# =========================================================
# PDF EXTRACTION + EVALUATION LOOP
# =========================================================

def extract_pdf_refs(job):
    pdf_path = resolve_pdf_path(PDF_BASE_DIR, job["pdf_rel_path"])

    try:
        tei = grobid_fulltext_to_tei(pdf_path)
        refs = parse_tei_references(tei)

        return {
            "success": True,
            "error": "",
            "pdf_path": str(pdf_path),
            "refs": [
                {
                    "reference_index": r.get("tei_ref_position", i),
                    "raw_reference": clean_str(r.get("raw_reference", "")),
                    "parsed_title": clean_str(r.get("title", "")),
                    "parsed_author": clean_str(r.get("author", "")),
                    "parsed_pub_date": clean_str(r.get("pub_date", "")),
                    "parsed_venue": clean_str(r.get("venue", "")),
                }
                for i, r in enumerate(refs, start=1)
                if clean_str(r.get("raw_reference", ""))
            ],
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "pdf_path": str(pdf_path),
            "refs": [],
        }


def compare_pdf_to_gold(pdf_refs, gold_refs):
    return {
        "pdf_reference_count": len(pdf_refs),
        "gold_reference_count": len(gold_refs),
        "difference_pdf_minus_gold": len(pdf_refs) - len(gold_refs),
        "pdf_gold_ratio": len(pdf_refs) / len(gold_refs) if gold_refs else "",
    }


def run_pdf_extraction_evaluation(jobs_df, sample_size=PDF_SAMPLE_SIZE):
    if sample_size is not None:
        jobs_eval = jobs_df.head(sample_size).copy()
    else:
        jobs_eval = jobs_df.copy()

    summary_rows = []
    pdf_reference_rows = []
    gold_reference_rows = []

    for _, job_row in tqdm(jobs_eval.iterrows(), total=len(jobs_eval)):
        job = job_row.to_dict()

        pdf_result = extract_pdf_refs(job)
        gold_refs = extract_gold_refs(job)

        stats = compare_pdf_to_gold(pdf_result["refs"], gold_refs)

        base = {
            "identifier": job.get("identifier", ""),
            "publisher_id": job.get("publisher_id", ""),
            "doi": job.get("doi", ""),
            "article_title": job.get("article_title", ""),
            "lang": job.get("lang", ""),
            "pdf_filename": job.get("pdf_filename", ""),
            "pdf_path": pdf_result["pdf_path"],
            "grobid_success": pdf_result["success"],
            "error": pdf_result["error"],
        }

        summary_rows.append({**base, **stats})

        for r in pdf_result["refs"]:
            pdf_reference_rows.append({**base, **r})

        for r in gold_refs:
            gold_reference_rows.append({**base, **r})

    write_csv(summary_rows, PDF_EVAL_DIR / "pdf_vs_oai_summary.csv")
    write_csv(pdf_reference_rows, PDF_EVAL_DIR / "pdf_extracted_references.csv")
    write_csv(gold_reference_rows, PDF_EVAL_DIR / "oai_gold_references_for_pdf_sample.csv")

    return pd.DataFrame(summary_rows)


# =========================================================
# ANYSTYLE PARSING OF OAI-PMH REFERENCES
# =========================================================

def parse_gold_reference(raw_reference):
    raw_reference = clean_str(raw_reference)

    if not raw_reference:
        return {}

    parsed = parse_with_anystyle_single_reference(raw_reference)
    mapped = map_anystyle_to_oc(parsed)

    # AnyStyle map zwykle nie daje DOI, więc zostawiamy pole
    mapped.setdefault("doi", "")
    mapped["raw_reference"] = raw_reference
    mapped["parsed_json"] = json.dumps(parsed, ensure_ascii=False)

    return mapped

def parse_with_anystyle_batch(refs, anystyle_cmd=ANYSTYLE_CMD):
    refs = [normalize_text(r) for r in refs if normalize_text(r)]

    if not refs:
        return []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        input_path = tmpdir / "references.txt"

        # Każdy przypis w osobnej linii
        input_path.write_text("\n".join(refs), encoding="utf-8")

        result = subprocess.run(
            [anystyle_cmd, "--stdout", "-f", "json", "parse", str(input_path)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"AnyStyle error {result.returncode}\n"
                f"STDERR:\n{result.stderr}\n"
                f"STDOUT:\n{result.stdout}"
            )

        stdout = result.stdout.strip()
        if not stdout:
            return []

        return json.loads(stdout)

def parse_all_oai_references_batch(jobs_df):
    parsed_rows = []
    errors = []

    for _, job_row in tqdm(jobs_df.iterrows(), total=len(jobs_df)):
        job = job_row.to_dict()
        gold_refs = extract_gold_refs(job)

        citing_id = id_from_doi_or_temp(
            job.get("doi", ""),
            job.get("identifier", ""),
            job.get("publisher_id", ""),
            job.get("article_title", ""),
            job.get("lang", ""),
        )

        raw_refs = [r["raw_reference"] for r in gold_refs]

        try:
            parsed_batch = parse_with_anystyle_batch(raw_refs)

            for ref_meta, parsed in zip(gold_refs, parsed_batch):
                mapped = map_anystyle_to_oc(parsed)
                mapped.setdefault("doi", "")

                parsed_rows.append({
                    "identifier": job.get("identifier", ""),
                    "publisher_id": job.get("publisher_id", ""),
                    "article_doi": normalize_doi(job.get("doi", "")),
                    "article_title": normalize_text(job.get("article_title", "")),
                    "lang": job.get("lang", ""),
                    "citing_id": citing_id,
                    "reference_index": ref_meta["reference_index"],
                    "gold_ref_id": ref_meta.get("gold_ref_id", ""),
                    "raw_reference": ref_meta["raw_reference"],
                    "parsed_json": json.dumps(parsed, ensure_ascii=False),
                    **mapped,
                })

        except Exception as e:
            for ref_meta in gold_refs:
                errors.append({
                    "identifier": job.get("identifier", ""),
                    "publisher_id": job.get("publisher_id", ""),
                    "article_doi": normalize_doi(job.get("doi", "")),
                    "article_title": normalize_text(job.get("article_title", "")),
                    "lang": job.get("lang", ""),
                    "citing_id": citing_id,
                    "reference_index": ref_meta["reference_index"],
                    "raw_reference": ref_meta["raw_reference"],
                    "error": str(e),
                })

    write_csv(
        parsed_rows,
        OAI_PIPELINE_DIR / "oai_references_parsed_anystyle.csv"
    )

    write_csv(
        errors,
        OAI_PIPELINE_DIR / "oai_references_parsing_errors.csv"
    )

    return pd.DataFrame(parsed_rows), pd.DataFrame(errors)

# =========================================================
# CROSSREF DOI ENRICHMENT
# =========================================================

def sim_string(a, b):
    a = clean_str(a).lower()
    b = clean_str(b).lower()

    if not a or not b:
        return 0.0

    return 1 - lev(a, b) / max(len(a), len(b))


def normalize_title_for_query(title):
    title = clean_str(title)
    title = re.sub(r"\s+", " ", title)
    return title


def crossref_candidate_to_record(item):
    title = ""
    if item.get("title"):
        title = clean_str(item["title"][0])

    container = ""
    if item.get("container-title"):
        container = clean_str(item["container-title"][0])

    year = ""
    for key in ["published-print", "published-online", "published", "issued"]:
        parts = item.get(key, {}).get("date-parts", [])
        if parts and parts[0]:
            year = str(parts[0][0])
            break

    authors = []
    for a in item.get("author", []) or []:
        given = clean_str(a.get("given", ""))
        family = clean_str(a.get("family", ""))
        name = " ".join([given, family]).strip()
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


def build_crossref_query(row):
    parts = []

    if clean_str(row.get("title", "")):
        parts.append(clean_str(row.get("title", "")))

    if clean_str(row.get("author", "")):
        parts.append(clean_str(row.get("author", "")).split(";")[0])

    if clean_str(row.get("pub_date", "")):
        parts.append(extract_year(row.get("pub_date", "")))

    if clean_str(row.get("venue", "")):
        parts.append(clean_str(row.get("venue", "")))

    if not parts:
        parts.append(clean_str(row.get("raw_reference", "")))

    return " ".join(p for p in parts if p)


def query_crossref(query, cache, rows=CROSSREF_ROWS):
    query = clean_str(query)
    cache_key = md5_short(query, n=32)

    if cache_key in cache:
        return cache[cache_key]

    url = "https://api.crossref.org/works"
    params = {
        "query.bibliographic": query,
        "rows": rows,
        "mailto": CROSSREF_MAILTO,
    }

    headers = {
        "User-Agent": CROSSREF_USER_AGENT,
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)

        if response.status_code in {429, 503}:
            time.sleep(3)
            response = requests.get(url, params=params, headers=headers, timeout=30)

        response.raise_for_status()
        data = response.json()

        items = data.get("message", {}).get("items", [])
        candidates = [crossref_candidate_to_record(item) for item in items]

        cache[cache_key] = {
            "query": query,
            "candidates": candidates,
            "error": "",
        }

        time.sleep(CROSSREF_SLEEP)
        return cache[cache_key]

    except Exception as e:
        cache[cache_key] = {
            "query": query,
            "candidates": [],
            "error": str(e),
        }
        return cache[cache_key]


def score_crossref_candidate(row, cand):
    title_sim = sim_string(row.get("title", ""), cand.get("crossref_title", ""))
    author_sim = sim_string(
        clean_str(row.get("author", "")).split(";")[0],
        clean_str(cand.get("crossref_author", "")).split(";")[0],
    )
    venue_sim = sim_string(row.get("venue", ""), cand.get("crossref_venue", ""))

    row_year = extract_year(row.get("pub_date", ""))
    cand_year = extract_year(cand.get("crossref_pub_date", ""))
    year_match = int(bool(row_year and cand_year and row_year == cand_year))

    final_score = (
        0.60 * title_sim +
        0.20 * author_sim +
        0.10 * venue_sim +
        0.10 * year_match
    )

    return {
        "crossref_match_score": final_score,
        "crossref_title_similarity": title_sim,
        "crossref_author_similarity": author_sim,
        "crossref_venue_similarity": venue_sim,
        "crossref_year_match": year_match,
    }


def accept_crossref_candidate(row, cand, score):
    if not cand.get("crossref_doi"):
        return False

    title_sim = score["crossref_title_similarity"]
    year_match = score["crossref_year_match"]
    final_score = score["crossref_match_score"]

    # konserwatywny wariant
    if title_sim >= DOI_ACCEPT_TITLE_THRESHOLD and year_match == 1:
        return True

    # trochę luźniejszy, ale nadal kontrolowany
    if final_score >= DOI_ACCEPT_LOOSE_THRESHOLD and title_sim >= 0.80:
        return True

    return False


def enrich_with_crossref(df_parsed):
    cache = load_json_cache(CROSSREF_CACHE_PATH)

    enriched_rows = []
    diagnostics_rows = []

    for _, row in tqdm(df_parsed.iterrows(), total=len(df_parsed)):
        row = row.to_dict()

        existing_doi = normalize_doi(row.get("doi", ""))

        if existing_doi:
            row["doi_enriched"] = existing_doi
            row["doi_source"] = "existing"
            enriched_rows.append(row)
            continue

        query = build_crossref_query(row)
        result = query_crossref(query, cache)

        best = None
        best_score = None
        accepted = False

        for cand in result["candidates"]:
            score = score_crossref_candidate(row, cand)
            is_accepted = accept_crossref_candidate(row, cand, score)

            diagnostics_rows.append({
                "identifier": row.get("identifier", ""),
                "publisher_id": row.get("publisher_id", ""),
                "reference_index": row.get("reference_index", ""),
                "raw_reference": row.get("raw_reference", ""),
                "local_title": row.get("title", ""),
                "local_author": row.get("author", ""),
                "local_pub_date": row.get("pub_date", ""),
                "local_venue": row.get("venue", ""),
                "crossref_query": query,
                **cand,
                **score,
                "accepted": is_accepted,
                "crossref_error": result.get("error", ""),
            })

            if best_score is None or score["crossref_match_score"] > best_score["crossref_match_score"]:
                best = cand
                best_score = score
                accepted = is_accepted

        if best and accepted:
            row["doi_enriched"] = best["crossref_doi"]
            row["doi_source"] = "crossref"
            row["crossref_match_score"] = best_score["crossref_match_score"]
        else:
            row["doi_enriched"] = ""
            row["doi_source"] = "none"
            row["crossref_match_score"] = ""

        enriched_rows.append(row)

    save_json_cache(cache, CROSSREF_CACHE_PATH)

    write_csv(enriched_rows, OAI_PIPELINE_DIR / "oai_references_crossref_enriched.csv")
    write_csv(diagnostics_rows, OAI_PIPELINE_DIR / "crossref_diagnostics.csv")

    return pd.DataFrame(enriched_rows), pd.DataFrame(diagnostics_rows)

def query_crossref_threadsafe(query, cache, rows=CROSSREF_ROWS):
    query = clean_str(query)
    cache_key = md5_short(query, n=32)

    # 1. szybki odczyt cache
    with cache_lock:
        if cache_key in cache:
            return cache[cache_key]

    # 2. request HTTP poza lockiem
    url = "https://api.crossref.org/works"
    params = {
        "query.bibliographic": query,
        "rows": rows,
        "mailto": CROSSREF_MAILTO,
    }
    headers = {"User-Agent": CROSSREF_USER_AGENT}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)

        if response.status_code in {429, 503}:
            time.sleep(3)
            response = requests.get(url, params=params, headers=headers, timeout=30)

        response.raise_for_status()
        data = response.json()

        items = data.get("message", {}).get("items", [])
        candidates = [crossref_candidate_to_record(item) for item in items]

        result = {
            "query": query,
            "candidates": candidates,
            "error": "",
        }

    except Exception as e:
        result = {
            "query": query,
            "candidates": [],
            "error": str(e),
        }

    # 3. zapis cache pod lockiem
    with cache_lock:
        cache[cache_key] = result

    return result

def enrich_one_row_crossref(row, cache):
    row = row.to_dict() if hasattr(row, "to_dict") else dict(row)

    existing_doi = normalize_doi(row.get("doi", ""))

    if existing_doi:
        row["doi_enriched"] = existing_doi
        row["doi_source"] = "existing"
        row["crossref_match_score"] = ""
        return row, []

    query = build_crossref_query(row)

    # cache musi być chroniony przy wątkach
    result = query_crossref_threadsafe(query, cache)

    best = None
    best_score = None
    accepted = False
    diagnostics_rows = []

    for cand in result["candidates"]:
        score = score_crossref_candidate(row, cand)
        is_accepted = accept_crossref_candidate(row, cand, score)

        diagnostics_rows.append({
            "identifier": row.get("identifier", ""),
            "publisher_id": row.get("publisher_id", ""),
            "reference_index": row.get("reference_index", ""),
            "raw_reference": row.get("raw_reference", ""),
            "local_title": row.get("title", ""),
            "local_author": row.get("author", ""),
            "local_pub_date": row.get("pub_date", ""),
            "local_venue": row.get("venue", ""),
            "crossref_query": query,
            **cand,
            **score,
            "accepted": is_accepted,
            "crossref_error": result.get("error", ""),
        })

        if best_score is None or score["crossref_match_score"] > best_score["crossref_match_score"]:
            best = cand
            best_score = score
            accepted = is_accepted

    if best and accepted:
        row["doi_enriched"] = best["crossref_doi"]
        row["doi_source"] = "crossref"
        row["crossref_match_score"] = best_score["crossref_match_score"]
    else:
        row["doi_enriched"] = ""
        row["doi_source"] = "none"
        row["crossref_match_score"] = ""

    return row, diagnostics_rows


def enrich_with_crossref_parallel(df_parsed, max_workers=CROSSREF_MAX_WORKERS):
    cache = load_json_cache(CROSSREF_CACHE_PATH)

    enriched_rows = []
    diagnostics_rows = []

    records = df_parsed.to_dict("records")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(enrich_one_row_crossref, row, cache)
            for row in records
        ]

        for future in tqdm(as_completed(futures), total=len(futures)):
            row, diag = future.result()
            enriched_rows.append(row)
            diagnostics_rows.extend(diag)

    save_json_cache(cache, CROSSREF_CACHE_PATH)

    write_csv(
        enriched_rows,
        OAI_PIPELINE_DIR / "oai_references_crossref_enriched.csv"
    )

    write_csv(
        diagnostics_rows,
        OAI_PIPELINE_DIR / "crossref_diagnostics.csv"
    )

    return pd.DataFrame(enriched_rows), pd.DataFrame(diagnostics_rows)

# =========================================================
# MATCHING SCORE / DEDUPLICATION
# =========================================================

def m_doi(a, b):
    da = normalize_doi(a.get("doi_enriched", "") or a.get("doi", ""))
    db = normalize_doi(b.get("doi_enriched", "") or b.get("doi", ""))
    return int(bool(da and db and da == db))


def m_title(a, b):
    return sim_string(a.get("title", ""), b.get("title", ""))


def m_first_author(a, b):
    a1 = clean_str(a.get("author", "")).split(";")[0]
    b1 = clean_str(b.get("author", "")).split(";")[0]
    return sim_string(a1, b1)


def m_source(a, b):
    return sim_string(a.get("venue", ""), b.get("venue", ""))


def m_other(a, b):
    ya = extract_year(a.get("pub_date", ""))
    yb = extract_year(b.get("pub_date", ""))

    return int(
        bool(ya and yb and ya == yb) and
        clean_str(a.get("volume", "")) == clean_str(b.get("volume", "")) and
        clean_str(a.get("issue", "")) == clean_str(b.get("issue", "")) and
        clean_str(a.get("page", "")) == clean_str(b.get("page", ""))
    )


def matching_score(a, b):
    return (
        50 * m_doi(a, b) +
        25 * m_title(a, b) +
        10 * m_first_author(a, b) +
        10 * m_source(a, b) +
        5 * m_other(a, b)
    )


def deduplicate_records(df, threshold=30):
    records = df.to_dict("records")

    canonical = []
    mappings = []

    for rec in tqdm(records):
        rec_temp_id = id_from_doi_or_temp(
            rec.get("doi_enriched", "") or rec.get("doi", ""),
            rec.get("title", ""),
            rec.get("author", ""),
            rec.get("pub_date", ""),
            rec.get("venue", ""),
            rec.get("raw_reference", ""),
        )

        best_match = None
        best_score = 0

        for can in canonical:
            score = matching_score(rec, can)
            if score > best_score:
                best_score = score
                best_match = can

        if best_match is not None and best_score > threshold:
            canonical_id = best_match["_canonical_id"]
            mappings.append({
                "original_temp_id": rec_temp_id,
                "canonical_id": canonical_id,
                "matching_score": best_score,
                "dedup_status": "matched_existing",
                "title": rec.get("title", ""),
                "canonical_title": best_match.get("title", ""),
            })
        else:
            canonical_id = rec_temp_id
            rec["_canonical_id"] = canonical_id
            canonical.append(rec)

            mappings.append({
                "original_temp_id": rec_temp_id,
                "canonical_id": canonical_id,
                "matching_score": "",
                "dedup_status": "new_canonical",
                "title": rec.get("title", ""),
                "canonical_title": rec.get("title", ""),
            })

    df_canonical = pd.DataFrame(canonical)
    df_mappings = pd.DataFrame(mappings)

    write_csv(df_canonical.to_dict("records"), OAI_PIPELINE_DIR / "deduplicated_metadata_records.csv")
    write_csv(df_mappings.to_dict("records"), OAI_PIPELINE_DIR / "deduplication_mapping.csv")

    return df_canonical, df_mappings


# =========================================================
# OPENCITATIONS EXPORT
# =========================================================

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

OC_CITATION_FIELDS = [
    "citing_id",
    "cited_id",
]


def build_citing_metadata_from_job(job):
    citing_id = id_from_doi_or_temp(
        job.get("doi", ""),
        job.get("identifier", ""),
        job.get("publisher_id", ""),
        job.get("article_title", ""),
        job.get("lang", ""),
    )

    authors = []
    for a in job.get("authors", []) or []:
        if isinstance(a, dict):
            full_name = clean_str(a.get("full_name", ""))
            if full_name:
                authors.append(full_name)

    fpage = clean_str(job.get("fpage", ""))
    lpage = clean_str(job.get("lpage", ""))
    page = f"{fpage}-{lpage}" if fpage and lpage else fpage

    return {
        "id": citing_id,
        "title": clean_str(job.get("article_title", "")),
        "author": "; ".join(authors),
        "pub_date": clean_str(job.get("year", "")),
        "venue": clean_str(job.get("journal_title", "")),
        "volume": "",
        "issue": clean_str(job.get("issue", "")),
        "page": page,
        "type": "journal article",
        "publisher": "",
        "editor": "",
    }


def build_oc_export(jobs_df, df_enriched, df_canonical, df_mappings):
    canonical_by_title_key = {}

    for _, row in df_canonical.iterrows():
        row = row.to_dict()

        rid = id_from_doi_or_temp(
            row.get("doi_enriched", "") or row.get("doi", ""),
            row.get("title", ""),
            row.get("author", ""),
            row.get("pub_date", ""),
            row.get("venue", ""),
            row.get("raw_reference", ""),
        )

        canonical_by_title_key[rid] = {
            "id": row.get("_canonical_id", rid),
            "title": clean_str(row.get("title", "")),
            "author": clean_str(row.get("author", "")),
            "pub_date": clean_str(row.get("pub_date", "")),
            "venue": clean_str(row.get("venue", "")),
            "volume": clean_str(row.get("volume", "")),
            "issue": clean_str(row.get("issue", "")),
            "page": clean_str(row.get("page", "")),
            "type": clean_str(row.get("type", "")) or "journal article",
            "publisher": clean_str(row.get("publisher", "")),
            "editor": clean_str(row.get("editor", "")),
        }

    mapping_lookup = dict(
        zip(
            df_mappings["original_temp_id"],
            df_mappings["canonical_id"]
        )
    )

    metadata_rows = []
    citation_rows = []

    # citing articles
    for _, job_row in jobs_df.iterrows():
        job = job_row.to_dict()
        metadata_rows.append(build_citing_metadata_from_job(job))

    # cited canonical records
    metadata_rows.extend(canonical_by_title_key.values())

    # citations
    for _, row in df_enriched.iterrows():
        row = row.to_dict()

        original_id = id_from_doi_or_temp(
            row.get("doi_enriched", "") or row.get("doi", ""),
            row.get("title", ""),
            row.get("author", ""),
            row.get("pub_date", ""),
            row.get("venue", ""),
            row.get("raw_reference", ""),
        )

        cited_id = mapping_lookup.get(original_id, original_id)

        citation_rows.append({
            "citing_id": row.get("citing_id", ""),
            "cited_id": cited_id,
        })

    # drop exact duplicate metadata ids
    seen = set()
    metadata_unique = []
    for row in metadata_rows:
        if row["id"] not in seen:
            seen.add(row["id"])
            metadata_unique.append(row)

    # drop exact duplicate citation pairs
    seen_pairs = set()
    citation_unique = []
    for row in citation_rows:
        pair = (row["citing_id"], row["cited_id"])
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            citation_unique.append(row)

    write_csv(metadata_unique, OAI_PIPELINE_DIR / "example_metadata.csv", OC_METADATA_FIELDS)
    write_csv(citation_unique, OAI_PIPELINE_DIR / "example_citations.csv", OC_CITATION_FIELDS)

    return pd.DataFrame(metadata_unique), pd.DataFrame(citation_unique)

# Validation
def run_oc_validator(
    metadata_path,
    citations_path,
    output_dir=OAI_PIPELINE_DIR / "validation",
):
    output_dir = Path(output_dir)
    meta_out = output_dir / "metadata"
    cits_out = output_dir / "citations"

    meta_out.mkdir(exist_ok=True, parents=True)
    cits_out.mkdir(exist_ok=True, parents=True)

    oc_python = Path(r"C:\Users\pracownik\Documents\oc_validator_env\Scripts\python.exe")

    if not oc_python.exists():
        raise FileNotFoundError(f"OC validator Python not found: {oc_python}")

    commands = [
        ("metadata", [
            str(oc_python),
            "-m",
            "oc_validator.main",
            "-i",
            str(metadata_path),
            "-o",
            str(meta_out),
        ]),
        ("citations", [
            str(oc_python),
            "-m",
            "oc_validator.main",
            "-i",
            str(citations_path),
            "-o",
            str(cits_out),
        ]),
    ]

    results = []

    for name, cmd in commands:
        print("\nCMD:", cmd)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        report = {
            "type": name,
            "command": " ".join(cmd),
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

        results.append(report)

        print("=" * 80)
        print(name.upper())
        print("returncode:", result.returncode)
        print("--- STDOUT ---")
        print(result.stdout)
        print("--- STDERR ---")
        print(result.stderr)

    (output_dir / "oc_validator_report.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return results

#%%
# =========================================================
# RUN STAGES
# =========================================================

# ETAP 1: wolny, opcjonalnie próbkowany PDF/GROBID
df_pdf_eval = run_pdf_extraction_evaluation(
    jobs_df,
    sample_size=PDF_SAMPLE_SIZE,
)

# ETAP 2: szybki, pełny OAI-PMH → AnyStyle
df_parsed, df_parse_errors = parse_all_oai_references_batch(
    jobs_df[:PDF_SAMPLE_SIZE]
)

df_parsed["crossref_query"] = df_parsed.apply(build_crossref_query, axis=1)

unique_queries = df_parsed["crossref_query"].dropna().drop_duplicates()
print(len(df_parsed), len(unique_queries))

# ETAP 3: Crossref DOI enrichment
# df_enriched, df_crossref_diag = enrich_with_crossref(df_parsed)

df_enriched, df_crossref_diag = enrich_with_crossref_parallel(
    df_parsed,
    max_workers=CROSSREF_MAX_WORKERS
)

# ETAP 4: deduplikacja
df_canonical, df_mappings = deduplicate_records(df_enriched, threshold=30)

# ETAP 5: OpenCitations export
df_oc_metadata, df_oc_citations = build_oc_export(
    jobs_df=jobs_df,
    df_enriched=df_enriched,
    df_canonical=df_canonical,
    df_mappings=df_mappings,
)

# ETAP 6: OpenCitations validator
validation_results = run_oc_validator(
    metadata_path=OAI_PIPELINE_DIR / "example_metadata.csv",
    citations_path=OAI_PIPELINE_DIR / "example_citations.csv",
)

print("DONE")
print(f"PDF evaluation rows: {len(df_pdf_eval)}")
print(f"Parsed OAI references: {len(df_parsed)}")
print(f"Crossref-enriched references: {len(df_enriched)}")
print(f"Canonical cited records: {len(df_canonical)}")
print(f"OC metadata rows: {len(df_oc_metadata)}")
print(f"OC citation rows: {len(df_oc_citations)}")
print(f"Output directory: {OUTPUT_DIR}")


#%%

from oc_validator.interface.gui import make_gui

csv_path = r"C:\Users\pracownik\Documents\Miniatura-NCN\data\final_pipeline_output\02_oai_to_opencitations\example_metadata.csv"
report_path = r"C:\Users\pracownik\Documents\Miniatura-NCN\data\out_validate_meta.jsonl"
output_html_path = 'data/oc_validator.html'

make_gui(csv_path, report_path, output_html_path)








from oc_validator.main import Validator

# Basic validation
v = Validator(r"C:\Users\pracownik\Documents\Miniatura-NCN\data\final_pipeline_output\02_oai_to_opencitations\example_metadata.csv", 'data')
v.validate()




## findings
# I need to run the whole code in python 3.12
# tam, gdzie mam typ z crossref, wziąć typ z crossref, a reszta anystyle
# I need to have a test validation to catch errors that can be fixed
# new cleaning added
# final validation
# deleting the mistakes from the meta and citatons file with dedicated OC tool
# double check that there is ultimate validation with no errors
# w artykule namierzyć miejsca dla human-in-the-loop, 

























