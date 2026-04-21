import sys
sys.path.insert(1, r'C:\Users\pracownik\Documents\IBL-PAN-Python')
from my_functions import gsheet_to_df
import requests
from zipfile import ZipFile, BadZipFile
from io import BytesIO
import pandas as pd
from bs4 import BeautifulSoup
import os
from sickle import Sickle
from sickle.oaiexceptions import IdDoesNotExist, BadVerb
import regex as re
from tqdm import tqdm
import time
import json
from requests.exceptions import HTTPError
import pickle
import urllib3
from datetime import date
import shutil
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import xml.etree.ElementTree as ET
from urllib.parse import unquote

#%% define resources

#teksty_drugie = 'https://rcin.org.pl/dlibra/oai-pmh-repository.xml'
teksty_drugie = 'https://rcin.org.pl/ibl/dlibra/publication/63380#structure'

forum_poetyki = 'http://pressto.amu.edu.pl/index.php/fp/oai'

zagadnienia_rodzajow_literackich = 'http://czasopisma.ltn.lodz.pl/index.php/Zagadnienia-Rodzajow-Literackich/oai'

#%%Teksty Drugie
teksty_drugie = "https://rcin.org.pl/dlibra/oai-pmh-repository.xml"

results_teksty_drugie = []
articles_counter_ojs = 0

sick = Sickle(teksty_drugie)
records = sick.ListRecords(metadataPrefix='oai_dc', set='rcin.org.pl:partnerCollections:iblit:serials')

for record in tqdm(records):
    if record.deleted == False:
        articles_counter_ojs += 1
        record = record.get_metadata()
        results_teksty_drugie.append(record)
        
with open("data/IBL_data.pkl", "wb") as f:
    pickle.dump(results_teksty_drugie, f)
        
with open("data/IBL_data.pkl", "rb") as f:
    results_teksty_drugie = pickle.load(f)

teksty_drugie_selected = [e for e in results_teksty_drugie if any('Teksty Drugie' in el for el in e.get('title'))]

teksty_drugie_selected = [e for e in teksty_drugie_selected if len(e.get('relation')) >= 3]

with open("data/Teksty_Drugie_harvested.pkl", "wb") as f:
    pickle.dump(teksty_drugie_selected, f)

#%% Teksty Drugie PDF

# =========================================================
# CONFIG
# =========================================================

INPUT_PICKLE = "data/Teksty_Drugie_harvested.pkl"
BASE_OUTPUT_DIR = Path("data/Teksty_Drugie")
PDF_DIR = BASE_OUTPUT_DIR / "pdfs"
TMP_ZIP_DIR = BASE_OUTPUT_DIR / "tmp_zip"
EXTRACT_DIR = BASE_OUTPUT_DIR / "extracted"

MAX_WORKERS = 16   # możesz testować 8, 12, 16, 20
TIMEOUT = 60

BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)
TMP_ZIP_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================
# HELPERS
# =========================================================

def rcin_to_pdf_zip(url: str) -> tuple[str, str]:
    """
    Zwraca (edition_id, download_url)
    """
    match = re.search(r"/edition/(\d+)/content", url)
    if not match:
        raise ValueError(f"Niepoprawny URL RCIN: {url}")

    edition_id = match.group(1)
    download_url = f"https://rcin.org.pl/Content/{edition_id}/download/"
    return edition_id, download_url


def safe_name(name: str) -> str:
    """
    Sanitizacja nazwy pliku pod Windows.
    """
    name = name.strip().replace("\x00", "")
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def unique_path(path: Path) -> Path:
    """
    Jeśli plik już istnieje, dodaje sufiks _1, _2, ...
    """
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    i = 1
    while True:
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def extract_first_pdf_from_zip(zip_bytes: bytes, edition_id: str) -> dict:
    """
    Rozpakowuje zip z pamięci, znajduje PDF, zapisuje go do PDF_DIR.
    Zwraca dict z informacją o zapisanym pliku.
    """
    extract_subdir = EXTRACT_DIR / edition_id
    extract_subdir.mkdir(parents=True, exist_ok=True)

    pdf_candidates = []

    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        members = zf.namelist()

        # opcjonalnie zapis surowego ZIP do debugowania
        zip_path = TMP_ZIP_DIR / f"{edition_id}.zip"
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)

        # rozpakowanie
        zf.extractall(extract_subdir)

        for member in members:
            if member.lower().endswith(".pdf"):
                pdf_candidates.append(member)

    if not pdf_candidates:
        return {
            "status": "zip_without_pdf",
            "pdf_filename": None,
            "pdf_path": None,
            "zip_path": str(TMP_ZIP_DIR / f"{edition_id}.zip"),
        }

    # bierzemy pierwszy PDF; jeśli będzie ich więcej, też raportujemy liczbę
    pdf_member = pdf_candidates[0]
    extracted_pdf_path = extract_subdir / pdf_member

    if not extracted_pdf_path.exists():
        # fallback: rekursywne szukanie po rozpakowaniu
        found = list(extract_subdir.rglob("*.pdf"))
        if not found:
            return {
                "status": "pdf_missing_after_extract",
                "pdf_filename": None,
                "pdf_path": None,
                "zip_path": str(TMP_ZIP_DIR / f"{edition_id}.zip"),
            }
        extracted_pdf_path = found[0]

    final_name = safe_name(extracted_pdf_path.name)
    final_pdf_path = unique_path(PDF_DIR / final_name)

    shutil.copy2(extracted_pdf_path, final_pdf_path)

    return {
        "status": "ok",
        "pdf_filename": final_pdf_path.name,
        "pdf_path": str(final_pdf_path),
        "zip_path": str(TMP_ZIP_DIR / f"{edition_id}.zip"),
        "pdf_count_in_zip": len(pdf_candidates),
    }


def process_record(record_url: str, session: requests.Session) -> dict:
    """
    Pobiera ZIP, rozpakowuje, znajduje PDF i zwraca wynik przetwarzania.
    """
    try:
        edition_id, download_url = rcin_to_pdf_zip(record_url)
    except ValueError as e:
        return {
            "record_url": record_url,
            "edition_id": None,
            "download_url": None,
            "status": "invalid_rcin_url",
            "error": str(e),
            "pdf_filename": None,
            "pdf_path": None,
        }

    try:
        response = session.get(download_url, timeout=TIMEOUT, stream=True)
    except requests.RequestException as e:
        return {
            "record_url": record_url,
            "edition_id": edition_id,
            "download_url": download_url,
            "status": "request_exception",
            "error": str(e),
            "pdf_filename": None,
            "pdf_path": None,
        }

    if response.status_code == 401:
        return {
            "record_url": record_url,
            "edition_id": edition_id,
            "download_url": download_url,
            "status": "unauthorized_401",
            "pdf_filename": None,
            "pdf_path": None,
        }

    if response.status_code != 200:
        return {
            "record_url": record_url,
            "edition_id": edition_id,
            "download_url": download_url,
            "status": f"http_{response.status_code}",
            "pdf_filename": None,
            "pdf_path": None,
        }

    content = response.content

    # czasem serwer może zwrócić HTML zamiast ZIP
    content_type = response.headers.get("Content-Type", "").lower()
    if "zip" not in content_type and not content[:4] == b"PK\x03\x04":
        return {
            "record_url": record_url,
            "edition_id": edition_id,
            "download_url": download_url,
            "status": "not_a_zip",
            "content_type": content_type,
            "pdf_filename": None,
            "pdf_path": None,
        }

    try:
        extracted = extract_first_pdf_from_zip(content, edition_id)
    except zipfile.BadZipFile:
        return {
            "record_url": record_url,
            "edition_id": edition_id,
            "download_url": download_url,
            "status": "bad_zip",
            "pdf_filename": None,
            "pdf_path": None,
        }
    except Exception as e:
        return {
            "record_url": record_url,
            "edition_id": edition_id,
            "download_url": download_url,
            "status": "extract_exception",
            "error": str(e),
            "pdf_filename": None,
            "pdf_path": None,
        }

    return {
        "record_url": record_url,
        "edition_id": edition_id,
        "download_url": download_url,
        **extracted
    }


# =========================================================
# LOAD INPUT
# =========================================================

with open(INPUT_PICKLE, "rb") as f:
    results_teksty_drugie = pickle.load(f)

record_urls = []
for e in results_teksty_drugie:
    identifiers = e.get("identifier", [])
    dlibra_links = [x for x in identifiers if isinstance(x, str) and "dlibra/publication" in x]
    if dlibra_links:
        record_urls.append(dlibra_links[0])

record_urls = sorted(set(record_urls))

# =========================================================
# PARALLEL DOWNLOAD + EXTRACT
# =========================================================

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://rcin.org.pl/"
}

results = []

with requests.Session() as session:
    session.headers.update(headers)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_record, record_url, session): record_url
            for record_url in record_urls
        }

        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            results.append(result)

results = [e for e in results if e.get('pdf_count_in_zip')]

# =========================================================
# SAVE RESULTS
# =========================================================

df_results = pd.DataFrame(results)
url_to_pdf = dict(zip(df_results["record_url"], df_results["pdf_filename"]))

td_final = []
for e in results_teksty_drugie:
    dlibra_id = [el for el in e.get('identifier') if 'dlibra' in el][0]
    if dlibra_id in url_to_pdf:
        e.update({'dlibra_id': dlibra_id,
                  'pdf_name': url_to_pdf.get(dlibra_id)})


results_filtered = [
    e for e in results_teksty_drugie
    if any(
        isinstance(x, str) and x in url_to_pdf.keys()
        for x in e.get("identifier", [])
    )
]

with open("data/Teksty_Drugie_harvested.pkl", "wb") as f:
    pickle.dump(results_filtered, f)


#%% Forum Poetyki

OAI_BASE = "https://pressto.amu.edu.pl/index.php/fp/oai"


def clean_text(text):
    if text is None:
        return None
    text = " ".join(text.split())
    return text if text else None


def get_local_tag(tag):
    """
    Zamienia np. '{namespace}article-title' -> 'article-title'
    """
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def find_first_text(root, tag_name):
    for elem in root.iter():
        if get_local_tag(elem.tag) == tag_name:
            text = "".join(elem.itertext())
            return clean_text(text)
    return None


def find_all_texts(root, tag_name):
    values = []
    for elem in root.iter():
        if get_local_tag(elem.tag) == tag_name:
            text = "".join(elem.itertext())
            text = clean_text(text)
            if text is not None:
                values.append(text)
    return values


def extract_pdf_urls(root):
    pdf_urls = []
    article_urls = []

    for elem in root.iter():
        if get_local_tag(elem.tag) == "self-uri":
            href = elem.attrib.get("{http://www.w3.org/1999/xlink}href")
            content_type = elem.attrib.get("content-type")

            if href:
                if content_type == "application/pdf":
                    pdf_urls.append(href)
                else:
                    article_urls.append(href)

    return pdf_urls, article_urls


def extract_authors(root):
    authors = []

    for contrib_group in root.iter():
        if get_local_tag(contrib_group.tag) != "contrib-group":
            continue

        # interesuje nas grupa autorów
        if contrib_group.attrib.get("content-type") != "author":
            continue

        for contrib in contrib_group:
            if get_local_tag(contrib.tag) != "contrib":
                continue

            surname = None
            given_names = None
            orcid = None

            for elem in contrib.iter():
                local = get_local_tag(elem.tag)

                if local == "surname" and surname is None:
                    surname = clean_text("".join(elem.itertext()))

                elif local == "given-names" and given_names is None:
                    given_names = clean_text("".join(elem.itertext()))

                elif local == "contrib-id":
                    if elem.attrib.get("contrib-id-type") == "orcid":
                        orcid = clean_text("".join(elem.itertext()))

            authors.append({
                "surname": surname,
                "given_names": given_names,
                "full_name": clean_text(f"{given_names or ''} {surname or ''}"),
                "orcid": orcid
            })

    return authors


def extract_references(root):
    references = []

    for ref in root.iter():
        if get_local_tag(ref.tag) != "ref":
            continue

        ref_id = ref.attrib.get("id")
        citation_text = None

        for child in ref.iter():
            if get_local_tag(child.tag) == "mixed-citation":
                citation_text = clean_text("".join(child.itertext()))
                break

        if citation_text:
            references.append({
                "ref_id": ref_id,
                "citation": citation_text
            })

    return references


def parse_jats_record(xml_bytes, oai_identifier):
    root = ET.fromstring(xml_bytes)

    pdf_urls, article_urls = extract_pdf_urls(root)
    authors = extract_authors(root)
    references = extract_references(root)

    record = {
        "identifier": oai_identifier,
        "journal_title": find_first_text(root, "journal-title"),
        "article_title": find_first_text(root, "article-title"),
        "doi": None,
        "publisher_id": None,
        "year": None,
        "issue": find_first_text(root, "issue"),
        "fpage": find_first_text(root, "fpage"),
        "lpage": find_first_text(root, "lpage"),
        "abstract": find_first_text(root, "abstract"),
        "keywords": find_all_texts(root, "kwd"),
        "authors": authors,
        "pdf_urls": pdf_urls,
        "article_urls": article_urls,
        "references": references
    }

    # article-id z atrybutami
    for elem in root.iter():
        if get_local_tag(elem.tag) == "article-id":
            value = clean_text("".join(elem.itertext()))
            pub_id_type = elem.attrib.get("pub-id-type")

            if pub_id_type == "doi" and record["doi"] is None:
                record["doi"] = value
            elif pub_id_type == "publisher-id" and record["publisher_id"] is None:
                record["publisher_id"] = value

    # pub-date: bierzemy rok z publication-format="epub" albo pierwszy sensowny
    for elem in root.iter():
        if get_local_tag(elem.tag) == "pub-date":
            year = None
            for child in elem:
                if get_local_tag(child.tag) == "year":
                    year = clean_text("".join(child.itertext()))
                    break
            if year:
                record["year"] = year
                break

    return record


def get_record_xml(oai_base, identifier, timeout=60):
    url = f"{oai_base}?verb=GetRecord&metadataPrefix=jats&identifier={identifier}"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


# ==========================================
# HARVEST
# ==========================================

sick = Sickle(OAI_BASE)
records = sick.ListRecords(metadataPrefix="jats")

results_forum_poetyki = []
errors = []

for record in tqdm(records):
    if record.deleted is False:
        identifier = record.header.identifier

        try:
            xml_bytes = get_record_xml(OAI_BASE, identifier)
            parsed = parse_jats_record(xml_bytes, identifier)
            results_forum_poetyki.append(parsed)

        except Exception as e:
            errors.append({
                "identifier": identifier,
                "error": str(e)
            })
            
with open("data/Forum_Poetyki_harvested.pkl", "wb") as f:
    pickle.dump(results_forum_poetyki, f)

#%% Forum Poetyki PDF

with open("data/Forum_Poetyki_harvested.pkl", "rb") as f:
    results_forum_poetyki = pickle.load(f)

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = Path("data/forum_poetyki_pdfs")
PL_DIR = BASE_DIR / "pl"
EN_DIR = BASE_DIR / "en"

PL_DIR.mkdir(parents=True, exist_ok=True)
EN_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://pressto.amu.edu.pl/"
}


# =========================================================
# HELPERS
# =========================================================

def extract_last_number(url: str) -> int:
    match = re.search(r"/(\d+)/?$", url)
    if not match:
        raise ValueError(f"Nie udało się wyciągnąć numeru z URL: {url}")
    return int(match.group(1))


def split_pdf_urls(pdf_urls: list[str]) -> tuple[str | None, str | None]:
    """
    Zakładamy:
    - 2 linki viewerowe w pdf_urls
    - mniejsza liczba na końcu URL = pl
    - większa liczba na końcu URL = en
    """
    if not pdf_urls:
        return None, None

    if len(pdf_urls) == 1:
        return pdf_urls[0], None

    sorted_urls = sorted(pdf_urls, key=extract_last_number)
    pl_view_url = sorted_urls[0]
    en_view_url = sorted_urls[1]

    return pl_view_url, en_view_url


def safe_filename(name: str) -> str:
    name = unquote(name)
    name = os.path.basename(name)
    name = name.strip().replace("\x00", "")
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name


def filename_from_cd(content_disposition: str) -> str | None:
    if not content_disposition:
        return None

    patterns = [
        r"filename\*=UTF-8''([^;]+)",
        r'filename="([^"]+)"',
        r"filename=([^;]+)"
    ]

    for pattern in patterns:
        match = re.search(pattern, content_disposition, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().strip('"')

    return None


def fallback_filename(article: dict, lang: str) -> str:
    publisher_id = article.get("publisher_id") or "no_id"
    year = article.get("year") or "no_year"
    issue = article.get("issue") or "no_issue"
    return f"{publisher_id}_{year}_{issue}_{lang}.pdf"


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    i = 1

    while True:
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def resolve_direct_pdf_url(view_url: str, session: requests.Session) -> str | None:
    """
    pdf_urls z rekordu OJS prowadzą do strony viewerowej HTML.
    Ta funkcja wyciąga z niej właściwy link /article/download/...
    """
    response = session.get(view_url, timeout=60)
    response.raise_for_status()

    html = response.text

    # 1. link z przycisku "Pobierz"
    match = re.search(r'href="(https?://[^"]+/article/download/[^"]+)"', html)
    if match:
        return match.group(1)

    # 2. fallback: URL PDF w iframe pdf.js ?file=
    match = re.search(r'viewer\.html\?file=([^"]+)', html)
    if match:
        return unquote(match.group(1))

    return None


def download_direct_pdf(
    pdf_url: str,
    target_dir: Path,
    article: dict,
    lang: str,
    session: requests.Session
) -> dict:
    try:
        response = session.get(pdf_url, timeout=60, stream=True)

        if response.status_code == 401:
            return {
                "status": "unauthorized_401",
                "filename": None,
                "server_filename": None,
                "path": None
            }

        if response.status_code != 200:
            return {
                "status": f"http_{response.status_code}",
                "filename": None,
                "server_filename": None,
                "path": None
            }

        content_type = response.headers.get("Content-Type", "").lower()
        if "pdf" not in content_type:
            return {
                "status": f"not_pdf_content_type:{content_type}",
                "filename": None,
                "server_filename": None,
                "path": None
            }

        cd = response.headers.get("Content-Disposition", "")
        server_filename = filename_from_cd(cd)

        # lokalnie zapisujemy pod własną, krótką nazwą
        filename = safe_filename(fallback_filename(article, lang))
        if not filename.lower().endswith(".pdf"):
            filename += ".pdf"

        target_dir.mkdir(parents=True, exist_ok=True)
        output_path = unique_path(target_dir / filename)

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return {
            "status": "ok",
            "filename": output_path.name,
            "server_filename": server_filename,
            "path": str(output_path)
        }

    except requests.RequestException as e:
        return {
            "status": "request_exception",
            "filename": None,
            "server_filename": None,
            "path": None,
            "error": str(e)
        }
    except OSError as e:
        return {
            "status": "os_error",
            "filename": None,
            "server_filename": None,
            "path": None,
            "error": str(e)
        }


def process_one_language(
    view_url: str | None,
    target_dir: Path,
    article: dict,
    lang: str,
    session: requests.Session
) -> dict:
    if not view_url:
        return {
            "view_url": None,
            "direct_pdf_url": None,
            "status": "missing",
            "filename": None,
            "server_filename": None,
            "path": None
        }

    try:
        direct_pdf_url = resolve_direct_pdf_url(view_url, session)
    except requests.RequestException as e:
        return {
            "view_url": view_url,
            "direct_pdf_url": None,
            "status": "resolve_exception",
            "filename": None,
            "server_filename": None,
            "path": None,
            "error": str(e)
        }

    if not direct_pdf_url:
        return {
            "view_url": view_url,
            "direct_pdf_url": None,
            "status": "direct_pdf_not_found",
            "filename": None,
            "server_filename": None,
            "path": None
        }

    result = download_direct_pdf(direct_pdf_url, target_dir, article, lang, session)
    result["view_url"] = view_url
    result["direct_pdf_url"] = direct_pdf_url
    return result


def download_forum_poetyki_pdfs(results_forum_poetyki: list[dict]) -> pd.DataFrame:
    manifest = []

    with requests.Session() as session:
        session.headers.update(HEADERS)

        for article in tqdm(results_forum_poetyki):
            pl_view_url, en_view_url = split_pdf_urls(article.get("pdf_urls", []))

            pl_result = process_one_language(pl_view_url, PL_DIR, article, "pl", session)
            en_result = process_one_language(en_view_url, EN_DIR, article, "en", session)

            manifest.append({
                "identifier": article.get("identifier"),
                "publisher_id": article.get("publisher_id"),
                "doi": article.get("doi"),
                "article_title": article.get("article_title"),

                "pl_view_url": pl_result.get("view_url"),
                "pl_direct_pdf_url": pl_result.get("direct_pdf_url"),
                "pl_status": pl_result.get("status"),
                "pl_filename": pl_result.get("filename"),
                "pl_server_filename": pl_result.get("server_filename"),
                "pl_path": pl_result.get("path"),

                "en_view_url": en_result.get("view_url"),
                "en_direct_pdf_url": en_result.get("direct_pdf_url"),
                "en_status": en_result.get("status"),
                "en_filename": en_result.get("filename"),
                "en_server_filename": en_result.get("server_filename"),
                "en_path": en_result.get("path"),
            })

    return pd.DataFrame(manifest)


# =========================================================
# OPTIONAL: LOAD DATA
# =========================================================
# Jeśli masz dane zapisane w pickle:
#
# with open("data/results_forum_poetyki.pkl", "rb") as f:
#     results_forum_poetyki = pickle.load(f)


# =========================================================
# RUN
# =========================================================
# Zakładam, że masz już listę słowników w zmiennej results_forum_poetyki

df_manifest = download_forum_poetyki_pdfs(results_forum_poetyki)
df_manifest.to_excel('data/forum_poetyki_harvesting_info.xlsx', index = False)

#%% Zagadnienia Rodzajów Literackich

OAI_BASE = "http://czasopisma.ltn.lodz.pl/index.php/Zagadnienia-Rodzajow-Literackich/oai"

# ==========================================
# HARVEST
# ==========================================

sick = Sickle(OAI_BASE)
records = sick.ListRecords(metadataPrefix="jats")

results_zagadnienia = []
errors = []

for record in tqdm(records):
    if record.deleted is False:
        identifier = record.header.identifier

        try:
            xml_bytes = get_record_xml(OAI_BASE, identifier)
            parsed = parse_jats_record(xml_bytes, identifier)
            results_zagadnienia.append(parsed)

        except Exception as e:
            errors.append({
                "identifier": identifier,
                "error": str(e)
            })

with open("data/Zagadnienia_Rodzajów_Literackich_harvested.pkl", "wb") as f:
    pickle.dump(results_zagadnienia, f)


#%% Zagadnienia Rodzajów Literackich PDF

with open("data/Zagadnienia_Rodzajów_Literackich_harvested.pkl", "rb") as f:
    results_zagadnienia = pickle.load(f)

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = Path("data/zagadnienia_rodzajow_literackich_pdfs")
PL_DIR = BASE_DIR / "pl"
EN_DIR = BASE_DIR / "en"

PL_DIR.mkdir(parents=True, exist_ok=True)
EN_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://pressto.amu.edu.pl/"
}

df_manifest = download_forum_poetyki_pdfs(results_zagadnienia)
df_manifest.to_excel('data/zagadnienia_rodzajow_literackich_harvesting_info.xlsx', index = False)



























    
    