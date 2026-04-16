from requests import get
import glob
from tqdm import tqdm
import pandas as pd
import regex as re
import requests
from concurrent.futures import ThreadPoolExecutor
import pickle
from opencitations_token import oc_token
import csv
from collections import Counter
# from core_api_key import core_api_key
from functools import partial
import zipfile
import io
import tarfile
import time
import matplotlib.pyplot as plt
import numpy as np

headers = {"authorization": oc_token}
#%% Pipeline
#1 Read file with journals information

df = pd.read_excel('data/czasopisma_literaturoznawcze_scopus.xlsx')

#2 Harvest OMID of the venue based on ISSN
issns_list = [e for e in df['ISSN'].to_list() + df['e-ISSN'].to_list() if pd.notnull(e)]
issns_list = list(dict.fromkeys(issns_list))  # usuwa duplikaty, zachowuje kolejność

issn_omid_dict = {}

MIN_INTERVAL = 0.36
last_request_time = 0
max_retries = 5

for issn in tqdm(issns_list):
    # pilnowanie limitu requestów
    #issn = '2083-2222'
    elapsed = time.monotonic() - last_request_time
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)

    url = f'https://api.opencitations.net/meta/v1/metadata/issn:{issn}'

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=60)
            last_request_time = time.monotonic()

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_time = float(retry_after) if retry_after else 5 * (attempt + 1)
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            response_json = response.json()

            if response_json:
                ids = response_json[0].get('id', '')
                omids = [e.replace('omid:', '') for e in ids.split(' ') if 'omid:' in e]

                if omids:
                    issn_omid_dict.update({issn: omids[0]})
                else:
                    issn_omid_dict.update({issn: None})
            else:
                issn_omid_dict.update({issn: None})

            break

        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f'Błąd dla {issn}: {e}')
                issn_omid_dict.update({issn: None})
            else:
                time.sleep(3 * (attempt + 1))


# issns_list = [e for e in df['ISSN'].to_list() + df['e-ISSN'].to_list() if pd.notnull(e)]

# issn_omid_dict = {}
# headers = {"authorization": oc_token}
# for issn in tqdm(issns_list):
#     # issn = issns_list[0]
#     response = requests.get(f'https://api.opencitations.net/meta/v1/metadata/issn:{issn}', headers=headers)
#     response_json = response.json()
#     if response_json:
#         omid = [e.replace('omid:', '') for e in response_json[0].get('id').split(' ') if 'omid' in e][0]
#         issn_omid_dict.update({issn: omid})
#     else: issn_omid_dict.update({issn: None})

df['OMID for ISSN'] = df['ISSN'].apply(lambda x: issn_omid_dict.get(x))
df['OMID for e-ISSN'] = df['e-ISSN'].apply(lambda x: issn_omid_dict.get(x))
df['OMIDs identical'] = df[['OMID for ISSN', 'OMID for e-ISSN']].apply(lambda x: x['OMID for ISSN'] == x['OMID for e-ISSN'] if pd.notna(x['OMID for ISSN']) and pd.notna(x['OMID for e-ISSN']) else '', axis=1)
df['OMID'] = df[['OMID for ISSN', 'OMID for e-ISSN']].apply(lambda x: x['OMID for ISSN'] if pd.notna(x['OMID for ISSN']) else x['OMID for e-ISSN'] if pd.notna(x['OMID for e-ISSN']) else '', axis=1)
df['in OpenCitations'] = df['OMID'].apply(lambda x: True if pd.notnull(x) and x!='' else False)

df.to_excel('data/literary_journals_scopus_with_omid.xlsx', index=False)

# 3 Count of citations for a venue based on issn
df = pd.read_excel('data/literary_journals_scopus_with_omid.xlsx')
df_omid = df.loc[df['OMID'].notna()]
issns_list = [e for e in df_omid['ISSN'].to_list() + df_omid['e-ISSN'].to_list() if pd.notnull(e)]
issns_list = list(dict.fromkeys(issns_list))

issn_citations_dict = {}

MIN_INTERVAL = 0.36
last_request_time = 0
max_retries = 5

for issn in tqdm(issns_list):
    # issn = issns_list[0]
    # pilnowanie limitu requestów
    elapsed = time.monotonic() - last_request_time
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)

    url = f'https://api.opencitations.net/index/v2/venue-citation-count/issn:{issn}'

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=60)
            last_request_time = time.monotonic()

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_time = float(retry_after) if retry_after else 5 * (attempt + 1)
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            response_json = response.json()

            if response_json:
                citations_counted = int(response_json[0].get('count'))
                issn_citations_dict.update({issn: citations_counted})

            else:
                issn_citations_dict.update({issn: None})

            break

        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f'Błąd dla {issn}: {e}')
                issn_citations_dict.update({issn: None})
            else:
                time.sleep(3 * (attempt + 1))

df['citations counted'] = df[['ISSN', 'e-ISSN']].apply(lambda x: issn_citations_dict.get(x['ISSN'], issn_citations_dict.get(x['e-ISSN'], 0)), axis=1)

df.to_excel('data/literary_journals_scopus_with_omid_citations_counted.xlsx', index=False)

#%% Visualisations

# =========================
# 1. Wczytanie danych
# =========================
file_path = "data/literary_journals_scopus_with_omid_citations_counted.xlsx"
df = pd.read_excel(file_path)

# Upewnienie się, że kolumna z cytowaniami jest liczbowa
df["citations counted"] = pd.to_numeric(df["citations counted"], errors="coerce").fillna(0)

# =========================
# 2. Wykres kołowy:
#    liczba czasopism w OpenCitations i poza OpenCitations
# =========================
oc_counts = df["in OpenCitations"].value_counts(dropna=False)

# Mapowanie etykiet na bardziej czytelne
oc_labels = []
for val in oc_counts.index:
    if val is True:
        oc_labels.append("in OpenCitations")
    elif val is False:
        oc_labels.append("not in OpenCitations")
    else:
        oc_labels.append("no data")

plt.figure(figsize=(7, 7))
plt.pie(
    oc_counts.values,
    labels=oc_labels,
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Polish Literary Journals in OpenCitations")
plt.axis("equal")
plt.tight_layout()
plt.show()

# =========================
# 3. Wykres słupkowy:
#    liczba cytowań dla czasopism
# =========================
# Sortowanie malejąco po liczbie cytowań
df_citations = df[["Tytuł", "citations counted"]].copy()
df_citations = df_citations.sort_values("citations counted", ascending=False)

plt.figure(figsize=(14, 8))
plt.bar(df_citations["Tytuł"], df_citations["citations counted"])
plt.title("Citations counted by journal")
plt.xlabel("Journal title")
plt.ylabel("Number of citations")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

#%% Visualisations 2

# =========================
# Wczytanie danych
# =========================
file_path = "data/literary_journals_scopus_with_omid_citations_counted.xlsx"
df = pd.read_excel(file_path)

df["citations counted"] = pd.to_numeric(df["citations counted"], errors="coerce").fillna(0)

# =========================
# Klasyfikacja
# =========================
def classify(row):
    if row["in OpenCitations"] == False:
        return "no data in OpenCitations"
    elif row["citations counted"] == 0:
        return "in OpenCitations but 0 citations"
    else:
        return "cited in OpenCitations"

df["category"] = df.apply(classify, axis=1)

# =========================
# Sortowanie
# =========================
df_sorted = df.sort_values("citations counted", ascending=False).reset_index(drop=True)

# =========================
# Kolory
# =========================
color_map = {
    "no data in OpenCitations": "lightgray",
    "in OpenCitations but 0 citations": "red",
    "cited in OpenCitations": "blue"
}

colors = df_sorted["category"].map(color_map)

# podział
df_zero = df[df["citations counted"] == 0]
df_nonzero = df[df["citations counted"] > 0]

# ===== Panel 1: TOP + rozkład dodatni
plt.figure(figsize=(12, 6))
df_nonzero_sorted = df_nonzero.sort_values("citations counted", ascending=False)

plt.bar(df_nonzero_sorted["Tytuł"], df_nonzero_sorted["citations counted"])
plt.title("Journals with citations (>0)")
plt.xlabel("Journal title")
plt.ylabel("Citations")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# ===== Panel 2: same zera (KLUCZOWE)
zero_counts = df_zero["category"].value_counts()

plt.figure(figsize=(6, 6))
plt.bar(zero_counts.index, zero_counts.values)

plt.title("Journals with 0 citations (by type)")
plt.ylabel("Count")
plt.xticks(rotation=30)
plt.tight_layout()
plt.show()

#%% # all articles for the venue -- meta csv -- select venue column based on ID, keep all the rows related
file_path = "data/literary_journals_scopus_with_omid_citations_counted.xlsx"
df = pd.read_excel(file_path)
# omids = [e for e in df['OMID'].to_list() if pd.notna(e)]
# omids = ['br/0607826441', 'br/0606841622', 'br/06011671914', 'br/06901277869']
omid_title_dict = dict(zip(df['OMID'].to_list(), df['Tytuł'].to_list()))

tar_path = r"data\OpenCitations\Meta_08.04.2026.tar"

filtered_parts = []
omids_set = set(omids)

with tarfile.open(tar_path, "r") as tar:
    for member in tqdm(tar.getmembers()):
        if not (member.isfile() and member.name.startswith("output_csv_2026_01_14/") and member.name.endswith(".csv")):
            continue

        f = tar.extractfile(member)
        if f is None:
            continue

        df_iter = pd.read_csv(f)
        df_iter["omid"] = (
            df_iter["venue"]
            .astype("string")
            .str.extract(r"omid:(br/\d+)", expand=False)
            )
        df_filtered = df_iter[df_iter["omid"].isin(omids_set)]

        if not df_filtered.empty:
            filtered_parts.append(df_filtered)

df_final = pd.concat(filtered_parts, ignore_index=True) if filtered_parts else pd.DataFrame()

df_final.to_excel('data/articles_of_literary_journals_scopus.xlsx', index=False)
                
# df_iter.to_excel('data/venue_error.xlsx', index=False)

#%%

df = pd.read_excel("data/literary_journals_scopus_with_omid_citations_counted.xlsx")
df_final = pd.read_excel('data/articles_of_literary_journals_scopus.xlsx')

articles_counted = dict(Counter(df_final['omid'].to_list()))
df['articles_counter'] = df['OMID'].apply(lambda x: articles_counted.get(x))
df['citation_article_ratio'] = df[['citations counted', 'articles_counter']].apply(lambda x: x['citations counted']/x['articles_counter'], axis=1)

df.to_excel('data/literary_journals_scopus_opencitation_final.xlsx', index=False)



















#%% OpenCitations Meta

tar_path = r"C:\Users\pracownik\Downloads\OpenCitations\Meta_08.04.2026.tar"

with tarfile.open(tar_path, "r") as tar:
    for member in tar.getmembers():
        # tylko zwykłe pliki CSV z danego katalogu
        if member.isfile() and member.name.startswith("output_csv_2026_01_14/") and member.name.endswith(".csv"):
            print(f"Czytam: {member.name}")
            
            f = tar.extractfile(member)
            if f is not None:
                df = pd.read_csv(f)
                print(df.head())
        else: print('error')
        
        
text_counter = 0

headers = ['venue-name', 'venue-ids', 'type', 'issn', 'issn_count']

with open("data/opencitations_metadata.csv", "w", newline="", encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    
    for file in tqdm(files):
        # file = files[0]
        df = pd.read_csv(file, low_memory=False)
        
        no_of_texts = df.shape[0]
        text_counter += no_of_texts
        
        v_iteration = list(zip(df['venue'].to_list(), df['type'].to_list()))
        #v_p_iteration = list(zip(df['venue'].to_list(), df['publisher'].to_list(), df['type'].to_list()))
        
        temp_list = []
        for v, t in v_iteration:
            # v, p = v_p_iteration[0]
            v_name = v.split('[')[0].strip() if pd.notnull(v) else None
            v_ids = v.replace(v_name, '').strip() if pd.notnull(v) else None
            if pd.notnull(v) and 'issn' in v:
                v_issn = re.findall('(?>issn\:)(.{4}-.{4})', v_ids)
                if v_issn:
                    v_issn = v_issn[0]
                else: v_issn = None
            else: v_issn = None
            if v_issn:
                temp_dict = {'venue-name': v_name,
                             'venue-ids': v_ids,
                             'type': t,
                             'issn': v_issn}
                
                temp_list.append(temp_dict)
        issn_counter = Counter(d.get('issn') for d in temp_list)
        for d in temp_list:
            d['issn_count'] = issn_counter.get(d.get('issn'))
        dict_set = [dict(s) for s in set(frozenset(d.items()) for d in temp_list)]
        for ds in dict_set:
            writer.writerow(ds)
            
            

#%% OpenCitations Index

outer_zip_path = "archiwum_nadrzedne.zip"

with zipfile.ZipFile(outer_zip_path, "r") as outer_zip:
    # iteracja po plikach w nadrzędnym ZIP-ie
    for inner_name in outer_zip.namelist():
        if inner_name.endswith(".zip"):
            print(f"Otwieram zagnieżdżone archiwum: {inner_name}")
            
            # wczytanie zagnieżdżonego ZIP-a do pamięci
            inner_bytes = outer_zip.read(inner_name)
            
            with zipfile.ZipFile(io.BytesIO(inner_bytes), "r") as inner_zip:
                # iteracja po plikach CSV w zagnieżdżonym ZIP-ie
                for csv_name in inner_zip.namelist():
                    if csv_name.endswith(".csv"):
                        print(f"  Czytam CSV: {csv_name}")
                        
                        with inner_zip.open(csv_name) as csv_file:
                            df = pd.read_csv(csv_file)
                            print(df.head())






































#%% chatgpt notes
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# KONFIGURACJA
# =========================

META_API_BASE = "https://api.opencitations.net/meta/v1"
INDEX_API_BASE = "https://api.opencitations.net/index/v2"

# Uzupełnij, jeśli masz token OpenCitations.
# Jeśli nie masz tokenu, zostaw None – zapytania też mogą działać, ale token jest zalecany.
OPENCITATIONS_TOKEN: Optional[str] = None

# Ograniczenie bezpieczeństwa względem oficjalnego limitu 180 req/min.
# 0.4 s => 150 req/min, więc bezpiecznie poniżej limitu.
REQUEST_SLEEP_SECONDS = 0.4

# Timeout pojedynczego requestu
REQUEST_TIMEOUT = 30


# =========================
# NARZĘDZIA POMOCNICZE
# =========================

def normalize_doi(doi: str) -> str:
    """Czyści DOI z typowych prefiksów URL i białych znaków."""
    if doi is None:
        return ""
    doi = doi.strip()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    doi = doi.replace("doi:", "")
    return doi.strip()


def normalize_issn(issn: str) -> str:
    """Czyści ISSN z białych znaków i prefiksu 'issn:'."""
    if issn is None:
        return ""
    issn = issn.strip().lower().replace("issn:", "").strip()
    return issn.upper()


def make_session() -> requests.Session:
    """Tworzy sesję requests z retry dla błędów chwilowych."""
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    headers = {
        "User-Agent": "OpenCitations journal/article pipeline (Python requests)"
    }
    if OPENCITATIONS_TOKEN:
        headers["authorization"] = OPENCITATIONS_TOKEN

    session.headers.update(headers)
    return session


@dataclass
class OpenCitationsClient:
    session: requests.Session
    meta_base: str = META_API_BASE
    index_base: str = INDEX_API_BASE
    sleep_seconds: float = REQUEST_SLEEP_SECONDS
    timeout: int = REQUEST_TIMEOUT

    def _get_json(self, url: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        """Wysyła GET i zwraca JSON jako listę słowników."""
        response = self.session.get(url, params=params, timeout=self.timeout)

        # Nawet po retry może wrócić błąd; wtedy zwracamy pustą listę,
        # a szczegóły można ewentualnie logować.
        if not response.ok:
            return []

        try:
            data = response.json()
        except ValueError:
            return []

        time.sleep(self.sleep_seconds)

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []

    # ---------- META API ----------

    def metadata_by_doi(self, doi: str) -> list[dict[str, Any]]:
        doi = normalize_doi(doi)
        if not doi:
            return []
        url = f"{self.meta_base}/metadata/doi:{doi}"
        return self._get_json(url)

    def metadata_by_issn(self, issn: str) -> list[dict[str, Any]]:
        issn = normalize_issn(issn)
        if not issn:
            return []
        url = f"{self.meta_base}/metadata/issn:{issn}"
        return self._get_json(url)

    # ---------- INDEX API ----------

    def citation_count_by_doi(self, doi: str) -> Optional[int]:
        doi = normalize_doi(doi)
        if not doi:
            return None
        url = f"{self.index_base}/citation-count/doi:{doi}"
        data = self._get_json(url)
        if data and "count" in data[0]:
            try:
                return int(data[0]["count"])
            except (TypeError, ValueError):
                return None
        return None

    def reference_count_by_doi(self, doi: str) -> Optional[int]:
        doi = normalize_doi(doi)
        if not doi:
            return None
        url = f"{self.index_base}/reference-count/doi:{doi}"
        data = self._get_json(url)
        if data and "count" in data[0]:
            try:
                return int(data[0]["count"])
            except (TypeError, ValueError):
                return None
        return None

    def citations_by_doi(self, doi: str) -> list[dict[str, Any]]:
        doi = normalize_doi(doi)
        if not doi:
            return []
        url = f"{self.index_base}/citations/doi:{doi}"
        return self._get_json(url)

    def references_by_doi(self, doi: str) -> list[dict[str, Any]]:
        doi = normalize_doi(doi)
        if not doi:
            return []
        url = f"{self.index_base}/references/doi:{doi}"
        return self._get_json(url)

    def venue_citation_count_by_issn(self, issn: str) -> Optional[int]:
        issn = normalize_issn(issn)
        if not issn:
            return None
        url = f"{self.index_base}/venue-citation-count/issn:{issn}"
        data = self._get_json(url)
        if data and "count" in data[0]:
            try:
                return int(data[0]["count"])
            except (TypeError, ValueError):
                return None
        return None


# =========================
# PARSOWANIE METADANYCH
# =========================

def parse_meta_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Ujednolica podstawowe pola zwracane przez Meta API.
    Oficjalnie Meta /metadata zwraca m.in.:
    id, title, author, pub_date, venue, volume, issue, page, type, publisher, editor.
    """
    return {
        "id": record.get("id"),
        "title": record.get("title"),
        "author": record.get("author"),
        "pub_date": record.get("pub_date"),
        "venue": record.get("venue"),
        "volume": record.get("volume"),
        "issue": record.get("issue"),
        "page": record.get("page"),
        "type": record.get("type"),
        "publisher": record.get("publisher"),
        "editor": record.get("editor"),
    }


def best_meta_record(records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Jeśli API zwraca kilka rekordów, wybieramy pierwszy.
    W razie potrzeby można tu później dodać bardziej wyrafinowaną logikę.
    """
    if not records:
        return {}
    return parse_meta_record(records[0])


# =========================
# PIPELINE: CZASOPISMA
# =========================

def journal_pipeline(
    issns: Iterable[str],
    client: OpenCitationsClient,
) -> pd.DataFrame:
    """
    Dla listy ISSN buduje tabelę z metadanymi venue i liczbą cytowań venue.
    """
    results: list[dict[str, Any]] = []

    for raw_issn in issns:
        issn = normalize_issn(raw_issn)

        meta_records = client.metadata_by_issn(issn)
        meta = best_meta_record(meta_records)

        venue_citation_count = client.venue_citation_count_by_issn(issn)

        row = {
            "query_issn": issn,
            "found_in_meta": bool(meta_records),
            "meta_records_n": len(meta_records),
            "venue_citation_count": venue_citation_count,
            **meta,
        }
        results.append(row)

    df = pd.DataFrame(results)

    # Drobne uporządkowanie kolumn
    preferred_cols = [
        "query_issn",
        "found_in_meta",
        "meta_records_n",
        "venue_citation_count",
        "title",
        "venue",
        "type",
        "publisher",
        "id",
        "pub_date",
        "volume",
        "issue",
        "page",
        "author",
        "editor",
    ]
    ordered_cols = [c for c in preferred_cols if c in df.columns] + [
        c for c in df.columns if c not in preferred_cols
    ]
    return df[ordered_cols]


# =========================
# PIPELINE: ARTYKUŁY
# =========================

def article_pipeline(
    dois: Iterable[str],
    client: OpenCitationsClient,
    fetch_full_lists: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Dla listy DOI buduje:
    1) tabelę zbiorczą artykułów,
    2) tabelę wszystkich incoming citations,
    3) tabelę wszystkich outgoing references.

    Jeśli fetch_full_lists=False, drugie i trzecie DataFrame będą puste.
    """
    article_rows: list[dict[str, Any]] = []
    all_citations_rows: list[dict[str, Any]] = []
    all_references_rows: list[dict[str, Any]] = []

    for raw_doi in dois:
        doi = normalize_doi(raw_doi)

        meta_records = client.metadata_by_doi(doi)
        meta = best_meta_record(meta_records)

        citation_count = client.citation_count_by_doi(doi)
        reference_count = client.reference_count_by_doi(doi)

        article_rows.append({
            "query_doi": doi,
            "found_in_meta": bool(meta_records),
            "meta_records_n": len(meta_records),
            "citation_count": citation_count,
            "reference_count": reference_count,
            **meta,
        })

        if fetch_full_lists:
            citations = client.citations_by_doi(doi)
            references = client.references_by_doi(doi)

            for row in citations:
                row_copy = dict(row)
                row_copy["query_doi"] = doi
                all_citations_rows.append(row_copy)

            for row in references:
                row_copy = dict(row)
                row_copy["query_doi"] = doi
                all_references_rows.append(row_copy)

    df_articles = pd.DataFrame(article_rows)
    df_citations = pd.DataFrame(all_citations_rows)
    df_references = pd.DataFrame(all_references_rows)

    preferred_cols = [
        "query_doi",
        "found_in_meta",
        "meta_records_n",
        "citation_count",
        "reference_count",
        "title",
        "venue",
        "type",
        "publisher",
        "id",
        "pub_date",
        "volume",
        "issue",
        "page",
        "author",
        "editor",
    ]
    if not df_articles.empty:
        ordered_cols = [c for c in preferred_cols if c in df_articles.columns] + [
            c for c in df_articles.columns if c not in preferred_cols
        ]
        df_articles = df_articles[ordered_cols]

    return df_articles, df_citations, df_references


# =========================
# ZAPIS WYNIKÓW
# =========================

def save_results_to_excel(
    journal_df: pd.DataFrame,
    article_df: pd.DataFrame,
    citations_df: pd.DataFrame,
    references_df: pd.DataFrame,
    output_path: str = "opencitations_results.xlsx",
) -> None:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        journal_df.to_excel(writer, sheet_name="journals", index=False)
        article_df.to_excel(writer, sheet_name="articles", index=False)
        citations_df.to_excel(writer, sheet_name="article_citations", index=False)
        references_df.to_excel(writer, sheet_name="article_references", index=False)


# =========================
# PRZYKŁADOWE UŻYCIE
# =========================

if __name__ == "__main__":
    # Przykładowe wejście:
    journal_issns = [
        "0138-9130",
        "1234-5678",
    ]

    article_dois = [
        "10.1108/JD-12-2013-0166",
        "10.7717/peerj-cs.421",
    ]

    session = make_session()
    client = OpenCitationsClient(session=session)

    journals_df = journal_pipeline(journal_issns, client)
    articles_df, citations_df, references_df = article_pipeline(
        article_dois,
        client,
        fetch_full_lists=True,
    )

    print("\n=== JOURNALS ===")
    print(journals_df.head())

    print("\n=== ARTICLES ===")
    print(articles_df.head())

    print("\n=== CITATIONS ===")
    print(citations_df.head())

    print("\n=== REFERENCES ===")
    print(references_df.head())

    save_results_to_excel(
        journal_df=journals_df,
        article_df=articles_df,
        citations_df=citations_df,
        references_df=references_df,
        output_path="opencitations_results.xlsx",
    )

    print("\nGotowe: zapisano plik opencitations_results.xlsx")