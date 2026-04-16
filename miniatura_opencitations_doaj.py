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

df = pd.read_csv('data/doaj_journalcsv_20260319_2320_utf8.csv')
df = df[['Journal title', 'URL in DOAJ', 'Journal ISSN (print version)', 'Journal EISSN (online version)', 'Keywords', 'Country of publisher']]

df_lit_pl = df.loc[(df['Country of publisher'] == 'Poland') &
                   (df['Keywords'].str.contains('lit'))]

df_lit_pl = df_lit_pl.rename(columns={'Journal ISSN (print version)':'ISSN', 'Journal EISSN (online version)':'e-ISSN'})


#2 Harvest OMID of the venue based on ISSN
issns_list = [e for e in df_lit_pl['ISSN'].to_list() + df_lit_pl['e-ISSN'].to_list() if pd.notnull(e)]
issns_list = list(dict.fromkeys(issns_list))  # usuwa duplikaty, zachowuje kolejność

issn_omid_dict = {}

MIN_INTERVAL = 0.36
last_request_time = 0
max_retries = 5

for issn in tqdm(issns_list):
    # pilnowanie limitu requestów
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

df_lit_pl['OMID for ISSN'] = df_lit_pl['ISSN'].apply(lambda x: issn_omid_dict.get(x))
df_lit_pl['OMID for e-ISSN'] = df_lit_pl['e-ISSN'].apply(lambda x: issn_omid_dict.get(x))
df_lit_pl['OMIDs identical'] = df_lit_pl[['OMID for ISSN', 'OMID for e-ISSN']].apply(lambda x: x['OMID for ISSN'] == x['OMID for e-ISSN'] if pd.notna(x['OMID for ISSN']) and pd.notna(x['OMID for e-ISSN']) else '', axis=1)
df_lit_pl['OMID'] = df_lit_pl[['OMID for ISSN', 'OMID for e-ISSN']].apply(lambda x: x['OMID for ISSN'] if pd.notna(x['OMID for ISSN']) else x['OMID for e-ISSN'] if pd.notna(x['OMID for e-ISSN']) else '', axis=1)
df_lit_pl['in OpenCitations'] = df_lit_pl['OMID'].apply(lambda x: True if pd.notnull(x) and x!='' else False)

df_lit_pl.to_excel('data/literary_journals_doaj_with_omid.xlsx', index=False)

# 3 Count of citations for a venue based on issn
df_lit_pl = pd.read_excel('data/literary_journals_doaj_with_omid.xlsx')
df_omid = df_lit_pl.loc[df_lit_pl['OMID'].notna()]
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

df_lit_pl['citations counted'] = df_lit_pl[['ISSN', 'e-ISSN']].apply(lambda x: issn_citations_dict.get(x['ISSN'], issn_citations_dict.get(x['e-ISSN'], 0)), axis=1)

df_lit_pl.to_excel('data/literary_journals_doaj_with_omid_citations_counted.xlsx', index=False)

#%% Visualisations

# =========================
# 1. Wczytanie danych
# =========================
file_path = "data/literary_journals_doaj_with_omid_citations_counted.xlsx"
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
df_citations = df[["Journal title", "citations counted"]].copy()
df_citations = df_citations.sort_values("citations counted", ascending=False)

plt.figure(figsize=(14, 8))
plt.bar(df_citations["Journal title"], df_citations["citations counted"])
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
file_path = "data/literary_journals_doaj_with_omid_citations_counted.xlsx"
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

plt.bar(df_nonzero_sorted["Journal title"], df_nonzero_sorted["citations counted"])
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






























