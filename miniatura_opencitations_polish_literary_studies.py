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
from functools import partial
import zipfile
import io
import tarfile
import time
import matplotlib.pyplot as plt
import numpy as np
import ast
from collections import defaultdict
from pathlib import Path
import sys
sys.path.insert(1, r'C:\Users\pracownik\Documents\IBL-PAN-Python')
from my_functions import gsheet_to_df

headers = {"authorization": oc_token}
#%% Pipeline
#1 Read file with journals information

df = pd.read_excel('data/czasopisma_literaturoznawcze.xlsx')

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
                ids = [e.get('id') for e in response_json]
                omids = sorted([elem.replace('omid:', '') for elem in [ele for sub in [[el for el in e.split(' ')] for e in ids] for ele in sub] if 'omid:' in elem])

                if omids:
                    issn_omid_dict.update({issn: omids})
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
# df['OMIDs identical'] = df[['OMID for ISSN', 'OMID for e-ISSN']].apply(lambda x: x['OMID for ISSN'] == x['OMID for e-ISSN'] if pd.notna(x['OMID for ISSN']) and pd.notna(x['OMID for e-ISSN']) else '', axis=1)
# df['OMID'] = df[['OMID for ISSN', 'OMID for e-ISSN']].apply(lambda x: x['OMID for ISSN'] if pd.notna(x['OMID for ISSN']) else x['OMID for e-ISSN'] if pd.notna(x['OMID for e-ISSN']) else '', axis=1)
df["OMID"] = df.apply(
    lambda x: sorted(set((x["OMID for ISSN"] or []) + (x["OMID for e-ISSN"] or [])))
    if (x["OMID for ISSN"] is not None or x["OMID for e-ISSN"] is not None)
    else None,
    axis=1
)
# df['in OpenCitations'] = df['OMID'].apply(lambda x: True if pd.notnull(x) and x!='' else False)
df['in OpenCitations'] = df['OMID'].apply(lambda x: bool(x))

df.to_excel('data/literary_journals_opencitations.xlsx', index=False)

# 3 Count of citations for a venue based on issn
# df = pd.read_excel('data/literary_journals_opencitations.xlsx')
# df_omid = df.loc[df['OMID'].notna()]
# issns_list = [e for e in df_omid['ISSN'].to_list() + df_omid['e-ISSN'].to_list() if pd.notnull(e)]
# issns_list = list(dict.fromkeys(issns_list))

# issn_citations_dict = {}

# MIN_INTERVAL = 0.36
# last_request_time = 0
# max_retries = 5

# for issn in tqdm(issns_list):
#     # issn = issns_list[0]
#     # pilnowanie limitu requestów
#     elapsed = time.monotonic() - last_request_time
#     if elapsed < MIN_INTERVAL:
#         time.sleep(MIN_INTERVAL - elapsed)

#     url = f'https://api.opencitations.net/index/v2/venue-citation-count/issn:{issn}'

#     for attempt in range(max_retries):
#         try:
#             response = requests.get(url, headers=headers, timeout=60)
#             last_request_time = time.monotonic()

#             if response.status_code == 429:
#                 retry_after = response.headers.get("Retry-After")
#                 wait_time = float(retry_after) if retry_after else 5 * (attempt + 1)
#                 time.sleep(wait_time)
#                 continue

#             response.raise_for_status()
#             response_json = response.json()

#             if response_json:
#                 citations_counted = int(response_json[0].get('count'))
#                 issn_citations_dict.update({issn: citations_counted})

#             else:
#                 issn_citations_dict.update({issn: None})

#             break

#         except requests.RequestException as e:
#             if attempt == max_retries - 1:
#                 print(f'Błąd dla {issn}: {e}')
#                 issn_citations_dict.update({issn: None})
#             else:
#                 time.sleep(3 * (attempt + 1))

# df['citations counted'] = df[['ISSN', 'e-ISSN']].apply(lambda x: issn_citations_dict.get(x['ISSN'], issn_citations_dict.get(x['e-ISSN'], 0)), axis=1)

# df.to_excel('data/literary_journals_opencitations.xlsx', index=False)



#%% # all articles for the venue -- meta csv -- select venue column based on ID, keep all the rows related
file_path = "data/literary_journals_opencitations.xlsx"
df = pd.read_excel(file_path)
df_omid = df.loc[df['OMID'].notna()]
df_omid['OMID'] = df_omid['OMID'].apply(
       lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)

omids = sorted(set([el for sub in [e for e in df_omid['OMID'].to_list()] for el in sub]))
# omids = ['br/0607826441', 'br/0606841622', 'br/06011671914', 'br/06901277869']
df_exploded = df_omid.explode('OMID')
omid_title_dict = dict(zip(df_exploded['OMID'], df_exploded['Tytuł']))
# omid_title_dict = dict(zip(df['OMID'].to_list(), df['Tytuł'].to_list()))

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
        df_iter["venue_omid"] = (
            df_iter["venue"]
            .astype("string")
            .str.extract(r"omid:(br/\d+)", expand=False)
            )
        df_filtered = df_iter[df_iter["venue_omid"].isin(omids_set)]

        if not df_filtered.empty:
            filtered_parts.append(df_filtered)

df_articles = pd.concat(filtered_parts, ignore_index=True) if filtered_parts else pd.DataFrame()
df_articles['venue_name'] = df_articles['venue_omid'].apply(lambda x: omid_title_dict.get(x))

omid_issn_dict = defaultdict(list)

for key, omid_list in issn_omid_dict.items():
    if isinstance(omid_list, list):
        for omid in omid_list:
            if omid and key not in omid_issn_dict[omid]:
                omid_issn_dict[omid].append(key)
omid_issn_dict = dict(omid_issn_dict)

df_articles['issn'] = df_articles['venue_omid'].apply(lambda x: omid_issn_dict.get(x))

# porównanie venue & publisher

# venue_publisher = [(v, p.split(' [')[0]) for v,p in list(zip(df_articles['venue_name'], df_articles['publisher']))]

# venue_publisher_unique = set(venue_publisher)
# venue_publisher_count = dict(Counter(venue_publisher))

# df_vp = pd.DataFrame(
#     [
#         {"journal": k[0], "publisher": k[1], "count": v}
#         for k, v in venue_publisher_count.items()
#     ]
# )

# # opcjonalnie sortowanie
# df_vp = df_vp.sort_values(by="count", ascending=False)

# df_vp.to_excel('data/literary_journal_articles_opencitations_venue_publisher.xlsx', index=False)

df_vp = gsheet_to_df('17BXcIB9lnwF16qpUBUH1yYFN7nV82lUZM2pkWKPDnmU', 'Sheet1')
df_vp = df_vp.loc[df_vp['false_info'].isna()]

df_articles['publisher_name'] = df_articles['publisher'].apply(lambda x: x.split(' [')[0].strip())

vp_correct = [(v.strip(), p.strip()) for v,p in zip(df_vp['journal'],df_vp['publisher'])]

df_articles = df_articles[df_articles.set_index(["venue_name", "publisher_name"]).index.isin(vp_correct)]

df_articles.to_excel('data/literary_journal_articles_opencitations.xlsx', index=False)
                
#%% opencitations citations of the article

df_articles = pd.read_excel('data/literary_journal_articles_opencitations.xlsx')
df_articles['volume'] = (
    df_articles['volume']
    .astype(str)
    .str.strip()
    .replace({'nan': None, 'None': None})
)

df_articles_duplicates = df_articles[df_articles.duplicated(keep=False)]

df_articles_duplicates.to_excel(
    'data/literary_journal_articles_opencitations_duplicates.xlsx',
    index=False
)

df_articles = df_articles.drop_duplicates()

article_omids = [[el.replace('omid:', '') for el in e.split(' ') if 'omid:' in el][0] for e in df_articles['id'].to_list()]

df_articles['article_omid'] = article_omids

article_omids_set = set([f'omid:{e}' for e in article_omids])

base_dir = Path("data/OpenCitations/Index_09.04.2026_unzipped")
csv_files = list(base_dir.rglob("*.csv"))

usecols = ["id", "citing", "cited"]
dtype_map = {
    "id": "string",
    "citing": "string",
    "cited": "string"
}

def process_csv(csv_path):
    df_iter = pd.read_csv(csv_path, usecols=usecols, dtype=dtype_map)
    df_filtered = df_iter[df_iter["cited"].isin(article_omids_set)]
    return df_filtered if not df_filtered.empty else None

with ThreadPoolExecutor(max_workers=8) as executor:
    results = list(tqdm(executor.map(process_csv, csv_files), total=len(csv_files)))

article_citations = [df for df in results if df is not None]

if article_citations:
    article_citations_df = pd.concat(article_citations, ignore_index=True)
else:
    article_citations_df = pd.DataFrame(columns=usecols)

df_citations = pd.concat(article_citations, ignore_index=True) if article_citations else pd.DataFrame()

# usunięcie citing = cited
wrong_citations = df_citations[df_citations['citing'] == df_citations['cited']]

wrong_citations.to_excel(
    'data/citations_of_literary_journal_articles_opencitations_self-citations.xlsx',
    index=False
)

df_citations = df_citations[df_citations['citing'] != df_citations['cited']]



#%% opencitations article counter and citation-article ratio
#counting preparation

df['internal_id'] = range(1, len(df) + 1)

df['OMID'] = df_omid['OMID'].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)

df_exploded = df.explode('OMID')
omid_internal_id_dict = {k:v for k,v in dict(zip(df_exploded['OMID'], df_exploded['internal_id'])).items() if pd.notna(k)}

df_articles['venue_internal_id'] = df_articles['venue_omid'].apply(lambda x: omid_internal_id_dict.get(x))

#citations counted -- article level

article_omids = [[el.replace('omid:', '') for el in e.split(' ') if 'omid:' in el][0] for e in df_citations['cited'].to_list()]

df_citations['article_omid'] = article_omids
df_citations.to_excel('data/citations_of_literary_journal_articles_opencitations.xlsx', index=False)

citations_counted = dict(Counter(df_citations['article_omid'].to_list()))

df_articles['citedby_count'] = df_articles['article_omid'].apply(lambda x: citations_counted.get(x))

df_articles.to_excel('data/literary_journal_articles_opencitations.xlsx', index=False)

#articles counted and citations counted -- venue level

df_agg = (
    df_articles.groupby("venue_internal_id")
    .agg(
        total_citations=("citedby_count", "sum"),
        articles_count=("article_omid", "count")
    )
    .reset_index()
)

articles_counted = dict(zip(df_agg['venue_internal_id'], df_agg['articles_count']))
venue_citations = dict(zip(df_agg['venue_internal_id'], df_agg['total_citations']))

df['oc_articles_counted'] = df['internal_id'].apply(lambda x: articles_counted.get(x))
df['oc_citations_counted'] = df['internal_id'].apply(lambda x: venue_citations.get(x))
df['oc_citation_article_ratio'] = df[['oc_citations_counted', 'oc_articles_counted']].apply(lambda x: x['oc_citations_counted']/x['oc_articles_counted'], axis=1)

df.to_excel('data/literary_journals_opencitations.xlsx', index=False)






















