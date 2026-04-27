import requests
import pandas as pd
from tqdm import tqdm
from scopus_api import scopus_api_key
import time
from collections import Counter

#%% scopus api

headers = {
    "X-ELS-APIKey": scopus_api_key,
    "Accept": "application/json"
}

BASE_URL = "https://api.elsevier.com/content/search/scopus"

# konserwatywnie: 1 request co 0.25 s = 4 req/s
sleep_between_requests = 0.25

# ile rekordów na stronę
page_size = 25

# liczba ponowień przy błędzie
max_retries = 5

#%% issn set
df = pd.read_excel('data/literary_journals_opencitations.xlsx')
issns_list = [e for e in df['ISSN'].to_list() + df['e-ISSN'].to_list() if pd.notnull(e)]
issns_set = set(issns_list)
#%% def for single request

def scopus_request(query, start=0, count=25, view="STANDARD"):
    params = {
        "query": query,
        "start": start,
        "count": count,
        "view": view
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_time = float(retry_after) if retry_after else 2 ** (attempt + 1)
                print(f"429 Too Many Requests. Czekam {wait_time} s.")
                time.sleep(wait_time)
            
            elif response.status_code in [500, 502, 503, 504]:
                wait_time = 2 ** (attempt + 1)
                print(f"{response.status_code} Server error. Czekam {wait_time} s.")
                time.sleep(wait_time)
            
            else:
                print(f"Błąd {response.status_code}: {response.text[:500]}")
                return None
        
        except Exception as e:
            wait_time = 2 ** (attempt + 1)
            print(f"Exception: {e}. Czekam {wait_time} s.")
            time.sleep(wait_time)
    
    return None

#%% test query scopus
issn = "1641-9278"
query = f"ISSN({issn}) AND DOCTYPE(ar)"

first_result = scopus_request(query=query, start=0, count=page_size)

total_results = int(first_result["search-results"]["opensearch:totalResults"])
#%% query scopus
scopus_article_counter = {}
all_rows = []

for issn in tqdm(issns_set):
    # issn = "1641-9278"
    query = f"ISSN({issn}) AND DOCTYPE(ar)"
    
    first_result = scopus_request(query=query, start=0, count=page_size)
    
    time.sleep(sleep_between_requests)
    
    if first_result is None:
        all_rows.append({
            "issn_query": issn,
            "eid": None,
            "title": None,
            "doi": None,
            "publication_name": None,
            "cover_date": None,
            "issn_returned": None,
            "eissn_returned": None,
            "citedby_count": None
        })
        continue
    
    total_results = int(first_result["search-results"]["opensearch:totalResults"])
    scopus_article_counter.update({issn:total_results})
    
    entries = first_result["search-results"].get("entry", [])
    
    if not entries:
        all_rows.append({
            "issn_query": issn,
            "eid": None,
            "title": None,
            "doi": None,
            "publication_name": None,
            "cover_date": None,
            "issn_returned": None,
            "eissn_returned": None,
            "citedby_count": None
        })
        continue
    
    for e in entries:
        all_rows.append({
            "issn_query": issn,
            "eid": e.get("eid"),
            "title": e.get("dc:title"),
            "doi": e.get("prism:doi"),
            "publication_name": e.get("prism:publicationName"),
            "cover_date": e.get("prism:coverDate"),
            "issn_returned": e.get("prism:issn"),
            "eissn_returned": e.get("prism:eIssn"),
            "citedby_count": pd.to_numeric(e.get("citedby-count"), errors="coerce")
        })
    
    for start in range(page_size, total_results, page_size):
        result = scopus_request(query=query, start=start, count=page_size)
        time.sleep(sleep_between_requests)
        
        if result is None:
            continue
        
        entries = result["search-results"].get("entry", [])
        
        for e in entries:
            all_rows.append({
                "issn_query": issn,
                "eid": e.get("eid"),
                "title": e.get("dc:title"),
                "doi": e.get("prism:doi"),
                "publication_name": e.get("prism:publicationName"),
                "cover_date": e.get("prism:coverDate"),
                "issn_returned": e.get("prism:issn"),
                "eissn_returned": e.get("prism:eIssn"),
                "citedby_count": pd.to_numeric(e.get("citedby-count"), errors="coerce")
            })
            
df_scopus = pd.DataFrame(all_rows)
df_scopus = df_scopus[df_scopus['eid'].notna()].drop(columns='issn_query').drop_duplicates()

df = pd.read_excel("data/literary_journals_opencitations.xlsx")
issn_to_id = dict(zip(df['ISSN'], df['internal_id']))
eissn_to_id = dict(zip(df['e-ISSN'], df['internal_id']))
issn_to_id = issn_to_id | eissn_to_id

df_scopus['venue_internal_id'] = df_scopus[['issn_returned', 'eissn_returned']].apply(lambda x: issn_to_id.get(f"{x['issn_returned'][:4]}-{x['issn_returned'][4:]}") if pd.notna(x['issn_returned']) else issn_to_id.get(f"{x['eissn_returned'][:4]}-{x['eissn_returned'][4:]}") if pd.notna(x['eissn_returned']) else None, axis=1)

df_scopus.to_excel('data/articles_of_literary_journals_scopus.xlsx', index=False)

scopus_venue_article_counter = dict(Counter(df_scopus['venue_internal_id']))

df['scopus_articles_counted'] = df['internal_id'].apply(lambda x: scopus_venue_article_counter.get(x))

scopus_venue_citation_counter = (
        df_scopus.groupby("venue_internal_id")["citedby_count"]
        .sum()
        .astype(int)
        .to_dict()
)

df['scopus_citations_counted'] = df['internal_id'].apply(lambda x: scopus_venue_citation_counter.get(x))
df['scopus_citation_article_ratio'] = df[['scopus_citations_counted', 'scopus_articles_counted']].apply(lambda x: x['scopus_citations_counted']/x['scopus_articles_counted'], axis=1)

df["oc_more_articles"] = (
    df["oc_articles_counted"].fillna(-1) > df["scopus_articles_counted"].fillna(-1)
)
df["oc_more_citations"] = (
    df["oc_citations_counted"].fillna(-1) > df["scopus_articles_counted"].fillna(-1)
)

df.to_excel('data/literary_journals_opencitations_scopus.xlsx', index=False)
























