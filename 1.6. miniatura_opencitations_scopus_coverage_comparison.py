import re
import unicodedata
import numpy as np
import pandas as pd

from rapidfuzz import fuzz, process

# =========================================================
# PATHS
# =========================================================

oc_path = r"data/literary_journal_articles_opencitations_final.xlsx"
sc_path = r"data/articles_of_literary_journals_scopus.xlsx"
manual_review_path = r"data/oc_scopus_fuzzy_manual_review.xlsx"

out_master = r"data/oc_scopus_articles_matched_master.xlsx"
out_oc_only = r"data/oc_articles_unmatched.xlsx"
out_sc_only = r"data/scopus_articles_unmatched.xlsx"
out_manual_review_new = r"data/oc_scopus_fuzzy_manual_review_new.xlsx"

# =========================================================
# HELPERS
# =========================================================

def extract_doi_from_oc_id(x):
    """
    Extract DOI from OpenCitations 'id' field.
    Example:
    'omid:... doi:10.26485/ai/2019/21/12 openalex:...'
    """
    if pd.isna(x):
        return np.nan
    m = re.search(r"doi:([^\s]+)", str(x), flags=re.IGNORECASE)
    return m.group(1).strip() if m else np.nan


def normalize_doi(x):
    """
    Normalize DOI:
    - lower
    - remove URL prefixes
    - remove leading doi:
    - trim spaces
    """
    if pd.isna(x):
        return np.nan

    s = str(x).strip().lower()
    s = re.sub(r"^https?://(dx\.)?doi\.org/", "", s)
    s = re.sub(r"^doi:\s*", "", s)
    s = s.strip()

    return s if s else np.nan


def strip_accents(text):
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def normalize_title(x):
    """
    Conservative title normalization:
    - lower
    - remove accents
    - unify punctuation
    - remove punctuation
    - collapse spaces
    """
    if pd.isna(x):
        return np.nan

    s = str(x).strip().lower()
    s = strip_accents(s)

    s = (
        s.replace("“", '"')
         .replace("”", '"')
         .replace("‘", "'")
         .replace("’", "'")
         .replace("–", "-")
         .replace("—", "-")
         .replace("‐", "-")
    )

    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s if s else np.nan


def safe_year(x):
    dt = pd.to_datetime(x, errors="coerce")
    return dt.year if pd.notna(dt) else np.nan


# =========================================================
# LOAD DATA
# =========================================================

oc = pd.read_excel(oc_path)
sc = pd.read_excel(sc_path)

# =========================================================
# PREPARE OPENCITATIONS
# =========================================================

oc = oc.copy()

oc["doi_raw"] = oc["id"].map(extract_doi_from_oc_id)
oc["doi_norm"] = oc["doi_raw"].map(normalize_doi)
oc["title_norm"] = oc["title"].map(normalize_title)
oc["year"] = oc["pub_date"].map(safe_year)

oc = oc.rename(columns={
    "article_omid": "article_id_oc",
    "venue_name": "venue_name_oc",
    "title": "title_oc",
    "pub_date": "pub_date_oc",
    "citedby_count": "citedby_count_oc"
})

oc["citedby_count_oc"] = pd.to_numeric(oc["citedby_count_oc"], errors="coerce").fillna(0)

oc = oc[
    [
        "article_id_oc",
        "venue_internal_id",
        "venue_name_oc",
        "title_oc",
        "title_norm",
        "doi_raw",
        "doi_norm",
        "year",
        "pub_date_oc",
        "citedby_count_oc",
        "id"
    ]
].drop_duplicates()

# =========================================================
# PREPARE SCOPUS
# =========================================================

sc = sc.copy()

sc["doi_norm"] = sc["doi"].map(normalize_doi)
sc["title_norm"] = sc["title"].map(normalize_title)
sc["year"] = sc["cover_date"].map(safe_year)

sc = sc.rename(columns={
    "eid": "article_id_scopus",
    "publication_name": "venue_name_scopus",
    "title": "title_scopus",
    "cover_date": "pub_date_scopus",
    "doi": "doi_raw_scopus",
    "citedby_count": "citedby_count_scopus"
})

sc["citedby_count_scopus"] = pd.to_numeric(sc["citedby_count_scopus"], errors="coerce").fillna(0)

sc = sc[
    [
        "article_id_scopus",
        "venue_internal_id",
        "venue_name_scopus",
        "title_scopus",
        "title_norm",
        "doi_raw_scopus",
        "doi_norm",
        "year",
        "pub_date_scopus",
        "citedby_count_scopus"
    ]
].drop_duplicates()

# =========================================================
# MATCH 1: DOI EXACT
# =========================================================

doi_matches = oc.merge(
    sc,
    on=["venue_internal_id", "doi_norm"],
    how="inner",
    suffixes=("_oc", "_sc")
).copy()

doi_matches["match_method"] = "doi_exact"

doi_matches_std = pd.DataFrame({
    "article_id_oc": doi_matches["article_id_oc"],
    "article_id_scopus": doi_matches["article_id_scopus"],
    "venue_internal_id": doi_matches["venue_internal_id"],
    "venue_name_oc": doi_matches["venue_name_oc"],
    "venue_name_scopus": doi_matches["venue_name_scopus"],
    "title_oc": doi_matches["title_oc"],
    "title_scopus": doi_matches["title_scopus"],
    "title_norm_oc": doi_matches["title_norm_oc"],
    "title_norm_sc": doi_matches["title_norm_sc"],
    "doi_raw": doi_matches["doi_raw"],
    "doi_raw_scopus": doi_matches["doi_raw_scopus"],
    "doi_norm": doi_matches["doi_norm"],
    "year_oc": doi_matches["year_oc"],
    "year_sc": doi_matches["year_sc"],
    "pub_date_oc": doi_matches["pub_date_oc"],
    "pub_date_scopus": doi_matches["pub_date_scopus"],
    "citedby_count_oc": doi_matches["citedby_count_oc"],
    "citedby_count_scopus": doi_matches["citedby_count_scopus"],
    "match_method": doi_matches["match_method"],
    "fuzzy_score": np.nan
})

doi_matches_std = doi_matches_std.drop_duplicates(subset=["article_id_oc"], keep="first")
doi_matches_std = doi_matches_std.drop_duplicates(subset=["article_id_scopus"], keep="first")

matched_oc_ids = set(doi_matches_std["article_id_oc"])
matched_sc_ids = set(doi_matches_std["article_id_scopus"])

# =========================================================
# MATCH 2: TITLE EXACT + YEAR
# =========================================================

oc_left = oc[~oc["article_id_oc"].isin(matched_oc_ids)].copy()
sc_left = sc[~sc["article_id_scopus"].isin(matched_sc_ids)].copy()

title_matches = oc_left.merge(
    sc_left,
    on=["venue_internal_id", "title_norm", "year"],
    how="inner",
    suffixes=("_oc", "_sc")
).copy()

title_matches["match_method"] = "title_exact_year"

title_matches_std = pd.DataFrame({
    "article_id_oc": title_matches["article_id_oc"],
    "article_id_scopus": title_matches["article_id_scopus"],
    "venue_internal_id": title_matches["venue_internal_id"],
    "venue_name_oc": title_matches["venue_name_oc"],
    "venue_name_scopus": title_matches["venue_name_scopus"],
    "title_oc": title_matches["title_oc"],
    "title_scopus": title_matches["title_scopus"],
    "title_norm_oc": title_matches["title_norm"],
    "title_norm_sc": title_matches["title_norm"],
    "doi_raw": title_matches["doi_raw"],
    "doi_raw_scopus": title_matches["doi_raw_scopus"],
    "doi_norm": title_matches["doi_norm_oc"].combine_first(title_matches["doi_norm_sc"]),
    "year_oc": title_matches["year"],
    "year_sc": title_matches["year"],
    "pub_date_oc": title_matches["pub_date_oc"],
    "pub_date_scopus": title_matches["pub_date_scopus"],
    "citedby_count_oc": title_matches["citedby_count_oc"],
    "citedby_count_scopus": title_matches["citedby_count_scopus"],
    "match_method": title_matches["match_method"],
    "fuzzy_score": np.nan
})

title_matches_std = title_matches_std.drop_duplicates(subset=["article_id_oc"], keep="first")
title_matches_std = title_matches_std.drop_duplicates(subset=["article_id_scopus"], keep="first")

matched_oc_ids.update(title_matches_std["article_id_oc"])
matched_sc_ids.update(title_matches_std["article_id_scopus"])

# =========================================================
# MATCH 3: FUZZY TITLE + YEAR
# =========================================================

oc_left = oc[~oc["article_id_oc"].isin(matched_oc_ids)].copy()
sc_left = sc[~sc["article_id_scopus"].isin(matched_sc_ids)].copy()

FUZZY_THRESHOLD = 96
MIN_GAP_TO_SECOND = 3

fuzzy_auto_matches = []
manual_review_rows = []

common_venues = sorted(set(oc_left["venue_internal_id"].dropna()) & set(sc_left["venue_internal_id"].dropna()))

for venue_id in common_venues:
    oc_sub = oc_left[oc_left["venue_internal_id"] == venue_id].copy()
    sc_sub = sc_left[sc_left["venue_internal_id"] == venue_id].copy()

    common_years = sorted(set(oc_sub["year"].dropna()) & set(sc_sub["year"].dropna()))

    for year in common_years:
        oc_y = oc_sub[oc_sub["year"] == year].copy()
        sc_y = sc_sub[sc_sub["year"] == year].copy()

        if oc_y.empty or sc_y.empty:
            continue

        sc_choices = dict(zip(sc_y["article_id_scopus"], sc_y["title_norm"]))
        used_scopus = set()

        for _, oc_row in oc_y.iterrows():
            query = oc_row["title_norm"]

            if pd.isna(query) or not str(query).strip():
                continue

            candidates = process.extract(
                query,
                sc_choices,
                scorer=fuzz.ratio,
                limit=5
            )

            if not candidates:
                continue

            best_string, best_score, best_scopus_id = candidates[0]
            second_score = candidates[1][1] if len(candidates) > 1 else -999

            best_sc_row = sc_y.loc[sc_y["article_id_scopus"] == best_scopus_id].iloc[0]

            review_row = {
                "venue_internal_id": venue_id,
                "year": year,
                "article_id_oc": oc_row["article_id_oc"],
                "title_oc": oc_row["title_oc"],
                "doi_oc": oc_row["doi_norm"],
                "article_id_scopus": best_sc_row["article_id_scopus"],
                "title_scopus": best_sc_row["title_scopus"],
                "doi_scopus": best_sc_row["doi_norm"],
                "best_score": best_score,
                "second_score": second_score,
                "score_gap": best_score - second_score,
            }

            if (
                best_score >= FUZZY_THRESHOLD
                and (best_score - second_score) >= MIN_GAP_TO_SECOND
                and best_scopus_id not in used_scopus
            ):
                fuzzy_auto_matches.append({
                    "article_id_oc": oc_row["article_id_oc"],
                    "article_id_scopus": best_sc_row["article_id_scopus"],
                    "venue_internal_id": venue_id,
                    "venue_name_oc": oc_row["venue_name_oc"],
                    "venue_name_scopus": best_sc_row["venue_name_scopus"],
                    "title_oc": oc_row["title_oc"],
                    "title_scopus": best_sc_row["title_scopus"],
                    "title_norm_oc": oc_row["title_norm"],
                    "title_norm_sc": best_sc_row["title_norm"],
                    "doi_raw": oc_row["doi_raw"],
                    "doi_raw_scopus": best_sc_row["doi_raw_scopus"],
                    "doi_norm": oc_row["doi_norm"] if pd.notna(oc_row["doi_norm"]) else best_sc_row["doi_norm"],
                    "year_oc": oc_row["year"],
                    "year_sc": best_sc_row["year"],
                    "pub_date_oc": oc_row["pub_date_oc"],
                    "pub_date_scopus": best_sc_row["pub_date_scopus"],
                    "citedby_count_oc": oc_row["citedby_count_oc"],
                    "citedby_count_scopus": best_sc_row["citedby_count_scopus"],
                    "match_method": "title_fuzzy_year_auto",
                    "fuzzy_score": best_score
                })
                used_scopus.add(best_scopus_id)
            else:
                manual_review_rows.append(review_row)

fuzzy_matches_std = pd.DataFrame(fuzzy_auto_matches)
manual_review_df = pd.DataFrame(manual_review_rows)

if fuzzy_matches_std.empty:
    fuzzy_matches_std = pd.DataFrame(columns=doi_matches_std.columns)

matched_oc_ids.update(fuzzy_matches_std["article_id_oc"])
matched_sc_ids.update(fuzzy_matches_std["article_id_scopus"])

# =========================================================
# MATCH 4: MANUAL CONFIRMED
# =========================================================

if manual_review_path:
    manual_df = pd.read_excel(manual_review_path)

    manual_accept = manual_df[
        manual_df["the same"].astype(str).str.strip().str.lower().eq("x")
    ].copy()

    if not manual_accept.empty:
        # bierzemy tylko klucze z manual review
        manual_accept = manual_accept[["article_id_oc", "article_id_scopus"]].drop_duplicates()

        # przygotowanie jednoznacznie nazwanych tabel pomocniczych
        oc_manual = oc.rename(columns={
            "venue_internal_id": "venue_internal_id_oc",
            "venue_name_oc": "venue_name_oc_manual",
            "title_oc": "title_oc_manual",
            "title_norm": "title_norm_oc_manual",
            "doi_raw": "doi_raw_oc_manual",
            "doi_norm": "doi_norm_oc_manual",
            "year": "year_oc_manual",
            "pub_date_oc": "pub_date_oc_manual",
            "citedby_count_oc": "citedby_count_oc_manual"
        })[
            [
                "article_id_oc",
                "venue_internal_id_oc",
                "venue_name_oc_manual",
                "title_oc_manual",
                "title_norm_oc_manual",
                "doi_raw_oc_manual",
                "doi_norm_oc_manual",
                "year_oc_manual",
                "pub_date_oc_manual",
                "citedby_count_oc_manual"
            ]
        ].drop_duplicates()

        sc_manual = sc.rename(columns={
            "venue_internal_id": "venue_internal_id_sc",
            "venue_name_scopus": "venue_name_scopus_manual",
            "title_scopus": "title_scopus_manual",
            "title_norm": "title_norm_sc_manual",
            "doi_raw_scopus": "doi_raw_scopus_manual",
            "doi_norm": "doi_norm_sc_manual",
            "year": "year_sc_manual",
            "pub_date_scopus": "pub_date_scopus_manual",
            "citedby_count_scopus": "citedby_count_scopus_manual"
        })[
            [
                "article_id_scopus",
                "venue_internal_id_sc",
                "venue_name_scopus_manual",
                "title_scopus_manual",
                "title_norm_sc_manual",
                "doi_raw_scopus_manual",
                "doi_norm_sc_manual",
                "year_sc_manual",
                "pub_date_scopus_manual",
                "citedby_count_scopus_manual"
            ]
        ].drop_duplicates()

        manual_accept = manual_accept.merge(
            oc_manual,
            on="article_id_oc",
            how="left"
        ).merge(
            sc_manual,
            on="article_id_scopus",
            how="left"
        )

        manual_matches_std = pd.DataFrame({
            "article_id_oc": manual_accept["article_id_oc"],
            "article_id_scopus": manual_accept["article_id_scopus"],
            "venue_internal_id": manual_accept["venue_internal_id_oc"].combine_first(manual_accept["venue_internal_id_sc"]),
            "venue_name_oc": manual_accept["venue_name_oc_manual"],
            "venue_name_scopus": manual_accept["venue_name_scopus_manual"],
            "title_oc": manual_accept["title_oc_manual"],
            "title_scopus": manual_accept["title_scopus_manual"],
            "title_norm_oc": manual_accept["title_norm_oc_manual"],
            "title_norm_sc": manual_accept["title_norm_sc_manual"],
            "doi_raw": manual_accept["doi_raw_oc_manual"],
            "doi_raw_scopus": manual_accept["doi_raw_scopus_manual"],
            "doi_norm": manual_accept["doi_norm_oc_manual"].combine_first(manual_accept["doi_norm_sc_manual"]),
            "year_oc": manual_accept["year_oc_manual"],
            "year_sc": manual_accept["year_sc_manual"],
            "pub_date_oc": manual_accept["pub_date_oc_manual"],
            "pub_date_scopus": manual_accept["pub_date_scopus_manual"],
            "citedby_count_oc": manual_accept["citedby_count_oc_manual"],
            "citedby_count_scopus": manual_accept["citedby_count_scopus_manual"],
            "match_method": "manual_confirmed",
            "fuzzy_score": np.nan
        })

        manual_matches_std = manual_matches_std.drop_duplicates(subset=["article_id_oc"], keep="first")
        manual_matches_std = manual_matches_std.drop_duplicates(subset=["article_id_scopus"], keep="first")
    else:
        manual_matches_std = pd.DataFrame(columns=doi_matches_std.columns)
else:
    manual_matches_std = pd.DataFrame(columns=doi_matches_std.columns)

# =========================================================
# CONCAT ALL MATCHES
# =========================================================

all_matches = pd.concat(
    [doi_matches_std, title_matches_std, fuzzy_matches_std, manual_matches_std],
    ignore_index=True
)

match_priority = {
    "doi_exact": 1,
    "title_exact_year": 2,
    "manual_confirmed": 3,
    "title_fuzzy_year_auto": 4
}

all_matches["match_priority"] = all_matches["match_method"].map(match_priority).fillna(999)

all_matches = all_matches.sort_values(
    ["match_priority", "article_id_oc", "article_id_scopus"]
).drop_duplicates(subset=["article_id_oc"], keep="first")

all_matches = all_matches.sort_values(
    ["match_priority", "article_id_oc", "article_id_scopus"]
).drop_duplicates(subset=["article_id_scopus"], keep="first")

all_matches = all_matches.drop(columns="match_priority")

# =========================================================
# BUILD MASTER
# =========================================================

matched_oc_ids = set(all_matches["article_id_oc"].dropna())
matched_sc_ids = set(all_matches["article_id_scopus"].dropna())

oc_only = oc[~oc["article_id_oc"].isin(matched_oc_ids)].copy()
sc_only = sc[~sc["article_id_scopus"].isin(matched_sc_ids)].copy()

oc_only_master = pd.DataFrame({
    "article_id_oc": oc_only["article_id_oc"],
    "article_id_scopus": np.nan,
    "venue_internal_id": oc_only["venue_internal_id"],
    "venue_name_oc": oc_only["venue_name_oc"],
    "venue_name_scopus": np.nan,
    "title_oc": oc_only["title_oc"],
    "title_scopus": np.nan,
    "title_norm_oc": oc_only["title_norm"],
    "title_norm_sc": np.nan,
    "doi_raw": oc_only["doi_raw"],
    "doi_raw_scopus": np.nan,
    "doi_norm": oc_only["doi_norm"],
    "year_oc": oc_only["year"],
    "year_sc": np.nan,
    "pub_date_oc": oc_only["pub_date_oc"],
    "pub_date_scopus": np.nan,
    "citedby_count_oc": oc_only["citedby_count_oc"],
    "citedby_count_scopus": np.nan,
    "match_method": "oc_only",
    "fuzzy_score": np.nan
})

sc_only_master = pd.DataFrame({
    "article_id_oc": np.nan,
    "article_id_scopus": sc_only["article_id_scopus"],
    "venue_internal_id": sc_only["venue_internal_id"],
    "venue_name_oc": np.nan,
    "venue_name_scopus": sc_only["venue_name_scopus"],
    "title_oc": np.nan,
    "title_scopus": sc_only["title_scopus"],
    "title_norm_oc": np.nan,
    "title_norm_sc": sc_only["title_norm"],
    "doi_raw": np.nan,
    "doi_raw_scopus": sc_only["doi_raw_scopus"],
    "doi_norm": sc_only["doi_norm"],
    "year_oc": np.nan,
    "year_sc": sc_only["year"],
    "pub_date_oc": np.nan,
    "pub_date_scopus": sc_only["pub_date_scopus"],
    "citedby_count_oc": np.nan,
    "citedby_count_scopus": sc_only["citedby_count_scopus"],
    "match_method": "scopus_only",
    "fuzzy_score": np.nan
})

master = pd.concat(
    [all_matches, oc_only_master, sc_only_master],
    ignore_index=True
)

master["source_status"] = np.select(
    [
        master["match_method"].isin(["doi_exact", "title_exact_year", "title_fuzzy_year_auto", "manual_confirmed"]),
        master["match_method"].eq("oc_only"),
        master["match_method"].eq("scopus_only")
    ],
    [
        "both",
        "open_citations_only",
        "scopus_only"
    ],
    default="unknown"
)

master["venue_name"] = master["venue_name_oc"].combine_first(master["venue_name_scopus"])
master["title_preferred"] = master["title_oc"].combine_first(master["title_scopus"])
master["year_preferred"] = master["year_oc"].combine_first(master["year_sc"])

# =========================================================
# SAVE
# =========================================================

master.to_excel(out_master, index=False)
oc_only_master.to_excel(out_oc_only, index=False)
sc_only_master.to_excel(out_sc_only, index=False)

if not manual_review_df.empty:
    manual_review_df.sort_values(
        ["best_score", "score_gap"],
        ascending=[False, False]
    ).to_excel(out_manual_review_new, index=False)

# =========================================================
# SUMMARY
# =========================================================

print("=== MATCH SUMMARY ===")
print(f"OpenCitations records: {len(oc)}")
print(f"Scopus records: {len(sc)}")
print()
print(f"DOI matches: {len(doi_matches_std)}")
print(f"Exact title+year matches: {len(title_matches_std)}")
print(f"Auto fuzzy matches: {len(fuzzy_matches_std)}")
print(f"Manual confirmed matches: {len(manual_matches_std)}")
print(f"Total matched pairs: {len(all_matches)}")
print()
print(f"OC only: {len(oc_only_master)}")
print(f"Scopus only: {len(sc_only_master)}")
print()
if not manual_review_df.empty:
    print(f"New manual review candidates: {len(manual_review_df)}")
else:
    print("New manual review candidates: 0")

print()
print("=== MATCH METHODS ===")
print(master["match_method"].value_counts(dropna=False))

print()
print("=== SOURCE STATUS ===")
print(master["source_status"].value_counts(dropna=False))














