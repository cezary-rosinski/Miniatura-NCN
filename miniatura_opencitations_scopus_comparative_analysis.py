import numpy as np
import pandas as pd
from scipy.stats import spearmanr, pearsonr, kendalltau

# =========================================================
# PATHS
# =========================================================

file_path = r"data/oc_scopus_articles_matched_master.xlsx"
output_path = r"data/oc_scopus_comparative_analysis.xlsx"

# =========================================================
# LOAD DATA
# =========================================================

df = pd.read_excel(file_path)

# =========================================================
# BASIC CLEANING
# =========================================================

df = df.copy()

# numeric fields
for col in ["citedby_count_oc", "citedby_count_scopus", "year_preferred", "year_oc", "year_sc", "venue_internal_id"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# citation counts
df["citedby_count_oc"] = pd.to_numeric(df["citedby_count_oc"], errors="coerce")
df["citedby_count_scopus"] = pd.to_numeric(df["citedby_count_scopus"], errors="coerce")

# flags
df["is_both"] = df["source_status"].eq("both")
df["is_oc_only"] = df["source_status"].eq("open_citations_only")
df["is_scopus_only"] = df["source_status"].eq("scopus_only")

df["has_doi"] = df["doi_norm"].notna() & (df["doi_norm"].astype(str).str.strip() != "")
df["matched_by_doi"] = df["match_method"].eq("doi_exact")
df["matched_by_title_exact"] = df["match_method"].eq("title_exact_year")
df["matched_by_fuzzy"] = df["match_method"].eq("title_fuzzy_year_auto")
df["matched_manual"] = df["match_method"].eq("manual_confirmed")

# stable display fields
df["venue_name"] = df["venue_name"].fillna(df["venue_name_oc"]).fillna(df["venue_name_scopus"])
df["year_preferred"] = df["year_preferred"].fillna(df["year_oc"]).fillna(df["year_sc"])

# =========================================================
# PREFERRED JOURNAL NAME PER venue_internal_id
# =========================================================

name_map = (
    df.dropna(subset=["venue_internal_id", "venue_name"])
      .groupby(["venue_internal_id", "venue_name"])
      .size()
      .reset_index(name="n_name_occurrences")
      .sort_values(
          ["venue_internal_id", "n_name_occurrences", "venue_name"],
          ascending=[True, False, True]
      )
      .drop_duplicates(subset=["venue_internal_id"], keep="first")
      .rename(columns={"venue_name": "venue_name_preferred"})
      [["venue_internal_id", "venue_name_preferred"]]
)

# fallback in case some venue has only missing names
all_venues = pd.DataFrame({"venue_internal_id": sorted(df["venue_internal_id"].dropna().unique())})
name_map = all_venues.merge(name_map, on="venue_internal_id", how="left")
name_map["venue_name_preferred"] = name_map["venue_name_preferred"].fillna("UNKNOWN JOURNAL")

# =========================================================
# HELPER FUNCTIONS
# =========================================================

def gini_coefficient(x):
    x = np.array(pd.Series(x).dropna(), dtype=float)

    if len(x) == 0:
        return np.nan
    if np.any(x < 0):
        raise ValueError("Gini coefficient is not defined for negative values.")
    if np.all(x == 0):
        return 0.0

    x_sorted = np.sort(x)
    n = len(x_sorted)
    index = np.arange(1, n + 1)
    gini = (2 * np.sum(index * x_sorted)) / (n * np.sum(x_sorted)) - (n + 1) / n
    return gini


def theil_index(x):
    x = np.array(pd.Series(x).dropna(), dtype=float)

    if len(x) == 0:
        return np.nan
    if np.any(x < 0):
        raise ValueError("Theil index is not defined for negative values.")

    mean_x = np.mean(x)
    if mean_x == 0:
        return 0.0

    ratios = x / mean_x
    ratios = ratios[ratios > 0]
    return np.mean(ratios * np.log(ratios))


def top_10_citation_share(x):
    x = np.array(pd.Series(x).dropna(), dtype=float)

    if len(x) == 0:
        return np.nan

    total_citations = x.sum()
    if total_citations == 0:
        return 0.0

    n_top = max(1, int(np.ceil(len(x) * 0.10)))
    x_sorted_desc = np.sort(x)[::-1]
    top_sum = x_sorted_desc[:n_top].sum()

    return top_sum / total_citations


def citation_profile_metrics(citations):
    citations = pd.Series(citations).dropna().astype(float)

    if len(citations) == 0:
        return {
            "n_articles": 0,
            "total_citations": np.nan,
            "mean_citations": np.nan,
            "median_citations": np.nan,
            "mean_median_gap": np.nan,
            "share_uncited": np.nan,
            "share_ge_1": np.nan,
            "share_ge_2": np.nan,
            "share_ge_5": np.nan,
            "share_ge_10": np.nan,
            "p95_citations": np.nan,
            "p99_citations": np.nan,
            "std_citations": np.nan,
            "cv_citations": np.nan,
            "gini_citations": np.nan,
            "theil_citations": np.nan,
            "top_10pct_citation_share": np.nan
        }

    mean_citations = citations.mean()
    median_citations = citations.median()
    std_citations = citations.std(ddof=1)

    if mean_citations == 0:
        cv = np.nan if std_citations > 0 else 0.0
    else:
        cv = std_citations / mean_citations

    return {
        "n_articles": len(citations),
        "total_citations": citations.sum(),
        "mean_citations": mean_citations,
        "median_citations": median_citations,
        "mean_median_gap": mean_citations - median_citations,
        "share_uncited": (citations == 0).mean(),
        "share_ge_1": (citations >= 1).mean(),
        "share_ge_2": (citations >= 2).mean(),
        "share_ge_5": (citations >= 5).mean(),
        "share_ge_10": (citations >= 10).mean(),
        "p95_citations": citations.quantile(0.95),
        "p99_citations": citations.quantile(0.99),
        "std_citations": std_citations,
        "cv_citations": cv,
        "gini_citations": gini_coefficient(citations),
        "theil_citations": theil_index(citations),
        "top_10pct_citation_share": top_10_citation_share(citations)
    }


def safe_corr(x, y, method="spearman"):
    tmp = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(tmp) < 2:
        return np.nan

    if method == "spearman":
        return spearmanr(tmp["x"], tmp["y"]).statistic
    elif method == "pearson":
        return pearsonr(tmp["x"], tmp["y"])[0]
    elif method == "kendall":
        return kendalltau(tmp["x"], tmp["y"]).statistic
    else:
        raise ValueError("Unknown method")


# =========================================================
# 1. GLOBAL COVERAGE
# =========================================================

global_coverage = pd.DataFrame({
    "category": ["both", "open_citations_only", "scopus_only", "total"],
    "n_articles": [
        int(df["is_both"].sum()),
        int(df["is_oc_only"].sum()),
        int(df["is_scopus_only"].sum()),
        int(len(df))
    ]
})

n_both = int(df["is_both"].sum())
n_oc_only = int(df["is_oc_only"].sum())
n_sc_only = int(df["is_scopus_only"].sum())

global_overlap = pd.DataFrame({
    "metric": [
        "overlap_share_of_total",
        "overlap_relative_to_oc",
        "overlap_relative_to_scopus",
        "jaccard_index"
    ],
    "value": [
        n_both / len(df) if len(df) else np.nan,
        n_both / (n_both + n_oc_only) if (n_both + n_oc_only) else np.nan,
        n_both / (n_both + n_sc_only) if (n_both + n_sc_only) else np.nan,
        n_both / (n_both + n_oc_only + n_sc_only) if (n_both + n_oc_only + n_sc_only) else np.nan
    ]
})

# =========================================================
# 2. JOURNAL-LEVEL OVERLAP
# =========================================================

journal_overlap = (
    df.groupby("venue_internal_id", dropna=False)
      .agg(
          n_both=("is_both", "sum"),
          n_oc_only=("is_oc_only", "sum"),
          n_scopus_only=("is_scopus_only", "sum")
      )
      .reset_index()
      .merge(name_map, on="venue_internal_id", how="left")
      .rename(columns={"venue_name_preferred": "venue_name"})
)

journal_overlap["n_oc_total"] = journal_overlap["n_both"] + journal_overlap["n_oc_only"]
journal_overlap["n_scopus_total"] = journal_overlap["n_both"] + journal_overlap["n_scopus_only"]
journal_overlap["n_union"] = journal_overlap["n_both"] + journal_overlap["n_oc_only"] + journal_overlap["n_scopus_only"]

journal_overlap["share_both_in_oc"] = journal_overlap["n_both"] / journal_overlap["n_oc_total"]
journal_overlap["share_both_in_scopus"] = journal_overlap["n_both"] / journal_overlap["n_scopus_total"]
journal_overlap["jaccard_index"] = journal_overlap["n_both"] / journal_overlap["n_union"]

journal_overlap = journal_overlap.sort_values(
    ["jaccard_index", "share_both_in_scopus", "share_both_in_oc"],
    ascending=False
).reset_index(drop=True)

# =========================================================
# 3. MATCHED-ARTICLES CITATION COMPARISON
# =========================================================

matched = df[df["is_both"]].copy()

matched["citation_delta_scopus_minus_oc"] = matched["citedby_count_scopus"] - matched["citedby_count_oc"]
matched["citation_abs_delta"] = matched["citation_delta_scopus_minus_oc"].abs()
matched["citation_relative_delta_scopus_base"] = matched["citation_delta_scopus_minus_oc"] / matched["citedby_count_scopus"].clip(lower=1)

matched_global_summary = pd.DataFrame({
    "metric": [
        "n_matched_articles",
        "mean_citations_oc",
        "mean_citations_scopus",
        "median_citations_oc",
        "median_citations_scopus",
        "mean_delta_scopus_minus_oc",
        "median_delta_scopus_minus_oc",
        "mean_abs_delta",
        "share_equal_counts",
        "share_oc_higher",
        "share_scopus_higher",
        "pearson_r",
        "spearman_rho",
        "kendall_tau"
    ],
    "value": [
        len(matched),
        matched["citedby_count_oc"].mean(),
        matched["citedby_count_scopus"].mean(),
        matched["citedby_count_oc"].median(),
        matched["citedby_count_scopus"].median(),
        matched["citation_delta_scopus_minus_oc"].mean(),
        matched["citation_delta_scopus_minus_oc"].median(),
        matched["citation_abs_delta"].mean(),
        (matched["citedby_count_oc"] == matched["citedby_count_scopus"]).mean(),
        (matched["citedby_count_oc"] > matched["citedby_count_scopus"]).mean(),
        (matched["citedby_count_scopus"] > matched["citedby_count_oc"]).mean(),
        safe_corr(matched["citedby_count_oc"], matched["citedby_count_scopus"], method="pearson"),
        safe_corr(matched["citedby_count_oc"], matched["citedby_count_scopus"], method="spearman"),
        safe_corr(matched["citedby_count_oc"], matched["citedby_count_scopus"], method="kendall")
    ]
})

matched_delta_distribution = pd.DataFrame({
    "metric": [
        "delta_q01",
        "delta_q05",
        "delta_q25",
        "delta_q50",
        "delta_q75",
        "delta_q95",
        "delta_q99",
        "abs_delta_q95",
        "abs_delta_q99"
    ],
    "value": [
        matched["citation_delta_scopus_minus_oc"].quantile(0.01),
        matched["citation_delta_scopus_minus_oc"].quantile(0.05),
        matched["citation_delta_scopus_minus_oc"].quantile(0.25),
        matched["citation_delta_scopus_minus_oc"].quantile(0.50),
        matched["citation_delta_scopus_minus_oc"].quantile(0.75),
        matched["citation_delta_scopus_minus_oc"].quantile(0.95),
        matched["citation_delta_scopus_minus_oc"].quantile(0.99),
        matched["citation_abs_delta"].quantile(0.95),
        matched["citation_abs_delta"].quantile(0.99)
    ]
})

matched_journal_comparison = (
    matched.groupby("venue_internal_id", dropna=False)
           .apply(
               lambda g: pd.Series({
                   "n_matched_articles": len(g),
                   "mean_citations_oc": g["citedby_count_oc"].mean(),
                   "mean_citations_scopus": g["citedby_count_scopus"].mean(),
                   "median_citations_oc": g["citedby_count_oc"].median(),
                   "median_citations_scopus": g["citedby_count_scopus"].median(),
                   "mean_delta_scopus_minus_oc": (g["citedby_count_scopus"] - g["citedby_count_oc"]).mean(),
                   "median_delta_scopus_minus_oc": (g["citedby_count_scopus"] - g["citedby_count_oc"]).median(),
                   "share_equal_counts": (g["citedby_count_oc"] == g["citedby_count_scopus"]).mean(),
                   "share_oc_higher": (g["citedby_count_oc"] > g["citedby_count_scopus"]).mean(),
                   "share_scopus_higher": (g["citedby_count_scopus"] > g["citedby_count_oc"]).mean(),
                   "pearson_r": safe_corr(g["citedby_count_oc"], g["citedby_count_scopus"], method="pearson"),
                   "spearman_rho": safe_corr(g["citedby_count_oc"], g["citedby_count_scopus"], method="spearman"),
                   "kendall_tau": safe_corr(g["citedby_count_oc"], g["citedby_count_scopus"], method="kendall")
               })
           )
           .reset_index()
           .merge(name_map, on="venue_internal_id", how="left")
           .rename(columns={"venue_name_preferred": "venue_name"})
)

# =========================================================
# 4. FULL VS MATCHED CITATION DISTRIBUTION PROFILES
# =========================================================

profiles = []

journal_keys = name_map.rename(columns={"venue_name_preferred": "venue_name"}).copy()

for _, row in journal_keys.iterrows():
    venue_id = row["venue_internal_id"]
    venue_name = row["venue_name"]

    sub = df[df["venue_internal_id"] == venue_id].copy()

    # full OC
    oc_full = sub[sub["source_status"].isin(["both", "open_citations_only"])]["citedby_count_oc"]
    metrics_oc_full = citation_profile_metrics(oc_full)
    metrics_oc_full.update({
        "venue_internal_id": venue_id,
        "venue_name": venue_name,
        "profile_type": "oc_full"
    })
    profiles.append(metrics_oc_full)

    # full Scopus
    sc_full = sub[sub["source_status"].isin(["both", "scopus_only"])]["citedby_count_scopus"]
    metrics_sc_full = citation_profile_metrics(sc_full)
    metrics_sc_full.update({
        "venue_internal_id": venue_id,
        "venue_name": venue_name,
        "profile_type": "scopus_full"
    })
    profiles.append(metrics_sc_full)

    # matched OC
    oc_matched = sub[sub["source_status"].eq("both")]["citedby_count_oc"]
    metrics_oc_matched = citation_profile_metrics(oc_matched)
    metrics_oc_matched.update({
        "venue_internal_id": venue_id,
        "venue_name": venue_name,
        "profile_type": "oc_matched"
    })
    profiles.append(metrics_oc_matched)

    # matched Scopus
    sc_matched = sub[sub["source_status"].eq("both")]["citedby_count_scopus"]
    metrics_sc_matched = citation_profile_metrics(sc_matched)
    metrics_sc_matched.update({
        "venue_internal_id": venue_id,
        "venue_name": venue_name,
        "profile_type": "scopus_matched"
    })
    profiles.append(metrics_sc_matched)

profiles_df = pd.DataFrame(profiles)

profile_comparison = profiles_df.pivot_table(
    index=["venue_internal_id", "venue_name"],
    columns="profile_type",
    values=[
        "n_articles",
        "total_citations",
        "mean_citations",
        "median_citations",
        "share_uncited",
        "share_ge_1",
        "share_ge_2",
        "share_ge_5",
        "share_ge_10",
        "p95_citations",
        "p99_citations",
        "gini_citations",
        "theil_citations",
        "top_10pct_citation_share"
    ]
)

profile_comparison.columns = [
    f"{metric}__{profile}"
    for metric, profile in profile_comparison.columns
]
profile_comparison = profile_comparison.reset_index()

for metric in [
    "mean_citations",
    "median_citations",
    "share_uncited",
    "gini_citations",
    "top_10pct_citation_share"
]:
    oc_full_col = f"{metric}__oc_full"
    sc_full_col = f"{metric}__scopus_full"
    oc_matched_col = f"{metric}__oc_matched"
    sc_matched_col = f"{metric}__scopus_matched"

    profile_comparison[f"{metric}__delta_full_scopus_minus_oc"] = profile_comparison[sc_full_col] - profile_comparison[oc_full_col]
    profile_comparison[f"{metric}__delta_matched_scopus_minus_oc"] = profile_comparison[sc_matched_col] - profile_comparison[oc_matched_col]
    profile_comparison[f"{metric}__selection_effect_scopus"] = profile_comparison[sc_matched_col] - profile_comparison[sc_full_col]
    profile_comparison[f"{metric}__selection_effect_oc"] = profile_comparison[oc_matched_col] - profile_comparison[oc_full_col]

profile_global_summary = []

for profile_type in ["oc_full", "scopus_full", "oc_matched", "scopus_matched"]:
    tmp = profiles_df[profiles_df["profile_type"] == profile_type].copy()

    profile_global_summary.append({
        "profile_type": profile_type,
        "mean_of_mean_citations": tmp["mean_citations"].mean(),
        "median_of_mean_citations": tmp["mean_citations"].median(),
        "mean_of_median_citations": tmp["median_citations"].mean(),
        "median_of_median_citations": tmp["median_citations"].median(),
        "mean_share_uncited": tmp["share_uncited"].mean(),
        "median_share_uncited": tmp["share_uncited"].median(),
        "mean_gini": tmp["gini_citations"].mean(),
        "median_gini": tmp["gini_citations"].median(),
        "mean_top10_share": tmp["top_10pct_citation_share"].mean(),
        "median_top10_share": tmp["top_10pct_citation_share"].median()
    })

profile_global_summary = pd.DataFrame(profile_global_summary)

# =========================================================
# 5. YEAR ANALYSIS
# =========================================================

year_analysis = (
    df.groupby("year_preferred", dropna=False)
      .agg(
          n_both=("is_both", "sum"),
          n_oc_only=("is_oc_only", "sum"),
          n_scopus_only=("is_scopus_only", "sum"),
          n_with_doi=("has_doi", "sum"),
          n_total=("source_status", "size")
      )
      .reset_index()
      .sort_values("year_preferred")
)

year_analysis["share_both_total"] = year_analysis["n_both"] / year_analysis["n_total"]
year_analysis["share_both_in_oc"] = year_analysis["n_both"] / (year_analysis["n_both"] + year_analysis["n_oc_only"])
year_analysis["share_both_in_scopus"] = year_analysis["n_both"] / (year_analysis["n_both"] + year_analysis["n_scopus_only"])
year_analysis["share_with_doi"] = year_analysis["n_with_doi"] / year_analysis["n_total"]

matched_year_analysis = (
    matched.groupby("year_preferred", dropna=False)
           .apply(
               lambda g: pd.Series({
                   "n_matched": len(g),
                   "mean_citations_oc": g["citedby_count_oc"].mean(),
                   "mean_citations_scopus": g["citedby_count_scopus"].mean(),
                   "median_citations_oc": g["citedby_count_oc"].median(),
                   "median_citations_scopus": g["citedby_count_scopus"].median(),
                   "mean_delta_scopus_minus_oc": (g["citedby_count_scopus"] - g["citedby_count_oc"]).mean(),
                   "share_equal_counts": (g["citedby_count_oc"] == g["citedby_count_scopus"]).mean()
               })
           )
           .reset_index()
           .sort_values("year_preferred")
)

# =========================================================
# 6. DOI ANALYSIS
# =========================================================

doi_analysis_global = pd.DataFrame({
    "metric": [
        "share_with_doi_total",
        "share_with_doi_both",
        "share_with_doi_oc_only",
        "share_with_doi_scopus_only",
        "share_matches_by_doi_among_both",
        "share_matches_by_title_exact_among_both",
        "share_matches_by_fuzzy_among_both",
        "share_matches_manual_among_both"
    ],
    "value": [
        df["has_doi"].mean(),
        df.loc[df["is_both"], "has_doi"].mean(),
        df.loc[df["is_oc_only"], "has_doi"].mean(),
        df.loc[df["is_scopus_only"], "has_doi"].mean(),
        df.loc[df["is_both"], "matched_by_doi"].mean(),
        df.loc[df["is_both"], "matched_by_title_exact"].mean(),
        df.loc[df["is_both"], "matched_by_fuzzy"].mean(),
        df.loc[df["is_both"], "matched_manual"].mean()
    ]
})

doi_analysis_journal = (
    df.groupby("venue_internal_id", dropna=False)
      .apply(
          lambda g: pd.Series({
              "n_total": len(g),
              "share_with_doi_total": g["has_doi"].mean(),
              "share_with_doi_both": g.loc[g["is_both"], "has_doi"].mean() if g["is_both"].any() else np.nan,
              "share_with_doi_oc_only": g.loc[g["is_oc_only"], "has_doi"].mean() if g["is_oc_only"].any() else np.nan,
              "share_with_doi_scopus_only": g.loc[g["is_scopus_only"], "has_doi"].mean() if g["is_scopus_only"].any() else np.nan,
              "share_matches_by_doi": g.loc[g["is_both"], "matched_by_doi"].mean() if g["is_both"].any() else np.nan,
              "share_matches_by_title_exact": g.loc[g["is_both"], "matched_by_title_exact"].mean() if g["is_both"].any() else np.nan,
              "share_matches_by_fuzzy": g.loc[g["is_both"], "matched_by_fuzzy"].mean() if g["is_both"].any() else np.nan,
              "share_matches_manual": g.loc[g["is_both"], "matched_manual"].mean() if g["is_both"].any() else np.nan
          })
      )
      .reset_index()
      .merge(name_map, on="venue_internal_id", how="left")
      .rename(columns={"venue_name_preferred": "venue_name"})
)

# =========================================================
# 7. JOURNAL TYPOLOGY / OUTLIERS
# =========================================================

journal_master = journal_overlap.merge(
    matched_journal_comparison,
    on=["venue_internal_id", "venue_name"],
    how="left"
).merge(
    profile_comparison,
    on=["venue_internal_id", "venue_name"],
    how="left"
).merge(
    doi_analysis_journal,
    on=["venue_internal_id", "venue_name"],
    how="left"
)

conditions = [
    (journal_master["jaccard_index"] >= 0.70) & (journal_master["mean_citations__delta_full_scopus_minus_oc"].abs() <= 0.25),
    (journal_master["jaccard_index"] < 0.40) & (journal_master["mean_citations__delta_full_scopus_minus_oc"] < -0.25),
    (journal_master["jaccard_index"] >= 0.50) & (journal_master["mean_delta_scopus_minus_oc"].abs() > 0.50),
    (journal_master["jaccard_index"] < 0.30)
]

choices = [
    "high_overlap_low_distortion",
    "low_overlap_oc_more_citation_dense",
    "moderate_overlap_high_count_divergence",
    "low_overlap_structural_gap"
]

journal_master["journal_type"] = np.select(conditions, choices, default="mixed")

top_low_overlap = journal_master.sort_values("jaccard_index", ascending=True).head(15)
top_high_overlap = journal_master.sort_values("jaccard_index", ascending=False).head(15)

top_scopus_advantage = journal_master.sort_values("mean_citations__delta_full_scopus_minus_oc", ascending=False).head(15)
top_oc_advantage = journal_master.sort_values("mean_citations__delta_full_scopus_minus_oc", ascending=True).head(15)

top_scopus_uncited_gap = journal_master.sort_values("share_uncited__delta_full_scopus_minus_oc", ascending=False).head(15)
top_gini_gap = journal_master.sort_values("gini_citations__delta_full_scopus_minus_oc", ascending=False).head(15)

# =========================================================
# 8. CORRELATION / EXPLANATORY BLOCK
# =========================================================

corr_vars = [
    "jaccard_index",
    "share_both_in_oc",
    "share_both_in_scopus",
    "share_with_doi_total",
    "share_matches_by_doi",
    "mean_citations__delta_full_scopus_minus_oc",
    "share_uncited__delta_full_scopus_minus_oc",
    "gini_citations__delta_full_scopus_minus_oc",
    "top_10pct_citation_share__delta_full_scopus_minus_oc",
    "n_oc_total",
    "n_scopus_total"
]

corr_df = journal_master[corr_vars].copy()
corr_matrix_spearman = corr_df.corr(method="spearman")
corr_matrix_pearson = corr_df.corr(method="pearson")

# =========================================================
# 9. ARTICLE-LEVEL EXPORTS FOR VISUALIZATIONS
# =========================================================

matched_export = matched[
    [
        "venue_internal_id",
        "venue_name",
        "article_id_oc",
        "article_id_scopus",
        "title_preferred",
        "year_preferred",
        "doi_norm",
        "citedby_count_oc",
        "citedby_count_scopus",
        "citation_delta_scopus_minus_oc",
        "citation_abs_delta",
        "citation_relative_delta_scopus_base",
        "match_method"
    ]
].copy()

matched_export = matched_export.merge(name_map, on="venue_internal_id", how="left")
matched_export["venue_name"] = matched_export["venue_name_preferred"]
matched_export = matched_export.drop(columns=["venue_name_preferred"])

coverage_export = df[
    [
        "venue_internal_id",
        "venue_name",
        "article_id_oc",
        "article_id_scopus",
        "title_preferred",
        "year_preferred",
        "doi_norm",
        "source_status",
        "match_method",
        "citedby_count_oc",
        "citedby_count_scopus"
    ]
].copy()

coverage_export = coverage_export.merge(name_map, on="venue_internal_id", how="left")
coverage_export["venue_name"] = coverage_export["venue_name_preferred"]
coverage_export = coverage_export.drop(columns=["venue_name_preferred"])

# =========================================================
# SAVE TO EXCEL
# =========================================================

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    global_coverage.to_excel(writer, sheet_name="global_coverage", index=False)
    global_overlap.to_excel(writer, sheet_name="global_overlap", index=False)

    journal_overlap.to_excel(writer, sheet_name="journal_overlap", index=False)

    matched_global_summary.to_excel(writer, sheet_name="matched_global_summary", index=False)
    matched_delta_distribution.to_excel(writer, sheet_name="matched_delta_distribution", index=False)
    matched_journal_comparison.to_excel(writer, sheet_name="matched_journal_comp", index=False)

    profiles_df.to_excel(writer, sheet_name="profiles_long", index=False)
    profile_comparison.to_excel(writer, sheet_name="profiles_comparison", index=False)
    profile_global_summary.to_excel(writer, sheet_name="profiles_global_summary", index=False)

    year_analysis.to_excel(writer, sheet_name="year_coverage", index=False)
    matched_year_analysis.to_excel(writer, sheet_name="year_matched_counts", index=False)

    doi_analysis_global.to_excel(writer, sheet_name="doi_global", index=False)
    doi_analysis_journal.to_excel(writer, sheet_name="doi_journal", index=False)

    journal_master.to_excel(writer, sheet_name="journal_master", index=False)

    top_low_overlap.to_excel(writer, sheet_name="top_low_overlap", index=False)
    top_high_overlap.to_excel(writer, sheet_name="top_high_overlap", index=False)
    top_scopus_advantage.to_excel(writer, sheet_name="top_scopus_advantage", index=False)
    top_oc_advantage.to_excel(writer, sheet_name="top_oc_advantage", index=False)
    top_scopus_uncited_gap.to_excel(writer, sheet_name="top_uncited_gap", index=False)
    top_gini_gap.to_excel(writer, sheet_name="top_gini_gap", index=False)

    corr_matrix_spearman.to_excel(writer, sheet_name="corr_spearman")
    corr_matrix_pearson.to_excel(writer, sheet_name="corr_pearson")

    matched_export.to_excel(writer, sheet_name="matched_articles_export", index=False)
    coverage_export.to_excel(writer, sheet_name="coverage_export", index=False)

print(f"Saved analysis to: {output_path}")

# =========================================================
# PRINT QUICK SUMMARY
# =========================================================

print("\n=== GLOBAL COVERAGE ===")
print(global_coverage)

print("\n=== GLOBAL OVERLAP ===")
print(global_overlap)

print("\n=== MATCHED ARTICLES SUMMARY ===")
print(matched_global_summary)

print("\n=== PROFILE GLOBAL SUMMARY ===")
print(profile_global_summary)

print("\n=== TOP 10 LOWEST OVERLAP JOURNALS ===")
print(top_low_overlap[["venue_name", "n_both", "n_oc_only", "n_scopus_only", "jaccard_index"]].head(10))

print("\n=== TOP 10 HIGHEST OVERLAP JOURNALS ===")
print(top_high_overlap[["venue_name", "n_both", "n_oc_only", "n_scopus_only", "jaccard_index"]].head(10))