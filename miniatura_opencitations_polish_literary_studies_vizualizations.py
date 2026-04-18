import pandas as pd
import matplotlib.pyplot as plt
import json

#%%
# === 1. Wczytanie danych ===
file_path = r"data/literary_journal_articles_opencitations_final.xlsx"
df = pd.read_excel(file_path)

# === 2. Przygotowanie danych ===
# brak citedby_count traktujemy jako 0
df["citedby_count"] = df["citedby_count"].fillna(0)

# jeśli wartości są floatami typu 12.0, zamieniamy na int
df["citedby_count"] = df["citedby_count"].astype(int)

# na wszelki wypadek zostawiamy unikalne artykuły
articles = df[["article_omid", "citedby_count"]].drop_duplicates()

# === 3. Rozkład liczby cytowań ===
# ile artykułów ma 0 cytowań, ile ma 1, ile ma 2 itd.
citation_distribution = (
    articles.groupby("citedby_count")["article_omid"]
    .nunique()
    .reset_index(name="article_count")
    .sort_values("citedby_count")
)

# === 4. Statystyki ===
mean_citations = articles["citedby_count"].mean()
median_citations = articles["citedby_count"].median()

print(f"Średnia liczba cytowań: {mean_citations:.2f}")
print(f"Mediana liczby cytowań: {median_citations:.2f}")

# upewnij się, że dane są posortowane po osi X
citation_distribution = citation_distribution.sort_values("citedby_count")

# === 5A. Wykres standardowy ===
plt.figure(figsize=(14, 7))

plt.bar(
    citation_distribution["citedby_count"],
    citation_distribution["article_count"]
)

plt.axvline(
    mean_citations,
    linestyle="--",
    linewidth=2,
    label=f"Mean = {mean_citations:.2f}",
    color='red'
)

plt.axvline(
    median_citations,
    linestyle="--",
    linewidth=2,
    label=f"Median = {median_citations:.2f}",
    color='pink'
)

plt.xlabel("No. of citations (citedby_count)")
plt.ylabel("No. of articles")
plt.title("Distribution of the number of citations")
plt.legend()
plt.tight_layout()
plt.show()


# === 5B. Wykres z osią Y w skali log ===
plt.figure(figsize=(14, 7))

plt.bar(
    citation_distribution["citedby_count"],
    citation_distribution["article_count"]
)

plt.yscale("log")

plt.axvline(
    mean_citations,
    linestyle="--",
    linewidth=2,
    label=f"Mean = {mean_citations:.2f}",
    color='red'
)

plt.axvline(
    median_citations,
    linestyle="--",
    linewidth=2,
    label=f"Median = {median_citations:.2f}",
    color='pink'
)

plt.xlabel("No. of citations")
plt.ylabel("No. of articles (log scale)")
plt.title("Distribution of the number of citations (log scale)")
plt.legend()
plt.tight_layout()
plt.show()


#%% venue osobno

articles = df[["article_omid", "venue_name", "citedby_count"]].drop_duplicates()

mean_agg = {}
median_agg = {}


for venue in sorted(articles["venue_name"].unique()):
    venue_articles = articles.loc[articles["venue_name"] == venue].copy()

    mean_citations = venue_articles["citedby_count"].mean()
    median_citations = venue_articles["citedby_count"].median()
    
    mean_agg.update({venue:mean_citations})
    median_agg.update({venue:median_citations})

    citation_distribution = (
        venue_articles.groupby("citedby_count")["article_omid"]
        .nunique()
        .reset_index(name="article_count")
        .sort_values("citedby_count")
    )

    plt.figure(figsize=(14, 7))

    plt.bar(
        citation_distribution["citedby_count"],
        citation_distribution["article_count"]
    )

    plt.yscale("log")

    plt.axvline(
        mean_citations,
        linestyle="--",
        linewidth=2,
        label=f"Mean = {mean_citations:.2f}",
        color="red"
    )

    plt.axvline(
        median_citations,
        linestyle="--",
        linewidth=2,
        label=f"Median = {median_citations:.2f}",
        color="pink"
    )

    plt.xlabel("No. of citations (citedby_count)")
    plt.ylabel("No. of articles (log scale)")
    plt.title(f"Distribution of the number of citations (log scale)\n{venue}")
    plt.legend()
    plt.tight_layout()
    plt.show()




json_str = json.dumps(mean_agg, indent=4)
with open("data/opencitations_mean_agg.json", "w") as f:
    f.write(json_str)

json_str = json.dumps(median_agg, indent=4)
with open("data/opencitations_median_agg.json", "w") as f:
    f.write(json_str)


#%%

import math
import pandas as pd
import matplotlib.pyplot as plt

# === 1. Przygotowanie danych ===
# zakładam, że df jest już wczytany
# df = pd.read_excel(...)

df["citedby_count"] = df["citedby_count"].fillna(0).astype(int)

# zostawiamy unikalne artykuły w obrębie czasopisma
articles = (
    df[["venue_name", "article_omid", "citedby_count"]]
    .drop_duplicates()
    .copy()
)

# opcjonalnie: usuń wiersze bez nazwy czasopisma
articles = articles[articles["venue_name"].notna()].copy()

# === 2. Parametry dashboardu ===
ncols = 3                         # liczba kolumn w dashboardzie
panel_width = 7
panel_height = 4.5

venues = sorted(articles["venue_name"].unique())
n_venues = len(venues)
nrows = math.ceil(n_venues / ncols)

fig, axes = plt.subplots(
    nrows=nrows,
    ncols=ncols,
    figsize=(panel_width * ncols, panel_height * nrows)
)

# gdy subplot jest tylko jeden, ujednolicamy strukturę
if n_venues == 1:
    axes = [axes]
else:
    axes = axes.flatten()

# === 3. Rysowanie wykresów dla każdego czasopisma ===
for ax, venue in zip(axes, venues):
    venue_articles = articles.loc[articles["venue_name"] == venue].copy()

    # statystyki
    mean_citations = venue_articles["citedby_count"].mean()
    median_citations = venue_articles["citedby_count"].median()

    # rozkład: liczba artykułów dla każdej liczby cytowań
    citation_distribution = (
        venue_articles.groupby("citedby_count")["article_omid"]
        .nunique()
        .reset_index(name="article_count")
        .sort_values("citedby_count")
    )

    # wykres słupkowy
    ax.bar(
        citation_distribution["citedby_count"],
        citation_distribution["article_count"]
    )

    # skala logarytmiczna na osi Y
    ax.set_yscale("log")

    # średnia i mediana
    ax.axvline(
        mean_citations,
        linestyle="--",
        linewidth=2,
        label=f"Mean = {mean_citations:.2f}",
        color="red"
    )

    ax.axvline(
        median_citations,
        linestyle="--",
        linewidth=2,
        label=f"Median = {median_citations:.2f}",
        color="pink"
    )

    # tytuł i osie
    ax.set_title(f"{venue}\nN articles = {len(venue_articles)}", fontsize=11)
    ax.set_xlabel("No. of citations (citedby_count)")
    ax.set_ylabel("No. of articles (log scale)")
    ax.legend(fontsize=8)

# === 4. Ukrycie pustych paneli ===
for ax in axes[n_venues:]:
    ax.set_visible(False)

plt.tight_layout()
plt.show()













#%% old
#%% Visualisations

# =========================
# 1. Wczytanie danych
# =========================
file_path = "data/literary_journals_opencitations.xlsx"
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
file_path = "data/literary_journals_opencitations.xlsx"
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