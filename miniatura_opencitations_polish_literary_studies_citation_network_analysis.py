import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

#%% Sigma

import pandas as pd
import networkx as nx
from tqdm import tqdm
from ipysigma import Sigma

# =========================
# 1. Wczytanie i przygotowanie danych
# =========================

file_path = "data/citations_of_literary_journal_articles_opencitations.xlsx"
df = pd.read_excel(file_path)

df = df[["citing", "cited"]].dropna().drop_duplicates()

# usunięcie self-loops
df = df[df["citing"] != df["cited"]]

# =========================
# 2. Budowa grafu skierowanego
# =========================

G = nx.DiGraph()

for _, row in tqdm(df.iterrows(), total=len(df)):
    citing = row["citing"]
    cited = row["cited"]

    # klasyfikacja węzłów zrobimy później, więc tu tylko dodajemy węzły
    if not G.has_node(citing):
        G.add_node(citing)

    if not G.has_node(cited):
        G.add_node(cited)

    # krawędź skierowana: citing -> cited
    G.add_edge(citing, cited, weight=1)

# =========================
# 3. Typy węzłów
# =========================

citing_set = set(df["citing"])
cited_set = set(df["cited"])

node_type = {}
node_color = {}

for node in G.nodes():
    in_citing = node in citing_set
    in_cited = node in cited_set

    if in_citing and in_cited:
        node_type[node] = "both"
        node_color[node] = "#984ea3"   # fiolet
    elif in_cited:
        node_type[node] = "cited_only"
        node_color[node] = "#e41a1c"   # czerwony
    elif in_citing:
        node_type[node] = "citing_only"
        node_color[node] = "#377eb8"   # niebieski
    else:
        node_type[node] = "other"
        node_color[node] = "#999999"

nx.set_node_attributes(G, node_type, name="node_type")
nx.set_node_attributes(G, node_color, name="color")

# =========================
# 4. Metryki sieciowe
# =========================

in_degree_dict = dict(G.in_degree())
out_degree_dict = dict(G.out_degree())
degree_dict = dict(G.degree())

pagerank_dict = nx.pagerank(G, alpha=0.85)

# HITS bywa niestabilny dla niektórych sieci, więc warto zabezpieczyć
try:
    hubs_dict, authorities_dict = nx.hits(G, max_iter=1000, normalized=True)
except Exception:
    hubs_dict = {n: 0 for n in G.nodes()}
    authorities_dict = {n: 0 for n in G.nodes()}

nx.set_node_attributes(G, in_degree_dict, name="in_degree")
nx.set_node_attributes(G, out_degree_dict, name="out_degree")
nx.set_node_attributes(G, degree_dict, name="degree")
nx.set_node_attributes(G, pagerank_dict, name="pagerank")
nx.set_node_attributes(G, authorities_dict, name="authority")
nx.set_node_attributes(G, hubs_dict, name="hub")

# =========================
# 5. Prostsza etykieta
# =========================

# jeśli chcesz, można skrócić omid w etykietach
label_dict = {}
for node in G.nodes():
    label_dict[node] = node.replace("omid:", "")

nx.set_node_attributes(G, label_dict, name="label")

# =========================
# 6. Interaktywny podgląd w Jupyterze
# =========================

# Sigma(
#     G,
#     node_color="node_type",     # kolor wg typu węzła
#     node_size="pagerank",       # rozmiar wg PageRank
#     node_label="label",         # etykieta
#     node_label_size="in_degree" # większe etykiety dla częściej cytowanych
# )

Sigma.write_html(
    G,
    'data/OC_network.html',
    G,
    node_color="node_type",     # kolor wg typu węzła
    node_size="pagerank",       # rozmiar wg PageRank
    node_label="label",         # etykieta
    node_label_size="in_degree" # większe etykiety dla częściej cytowanych
)

#%% notes
# # =========================
# # 1. Wczytanie danych
# # =========================

# file_path = "data/citations_of_literary_journal_articles_opencitations.xlsx"
# df = pd.read_excel(file_path)

# # Zostawiamy tylko potrzebne kolumny
# df = df[["citing", "cited"]].dropna().drop_duplicates()

# print("Liczba krawędzi:", len(df))
# print("Liczba unikalnych citing:", df["citing"].nunique())
# print("Liczba unikalnych cited:", df["cited"].nunique())

# # =========================
# # 2. Budowa sieci skierowanej
# # =========================

# G = nx.from_pandas_edgelist(
#     df,
#     source="citing",
#     target="cited",
#     create_using=nx.DiGraph()
# )

# print("Liczba węzłów:", G.number_of_nodes())
# print("Liczba krawędzi:", G.number_of_edges())

# # =========================
# # 3. Podstawowe miary centralności
# # =========================

# in_degree_dict = dict(G.in_degree())
# out_degree_dict = dict(G.out_degree())

# pagerank_dict = nx.pagerank(G, alpha=0.85)

# # HITS może czasem mieć problemy z konwergencją przy większych sieciach,
# # ale tu powinno działać
# hubs_dict, authorities_dict = nx.hits(G, max_iter=1000, normalized=True)

# # =========================
# # 4. Tabela z wynikami dla węzłów
# # =========================

# nodes_df = pd.DataFrame({
#     "node": list(G.nodes()),
#     "in_degree": [in_degree_dict.get(n, 0) for n in G.nodes()],
#     "out_degree": [out_degree_dict.get(n, 0) for n in G.nodes()],
#     "pagerank": [pagerank_dict.get(n, 0) for n in G.nodes()],
#     "authority": [authorities_dict.get(n, 0) for n in G.nodes()],
#     "hub": [hubs_dict.get(n, 0) for n in G.nodes()],
# })

# # Prosta klasyfikacja typu węzła
# cited_set = set(df["cited"])
# citing_set = set(df["citing"])

# def classify_node(n):
#     in_citing = n in citing_set
#     in_cited = n in cited_set
#     if in_citing and in_cited:
#         return "both"
#     elif in_cited:
#         return "cited_only"
#     elif in_citing:
#         return "citing_only"
#     else:
#         return "other"

# nodes_df["node_type"] = nodes_df["node"].apply(classify_node)

# # =========================
# # 5. Najsilniejsze centra
# # =========================

# print("\nTOP 20 wg in-degree")
# print(nodes_df.sort_values("in_degree", ascending=False).head(20))

# print("\nTOP 20 wg PageRank")
# print(nodes_df.sort_values("pagerank", ascending=False).head(20))

# print("\nTOP 20 wg authority")
# print(nodes_df.sort_values("authority", ascending=False).head(20))

# # =========================
# # 6. Struktura sieci
# # =========================

# # Składowe słabo spójne dla sieci skierowanej
# weak_components = list(nx.weakly_connected_components(G))
# weak_components_sizes = sorted([len(c) for c in weak_components], reverse=True)

# print("\nLiczba weakly connected components:", len(weak_components))
# print("10 największych komponentów:", weak_components_sizes[:10])

# # =========================
# # 7. Rdzeń sieci (k-core) i społeczności
# #    na wersji nieskierowanej
# # =========================

# G_und = G.to_undirected()

# core_numbers = nx.core_number(G_und)
# nodes_df["core_number"] = nodes_df["node"].map(core_numbers)

# print("\nTOP 20 wg core number")
# print(nodes_df.sort_values(["core_number", "in_degree"], ascending=[False, False]).head(20))

# # Community detection (greedy modularity)
# communities = nx.community.greedy_modularity_communities(G_und)
# community_map = {}
# for i, comm in enumerate(communities):
#     for node in comm:
#         community_map[node] = i

# nodes_df["community"] = nodes_df["node"].map(community_map)

# print("\nLiczba społeczności:", len(communities))
# print("Rozmiary 10 największych społeczności:",
#       sorted([len(c) for c in communities], reverse=True)[:10])

# # =========================
# # 8. Zapis wyników
# # =========================

# nodes_df.to_excel("data/network_node_metrics.xlsx", index=False)

# # Edge list z atrybutami można też zachować
# df.to_excel("data/network_edges.xlsx", index=False)

# # Eksport do Gephi
# nx.write_gexf(G, "data/citation_network.gexf")

# print("\nZapisano:")
# print("- network_node_metrics.xlsx")
# print("- network_edges.xlsx")
# print("- citation_network.gexf")

# # =========================
# # 9. Wizualizacja najważniejszego subgrafu
# # =========================

# top_n = 100
# top_nodes = nodes_df.sort_values("pagerank", ascending=False).head(top_n)["node"].tolist()
# SG = G.subgraph(top_nodes).copy()

# plt.figure(figsize=(16, 12))
# pos = nx.spring_layout(SG, k=0.5, seed=42)

# # rozmiar węzła zależny od in-degree
# node_sizes = []
# for n in SG.nodes():
#     indeg = SG.in_degree(n)
#     node_sizes.append(50 + indeg * 30)

# nx.draw_networkx_edges(SG, pos, alpha=0.2, arrows=True, width=0.5)
# nx.draw_networkx_nodes(SG, pos, node_size=node_sizes, alpha=0.8)

# # etykiety tylko dla najmocniejszych węzłów
# top_labels = nodes_df.sort_values("pagerank", ascending=False).head(20)["node"].tolist()
# label_dict = {n: n.replace("omid:", "") for n in top_labels if n in SG.nodes()}

# nx.draw_networkx_labels(SG, pos, labels=label_dict, font_size=8)

# plt.title("Najważniejszy fragment sieci cytowań (TOP 100 wg PageRank)")
# plt.axis("off")
# plt.tight_layout()
# plt.show()































