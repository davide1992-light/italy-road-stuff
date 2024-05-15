import osmnx
import pickle
from pathlib import Path 
import networkx as nx
from shapely import LineString
import pandas as pd
import tqdm

RESULT_PATH = Path("./results")

REGIONS = [
    "Valle d'Aosta",
    "Molise",
    "Liguria",
    "Friuli Venezia Giulia",
    "Umbria",
    "Marche",
    "Basilicata",
    "Abruzzo",
    "Campania",
    "Trentino Alto Adige",
    "Calabria",
    "Lazio",
    "Veneto",
    "Puglia",
    "Emilia Romagna",
    "Toscana",
    "Lombardia",
    "Sardegna",
    "Piemonte",
    "Sicilia"
]

def format_highway(highway):
    if isinstance(highway, str):
        return highway
    return ";".join(highway)

def format_geometry(geom: LineString | None):
    if not geom or pd.isna(geom):
        return ""
    return ";".join([f"{x},{y}" for x,y in geom.coords])

for region in tqdm.tqdm(REGIONS[:2]):
    region_path = RESULT_PATH / region
    region_path.mkdir(parents=True, exist_ok=True)
    nx_path = region_path / "nx.pkl"
    G = osmnx.graph_from_place(region, network_type="walk", retain_all=True)

    df: pd.DataFrame = nx.to_pandas_edgelist(G)[["source", "target", "length", "highway", "geometry"]]
    df["highway"] = df["highway"].apply(format_highway)
    df["geometry"] = df["geometry"].apply(format_geometry)
    df.to_parquet(region_path / "edges.parquet")

    for node in G.nodes:
        keys_to_remove = [x for x in G.nodes[node].keys() if x not in {"x", "y"}]
        for key in keys_to_remove:
            del G.nodes[node][key]
    for edge in G.edges:
        keys_to_remove = [x for x in G.edges[edge].keys() if x not in {"length", "highway"}]
        for key in keys_to_remove:
            del G.edges[edge][key]
    nx_path = region_path / "nx.pkl"
    with nx_path.open("wb") as f:
        pickle.dump(G, f)    
    del G
    del df

def load_with_pickle(region):
    path = RESULT_PATH / region / "nx.pkl"
    with path.open("rb") as f:
        G = pickle.load(f)
    return G

italy_graph = nx.compose_all([load_with_pickle(r) for r in REGIONS[:2]])
with (RESULT_PATH / "italy.pkl").open("wb") as f:
    pickle.dump(italy_graph, f)
    