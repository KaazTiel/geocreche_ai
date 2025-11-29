import numpy as np
import networkx as nx
import multiprocessing as mp
import osmnx as ox
import pandas as pd

def _process_chunk(args):
    # compatibilidade com seu antigo process_chunk
    G, maes_chunk, creches_nodes = args
    resultados = []
    for idx, row in maes_chunk.iterrows():
        try:
            node_mae = ox.distance.nearest_nodes(G, X=row.geometry.x, Y=row.geometry.y)
        except Exception:
            resultados.append((row["mother_id"], None, None))
            continue
        melhor_dist = float("inf")
        melhor_creche = None
        for creche_nome, node_creche in creches_nodes.items():
            try:
                dist = nx.shortest_path_length(G, node_mae, node_creche, weight="length")
            except Exception:
                continue
            if dist < melhor_dist:
                melhor_dist = dist
                melhor_creche = creche_nome
        resultados.append((row["mother_id"], melhor_creche, melhor_dist))
    return resultados

def calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=4):
    creches_gdf["node"] = creches_gdf.apply(lambda r: ox.distance.nearest_nodes(G, X=r.geometry.x, Y=r.geometry.y), axis=1)
    creches_nodes = {r["Nome"]: r["node"] for _, r in creches_gdf.iterrows()}
    chunks = np.array_split(maes_gdf, num_processos)
    args = [(G, chunk, creches_nodes) for chunk in chunks]
    with mp.Pool(processes=num_processos) as pool:
        partials = pool.map(_process_chunk, args)
    resultados = [r for p in partials for r in p]
    res_map = {m: (c, d) for m, c, d in resultados}
    res_df = maes_gdf.copy()
    res_df["creche_mais_proxima"] = res_df["mother_id"].map(lambda m: res_map.get(m, (None, None))[0])
    res_df["distancia_metros"] = res_df["mother_id"].map(lambda m: res_map.get(m, (None, None))[1])
    return res_df
