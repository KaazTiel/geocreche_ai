import osmnx as ox

def carregar_grafo_osm(poligono, network_type="walk"):
    G = ox.graph_from_polygon(poligono, network_type=network_type)
    G = ox.distance.add_edge_lengths(G)
    return G
