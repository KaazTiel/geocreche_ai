import pandas as pd
from src.utils import config
from src.reporting.mapas_cluster import gerar_mapa_clusters, gerar_mapa_tematico
import src.utils.config as cfg

def gerar_mapas():
    print("[INFO] Gerando mapas...")

    df_maes = pd.read_csv(config.MAES_SAIDA)
    df_creches = pd.read_csv(config.CRECHES_CSV)

    # Mapa de clusters
    html_clusters = gerar_mapa_clusters(df_maes, df_creches)
    with open(cfg.MAPAS_CLUSTERS_DIR / "clusters.html", "w", encoding="utf-8") as f:
        f.write(html_clusters)

    print("[OK] Mapa de clusters salvo.")

    # Mapa temático
    html_tematico = gerar_mapa_tematico(df_maes, config.BAIRROS_GEOJSON, df_creches)
    with open(cfg.MAPAS_TEMATICOS_DIR / "tematico.html", "w", encoding="utf-8") as f:
        f.write(html_tematico)

    print("[OK] Mapa temático salvo.")
