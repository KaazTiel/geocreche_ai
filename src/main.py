import os
import pandas as pd
import geopandas as gpd

from src.utils import config

# Importação do .env
from src.utils.env_loader import DB_CONFIG, DATA_CUTO, IDADE_MAX_DIAS, NUM_PROCESSOS

# ---------------------------
# ETAPA 1 — PIPELINE DAS MÃES
# ---------------------------
from src.loaders.postgres_loader import carregar_prenatals_postgis
from src.processing.prenatals_filter import aplicar_filtro_validade
from src.processing.mother_preparation import preparar_maes_para_osm
from src.loaders.creches_loader import carregar_creches
from src.loaders.municipio_loader import carregar_municipio
from src.processing.osm_network import carregar_grafo_osm
from src.processing.nearest_creche import calcular_creche_mais_proxima

# ---------------------------
# ETAPA 2 — MAPAS
# ---------------------------
from src.reporting.mapas_cluster import gerar_mapa_clusters, gerar_mapa_tematico

# ---------------------------
# ETAPA 3 — GRÁFICOS DE PREVISÃO
# ---------------------------
from src.reporting.plots_previsao import gerar_previsoes_bairros

# ---------------------------
# ETAPA 4 — RELATÓRIO FINAL
# ---------------------------
from src.reporting.report_builder import gerar_relatorio_final


def pipeline_maes():

    if os.path.exists(config.MAES_SAIDA):
        print(f"[INFO] {config.MAES_SAIDA} já existe. Pulando processamento.")
        return pd.read_csv(config.MAES_SAIDA)

    print("[INFO] Executando pipeline completa de mães...")

    df = carregar_prenatals_postgis(DB_CONFIG)
    df_all, df_validos = aplicar_filtro_validade(df, DATA_CUTO, IDADE_MAX_DIAS)
    maes_gdf = preparar_maes_para_osm(df_validos)

    creches_gdf = carregar_creches()
    municipio_gdf, poligono = carregar_municipio()

    G = carregar_grafo_osm(poligono, network_type="walk")

    res_df = calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=NUM_PROCESSOS)

    res_df.to_csv(config.MAES_SAIDA, index=False)
    print(f"[OK] Resultado salvo em {config.MAES_SAIDA}")

    return res_df



def pipeline_mapas(df_maes):
    """
    Gera os mapas HTML (clusters e temático).
    """
    print("[INFO] Gerando mapas...")

    df_creches = pd.read_csv(config.CRECHES_CSV)

    # clusters
    html_clusters = gerar_mapa_clusters(df_maes, df_creches)
    with open(config.MAPAS_CLUSTERS_DIR / "clusters.html", "w", encoding="utf-8") as f:
        f.write(html_clusters)

    # temático
    html_tematico = gerar_mapa_tematico(df_maes, config.BAIRROS_GEOJSON, df_creches)
    with open(config.MAPAS_TEMATICOS_DIR / "tematico.html", "w", encoding="utf-8") as f:
        f.write(html_tematico)

    print("[OK] Mapas gerados.")


def pipeline_previsoes():
    """
    Gera os gráficos forecast ARIMA de todos os bairros.
    """
    print("[INFO] Gerando previsões (ARIMA)...")
    gerar_previsoes_bairros()
    print("[OK] Previsões geradas.")


def main():
    print("\n========== PIPELINE GEO CRECHE AI ==========")

    # 1. Pipeline das mães
    df_maes = pipeline_maes()

    # 2. Mapas
    pipeline_mapas(df_maes)

    # 3. Gráficos ARIMA
    pipeline_previsoes()

    # 4. Relatório final
    gerar_relatorio_final()

    print("\n[✔] Pipeline completa gerada!")
    print(f"Relatório final: {config.OUTPUT_DIR / 'relatorio_final.html'}\n")


if __name__ == "__main__":
    main()
