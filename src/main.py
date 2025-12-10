import os
import sys
import time
import pandas as pd
import geopandas as gpd
import argparse

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


# ---- Função auxiliar para tempo ----
def log_tempo(inicio, mensagem):
    dur = time.time() - inicio

    mins = int(dur // 60)
    secs = dur % 60  # segundos com fração
    secs_fmt = f"{secs:05.2f}"

    if mins > 0:
        print(f"[OK] {mensagem} em {mins} min {secs_fmt}s.")
    else:
        print(f"[OK] {mensagem} em {secs_fmt}s.")


def verificar_limite_processos(valor):
    """Verifica se o valor excede o limite e aborta se sim."""
    max_procs = os.cpu_count() or 1

    if valor > max_procs:
        print("\n[ERRO] Número de processos solicitado excede o limite permitido.")
        print(f"       Solicitado: {valor}")
        print(f"       Máximo permitido: {max_procs}\n")
        print("       Execução abortada para evitar travamento do sistema.\n")
        sys.exit(1)

    if valor < 1:
        print("\n[ERRO] Número de processos inválido (<1).")
        print("       Execução abortada.\n")
        sys.exit(1)


def pipeline_maes():

    if os.path.exists(config.MAES_SAIDA):
        print(f"[INFO] {config.MAES_SAIDA} já existe. Pulando processamento.")
        return pd.read_csv(config.MAES_SAIDA)

    print("\n--- ETAPA 1: PIPELINE DAS MÃES ---")
    t0 = time.time()

    # 1. Carregar dados do Postgres
    t = time.time()
    df = carregar_prenatals_postgis(DB_CONFIG)
    log_tempo(t, "Carregamento de dados do Postgres")

    # 2. Filtrar validade (idade, etc.)
    t = time.time()
    _, df_validos = aplicar_filtro_validade(df, DATA_CUTO, IDADE_MAX_DIAS)
    log_tempo(t, "Aplicação de filtros de validade")

    # 3. Preparar mães para geoprocessamento
    t = time.time()
    maes_gdf = preparar_maes_para_osm(df_validos)
    log_tempo(t, "Preparação de geodados das mães")

    # 4. Carregar creches
    t = time.time()
    creches_gdf = carregar_creches()
    log_tempo(t, "Carregamento de dados das creches")

    # 5. Carregar polígono e grafo OSM
    t = time.time()
    _, poligono = carregar_municipio()
    G = carregar_grafo_osm(poligono, network_type="walk")
    log_tempo(t, "Carregamento do grafo OSM")

    # 6. Calcular creche mais próxima
    t = time.time()
    res_df = calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=NUM_PROCESSOS)
    log_tempo(t, "Cálculo de creche mais próxima (network)")

    # 7. Salvar resultado
    t = time.time()
    res_df.to_csv(config.MAES_SAIDA, index=False)
    log_tempo(t, "Salvamento do arquivo final")

    log_tempo(t0, "ETAPA 1 — Pipeline das mães concluída total")

    return res_df


def pipeline_mapas(df_maes):
    print("\n--- ETAPA 2: MAPAS ---")
    t0 = time.time()

    # 1. carregar creches
    t = time.time()
    df_creches = pd.read_csv(config.CRECHES_CSV)
    log_tempo(t, "Carregamento de creches para mapas")

    # 2. mapa de clusters
    t = time.time()
    html_clusters = gerar_mapa_clusters(df_maes, df_creches)
    with open(config.MAPAS_CLUSTERS_DIR / "clusters.html", "w", encoding="utf-8") as f:
        f.write(html_clusters)
    log_tempo(t, "Geração de mapa de clusters")

    # 3. mapa temático
    t = time.time()
    html_tematico = gerar_mapa_tematico(df_maes, config.BAIRROS_GEOJSON, df_creches)
    with open(config.MAPAS_TEMATICOS_DIR / "tematico.html", "w", encoding="utf-8") as f:
        f.write(html_tematico)
    log_tempo(t, "Geração de mapa temático")

    log_tempo(t0, "ETAPA 2 — Mapas concluída total")


def pipeline_previsoes():
    print("\n--- ETAPA 3: PREVISÕES ---")
    t0 = time.time()

    t = time.time()
    gerar_previsoes_bairros()
    log_tempo(t, "Geração dos gráficos ARIMA")

    log_tempo(t0, "ETAPA 3 — Previsões concluída total")


def main():
    print("\n========== PIPELINE GEO CRECHE AI ==========")

    # -------------------------------------------------
    # CLI: permitir sobrescrever NUM_PROCESSOS
    # -------------------------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--processos",
        type=int,
        help="Número de processos para paralelismo (sobrescreve o .env)"
    )
    args = parser.parse_args()

    # Importa variável global
    global NUM_PROCESSOS

    # Se usuário passou -p
    if args.processos is not None:
        verificar_limite_processos(args.processos)
        NUM_PROCESSOS = args.processos
    else:
        verificar_limite_processos(NUM_PROCESSOS)

    print(f"[INFO] Número de processos configurado: {NUM_PROCESSOS}")

    # -------------------------------------------------
    # PIPELINE
    # -------------------------------------------------

    # 1. Pipeline das mães
    df_maes = pipeline_maes()

    # 2. Mapas
    pipeline_mapas(df_maes)

    # 3. Gráficos ARIMA
    pipeline_previsoes()

    # 4. Relatório final
    print("\n--- ETAPA 4: RELATÓRIO ---")
    t = time.time()
    gerar_relatorio_final()
    log_tempo(t, "Relatório final gerado")

    print("\n[✔] Pipeline completa gerada!")
    print(f"Relatório final: {config.OUTPUT_DIR / 'relatorio_final.html'}\n")


if __name__ == "__main__":
    main()
