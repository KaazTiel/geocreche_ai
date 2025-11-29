import os
import geopandas as gpd
from src.utils import config
from src.loaders.postgres_loader import carregar_prenatals_postgis
from src.processing.prenatals_filter import aplicar_filtro_validade
from src.processing.mother_preparation import preparar_maes_para_osm
from src.loaders.creches_loader import carregar_creches
from src.loaders.municipio_loader import carregar_municipio
from src.processing.osm_network import carregar_grafo_osm
from src.processing.nearest_creche import calcular_creche_mais_proxima


def main():
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # ---------------------------------------------------------
    # 1. SE O ARQUIVO DE MÃES JÁ EXISTE → PULAR TODO PROCESSAMENTO
    # ---------------------------------------------------------
    if os.path.exists(config.MAES_SAIDA):
        print(f"[INFO] Arquivo {config.MAES_SAIDA} encontrado. Pulando processamento pesado...")
        maes_gdf = gpd.read_file(config.MAES_SAIDA)
        print("[OK] Dados de mães carregados diretamente.")
        return

    print("[INFO] Arquivo de mães não encontrado. Executando pipeline completo...")

    # DB config
    db_config = {
        "dbname": "geocrecheai-prenataldb",
        "user": "postgres",
        "password": "Meudodoi@21",
        "host": "localhost",
        "port": "5433"
    }

    # ---------------------------------------------------------
    # 2. PROCESSAMENTO COMPLETO (só roda se o arquivo não existir)
    # ---------------------------------------------------------
    df = carregar_prenatals_postgis(db_config)
    df_all, df_validos = aplicar_filtro_validade(df, "2025-11-25", 47)
    maes_gdf = preparar_maes_para_osm(df_validos)

    creches_gdf = carregar_creches()
    municipio_gdf, poligono = carregar_municipio()

    G = carregar_grafo_osm(poligono, network_type="walk")

    res_df = calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=6)

    # salvar resultado final
    out_df = res_df[["mother_id", "n_children", "lat", "lon", "geometry",
                     "creche_mais_proxima", "distancia_metros"]]
    
    out_df.to_csv(config.MAES_SAIDA, index=False)
    print(f"[OK] Arquivo gerado em {config.MAES_SAIDA}")


if __name__ == "__main__":
    main()
