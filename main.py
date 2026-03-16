from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import pandas as pd
import psycopg2
import os
import sys
from pathlib import Path

# CONFIGURACAO DE CAMINHO
current_file = Path(__file__).resolve()
project_root = current_file.parent 
sys.path.append(str(project_root))

from src.utils import config
from src.utils.env_loader import DATA_CUTO, IDADE_MAX_DIAS
from src.utils.config import GRAFICOS_PREV_DIR, MAPAS_CLUSTERS_DIR, MAPAS_TEMATICOS_DIR, OUTPUT_DIR

from src.processing.prenatals_filter import aplicar_filtro_validade
from src.processing.mother_preparation import preparar_maes_para_osm
from src.loaders.creches_loader import carregar_creches
from src.loaders.municipio_loader import carregar_municipio
from src.processing.osm_network import carregar_grafo_osm
from src.processing.nearest_creche import calcular_creche_mais_proxima
from src.reporting.mapas_cluster import gerar_mapa_clusters, gerar_mapa_tematico
from src.reporting.plots_previsao import gerar_previsoes_bairros
from src.reporting.report_builder import gerar_relatorio_final

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="GeoCreche AI API")

# Configuração do CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Em produção, substitua pelo IP/URL do seu front
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Modelos de entrada para substituir a Sidebar do Streamlit
class DBConfig(BaseModel):
    host: str = "localhost"
    port: str = "5432"
    dbname: str
    user: str = "postgres"
    password: str

class QueryConfig(BaseModel):
    table: str = "prenatals"
    col_id: str = "prenatal_id"
    col_mother: str = "mother_id"
    col_date: str = "dum"
    col_lat: str = "lat"
    col_lon: str = "lon"

class RelatorioRequest(BaseModel):
    db_params: DBConfig
    query_config: QueryConfig

def carregar_dados_do_banco(db_params: dict, query_config: dict):
    try:
        conn = psycopg2.connect(**db_params)
        sql = f"""
            SELECT 
                {query_config.get("col_id")} as prenatal_id, 
                {query_config.get("col_mother")} as mother_id, 
                {query_config.get("col_date")} as dum, 
                {query_config.get("col_lat")} as lat, 
                {query_config.get("col_lon")} as lon 
            FROM {query_config.get("table")};
        """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        raise Exception(f"Erro ao conectar ao banco: {str(e)}")

@app.post("/get-schema")
def get_schema(params: DBConfig):
    try:
        conn = psycopg2.connect(
            host=params.host,
            port=params.port,
            database=params.dbname,
            user=params.user,
            password=params.password,
            connect_timeout=5 # Timeout curto para não travar a API
        )
        cursor = conn.cursor()

        # 1. Buscar Tabelas
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cursor.fetchall()]

        # 2. Buscar Colunas (de todas as tabelas para facilitar o cache no front)
        schema_info = {}
        for table in tables:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
            schema_info[table] = [row[0] for row in cursor.fetchall()]

        conn.close()
        return {"status": "success", "schema": schema_info}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/gerar_relatorio", response_class=HTMLResponse)
async def gerar_relatorio_api(request: RelatorioRequest):
    try:
        db_dict = request.db_params.dict()
        query_dict = request.query_config.dict()
        
        # Etapa 1 Processamento
        df = carregar_dados_do_banco(db_dict, query_dict)
        if df is None or df.empty:
            raise HTTPException(status_code=400, detail="Nenhum dado retornado do banco.")

        _, df_validos = aplicar_filtro_validade(df, DATA_CUTO, IDADE_MAX_DIAS)
        maes_gdf = preparar_maes_para_osm(df_validos)
        creches_gdf = carregar_creches()
        _, poligono = carregar_municipio()
        
        G = carregar_grafo_osm(poligono)
        
        # Removido o slider de CPU, fixando um valor seguro para servidores cloud
        df_maes = calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=1)
        
        path_maes = config.MAES_SAIDA
        os.makedirs(os.path.dirname(path_maes), exist_ok=True)
        df_maes.to_csv(path_maes, index=False)

        # Etapa 2 Mapas
        df_creches = pd.read_csv(config.CRECHES_CSV)
        os.makedirs(MAPAS_CLUSTERS_DIR, exist_ok=True)
        os.makedirs(MAPAS_TEMATICOS_DIR, exist_ok=True)

        html_c = gerar_mapa_clusters(df_maes, df_creches)
        with open(MAPAS_CLUSTERS_DIR / "clusters.html", "w", encoding="utf-8") as f: f.write(html_c)

        html_t = gerar_mapa_tematico(df_maes, config.BAIRROS_GEOJSON, df_creches)
        with open(MAPAS_TEMATICOS_DIR / "tematico.html", "w", encoding="utf-8") as f: f.write(html_t)

        # Etapa 3 Graficos de Previsao
        os.makedirs(GRAFICOS_PREV_DIR, exist_ok=True)
        gerar_previsoes_bairros()

        # Etapa 4 Compilacao do Relatorio Final
        gerar_relatorio_final() 

        # Etapa 5 Retornar HTML via API
        relatorio_path = config.OUTPUT_DIR / "relatorio_final.html"
        if not relatorio_path.exists():
            raise HTTPException(status_code=500, detail="Falha ao gerar o relatorio.")
            
        with open(relatorio_path, "r", encoding="utf-8") as f:
            html_content = f.read()
            
        return html_content

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))