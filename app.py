import streamlit as st
import pandas as pd
import psycopg2
import os
import sys
from pathlib import Path
import streamlit.components.v1 as components

# --- CONFIGURAÇÃO DE CAMINHO ---
current_file = Path(__file__).resolve()
project_root = current_file.parent 
sys.path.append(str(project_root))

# --- IMPORTAÇÕES DO PROJETO ---
from src.utils import config
from src.utils.env_loader import DB_CONFIG as DEFAULT_DB_CONFIG, DATA_CUTO, IDADE_MAX_DIAS
from src.utils.config import GRAFICOS_PREV_DIR, MAPAS_CLUSTERS_DIR, MAPAS_TEMATICOS_DIR, OUTPUT_DIR

# Steps do Pipeline
from src.processing.prenatals_filter import aplicar_filtro_validade
from src.processing.mother_preparation import preparar_maes_para_osm
from src.loaders.creches_loader import carregar_creches
from src.loaders.municipio_loader import carregar_municipio
from src.processing.osm_network import carregar_grafo_osm
from src.processing.nearest_creche import calcular_creche_mais_proxima
from src.reporting.mapas_cluster import gerar_mapa_clusters, gerar_mapa_tematico
from src.reporting.plots_previsao import gerar_previsoes_bairros
from src.reporting.report_builder import gerar_relatorio_final

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Geo Creche AI", layout="wide", page_icon="📍")

# --- FUNÇÕES DE BANCO DE DADOS ---
def get_db_connection(db_params):
    try:
        return psycopg2.connect(**db_params)
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

def listar_tabelas(db_params):
    conn = get_db_connection(db_params)
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = sorted([row[0] for row in cursor.fetchall()])
        conn.close()
        return tables
    return []

def listar_colunas(db_params, tabela):
    conn = get_db_connection(db_params)
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (tabela,))
        cols = sorted([row[0] for row in cursor.fetchall()])
        conn.close()
        return cols
    return []

def carregar_dados_do_banco(db_params, query_config):
    conn = get_db_connection(db_params)
    if conn:
        sql = f"""
            SELECT 
                {query_config.get("col_id")} as prenatal_id, 
                {query_config.get("col_mother")} as mother_id, 
                {query_config.get("col_date")} as dum, 
                {query_config.get("col_lat")} as lat, 
                {query_config.get("col_lon")} as lon 
            FROM {query_config.get("table")};
        """
        st.info(f"Executando Query...")
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    return None

# --- SIDEBAR (CONFIGURAÇÃO) ---
with st.sidebar:
    st.header("⚙️ Banco de Dados")
    db_params = {
        "host": st.text_input("Host", value=DEFAULT_DB_CONFIG.get("host", "localhost")),
        "port": st.text_input("Port", value=DEFAULT_DB_CONFIG.get("port", "5432")),
        "dbname": st.text_input("Database", value=DEFAULT_DB_CONFIG.get("dbname", "")),
        "user": st.text_input("User", value=DEFAULT_DB_CONFIG.get("user", "postgres")),
        "password": st.text_input("Password", value=DEFAULT_DB_CONFIG.get("password", ""), type="password")
    }

    if st.button("Conectar"):
        tabelas = listar_tabelas(db_params)
        if tabelas:
            st.session_state['tabelas'] = tabelas
            st.success("Conectado!")
        else:
            st.error("Erro ao listar tabelas.")

    query_config = {}
    if 'tabelas' in st.session_state:
        st.markdown("---")
        tabela = st.selectbox("Tabela", st.session_state['tabelas'])
        cols = listar_colunas(db_params, tabela)
        if cols:
            query_config = {
                "table": tabela,
                "col_id": st.selectbox("ID (PK)", cols, index=0),
                "col_mother": st.selectbox("ID Mãe", cols, index=min(1, len(cols)-1)),
                "col_date": st.selectbox("Data (DUM)", cols, index=min(2, len(cols)-1)),
                "col_lat": st.selectbox("Latitude", cols, index=min(3, len(cols)-1)),
                "col_lon": st.selectbox("Longitude", cols, index=min(4, len(cols)-1))
            }

# --- ÁREA PRINCIPAL ---
st.title("Geo Creche AI - Pipeline Manager")

# Definição do caminho do relatório final
RELATORIO_PATH = config.OUTPUT_DIR / "relatorio_final.html"

with st.container():
    c1, c2 = st.columns(2)
    # Ajuste de CPU para ambiente container (geralmente começa com 1 ou 2 vCPUs)
    cpu_disponivel = os.cpu_count() or 1
    num_processos = c1.slider("Processos (CPU)", 1, cpu_disponivel, max(1, int(cpu_disponivel/2)))
    sincronizar = c2.checkbox("Recalcular Tudo (Ignorar Cache)", value=False)
    
    if st.button("🚀 INICIAR PIPELINE E GERAR RELATÓRIO", type="primary", use_container_width=True):
        if not query_config and sincronizar:
            st.warning("Configure o banco na barra lateral para recalcular.")
            st.stop()

        with st.status("Executando Pipeline...", expanded=True) as status:
            try:
                # 1. Processamento
                st.write("1. Processando dados...")
                path_maes = config.MAES_SAIDA
                os.makedirs(os.path.dirname(path_maes), exist_ok=True)

                if not os.path.exists(path_maes) or sincronizar:
                    df = carregar_dados_do_banco(db_params, query_config)
                    _, df_validos = aplicar_filtro_validade(df, DATA_CUTO, IDADE_MAX_DIAS)
                    maes_gdf = preparar_maes_para_osm(df_validos)
                    creches_gdf = carregar_creches()
                    _, poligono = carregar_municipio()
                    G = carregar_grafo_osm(poligono)
                    df_maes = calcular_creche_mais_proxima(G, maes_gdf, creches_gdf, num_processos=num_processos)
                    df_maes.to_csv(path_maes, index=False)
                else:
                    df_maes = pd.read_csv(path_maes)

                # 2. Mapas
                st.write("2. Gerando arquivos de Mapas...")
                df_creches = pd.read_csv(config.CRECHES_CSV)
                
                os.makedirs(MAPAS_CLUSTERS_DIR, exist_ok=True)
                os.makedirs(MAPAS_TEMATICOS_DIR, exist_ok=True)

                html_c = gerar_mapa_clusters(df_maes, df_creches)
                with open(MAPAS_CLUSTERS_DIR / "clusters.html", "w", encoding="utf-8") as f: f.write(html_c)

                html_t = gerar_mapa_tematico(df_maes, config.BAIRROS_GEOJSON, df_creches)
                with open(MAPAS_TEMATICOS_DIR / "tematico.html", "w", encoding="utf-8") as f: f.write(html_t)

                # 3. Gráficos de Previsão
                st.write("3. Gerando Gráficos de Previsão...")
                os.makedirs(GRAFICOS_PREV_DIR, exist_ok=True)
                gerar_previsoes_bairros()

                # 4. Compilação do Relatório Final
                st.write("4. Compilando Relatório HTML Unificado...")
                gerar_relatorio_final() 

                status.update(label="Concluído! Relatório gerado.", state="complete", expanded=False)
                st.success("Processo finalizado com sucesso.")
                st.session_state['report_ready'] = True

            except Exception as e:
                st.error(f"Erro Fatal: {e}")
                st.stop()

# --- VISUALIZAÇÃO DO RELATÓRIO FINAL ---
if st.session_state.get('report_ready') or RELATORIO_PATH.exists():
    
    st.divider()
    st.header("📄 Relatório Final")
    
    if RELATORIO_PATH.exists():
        with open(RELATORIO_PATH, "rb") as f:
            st.download_button(
                label="📥 Baixar Relatório HTML Completo",
                data=f,
                file_name="relatorio_geocreche.html",
                mime="text/html"
            )

        try:
            with open(RELATORIO_PATH, 'r', encoding='utf-8') as f:
                html_report = f.read()
            components.html(html_report, height=1000, scrolling=True)
        except Exception as e:
            st.error(f"Erro ao ler o relatório: {e}")
    else:
        st.warning("O arquivo de relatório não foi encontrado. Execute o pipeline.")