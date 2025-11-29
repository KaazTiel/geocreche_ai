from pathlib import Path

# ============================
# BASE FOLDERS
# ============================

# raiz do projeto → geocreche_ai/
BASE_DIR = Path(__file__).resolve().parents[2]

# data/
DATA_DIR = BASE_DIR / "data"

# data/input/
INPUT_DIR = DATA_DIR / "input"

# data/output/
OUTPUT_DIR = DATA_DIR / "output"

# Garantir que as pastas existem
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================
# INPUT FILES
# ============================

# creches geocodificadas
CRECHES_CSV = INPUT_DIR / "creches_caxias_geocodificado_bkp.csv"

# polígono do município
MUNICIPIO_GEOJSON = INPUT_DIR / "caxias_municipio.geojson"

# bairros
BAIRROS_GEOJSON = INPUT_DIR / "caxias_bairros_corrigido.geojson"

# ============================
# OUTPUT FILES
# ============================

# resultado final das mães
MAES_SAIDA = OUTPUT_DIR / "maes_resultado.csv"

# (opcional) resultados intermediários
MAES_VALIDAS_CSV = OUTPUT_DIR / "maes_validas.csv"
MAES_PREP_OSM_CSV = OUTPUT_DIR / "maes_preparadas_para_osm.csv"

# ============================
# SÉRIES HISTÓRICAS E RELATÓRIOS
# ============================

# Diretório onde ficam as séries históricas geradas (um CSV por bairro)
SERIES_DIR = INPUT_DIR / "series_bairros"

# Diretório dos gráficos de previsão anual (Plotly)
GRAFICOS_PREV_DIR = OUTPUT_DIR / "graficos_previsao"
GRAFICOS_PREV_DIR.mkdir(parents=True, exist_ok=True)

# ============================
# MAPAS (clusters e temáticos)
# ============================

MAPAS_DIR = OUTPUT_DIR / "mapas"
MAPAS_DIR.mkdir(parents=True, exist_ok=True)

MAPAS_CLUSTERS_DIR = MAPAS_DIR / "clusters"
MAPAS_CLUSTERS_DIR.mkdir(parents=True, exist_ok=True)

MAPAS_TEMATICOS_DIR = MAPAS_DIR / "tematicos"
MAPAS_TEMATICOS_DIR.mkdir(parents=True, exist_ok=True)


# ============================
# Helper: garantir strings p/ pandas/geopandas
# ============================

def to_str(path: Path) -> str:
    """Converte Path → string (necessário para pandas, geopandas, OSMnx)."""
    return str(path)
