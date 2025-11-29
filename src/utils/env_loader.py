import os
from dotenv import load_dotenv

# Carrega o .env
load_dotenv()

def get_env(key: str, default=None):
    """
    Retorna uma variável do .env ou default se não existir.
    """
    return os.getenv(key, default)


# ---------------------------
# VARIÁVEIS DE BANCO
# ---------------------------
DB_CONFIG = {
    "dbname": get_env("DB_NAME"),
    "user": get_env("DB_USER"),
    "password": get_env("DB_PASSWORD"),
    "host": get_env("DB_HOST"),
    "port": get_env("DB_PORT"),
}

# ---------------------------
# PARAMETROS DE PIPELINE
# ---------------------------
DATA_CUTO = get_env("DATA_CUTO", "2025-11-25")
IDADE_MAX_DIAS = int(get_env("IDADE_MAX_DIAS", "47"))
NUM_PROCESSOS = int(get_env("NUM_PROCESSOS", "4"))

# ----------------------------
# TOKENS E CHAVES DE API
# ----------------------------
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")