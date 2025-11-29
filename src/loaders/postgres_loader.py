import pandas as pd
import psycopg2
from src.utils.config import *

def carregar_prenatals_postgis(db_config):
    conn = psycopg2.connect(**db_config)
    sql = "SELECT prenatal_id, mother_id, dum, lat, lon FROM prenatals;"
    df = pd.read_sql(sql, conn)
    conn.close()
    return df
