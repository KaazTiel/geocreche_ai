from datetime import datetime
import pandas as pd
from src.utils.age_utils import calcular_idade_meses, faixa_etaria

def aplicar_filtro_validade(df, data_referencia_str, idade_limite_meses=47):
    data_ref_dt = datetime.strptime(data_referencia_str, "%Y-%m-%d")
    df = df.copy()
    df["idade_meses"] = df["dum"].apply(lambda d: calcular_idade_meses(d, data_ref_dt))
    df["faixa"] = df["idade_meses"].apply(faixa_etaria)
    df.loc[df["idade_meses"].notnull() & (df["idade_meses"] > idade_limite_meses), "faixa"] = "inválido"
    validos = df[df["faixa"] != "inválido"].copy()
    return df, validos
