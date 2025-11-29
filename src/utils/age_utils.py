import pandas as pd
from datetime import datetime

def calcular_idade_meses(dum, data_ref_dt):
    if pd.isna(dum):
        return None
    try:
        dum_dt = pd.to_datetime(dum)
    except Exception:
        return None
    delta = data_ref_dt - dum_dt
    return int(delta.days // 30)

def faixa_etaria(idade_meses):
    if idade_meses is None or idade_meses < 0:
        return "inválido"
    anos = idade_meses // 12
    if anos < 1: return "0 anos"
    if anos < 2: return "1 ano"
    if anos < 3: return "2 anos"
    if anos < 4: return "3 anos"
    return "inválido"
