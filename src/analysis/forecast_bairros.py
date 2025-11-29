# src/analysis/forecast_bairros.py
import warnings
import os
import json
from pathlib import Path

import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

# use config centralizado
from src.utils import config

warnings.filterwarnings("ignore")

# ---------- parâmetros ----------
DIR_SERIES = config.INPUT_DIR / "series_bairros"
OUTPUT_FILE = config.OUTPUT_DIR / "previsoes_bairros.csv"

GRAFICOS_DIR = config.OUTPUT_DIR / "graficos_previsao"
GRAFICOS_DIR.mkdir(parents=True, exist_ok=True)

# novos horizontes de previsão
HORIZONTES = [12, 24, 36]

ARIMA_ORDER = (1, 1, 1)
MIN_POINTS_FOR_ARIMA = 12


def fit_and_forecast_arima(ts, steps, order=(1, 1, 1)):
    model = ARIMA(ts, order=order)
    fitted = model.fit()
    forecast = fitted.forecast(steps=steps)
    return pd.Series(forecast).astype(float).reset_index(drop=True)


def fallback_forecast(ts, steps, method="last"):
    if method == "mean":
        val = float(ts.mean())
        return pd.Series([val] * steps)
    else:
        last = float(ts.iloc[-1])
        return pd.Series([last] * steps)


def run(dir_series: Path = DIR_SERIES, output_file: Path = OUTPUT_FILE):
    dir_series = Path(dir_series)
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if not dir_series.exists():
        print(f"[ERRO] Diretório de séries não encontrado: {dir_series}")
        return

    arquivos = sorted([f for f in os.listdir(dir_series) if f.endswith(".csv")])
    if not arquivos:
        print(f"[AVISO] Nenhum CSV encontrado em {dir_series}.")
        return

    print(f"[INFO] Séries encontradas: {len(arquivos)} em {dir_series}")

    resultados = []

    for arq in arquivos:
        caminho = dir_series / arq
        bairro_name = arq.replace("serie_", "").replace(".csv", "").replace("_", " ").strip()

        try:
            df = pd.read_csv(caminho)
        except Exception as e:
            print(f"[ERRO] Falha ao ler {caminho}: {e}")
            continue

        if "date" not in df.columns or "value" not in df.columns:
            print(f"[WARN] Arquivo {arq} ignorado — colunas esperadas: date,value")
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        if df.empty:
            print(f"[WARN] Série vazia após limpeza: {arq}")
            continue

        ts = df.set_index("date")["value"].astype(float)

        # --- previsão ARIMA ou fallback ---
        previsoes = {}
        metodo_usado = "arima"

        for H in HORIZONTES:
            try:
                if len(ts) >= MIN_POINTS_FOR_ARIMA:
                    previsoes[H] = fit_and_forecast_arima(ts, steps=H, order=ARIMA_ORDER)
                else:
                    raise ValueError("Série curta para ARIMA (fallback).")
            except Exception as e:
                metodo_usado = "fallback"
                previsoes[H] = fallback_forecast(ts, steps=H, method="last")

        try:
            atual = int(round(float(ts.iloc[-1])))
        except Exception:
            atual = None

        out = {
            "neighborhood": bairro_name,
            "valor_atual": atual,
            "method": metodo_usado,
            "n_obs": len(ts),
        }

        # extrair últimos valores de cada horizonte
        for H in HORIZONTES:
            serie = previsoes[H]
            out[f"prev_{H}_meses"] = int(round(float(serie.iloc[-1])))
            out[f"forecast_list_{H}"] = json.dumps([float(x) for x in serie.tolist()])

        resultados.append(out)

        print(f"[OK] {bairro_name} | obs={len(ts)} | metodo={metodo_usado} | prev36={out['prev_36_meses']}")

    df_out = pd.DataFrame(resultados).sort_values("neighborhood")
    df_out.to_csv(output_file, index=False)
    print(f"[SUCESSO] Previsões salvas em: {output_file}")


if __name__ == "__main__":
    run()
