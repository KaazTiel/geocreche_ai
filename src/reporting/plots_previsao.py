import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from statsmodels.tsa.arima.model import ARIMA
import warnings

warnings.filterwarnings("ignore")

from src.utils import config

PERIODOS_PREVISAO = 36   # 3 years


def gerar_grafico_bairro(arquivo_csv: Path):

    df = pd.read_csv(arquivo_csv)
    bairro = arquivo_csv.stem.replace("serie_", "").replace("_", " ")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    ts = df.set_index("date")["value"]

    # -------- Annual Aggregation --------
    anual = ts.resample("A").last()
    anual.index = anual.index.year

    # -------- ARIMA 36 months --------
    model = ARIMA(ts, order=(2, 1, 2))
    fitted = model.fit()
    forecast = fitted.forecast(steps=PERIODOS_PREVISAO)

    last_year = anual.index[-1]

    anos_futuros = [last_year + i for i in range(1, 4)]
    previsoes_anuais = {
        anos_futuros[0]: int(forecast.iloc[11]),
        anos_futuros[1]: int(forecast.iloc[23]),
        anos_futuros[2]: int(forecast.iloc[35]),
    }
    anual_previsto = pd.Series(previsoes_anuais)

    # ------------------------------
    # HISTORY
    # ------------------------------
    anos_hist = list(anual.index)
    valores_hist = list(anual.values)

    # ------------------------------
    # FORECAST
    # ------------------------------
    anos_prev = list(anual_previsto.index)
    valores_prev = list(anual_previsto.values)

    # ----------- Chart ----------- #
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=anos_hist,
        y=valores_hist,
        mode="lines+markers",
        name="Historical",
        line=dict(width=3)
    ))

    cor_prev = "orange"

    fig.add_trace(go.Scatter(
        x=[anos_hist[-1]] + anos_prev,
        y=[valores_hist[-1]] + valores_prev,
        mode="lines+markers",
        name="Forecast (3 years)",
        line=dict(width=3, dash="dash", color=cor_prev),
        marker=dict(size=8),
    ))

    fig.data[-1].marker.color = ["rgba(0,0,0,0)"] + [cor_prev] * len(anos_prev)

    fig.update_layout(
        title=f"Annual Forecast - {bairro}",
        xaxis_title="Year",
        yaxis_title="Estimated Demand"
    )

    fig.write_html(config.GRAFICOS_PREV_DIR / f"{bairro}_previsao.html", include_plotlyjs="cdn")
    print(f"[OK] Gráfico gerado para {bairro}")

def gerar_grafico_total():
    print("\n[INFO] Gerando gráfico total agregado de todos os bairros...\n")

    # ------------------------------
    # Sum all series
    # ------------------------------
    series = []

    for arq in config.SERIES_DIR.iterdir():
        if arq.suffix == ".csv":
            df = pd.read_csv(arq)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            ts = df.set_index("date")["value"]
            series.append(ts)

    # Ensure alignment and sum
    ts_total = pd.concat(series, axis=1).sum(axis=1).sort_index()

    # -------- Annual Aggregation --------
    anual = ts_total.resample("A").last()
    anual.index = anual.index.year

    # -------- ARIMA 36 months --------
    model = ARIMA(ts_total, order=(2, 1, 2))
    fitted = model.fit()
    forecast = fitted.forecast(steps=PERIODOS_PREVISAO)

    last_year = anual.index[-1]

    anos_futuros = [last_year + i for i in range(1, 4)]
    previsoes_anuais = {
        anos_futuros[0]: int(forecast.iloc[11]),
        anos_futuros[1]: int(forecast.iloc[23]),
        anos_futuros[2]: int(forecast.iloc[35]),
    }
    anual_previsto = pd.Series(previsoes_anuais)

    # ------------------------------ #
    #   HISTORY
    # ------------------------------ #
    anos_hist = list(anual.index)
    valores_hist = list(anual.values)

    # ------------------------------ #
    #   FORECAST
    # ------------------------------ #
    anos_prev = list(anual_previsto.index)
    valores_prev = list(anual_previsto.values)

    # ----------- Chart ----------- #
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=anos_hist,
        y=valores_hist,
        mode="lines+markers",
        name="Historical (Total)",
        line=dict(width=3)
    ))

    cor_prev = "orange"

    fig.add_trace(go.Scatter(
        x=[anos_hist[-1]] + anos_prev,
        y=[valores_hist[-1]] + valores_prev,
        mode="lines+markers",
        name="Total Forecast (3 years)",
        line=dict(width=3, dash="dash", color=cor_prev),
        marker=dict(size=8),
    ))

    fig.data[-1].marker.color = ["rgba(0,0,0,0)"] + [cor_prev] * len(anos_prev)

    fig.update_layout(
        title="Annual Forecast - Total Neighborhoods",
        xaxis_title="Year",
        yaxis_title="Total Estimated Demand"
    )

    fig.write_html(config.GRAFICOS_PREV_DIR / "TOTAL_previsao.html", include_plotlyjs="cdn")

    print("\n[OK] Gráfico TOTAL gerado com sucesso!\n")


# ==========================================
#   AGGREGATOR FUNCTION (OPTION 2)
# ==========================================
def gerar_previsoes_bairros():
    print("\n[INFO] Iniciando geração de previsões para todos os bairros...\n")

    # 1) Total Chart first
    gerar_grafico_total()

    # 2) Individual Charts
    for arq in config.SERIES_DIR.iterdir():
        if arq.suffix == ".csv":
            gerar_grafico_bairro(arq)

    print("\n[INFO] Finalizado! Todos os gráficos foram gerados.\n")