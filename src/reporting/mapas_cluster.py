import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px
from sklearn.cluster import KMeans
from pathlib import Path

from src.utils import config
from src.processing.spatial_join import adicionar_bairro  # ⬅️ SUA FUNÇÃO NOVA

from src.utils.env_loader import MAPBOX_TOKEN

# ======================================================
# CLUSTERS
# ======================================================
def gerar_mapa_clusters(df_maes: pd.DataFrame, df_creches: pd.DataFrame) -> str:
    """Gera o mapa de clusters e retorna HTML."""

    # --------------------------
    # GARANTIR BAIRROS
    # --------------------------
    if "neighborhood" not in df_maes.columns:
        df_maes = adicionar_bairro(df_maes, config.BAIRROS_GEOJSON)
        # garante string normal p/ pandas
        if hasattr(df_maes, "to_pandas"):
            df_maes = df_maes.to_pandas()

    # --------------------------
    # CLUSTERS
    # --------------------------
    kmeans = KMeans(n_clusters=3, random_state=0)
    df_maes["cluster"] = kmeans.fit_predict(df_maes[["distancia_metros"]])

    centers = kmeans.cluster_centers_.flatten()
    order = centers.argsort()
    df_maes["cluster_rank"] = df_maes["cluster"].apply(lambda x: order.tolist().index(x))

    colors = ["green", "orange", "red"]

    fig = px.scatter_mapbox(
        df_maes,
        lat="lat",
        lon="lon",
        color="cluster_rank",
        color_continuous_scale=colors,
        hover_data=["distancia_metros", "neighborhood"],
        mapbox_style="carto-positron"
    )

    # --------------------------
    # CRECHES
    # --------------------------
    fig.add_scattermapbox(
        lat=df_creches["Latitude"],
        lon=df_creches["Longitude"],
        mode="markers",
        marker=dict(size=15, symbol="circle", color="blue"),
        hovertext=df_creches["Nome"],
        hoverinfo="text",
        name="Creches"
    )

    fig.update_layout(
        title="Clusters de Prioridade de Distância + Localização das Creches",
        mapbox_accesstoken=MAPBOX_TOKEN,
        mapbox=dict(
            center=dict(lat=df_maes.lat.mean(), lon=df_maes.lon.mean()),
            zoom=12
        )
    )

    return fig.to_html(full_html=False, include_plotlyjs="cdn")



# ======================================================
# MAPA TEMÁTICO POR BAIRRO
# ======================================================
def gerar_mapa_tematico(df_maes: pd.DataFrame, bairros_geojson: Path, df_creches: pd.DataFrame) -> str:
    """Gera o mapa temático por bairro e retorna HTML."""

    # --------------------------
    # GARANTIR BAIRROS
    # --------------------------
    if "neighborhood" not in df_maes.columns:
        df_maes = adicionar_bairro(df_maes, bairros_geojson)
        if hasattr(df_maes, "to_pandas"):
            df_maes = df_maes.to_pandas()

    # --------------------------
    # MÉDIA POR BAIRRO
    # --------------------------
    media = df_maes.groupby("neighborhood")["distancia_metros"].mean().reset_index()
    media.columns = ["neighborhood", "media_distancia"]

    bairros = gpd.read_file(bairros_geojson).to_crs(4326)
    bairros["neighborhood"] = bairros["NM_BAIRRO"].str.strip()

    bairros = bairros.merge(media, on="neighborhood", how="left")
    bairros_json = bairros.__geo_interface__

    # --------------------------
    # CHOROPLETH
    # --------------------------
    fig = go.Figure(go.Choroplethmapbox(
        geojson=bairros_json,
        locations=bairros["neighborhood"],
        z=bairros["media_distancia"],
        colorscale="Reds",
        marker_opacity=0.7,
        marker_line_width=1,
        featureidkey="properties.neighborhood",
    ))

    # --------------------------
    # CRECHES
    # --------------------------
    fig.add_scattermapbox(
        lat=df_creches["Latitude"],
        lon=df_creches["Longitude"],
        mode="markers",
        marker=dict(size=15, symbol="circle", color="blue"),
        hovertext=df_creches["Nome"],
        hoverinfo="text",
        name="Creches"
    )

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_accesstoken=MAPBOX_TOKEN,
        mapbox=dict(
            center=dict(lat=df_maes.lat.mean(), lon=df_maes.lon.mean()),
            zoom=12
        ),
        title="Mapa Temático – Média de Distância por Bairro + Creches"
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)
