import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from sklearn.cluster import KMeans

from src.utils import config
from src.processing.spatial_join import adicionar_bairro
from src.utils.env_loader import MAPBOX_TOKEN


# ======================================================
# PROCESSING - ENSURE NEIGHBORHOODS
# ======================================================

def _garantir_bairros(df: pd.DataFrame, bairros_geojson: Path) -> pd.DataFrame:
    """
    Ensures the `neighborhood` column exists, executing a spatial join if necessary.
    """
    if "neighborhood" not in df.columns:
        df = adicionar_bairro(df, bairros_geojson)

        # Convert from cudf/gdf GeoDataFrame to pandas if necessary
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()

    return df


# ======================================================
# PROCESSING - CLUSTERS
# ======================================================

def _calcular_clusters(        df: pd.DataFrame, 
                                n_clusters: int = 3, 
                                col: str = "distancia_metros") -> pd.DataFrame:
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=0)
    df["cluster"] = kmeans.fit_predict(df[[col]])
    centers = kmeans.cluster_centers_.flatten()
    order = centers.argsort()
    df["cluster_rank"] = df["cluster"].apply(lambda x: order.tolist().index(x))
    return df

def _calcular_media_por_bairro(df: pd.DataFrame,
                               col_group: str = "neighborhood",
                               col_value: str = "distancia_metros") -> pd.DataFrame:
    
    media = df.groupby(col_group)[col_value].mean().reset_index()
    media.columns = [col_group, "media_distancia"]
    return media


# ======================================================
# CLUSTER MAP
# ======================================================

def gerar_mapa_clusters(df_maes: pd.DataFrame, df_creches: pd.DataFrame) -> str:
    """
    Generates the cluster map and returns HTML.
    """

    # --------------------------
    # PROCESSING
    # --------------------------

    # Ensure neighborhoods
    df_maes = _garantir_bairros(df_maes, config.BAIRROS_GEOJSON)

    # Calculate clusters
    df_maes = _calcular_clusters(df_maes, n_clusters=3, col="distancia_metros")

    # --------------------------
    # VISUALIZATION
    # --------------------------

    colors = ["green", "orange", "red"]

    fig = px.scatter_mapbox(
        df_maes,
        lat="lat",
        lon="lon",
        color="cluster_rank",
        color_continuous_scale=colors,
        hover_data=["distancia_metros", "neighborhood"],
        mapbox_style="carto-positron"
        # ❗️ REMOVED: name="Clusters" (does not exist in PX)
    )

    # Daycare centers
    fig.add_scattermapbox(
        lat=df_creches["Latitude"],
        lon=df_creches["Longitude"],
        mode="markers",
        marker=dict(size=15, symbol="circle", color="blue"),
        hovertext=df_creches["Nome"],
        hoverinfo="text",
        name="Daycare Centers"
    )

    fig.update_layout(
        autosize=True,  # Permite que o gráfico preencha o container
        height=None,    # Remove a altura fixa
        width=None,     # Remove a largura fixa
        margin=dict(l=0, r=0, t=40, b=0), # Aproveita melhor o espaço
        # ... resto do código
    )
    # Ao converter para HTML, adicione responsive=True
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


# ======================================================
# THEMATIC MAP BY NEIGHBORHOOD
# ======================================================

def gerar_mapa_tematico(df_maes: pd.DataFrame,
                        bairros_geojson: Path,
                        df_creches: pd.DataFrame) -> str:
    """
    Generates the thematic map by neighborhood and returns HTML.
    """

    # --------------------------
    # PROCESSING
    # --------------------------

    # Ensure neighborhoods
    df_maes = _garantir_bairros(df_maes, bairros_geojson)

    # Calculate averages
    media = _calcular_media_por_bairro(
        df_maes,
        col_group="neighborhood",
        col_value="distancia_metros"
    )

    # --------------------------
    # GEOMETRY
    # --------------------------

    bairros = gpd.read_file(bairros_geojson).to_crs(4326)
    bairros["neighborhood"] = bairros["NM_BAIRRO"].str.strip()

    bairros = bairros.merge(media, on="neighborhood", how="left")
    bairros_json = bairros.__geo_interface__

    # --------------------------
    # VISUALIZATION
    # --------------------------

    fig = go.Figure(go.Choroplethmapbox(
        geojson=bairros_json,
        locations=bairros["neighborhood"],
        z=bairros["media_distancia"],
        colorscale="Reds",
        marker_opacity=0.7,
        marker_line_width=1,
        featureidkey="properties.neighborhood",
        name="Average Distance",
    ))

    # Daycare centers
    fig.add_scattermapbox(
        lat=df_creches["Latitude"],
        lon=df_creches["Longitude"],
        mode="markers",
        marker=dict(size=15, symbol="circle", color="blue"),
        hovertext=df_creches["Nome"],
        hoverinfo="text",
        name="Daycare Centers"
    )

    fig.update_layout(
        autosize=True,  # Permite que o gráfico preencha o container
        height=None,    # Remove a altura fixa
        width=None,     # Remove a largura fixa
        margin=dict(l=0, r=0, t=40, b=0), # Aproveita melhor o espaço
        # ... resto do código
    )
    # Ao converter para HTML, adicione responsive=True
    return fig.to_html(full_html=False, include_plotlyjs="cdn")