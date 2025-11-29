import geopandas as gpd
import pandas as pd
from pathlib import Path

def adicionar_bairro(df_maes: pd.DataFrame, geo_bairros: Path) -> pd.DataFrame:
    """Atribui o bairro correto para cada mãe baseado no GEOJSON dos bairros."""

    # Criar GeoDataFrame com pontos
    gdf = gpd.GeoDataFrame(
        df_maes.copy(),
        geometry=gpd.points_from_xy(df_maes.lon, df_maes.lat),
        crs="EPSG:4326"
    )

    # Carregar bairros
    bairros = gpd.read_file(geo_bairros)
    bairros = bairros.to_crs("EPSG:4326")
    bairros["neighborhood"] = bairros["NM_BAIRRO"].str.strip()

    # Spatial join
    joined = gpd.sjoin(
        gdf,
        bairros[["neighborhood", "geometry"]],
        how="left",
        predicate="within"
    )

    # Retornar df normal
    df_maes["neighborhood"] = joined["neighborhood"]

    return df_maes
