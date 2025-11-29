import pandas as pd
import geopandas as gpd

def preparar_maes_para_osm(df_validos):
    grouped = df_validos.groupby("mother_id").agg({
        "lat": lambda x: pd.Series(x).mode().iloc[0],
        "lon": lambda x: pd.Series(x).mode().iloc[0],
        "prenatal_id": "count"
    }).rename(columns={"prenatal_id": "n_children"})
    gdf = gpd.GeoDataFrame(grouped, geometry=gpd.points_from_xy(grouped["lon"], grouped["lat"]), crs="EPSG:4326")
    gdf = gdf.reset_index()
    return gdf
