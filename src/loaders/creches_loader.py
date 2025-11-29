import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from src.utils.config import CRECHES_CSV

def carregar_creches(path=None):
    path = path or CRECHES_CSV
    df = pd.read_csv(path)
    df = df.dropna(subset=["Latitude", "Longitude"])
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326")
    return gdf
