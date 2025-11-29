import geopandas as gpd
from src.utils.config import MUNICIPIO_GEOJSON

def carregar_municipio(path=None):
    path = path or MUNICIPIO_GEOJSON
    gdf = gpd.read_file(path).to_crs(epsg=4326)
    poligono = gdf.geometry.unary_union
    return gdf, poligono
