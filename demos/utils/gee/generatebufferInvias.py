import requests
import geopandas as gpd
from io import BytesIO
from pathlib import Path
from django.conf import settings
from tqdm import tqdm

def generar_buffer_invias():

    url = "https://storage.googleapis.com/invias/maps_invias/dem_colombia/RedVialODAGOL_-7622711643947703228.geojson"
    carpeta_salida = Path(settings.MEDIA_ROOT) / "INVIAS"
    nombre_salida = "vias_invias.shp"

    carpeta_salida.mkdir(parents=True, exist_ok=True)
    shp_path = carpeta_salida / nombre_salida

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    gdf = gpd.read_file(BytesIO(response.content))

    gdf = gdf[gdf.geometry.notnull()]

    gdf = gdf.to_crs(epsg=9377)

    buffered_geoms = []
    for geom in tqdm(gdf.geometry, desc="Aplicando buffer", unit="geom"):
        if geom is not None:
            buffered_geoms.append(geom.buffer(200))

    gdf = gpd.GeoDataFrame(geometry=buffered_geoms, crs="EPSG:9377")

    gdf = gdf.dissolve()

    gdf = gdf.to_crs(epsg=4326)
    gdf.to_file(shp_path, driver="ESRI Shapefile")

    archivo_url = settings.MEDIA_URL + f"INVIAS/{nombre_salida}"
    return archivo_url
