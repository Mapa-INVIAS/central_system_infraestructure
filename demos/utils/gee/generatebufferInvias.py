import requests, warnings
import geopandas as gpd
import pandas as pd
from pathlib import Path
from tqdm import tqdm


class generar_buffer_invias():

    warnings.filterwarnings(
        "ignore",
        message="Column names longer than 10 characters*",
        category=UserWarning
    )

    warnings.filterwarnings(
        "ignore",
        message="Normalized/laundered field name*",
        category=RuntimeWarning
    )

    def __init__(self, url, carpeta_salida, nombre_salida, buffer_m=200):
        self.url = url
        self.carpeta_salida = Path(carpeta_salida)
        self.nombre_salida = nombre_salida
        self.buffer_m = buffer_m

        self.carpeta_salida.mkdir(parents=True, exist_ok=True)
        self.shp_path = self.carpeta_salida / self.nombre_salida

        self.id_capa_vias = 14
        self.timeout_s = 120
        self.where = "1=1"
        self.out_fields = "*"
        self.out_sr = 9377

    def ejecutar(self):

        gdf = self._descargar_vias_desde_arcgis_rest()

        gdf = gdf.to_crs(epsg=9377)

        with tqdm(total=2, desc="Procesando buffer INVÍAS") as pbar:

            gdf["geometry"] = gdf.buffer(self.buffer_m)
            gdf = gdf.dissolve()
            pbar.update(1)

            gdf = gdf.to_crs(epsg=4326)
            gdf.to_file(self.shp_path, driver="ESRI Shapefile")
            pbar.update(1)

        print(f"Shapefile creado exitosamente en: {self.shp_path}")

    def _descargar_vias_desde_arcgis_rest(self) -> gpd.GeoDataFrame:

        layer_url = f"{self.url.rstrip('/')}/{self.id_capa_vias}"
        query_url = f"{layer_url}/query"

        info = requests.get(layer_url, params={"f": "pjson"}, timeout=self.timeout_s)
        info.raise_for_status()
        info_json = info.json()

        max_rc = int(info_json.get("maxRecordCount", 2000))
        oid_field = info_json.get("objectIdField", "OBJECTID")

        base_params = {
            "where": self.where,
            "outFields": self.out_fields,
            "returnGeometry": "true",
            "f": "geojson",
            "outSR": str(self.out_sr),
        }

        gdfs = []
        offset = 0

        with tqdm(desc="Descargando Vías (IGAC)", unit="page") as pbar:
            while True:
                params = {
                    **base_params,
                    "resultOffset": str(offset),
                    "resultRecordCount": str(max_rc),
                    "orderByFields": oid_field
                }

                r = requests.post(query_url, data=params, timeout=self.timeout_s)
                r.raise_for_status()
                data = r.json()

                features = data.get("features", [])
                if not features:
                    break

                gdf_part = gpd.GeoDataFrame.from_features(features, crs=f"EPSG:{self.out_sr}")
                gdfs.append(gdf_part)

                offset += max_rc
                pbar.update(1)

        if not gdfs:
            raise RuntimeError("La consulta no devolvió features. Revisa el 'where' o la disponibilidad del servicio.")

        df = pd.concat(gdfs, ignore_index=True)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=f"EPSG:{self.out_sr}")
        return gdf
    
    def to_dict(self): 
        return { 
            "url": self.url,
            "carpeta_salida": str(self.carpeta_salida),
            "nombre_salida": self.nombre_salida,
            "buffer_m": self.buffer_m,
            "shp_path": str(self.shp_path),
        }
