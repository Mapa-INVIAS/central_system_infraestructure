import warnings
import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path
from tqdm import tqdm


class DescargaViasIGAC:

    def __init__(self, url, carpeta_salida, nombre_salida):

        self.url = url.rstrip("/")
        self.carpeta_salida = Path(carpeta_salida)
        self.nombre_salida = nombre_salida

        self.carpeta_salida.mkdir(parents=True, exist_ok=True)
        self.shp_path = self.carpeta_salida / self.nombre_salida

        self.layer_id = 15
        self.query_url = f"{self.url}/{self.layer_id}/query"

        self.timeout_s = 120
        self.where = "1=1"
        self.out_fields = "*"

        self.page_size = 2000

    def _silenciar_warnings(self) -> None:
        warnings.filterwarnings("ignore", message=".*Column names longer than.*")
        warnings.filterwarnings("ignore", message=".*Normalized/laundered field name.*")
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.filterwarnings("ignore", category=RuntimeWarning)

    @staticmethod
    def _acortar_campos_shp(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

        cols = list(gdf.columns)
        if "geometry" in cols:
            cols.remove("geometry")

        usados = set()
        ren = {}

        for c in cols:
            base = "".join(ch if ch.isalnum() else "_" for ch in str(c).upper())
            base = base[:10] if base else "FIELD"

            cand = base
            i = 1
            while cand in usados:
                suf = str(i)
                cand = (base[:10 - len(suf)] + suf)[:10]
                i += 1

            usados.add(cand)
            ren[c] = cand

        return gdf.rename(columns=ren)

    def _request_json(self, params: dict) -> dict:
        r = requests.get(self.query_url, params=params, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"Error ArcGIS REST: {data['error']}")
        return data

    def _obtener_total_registros(self) -> int:
        params = {
            "where": self.where,
            "returnCountOnly": "true",
            "f": "json"
        }
        data = self._request_json(params)
        return int(data.get("count", 0))

    def _descargar_vias_paginado(self) -> gpd.GeoDataFrame:
        total = self._obtener_total_registros()
        if total == 0:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

        gdfs = []

        with tqdm(total=total, desc="Descargando vías IGAC", unit="feat") as pbar:
            for offset in range(0, total, self.page_size):
                params = {
                    "where": self.where,
                    "outFields": self.out_fields,
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "resultOffset": str(offset),
                    "resultRecordCount": str(self.page_size),
                    "f": "geojson"
                }

                r = requests.get(self.query_url, params=params, timeout=self.timeout_s)
                r.raise_for_status()
                geojson = r.json()

                feats = geojson.get("features", [])
                if not feats:
                    continue

                gdf_part = gpd.GeoDataFrame.from_features(feats)
                gdf_part = gdf_part.set_crs(epsg=4326, allow_override=True)

                gdfs.append(gdf_part)
                pbar.update(len(gdf_part))

        if not gdfs:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

        gdf = pd.concat(gdfs, ignore_index=True)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
        return gdf

    def ejecutar(self):

        self._silenciar_warnings()

        gdf = self._descargar_vias_paginado()

        gdf = self._acortar_campos_shp(gdf)

        with tqdm(total=1, desc="Guardando shapefile") as pbar:
            gdf.to_file(self.shp_path, driver="ESRI Shapefile")
            pbar.update(1)

        print(f"Shapefile creado exitosamente: {self.shp_path}")

