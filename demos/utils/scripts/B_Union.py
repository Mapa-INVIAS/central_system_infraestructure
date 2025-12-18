# -*- coding: utf-8 -*-

import os
import math
import json
import fiona
from tqdm import tqdm
from shapely.geometry import shape, mapping
from shapely.ops import transform
from pyproj import Transformer
from concurrent.futures import ProcessPoolExecutor, as_completed
from fiona.crs import from_epsg



# WORKER (cada proceso)
def procesar_archivo_worker(args):
    ruta_archivo, buffer_grados = args
    features = []

    try:
        with fiona.open(ruta_archivo, "r") as src:

            crs_src = src.crs or "EPSG:9377"
            transformer = Transformer.from_crs(crs_src,
                                               "EPSG:4326",
                                               always_xy=True)

            for feat in src:

                if feat["geometry"] is None:
                    continue

                geom = shape(feat["geometry"])
                geom_4326 = transform(transformer.transform, geom)
                tipo = geom_4326.geom_type

                # si es línea → buffer
                if tipo in ["LineString", "MultiLineString"]:
                    geom_4326 = geom_4326.buffer(buffer_grados)

                features.append(mapping(geom_4326))

    except Exception as e:
        print(f"[ERROR] {os.path.basename(ruta_archivo)}: {e}")

    return features


# CLASE PRINCIPAL
class UnirShapefile:

    def __init__(self,
                 carpeta_entrada,
                 salida_shp,
                 buffer_metros,
                 max_workers):

        self.ejecutar(carpeta_entrada,
                      salida_shp,
                      buffer_metros,
                      max_workers)


    def ejecutar(self,
                 carpeta_entrada,
                 salida_shp,
                 buffer_metros,
                 max_workers):

        archivos = [f for f in os.listdir(carpeta_entrada)
                    if f.lower().endswith(".geojson")]

        if not archivos:
            raise RuntimeError("No se encontraron archivos .geojson")

        # metros → grados
        buffer_grados = buffer_metros / 111320.0

        rutas = [
            (os.path.join(carpeta_entrada, archivo), buffer_grados)
            for archivo in archivos
        ]

        geometries = []

        print("\nProcesando archivos en MULTIPROCESO...")

        with ProcessPoolExecutor(max_workers=max_workers) as exe:

            futures = {
                exe.submit(procesar_archivo_worker, args): args[0]
                for args in rutas
            }

            for fut in tqdm(as_completed(futures),
                            total=len(futures),
                            desc="Archivos procesados"):

                resultado = fut.result()
                geometries.extend(resultado)

        if not geometries:
            raise RuntimeError("No se generaron geometrías válidas")

        print("\nGuardando SHP final...")

        self._guardar_shp(geometries, salida_shp)

        print(f"\n✔ Shapefile generado: {salida_shp}")

    def _guardar_shp(self, geometries, salida_shp):

        schema = {
            "geometry": "Polygon",
            "properties": {
                "ID": "int"
            }
        }

        # Crear carpeta si no existe
        carpeta = os.path.dirname(salida_shp)
        if carpeta and not os.path.exists(carpeta):
            os.makedirs(carpeta)

        with fiona.open(
            salida_shp,
            "w",
            driver="ESRI Shapefile",
            crs=from_epsg(4326),
            schema=schema
        ) as dst:

            for i, geom in enumerate(geometries):
                dst.write({
                    "geometry": geom,
                    "properties": {"ID": i + 1}
                })


'''
# ============================================================
# EJECUCIÓN
# ============================================================
if __name__ == "__main__":

    CARPETA_ENTRADA = r"H:/A_2025/Automatizacion/Descargas_CAgua"
    SALIDA = r"H:/A_2025/Automatizacion/A_rasterizar/CAgua.shp"

    BUFFER_METROS = 50 * math.sqrt(2)

    MAX_WORKERS = os.cpu_count() - 2   # deja 2 libres para el sistema

    UnirShapefile(CARPETA_ENTRADA,
                  SALIDA,
                  BUFFER_METROS,
                  MAX_WORKERS)
'''