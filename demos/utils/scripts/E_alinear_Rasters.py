# -*- coding: utf-8 -*-

import os
import logging
import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from rasterio.features import geometry_mask
from scipy.ndimage import distance_transform_edt
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import fiona



# CONFIGURACIÓN LOGGING
def configurar_logger(ruta_log):
    handlers = []

    if ruta_log is not None:
        handlers.append(logging.FileHandler(ruta_log, encoding="utf-8"))

    handlers.append(logging.StreamHandler())

    logging.basicConfig( level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=handlers,)

    logger = logging.getLogger(__name__)
    logger.propagate = False
    return logger


class AlinearRasters:
    def __init__(self,
                 carpeta,
                 raster_ref,
                 carpeta_salida,
                 vector_aoi,
                 usar_paralelo,
                 max_workers,
                 ruta_log):

        # Crear carpeta de salida
        os.makedirs(carpeta_salida, exist_ok=True)

        self.logger = configurar_logger(ruta_log)

        self.logger.info("=== INICIO DEL PROCESO ===")
        self.logger.info(f"Entrada: {carpeta}")
        self.logger.info(f"Referencia: {raster_ref}")
        self.logger.info(f"Salida: {carpeta_salida}")
        if vector_aoi:
            self.logger.info(f"AOI: {vector_aoi}")
        self.logger.info(f"Paralelo: {usar_paralelo} | Workers: {max_workers}")

        # 1. Raster referencia
        ref_arr, ref_meta = self.cargar_raster(raster_ref)

        # 2. AOI opcional
        geometries_aoi = None
        if vector_aoi is not None:
            geometries_aoi = self.cargar_aoi(vector_aoi)
            self.logger.info(f"AOI cargado: {len(geometries_aoi)} geometrías")

        # 3. Lista de rasters
        rasters = self.listar_rasters(carpeta)
        raster_ref_abs = os.path.abspath(raster_ref)

        rasters_procesar = [r for r in rasters if os.path.abspath(r) != raster_ref_abs]

        total = len(rasters_procesar)
        self.logger.info(f"Rásters encontrados: {len(rasters)}")
        self.logger.info(f"Rásters a procesar: {total}")

        # 4. Ejecución con barra anclada
        with logging_redirect_tqdm():
            if usar_paralelo and total > 0:
                self.logger.info("Procesamiento paralelo…")

                with ThreadPoolExecutor(max_workers=max_workers) as executor:

                    futures = {executor.submit(self.procesar_un_raster,
                                               ruta,
                                               ref_meta,
                                               geometries_aoi,
                                               carpeta_salida): ruta for ruta in rasters_procesar}

                    for f in tqdm(as_completed(futures),
                                  total=total,
                                  desc="Progreso",
                                  dynamic_ncols=True,
                                  leave=True ):
                        try:
                            f.result()
                        except Exception as e:
                            self.logger.error(f"Error: {e}")

            else:
                self.logger.info("Procesamiento secuencial…")

                for ruta in tqdm(rasters_procesar,
                                 desc="Progreso",
                                 dynamic_ncols=True,
                                 leave=True ):
                    self.procesar_un_raster(ruta,
                                             ref_meta, 
                                             geometries_aoi, 
                                             carpeta_salida)

        self.logger.info("=== PROCESO FINALIZADO ===")


    def listar_rasters(self, carpeta):
        return [os.path.join(carpeta, f)
                for f in os.listdir(carpeta)
                if f.lower().endswith(".tif")]

    def cargar_raster(self, ruta):
        self.logger.info(f"Cargando: {ruta}")
        with rasterio.open(ruta) as src:
            arr = src.read(1)
            meta = src.meta.copy()
        return arr, meta

    def cargar_aoi(self, ruta_vector):
        geoms = []
        with fiona.open(ruta_vector, "r") as src:
            for feat in src:
                geoms.append(feat["geometry"])
        return geoms

    def reproyectar(self, arr, meta, meta_ref):

        dst = np.zeros((meta_ref["height"], meta_ref["width"]), dtype=np.float32)
        new_meta = meta_ref.copy()

        reproject(source=arr,
                  destination=dst,
                  src_transform=meta["transform"],
                  src_crs=meta["crs"],
                  dst_transform=meta_ref["transform"],
                  dst_crs=meta_ref["crs"],
                  resampling=Resampling.bilinear)

        return dst, new_meta

    def aplicar_aoi(self, arr, meta_ref, geometries):
        if geometries is None:
            return arr

        mask = geometry_mask(geometries,
                             transform=meta_ref["transform"],
                             invert=True,
                             out_shape=(meta_ref["height"], meta_ref["width"]))

        out = arr.copy().astype(float)
        out[~mask] = np.nan
        return out

    def rellenar_huecos(self, arr):

        A = arr.copy().astype(float)
        nodata = np.isnan(A) | (A < -1e9)

        if not nodata.any():
            return A

        vals = A.copy()
        vals[nodata] = 0

        _, inds = distance_transform_edt(nodata, return_indices=True)
        A[nodata] = vals[inds[0][nodata], inds[1][nodata]]
        return A

    def guardar_raster(self, arr, meta, salida):
        meta2 = meta.copy()
        meta2.update(dtype="float32", nodata=np.nan)

        with rasterio.open(salida, "w", **meta2) as dst:
            dst.write(arr.astype("float32"), 1)

        self.logger.info(f"Guardado: {salida}")

    def procesar_un_raster(self, ruta, ref_meta, geometries_aoi, carpeta_salida):

        nombre = os.path.basename(ruta)
        self.logger.info(f"Procesando: {nombre}")

        arr, meta = self.cargar_raster(ruta)
        arr_al, meta_al = self.reproyectar(arr, meta, ref_meta)
        arr_fill = self.rellenar_huecos(arr_al)

        if geometries_aoi is not None:
            arr_fill = self.aplicar_aoi(arr_fill, ref_meta, geometries_aoi)

        salida = os.path.join(carpeta_salida, f"{nombre}")
        self.guardar_raster(arr_fill, meta_al, salida)



# Ejecución
if __name__ == "__main__":

    CARPETA_ENTRADA     = r"H:/A_2025/EPM_Fauna_electrocutada/TODO/"
    RASTER_REFERENCIA   = r"H:/A_2025/EPM_Fauna_electrocutada/TODO/SM_DEM.tif"
    CARPETA_SALIDA      = r"H:/A_2025/EPM_Fauna_electrocutada/Alineados2/"

    VECTOR_AOI          = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/Insumosa/Colombia.geojson"
    LOGFILE             = r"H:/A_2025/EPM_Fauna_electrocutada/Alineados2/log_alineacion.txt"

    usar_paralelo       = True
    max_workers         = 4

    AlinearRasters(CARPETA_ENTRADA,
                   RASTER_REFERENCIA,
                   CARPETA_SALIDA,
                   VECTOR_AOI,
                   usar_paralelo,
                   max_workers,
                   LOGFILE)
