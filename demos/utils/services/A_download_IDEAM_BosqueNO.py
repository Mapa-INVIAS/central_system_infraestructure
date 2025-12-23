# -*- coding: utf-8 -*-

import os
import zipfile
import requests
import shutil
from tqdm import tqdm
import rasterio
import numpy as np

class DownloadBosqueNoBosque:

    def __init__(self,
                 carpeta_salida,
                 anio_max,
                 anio_min,
                 timeout,
                 base_url,
                 nombre_final):

        os.makedirs(carpeta_salida, exist_ok=True)

        encontrado = False

        for anio in range(anio_max, anio_min - 1, -1):

            zip_path = self.descargar_zip(carpeta_salida,
                                          base_url,
                                          anio,
                                          timeout)

            if zip_path is None:
                continue

            tif_name = self.buscar_geotiff_en_zip(zip_path)

            if tif_name is not None:
                self.extraer_tif(zip_path,
                                 tif_name,
                                 carpeta_salida,
                                 nombre_final )

                os.remove(zip_path)

                self.generar_raster_filtrado(carpeta_salida,
                                             nombre_final)

                encontrado = True
                break

            os.remove(zip_path)

        if not encontrado:
            raise RuntimeError("No se encontró ninguna versión con GeoTIFF")

    def descargar_zip(self,
                      carpeta_salida,
                      base_url,
                      anio,
                      timeout):

        url = f"{base_url}/Bosque_No_Bosque_{anio}.zip"
        zip_path = os.path.join(carpeta_salida,
                                f"Bosque_No_Bosque_{anio}.zip")

        try:
            r = requests.get(url, stream=True, timeout=timeout)
            if r.status_code != 200:
                return None

            total = int(r.headers.get("content-length", 0))

            with open(zip_path, "wb") as f, tqdm(total=total,
                                                 unit="B",
                                                 unit_scale=True,
                                                 desc=f"Descargando {anio}" ) as pbar:

                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            return zip_path

        except Exception:
            return None

    def buscar_geotiff_en_zip(self,
                              zip_path):

        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                for name in z.namelist():
                    if name.lower().endswith((".tif", ".tiff")):
                        return name
        except Exception:
            pass

        return None

    def extraer_tif(self,
                    zip_path,
                    tif_name,
                    carpeta_salida,
                    nombre_final):

        with zipfile.ZipFile(zip_path, "r") as z:
            z.extract(tif_name, carpeta_salida)

        origen = os.path.join(carpeta_salida, tif_name)
        destino = os.path.join(carpeta_salida, nombre_final)

        if os.path.exists(destino):
            os.remove(destino)

        shutil.copy2(origen, destino)
        os.remove(origen)

        # Limpieza de carpetas intermedias
        carpeta = os.path.dirname(origen)
        while carpeta and carpeta != carpeta_salida:
            try:
                os.rmdir(carpeta)
            except OSError:
                break
            carpeta = os.path.dirname(carpeta)

    def generar_raster_filtrado(self,
                                carpeta_salida,
                                nombre_final):

        ruta_original = os.path.join(carpeta_salida,
                                     nombre_final)

        ruta_salida = os.path.join(carpeta_salida,
                                   "Bosque_1_filtrado.tif")

        with rasterio.open(ruta_original) as src:

            nodata = src.nodata
            if nodata is None:
                nodata = 255

            perfil = src.profile
            perfil.update(dtype=rasterio.uint8,
                           count=1,
                           nodata=nodata)

            with rasterio.open(ruta_salida, "w", **perfil) as dst:

                ventanas = list(src.block_windows(1))

                with tqdm(total=len(ventanas),
                          desc="Filtrando píxeles == 1",
                          unit="bloque") as pbar:

                    for _, window in ventanas:

                        data = src.read(1,
                                        window=window)

                        filtrado = np.where(data == 1,
                                             1,
                                             nodata).astype(np.uint8)

                        dst.write(filtrado,
                                  1,
                                  window=window)

                        pbar.update(1)

        os.remove(ruta_original)
        os.rename(ruta_salida, ruta_original)