# -*- coding: utf-8 -*-

import os
import warnings
import numpy as np
from osgeo import gdal
from tqdm import tqdm
import shutil

warnings.filterwarnings("ignore")
gdal.PushErrorHandler("CPLQuietErrorHandler")
gdal.UseExceptions()


class AlinearRastersSparsePorReferencia:
    """
    Alinea rasters al GRID EXACTO de un raster de referencia
    """

    def __init__(self,
                 carpeta_entrada,
                 raster_referencia,
                 carpeta_salida,
                 valores_nodata_virtuales=(-9999, -99999, -32768),
                 nodata_salida=-9999.0,
                 blocksize=512,
                 resample_alg="bilinear"):

        self.carpeta_entrada = carpeta_entrada
        self.raster_referencia = raster_referencia
        self.carpeta_salida = carpeta_salida

        self.valores_nodata_virtuales = tuple(valores_nodata_virtuales)
        self.nodata = float(nodata_salida)
        self.block = int(blocksize)
        self.resample_alg = str(resample_alg)

        os.makedirs(self.carpeta_salida, exist_ok=True)

        self.tmp_dir = os.path.join(self.carpeta_salida, "_tmp_alineado")
        os.makedirs(self.tmp_dir, exist_ok=True)

        self._leer_referencia()
        self._crear_mascara_referencia()
        self._ejecutar()
        self._limpieza_final()

    # ------------------------------------------------------------
    def _leer_referencia(self):
        ds = gdal.Open(self.raster_referencia, gdal.GA_ReadOnly)
        if ds is None:
            raise RuntimeError("No se pudo abrir raster de referencia")

        self.ref_gt = ds.GetGeoTransform()
        self.ref_proj = ds.GetProjection()
        self.ref_x = ds.RasterXSize
        self.ref_y = ds.RasterYSize

        b = ds.GetRasterBand(1)
        self.ref_nodata = b.GetNoDataValue()
        self.ref_arr = b.ReadAsArray()

        ds = None

        # Extent exacto de referencia
        self.xmin = self.ref_gt[0]
        self.ymax = self.ref_gt[3]
        self.xmax = self.xmin + self.ref_x * self.ref_gt[1]
        self.ymin = self.ymax + self.ref_y * self.ref_gt[5]  # ref_gt[5] negativo

        self.xres = float(self.ref_gt[1])
        self.yres = float(abs(self.ref_gt[5]))

    # ------------------------------------------------------------
    def _crear_mascara_referencia(self):
        """
        1 = dominio válido, 0 = fuera
        """
        self.mask = np.zeros(self.ref_arr.shape, dtype=np.uint8)
        if self.ref_nodata is None:
            # si tu ref NO usa 0 como fuera, ajusta esta regla
            self.mask[self.ref_arr != 0] = 1
        else:
            self.mask[self.ref_arr != self.ref_nodata] = 1

    # ------------------------------------------------------------
    def _listar_rasters(self):
        ref_abs = os.path.abspath(self.raster_referencia)
        return [os.path.join(self.carpeta_entrada, f)
                for f in os.listdir(self.carpeta_entrada)
                if f.lower().endswith(".tif")
                and os.path.abspath(os.path.join(self.carpeta_entrada, f)) != ref_abs]

    # ------------------------------------------------------------
    def _limpiar_nodata(self, arr, nodata_src):
        """
        Limpieza estricta SIN relleno:
        - NaN/Inf -> nodata
        - nodata_src -> nodata
        - nodata virtuales -> nodata
        """
        arr = arr.astype("float32", copy=False)

        arr[~np.isfinite(arr)] = self.nodata

        if nodata_src is not None:
            arr[arr == nodata_src] = self.nodata

        for v in self.valores_nodata_virtuales:
            arr[arr == v] = self.nodata

        return arr

    # ------------------------------------------------------------
    def _alinear_a_referencia(self, ruta):
        """
        SIEMPRE genera un raster temporal YA en el grid EXACTO de la referencia.
        Esto resuelve:
        - CRS distinto -> reproyecta
        - CRS igual pero resolución/grid distinto -> resamplea y alinea igual
        """
        base = os.path.splitext(os.path.basename(ruta))[0]
        tmp = os.path.join(self.tmp_dir, f"{base}_GRIDREF.tif")

        if os.path.exists(tmp):
            return tmp

        # Leer nodata origen para pasarlo a Warp si existe
        ds = gdal.Open(ruta, gdal.GA_ReadOnly)
        if ds is None:
            raise RuntimeError(f"No se pudo abrir {ruta}")
        b = ds.GetRasterBand(1)
        nodata_src = b.GetNoDataValue()
        ds = None

        # Intento 1 (normal, correcto)
        try:
            gdal.Warp  (tmp,
                        ruta,
                        format="GTiff",
                        dstSRS=self.ref_proj,
                        xRes=self.xres,
                        yRes=self.yres,
                        targetAlignedPixels=True,
                        outputBounds=[self.xmin, self.ymin, self.xmax, self.ymax],
                        resampleAlg=self.resample_alg,
                        srcNodata=nodata_src,
                        dstNodata=self.nodata,
                        multithread=True,
                        creationOptions=   ["BIGTIFF=YES",
                                            "TILED=YES",
                                            "COMPRESS=DEFLATE",
                                            "PREDICTOR=2",
                                            "ZLEVEL=6"])
            return tmp
        except Exception as e1:
            # Intento 2 (más tolerante para ciertos TIFF problemáticos):
            # sin tiled + sin compresión (reduce presión sobre libtiff), resample nearest
            # Sigue siendo TU GDAL.
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
                gdal.Warp  (tmp,
                            ruta,
                            format="GTiff",
                            dstSRS=self.ref_proj,
                            xRes=self.xres,
                            yRes=self.yres,
                            targetAlignedPixels=True,
                            outputBounds=[self.xmin, self.ymin, self.xmax, self.ymax],
                            resampleAlg="near",
                            srcNodata=nodata_src,
                            dstNodata=self.nodata,
                            multithread=True,
                            creationOptions=[
                                "BIGTIFF=YES",
                                "TILED=NO",
                                "COMPRESS=NONE"])
                return tmp
            except Exception as e2:
                raise RuntimeError (f"No se pudo alinear (Warp) el raster:\n{ruta}\n\n"
                                    f"Error 1:\n{e1}\n\nError 2:\n{e2}")

    # ------------------------------------------------------------
    def _procesar_un_raster(self, ruta):

        # 1) Forzar GRID exacto referencia (CRS + res + tap + te)
        ruta_alineada = self._alinear_a_referencia(ruta)

        base = os.path.splitext(os.path.basename(ruta))[0]
        salida = os.path.join(self.carpeta_salida, f"{base}.tif")

        # seguridad: nunca sobrescribir entrada
        if os.path.abspath(salida) == os.path.abspath(ruta):
            raise RuntimeError("Salida coincide con entrada. Revisa rutas.")

        ds = gdal.Open(ruta_alineada, gdal.GA_ReadOnly)
        if ds is None:
            raise RuntimeError(f"No se pudo abrir temporal alineado: {ruta_alineada}")

        # debería ser idéntico a referencia en grid
        if ds.RasterXSize != self.ref_x or ds.RasterYSize != self.ref_y:
            raise RuntimeError(f"Temporal no coincide en tamaño con referencia: {base}")

        if ds.GetProjection() != self.ref_proj:
            raise RuntimeError(f"Temporal no quedó en CRS de referencia: {base}")

        sb = ds.GetRasterBand(1)
        nodata_src = sb.GetNoDataValue()

        # 2) Crear salida final (limpia + máscara)
        drv = gdal.GetDriverByName("GTiff")
        if os.path.exists(salida):
            os.remove(salida)

        out = drv.Create   (salida,
                            self.ref_x,
                            self.ref_y,
                            1,
                            gdal.GDT_Float32,
                            options=   ["BIGTIFF=YES",
                                        "TILED=YES",
                                        "COMPRESS=DEFLATE",
                                        "PREDICTOR=2",
                                        "ZLEVEL=6"])
        out.SetGeoTransform(self.ref_gt)
        out.SetProjection(self.ref_proj)

        ob = out.GetRasterBand(1)
        ob.SetNoDataValue(self.nodata)

        # 3) Procesar por bloques (sin offsets, ya está en el mismo grid)
        for y in range(0, self.ref_y, self.block):
            h = min(self.block, self.ref_y - y)
            for x in range(0, self.ref_x, self.block):
                w = min(self.block, self.ref_x - x)

                m = self.mask[y:y+h, x:x+w]
                if (m != 1).all():
                    ob.WriteArray(np.full((h, w), self.nodata, np.float32), x, y)
                    continue

                try:
                    arr = sb.ReadAsArray(x, y, w, h)
                    if arr is None:
                        raise RuntimeError
                    arr = self._limpiar_nodata(arr, nodata_src)
                except Exception:
                    arr = np.full((h, w), self.nodata, np.float32)

                arr[m != 1] = self.nodata
                ob.WriteArray(arr, x, y)

        out.FlushCache()
        ds = None
        out = None

    # ------------------------------------------------------------
    def _ejecutar(self):
        rasters = self._listar_rasters()
        for r in tqdm(rasters, desc="Alinear al GRID de referencia (perfecto)"):
            self._procesar_un_raster(r)

    # ------------------------------------------------------------
    def _limpieza_final(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except Exception:
            pass


# ------------------------------------------------------------
# EJECUCIÓN
if __name__ == "__main__":

    CARPETA_ENTRADA   = r"D:/Downloads/juanDavid_Sukubun/out2_kr/capas/D_Distancia/"
    RASTER_REFERENCIA = r"D:/Downloads/juanDavid_Sukubun/out2_kr/capas/E_Alineadas/SM_DEM.tif"
    CARPETA_SALIDA    = r"D:/Downloads/juanDavid_Sukubun/out2_kr/capas/E_Alineadas/"

    AlinearRastersSparsePorReferencia  (CARPETA_ENTRADA,
                                        RASTER_REFERENCIA,
                                        CARPETA_SALIDA,
                                        valores_nodata_virtuales=(-9999, -99999, -32768),
                                        nodata_salida=-9999.0,
                                        blocksize=512,
                                        resample_alg="bilinear")