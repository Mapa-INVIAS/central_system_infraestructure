# -*- coding: utf-8 -*-

import os
import warnings
import numpy as np
from osgeo import gdal
from tqdm import tqdm
from scipy.ndimage import distance_transform_edt
import shutil


# ------------------------------------------------------------
# SILENCIAR TODO
warnings.filterwarnings("ignore")
gdal.PushErrorHandler("CPLQuietErrorHandler")
gdal.UseExceptions()


class AlinearRastersSparsePorReferencia:
    """
    Alinea rasters al grid EXACTO de una referencia y rellena TODOS los
    huecos internos (NaN, nodata reales y virtuales) siguiendo tendencias
    espaciales locales, SIN inventar valores fuera del dominio válido.
    """

    def __init__(self,
                 carpeta_entrada,
                 raster_referencia,
                 carpeta_salida,
                 valores_nodata_virtuales=(-9999, -99999, -32768),
                 nodata_warp=-9999.0):

        self.carpeta_entrada = carpeta_entrada
        self.raster_referencia = raster_referencia
        self.carpeta_salida = carpeta_salida

        self.valores_nodata_virtuales = tuple(valores_nodata_virtuales)
        self.nodata_warp = float(nodata_warp)

        os.makedirs(self.carpeta_salida, exist_ok=True)

        # carpeta temporal persistente
        self.tmp_dir = os.path.join(self.carpeta_salida, "_tmp")
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

    # ------------------------------------------------------------
    def _crear_mascara_referencia(self):
        """
        1 = dominio válido
        0 = fuera de análisis
        """
        self.mask = np.zeros(self.ref_arr.shape, dtype=np.uint8)

        if self.ref_nodata is None:
            self.mask[self.ref_arr != 0] = 1
        else:
            self.mask[self.ref_arr != self.ref_nodata] = 1

    # ------------------------------------------------------------
    def _listar_rasters(self):

        ref_abs = os.path.abspath(self.raster_referencia)

        return [
            os.path.join(self.carpeta_entrada, f)
            for f in os.listdir(self.carpeta_entrada)
            if f.lower().endswith(".tif")
            and os.path.abspath(os.path.join(self.carpeta_entrada, f)) != ref_abs
        ]

    # ------------------------------------------------------------
    def _rellenar_por_tendencia(self, arr, huecos):
        """
        Relleno por nearest neighbor SOLO donde hay huecos.
        """
        if not huecos.any():
            return arr

        base = arr.copy()
        base[huecos] = 0

        _, inds = distance_transform_edt(huecos, return_indices=True)
        arr[huecos] = base[inds[0][huecos], inds[1][huecos]]

        return arr

    # ------------------------------------------------------------
    def _procesar_un_raster(self, ruta):

        nombre = os.path.basename(ruta)
        salida = os.path.join(self.carpeta_salida, nombre)
        tmp = os.path.join(self.tmp_dir, nombre)

        # nodata origen
        ds_in = gdal.Open(ruta, gdal.GA_ReadOnly)
        if ds_in is None:
            return
        b_in = ds_in.GetRasterBand(1)
        nodata_origen = b_in.GetNoDataValue()
        b_in = None
        ds_in = None

        # --------------------------------------------------------
        # 1) Warp exacto
        gdal.Warp(
            tmp,
            ruta,
            format="GTiff",
            dstSRS=self.ref_proj,
            xRes=self.ref_gt[1],
            yRes=abs(self.ref_gt[5]),
            targetAlignedPixels=True,
            outputBounds=[
                self.ref_gt[0],
                self.ref_gt[3] + self.ref_y * self.ref_gt[5],
                self.ref_gt[0] + self.ref_x * self.ref_gt[1],
                self.ref_gt[3]
            ],
            resampleAlg="bilinear",
            srcNodata=nodata_origen,
            dstNodata=self.nodata_warp,
            creationOptions=["TILED=YES", "SPARSE_OK=YES"]
        )

        src = gdal.Open(tmp, gdal.GA_ReadOnly)
        sb = src.GetRasterBand(1)

        if os.path.exists(salida):
            os.remove(salida)

        drv = gdal.GetDriverByName("GTiff")
        out = drv.Create(
            salida,
            self.ref_x,
            self.ref_y,
            1,
            gdal.GDT_Float32,
            options=[
                "TILED=YES",
                "COMPRESS=LZW",
                "SPARSE_OK=YES",
                "BIGTIFF=YES"
            ]
        )

        out.SetGeoTransform(self.ref_gt)
        out.SetProjection(self.ref_proj)

        ob = out.GetRasterBand(1)
        ob.SetNoDataValue(self.nodata_warp)

        bx, by = sb.GetBlockSize()
        if bx <= 0 or by <= 0:
            bx = by = 1024

        # --------------------------------------------------------
        # 2) Procesamiento por bloques
        for y in range(0, self.ref_y, by):
            h = min(by, self.ref_y - y)
            for x in range(0, self.ref_x, bx):
                w = min(bx, self.ref_x - x)

                m = self.mask[y:y+h, x:x+w]
                if (m != 1).all():
                    continue

                arr = sb.ReadAsArray(x, y, w, h)
                if arr is None:
                    continue

                arr = arr.astype("float32", copy=False)

                # marcar huecos SOLO dentro de la máscara
                huecos = np.zeros(arr.shape, dtype=bool)

                huecos |= np.isnan(arr)
                huecos |= (arr == self.nodata_warp)

                if nodata_origen is not None:
                    huecos |= (arr == nodata_origen)

                for v in self.valores_nodata_virtuales:
                    huecos |= (arr == v)

                huecos &= (m == 1)

                # relleno por tendencia
                arr = self._rellenar_por_tendencia(arr, huecos)

                # fuera de máscara: nodata
                arr[m != 1] = self.nodata_warp

                ob.WriteArray(arr, x, y)

        out.FlushCache()

        sb = None
        src = None
        out = None

    # ------------------------------------------------------------
    def _ejecutar(self):

        rasters = self._listar_rasters()

        for r in tqdm(
            rasters,
            desc="Alineando + rellenando huecos internos (tendencias)"
        ):
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

    CARPETA_ENTRADA   = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/TodoColombia/Mapa_Prueba/a_Pacifico/rasterIN"
    RASTER_REFERENCIA = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/out/referencia/raster_IN/SM_DEM.tif"
    CARPETA_SALIDA    = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/out/E_Alineados/"

    AlinearRastersSparsePorReferencia(
        CARPETA_ENTRADA,
        RASTER_REFERENCIA,
        CARPETA_SALIDA,
        valores_nodata_virtuales=(-9999, -99999, -32768),
        nodata_warp=-9999.0
    )
