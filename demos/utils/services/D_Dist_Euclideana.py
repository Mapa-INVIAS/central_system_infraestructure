# -*- coding: utf-8 -*-

import os
import math
import warnings

import geopandas as gpd
from osgeo import gdal, ogr, osr
from tqdm import tqdm

# --------------------------------------
# SILENCIAR WARNINGs
warnings.filterwarnings("ignore")
gdal.PushErrorHandler("CPLQuietErrorHandler")
gdal.UseExceptions()


class DistanciaEuclidiana:

    def __init__(self,
                 carpeta_raster: str,
                 geojson_referencia: str,
                 carpeta_salida: str,
                 valor_fuente: int = 1):

        self.carpeta_raster = carpeta_raster
        self.geojson_referencia = geojson_referencia
        self.carpeta_salida = carpeta_salida
        self.valor_fuente = int(valor_fuente)

        # Crear carpeta de salida
        os.makedirs(self.carpeta_salida, exist_ok=True)

        # Leer vector de referencia
        self.area = gpd.read_file(self.geojson_referencia)
        if self.area.empty:
            raise RuntimeError("GeoJSON de referencia vacío")
        if self.area.crs is None:
            raise RuntimeError("El vector de referencia no tiene CRS")

        # Lista de .tif en la carpeta
        self.rasters = sorted([
            os.path.join(self.carpeta_raster, f)
            for f in os.listdir(self.carpeta_raster)
            if f.lower().endswith(".tif")
        ])
        if not self.rasters:
            raise RuntimeError("No se encontraron archivos .tif en la carpeta")

        # Ejecutar procesamiento
        self.ejecutar()

    def ejecutar(self):
        for ruta in tqdm(self.rasters,
                         desc="Distancia euclidiana (GDAL, máscara exacta, NaN real)",
                         unit="raster"):
            self._procesar_uno(ruta)

    # =======================================================
    # UTILIDADES PARA CRS
    def _crs_es_geografico(self, wkt: str) -> bool:
        sref = osr.SpatialReference()
        sref.ImportFromWkt(wkt)
        return bool(sref.IsGeographic())

    def _crs_es_metrico(self, wkt: str) -> bool:
        sref = osr.SpatialReference()
        sref.ImportFromWkt(wkt)
        if sref.IsGeographic():
            return False
        try:
            lu = float(sref.GetLinearUnits() or 0.0)
        except Exception:
            return False
        return 0.9 <= lu <= 1.1

    # =======================================================
    # PROCESAR UN RASTER
    def _procesar_uno(self, ruta_raster: str):

        nombre = os.path.splitext(os.path.basename(ruta_raster))[0]
        out_raster = os.path.join(self.carpeta_salida, f"dist_{nombre}.tif")

        tmp_clip = os.path.join(self.carpeta_salida, f"_tmp_clip_{nombre}.tif")
        tmp_mask = os.path.join(self.carpeta_salida, f"_tmp_mask_{nombre}.tif")
        tmp_bin  = os.path.join(self.carpeta_salida, f"_tmp_bin_{nombre}.tif")
        tmp_prox = os.path.join(self.carpeta_salida, f"_tmp_prox_{nombre}.tif")
        tmp_out  = os.path.join(self.carpeta_salida, f"_tmp_out_{nombre}.tif")
        cutline  = os.path.join(self.carpeta_salida, f"_tmp_area_{nombre}.gpkg")

        NODATA_INT = -9999.0
        NODATA_OUT = float("nan")

        # Abrir raster
        ds = gdal.Open(ruta_raster, gdal.GA_ReadOnly)
        if ds is None:
            raise RuntimeError(f"No se pudo abrir: {ruta_raster}")

        src_wkt = ds.GetProjection()
        if not src_wkt:
            ds = None
            raise RuntimeError(f"Raster sin proyección: {ruta_raster}")

        # Exportar área al CRS del raster
        self.area.to_crs(src_wkt).to_file(cutline,
                                          driver="GPKG",
                                          layer="area",
                                          index=False)

        # ------------------------------------------------------
        # 1) Clip al área
        gdal.Warp(tmp_clip,
                  ds,
                  cutlineDSName=cutline,
                  cutlineLayer="area",
                  cropToCutline=True,
                  dstNodata=NODATA_INT,
                  multithread=True,
                  creationOptions=["TILED=YES",
                                   "COMPRESS=LZW",
                                   "SPARSE_OK=YES",
                                   "BIGTIFF=YES"])
        ds = None

        # ------------------------------------------------------
        # 2) Crear máscara exacta
        ref = gdal.Open(tmp_clip, gdal.GA_ReadOnly)
        xsize, ysize = ref.RasterXSize, ref.RasterYSize
        gt = ref.GetGeoTransform()
        prj = ref.GetProjection()
        ref = None

        drv = gdal.GetDriverByName("GTiff")
        mask_ds = drv.Create(tmp_mask, xsize, ysize, 1,
                             gdal.GDT_Byte,
                             options=["TILED=YES",
                                      "COMPRESS=LZW",
                                      "SPARSE_OK=YES",
                                      "BIGTIFF=YES"])
        mask_ds.SetGeoTransform(gt)
        mask_ds.SetProjection(prj)

        mb = mask_ds.GetRasterBand(1)
        mb.SetNoDataValue(0)
        mb.Fill(0)

        vds = ogr.Open(cutline)
        lyr = vds.GetLayerByName("area")
        gdal.RasterizeLayer(mask_ds, [1], lyr, burn_values=[1])
        mask_ds.FlushCache()
        mask_ds = None
        vds = None

        # ------------------------------------------------------
        # 3) Binario de fuente dentro del área
        clip = gdal.Open(tmp_clip, gdal.GA_ReadOnly)
        mask_open = gdal.Open(tmp_mask, gdal.GA_ReadOnly)
        arr = clip.GetRasterBand(1).ReadAsArray()
        msk = mask_open.GetRasterBand(1).ReadAsArray()
        clip = None
        mask_open = None

        bin_ds = drv.Create(tmp_bin,
                            xsize, ysize, 1,
                            gdal.GDT_Byte,
                            options=["TILED=YES",
                                     "COMPRESS=LZW",
                                     "SPARSE_OK=YES",
                                     "BIGTIFF=YES"])
        
        bin_ds.SetGeoTransform(gt)
        bin_ds.SetProjection(prj)
        bb = bin_ds.GetRasterBand(1)
        bb.SetNoDataValue(0)
        bb.Fill(0)
        bb.WriteArray(((arr == self.valor_fuente) & (msk == 1)).astype("uint8"))
        bin_ds.FlushCache()
        bin_ds = None

        # ------------------------------------------------------
        # 4) Calcular proximidad (distancia euclidiana)
        bin_open = gdal.Open(tmp_bin, gdal.GA_ReadOnly)
        prox_ds = drv.Create(tmp_prox, xsize, ysize, 1, gdal.GDT_Float32,
                             options=["TILED=YES",
                                      "COMPRESS=LZW",
                                      "SPARSE_OK=YES",
                                      "BIGTIFF=YES"])
        
        prox_ds.SetGeoTransform(gt)
        prox_ds.SetProjection(prj)
        pb = prox_ds.GetRasterBand(1)
        pb.SetNoDataValue(NODATA_INT)

        is_geo = self._crs_es_geografico(src_wkt)
        is_m  = self._crs_es_metrico(src_wkt)

        # DISTUNITS=GEO produce distancia euclidiana en unidades del CRS cuando es métrico :contentReference[oaicite:1]{index=1}
        gdal.ComputeProximity(bin_open.GetRasterBand(1),
                              pb,
                              options=["VALUES=1",
                                       "DISTUNITS=GEO" if is_m else "PIXEL"])
        prox_ds.FlushCache()
        prox_ds = None
        bin_open = None

        # ------------------------------------------------------
        # 5) Aplicar máscara y generar raster final con NaN fuera
        dist_ds = gdal.Open(tmp_prox, gdal.GA_ReadOnly)
        mask_ds = gdal.Open(tmp_mask, gdal.GA_ReadOnly)

        out0 = drv.Create(tmp_out,
                          xsize, ysize, 1,
                          gdal.GDT_Float32,
                          options=["TILED=YES",
                                   "COMPRESS=LZW",
                                   "SPARSE_OK=YES",
                                   "BIGTIFF=YES"])
        
        out0.SetGeoTransform(gt)
        out0.SetProjection(prj)
        ob = out0.GetRasterBand(1)
        ob.SetNoDataValue(NODATA_OUT)

        db = dist_ds.GetRasterBand(1)
        mb = mask_ds.GetRasterBand(1)

        bx, by = db.GetBlockSize()
        if bx <= 0 or by <= 0:
            bx = by = 1024

        for y in range(0, ysize, by):
            for x in range(0, xsize, bx):
                w = min(bx, xsize - x)
                h = min(by, ysize - y)
                marr = mb.ReadAsArray(x, y, w, h)
                darr = db.ReadAsArray(x, y, w, h).astype("float32")
                darr[marr != 1] = NODATA_OUT
                ob.WriteArray(darr, x, y)

        out0.FlushCache()
        out0 = None
        dist_ds = None
        mask_ds = None

        # ------------------------------------------------------
        # GUARDAR RESULTADO
        gdal.Translate(out_raster, tmp_out,
                       creationOptions=["TILED=YES",
                                        "COMPRESS=LZW",
                                        "SPARSE_OK=YES",
                                        "BIGTIFF=YES"])
        # LIMPIEZA
        for p in (tmp_clip, tmp_mask, tmp_bin, tmp_prox, tmp_out, cutline):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass