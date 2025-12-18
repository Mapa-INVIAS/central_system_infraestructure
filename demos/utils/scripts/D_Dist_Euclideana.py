# -*- coding: utf-8 -*-

import os
import math
import warnings
import geopandas as gpd
from osgeo import gdal, osr
from tqdm import tqdm


# Silenciar TODO (Python + GDAL)

warnings.filterwarnings("ignore")
gdal.PushErrorHandler("CPLQuietErrorHandler")
gdal.UseExceptions()


class DistanciaEuclidiana:

    def __init__(self,
                 carpeta_raster,
                 geojson_referencia,
                 carpeta_salida,
                 valor_fuente=1,
                 bloque_escalado=0):

        self.carpeta_raster = carpeta_raster
        self.geojson_referencia = geojson_referencia
        self.carpeta_salida = carpeta_salida
        self.valor_fuente = int(valor_fuente)
        self.bloque_escalado = int(bloque_escalado)

        os.makedirs(self.carpeta_salida, exist_ok=True)
        
        # Área efectiva
        self.area = gpd.read_file(self.geojson_referencia)
        if self.area.empty:
            raise RuntimeError("GeoJSON de referencia vacío")
        if self.area.crs is None:
            raise RuntimeError("GeoJSON de referencia sin CRS")

        
        # Centroide → factor grados a metros
        area_wgs84 = self.area.to_crs("EPSG:4326")
        c = area_wgs84.unary_union.centroid
        lat = float(c.y)
        lat_rad = math.radians(lat)

        self.m_por_grado_lat = 111320.0
        self.m_por_grado_lon = 111320.0 * math.cos(lat_rad)

        
        # Lista de rasters
        self.rasters = [os.path.join(self.carpeta_raster, f)
                        for f in os.listdir(self.carpeta_raster)
                        if f.lower().endswith(".tif")]
        if not self.rasters:
            raise RuntimeError("No se encontraron rasters .tif")

        
        # EJECUTAR COMO TÚ QUIERES: instancias y corre
        
        self.ejecutar()


    def ejecutar(self):

        for ruta in tqdm(self.rasters,
                         desc="Distancia euclidiana (GDAL, centroide)",
                         unit="raster"):
            self._procesar_uno(ruta)

    # UTILIDADES CRS
    def _crs_es_geografico(self, wkt):
        sref = osr.SpatialReference()
        sref.ImportFromWkt(wkt)
        return bool(sref.IsGeographic())

    def _crs_es_metrico(self, wkt):
        sref = osr.SpatialReference()
        sref.ImportFromWkt(wkt)
        if sref.IsGeographic():
            return False
        try:
            u = float(sref.GetLinearUnits() or 0.0)
        except Exception:
            return False
        return 0.9 <= u <= 1.1

    # ESCALADO STREAMING
    def _escalar_raster(self, in_path, out_path, factor):

        ds = gdal.Open(in_path, gdal.GA_ReadOnly)
        if ds is None:
            raise RuntimeError(f"No se pudo abrir para escalado: {in_path}")

        band = ds.GetRasterBand(1)
        xsize, ysize = ds.RasterXSize, ds.RasterYSize

        bx, by = band.GetBlockSize()
        if self.bloque_escalado > 0:
            bx = by = self.bloque_escalado
        if bx <= 0 or by <= 0:
            bx = by = 1024

        drv = gdal.GetDriverByName("GTiff")
        out = drv.Create(out_path,
                         xsize, 
                         ysize,
                         1,
                         gdal.GDT_Float32,
                         options=["TILED=YES", "COMPRESS=LZW", "SPARSE_OK=YES", "BIGTIFF=YES"])
        out.SetGeoTransform(ds.GetGeoTransform())
        out.SetProjection(ds.GetProjection())
        out_band = out.GetRasterBand(1)

        for y in range(0, ysize, by):
            h = min(by, ysize - y)
            for x in range(0, xsize, bx):
                w = min(bx, xsize - x)
                arr = band.ReadAsArray(x, y, w, h)
                if arr is None:
                    continue
                out_band.WriteArray(arr.astype("float32") * float(factor), x, y)

        out.FlushCache()
        out = None
        ds = None

    # PROCESAR UN RASTER
    def _procesar_uno(self, ruta_raster):

        nombre = os.path.splitext(os.path.basename(ruta_raster))[0]
        out_raster = os.path.join(self.carpeta_salida, f"dist_{nombre}.tif")

        tmp_clip = os.path.join(self.carpeta_salida, f"_tmp_clip_{nombre}.tif")
        tmp_bin  = os.path.join(self.carpeta_salida, f"_tmp_bin_{nombre}.tif")
        tmp_prox = os.path.join(self.carpeta_salida, f"_tmp_prox_{nombre}.tif")
        tmp_m    = os.path.join(self.carpeta_salida, f"_tmp_m_{nombre}.tif")
        cutline  = os.path.join(self.carpeta_salida, f"_tmp_area_{nombre}.gpkg")

        src = gdal.Open(ruta_raster, gdal.GA_ReadOnly)
        if src is None:
            raise RuntimeError(f"No se pudo abrir raster: {ruta_raster}")

        src_wkt = src.GetProjection()
        if not src_wkt:
            raise RuntimeError(f"Raster sin proyección: {ruta_raster}")

        # Área al CRS del raster
        self.area.to_crs(src_wkt).to_file(cutline,
                                          driver="GPKG",
                                          layer="area",
                                          index=False)

        
        # Clip REAL (multipolígono) con SPARSE_OK
        # (sin dstNodata para NO llenar afuera)
        
        gdal.Warp(tmp_clip,
                  src,
                  cutlineDSName=cutline,
                  cutlineLayer="area",
                  cropToCutline=True,
                  creationOptions=["TILED=YES", 
                                   "COMPRESS=LZW", 
                                   "SPARSE_OK=YES", 
                                   "BIGTIFF=YES"])
        src = None

        
        # Binario exacto (== valor_fuente)
        
        clip = gdal.Open(tmp_clip, gdal.GA_ReadOnly)
        if clip is None:
            raise RuntimeError(f"No se pudo abrir temporal clip: {tmp_clip}")

        drv = gdal.GetDriverByName("GTiff")
        bin_ds = drv.Create(tmp_bin,
                            clip.RasterXSize,
                            clip.RasterYSize,
                            1,
                            gdal.GDT_Byte,
                            options=["TILED=YES", 
                                     "COMPRESS=LZW", 
                                     "BIGTIFF=YES"])
        bin_ds.SetGeoTransform(clip.GetGeoTransform())
        bin_ds.SetProjection(clip.GetProjection())

        arr = clip.GetRasterBand(1).ReadAsArray()
        if arr is None:
            raise RuntimeError(f"No se pudo leer array de: {tmp_clip}")

        bin_ds.GetRasterBand(1).WriteArray((arr == self.valor_fuente).astype("uint8"))
        bin_ds.FlushCache()
        bin_ds = None
        clip = None

        
        # Proximity
        
        bin_open = gdal.Open(tmp_bin, gdal.GA_ReadOnly)
        if bin_open is None:
            raise RuntimeError(f"No se pudo abrir temporal binario: {tmp_bin}")

        is_geo = self._crs_es_geografico(src_wkt)
        is_m = self._crs_es_metrico(src_wkt)

        prox_ds = drv.Create(tmp_prox,
                             bin_open.RasterXSize,
                             bin_open.RasterYSize,
                             1,
                             gdal.GDT_Float32,
                             options=["TILED=YES", 
                                      "COMPRESS=LZW", 
                                      "BIGTIFF=YES"])
        prox_ds.SetGeoTransform(bin_open.GetGeoTransform())
        prox_ds.SetProjection(bin_open.GetProjection())

        gdal.ComputeProximity(bin_open.GetRasterBand(1),
                              prox_ds.GetRasterBand(1),
                              options=["VALUES=1",
                                       "DISTUNITS=GEO" if is_m else "PIXEL" ] )
        prox_ds.FlushCache()
        prox_ds = None
        bin_open = None

        
        # Escalar a metros si CRS geográfico
        
        if is_geo and not is_m:
            prox_tmp = gdal.Open(tmp_prox, gdal.GA_ReadOnly)
            gt = prox_tmp.GetGeoTransform()
            prox_tmp = None

            px_m = math.sqrt((abs(gt[1]) * self.m_por_grado_lon) ** 2 +
                             (abs(gt[5]) * self.m_por_grado_lat) ** 2 ) / math.sqrt(2)

            self._escalar_raster(tmp_prox, tmp_m, px_m)
            dist_in = tmp_m
        else:
            dist_in = tmp_prox

        
        # Clip FINAL (sparse)
        
        gdal.Warp(out_raster,
                  dist_in,
                  cutlineDSName=cutline,
                  cutlineLayer="area",
                  cropToCutline=True,
                  multithread=True,
                  creationOptions=["TILED=YES", 
                                   "COMPRESS=LZW", 
                                   "SPARSE_OK=YES", 
                                   "BIGTIFF=YES"])

        # Limpieza
        for p in (tmp_clip, tmp_bin, tmp_prox, tmp_m, cutline):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


# Ejecucion
if __name__ == "__main__":

    CARPETA_RASTER = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/out/C_Raster"
    GEOJSON_REFERENCIA = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/Implementacion/CapaReferencia/Colombia.geojson"
    CARPETA_SALIDA = r"D:/Documents/SCRIPTS/RECOSFA/Auto_INVIAS_2025/out/D_Distancia"

    VALOR_FUENTE = 1
    BLOQUE_ESCALADO = 0

    DistanciaEuclidiana(CARPETA_RASTER,
                         GEOJSON_REFERENCIA,
                         CARPETA_SALIDA,
                         VALOR_FUENTE,
                         BLOQUE_ESCALADO)
