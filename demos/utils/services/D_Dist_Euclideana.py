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

        # Centroide → factor grados a metros (solo si CRS geográfico)
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

        # Ejecutar
        self.ejecutar()

    def ejecutar(self):
        for ruta in tqdm(self.rasters,
                         desc="Distancia euclidiana (GDAL, máscara exacta)",
                         unit="raster"):
            self._procesar_uno(ruta)

    # -----------------------------
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

    # -----------------------------
    # ESCALADO STREAMING (solo distancias)
    def _escalar_raster(self, in_path, out_path, factor, nodata_val=-9999.0):

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
        out_band.SetNoDataValue(float(nodata_val))

        in_nodata = band.GetNoDataValue()
        if in_nodata is None:
            in_nodata = nodata_val

        for y in range(0, ysize, by):
            h = min(by, ysize - y)
            for x in range(0, xsize, bx):
                w = min(bx, xsize - x)
                arr = band.ReadAsArray(x, y, w, h)
                if arr is None:
                    continue

                arr = arr.astype("float32", copy=False)

                # preservar nodata
                m_nodata = (arr == float(in_nodata))
                arr = arr * float(factor)
                arr[m_nodata] = float(nodata_val)

                out_band.WriteArray(arr, x, y)

        out.FlushCache()
        out = None
        ds = None

    # -----------------------------
    # PROCESAR UN RASTER
    def _procesar_uno(self, ruta_raster):

        nombre = os.path.splitext(os.path.basename(ruta_raster))[0]
        out_raster = os.path.join(self.carpeta_salida, f"dist_{nombre}.tif")

        tmp_clip = os.path.join(self.carpeta_salida, f"_tmp_clip_{nombre}.tif")
        tmp_mask = os.path.join(self.carpeta_salida, f"_tmp_mask_{nombre}.tif")
        tmp_bin  = os.path.join(self.carpeta_salida, f"_tmp_bin_{nombre}.tif")
        tmp_prox = os.path.join(self.carpeta_salida, f"_tmp_prox_{nombre}.tif")
        tmp_m    = os.path.join(self.carpeta_salida, f"_tmp_m_{nombre}.tif")
        tmp_out0 = os.path.join(self.carpeta_salida, f"_tmp_out0_{nombre}.tif")
        cutline  = os.path.join(self.carpeta_salida, f"_tmp_area_{nombre}.gpkg")

        NODATA = -9999.0

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

        # ---------------------------------------------------------
        # 1) CLIP A EXTENSIÓN DEL ÁREA (rápido, reduce trabajo)
        #    (aquí sí ponemos NoData para que afuera del recorte quede vacío real)
        # ---------------------------------------------------------
        gdal.Warp(tmp_clip,
                  src,
                  cutlineDSName=cutline,
                  cutlineLayer="area",
                  cropToCutline=True,
                  dstNodata=NODATA,
                  multithread=True,
                  creationOptions=["TILED=YES",
                                   "COMPRESS=LZW",
                                   "SPARSE_OK=YES",
                                   "BIGTIFF=YES"])
        src = None

        clip = gdal.Open(tmp_clip, gdal.GA_ReadOnly)
        if clip is None:
            raise RuntimeError(f"No se pudo abrir temporal clip: {tmp_clip}")

        # ---------------------------------------------------------
        # 2) MÁSCARA EXACTA DEL POLÍGONO (1 dentro, 0 fuera)
        #    IMPORTANTÍSIMO: esto evita cálculo/valores fuera del área real
        # ---------------------------------------------------------
        gdal.Warp(tmp_mask,
                  tmp_clip,
                  cutlineDSName=cutline,
                  cutlineLayer="area",
                  cropToCutline=True,
                  dstNodata=0,
                  multithread=True,
                  creationOptions=["TILED=YES",
                                   "COMPRESS=LZW",
                                   "SPARSE_OK=YES",
                                   "BIGTIFF=YES"],
                  # rasterizar polígono como máscara
                  warpOptions=["CUTLINE_ALL_TOUCHED=FALSE"],
                  )

        # OJO: el Warp anterior no garantiza máscara 1/0 si el raster tiene datos,
        # así que hacemos máscara real con Rasterize (sobre el grid del clip):
        # Creamos tmp_mask como byte y rasterizamos el polígono encima del grid.

        drv = gdal.GetDriverByName("GTiff")
        mask_ds = drv.Create(tmp_mask,
                             clip.RasterXSize,
                             clip.RasterYSize,
                             1,
                             gdal.GDT_Byte,
                             options=["TILED=YES", "COMPRESS=LZW", "SPARSE_OK=YES", "BIGTIFF=YES"])
        mask_ds.SetGeoTransform(clip.GetGeoTransform())
        mask_ds.SetProjection(clip.GetProjection())
        mb = mask_ds.GetRasterBand(1)
        mb.SetNoDataValue(0)
        mb.Fill(0)

        # Rasterizar polígono (burn=1)
        err = gdal.Rasterize(mask_ds, cutline, layers=["area"], burnValues=[1])
        if err != 0:
            mask_ds = None
            clip = None
            raise RuntimeError("No se pudo rasterizar la máscara del área")

        mask_ds.FlushCache()
        mask_ds = None

        # ---------------------------------------------------------
        # 3) BINARIO SOLO DENTRO DEL ÁREA (fuera = 0)
        # ---------------------------------------------------------
        mask_open = gdal.Open(tmp_mask, gdal.GA_ReadOnly)
        if mask_open is None:
            clip = None
            raise RuntimeError(f"No se pudo abrir máscara: {tmp_mask}")

        arr = clip.GetRasterBand(1).ReadAsArray()
        msk = mask_open.GetRasterBand(1).ReadAsArray()

        if arr is None or msk is None:
            clip = None
            mask_open = None
            raise RuntimeError("No se pudo leer arrays de clip/máscara")

        bin_ds = drv.Create(tmp_bin,
                            clip.RasterXSize,
                            clip.RasterYSize,
                            1,
                            gdal.GDT_Byte,
                            options=["TILED=YES",
                                     "COMPRESS=LZW",
                                     "SPARSE_OK=YES",
                                     "BIGTIFF=YES"])
        bin_ds.SetGeoTransform(clip.GetGeoTransform())
        bin_ds.SetProjection(clip.GetProjection())
        bb = bin_ds.GetRasterBand(1)
        bb.SetNoDataValue(0)

        # (arr == valor_fuente) dentro del área; fuera = 0
        bin_arr = ((arr == self.valor_fuente) & (msk == 1)).astype("uint8")
        bb.WriteArray(bin_arr)
        bin_ds.FlushCache()
        bin_ds = None
        clip = None
        mask_open = None

        # ---------------------------------------------------------
        # 4) PROXIMITY SOBRE EL RECORTE (NO se calcula afuera del recorte)
        #    y luego aplicamos máscara para que fuera del polígono quede NODATA real.
        # ---------------------------------------------------------
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
                                      "SPARSE_OK=YES",
                                      "BIGTIFF=YES"])
        prox_ds.SetGeoTransform(bin_open.GetGeoTransform())
        prox_ds.SetProjection(bin_open.GetProjection())
        pb = prox_ds.GetRasterBand(1)
        pb.SetNoDataValue(float(NODATA))
        pb.Fill(float(NODATA))

        gdal.ComputeProximity(bin_open.GetRasterBand(1),
                              pb,
                              options=["VALUES=1",
                                       "DISTUNITS=GEO" if is_m else "PIXEL"])
        prox_ds.FlushCache()
        prox_ds = None
        bin_open = None

        # ---------------------------------------------------------
        # 5) Escalar a metros si CRS geográfico
        # ---------------------------------------------------------
        if is_geo and not is_m:
            prox_tmp = gdal.Open(tmp_prox, gdal.GA_ReadOnly)
            gt = prox_tmp.GetGeoTransform()
            prox_tmp = None

            px_m = math.sqrt((abs(gt[1]) * self.m_por_grado_lon) ** 2 +
                             (abs(gt[5]) * self.m_por_grado_lat) ** 2) / math.sqrt(2)

            self._escalar_raster(tmp_prox, tmp_m, px_m, nodata_val=NODATA)
            dist_in = tmp_m
        else:
            dist_in = tmp_prox

        # ---------------------------------------------------------
        # 6) APLICAR MÁSCARA AL RESULTADO FINAL:
        #    fuera del área = NODATA (vacío real, no cero, no relleno)
        # ---------------------------------------------------------
        dist_ds = gdal.Open(dist_in, gdal.GA_ReadOnly)
        mask_ds = gdal.Open(tmp_mask, gdal.GA_ReadOnly)

        if dist_ds is None or mask_ds is None:
            raise RuntimeError("No se pudo abrir dist/mask para enmascarar")

        xsize, ysize = dist_ds.RasterXSize, dist_ds.RasterYSize
        out0 = drv.Create(tmp_out0,
                          xsize,
                          ysize,
                          1,
                          gdal.GDT_Float32,
                          options=["TILED=YES",
                                   "COMPRESS=LZW",
                                   "SPARSE_OK=YES",
                                   "BIGTIFF=YES"])
        out0.SetGeoTransform(dist_ds.GetGeoTransform())
        out0.SetProjection(dist_ds.GetProjection())
        ob = out0.GetRasterBand(1)
        ob.SetNoDataValue(float(NODATA))
        ob.Fill(float(NODATA))

        db = dist_ds.GetRasterBand(1)
        mb = mask_ds.GetRasterBand(1)

        bx, by = db.GetBlockSize()
        if bx <= 0 or by <= 0:
            bx = by = 1024

        for y in range(0, ysize, by):
            h = min(by, ysize - y)
            for x in range(0, xsize, bx):
                w = min(bx, xsize - x)

                darr = db.ReadAsArray(x, y, w, h)
                marr = mb.ReadAsArray(x, y, w, h)

                if darr is None or marr is None:
                    continue

                darr = darr.astype("float32", copy=False)
                out_arr = darr
                out_arr[marr != 1] = float(NODATA)

                ob.WriteArray(out_arr, x, y)

        out0.FlushCache()
        out0 = None
        dist_ds = None
        mask_ds = None

        # ---------------------------------------------------------
        # 7) Guardar FINAL (ya está recortado y enmascarado)
        # ---------------------------------------------------------
        # Copia directa (evita un Warp extra)
        gdal.Translate(out_raster,
                       tmp_out0,
                       creationOptions=["TILED=YES",
                                        "COMPRESS=LZW",
                                        "SPARSE_OK=YES",
                                        "BIGTIFF=YES"])

        # Limpieza
        for p in (tmp_clip, tmp_mask, tmp_bin, tmp_prox, tmp_m, tmp_out0, cutline):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

