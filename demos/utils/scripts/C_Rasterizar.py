# -*- coding: utf-8 -*-

import os
import math
import warnings
import geopandas as gpd
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from rasterio.mask import mask
from tqdm import tqdm

# Silenciar warnings irrelevantes
warnings.filterwarnings("ignore",
                        category=RuntimeWarning,
                        message=".*winding order.*")

class RasterizarCarpetaSHP:

    def __init__(self,
                 carpeta_shp,
                 geojson_referencia,
                 carpeta_salida,
                 pixel_m):

        os.makedirs(carpeta_salida, exist_ok=True)

        # Leer GEOJSON de referencia (área efectiva)
        ref = gpd.read_file(geojson_referencia)

        if ref.empty:
            raise RuntimeError("El GEOJSON de referencia está vacío")

        if ref.crs is None:
            raise RuntimeError("El GEOJSON de referencia no tiene CRS")

        crs_ref = ref.crs
        geometria_ref = ref.geometry.unary_union

        minx, miny, maxx, maxy = geometria_ref.bounds

        # Tamaño de pixel en unidades CRS
        pixel_x, pixel_y = self._pixel_en_unidades(pixel_m, 
                                                   crs_ref, 
                                                   miny, 
                                                   maxy)

        width = int((maxx - minx) / pixel_x)
        height = int((maxy - miny) / pixel_y)

        if width <= 0 or height <= 0:
            raise RuntimeError("Dimensiones raster inválidas")

        transform = from_origin(minx, maxy, pixel_x, pixel_y)

        # SHP a procesar
        shps = [f for f in os.listdir(carpeta_shp)
                if f.lower().endswith(".shp")]

        if not shps:
            raise RuntimeError("No se encontraron SHP")

        # Rasterizar cada SHP
        for shp in tqdm(shps, desc="Rasterizando por área efectiva", unit="capa"):
            self._rasterizar_uno(os.path.join(carpeta_shp, shp),
                                 carpeta_salida,
                                 crs_ref,
                                 transform,
                                 width,
                                 height,
                                 geometria_ref)

    def _pixel_en_unidades(self, pixel_m, crs, miny, maxy):

        if crs.is_geographic:
            lat_media = (miny + maxy) / 2.0
            lat_rad = math.radians(lat_media)

            metros_por_grado_lat = 111320.0
            metros_por_grado_lon = 111320.0 * math.cos(lat_rad)

            return (pixel_m / metros_por_grado_lon,
                    pixel_m / metros_por_grado_lat)

        return float(pixel_m), float(pixel_m)

    def _rasterizar_uno(self,
                        ruta_shp,
                        carpeta_salida,
                        crs_ref,
                        transform,
                        width,
                        height,
                        geometria_ref):

        nombre = os.path.splitext(os.path.basename(ruta_shp))[0]
        out_raster = os.path.join(carpeta_salida, f"{nombre}.tif")

        gdf = gpd.read_file(ruta_shp)

        if gdf.empty:
            return

        if gdf.crs != crs_ref:
            gdf = gdf.to_crs(crs_ref)

        # Rasterización inicial (grilla completa)
        raster = rasterize(((geom, 1) for geom in gdf.geometry),
                            out_shape=(height, width),
                            transform=transform,
                            fill=0,
                            dtype="uint8",
                            all_touched=False)


        # Guardar raster temporal
        with rasterio.open(out_raster,
                           "w",
                           driver="GTiff",
                           height=height,
                           width=width,
                           count=1,
                           dtype="uint8",
                           crs=crs_ref,
                           transform=transform,
                           nodata=0,
                           compress="lzw") as dst:
             dst.write(raster, 1)

        # MÁSCARA REAL → área efectiva del GEOJSON
        with rasterio.open(out_raster) as src:
            masked, new_transform = mask(src,
                                         [geometria_ref],
                                         crop=True,
                                         nodata=0)

            meta = src.meta.copy()
            meta.update({"height": masked.shape[1],
                         "width": masked.shape[2],
                         "transform": new_transform})

        # Sobrescribir raster FINAL (limpio)
        with rasterio.open(out_raster, "w", **meta) as dst:
            dst.write(masked)