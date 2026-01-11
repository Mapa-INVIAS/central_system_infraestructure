# -*- coding: utf-8 -*-

import os
import re
import glob
import warnings
import math

import numpy as np
import pandas as pd
import geopandas as gpd

from tqdm import tqdm
from shapely.geometry import Point
from shapely.ops import unary_union

import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds


class RecortePorRegion:

    def __init__(self,
                 carpeta_rasters_tif: str,
                 vias_vector_path: str,
                 regiones_shp_path: str,
                 csv_atropellamientos_path: str,
                 carpeta_salida: str,
                 campo_region: str = "REGION",
                 buffer_m: float = 200.0,
                 csv_lat_col: str = "Latitude",
                 csv_lon_col: str = "Longitude",
                 csv_crs_epsg: int = 4326):

        # 1) LISTAR RASTERS
        lista_rasters = self.listar_tifs(carpeta_rasters_tif)
        if len(lista_rasters) == 0:
            raise ValueError(f"No se encontraron .tif/.tiff en: {carpeta_rasters_tif}")

        # 2) LEER VECTORES (vías y regiones)
        vias = self.leer_vector(vias_vector_path, nombre="vías")
        regiones = self.leer_vector(regiones_shp_path, nombre="regiones")

        if campo_region not in regiones.columns:
            raise ValueError(f"El shapefile de regiones no tiene el campo '{campo_region}'. "
                             f"Campos disponibles: {list(regiones.columns)}")

        # 3) LEER CSV Y CONSTRUIR PUNTOS
        puntos = self.leer_csv_puntos(csv_atropellamientos_path,
                                      csv_lat_col=csv_lat_col,
                                      csv_lon_col=csv_lon_col,
                                      csv_crs_epsg=csv_crs_epsg)

        # Alinear puntos al CRS de regiones para filtro within sin reproyección repetida
        puntos = puntos.to_crs(regiones.crs)

        # 4) PROCESAR POR REGIÓN
        os.makedirs(carpeta_salida, exist_ok=True)

        for i in tqdm(range(len(regiones)), desc="Regiones", unit="reg"):
            fila = regiones.iloc[i]
            geom_region = fila.geometry
            nombre_region_raw = fila[campo_region]
            nombre_region = self.sanitizar_nombre_carpeta(nombre_region_raw)

            carpeta_region = os.path.join(carpeta_salida, nombre_region)
            carpeta_rasterin = os.path.join(carpeta_region, "rasterIN")
            os.makedirs(carpeta_region, exist_ok=True)
            os.makedirs(carpeta_rasterin, exist_ok=True)

            # 4.1) Recortar vías
            vias_rec = self.recortar_vias_a_region(vias_gdf=vias,
                                                   region_geom=geom_region,
                                                   region_crs=regiones.crs)
            self.exportar_vias(vias_recortadas=vias_rec,
                               out_path=os.path.join(carpeta_region, "vias.shp"))

            # 4.2) Filtrar puntos a región y exportar CSV
            df_pts_region = self.filtrar_puntos_en_region(puntos_gdf=puntos,
                                                          region_geom=geom_region,
                                                          region_crs=regiones.crs)
            self.exportar_csv(df=df_pts_region,
                              out_path=os.path.join(carpeta_region, "atropellamientos.csv"))

            # 4.3) Recortar rasters por máscara región+buffer (NODATA REAL)
            for raster_path in tqdm(lista_rasters, desc=f"Rasters {nombre_region}", unit="tif", leave=False):
                nombre_tif = os.path.basename(raster_path)
                out_raster = os.path.join(carpeta_rasterin, nombre_tif)

                self.recortar_raster_a_region_mascara_nodata_real(raster_path=raster_path,
                                                                  region_geom=geom_region,
                                                                  region_crs=regiones.crs,
                                                                  buffer_m=buffer_m,
                                                                  out_path=out_raster)


    def listar_tifs(self, carpeta_rasters_tif: str) -> list:
        patron1 = os.path.join(carpeta_rasters_tif, "*.tif")
        patron2 = os.path.join(carpeta_rasters_tif, "*.tiff")
        archivos = glob.glob(patron1) + glob.glob(patron2)
        return sorted(list(set(archivos)))

    def leer_vector(self, vector_path: str, nombre: str) -> gpd.GeoDataFrame:
        gdf = gpd.read_file(vector_path)
        if gdf.crs is None:
            raise ValueError(f"La capa '{nombre}' no tiene CRS definido.")
        if gdf.empty:
            warnings.warn(f"La capa '{nombre}' está vacía: {vector_path}")
        return gdf

    def leer_csv_puntos(self, csv_path: str, csv_lat_col: str, csv_lon_col: str, csv_crs_epsg: int) -> gpd.GeoDataFrame:
        df = pd.read_csv(csv_path)

        if csv_lat_col not in df.columns or csv_lon_col not in df.columns:
            raise ValueError(f"El CSV debe tener columnas '{csv_lat_col}' y '{csv_lon_col}'. "
                             f"Columnas encontradas: {list(df.columns)}")

        if df.empty:
            warnings.warn("El CSV está vacío. Se exportarán CSVs vacíos por región.")
            return gpd.GeoDataFrame(df, geometry=[], crs=f"EPSG:{csv_crs_epsg}")

        lat = pd.to_numeric(df[csv_lat_col], errors="coerce")
        lon = pd.to_numeric(df[csv_lon_col], errors="coerce")

        validos = lat.notna() & lon.notna()
        df2 = df.loc[validos].copy()

        geometria = [Point(xy) for xy in zip(df2[csv_lon_col].astype(float), df2[csv_lat_col].astype(float))]
        return gpd.GeoDataFrame(df2, geometry=geometria, crs=f"EPSG:{csv_crs_epsg}")

    def sanitizar_nombre_carpeta(self, nombre_region) -> str:
        nombre = str(nombre_region).strip()
        nombre = re.sub(r"[\\/:\*\?\"<>\|\n\r\t]+", "_", nombre)
        nombre = re.sub(r"\s+", " ", nombre).strip()
        return nombre if nombre else "REGION_SIN_NOMBRE"

    def recortar_vias_a_region(self, vias_gdf: gpd.GeoDataFrame, region_geom, region_crs) -> gpd.GeoDataFrame:
        region_gdf = gpd.GeoDataFrame({"geometry": [region_geom]}, crs=region_crs).to_crs(vias_gdf.crs)
        region_union = unary_union(region_gdf.geometry.values)

        vias_ok = vias_gdf[vias_gdf.geometry.notna()].copy()
        if vias_ok.empty:
            return vias_ok

        vias_ok = vias_ok[vias_ok.intersects(region_union)]
        if vias_ok.empty:
            return vias_ok

        vias_ok["geometry"] = vias_ok.geometry.intersection(region_union)
        vias_ok = vias_ok[vias_ok.geometry.notna() & ~vias_ok.geometry.is_empty].copy()
        return vias_ok

    def exportar_vias(self, vias_recortadas: gpd.GeoDataFrame, out_path: str) -> None:
        if vias_recortadas is None or vias_recortadas.empty:
            return

        try:
            vias_recortadas.to_file(out_path, driver="ESRI Shapefile", encoding="utf-8")
        except Exception:
            gpkg = os.path.splitext(out_path)[0] + ".gpkg"
            vias_recortadas.to_file(gpkg, driver="GPKG")

    def filtrar_puntos_en_region(self, puntos_gdf: gpd.GeoDataFrame, region_geom, region_crs) -> pd.DataFrame:
        region_gdf = gpd.GeoDataFrame({"geometry": [region_geom]}, crs=region_crs)
        pol = region_gdf.geometry.iloc[0]

        if puntos_gdf is None or puntos_gdf.empty:
            cols = [c for c in (puntos_gdf.columns if puntos_gdf is not None else []) if c != "geometry"]
            return pd.DataFrame(columns=cols)

        sub = puntos_gdf[puntos_gdf.geometry.within(pol)].copy()
        if sub.empty:
            columnas = [c for c in puntos_gdf.columns if c != "geometry"]
            return pd.DataFrame(columns=columnas)

        return pd.DataFrame(sub.drop(columns=["geometry"]))

    def exportar_csv(self, df: pd.DataFrame, out_path: str) -> None:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8")


    # RASTER — CORTE LIMPIO (elimina extremos ±3.402823e38)
    def _get_nodata_robusto(self, src: rasterio.io.DatasetReader):
        # 1) nodata directo
        nodata = src.nodata
        if nodata is not None:
            return nodata

        # 2) nodatavals (lista por banda)
        try:
            if getattr(src, "nodatavals", None):
                nv = src.nodatavals[0]
                if nv is not None:
                    return nv
        except Exception:
            pass

        # 3) tag GDAL_NODATA
        try:
            tag = src.tags().get("GDAL_NODATA", None)
            if tag is None:
                tag = src.tags(1).get("GDAL_NODATA", None)
            if tag is not None:
                try:
                    return float(tag)
                except Exception:
                    return None
        except Exception:
            pass

        return None

    def recortar_raster_a_region_mascara_nodata_real(self,
                                                     raster_path: str,
                                                     region_geom,
                                                     region_crs,
                                                     buffer_m: float,
                                                     out_path: str) -> None:
        try:
            with rasterio.open(raster_path) as src:

                # Región -> CRS del raster
                reg = gpd.GeoDataFrame({"geometry": [region_geom]}, crs=region_crs).to_crs(src.crs)
                geom = reg.geometry.iloc[0]

                # Buffer en CRS del raster (si es geográfico, convertir metros->grados)
                if src.crs is not None and src.crs.is_geographic:
                    c = geom.centroid
                    lat = float(c.y)
                    m_per_deg_lat = 111320.0
                    m_per_deg_lon = 111320.0 * max(1e-9, math.cos(math.radians(lat)))
                    deg_lat = float(buffer_m) / m_per_deg_lat
                    deg_lon = float(buffer_m) / m_per_deg_lon
                    buf_deg = max(deg_lat, deg_lon)
                    geom_buf = geom.buffer(buf_deg)
                else:
                    geom_buf = geom.buffer(float(buffer_m))

                # bounds intersect rápido
                rminx, rminy, rmaxx, rmaxy = src.bounds
                gminx, gminy, gmaxx, gmaxy = geom_buf.bounds
                if (gmaxx <= rminx) or (gminx >= rmaxx) or (gmaxy <= rminy) or (gminy >= rmaxy):
                    return

                # ventana ajustada al raster
                minx = max(gminx, rminx)
                miny = max(gminy, rminy)
                maxx = min(gmaxx, rmaxx)
                maxy = min(gmaxy, rmaxy)

                window = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
                window = window.round_offsets().round_lengths()
                if window.width <= 0 or window.height <= 0:
                    return

                # leer ventana (masked=True preserva la máscara original)
                data = src.read(window=window, masked=True)
                transform = src.window_transform(window)

                # máscara geométrica (True = dentro)
                inside = geometry_mask([geom_buf],
                                       out_shape=(data.shape[1], data.shape[2]),
                                       transform=transform,
                                       invert=True)

                # base mask a 3D
                base_mask = data.mask
                if np.isscalar(base_mask):
                    base_mask = np.zeros(data.shape, dtype=bool)
                elif base_mask.ndim == 2:
                    base_mask = np.broadcast_to(base_mask, data.shape)

                outside = ~inside
                outside_3d = np.broadcast_to(outside, data.shape)

                # máscara final: nodata original OR fuera del buffer
                final_mask = base_mask | outside_3d

                # nodata real (robusto)
                nodata = self._get_nodata_robusto(src)
                dtype = np.dtype(src.dtypes[0])

                # si float y nodata no existe, usar NaN como nodata real
                if nodata is None and np.issubdtype(dtype, np.floating):
                    nodata = np.nan

                profile = src.profile.copy()
                profile.update({"height": data.shape[1],
                                "width": data.shape[2],
                                "transform": transform})

                # fijar nodata si lo tenemos (esto hace que QGIS/ArcGIS excluyan extremos en stats/render)
                if nodata is not None:
                    profile["nodata"] = nodata
                else:
                    profile.pop("nodata", None)

                os.makedirs(os.path.dirname(out_path), exist_ok=True)

                # CLAVE: forzar que todo lo enmascarado se escriba como nodata (y NO quede el valor extremo como dato)
                if nodata is not None:
                    out_arr = np.array(data.data, copy=True)
                    # aplicar nodata en máscara final
                    for b in range(out_arr.shape[0]):
                        out_arr[b][final_mask[b]] = nodata
                else:
                    # si no tenemos nodata numérico, igual limpiamos con 0, pero la máscara manda
                    out_arr = np.array(data.data, copy=True)
                    for b in range(out_arr.shape[0]):
                        out_arr[b][final_mask[b]] = 0

                with rasterio.open(out_path, "w", **profile) as dst:
                    dst.write(out_arr)

                    # máscara real (0 nodata, 255 válido)
                    valid = (~final_mask).all(axis=0).astype(np.uint8) * 255
                    dst.write_mask(valid)

                    # copiar escalas/offsets si existen
                    try:
                        if getattr(src, "scales", None) is not None:
                            dst.scales = src.scales
                        if getattr(src, "offsets", None) is not None:
                            dst.offsets = src.offsets
                    except Exception:
                        pass

                    # copiar tags (por si hay metadata relevante)
                    try:
                        dst.update_tags(**src.tags())
                        for bi in range(1, src.count + 1):
                            dst.update_tags(bi, **src.tags(bi))
                    except Exception:
                        pass

        except Exception:
            return


# ======================================================
# EJECUCIÓN
# ======================================================
if __name__ == "__main__":

    CARPETA_RASTERS_TIF = r"D:/Downloads/juanDavid_Sukubun/out2_kr/capas/E_Alineadas"  # Rasteres ya alineados
    VIAS_VECTOR_PATH = r"D:/Downloads/juanDavid_Sukubun/Vias_Total.shp" # capa de vias original de INVIAS
    REGIONES_SHP_PATH = r"D:/Downloads/regiones/regiones.shp" # capa deregiones geográficas
    CSV_ATROPELLAMIENTOS_PATH = r"D:/Downloads/juanDavid_Sukubun/out2_kr/hotspots.csv" # .csv de hotspot sacados desde el K-ripley

    CARPETA_SALIDA = r"D:/Downloads/juanDavid_Sukubun/salida_Clip"

    CAMPO_REGION = "REGION" # campo de la capa de regiones
    BUFFER_M = 200.0 # este buffer se usa para que existaq un área de guarda 

    CSV_LAT_COL = "Latitude"
    CSV_LON_COL = "Longitude"
    CSV_CRS_EPSG = 4326

    RecortePorRegion(CARPETA_RASTERS_TIF,
                     VIAS_VECTOR_PATH,
                     REGIONES_SHP_PATH,
                     CSV_ATROPELLAMIENTOS_PATH,
                     CARPETA_SALIDA,
                     CAMPO_REGION,
                     BUFFER_M,
                     CSV_LAT_COL,
                     CSV_LON_COL,
                     CSV_CRS_EPSG)
