
# -*- coding: utf-8 -*-
# ============================================================
# Procesamiento MaxEnt + Exportación por ETAPA (TIFF + GEOJSON)
# ============================================================

import os
import numpy as np
import geopandas as gpd
import rasterio
from rasterio import features
from shapely.geometry import shape
from tqdm import tqdm


class PostProcesamientoMaxEnt:

    def __init__(self,
                 ruta_raster_maxent,
                 ruta_vias,
                 carpeta_salida,
                 min_area_m2,
                 campo_clase,
                 campo_rango,
                 driver_salida=None):

        print("=== PostProcesamientoMaxEnt: INICIO ===")

        # Asegurar carpeta de salidas
        self.carpeta_salida = carpeta_salida
        os.makedirs(self.carpeta_salida, exist_ok=True)

        base_name = os.path.splitext(os.path.basename(ruta_raster_maxent))[0]

        # Rutas de salida para CADA ETAPA
        self.path_reclas = os.path.join(self.carpeta_salida, f"{base_name}_reclas.tif")
        self.path_poligonos = os.path.join(self.carpeta_salida, f"{base_name}_poligonos.geojson")
        self.path_segmentos_intersect = os.path.join(self.carpeta_salida, f"{base_name}_segmentos.geojson")
        self.path_segmentos_disueltos = os.path.join(self.carpeta_salida, f"{base_name}_segmentos_disueltos.geojson")
        # salida final (GPKG por defecto)
        self.path_segmentos_final = os.path.join(self.carpeta_salida, f"{base_name}_vias_segmentadas.gpkg")

        # ---- 1: Reclasificación ----
        ruta_reclas, crs_raster, transform_r = self.reclasificar_raster(ruta_raster_maxent, self.path_reclas)

        # ---- 2: Vectorizar y filtrar ----
        gdf_poligonos = self.vectorizar_y_filtrar(ruta_reclas, 
                                                  crs_raster, 
                                                  transform_r, 
                                                  min_area_m2,
                                                  campo_clase, 
                                                  campo_rango, 
                                                  self.path_poligonos)

        # ---- 3: Cargar vías ----
        gdf_vias = self.cargar_vias(ruta_vias)

        # ---- 4: Segmentar ----
        gdf_segmentos = self.segmentar_vias(gdf_vias, gdf_poligonos,
                                            campo_clase, campo_rango,
                                            self.path_segmentos_intersect,
                                            self.path_segmentos_disueltos)

        # ---- 5: Guardar salida final (GPKG o SHP) ----
        self.guardar_segmentos(gdf_segmentos, self.path_segmentos_final, driver_salida)

        print("=== FIN ===")

    # ------------------------------------------------------------
    # 1. Reclasificar raster → Exporta .TIF
    # ------------------------------------------------------------
    def reclasificar_raster(self, ruta, path_out):

        print("[1/5] Reclasificando ráster...")

        with rasterio.open(ruta) as src:
            data = src.read(1, masked=True).astype("float32")
            profile = src.profile.copy()
            crs_r = src.crs
            transform = src.transform

        # limitar a [0,1]
        data = np.where((data >= 0) & (data <= 1), data, np.nan)

        # clases 0–9
        clases = np.floor(data * 10).astype("float32")
        clases = np.where(np.isnan(data), -9999, clases)
        clases = np.where(clases > 9, 9, clases)

        profile.update(dtype="float32", nodata=-9999)

        # ---- Guardar TIFF ----
        with rasterio.open(path_out, "w", **profile) as dst:
            dst.write(clases, 1)

        print("Ráster reclasificado guardado en:", path_out)
        return path_out, crs_r, transform

    # ------------------------------------------------------------
    # 2. Vectorizar → Exporta .GEOJSON
    # ------------------------------------------------------------
    def vectorizar_y_filtrar(self,
                             ruta_r,
                             crs_raster,
                             transform_r,
                             min_area_m2,
                             campo_clase,
                             campo_rango,
                             path_out_geojson):

        print("[2/5] Vectorizando polígonos...")

        with rasterio.open(ruta_r) as src:
            data = src.read(1)
            nodata = src.nodata
            transform = src.transform

        mask = data != nodata
        polys = []

        for geom, value in tqdm(features.shapes(data, mask=mask, transform=transform)):
            if value < 0 or value > 9:
                continue
            g = shape(geom)
            polys.append((g, int(value)))

        if not polys:
            print("  -> No hay polígonos válidos.")
            gdf = gpd.GeoDataFrame(columns=["geometry", campo_clase], crs=crs_raster)
            gdf.to_file(path_out_geojson, driver="GeoJSON")
            print("Polígonos vacíos guardados:", path_out_geojson)
            return gdf

        gdf = gpd.GeoDataFrame(polys, columns=["geometry", campo_clase], crs=crs_raster)

        # reproyectar a métrico
        gdf = gdf.to_crs(3857)

        # filtrar por área
        gdf["area_m2"] = gdf.area
        gdf = gdf[gdf["area_m2"] >= min_area_m2].copy()

        # rango textual
        gdf[campo_rango] = gdf[campo_clase].apply(lambda c: f"{c*0.1:.1f}-{(c+1)*0.1:.1f}")

        # volver al CRS original
        gdf = gdf.to_crs(crs_raster)

        # ---- Guardar GEOJSON ----
        gdf.to_file(path_out_geojson, driver="GeoJSON")
        print("Polígonos guardados en:", path_out_geojson)

        return gdf

    # ------------------------------------------------------------
    # 3. Cargar vías
    # ------------------------------------------------------------
    def cargar_vias(self, ruta):

        print("[3/5] Cargando vías...")
        vias = gpd.read_file(ruta)

        if vias.empty:
            print("ADVERTENCIA: capa de vías vacía.")

        return vias

    # ------------------------------------------------------------
    # 4. Segmentación → Exporta 2 GEOJSON:
    #       • Intersección
    #       • Disolución
    # ------------------------------------------------------------
    def segmentar_vias(self,
                       gdf_vias,
                       gdf_poligonos,
                       campo_clase,
                       campo_rango,
                       path_intersect,
                       path_disueltos):

        print("[4/5] Segmentando vías...")

        if gdf_poligonos is None or gdf_poligonos.empty:
            print("Sin polígonos. No se segmenta, devuelvo vías originales.")
            return gdf_vias

        # Asegurar CRS coherente
        if gdf_vias.crs != gdf_poligonos.crs:
            gdf_poligonos = gdf_poligonos.to_crs(gdf_vias.crs)

        # ---- OVERLAY ----
        print("Ejecutando overlay INTERSECTION...")
        seg = gpd.overlay(gdf_vias,
                          gdf_poligonos[[campo_clase, campo_rango, "geometry"]],
                          how="intersection")

        seg = seg[seg.geometry.type.isin(["LineString", "MultiLineString"])].copy()

        # Guardar intersección
        seg.to_file(path_intersect, driver="GeoJSON")
        print("Segmentos por intersección guardados:", path_intersect)

        # ---- DISOLVER ----
        print("Disolviendo SEGMENTOS por:", campo_rango)
        seg_dis = seg.dissolve(by=campo_rango, as_index=False)

        seg_dis.to_file(path_disueltos, driver="GeoJSON")
        print("Segmentos disueltos guardados:", path_disueltos)

        return seg_dis

    # ------------------------------------------------------------
    # 5. Guardar salida final (GPKG o SHP) + limpiar campos problemáticos
    # ------------------------------------------------------------
    def guardar_segmentos(self, gdf, ruta, driver_salida):

        print("[5/5] Guardando salida final...")

        if gdf is None or gdf.empty:
            print("Nada que guardar.")
            return

        # eliminar campos problemáticos (fid, id, ogc_fid, etc.)
        cols_bad = ["fid", "FID", "id", "ID", "ogc_fid", "OGC_FID"]
        cols_ex = [c for c in gdf.columns if c in cols_bad]
        if cols_ex:
            print("  -> Eliminando campos problemáticos:", cols_ex)
            gdf = gdf.drop(columns=cols_ex)

        ext = os.path.splitext(ruta)[1].lower()
        if driver_salida is None:
            driver_salida = "GPKG" if ext == ".gpkg" else "ESRI Shapefile"

        if os.path.exists(ruta):
            os.remove(ruta)

        gdf.to_file(ruta, driver=driver_salida)
        print("Archivo final guardado:", ruta)



# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================
if __name__ == "__main__":

    ruta_raster_maxent =    r"RasterResult/raster_resultado1_1.tif"
    ruta_vias =             r"a_Pacifico/vias.shp"
    ruta_salidas =          r"a_Pacifico/viaRANGOS/vector_salida"
    min_area_m2=            100
    campo_clase=            "clase_prob"
    campo_rango=            "rango_prob"

    PostProcesamientoMaxEnt(ruta_raster_maxent,
                            ruta_vias,
                            ruta_salidas,
                            min_area_m2,
                            campo_clase,
                            campo_rango)