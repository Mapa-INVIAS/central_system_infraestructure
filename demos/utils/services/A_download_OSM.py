# -*- coding: utf-8 -*-

import os
import re
import json
import time
import requests
from tqdm import tqdm
import fiona
from fiona.crs import from_epsg


# Teselación para evitar queries gigantes
def generar_tiles(bbox, nx=3, ny=3):
    south, west, north, east = bbox
    dx = (east - west) / nx
    dy = (north - south) / ny

    tiles = []
    for i in range(nx):
        for j in range(ny):
            w = west + i * dx
            e = west + (i + 1) * dx
            s = south + j * dy
            n = south + (j + 1) * dy
            tiles.append((s, w, n, e))
    return tiles


class DownloadOSMVias:
    def __init__(self,
                 carpeta_salida,
                 nombre_salida,
                 bbox,
                 highway_tipos,
                 overpass_url,
                 timeout,
                 reintentos,
                 usar_tqdm,
                 logfile):

        os.makedirs(carpeta_salida, exist_ok=True)
        logfile = logfile or os.path.join(carpeta_salida, "log_osm.txt")

        # dividir Colombia en tiles manejables
        tiles = generar_tiles(bbox, nx=3, ny=3)

        todas_features = []

        for k, tile in enumerate(tiles, start=1):

            tqdm.write(f"\n=== TILE {k}/9 ===")
            filtro = self.construir_filtro_highway(highway_tipos)
            query = self.construir_query_overpass(tile, filtro, timeout)

            datos_osm = self.descargar_osm(overpass_url,
                                           query,
                                           timeout,
                                           reintentos,
                                           logfile)

            if datos_osm is None:
                tqdm.write("Tile vacío o error.")
                continue

            geojson = self.osm_a_geojson(datos_osm, usar_tqdm)

            tqdm.write(f"Features tile {k}: {len(geojson['features'])}")

            todas_features.extend(geojson["features"])

        tqdm.write(f"\nTOTAL FEATURES DESCARGADAS: {len(todas_features)}")

        ruta_salida = os.path.join(carpeta_salida, nombre_salida)
        self.guardar_shp(todas_features, ruta_salida)

        tqdm.write(f"Archivo guardado: {ruta_salida}")


    def write_log(self, logfile, texto):
        with open(logfile, "a", encoding="utf-8") as f:
            f.write(texto + "\n")


    def construir_filtro_highway(self, tipos):
        if tipos == "ALL" or tipos is None:
            return '["highway"]'
        valores = "|".join(tipos)
        return f'["highway"~"{valores}"]'


    def construir_query_overpass(self, bbox, filtro_highway, timeout_seg):
        s, w, n, e = bbox
        return f"""
        [out:json][timeout:{timeout_seg}];
        (way{filtro_highway}({s},{w},{n},{e}););
        out body;
        >;
        out skel qt;
        """.strip()


    def descargar_osm(self, 
                      overpass_url, 
                      query, 
                      timeout, 
                      reintentos, 
                      logfile):
        
        data = {"data": query}

        for intento in range(1, reintentos + 1):
            tqdm.write(f"Intento {intento}...")
            try:
                r = requests.post(overpass_url, 
                                  data=data, 
                                  timeout=timeout+10)
                if r.status_code == 200:
                    return r.json()
            except:
                time.sleep(1)

        return None


    def osm_a_geojson(self, datos_osm, usar_tqdm):
        elementos = datos_osm.get("elements", [])

        # nodos
        nodos = {e["id"]: (e["lon"], e["lat"])
                 for e in elementos if e["type"] == "node"}

        # ways
        ways = [e for e in elementos if e["type"] == "way"]

        iterable = tqdm(ways, desc="ways", disable=not usar_tqdm)

        feats = []
        for w in iterable:
            wid = w["id"]
            tags = w.get("tags", {})
            coords = []

            if "geometry" in w:
                coords = [[p["lon"], p["lat"]] for p in w["geometry"]]
            else:
                for nid in w.get("nodes", []):
                    if nid in nodos:
                        coords.append(list(nodos[nid]))

            if len(coords) < 2:
                continue

            feats.append({"type": "Feature",
                          "geometry": {"type": "LineString",
                                       "coordinates": coords},
                          "properties": {"osm_id": wid,
                                         "type": tags.get("highway", ""),
                                         "name": tags.get("name", "")}})

        return {"type": "FeatureCollection", "features": feats}


    def guardar_shp(self, features, ruta_salida):

        schema = {"geometry": "LineString",
                  "properties": {"osm_id": "int",
                                 "type": "str",
                                 "name": "str"}}

        with fiona.open(ruta_salida,
                        "w",
                        driver="ESRI Shapefile",
                        crs=from_epsg(4326),
                        schema=schema) as dst:

            for feat in features:
                dst.write(feat)