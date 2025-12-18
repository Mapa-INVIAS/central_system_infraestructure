# -*- coding: utf-8 -*-

import os
import re
import json
import time
import requests
from tqdm import tqdm
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

print_lock = Lock()   # Evita choques de impresión entre hilos

class Downloadserver_REST:

    def __init__(self,
                 url_servicio,
                 carpeta_salida,
                 target_ids,
                 bbox_fijo,
                 chunk_inicial,
                 min_chunk,
                 timeout,
                 reintentos,
                 usar_tqdm,
                 usar_paralelo,
                 max_workers,
                 max_depth,
                 sleep_s,
                 umbral_paralelo,
                 formato_salida,
                 wkid_salida):

        os.makedirs(carpeta_salida, exist_ok=True)

        service_info = self._request_json(url_servicio, 
                                          {"f": "json"}, 
                                          timeout, 
                                          reintentos)
        
        if not service_info:
            raise RuntimeError("No se pudo leer el servicio")

        capas = self._obtener_capas(service_info, target_ids)
        iterable = tqdm(capas, 
                        desc="Capas", 
                        position=0, 
                        leave=True) if usar_tqdm else capas

        for capa in iterable:
            self._procesar_capa(url_servicio,
                                service_info,
                                capa,
                                carpeta_salida,
                                bbox_fijo,
                                chunk_inicial,
                                min_chunk,
                                timeout,
                                reintentos,
                                usar_tqdm,
                                usar_paralelo,
                                max_workers,
                                max_depth,
                                sleep_s,
                                umbral_paralelo,
                                formato_salida,
                                wkid_salida)


    def _request_json(self, url, params, timeout, reintentos):
        for _ in range(reintentos):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                j = r.json()
                if isinstance(j, dict) and "error" in j:
                    time.sleep(1)
                    continue
                return j
            except Exception:
                time.sleep(1)
        return None

    def _obtener_capas(self, service_info, target_ids):
        capas = service_info.get("layers", []) or [{"id": 0, "name": "layer_0"}]
        if target_ids == "ALL":
            return capas
        d = {c["id"]: c for c in capas}
        return [d[i] for i in target_ids if i in d]

    def _leer_wkid(self, layer_info, service_info):
        def _wk(sr):
            if not isinstance(sr, dict):
                return None
            return sr.get("latestWkid") or sr.get("wkid")

        return (_wk(layer_info.get("extent", {}).get("spatialReference", {}))
                or _wk(layer_info.get("spatialReference", {}))
                or _wk(service_info.get("spatialReference", {}))
                or _wk(service_info.get("fullExtent", {}).get("spatialReference", {})))

    def _leer_bbox(self, layer_info, service_info, bbox_fijo, wkid):
        if bbox_fijo:
            return {"xmin": bbox_fijo[0],
                    "ymin": bbox_fijo[1],
                    "xmax": bbox_fijo[2],
                    "ymax": bbox_fijo[3],
                    "spatialReference": {"wkid": wkid}}
        return layer_info.get("extent") or service_info.get("fullExtent")

    def _limpiar_nombre(self, t):
        return re.sub(r"[^A-Za-z0-9_-]+", "_", str(t).strip())


    # ESTABLE CON OIDs
    def _obtener_oids(self, 
                      url_servicio, 
                      layer_id, 
                      env, 
                      wkid, 
                      timeout, 
                      reintentos):
        params = {"f": "json",
                  "where": "1=1",
                  "returnIdsOnly": "true",
                  "geometryType": "esriGeometryEnvelope",
                  "inSR": wkid}

        if env:
            params["geometry"] = f"{env['xmin']},{env['ymin']},{env['xmax']},{env['ymax']}"
            params["spatialRel"] = "esriSpatialRelIntersects"

        d = self._request_json(f"{url_servicio}/{layer_id}/query", 
                               params, 
                               timeout, 
                               reintentos)
        if not d:
            return []

        oids = d.get("objectIds") or []
        try:
            oids = sorted(oids)
        except Exception:
            pass
        return oids

    def _descargar_por_oids(self,
                            url_servicio,
                            layer_id,
                            oids,
                            chunk_oids,
                            timeout,
                            reintentos,
                            usar_tqdm,
                            sleep_s,
                            posicion_barra):

        features = []

        pbar = tqdm(desc=f"OIDs {posicion_barra}",
                    unit="oid",
                    disable=not usar_tqdm,
                    position=posicion_barra,
                    leave=False,
                    mininterval=0.15)

        n = len(oids)
        i = 0
        while i < n:
            lote = oids[i:i + chunk_oids]

            params = {"f": "json",
                      "where": "1=1",
                      "objectIds": ",".join(map(str, lote)),
                      "outFields": "*",
                      "returnGeometry": "true"}

            d = self._request_json(f"{url_servicio}/{layer_id}/query", 
                                   params, 
                                   timeout, 
                                   reintentos)
            
            if d and "features" in d:
                features.extend(d["features"])

            i += len(lote)
            pbar.update(len(lote))

            if sleep_s:
                time.sleep(sleep_s)

        pbar.close()
        return features

    # Reproyección de entidades
    def _reproyectar_features_esri(self, 
                                   features, 
                                   geometry_type, 
                                   wkid_in, 
                                   wkid_out):
        
        if not features or wkid_in == wkid_out:
            return features

        from pyproj import Transformer
        transformer = Transformer.from_crs(f"EPSG:{wkid_in}", 
                                           f"EPSG:{wkid_out}", 
                                           always_xy=True)

        def tr(x, y):
            return transformer.transform(x, y)

        for f in features:
            g = f.get("geometry")
            if not g:
                continue

            if geometry_type == "esriGeometryPoint":
                g["x"], g["y"] = tr(g["x"], g["y"])

            elif geometry_type == "esriGeometryPolyline":
                g["paths"] = [[list(tr(*p)) for p in path] 
                              for path in g["paths"]]

            elif geometry_type == "esriGeometryPolygon":
                g["rings"] = [[[list(tr(*p)) for p in ring] 
                               for ring in g["rings"]]][0]

        return features

    def _esri_feature_to_geojson(self, feat, geometry_type):
        attrs = feat.get("attributes", {})
        g = feat.get("geometry")

        if geometry_type == "esriGeometryPoint":
            geom = {"type": "Point", "coordinates": [g["x"], g["y"]]}
        elif geometry_type == "esriGeometryPolyline":
            geom = {"type": "LineString", "coordinates": g["paths"][0]}
        elif geometry_type == "esriGeometryPolygon":
            geom = {"type": "Polygon", "coordinates": g["rings"]}
        else:
            geom = None

        return {"type": "Feature", "properties": attrs, "geometry": geom}

    def _exportar_final(self,
                        features_esri,
                        layer_info,
                        carpeta_salida,
                        lid,
                        lname,
                        formato_salida,
                        wkid_in,
                        wkid_out):

        geom_type = layer_info.get("geometryType")
        features_esri = self._reproyectar_features_esri(features_esri, 
                                                        geom_type, 
                                                        wkid_in, 
                                                        wkid_out )

        gj = {"type": "FeatureCollection",
              "features": [self._esri_feature_to_geojson(ft, geom_type) 
                           for ft in features_esri]}

        base = f"{lid}_{self._limpiar_nombre(lname)}"
        out = os.path.join(carpeta_salida, f"{base}.{formato_salida}")

        if formato_salida == "geojson":
            with open(out, "w", encoding="utf-8") as f:
                json.dump(gj, f, ensure_ascii=False)
        else:
            import geopandas as gpd
            gdf = gpd.GeoDataFrame.from_features(gj["features"], 
                                                 crs=f"EPSG:{wkid_out}")
            gdf.to_file(out, driver="ESRI Shapefile", encoding="utf-8")

        return out, len(gj["features"])

    # Procesar capas con formato
    def _procesar_capa(self,
                       url_servicio,
                       service_info,
                       capa,
                       carpeta_salida,
                       bbox_fijo,
                       chunk_inicial,
                       min_chunk,
                       timeout,
                       reintentos,
                       usar_tqdm,
                       usar_paralelo,
                       max_workers,
                       max_depth,
                       sleep_s,
                       umbral_paralelo,
                       formato_salida,
                       wkid_salida):

        lid = capa["id"]
        lname = capa.get("name", f"layer_{lid}")
        print(f"\nDescargando capa {lid}: {lname}")

        layer_info = self._request_json(f"{url_servicio}/{lid}", 
                                        {"f": "json"}, 
                                        timeout, 
                                        reintentos)
        if not layer_info:
            print("Capa no disponible")
            return

        wkid_in = self._leer_wkid(layer_info, service_info) or 4326
        env = self._leer_bbox(layer_info, service_info, bbox_fijo, wkid_in)

        oids = self._obtener_oids(url_servicio, 
                                  lid, 
                                  env, 
                                  wkid_in, 
                                  timeout, 
                                  reintentos)
        if not oids:
            print("Sin datos")
            return

        features = self._descargar_por_oids(url_servicio, 
                                            lid, 
                                            oids,
                                            chunk_inicial, 
                                            timeout, 
                                            reintentos,
                                            usar_tqdm, 
                                            sleep_s, 
                                            1)

        out, n = self._exportar_final(features,
                                      layer_info,
                                      carpeta_salida,
                                      lid,
                                      lname,
                                      formato_salida,
                                      wkid_in,
                                      wkid_salida)

        print(f"OK: {out} ({n} features)")