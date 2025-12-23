# -*- coding: utf-8 -*-
# ============================================================
# Downloadserver_REST (RAPIDO + COMPLETO + SHP)
#
# FIRMA ORIGINAL (NO CAMBIA):
# Downloadserver_REST(URL,
#                     SALIDA,
#                     TARGET_IDS,
#                     BBOX,
#                     CHUNK_INICIAL,
#                     MIN_CHUNK,
#                     TIMEOUT,
#                     REINTENTOS,
#                     USAR_TQDM,
#                     USAR_PARALELO,
#                     MAX_WORKERS,
#                     MAX_DEPTH,
#                     SLEEP,
#                     UMBRAL_PARALELO,
#                     FORMATO_SALIDA,
#                     WKID_SALIDA)
#
# Estrategia:
# - OIDs (returnIdsOnly) -> descarga por lotes objectIds
# - Maneja exceededTransferLimit bajando el chunk automaticamente
# - Convierte BIEN: Point/MultiPoint, LineString/MultiLineString,
#   Polygon con huecos y MultiPolygon
# - Exporta SHP real (o GeoJSON si pides "geojson")
# ============================================================

import os
import re
import json
import time
import requests

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import geopandas as gpd
except Exception as e:
    raise RuntimeError("Falta geopandas. Instala: pip install geopandas") from e

try:
    from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon
except Exception as e:
    raise RuntimeError("Falta shapely. Instala: pip install shapely") from e


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

        self.url_servicio = str(url_servicio).rstrip("/")
        self.carpeta_salida = carpeta_salida
        self.target_ids = target_ids
        self.bbox_fijo = bbox_fijo

        self.chunk_inicial = int(chunk_inicial)
        self.min_chunk = int(min_chunk)
        self.timeout = int(timeout)
        self.reintentos = int(reintentos)

        self.usar_tqdm = bool(usar_tqdm)
        self.formato_salida = (str(formato_salida).strip().lower() if formato_salida else "shp")
        self.wkid_salida = int(wkid_salida) if wkid_salida else 4326

        self.sleep_s = float(sleep_s) if sleep_s else 0.0

        os.makedirs(self.carpeta_salida, exist_ok=True)

        service_info = self._request_json(f"{self.url_servicio}", {"f": "json"})
        if not service_info:
            raise RuntimeError("No se pudo leer el servicio REST")

        capas = self._obtener_capas(service_info, self.target_ids)

        iterable = capas
        if self.usar_tqdm and tqdm is not None:
            iterable = tqdm(capas, desc="Capas", ncols=110, leave=True)

        resumen = []
        for capa in iterable:
            r = self._procesar_capa(service_info, capa)
            if r:
                resumen.append(r)

        with open(os.path.join(self.carpeta_salida, "RESUMEN_DESCARGA.json"),
                  "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2)

    # =========================================================
    # HTTP
    # =========================================================
    def _request_json(self, url, params_or_data):
        last_err = None
        for _ in range(self.reintentos):
            try:
                r = requests.get(url, params=params_or_data, timeout=self.timeout)
                r.raise_for_status()
                j = r.json()
                if isinstance(j, dict) and "error" in j:
                    last_err = j.get("error")
                    time.sleep(1)
                    continue
                return j
            except Exception as e:
                last_err = str(e)
                time.sleep(1)

        # fallback POST
        for _ in range(self.reintentos):
            try:
                r = requests.post(url, data=params_or_data, timeout=self.timeout)
                r.raise_for_status()
                j = r.json()
                if isinstance(j, dict) and "error" in j:
                    last_err = j.get("error")
                    time.sleep(1)
                    continue
                return j
            except Exception as e:
                last_err = str(e)
                time.sleep(1)

        return None

    # =========================================================
    # Util
    # =========================================================
    def _limpiar_nombre(self, t):
        return re.sub(r"[^A-Za-z0-9_-]+", "_", str(t).strip())

    def _obtener_capas(self, service_info, target_ids):
        capas = service_info.get("layers", []) or [{"id": 0, "name": "layer_0"}]
        if target_ids == "ALL" or target_ids is None:
            return capas
        d = {c.get("id"): c for c in capas if "id" in c}
        out = []
        for i in target_ids:
            if i in d:
                out.append(d[i])
        return out

    def _wk(self, sr):
        if not isinstance(sr, dict):
            return None
        return sr.get("latestWkid") or sr.get("wkid")

    def _leer_wkid(self, layer_info, service_info):
        return (
            self._wk(layer_info.get("extent", {}).get("spatialReference", {}))
            or self._wk(layer_info.get("spatialReference", {}))
            or self._wk(service_info.get("spatialReference", {}))
            or self._wk(service_info.get("fullExtent", {}).get("spatialReference", {}))
            or 4326
        )

    def _leer_bbox(self, layer_info, service_info, wkid_in):
        if self.bbox_fijo:
            return {
                "xmin": float(self.bbox_fijo[0]),
                "ymin": float(self.bbox_fijo[1]),
                "xmax": float(self.bbox_fijo[2]),
                "ymax": float(self.bbox_fijo[3]),
                "spatialReference": {"wkid": int(wkid_in)}
            }
        env = layer_info.get("extent") or service_info.get("fullExtent")
        if not env:
            return None
        if "spatialReference" not in env:
            env["spatialReference"] = {"wkid": int(wkid_in)}
        return env

    def _envelope_params(self, env):
        if not env:
            return {}
        xmin = env.get("xmin")
        ymin = env.get("ymin")
        xmax = env.get("xmax")
        ymax = env.get("ymax")
        if xmin is None or ymin is None or xmax is None or ymax is None:
            return {}
        sr = env.get("spatialReference") or {}
        wkid = sr.get("latestWkid") or sr.get("wkid")
        p = {
            "geometry": f"{xmin},{ymin},{xmax},{ymax}",
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects"
        }
        if wkid:
            p["inSR"] = int(wkid)
        return p

    def _leer_oid_field(self, layer_info):
        for f in layer_info.get("fields", []) or []:
            if f.get("type") == "esriFieldTypeOID":
                n = f.get("name")
                if n:
                    return n
        return layer_info.get("objectIdField") or "OBJECTID"

    def _max_record_count(self, layer_info, service_info):
        m = layer_info.get("maxRecordCount")
        if isinstance(m, int) and m > 0:
            return m
        m2 = service_info.get("maxRecordCount")
        if isinstance(m2, int) and m2 > 0:
            return m2
        return 1000

    # =========================================================
    # OIDs
    # =========================================================
    def _obtener_oids(self, lid, env):
        params = {"f": "json", "where": "1=1", "returnIdsOnly": "true"}
        params.update(self._envelope_params(env))
        d = self._request_json(f"{self.url_servicio}/{lid}/query", params)
        if d and isinstance(d.get("objectIds"), list):
            try:
                return sorted(set(d["objectIds"]))
            except Exception:
                return d["objectIds"]
        return []

    # =========================================================
    # Query por OIDs con control de exceededTransferLimit
    # =========================================================
    def _query_oids_chunk(self, lid, env, wkid_out, oids_chunk):
        params = {
            "f": "json",
            "where": "1=1",
            "objectIds": ",".join(map(str, oids_chunk)),
            "outFields": "*",
            "returnGeometry": "true"
        }
        params.update(self._envelope_params(env))
        if wkid_out:
            params["outSR"] = int(wkid_out)
        return self._request_json(f"{self.url_servicio}/{lid}/query", params)

    # =========================================================
    # ESRI -> SHAPELY (COMPLETO)
    # =========================================================
    def _ring_area(self, ring):
        if not ring or len(ring) < 4:
            return 0.0
        a = 0.0
        for i in range(len(ring) - 1):
            x1, y1 = ring[i]
            x2, y2 = ring[i + 1]
            a += (x1 * y2 - x2 * y1)
        return 0.5 * a

    def _is_clockwise(self, ring):
        return self._ring_area(ring) < 0.0

    def _group_rings_esri(self, rings):
        outers = []
        holes_map = []

        for r in rings:
            if not r or len(r) < 4:
                continue
            if self._is_clockwise(r):
                outers.append(r)
                holes_map.append([])
            else:
                if not holes_map:
                    outers.append(r)
                    holes_map.append([])
                else:
                    holes_map[-1].append(r)

        polys = []
        for outer, holes in zip(outers, holes_map):
            try:
                polys.append(Polygon(outer, holes))
            except Exception:
                pass

        return polys

    def _esri_geom_to_shapely(self, g, geometry_type):
        if not g:
            return None

        if geometry_type == "esriGeometryPoint":
            x = g.get("x")
            y = g.get("y")
            if x is None or y is None:
                return None
            return Point(float(x), float(y))

        if geometry_type == "esriGeometryMultipoint":
            pts = g.get("points") or []
            if not pts:
                return None
            return MultiPoint([(float(x), float(y)) for x, y in pts])

        if geometry_type == "esriGeometryPolyline":
            paths = g.get("paths") or []
            if not paths:
                return None
            lines = []
            for p in paths:
                if p and len(p) >= 2:
                    try:
                        lines.append(LineString([(float(x), float(y)) for x, y in p]))
                    except Exception:
                        pass
            if not lines:
                return None
            if len(lines) == 1:
                return lines[0]
            return MultiLineString(lines)

        if geometry_type == "esriGeometryPolygon":
            rings = g.get("rings") or []
            if not rings:
                return None
            polys = self._group_rings_esri(rings)
            if not polys:
                return None
            if len(polys) == 1:
                return polys[0]
            return MultiPolygon(polys)

        return None

    # =========================================================
    # Shapefile: campos a 10 chars (evita pérdidas raras)
    # =========================================================
    def _sanitize_fields_for_shp(self, gdf):
        cols = [c for c in gdf.columns if c != "geometry"]
        used = set()
        rename = {}

        for c in cols:
            base = re.sub(r"[^A-Za-z0-9_]+", "_", str(c)).upper()
            base = base[:10] if base else "F"
            name = base
            k = 1
            while name in used:
                suf = str(k)
                name = (base[:(10 - len(suf))] + suf)[:10]
                k += 1
            used.add(name)
            rename[c] = name

        return gdf.rename(columns=rename)

    # =========================================================
    # Exportar
    # =========================================================
    def _exportar(self, lid, lname, wkid_out, features_esri, geometry_type):
        rows = []
        for ft in features_esri:
            attrs = ft.get("attributes") or {}
            geom = self._esri_geom_to_shapely(ft.get("geometry"), geometry_type)
            attrs["geometry"] = geom
            rows.append(attrs)

        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=f"EPSG:{wkid_out}")
        gdf = gdf[~gdf.geometry.isna()].copy()

        base = f"{lid}_{self._limpiar_nombre(lname)}"
        if self.formato_salida == "geojson":
            out = os.path.join(self.carpeta_salida, f"{base}.geojson")
            gdf.to_file(out, driver="GeoJSON", encoding="utf-8")
            return out, int(len(gdf))

        out = os.path.join(self.carpeta_salida, f"{base}.shp")
        gdf = self._sanitize_fields_for_shp(gdf)
        gdf.to_file(out, driver="ESRI Shapefile", encoding="utf-8")
        return out, int(len(gdf))

    # =========================================================
    # Proceso por capa
    # =========================================================
    def _procesar_capa(self, service_info, capa):
        lid = capa.get("id")
        lname = capa.get("name", f"layer_{lid}")
        if lid is None:
            return None

        print(f"\nDescargando capa {lid}: {lname}")

        layer_info = self._request_json(f"{self.url_servicio}/{lid}", {"f": "json"})
        if not layer_info:
            print("Capa no disponible")
            return None

        wkid_in = self._leer_wkid(layer_info, service_info) or 4326
        env = self._leer_bbox(layer_info, service_info, wkid_in)
        oid_field = self._leer_oid_field(layer_info)
        max_record = self._max_record_count(layer_info, service_info)
        geometry_type = layer_info.get("geometryType") or "esriGeometryPoint"

        # OIDs completos
        oids = self._obtener_oids(lid, env)
        if not oids:
            print("Sin datos (OIDs vacíos)")
            return None

        total_oids = len(oids)
        print(f"OIDs: {total_oids} | maxRecordCount: {max_record}")

        # chunk efectivo: no pida más que maxRecordCount
        chunk = min(max_record, max(self.min_chunk, self.chunk_inicial))
        chunk = max(self.min_chunk, int(chunk))

        features_all = []
        missing = set(oids)

        pbar = None
        if self.usar_tqdm and tqdm is not None:
            pbar = tqdm(total=total_oids, desc="OIDs", ncols=110, leave=False)

        i = 0
        while i < total_oids:
            lote = oids[i:i + chunk]

            # intento con chunk actual
            d = self._query_oids_chunk(lid, env, self.wkid_salida, lote)

            # si server se queja / devuelve vacío, baja chunk y reintenta
            if not d or not isinstance(d, dict):
                chunk = max(self.min_chunk, chunk // 2)
                if chunk == self.min_chunk and (not d):
                    i += len(lote)
                    if pbar:
                        pbar.update(len(lote))
                    continue
                continue

            feats = d.get("features") or []
            exceeded = bool(d.get("exceededTransferLimit", False))

            # si exceeded, baja chunk y reintenta el mismo segmento
            if exceeded and chunk > self.min_chunk:
                chunk = max(self.min_chunk, chunk // 2)
                continue

            # acumula
            if feats:
                features_all.extend(feats)

                for ft in feats:
                    a = ft.get("attributes") or {}
                    oid_val = a.get(oid_field)
                    if oid_val is not None and oid_val in missing:
                        missing.discard(oid_val)

            i += len(lote)
            if pbar:
                pbar.update(len(lote))

            if self.sleep_s:
                time.sleep(self.sleep_s)

        if pbar:
            pbar.close()

        # reintento final de faltantes (por si hubo lotes incompletos)
        if missing:
            miss = sorted(missing)
            print(f"Reintento faltantes: {len(miss)}")
            chunk2 = max(self.min_chunk, min(200, self.min_chunk))
            j = 0
            while j < len(miss):
                lote2 = miss[j:j + chunk2]
                d2 = self._query_oids_chunk(lid, env, self.wkid_salida, lote2)
                feats2 = (d2.get("features") if d2 else []) or []
                if feats2:
                    features_all.extend(feats2)
                    for ft in feats2:
                        a = ft.get("attributes") or {}
                        oid_val = a.get(oid_field)
                        if oid_val is not None and oid_val in missing:
                            missing.discard(oid_val)
                j += len(lote2)
                if self.sleep_s:
                    time.sleep(self.sleep_s)

        if not features_all:
            print("Sin features descargadas")
            return None

        out, n = self._exportar(lid, lname, self.wkid_salida, features_all, geometry_type)
        print(f"OK: {out} ({n} features) | Faltantes final: {len(missing)}")

        return {
            "layer_id": int(lid),
            "layer_name": lname,
            "geometry_type": geometry_type,
            "oids_total": int(total_oids),
            "downloaded_features": int(n),
            "missing_oids_final": int(len(missing)),
            "output": out
        }




# ============================================================
# EJECUCIÓN
# ============================================================
if __name__ == "__main__":
    URL = "https://mapas2.igac.gov.co/server/rest/services/carto/carto100000colombia2019/MapServer"
    SALIDA = r"D:/Downloads/juanDavid_Sukubun/out2_kr"
    TARGET_IDS = [20, 25, 26, 35, 36, 37, 39, 41, 42, 44, 47]
    BBOX = None

    CHUNK_INICIAL = 2000
    MIN_CHUNK = 200
    TIMEOUT = 30
    REINTENTOS = 6

    USAR_TQDM = True
    USAR_PARALELO = False
    MAX_WORKERS = 1
    MAX_DEPTH = 0
    SLEEP = 0.0
    UMBRAL_PARALELO = 0

    FORMATO_SALIDA = "shp"
    WKID_SALIDA = 4326


    Downloadserver_REST(URL,
                        SALIDA,
                        TARGET_IDS,
                        BBOX,
                        CHUNK_INICIAL,
                        MIN_CHUNK,
                        TIMEOUT,
                        REINTENTOS,
                        USAR_TQDM,
                        USAR_PARALELO,
                        MAX_WORKERS,
                        MAX_DEPTH,
                        SLEEP,
                        UMBRAL_PARALELO,
                        FORMATO_SALIDA,
                        WKID_SALIDA)
