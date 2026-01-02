# INSUMOS_VULNERABILIDAD_TILES_V9_SPLIT_RETRY.py
# -*- coding: utf-8 -*-

import logging
import time
import datetime as dt
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from tqdm import tqdm


import ee
import geemap
import geopandas as gpd
from shapely.geometry import box
from shapely.ops import unary_union
from django.conf import settings

logger = logging.getLogger(__name__)

# =========================
# CONFIG
# =========================

BASE_DIR = Path(settings.BASE_DIR) / "static" / "backend" / "geodata"
MASK_SHP = BASE_DIR / "mask200_Dissolve" / "mask200_Dissolve.shp"

# Leer shapefile con GeoPandas 
# gdf = gpd.read_file(MASK_SHP) 
# print(gdf.head())

ARQ_DIR = Path(settings.MEDIA_ROOT) / "ARQ_TESELAS"
ARQ_DIR.mkdir(parents=True, exist_ok=True)

# Campo(s) candidatos para partir por zonas (si existen)
PREFERRED_SPLIT_FIELDS = ["DPTO", "DEPARTAMEN", "NOMBRE_DPT", "NOM_DEPTO", "DEPARTAMENTO"]

# Si NO hay campo categórico válido, se parte por una grilla de regiones grandes:
REGION_SIZE_KM = 300  # ~300 km por lado (ajústalo si quieres más/menos zonas)

# Export (GCS)
SA_EMAIL   = "geoinformatica-442522@geoinformatica-442522.iam.gserviceaccount.com"
print("iformancio corie")
print(SA_EMAIL)


SA_KEY     = settings.GS_CREDENTIALS_FILE
print("contrañe clacec")
print(SA_KEY)

PROJECT_ID = "geoinformatica-442522"

GCS_BUCKET = "invias_mapa_vulnerabilidad_faunistica"
GCS_PREFIX = "s2/2025Q4"

# Fechas (últimos 365 días por defecto)
DYNAMIC_DATES  = True
DATE_START_FIX = "2025-01-01"
DATE_END_FIX   = "2025-12-31"

# Resolución exportada
SCALE_EXPORT_M = 100
CRS_EXPORT     = "EPSG:4326"
EXPORT_COG     = True

# Tiling dentro de cada zona
TILE_SIZE_KM   = 50      # lado de la tesela inicial por zona
OVERLAP_KM     = 0       # buffer opcional del tile bbox (en km)

# Control
MAX_CONCURRENT = 4       # tasks GEE simultáneas
PAUSE_BETWEEN  = 0.2     # segundos entre task.start()

# Subdivisión recursiva
MAX_SPLIT_DEPTH = 3      # niveles máximos de subdivisión (0=sin split; 1=una vez; etc.)
MIN_TILE_KM     = 3      # no subdividir si un lado baja de ~3 km (para evitar microteselas)

# Shapefiles
WRITE_TILES_SHP_PER_ZONE = True
WRITE_TILES_SHP_GLOBAL   = True

# Dry run / limitadores
LIMIT_ZONES     = None    # p.ej. 3 para solo 3 zonas; None = sin límite
DRY_RUN_TILES_N = None    # p.ej. 20 para encolar solo N tiles por zona; None = sin límite


print(ee.data.credentials_lib)



# =========================
# INIT EE
# =========================
def ee_init_with_service_account():
    # creds = ee.ServiceAccountCredentials(SA_EMAIL, SA_KEY)
    # try:
    #     ee.Initialize(credentials=creds, project=PROJECT_ID)
    # except TypeError:
    #     ee.Initialize(creds)
    # if hasattr(ee.data, "setCloudProject"):
    #     ee.data.setCloudProject(PROJECT_ID)
    ee.Initialize(
        credentials=ee.ServiceAccountCredentials(settings.GS_EMAIL, settings.GS_CREDENTIALS_FILE),
        project=settings.GS_PROJECT_ID
    )


def date_range():
    if not DYNAMIC_DATES:
        return DATE_START_FIX, DATE_END_FIX
    today = dt.date.today()
    start = today - dt.timedelta(days=365)
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

# =========================
# GEOM
# =========================
def km_to_deg(km):
    return km / 111.0

def add_overlap_wgs84(bx, overlap_km):
    if overlap_km <= 0:
        return bx
    g = gpd.GeoSeries([bx], crs="EPSG:4326").to_crs(3857)
    g = g.buffer(overlap_km * 1000)
    return g.to_crs(4326).iloc[0]

# =========================
# MASK / ZONES
# =========================
def load_mask_gdf():
    gdf = gpd.read_file(MASK_SHP)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    return gdf.to_crs(4326)

def pick_split_field(gdf):
    for c in PREFERRED_SPLIT_FIELDS:
        if c in gdf.columns and 1 < gdf[c].nunique() <= 200:
            return c
    return None

def build_regions_from_field(gdf, field):
    zones = []
    for val, sub in gdf.groupby(field):
        u = unary_union(sub.geometry)
        zones.append({"name": str(val), "gdf": sub, "union": u})
    return zones

def build_regions_by_grid(gdf, size_km):
    union = unary_union(gdf.geometry)
    xmin, ymin, xmax, ymax = gpd.GeoSeries([union]).total_bounds
    step = km_to_deg(size_km)
    zones = []
    i = 1
    y = ymin
    while y < ymax:
        x = xmin
        while x < xmax:
            box_g = box(x, y, min(x+step, xmax), min(y+step, ymax))
            inter = box_g.intersection(union)
            if not inter.is_empty:
                zones.append({
                    "name": f"REG_{i:03d}",
                    "gdf": gpd.GeoDataFrame(geometry=[inter], crs=4326),
                    "union": inter
                })
                i += 1
            x += step
        y += step
    return zones

def gdf_to_ee_aoi(gdf):
    fc = geemap.geopandas_to_ee(gdf)
    return fc.geometry().dissolve().simplify(500)

# =========================
# S2 mosaic
# =========================
BANDS_S2_ALL = ["B1","B2","B3","B4","B5","B6","B7","B8","B8A","B9","B11","B12","SCL"]

def mask_s2_clouds(img):
    scl = img.select("SCL")
    mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    return img.updateMask(mask)

def build_s2_mosaic(aoi, start, end):
    col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
           .filterBounds(aoi)
           .filterDate(start, end)
           .map(mask_s2_clouds))
    return col.median().clip(aoi)

def ensure_default_projection(img, crs, scale):
    return img.reproject(crs=crs, scale=scale)

# =========================
# TILES
# =========================
def tiles_from_zone(union, tile_km, overlap_km):
    xmin, ymin, xmax, ymax = gpd.GeoSeries([union]).total_bounds
    step = km_to_deg(tile_km)
    tiles = []
    y = ymin
    while y < ymax:
        x = xmin
        while x < xmax:
            tile = add_overlap_wgs84(
                box(x, y, min(x+step, xmax), min(y+step, ymax)),
                overlap_km
            )
            if tile.intersects(union):
                tid = int(round(x*1e6 + y*1e3))
                tiles.append((tid, x, y, min(x+step, xmax), min(y+step, ymax)))
            x += step
        y += step
    return tiles

# =========================
# EXPORT
# =========================
def current_running_tasks():
    return sum(1 for t in ee.batch.Task.list() if t.active())

def wait_for_slot(max_c):
    while current_running_tasks() >= max_c:
        time.sleep(1)

def start_export_task(img, aoi, zname, tid, xmin, ymin, xmax, ymax, basename, scale):
    region = [[xmin,ymin],[xmin,ymax],[xmax,ymax],[xmax,ymin],[xmin,ymin]]
    safe = "".join(c if c.isalnum() else "_" for c in zname)
    cfg = dict(
        image=img.clip(aoi),
        description=f"S2_{basename}_{safe}_{tid}",
        bucket=GCS_BUCKET,
        fileNamePrefix=f"{GCS_PREFIX}/{basename}/{safe}/tile_{tid}",
        region=region,
        scale=scale,
        crs=CRS_EXPORT,
        maxPixels=1e13,
        fileFormat="GeoTIFF"
    )
    ee.batch.Export.image.toCloudStorage(**cfg).start()

# =========================
# SPLIT
# =========================
def split_bbox_2x4(xmin,ymin,xmax,ymax):
    xm = (xmin+xmax)/2
    ys = [ymin+i*(ymax-ymin)/4 for i in range(5)]
    return [(xmin,ys[i],xm,ys[i+1]) for i in range(4)] + \
           [(xm,ys[i],xmax,ys[i+1]) for i in range(4)]

def bbox_too_small(xmin,ymin,xmax,ymax):
    return (xmax-xmin) < km_to_deg(MIN_TILE_KM)

# =========================
# ENTRYPOINT
# =========================
def run_s2_export(limit_zones=None, dry_run_tiles=None):
    ee_init_with_service_account()
    start, end = date_range()

    gdf = load_mask_gdf()
    split = pick_split_field(gdf)

    zones = build_regions_from_field(gdf, split) if split else build_regions_by_grid(gdf, REGION_SIZE_KM)
    if limit_zones:
        zones = zones[:limit_zones]

    basename = dt.datetime.utcnow().strftime("%Y%m%d_%H%M")

    processed = submitted = 0

    for z in tqdm(zones, desc="Procesando zonas", unit="zona"):
        aoi = gdf_to_ee_aoi(z["gdf"])
        s2 = ensure_default_projection(build_s2_mosaic(aoi, start, end), CRS_EXPORT, SCALE_EXPORT_M)
        tiles = tiles_from_zone(z["union"], TILE_SIZE_KM, OVERLAP_KM)

        count = 0
        stack = [(t[0], *t[1:], 0) for t in tiles]

        # while stack:
        for tid,x1,y1,x2,y2,level in tqdm(stack, desc=f"Tiles {z['name']}", unit="tile", leave=False):
            # tid,x1,y1,x2,y2,level = stack.pop(0)
            processed += 1
            if dry_run_tiles and count >= dry_run_tiles:
                continue

            wait_for_slot(MAX_CONCURRENT)
            try:
                start_export_task(s2,aoi,z["name"],tid,x1,y1,x2,y2,basename,SCALE_EXPORT_M)
                submitted += 1
                count += 1
                time.sleep(PAUSE_BETWEEN)
            except Exception as e:
                if level < MAX_SPLIT_DEPTH and not bbox_too_small(x1,y1,x2,y2):
                    for k,b in enumerate(split_bbox_2x4(x1,y1,x2,y2)):
                        stack.insert(0,(tid*10+k,*b,level+1))

    return {
        "status": "ok",
        "zones": len(zones),
        "processed": processed,
        "enqueued": submitted,
        "date_range": [start, end]
    }