# INSUMOS_VULNERABILIDAD_TILES_V9_SPLIT_RETRY.py
# -*- coding: utf-8 -*-

import time
import datetime as dt
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import ee
import geemap
import geopandas as gpd
from shapely.geometry import box
from shapely.ops import unary_union

# =========================
# CONFIG
# =========================
ROOT          = Path(__file__).resolve().parent
MASK_SHP      = ROOT / "mask200_Dissolve.shp"           # máscara en WGS84 recomendado

ARQ_DIR       = ROOT / "ARQ_TESELAS"        # salidas shapefile
ARQ_DIR.mkdir(parents=True, exist_ok=True)

# Campo(s) candidatos para partir por zonas (si existen)
PREFERRED_SPLIT_FIELDS = ["DPTO", "DEPARTAMEN", "NOMBRE_DPT", "NOM_DEPTO", "DEPARTAMENTO"]

# Si NO hay campo categórico válido, se parte por una grilla de regiones grandes:
REGION_SIZE_KM = 300  # ~300 km por lado (ajústalo si quieres más/menos zonas)

# Export (GCS)
SA_EMAIL   = "geoinformatica-442522@geoinformatica-442522.iam.gserviceaccount.com"
SA_KEY     = "geoinformatica-442522-261aede7104e.json"
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
MAX_CONCURRENT = 3       # tasks GEE simultáneas
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

# =========================
# INIT
# =========================
def ee_init_with_service_account():
    creds = ee.ServiceAccountCredentials(SA_EMAIL, SA_KEY)
    try:
        ee.Initialize(credentials=creds,
                      opt_url="https://earthengine.googleapis.com",
                      project=PROJECT_ID)
    except TypeError:
        ee.Initialize(creds)
    if hasattr(ee.data, "setCloudProject"):
        ee.data.setCloudProject(PROJECT_ID)

def date_range():
    if not DYNAMIC_DATES:
        return DATE_START_FIX, DATE_END_FIX
    today = dt.date.today()
    start = today - dt.timedelta(days=365)
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

# =========================
# GEOM utils
# =========================
def km_to_deg(km: float) -> float:
    return km / 111.0

def add_overlap_wgs84(bx, overlap_km: int):
    """Aplica buffer en metros a un box WGS84 usando reproyección 3857."""
    if overlap_km <= 0:
        return bx
    gtmp = gpd.GeoSeries([bx], crs="EPSG:4326").to_crs(3857)
    gtmp = gtmp.buffer(overlap_km * 1000.0)
    return gtmp.to_crs(4326).iloc[0]

# =========================
# MASK / ZONAS
# =========================
def load_mask_gdf() -> gpd.GeoDataFrame:
    if not MASK_SHP.exists():
        raise FileNotFoundError(f"No se encontró la máscara: {MASK_SHP}")
    gdf = gpd.read_file(MASK_SHP)
    gdf = gdf if gdf.crs is not None else gdf.set_crs(epsg=4326)
    gdf = gdf.to_crs(epsg=4326)
    return gdf

def pick_split_field(gdf: gpd.GeoDataFrame) -> Optional[str]:
    # Prioriza campos conocidos
    for c in PREFERRED_SPLIT_FIELDS:
        if c in gdf.columns:
            nun = gdf[c].nunique(dropna=True)
            if 1 < nun <= 200:
                return c
    # Heurística genérica
    for c in gdf.columns:
        if c.lower() == "geometry":
            continue
        nun = gdf[c].nunique(dropna=True)
        if 1 < nun <= 200 and (gdf[c].dtype == "object" or str(gdf[c].dtype).startswith(("category","string"))):
            return c
    return None

def build_regions_from_field(gdf: gpd.GeoDataFrame, field: str) -> List[Dict]:
    zones = []
    for val, sub in gdf.groupby(field, dropna=True):
        sub = sub.explode(index_parts=False, ignore_index=True)
        sub['__one__'] = 1
        dis = sub.dissolve(by="__one__", as_index=False)
        union = unary_union(dis.geometry)
        zones.append({"name": str(val), "gdf": dis, "union": union})
    return zones

def build_regions_by_grid(gdf: gpd.GeoDataFrame, size_km: int) -> List[Dict]:
    union = unary_union(gdf.geometry)
    xmin, ymin, xmax, ymax = gpd.GeoSeries([union]).total_bounds
    step = km_to_deg(size_km)
    zones = []
    y = ymin
    idx = 1
    while y < ymax:
        x = xmin
        while x < xmax:
            x2, y2 = min(x+step, xmax), min(y+step, ymax)
            gx = add_overlap_wgs84(box(x, y, x2, y2), 0)
            inter = gx.intersection(union)
            if not inter.is_empty:
                zones.append({
                    "name": f"REG_{idx:03d}",
                    "gdf": gpd.GeoDataFrame(geometry=[inter], crs="EPSG:4326"),
                    "union": inter
                })
                idx += 1
            x = x2
        y = y2
    return zones

def gdf_to_ee_aoi(gdf_zone: gpd.GeoDataFrame) -> ee.Geometry:
    fc_ee = geemap.geopandas_to_ee(gdf_zone)
    return fc_ee.geometry().dissolve().simplify(500)

# =========================
# S2 mosaic
# =========================
BANDS_S2_ALL = ["B1","B2","B3","B4","B5","B6","B7","B8","B8A","B9","B11","B12","SCL"]

def mask_s2_clouds(img: ee.Image) -> ee.Image:
    scl = img.select("SCL")
    clean = (scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10)).And(scl.neq(11)))
    return img.updateMask(clean)

def build_s2_mosaic(aoi: ee.Geometry, start: str, end: str) -> ee.Image:
    col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
           .filterBounds(aoi)
           .filterDate(start, end)
           .map(mask_s2_clouds))
    med = col.median().clip(aoi)
    avail  = med.bandNames()
    wanted = ee.List(BANDS_S2_ALL)
    wanted_keep_or_none = wanted.map(lambda b: ee.Algorithms.If(avail.contains(b), b, None))
    wanted_clean = ee.List(wanted_keep_or_none).removeAll(ee.List([None]))
    return med.select(wanted_clean)

def ensure_default_projection(img: ee.Image, crs: str, scale_m: int) -> ee.Image:
    proj = ee.Projection(crs).atScale(scale_m)
    return img.reproject(crs=crs, scale=scale_m).setDefaultProjection(proj)

# =========================
# Fishnet por zona
# =========================
def tiles_from_zone(union_geom, tile_km: int, overlap_km: int) -> List[tuple]:
    """
    Devuelve lista de (tile_id, xmin, ymin, xmax, ymax) solo donde intersecta la zona.
    """
    xmin, ymin, xmax, ymax = gpd.GeoSeries([union_geom]).total_bounds
    step_deg = km_to_deg(tile_km)
    out: List[tuple] = []
    y = ymin
    while y < ymax:
        x = xmin
        while x < xmax:
            x2 = min(x + step_deg, xmax)
            y2 = min(y + step_deg, ymax)
            tile_geom = add_overlap_wgs84(box(x, y, x2, y2), overlap_km)
            if tile_geom.intersects(union_geom):
                tid = int(round(x * 1e6 + y * 1e3))
                out.append((tid, x, y, x2, y2))
            x = x2
        y = y2
    out.sort(key=lambda t: t[0])
    return out

def write_tiles_shp(tiles: List[tuple], out_path: Path, inter_union):
    rows = []
    for tid, x1, y1, x2, y2 in tiles:
        rows.append({
            "tile_id": tid,
            "xmin": x1, "ymin": y1, "xmax": x2, "ymax": y2,
            "geometry": box(x1, y1, x2, y2).intersection(inter_union)
        })
    if not rows:
        return False
    gdf_tiles = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf_tiles.to_file(out_path)
    return True

# =========================
# Export MGMT
# =========================
def current_running_tasks() -> int:
    return sum(1 for t in ee.batch.Task.list() if t.config and t.active())

def wait_for_slot(max_concurrent: int):
    while current_running_tasks() >= max_concurrent:
        time.sleep(1.0)

def start_export_task(img: ee.Image, aoi: ee.Geometry,
                      zone_name: str,
                      tile_id: int, xmin: float, ymin: float, xmax: float, ymax: float,
                      basename: str, scale_m: int):
    """Lanza el export a GCS. Lanza excepción si falla al crear la tarea."""
    region_bbox = [[xmin, ymin],[xmin, ymax],[xmax, ymax],[xmax, ymin],[xmin, ymin]]
    masked = img.updateMask(ee.Image.constant(1).clip(aoi)).clip(aoi)
    safe_zone = "".join(c if c.isalnum() else "_" for c in zone_name)[:40]

    cfg = {
        "image": masked,
        "description": f"S2_{basename}_{safe_zone}_tile{tile_id}",
        "bucket": GCS_BUCKET,
        "fileNamePrefix": f"{GCS_PREFIX}/{basename}/{safe_zone}/tile_{tile_id}",
        "region": region_bbox,
        "scale": scale_m,
        "crs": CRS_EXPORT,
        "maxPixels": 1e13,
        "fileFormat": "GeoTIFF"
    }
    if EXPORT_COG:
        cfg["formatOptions"] = {"cloudOptimized": True}

    try:
        ee.batch.Export.image.toCloudStorage(**cfg).start()
    except Exception as e:
        if "Unknown configuration options: {'formatOptions'" in str(e):
            cfg.pop("formatOptions", None)
            ee.batch.Export.image.toCloudStorage(**cfg).start()
        else:
            raise

# =========================
# Subdivisión recursiva (2x4 = 8)
# =========================
def split_bbox_2x4(xmin: float, ymin: float, xmax: float, ymax: float) -> List[Tuple[float,float,float,float]]:
    """Divide el bbox en 8 sub-bboxes (2 columnas x 4 filas)."""
    xm = (xmin + xmax) / 2.0
    y1 = ymin + (ymax - ymin) * 0.25
    y2 = ymin + (ymax - ymin) * 0.50
    y3 = ymin + (ymax - ymin) * 0.75
    parts = [
        (xmin, ymin, xm,   y1),
        (xm,   ymin, xmax, y1),
        (xmin, y1,   xm,   y2),
        (xm,   y1,   xmax, y2),
        (xmin, y2,   xm,   y3),
        (xm,   y2,   xmax, y3),
        (xmin, y3,   xm,   ymax),
        (xm,   y3,   xmax, ymax),
    ]
    return parts

def bbox_too_small(xmin: float, ymin: float, xmax: float, ymax: float) -> bool:
    """Evita subdividir por debajo de MIN_TILE_KM aprox (en grados)."""
    min_deg = km_to_deg(MIN_TILE_KM)
    return (xmax - xmin) < min_deg or (ymax - ymin) < min_deg

# =========================
# MAIN
# =========================
def main():
    ee_init_with_service_account()
    start, end = date_range()
    print(f"Rango de fechas S2: {start} → {end}")

    gdf = load_mask_gdf()

    # Zonas
    split_field = pick_split_field(gdf)
    if split_field:
        zones = build_regions_from_field(gdf, split_field)
        print(f"[INFO] Particionando por campo '{split_field}' en {len(zones)} zonas.")
    else:
        zones = build_regions_by_grid(gdf, REGION_SIZE_KM)
        print(f"[INFO] No se encontró campo categórico; usando grilla de {len(zones)} regiones ~{REGION_SIZE_KM} km.")

    if LIMIT_ZONES:
        zones = zones[:LIMIT_ZONES]
        print(f"[INFO] LIMIT_ZONES activo: procesando solo {LIMIT_ZONES} zonas.")

    basename = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M")

    # Para shapefile global con splits
    global_rows: List[Dict] = []

    total_submitted = 0
    total_processed = 0

    for zi, z in enumerate(zones, 1):
        zname  = z["name"]
        zunion = z["union"]
        zgdf   = z["gdf"]

        print(f"\n=== Zona {zi}/{len(zones)}: {zname} ===")
        aoi_z = gdf_to_ee_aoi(zgdf)
        s2    = ensure_default_projection(build_s2_mosaic(aoi_z, start, end), CRS_EXPORT, SCALE_EXPORT_M)

        tiles = tiles_from_zone(zunion, TILE_SIZE_KM, OVERLAP_KM)
        print(f"[INFO] Teselas en zona '{zname}': {len(tiles)}")

        # Shapefile por zona (opcional)
        if WRITE_TILES_SHP_PER_ZONE and tiles:
            out_shp = ARQ_DIR / f"Tiles_{zname}.shp"
            if write_tiles_shp(tiles, out_shp, zunion):
                print(f"[OK] Tiles zona '{zname}' en: {out_shp}")

        # Cola por zona
        submitted_zone = 0
        processed_zone = 0

        # --- pila para procesar subdivisiones ---
        # cada item: (tile_id, xmin, ymin, xmax, ymax, level, parent_id)
        stack: List[Tuple[int,float,float,float,float,int,Optional[int]]] = [
            (tid, x1, y1, x2, y2, 0, None) for tid, x1, y1, x2, y2 in tiles
        ]

        while stack:
            tid, xmin, ymin, xmax, ymax, level, parent = stack.pop(0)
            processed_zone += 1
            total_processed += 1

            # Registrar intento en la geometría global (estado por ahora "PENDING")
            global_rows.append({
                "zone": zname, "tile_id": tid, "parent_id": parent if parent is not None else -1,
                "level": level, "status": "PENDING",
                "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
                "geometry": box(xmin, ymin, xmax, ymax).intersection(zunion)
            })

            # Límite por zona (dry run)
            if DRY_RUN_TILES_N is not None and submitted_zone >= DRY_RUN_TILES_N:
                print(f"[STOP] DRY_RUN_TILES_N={DRY_RUN_TILES_N} alcanzado en zona '{zname}'.")
                # Marca como SKIPPED el último registro añadido
                global_rows[-1]["status"] = "SKIPPED"
                continue

            # Control de concurrencia
            wait_for_slot(MAX_CONCURRENT)

            # Intento de export
            try:
                start_export_task(s2, aoi_z, zname, tid, xmin, ymin, xmax, ymax, basename, SCALE_EXPORT_M)
                submitted_zone += 1
                total_submitted += 1
                global_rows[-1]["status"] = "ENQUEUED"
                time.sleep(PAUSE_BETWEEN)
                continue  # siguiente tile / sub-tile
            except Exception as e:
                msg = str(e)
                global_rows[-1]["status"] = "FAILED"
                global_rows[-1]["error"]  = msg
                print(f"[WARN] Falló tile {tid} (nivel {level}) en zona '{zname}': {msg}")

                # ¿Razón que amerita subdividir?
                should_split = any(key in msg for key in [
                    "Request payload size exceeds",   # payload > 10MB
                    "User memory limit exceeded",     # memoria
                    "Computation timed out"           # a veces es útil subdividir
                ])

                # ¿Aún podemos subdividir?
                if should_split and (level < MAX_SPLIT_DEPTH) and (not bbox_too_small(xmin, ymin, xmax, ymax)):
                    # subdividir en 8 (2x4) y encolar los hijos al frente de la lista
                    children = split_bbox_2x4(xmin, ymin, xmax, ymax)
                    print(f"[INFO] Subdividiendo tile {tid} -> {len(children)} subtiles (nivel {level+1}).")
                    # Generar IDs hijos determinísticos
                    # p. ej. hijo k => tid*10 + (k+1) para mantener unicidad
                    for k, (cx1, cy1, cx2, cy2) in enumerate(children):
                        child_id = int(tid) * 10 + (k + 1)
                        stack.insert(0, (child_id, cx1, cy1, cx2, cy2, level + 1, tid))
                    # (No contamos submitted aquí; solo cuando logre encolar)
                else:
                    # No subdividimos; lo dejamos fallado.
                    pass

        print(f"[RES] Zona '{zname}': evaluadas={processed_zone}, encoladas={submitted_zone}")

    # Shapefile global con TODAS las teselas/subteselas
    if WRITE_TILES_SHP_GLOBAL and global_rows:
        gdf_all = gpd.GeoDataFrame(global_rows, geometry="geometry", crs="EPSG:4326")
        out_all = ARQ_DIR / "Tiles_ALL_with_splits.shp"
        gdf_all.to_file(out_all)
        print(f"[OK] Tiles_ALL_with_splits.shp generado en: {out_all}")
    else:
        print("[INFO] No hay registros exportados para el shapefile final.")

    print(f"\n[OK] TOTAL: evaluadas={total_processed} | encoladas={total_submitted}")
    print("Monitorea tareas en: https://code.earthengine.google.com/tasks")


def run_s2_export():
    main()