# -*- coding: utf-8 -*-
"""
MOSAICO NACIONAL S2 / HANSEN / SRTM
Adaptado para Django (MEDIA_ROOT/EXPORTS)

Autor: refactor seguro para backend Django
"""

from __future__ import annotations

import re
from pathlib import Path
from django.conf import settings
from typing import List, Tuple, Dict, Optional

import numpy as np
import rasterio
from rasterio.warp import reproject, calculate_default_transform, Resampling
from rasterio.transform import from_bounds
from tqdm import tqdm

# =========================
# CONSTANTES
# =========================
VALID_TIF_EXT = {".tif", ".tiff"}
INDEX_NODATA = -9999.0

S2_BAND_NAMES = [
    "B01","B02","B03","B04","B05","B06","B07",
    "B08","B08A","B09","B11","B12","SCL"
]
HANSEN_BAND_NAMES = ["treecover2000", "loss", "gain", "lossyear"]

GTIFF = dict(
    driver="GTiff",
    compress="LZW",
    tiled=True,
    blockxsize=512,
    blockysize=512,
    BIGTIFF="YES",
)

# =========================
# PATHS
# =========================
def build_paths(exports_dir: Path) -> Dict[str, Path]:
    out_root = Path(settings.MEDIA_ROOT) / "MOSAICS"
    return {
        "OUT_ROOT": out_root,
        "OUT_S2": out_root / "S2",
        "OUT_HANSEN": out_root / "HANSEN",
        "OUT_SRTM": out_root / "SRTM",
    }

# =========================
# UTILS
# =========================
def _latest_subfolder_by_regex(base: Path, pattern: str) -> Path:
    rx = re.compile(pattern)
    cands = [p for p in base.iterdir() if p.is_dir() and rx.match(p.name)]
    if not cands:
        raise RuntimeError(f"No hay carpetas válidas en {base}")
    cands.sort(key=lambda p: p.name)
    return cands[-1]

def find_latest_s2_folder(exports_dir: Path) -> Path:
    return _latest_subfolder_by_regex(
        exports_dir / "s2" / "2025Q4", r"\d{8}_\d{4}"
    )

def find_latest_hansen_folder(exports_dir: Path) -> Path:
    return _latest_subfolder_by_regex(
        exports_dir / "hansen_gfc_v1_12", r"HANSEN_\d{8}_\d{4}"
    )

def find_latest_srtm_folder(exports_dir: Path) -> Path:
    return _latest_subfolder_by_regex(
        exports_dir / "SRTM", r"SRTM_\d{8}_\d{4}"
    )

def list_tifs_recursive(folder: Path) -> List[Path]:
    return sorted(
        [p for p in folder.rglob("*") if p.suffix.lower() in VALID_TIF_EXT]
    )

def validate_all_same_bandcount(paths: List[Path], expected: int, label: str):
    if not paths:
        raise RuntimeError(f"[{label}] No hay tiles")
    with rasterio.open(paths[0]) as ds:
        if ds.count != expected:
            raise RuntimeError(f"[{label}] Bandas incorrectas")

# =========================
# MOSAICO CORE (idéntico a tu lógica)
# =========================
def collect_arrays_from_tiles(paths, band, desc=None):
    arrays, transforms, crs_list, res_list = [], [], [], []

    for p in tqdm(paths, desc=desc or f"Leyendo banda {band}", leave=False):
        with rasterio.open(p) as ds:

            a = ds.read(band)

            # FORZAR 2D SIEMPRE
            if a.ndim == 3:
                a = a[0]

            if a.ndim != 2:
                raise RuntimeError(
                    f"Raster inválido {p.name}: shape={a.shape}"
                )

            arrays.append(a.astype("float32"))
            transforms.append(ds.transform)
            crs_list.append(ds.crs)
            res_list.append(ds.res)

    return arrays, transforms, crs_list, res_list


def compute_ref_grid(transforms, crs_list, res_list, arrays):
    crs = crs_list[0]
    xres = min(abs(r[0]) for r in res_list)
    yres = min(abs(r[1]) for r in res_list)

    bounds = []
    for a, T in zip(arrays, transforms):

        # VALIDACIÓN DURA
        if a.ndim != 2:
            raise RuntimeError(
                f"Array inválido en compute_ref_grid: shape={a.shape}"
            )

        h, w = a.shape

        bounds.append(
            rasterio.transform.array_bounds(h, w, T)
        )

    minx = min(b[0] for b in bounds)
    miny = min(b[1] for b in bounds)
    maxx = max(b[2] for b in bounds)
    maxy = max(b[3] for b in bounds)

    width = int((maxx - minx) / xres)
    height = int((maxy - miny) / yres)

    return crs, from_bounds(minx, miny, maxx, maxy, width, height), width, height

def mosaic_reproject_first(arrays, transforms, crs_list, dst_grid, desc=None):
    crs, T, w, h = dst_grid
    dst = np.zeros((h, w), dtype="float32")
    mask = np.ones((h, w), dtype=bool)

    iterable = zip(arrays, transforms, crs_list)
    iterable = tqdm(iterable, total=len(arrays), desc=desc, leave=False)

    for a, srcT, srcCRS in iterable:

        if a.ndim != 2:
            raise RuntimeError(
                f"Array inválido antes de reproject: shape={a.shape}"
            )

        temp = np.zeros((h, w), dtype="float32")

        reproject(
            source=a,
            destination=temp,
            src_transform=srcT,
            src_crs=srcCRS,
            dst_transform=T,
            dst_crs=crs,
            resampling=Resampling.nearest
        )

        take = (temp != 0) & mask
        dst[take] = temp[take]
        mask[take] = False

    return np.ma.array(dst, mask=mask), T, crs


def save_tif(path: Path, arr, T, crs, dtype="float32"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path, "w",
        height=arr.shape[0], width=arr.shape[1],
        count=1, dtype=dtype,
        crs=crs, transform=T,
        nodata=0, **GTIFF
    ) as dst:
        dst.write(arr.filled(0), 1)

# =========================
# PRODUCTOS
# =========================
def mosaic_s2(folder: Path, out_dir: Path):
    tiles = list_tifs_recursive(folder)
    validate_all_same_bandcount(tiles, len(S2_BAND_NAMES), "S2")

    # Grid base (una sola vez)
    arrays, transforms, crs_list, res = collect_arrays_from_tiles(
        tiles, 2, desc="S2 → preparando grid"
    )
    grid = compute_ref_grid(transforms, crs_list, res, arrays)

    for i, b in tqdm(
        list(enumerate(S2_BAND_NAMES, start=1)),
        desc="Mosaico S2 (bandas)"
    ):
        arrays, transforms, crs_list, _ = collect_arrays_from_tiles(
            tiles, i, desc=f"S2 leyendo {b}"
        )

        mosaic, T, crs = mosaic_reproject_first(
            arrays, transforms, crs_list, grid,
            desc=f"S2 reproyectando {b}"
        )

        save_tif(out_dir / "BANDS" / f"{b}.tif", mosaic, T, crs, "uint16")


def mosaic_hansen(folder: Path, out_dir: Path):
    tiles = list_tifs_recursive(folder)
    validate_all_same_bandcount(tiles, 4, "HANSEN")

    arrays, transforms, crs_list, res = collect_arrays_from_tiles(
        tiles, 1, desc="HANSEN → preparando grid"
    )
    grid = compute_ref_grid(transforms, crs_list, res, arrays)

    for i, b in tqdm(
        list(enumerate(HANSEN_BAND_NAMES, start=1)),
        desc="Mosaico HANSEN (bandas)"
    ):
        arrays, transforms, crs_list, _ = collect_arrays_from_tiles(
            tiles, i, desc=f"HANSEN leyendo {b}"
        )

        mosaic, T, crs = mosaic_reproject_first(
            arrays, transforms, crs_list, grid,
            desc=f"HANSEN reproyectando {b}"
        )

        save_tif(out_dir / "BANDS" / f"{b}.tif", mosaic, T, crs, "uint8")


def mosaic_srtm(folder: Path, out_dir: Path):
    tiles = list_tifs_recursive(folder)

    arrays, transforms, crs_list, res = collect_arrays_from_tiles(
        tiles, 1, desc="SRTM leyendo tiles"
    )
    grid = compute_ref_grid(transforms, crs_list, res, arrays)

    mosaic, T, crs = mosaic_reproject_first(
        arrays, transforms, crs_list, grid,
        desc="SRTM reproyectando"
    )

    save_tif(out_dir / "mosaic_SRTM.tif", mosaic, T, crs, "int16")


# =========================
# ORQUESTADOR DJANGO
# =========================
def full_mosaic_nacional(
    exports_dir: Path,
    run_s2=True,
    run_hansen=True,
    run_srtm=True,
):
    paths = build_paths(exports_dir)
    paths["OUT_ROOT"].mkdir(parents=True, exist_ok=True)

    outputs = {}

    if run_s2:
        mosaic_s2(find_latest_s2_folder(exports_dir), paths["OUT_S2"])
        outputs["s2"] = str(paths["OUT_S2"])

    if run_hansen:
        mosaic_hansen(find_latest_hansen_folder(exports_dir), paths["OUT_HANSEN"])
        outputs["hansen"] = str(paths["OUT_HANSEN"])

    if run_srtm:
        mosaic_srtm(find_latest_srtm_folder(exports_dir), paths["OUT_SRTM"])
        outputs["srtm"] = str(paths["OUT_SRTM"])

    return outputs
