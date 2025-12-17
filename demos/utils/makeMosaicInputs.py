# apps/processing/services/mosaics.py
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import numpy as np
import rasterio
from rasterio.warp import reproject, calculate_default_transform, Resampling
from rasterio.transform import from_bounds
from tqdm import tqdm

from django.conf import settings

# =========================
# CONFIG
# =========================
WORKDIR = Path(settings.BASE_DIR)
EXPORTS_DIR = WORKDIR / "EXPORTS"

OUT_ROOT = EXPORTS_DIR / "MOSAICS"
OUT_S2 = OUT_ROOT / "S2"
OUT_HANSEN = OUT_ROOT / "HANSEN"
OUT_SRTM = OUT_ROOT / "SRTM"

VALID_TIF_EXT = {".tif", ".tiff"}

GTIFF = dict(
    driver="GTiff",
    compress="LZW",
    tiled=True,
    blockxsize=512,
    blockysize=512,
    BIGTIFF="YES",
)

INDEX_NODATA = -9999.0

S2_BAND_NAMES = ["B01","B02","B03","B04","B05","B06","B07","B08","B08A","B09","B11","B12","SCL"]
HANSEN_BAND_NAMES = ["treecover2000", "loss", "gain", "lossyear"]

# =========================
def _latest_subfolder_by_regex(base: Path, pattern: str, token_group: int = 1) -> Path:
    rx = re.compile(pattern)
    cands: List[Tuple[str, Path]] = []
    if not base.exists():
        raise FileNotFoundError(f"No existe: {base}")

    for p in base.iterdir():
        if p.is_dir():
            m = rx.match(p.name)
            if m:
                token = m.group(token_group)
                cands.append((token, p))

    if not cands:
        raise RuntimeError(f"No se encontraron subcarpetas válidas en {base} con patrón: {pattern}")

    cands.sort(key=lambda x: x[0])
    return cands[-1][1]


def find_latest_s2_folder(exports_dir: Path) -> Path:
    base = exports_dir / "s2" / "2025Q4"
    return _latest_subfolder_by_regex(base, r"^(\d{8}_\d{4})$")


def find_latest_hansen_folder(exports_dir: Path) -> Path:
    base = exports_dir / "hansen_gfc_v1_12"
    return _latest_subfolder_by_regex(base, r"^(HANSEN_\d{8}_\d{4})$")


def find_latest_srtm_folder(exports_dir: Path) -> Path:
    base = exports_dir / "SRTM"
    return _latest_subfolder_by_regex(base, r"^(SRTM_\d{8}_\d{4})$")


# =========================
# Utils: listar tiles (recursivo)
# =========================
def list_tifs_recursive(folder: Path) -> List[Path]:
    tifs: List[Path] = []
    for p in folder.rglob("*"):
        if p.is_file() and p.suffix.lower() in VALID_TIF_EXT:
            tifs.append(p)
    tifs.sort(key=lambda x: str(x))
    return tifs


def validate_all_same_bandcount(paths: List[Path], expected: int, label: str):
    if not paths:
        raise RuntimeError(f"[{label}] No hay tiles .tif para procesar.")
    with rasterio.open(paths[0]) as src0:
        nb = src0.count
    if nb != expected:
        raise RuntimeError(f"[{label}] El primer tile tiene {nb} bandas, se esperaban {expected}. Tile={paths[0]}")
    # chequeo por muestreo
    sample = paths[::max(1, len(paths)//10)]
    for p in sample:
        with rasterio.open(p) as s:
            if s.count != expected:
                raise RuntimeError(f"[{label}] Inconsistencia de bandas: {p} tiene {s.count}, esperado {expected}.")


# =========================
# Núcleo mosaico tipo Concatenador: grilla fija + reproyección
# =========================
def _to_2d_float_with_nan(arr) -> Optional[np.ndarray]:
    if hasattr(arr, "mask"):
        arr = arr.astype("float32")
        arr[arr.mask] = np.nan
        arr = arr.data
    arr = np.asarray(arr)
    arr = np.squeeze(arr)
    if arr.ndim != 2:
        return None
    if not np.issubdtype(arr.dtype, np.floating):
        arr = arr.astype("float32", copy=False)
    return arr


def collect_arrays_from_tiles(paths: List[Path], band_index_1based: int):
    arrays, transforms, crs_list, res_list = [], [], [], []
    for p in paths:
        try:
            with rasterio.open(p) as ds:
                if band_index_1based < 1 or band_index_1based > ds.count:
                    continue
                a = ds.read(band_index_1based, masked=True)
                a = _to_2d_float_with_nan(a)
                if a is None:
                    continue
                arrays.append(a)
                transforms.append(ds.transform)
                crs_list.append(ds.crs)
                res_list.append(ds.res)
        except Exception:
            continue
    return arrays, transforms, crs_list, res_list


def compute_ref_grid(transforms, crs_list, res_list, arrays, prefer_crs=None):
    out_crs = prefer_crs or crs_list[0]
    xres = min(abs(r[0]) for r in res_list)
    yres = min(abs(r[1]) for r in res_list)

    repro_bounds = []
    for a, T, crs in zip(arrays, transforms, crs_list):
        h, w = a.shape
        b = rasterio.transform.array_bounds(h, w, T) 
        west, south, east, north = b

        if crs != out_crs:
            dT, W, H = calculate_default_transform(crs, out_crs, w, h, west, south, east, north)
            minx = dT.c
            maxy = dT.f
            maxx = minx + W * dT.a
            miny = maxy + H * dT.e
            repro_bounds.append((min(minx, maxx), min(miny, maxy), max(minx, maxx), max(miny, maxy)))
        else:
            repro_bounds.append((west, south, east, north))

    minx = min(b[0] for b in repro_bounds)
    miny = min(b[1] for b in repro_bounds)
    maxx = max(b[2] for b in repro_bounds)
    maxy = max(b[3] for b in repro_bounds)

    out_w = int(np.ceil((maxx - minx) / xres))
    out_h = int(np.ceil((maxy - miny) / yres))
    out_T = from_bounds(minx, miny, maxx, maxy, out_w, out_h)
    return out_crs, out_T, out_w, out_h


def mosaic_reproject_first(arrays, transforms, crs_list, dst_grid, resampling=Resampling.nearest):

    out_crs, out_T, out_w, out_h = dst_grid

    dst = np.zeros((out_h, out_w), dtype="float32")
    dst_mask = np.ones((out_h, out_w), dtype=bool)  # True = vacío

    for a, srcT, srcCRS in zip(arrays, transforms, crs_list):
        temp = np.zeros_like(dst, dtype="float32")
        temp_mask = np.ones_like(dst_mask, dtype=np.uint8)

        # datos
        reproject(
            source=np.nan_to_num(a, nan=0.0),
            destination=temp,
            src_transform=srcT, src_crs=srcCRS,
            dst_transform=out_T, dst_crs=out_crs,
            resampling=resampling,
            dst_nodata=0.0
        )
        # máscara: 1 donde es NaN
        reproject(
            source=np.isnan(a).astype(np.uint8),
            destination=temp_mask,
            src_transform=srcT, src_crs=srcCRS,
            dst_transform=out_T, dst_crs=out_crs,
            resampling=Resampling.nearest,
            dst_nodata=1
        )
        temp_mask = temp_mask.astype(bool)

        take = (~temp_mask) & (dst_mask)
        dst[take] = temp[take]
        dst_mask[take] = False

    return np.ma.array(dst, mask=dst_mask), out_T, out_crs


def save_tif_singleband(path: Path, arr_masked: np.ma.MaskedArray, T, crs, dtype_out: str):

    path.parent.mkdir(parents=True, exist_ok=True)

    h, w = arr_masked.shape
    if dtype_out == "uint8":
        data = np.round(arr_masked.filled(0)).clip(0, 255).astype("uint8")
    elif dtype_out == "uint16":
        data = np.round(arr_masked.filled(0)).clip(0, 65535).astype("uint16")
    elif dtype_out == "int16":
        data = np.round(arr_masked.filled(0)).clip(-32768, 32767).astype("int16")
    else:
        data = arr_masked.filled(0).astype("float32")
        dtype_out = "float32"

    meta = dict(
        crs=crs,
        transform=T,
        height=h,
        width=w,
        count=1,
        dtype=dtype_out,
        nodata=0,
        **GTIFF,
    )
    alpha = np.where(np.ma.getmaskarray(arr_masked), 0, 255).astype("uint8")

    with rasterio.open(path, "w", **meta) as dst:
        dst.write(data, 1)
        dst.write_mask(alpha)

        # Estadísticas (útil ArcGIS)
        valid = ~np.ma.getmaskarray(arr_masked)
        vals = arr_masked.data[valid]
        if vals.size > 0:
            vmin = float(np.nanpercentile(vals, 2))
            vmax = float(np.nanpercentile(vals, 98))
            vmean = float(np.nanmean(vals))
            vstd = float(np.nanstd(vals))
            dst.update_tags(1,
                STATISTICS_MINIMUM=str(vmin),
                STATISTICS_MAXIMUM=str(vmax),
                STATISTICS_MEAN=str(vmean),
                STATISTICS_STDDEV=str(vstd),
            )


def write_multiband_from_individuals(out_path: Path, band_files: List[Path], band_names: List[str]):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(band_files[0]) as ref:
        meta = ref.meta.copy()
        meta.update({
            "count": len(band_files),
            "compress": "LZW",
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "BIGTIFF": "YES",
        })
        out_dtype = meta["dtype"]

        with rasterio.open(out_path, "w", **meta) as dst:
            for i, (bf, bname) in enumerate(zip(band_files, band_names), start=1):
                with rasterio.open(bf) as src:
                    for _, window in src.block_windows(1):
                        data = src.read(1, window=window)
                        if data.dtype != np.dtype(out_dtype):
                            data = data.astype(out_dtype, copy=False)
                        dst.write(data, i, window=window)
                try:
                    dst.set_band_description(i, bname)
                except Exception:
                    pass


# =========================
# Índices por bloques (igual a tu versión, pero lee uint16/uint8 sin problema)
# =========================
def safe_div(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
    out = np.full_like(numer, np.nan, dtype=np.float32)
    mask = np.isfinite(numer) & np.isfinite(denom) & (denom != 0)
    out[mask] = (numer[mask] / denom[mask]).astype(np.float32)
    return out


def compute_indices_blockwise(band_paths: Dict[str, Path], out_dir: Path):
    req = ["B02","B03","B04","B08","B11","B12"]
    for r in req:
        if r not in band_paths:
            raise RuntimeError(f"Falta banda requerida para índices: {r}")

    blue_p  = band_paths["B02"]
    green_p = band_paths["B03"]
    red_p   = band_paths["B04"]
    nir_p   = band_paths["B08"]
    swir1_p = band_paths["B11"]
    swir2_p = band_paths["B12"]

    out_dir.mkdir(parents=True, exist_ok=True)

    with rasterio.open(red_p) as ref:
        meta = ref.meta.copy()
        meta.update({
            "dtype": "float32",
            "count": 1,
            "nodata": INDEX_NODATA,
            "compress": "LZW",
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "BIGTIFF": "YES",
        })

        out_files = {
            "i_ndvi": out_dir / "i_ndvi.tif",
            "i_ndmi": out_dir / "i_ndmi.tif",
            "i_nbri": out_dir / "i_nbri.tif",
            "i_msi":  out_dir / "i_msi.tif",
            "i_gndvi": out_dir / "i_gndvi.tif",
            "i_gci":  out_dir / "i_gci.tif",
            "i_evi":  out_dir / "i_evi.tif",
            "i_savi": out_dir / "i_savi.tif",
            "i_bsi":  out_dir / "i_bsi.tif",
            "i_avi":  out_dir / "i_avi.tif",
        }

        dsets = {k: rasterio.open(v, "w", **meta) for k, v in out_files.items()}

        try:
            with rasterio.open(blue_p) as blue, rasterio.open(green_p) as green, rasterio.open(nir_p) as nir, \
                 rasterio.open(swir1_p) as swir1, rasterio.open(swir2_p) as swir2:

                for _, window in tqdm(list(ref.block_windows(1)), desc="Índices (bloques)"):
                    B = blue.read(1, window=window).astype(np.float32)
                    G = green.read(1, window=window).astype(np.float32)
                    R = ref.read(1, window=window).astype(np.float32)
                    N = nir.read(1, window=window).astype(np.float32)
                    S1 = swir1.read(1, window=window).astype(np.float32)
                    S2 = swir2.read(1, window=window).astype(np.float32)

                    nod = ref.nodata
                    if nod is not None:
                        m = (R == nod)
                        B[m] = np.nan; G[m] = np.nan; R[m] = np.nan; N[m] = np.nan; S1[m] = np.nan; S2[m] = np.nan

                    ndvi = safe_div(N - R, N + R)
                    ndmi = safe_div(N - S1, N + S1)
                    nbri = safe_div(N - S2, N + S2)
                    msi  = safe_div(S1, N)
                    gndvi = safe_div(N - G, N + G)
                    gci = safe_div(N, G) - 1.0
                    evi = 2.5 * safe_div(N - R, (N + 6.0*R - 7.5*B + 1.0))
                    savi = 1.5 * safe_div(N - R, (N + R + 0.5))
                    bsi = safe_div((S1 + R) - (N + B), (S1 + R) + (N + B))

                    avi_raw = N * (1.0 - R) * (N - R)
                    avi_raw = np.where(np.isfinite(avi_raw) & (avi_raw > 0), avi_raw, np.nan)
                    avi = np.cbrt(avi_raw).astype(np.float32)

                    def _write(ds, arr):
                        out = np.where(np.isfinite(arr), arr, INDEX_NODATA).astype(np.float32)
                        ds.write(out, 1, window=window)

                    _write(dsets["i_ndvi"], ndvi)
                    _write(dsets["i_ndmi"], ndmi)
                    _write(dsets["i_nbri"], nbri)
                    _write(dsets["i_msi"],  msi)
                    _write(dsets["i_gndvi"], gndvi)
                    _write(dsets["i_gci"],  gci)
                    _write(dsets["i_evi"],  evi)
                    _write(dsets["i_savi"], savi)
                    _write(dsets["i_bsi"],  bsi)
                    _write(dsets["i_avi"],  avi)

        finally:
            for ds in dsets.values():
                ds.close()


# =========================
# Procesos por producto (usa grilla fija)
# =========================
def mosaic_product_fixedgrid(
    tiles: List[Path],
    band_names: List[str],
    out_individual_dir: Path,
    out_multiband_path: Path,
    dtype_map: Dict[str, str],
    ref_band_name: str,
    label: str,
):

    out_individual_dir.mkdir(parents=True, exist_ok=True)

    # 1) Grilla común
    ref_idx = band_names.index(ref_band_name) + 1  # 1-based
    arrays_ref, transforms_ref, crs_ref, res_ref = collect_arrays_from_tiles(tiles, ref_idx)
    if not arrays_ref:
        raise RuntimeError(f"[{label}] No se pudo establecer grilla de referencia con {ref_band_name}.")
    dst_grid = compute_ref_grid(transforms_ref, crs_ref, res_ref, arrays_ref, prefer_crs=crs_ref[0])

    # 2) Bandas
    band_files: List[Path] = []
    band_paths_map: Dict[str, Path] = {}

    print(f"[INFO] {label}: creando mosaicos individuales por banda (grilla fija)...")
    for bi, bname in enumerate(band_names, start=1):
        out_band = out_individual_dir / f"{bname}.tif"
        band_files.append(out_band)
        band_paths_map[bname] = out_band

        if out_band.exists():
            print(f"  [SKIP] ya existe: {out_band.name}")
            continue

        arrays, transforms, crs_list, _ = collect_arrays_from_tiles(tiles, bi)
        if not arrays:
            print(f"  [WARN] sin datos válidos para banda {bname}")
            continue

        mosaic, T, out_crs = mosaic_reproject_first(
            arrays, transforms, crs_list,
            dst_grid=dst_grid,
            resampling=Resampling.nearest
        )

        dtype_out = dtype_map.get(bname, "float32")
        save_tif_singleband(out_band, mosaic, T, out_crs, dtype_out=dtype_out)
        print(f"  [OK] {out_band.name}")

    # 3) Multibanda
    if not out_multiband_path.exists():
        print(f"[INFO] {label}: creando multibanda...")
        # ojo: si alguna banda faltó, la multibanda fallaría. Filtramos a existentes.
        existing = [(p, n) for p, n in zip(band_files, band_names) if p.exists()]
        if len(existing) != len(band_names):
            missing = [n for p, n in zip(band_files, band_names) if not p.exists()]
            raise RuntimeError(f"[{label}] Faltan bandas individuales para multibanda: {missing}")
        write_multiband_from_individuals(out_multiband_path, band_files, band_names)
        print(f"[OK] {out_multiband_path}")
    else:
        print(f"[SKIP] multibanda ya existe: {out_multiband_path}")

    return band_paths_map


def mosaic_s2(latest_folder: Path):
    print(f"[INFO] S2 latest_folder = {latest_folder}")
    tiles = list_tifs_recursive(latest_folder)
    print(f"[INFO] S2 tiles encontrados = {len(tiles)}")
    validate_all_same_bandcount(tiles, expected=len(S2_BAND_NAMES), label="S2")

    OUT_S2.mkdir(parents=True, exist_ok=True)
    out_individual_dir = OUT_S2 / "BANDS"
    out_indices_dir = OUT_S2 / "INDICES"

    dtype_map = {b: ("uint8" if b == "SCL" else "uint16") for b in S2_BAND_NAMES}

    band_paths_map = mosaic_product_fixedgrid(
        tiles=tiles,
        band_names=S2_BAND_NAMES,
        out_individual_dir=out_individual_dir,
        out_multiband_path=OUT_S2 / "mosaic_S2_BANDS_13.tif",
        dtype_map=dtype_map,
        ref_band_name="B02",
        label="S2",
    )

    print("[INFO] S2: calculando índices (bloques)...")
    compute_indices_blockwise(band_paths=band_paths_map, out_dir=out_indices_dir)
    print(f"[OK] Índices en: {out_indices_dir}")


def mosaic_hansen(latest_folder: Path):
    print(f"[INFO] HANSEN latest_folder = {latest_folder}")
    tiles = list_tifs_recursive(latest_folder)
    print(f"[INFO] HANSEN tiles encontrados = {len(tiles)}")
    validate_all_same_bandcount(tiles, expected=len(HANSEN_BAND_NAMES), label="HANSEN")

    OUT_HANSEN.mkdir(parents=True, exist_ok=True)
    out_individual_dir = OUT_HANSEN / "BANDS"

    # en Hansen, normalmente todo es byte. Si tu export tiene otro dtype, cámbialo aquí.
    dtype_map = {b: "uint8" for b in HANSEN_BAND_NAMES}

    mosaic_product_fixedgrid(
        tiles=tiles,
        band_names=HANSEN_BAND_NAMES,
        out_individual_dir=out_individual_dir,
        out_multiband_path=OUT_HANSEN / "mosaic_HANSEN_4B.tif",
        dtype_map=dtype_map,
        ref_band_name="treecover2000",
        label="HANSEN",
    )


def mosaic_srtm(latest_folder: Path):
    print(f"[INFO] SRTM latest_folder = {latest_folder}")
    tiles = list_tifs_recursive(latest_folder)
    print(f"[INFO] SRTM tiles encontrados = {len(tiles)}")
    validate_all_same_bandcount(tiles, expected=1, label="SRTM")

    OUT_SRTM.mkdir(parents=True, exist_ok=True)
    out_mosaic = OUT_SRTM / "mosaic_SRTM_1B.tif"
    if out_mosaic.exists():
        print(f"[SKIP] SRTM mosaico ya existe: {out_mosaic}")
        return

    # grilla común desde banda 1
    arrays_ref, transforms_ref, crs_ref, res_ref = collect_arrays_from_tiles(tiles, 1)
    if not arrays_ref:
        raise RuntimeError("[SRTM] No hay arrays válidos en banda 1.")
    dst_grid = compute_ref_grid(transforms_ref, crs_ref, res_ref, arrays_ref, prefer_crs=crs_ref[0])

    arrays, transforms, crs_list, _ = collect_arrays_from_tiles(tiles, 1)
    mosaic, T, out_crs = mosaic_reproject_first(arrays, transforms, crs_list, dst_grid=dst_grid, resampling=Resampling.nearest)

    # SRTM suele ser int16; si el tuyo viene float, puedes poner "float32"
    save_tif_singleband(out_mosaic, mosaic, T, out_crs, dtype_out="int16")
    print(f"[OK] {out_mosaic}")

# =========================


def run_all():
    """Entry point para Django / Celery"""
    if not EXPORTS_DIR.exists():
        raise FileNotFoundError(f"No existe carpeta EXPORTS: {EXPORTS_DIR}")

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    OUT_S2.mkdir(parents=True, exist_ok=True)
    OUT_HANSEN.mkdir(parents=True, exist_ok=True)
    OUT_SRTM.mkdir(parents=True, exist_ok=True)

    s2_latest = find_latest_s2_folder(EXPORTS_DIR)
    hansen_latest = find_latest_hansen_folder(EXPORTS_DIR)
    srtm_latest = find_latest_srtm_folder(EXPORTS_DIR)

    mosaic_s2(s2_latest)
    mosaic_hansen(hansen_latest)
    mosaic_srtm(srtm_latest)

    return str(OUT_ROOT)
