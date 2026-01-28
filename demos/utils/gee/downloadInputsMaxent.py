# -*- coding: utf-8 -*-
"""
DESCARGA_GCS_EXPORTS_LATEST_V1.py

Descarga a local (carpeta EXPORTS/) la información del bucket conservando jerarquía,
pero SOLO tomando la subcarpeta "más reciente" por producto:

- HANSEN: hansen_gfc_v1_12/HANSEN_YYYYMMDD_HHMM/**
- S2:     s2/2025Q4/YYYYMMDD_HHMM/**
- SRTM:   SRTM/SRTM_YYYYMMDD_HHMM/**

Ejemplo:
  EXPORTS/hansen_gfc_v1_12/HANSEN_20251214_1808/...
  EXPORTS/s2/2025Q4/20251024_1341/...
  EXPORTS/SRTM/SRTM_20251214_1530/...

Requisitos:
  pip install google-cloud-storage tqdm
"""

import os
import re
from pathlib import Path
from typing import List, Tuple, Dict, Optional

from tqdm import tqdm
from google.cloud import storage
from django.conf import settings


# Definición de productos: (nombre, base_prefix, regex_carpeta_fecha)
# regex debe capturar en grupo(1) el "token" comparable lexicográficamente.
PRODUCTS = [
    {
        "name": "HANSEN",
        "base_prefix": "hansen_gfc_v1_12/",
        # carpetas: hansen_gfc_v1_12/HANSEN_YYYYMMDD_HHMM/
        "folder_regex": r"^hansen_gfc_v1_12/(HANSEN_(\d{8})_(\d{4}))/\$",
        # NOTA: esta regex se usa sobre "prefix final", ver función build_candidates()
        # la normalizamos abajo
    },
    {
        "name": "S2",
        "base_prefix": "s2/2025Q4/",
        # carpetas: s2/2025Q4/YYYYMMDD_HHMM/
        "folder_regex": r"^s2/2025Q4/(\d{8}_\d{4})/\$",
    },
    {
        "name": "SRTM",
        "base_prefix": "SRTM/",
        # carpetas: SRTM/SRTM_YYYYMMDD_HHMM/
        "folder_regex": r"^SRTM/(SRTM_(\d{8})_(\d{4}))/\$",
    },
]


# =========================
# GCS helpers
# =========================
def get_client() -> storage.Client:
    cred = Path(settings.GS_CREDENTIALS_FILE)
    if cred.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred)
    return storage.Client()


def list_prefixes(bucket: storage.Bucket, prefix: str) -> List[str]:

    it = bucket.list_blobs(prefix=prefix, delimiter="/")
    _ = list(it)  # consumir para llenar it.prefixes
    return sorted(list(it.prefixes))


def build_candidates(prefixes: List[str], pattern: str) -> List[Tuple[str, str]]:

    rx = re.compile(pattern)
    out = []
    for p in prefixes:
        s = p
        if s.endswith("/"):
            s_match = s + "$"
        else:
            s_match = s + "/$"
        m = rx.match(s_match)
        if m:
            token = m.group(1)
            out.append((token, p))
    return out


def find_latest_folder(bucket: storage.Bucket, base_prefix: str, folder_regex: str) -> str:

    prefixes = list_prefixes(bucket, base_prefix)
    cands = build_candidates(prefixes, folder_regex)

    if not cands:
        raise RuntimeError(f"No se encontraron subcarpetas fecha bajo '{base_prefix}' con regex '{folder_regex}'")

    # token (YYYYMMDD_HHMM o HANSEN_YYYYMMDD_HHMM) es comparable lexicográficamente
    cands.sort(key=lambda x: x[0])
    latest_prefix = cands[-1][1]
    return latest_prefix


def list_all_blobs_under(bucket: storage.Bucket, prefix: str) -> List[storage.Blob]:
    return list(bucket.list_blobs(prefix=prefix))


def download_blob_to_local(blob: storage.Blob, local_root: Path, skip_existing: bool = True) -> Path:

    dest = local_root / blob.name
    dest.parent.mkdir(parents=True, exist_ok=True)

    if skip_existing and dest.exists():
        # Si el archivo ya está y coincide tamaño, lo saltamos
        try:
            if blob.size is not None and dest.stat().st_size == blob.size:
                return dest
        except Exception:
            pass

    blob.download_to_filename(str(dest))
    return dest


# =========================
# MAIN
# =========================

def download_latest_exports() -> Dict:
    # =========================
    # CONFIG
    # =========================

    # EXPORTS_DIR = settings.MEDIA_ROOT / "exportsCGS"
    EXPORTS_DIR = Path(settings.MEDIA_ROOT, 'modula_carga', 'exportsCGS')
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    client = get_client()
    bucket = client.bucket(settings.GS_BUCKET_NAME)

    print(f"[INFO] Bucket: gs://{settings.GS_BUCKET_NAME}")
    print(f"[INFO] Carpeta local: {EXPORTS_DIR}")

    chosen: Dict[str, str] = {}

    # 1) Determinar carpetas más recientes
    for p in PRODUCTS:
        name = p["name"]
        base_prefix = p["base_prefix"]
        folder_regex = p["folder_regex"]

        latest = find_latest_folder(bucket, base_prefix, folder_regex)
        chosen[name] = latest
        print(f"[INFO] {name}: carpeta más reciente = gs://{settings.GS_BUCKET_NAME}/{latest}")

    # 2) Descargar cada producto (solo la carpeta elegida)
    for p in PRODUCTS:
        name = p["name"]
        latest_prefix = chosen[name]

        blobs = list_all_blobs_under(bucket, latest_prefix)
        # filtrar "carpetas" (en GCS a veces aparecen blobs con name terminado en '/')
        blobs = [b for b in blobs if not b.name.endswith("/")]

        if not blobs:
            print(f"[WARN] {name}: no hay blobs para descargar bajo {latest_prefix}")
            continue

        print(f"\n=== DESCARGA {name} | archivos={len(blobs)} ===")
        for b in tqdm(blobs, desc=f"{name}"):
            download_blob_to_local(b, EXPORTS_DIR, skip_existing=True)

        print(f"[OK] {name}: descargado en {EXPORTS_DIR / latest_prefix}")

    print("\n[OK] DESCARGA COMPLETA. Estructura conservada bajo ./EXPORTS/")


