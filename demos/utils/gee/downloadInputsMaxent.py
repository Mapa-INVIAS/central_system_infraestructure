import os
import re
from pathlib import Path
from typing import List, Dict

from django.conf import settings
from google.cloud import storage


PRODUCTS = [
    {
        "name": "HANSEN",
        "base_prefix": "hansen_gfc_v1_12/",
        "folder_regex": r"^hansen_gfc_v1_12/(HANSEN_\d{8}_\d{4})/$",
    },
    {
        "name": "S2",
        "base_prefix": "s2/2025Q4/",
        "folder_regex": r"^s2/2025Q4/(\d{8}_\d{4})/$",
    },
    {
        "name": "SRTM",
        "base_prefix": "SRTM/",
        "folder_regex": r"^SRTM/(SRTM_\d{8}_\d{4})/$",
    },
]


def get_client() -> storage.Client:
    cred = Path(settings.GS_CREDENTIALS_FILE)
    if cred.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred)
    return storage.Client()


def list_prefixes(bucket, prefix: str) -> List[str]:
    """
    Detección manual de subcarpetas reales en GCS.
    Funciona incluso si GCS no devuelve prefixes.
    """
    prefixes = set()

    for blob in bucket.list_blobs(prefix=prefix):
        remainder = blob.name[len(prefix):]

        if "/" in remainder:
            folder = remainder.split("/")[0]
            prefixes.add(f"{prefix}{folder}/")

    return sorted(prefixes)


def build_candidates(prefixes: List[str], pattern: str):
    rx = re.compile(pattern)
    out = []

    for p in prefixes:
        m = rx.match(p)
        if m:
            token = m.group(1)
            out.append((token, p))

    return out


def find_latest_folder(bucket, base_prefix: str, folder_regex: str) -> str:
    prefixes = list_prefixes(bucket, base_prefix)
    cands = build_candidates(prefixes, folder_regex)

    if not cands:
        raise RuntimeError(f"No se encontraron subcarpetas fecha bajo {base_prefix}")

    cands.sort(key=lambda x: x[0])
    return cands[-1][1]


def list_all_blobs(bucket, prefix: str):
    return [b for b in bucket.list_blobs(prefix=prefix) if not b.name.endswith("/")]


def download_blob(blob, local_root: Path):
    dest = local_root / blob.name
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and blob.size == dest.stat().st_size:
        return str(dest)

    blob.download_to_filename(str(dest))
    return str(dest)


def download_latest_exports() -> Dict:
    EXPORTS_DIR = settings.MEDIA_ROOT / "exportsCGS"
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    client = get_client()
    bucket = client.bucket(settings.GS_BUCKET_NAME)

    results = {"products": []}

    for p in PRODUCTS:
        latest_prefix = find_latest_folder(bucket, p["base_prefix"], p["folder_regex"])
        blobs = list_all_blobs(bucket, latest_prefix)

        downloaded = []
        for b in blobs:
            downloaded.append(download_blob(b, EXPORTS_DIR))

        results["products"].append({
            "name": p["name"],
            "prefix": latest_prefix,
            "files_downloaded": downloaded,
        })

    return results
