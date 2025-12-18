import os
import math

from .scripts.A_download_REST import Downloadserver_REST
from .scripts.A_download_IDEAM_BosqueNO import DownloadBosqueNoBosque
from .scripts.A_download_OSM import DownloadOSMVias
from .scripts.B_Union import UnirShapefile
from .scripts.C_Rasterizar import RasterizarCarpetaSHP
from .scripts.D_Dist_Euclideana import DistanciaEuclidiana


def ejecutar_pipeline(base_path, geojson_referencia):
    os.makedirs(base_path, exist_ok=True)

    # =============================
    # CONFIG GENERAL
    # =============================
    CHUNK_INICIAL = 1000
    MIN_CHUNK = 25
    TIMEOUT = 20
    REINTENTOS = 6
    SLEEP = 0.15
    USAR_TQDM = False      
    USAR_PARALELO = True
    MAX_WORKERS = 4
    MAX_DEPTH = 2
    UMBRAL_PARALELO = 1000
    WKID_SALIDA = 4326

    # =============================
    # 1. DESCARGA REST IGAC
    # =============================
    Downloadserver_REST(
        url_servicio="https://mapas2.igac.gov.co/server/rest/services/carto/carto100000colombia2019/MapServer",
        carpeta_salida=os.path.join(base_path, "A_paraUnirAguas"),
        target_ids=[20, 25, 26, 36, 37, 39, 41, 42, 44, 47],
        bbox_fijo=None,
        chunk_inicial=CHUNK_INICIAL,
        min_chunk=MIN_CHUNK,
        timeout=TIMEOUT,
        reintentos=REINTENTOS,
        usar_tqdm=USAR_TQDM,
        usar_paralelo=USAR_PARALELO,
        max_workers=MAX_WORKERS,
        max_depth=MAX_DEPTH,
        sleep_s=SLEEP,
        umbral_paralelo=UMBRAL_PARALELO,
        formato_salida="geojson",
        wkid_salida=WKID_SALIDA
    )

    # =============================
    # 2. IDEAM Bosque / No Bosque
    # =============================
    DownloadBosqueNoBosque(
        carpeta_salida=os.path.join(base_path, "C_Raster"),
        anio_max=2030,
        anio_min=2000,
        timeout=60,
        base_url="https://bart.ideam.gov.co/cneideam/Capasgeo",
        nombre_final="Bosque_No_Bosque.tif"
    )

    # =============================
    # 3. OSM
    # =============================
    DownloadOSMVias(
        carpeta_salida=os.path.join(base_path, "B_Vectoriales"),
        nombre_salida="Vias.shp",
        bbox=(-4.2258, -81.7357, 13.3948, -66.8567),
        highway_tipos=[
            "motorway", "trunk", "primary", "secondary",
            "tertiary", "unclassified", "residential", "service"
        ],
        overpass_url="https://overpass-api.de/api/interpreter",
        timeout=60,
        reintentos=3,
        usar_tqdm=False,
        logfile=None
    )

    # =============================
    # 4. UNIÓN
    # =============================
    UnirShapefile(
        carpeta_entrada=os.path.join(base_path, "A_paraUnirAguas"),
        salida_shp=os.path.join(base_path, "B_Vectoriales/CAgua.shp"),
        buffer_metros=50 * math.sqrt(2),
        max_workers=os.cpu_count() - 2
    )

    # =============================
    # 5. RASTERIZAR
    # =============================
    RasterizarCarpetaSHP(
        carpeta_shp=os.path.join(base_path, "B_Vectoriales"),
        geojson_referencia=geojson_referencia,
        carpeta_salida=os.path.join(base_path, "C_Raster"),
        pixel_m=100
    )

    # =============================
    # 6. DISTANCIA EUCLIDEANA
    # =============================
    DistanciaEuclidiana(
        carpeta_raster=os.path.join(base_path, "C_Raster"),
        geojson_referencia=geojson_referencia,
        carpeta_salida=os.path.join(base_path, "D_Distancia"),
        valor_fuente=1,
        bloque_escalado=0
    )
