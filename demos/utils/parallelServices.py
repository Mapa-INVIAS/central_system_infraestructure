import math, os, uuid
from demos.utils.control import should_stop_pipeline
from pathlib import Path
from django.conf import settings

####################################################################
################# Códigos de servicios paralelos ###################

from .services.A_download_REST import Downloadserver_REST
from .services.A_download_IDEAM_BosqueNO import DownloadBosqueNoBosque
from .services.A_download_OSM import DownloadOSMVias
from .services.B_Union import UnirShapefile
from .services.C_Rasterizar import RasterizarCarpetaSHP
from .services.D_Dist_Euclideana import DistanciaEuclidiana

from .kripley02 import KRipley_HS
####################################################################


def pipeline_process(output_dir, input_name):

    # Ejecución de K-Ripley

    if should_stop_pipeline():
        return {"status": "stopped", "stage": "Sukubun"}

    # ============================
    # ETAPA K-Ripley
    # ============================
    media_root = Path(settings.MEDIA_ROOT)
    uploads_folder =  Path(media_root, 'modula_servicios', 'sukubun')

    excel_files = list(uploads_folder.glob("*.xlsx")) + list(uploads_folder.glob("*.xls"))
    if not excel_files:
        raise FileNotFoundError(f"No se encontró ningún archivo Excel en {uploads_folder}")

    excel_path = max(excel_files, key=lambda f: f.stat().st_mtime)
    roads_path = media_root / "Vias_Total" / "Vias_Total.shp"

    if not excel_path.exists():
        raise FileNotFoundError(f"No existe el Excel: {excel_path}")
    if not roads_path.exists():
        raise FileNotFoundError(f"No existe el SHP de vías: {roads_path}")

    # Parámetros (puedes pasarlos como parte de payload o usar defaults)
    excel_sheet = "SUKUBUN"
    lat_field   = "y"
    lon_field   = "x"

    simplify_tolerance_m = 1.0
    precision_scale      = 0.001
    segment_spacing_m    = 50.0
    snap_tolerance_m     = 90.0
    r_start_m            = 100.0
    r_step_m             = 500.0

    n_sim_ripley  = 100
    n_sim_hotspot = 100
    random_seed   = 321

    hs_point_spacing_m   = 50.0
    n_workers            = 2
    max_hs_sample_points = None
    plot_png             = True

    run_id = uuid.uuid4().hex[:8]
    output_folder = Path(media_root, 'modula_servicios') / "kripley_resultados" / run_id
    output_folder.mkdir(parents=True, exist_ok=True)

    export_csv_hotspots_name = "hotspots.csv"
    export_csv_ripley_name   = "ripley_L.csv"
    export_shp_vias_name     = "vias_simplificadas.shp"

    KRipley_HS(
        excel_path,
        excel_sheet,
        lat_field,
        lon_field,
        roads_path,
        str(output_folder),
        simplify_tolerance_m,
        precision_scale,
        segment_spacing_m,
        snap_tolerance_m,
        r_start_m,
        r_step_m,
        n_sim_ripley,
        random_seed,
        n_sim_hotspot,
        hs_point_spacing_m,
        export_csv_hotspots_name,
        export_csv_ripley_name,
        export_shp_vias_name,
        plot_png,
        n_workers,
        max_hs_sample_points
    )

    # Configuración general
    CHUNK_INICIAL = 1000
    MIN_CHUNK = 25
    TIMEOUT = 20
    REINTENTOS = 6
    SLEEP = 0.15
    USAR_TQDM = True
    USAR_PARALELO = True
    MAX_WORKERS = 4
    MAX_DEPTH = 2
    UMBRAL_PARALELO = 1000
    WKID_SALIDA = 4326

    if should_stop_pipeline():
        return {"status": "stopped", "stage": "Unir aguas"}

     # Ejecución bajar partes C_Agua
    URL = "https://mapas2.igac.gov.co/server/rest/services/carto/carto100000colombia2019/MapServer"
    SALIDA = output_dir / "A_paraUnirAguas"
    TARGET_IDS = [20, 25, 26, 36, 37, 39, 41, 42, 44, 47]
    BBOX = None
    FORMATO_SALIDA = "geojson"
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

    if should_stop_pipeline():
        return {"status": "stopped", "stage": "RUNAP"}

    # Ejecución bajar RUNAP
    URL = "https://mapas.parquesnacionales.gov.co/arcgis/rest/services/pnn/runap/MapServer"
    SALIDA = output_dir / "B_Vectoriales"
    TARGET_IDS = [0]
    BBOX = None
    FORMATO_SALIDA = "shp"
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
    
    if should_stop_pipeline():
        return {"status": "stopped", "stage": "IDEAM"}

    # BAJAR servidor FTP IDEAM
    # configuración inicial IDEAM
    ANIO_MAX = 2030
    ANIO_MIN = 2000
    TIMEOUT = 60

    # Ejecución bajar FTP IDEAM
    BASE_URL = "https://bart.ideam.gov.co/cneideam/Capasgeo"
    SALIDA = output_dir / "C_Raster"
    NOMBRE_FINAL = "Bosque_No_Bosque.tif"
    DownloadBosqueNoBosque(SALIDA,
                        ANIO_MAX,
                        ANIO_MIN,
                        TIMEOUT,
                        BASE_URL,
                        NOMBRE_FINAL)
    
    if should_stop_pipeline():
        return {"status": "stopped", "stage": "OSM"}

    # BAJAR OSM
    # configuración inicial OSM
    BBOX_COLOMBIA = (-4.2258, -81.7357, 13.3948, -66.8567)
    timeout=60
    reintentos=3
    usar_tqdm=True
    HIGHWAY_TIPOS = ["motorway", 
                    "trunk", 
                    "primary", 
                    "secondary", 
                    "tertiary",
                    "unclassified", 
                    "residential", 
                    "service"]

    # Ejecución bajar OSM
    overpass_url="https://overpass-api.de/api/interpreter"
    CARPETA_SALIDA = output_dir / "B_Vectoriales"
    logfile= Path(CARPETA_SALIDA) / "log_osm.txt"
    NOMBRE_SALIDA = "Vias.shp"
    DownloadOSMVias(CARPETA_SALIDA,
                    NOMBRE_SALIDA,
                    BBOX_COLOMBIA,
                    HIGHWAY_TIPOS,
                    overpass_url,
                    timeout,
                    reintentos,
                    usar_tqdm,
                    logfile)

    # ETAPA ALISTAMIENTO VECTORIALES
    # configuracion inicial union
    BUFFER_METROS = 50 * math.sqrt(2)
    MAX_WORKERS = os.cpu_count() - 2

    # Ejecución Unión
    CARPETA_ENTRADA = output_dir / "A_paraUnirAguas"
    SALIDA = output_dir / "B_Vectoriales/CAgua.shp"
    UnirShapefile(CARPETA_ENTRADA,
                  SALIDA,
                  BUFFER_METROS,
                  MAX_WORKERS)
   
    # ETAPA RASTERIZADO
    # configuracion inicial Rasterizado
    PIXEL_METROS = 100

    # Ejecución Rasterizado
    CARPETA_SHP = output_dir / "B_Vectoriales"
    CARPETA_SALIDA = output_dir / "C_Raster"
    RasterizarCarpetaSHP(CARPETA_SHP,
                         input_name,
                         CARPETA_SALIDA,
                         PIXEL_METROS)

    # ETAPA CALCULO DE DISTANCIA EUCLIDEANA
    # configuracion inicial Rasterizado
    VALOR_FUENTE = 1

    # Ejecución Distancia Euclideana
    CARPETA_RASTER = output_dir / "C_Raster"
    CARPETA_SALIDA = output_dir / "D_Distancia"
    DistanciaEuclidiana(CARPETA_RASTER,
                        input_name,
                        CARPETA_SALIDA,
                        VALOR_FUENTE)
    
    # return {
    #     "status": "ok",
    #     "run_id": run_id,
    #     "output_folder": str(output_folder),
    #     "outputs": {
    #         "ripley": export_csv_ripley_name,
    #         "hotspots": export_csv_hotspots_name,
    #         "vias": export_shp_vias_name,
    #         "metadata": "metadata.json"
    #     }
    # }
    
