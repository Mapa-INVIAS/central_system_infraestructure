import math
import os

####################################################################
################# Códigos de servicios paralelos ###################

from .services.A_download_REST import Downloadserver_REST
from .services.A_download_IDEAM_BosqueNO import DownloadBosqueNoBosque
from .services.A_download_OSM import DownloadOSMVias
from .services.B_Union import UnirShapefile
from .services.C_Rasterizar import RasterizarCarpetaSHP
from .services.D_Dist_Euclideana import DistanciaEuclidiana

####################################################################


def pipeline_process(output_dir, input_name):

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
    logfile=output_dir / "CARPETA_SALIDA" / "log_osm.txt"
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
    BLOQUE_ESCALADO = 0

    # Ejecución Distancia Euclideana
    CARPETA_RASTER = output_dir / "C_Raster"
    CARPETA_SALIDA = output_dir / "D_Distancia"
    DistanciaEuclidiana(CARPETA_RASTER,
                        input_name,
                        CARPETA_SALIDA,
                        VALOR_FUENTE,
                        BLOQUE_ESCALADO)


