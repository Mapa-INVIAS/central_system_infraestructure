# Central process functions

from demos.utils.control import should_stop_pipeline
from pathlib import Path
from django.conf import settings

import os, shutil

from .model.E_alinear_Rasters import AlinearRastersSparsePorReferencia
from .model.F_clipCapas import RecortePorRegion
from .model.maxentModel02 import MaxEntWorkflow
from .model.moverTifs import MoverTifs


def centralModelProcess():

    media_root = Path(settings.MEDIA_ROOT)
    static_root = Path(settings.BASE_DIR, 'static')

    proceso = MoverTifs() 
    resultado = proceso.run()

    # Mover todos los archivos al directorio rasterIN

    # Alinear capas
    CARPETA_ENTRADA     = Path(media_root, 'modula_proceso', 'EXPORTS_TIF')
    RASTER_REFERENCIA   = Path(CARPETA_ENTRADA) / "mosaic_SRTM_1B.tif"
    CARPETA_SALIDA      = Path(media_root, 'modula_proceso', 'E_Alineados')


    AlinearRastersSparsePorReferencia(
        CARPETA_ENTRADA,
        RASTER_REFERENCIA,
        CARPETA_SALIDA,
        valores_nodata_virtuales=(-9999, -99999, -32768),
        nodata_warp=-9999.0
    )

    # Segmentar y generar Jacknife

    CARPETA_RASTERS_TIF = Path(media_root, 'modula_proceso') / 'E_Alineados'
    VIAS_VECTOR_PATH = Path(static_root, 'backend', 'geodata') / 'Vias_Total/Vias_Total.shp'
    REGIONES_SHP_PATH = Path(static_root, 'backend', 'geodata') / 'regionesColombia/regionesColombia/regiones.shp'
    CSV_ATROPELLAMIENTOS_PATH = Path(media_root, 'modula_servicios') / 'kripley_resultados_test\hotspots.csv'

    CARPETA_SALIDA = Path(media_root, 'modula_proceso', 'jacknife')

    CAMPO_REGION = "REGION" # campo de la capa de regiones
    BUFFER_M = 200.0 # este buffer se usa para que existaq un área de guarda 

    CSV_LAT_COL = "Latitude"
    CSV_LON_COL = "Longitude"
    CSV_CRS_EPSG = 4326

    RecortePorRegion(CARPETA_RASTERS_TIF,
                     VIAS_VECTOR_PATH,
                     REGIONES_SHP_PATH,
                     CSV_ATROPELLAMIENTOS_PATH,
                     CARPETA_SALIDA,
                     CAMPO_REGION,
                     BUFFER_M,
                     CSV_LAT_COL,
                     CSV_LON_COL,
                     CSV_CRS_EPSG)
    
    # Ejecutar modelo MAXENT

    from rpy2.robjects import conversion
    try:
        from rpy2.robjects.conversion import _converter as rpy2_converter  # para versiones nuevas
    except ImportError:
        from rpy2.robjects import default_converter as rpy2_converter  # versiones antiguas

    from rpy2.robjects import default_converter
    from rpy2.robjects.conversion import localconverter

    jackknife_root = os.path.join(settings.MEDIA_ROOT, 'modula_proceso', 'jacknife')

    if not os.path.isdir(jackknife_root):
        error_path = {"status": "error", "message": "No existe la carpeta jacknife"}

    # Detectar todas las regiones
    regiones = [
        d for d in os.listdir(jackknife_root)
        if os.path.isdir(os.path.join(jackknife_root, d))
    ]

    if not regiones:
        error_path = {"status": "error", "message": "No hay regiones dentro de jacknife"}

    resultados = {}

    # =============================== #
    #  Ejecutar workflow SECUENCIAL   #
    # =============================== #

    for region in regiones:
        try:
            workflow = MaxEntWorkflow(project_name=region)
            # Garantizar contexto de rpy2 dentro del mismo thread
            with localconverter(default_converter):
                workflow.run()
            resultados[region] = "OK"
        except Exception as e:
            resultados[region] = f"ERROR: {str(e)}"

