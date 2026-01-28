from .gee.generatebufferInvias import generar_buffer_invias
from .gee.exportTiles import run_s2_export
from .gee.downloadInputsMaxent import download_latest_exports
from .gee.makeMosaicInputs import full_mosaic_nacional
from .gee.generatebufferInvias import generar_buffer_invias

from .gee.ee_init import init_ee
from django.conf import settings
from pathlib import Path

def gee_pipeline(body: dict, MAX_CONCURRENT, PERIOD_AVERAGE):
    #  1. generate buffer
    #  2. run generate tiles
    #  3. download tiles
    #  4. make mosaic
    
    init_ee()

    # 1. Sistema de creación de mascara del mapa de rutas
    URL = "https://mapas2.igac.gov.co/server/rest/services/carto/carto100000colombia2019/MapServer"
    DIRECTORIO_SALIDA = Path(settings.MEDIA_ROOT, 'modula_carga')
    DIRECTORIO_SALIDA.mkdir(parents=True, exist_ok=True)
    CARPETA_SALIDA =  Path(DIRECTORIO_SALIDA, 'vias_invias')
    NOMBRE_SALIDA = 'vias_invias.shp'

    generar_bufferinvias = generar_buffer_invias(
        url=URL,
        carpeta_salida=CARPETA_SALIDA,
        nombre_salida=NOMBRE_SALIDA,
        buffer_m=200
    )

    generar_bufferinvias.ejecutar()

    # 2. Carga de elementos en el contenedor en la nube
    s2_result = run_s2_export(
        limit_zones=body.get("limit_zones"),
        dry_run_tiles=body.get("dry_run_tiles"),
        MAX_CONCURRENT=MAX_CONCURRENT,
        PERIOD_AVERAGE=PERIOD_AVERAGE
    )

    # 3. Descarga de elementos del contenedor de la nube
    exports_result = download_latest_exports()
    
    # 4. Generación del mosaico de las capas
    exports_dir = Path(DIRECTORIO_SALIDA, 'exportsCGS')
    mosaic_result = full_mosaic_nacional(
        exports_dir=exports_dir,
        run_s2=body.get("run_s2", True),
        run_hansen=body.get("run_hansen", True),
        run_srtm=body.get("run_srtm", True),
    )

    return {
        "buffer_invias": generar_bufferinvias,
        "run_s2_export": s2_result,
        "download_exports": exports_result,
        "mosaic_nacional": mosaic_result,
    }


