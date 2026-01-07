from .gee.exportTiles import run_s2_export
from .gee.downloadInputsMaxent import download_latest_exports
from .gee.makeMosaicInputs import full_mosaic_nacional
from .gee.generatebufferInvias import generar_buffer_invias

from .gee.ee_init import init_ee
from django.conf import settings
from pathlib import Path

def gee_pipeline(body: dict, MAX_CONCURRENT):
    #  1. run generate tiles
    #  2. download tiles
    #  3. make mosaic
    #  4. generate buffer
    
    init_ee()

    # 1. 
    s2_result = run_s2_export(
        limit_zones=body.get("limit_zones"),
        dry_run_tiles=body.get("dry_run_tiles"),
        MAX_CONCURRENT=MAX_CONCURRENT
    )

    # 2.
    exports_result = download_latest_exports()
    
    # 3.
    exports_dir = Path(settings.MEDIA_ROOT) / "EXPORTS"
    mosaic_result = full_mosaic_nacional(
        exports_dir=exports_dir,
        run_s2=body.get("run_s2", True),
        run_hansen=body.get("run_hansen", True),
        run_srtm=body.get("run_srtm", True),
    )

    # 4.
    try:
        archivo_url = generar_buffer_invias()  # tu función devuelve la URL del shapefile
        buffer_invias_result = {
            "status": "ok",
            "mensaje": "Shapefile INVIAS creado exitosamente",
            "archivo": archivo_url
        }
    except Exception as e:
        buffer_invias_result = {
            "status": "error",
            "mensaje": str(e)
        }

    return {
        "run_s2_export": s2_result,
        "download_exports": exports_result,
        "mosaic_nacional": mosaic_result,
        "buffer_invias": buffer_invias_result
    }



