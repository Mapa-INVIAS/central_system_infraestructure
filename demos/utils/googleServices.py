from .gee.exportTiles import run_s2_export
from .gee.downloadInputsMaxent import download_latest_exports
from .gee.makeMosaicInputs import full_mosaic_nacional

from .gee.ee_init import init_ee
from django.conf import settings
from pathlib import Path



def gee_pipeline(body: dict):
    #  1. run generate tiles
    #  2. download tiles
    #  3. make mosaic
    init_ee()

    s2_result = run_s2_export(
        limit_zones=body.get("limit_zones"),
        dry_run_tiles=body.get("dry_run_tiles")
    )
    
    exports_result = download_latest_exports()
    

    exports_dir = Path(settings.MEDIA_ROOT) / "EXPORTS"

    mosaic_result = full_mosaic_nacional(
        exports_dir=exports_dir,
        run_s2=body.get("run_s2", True),
        run_hansen=body.get("run_hansen", True),
        run_srtm=body.get("run_srtm", True),
    )

    return {
        "run_s2_export": s2_result,
        "download_exports": exports_result,
        "mosaic_nacional": mosaic_result
    }



