# app/gee/runner.py
import threading
import logging
from .exportTiles import run_s2_export

logger = logging.getLogger(__name__)

def launch_s2_job(params):

    def job():
        try:
            run_s2_export(
                limit_zones=params.get("limit_zones"),
                dry_run_tiles=params.get("dry_run_tiles"),
            )
        except Exception:
            logger.exception("Error ejecutando proceso S2")

    t = threading.Thread(target=job, daemon=True)
    t.start()
