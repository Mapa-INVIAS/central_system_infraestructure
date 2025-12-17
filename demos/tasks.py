# apps/processing/tasks.py
from celery import shared_task
from .utils.makeMosaicInputs import run_all


@shared_task(bind=True)
def run_mosaics_task(self):
    output_path = run_all()
    return {
        "status": "ok",
        "output": output_path
    }
