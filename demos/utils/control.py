from django.core.cache import cache
import ee

STOP_KEY = "STOP_PIPELINE"

def set_stop_pipeline(value: bool):
    cache.set(STOP_KEY, bool(value), timeout=None)

def should_stop_pipeline() -> bool:
    return bool(cache.get(STOP_KEY, False))

def cancel_active_tasks():
    for t in ee.batch.Task.list():
        if t.active():
            t.cancel()

