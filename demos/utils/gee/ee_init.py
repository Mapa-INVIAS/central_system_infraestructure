#  Reinicializar la conexión con el Bucket de Google Cloud Storage.

import ee
from django.conf import settings


_EE_READY = False   # flag propio


def init_ee():
    global _EE_READY
    if _EE_READY:
        return

    creds = ee.ServiceAccountCredentials(
        settings.GS_EMAIL,
        str(settings.GS_CREDENTIALS_FILE)
    )

    ee.Initialize(
        credentials=creds,
        project=settings.GS_PROJECT_ID
    )

    # Verificación real
    ee.Number(1).getInfo()

    _EE_READY = True
