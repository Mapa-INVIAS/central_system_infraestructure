import os
import shutil
from pathlib import Path
from django.conf import settings
from tqdm import tqdm

def mover_tifs():
    """
    Mueve archivos .tif desde modula_carga y modula_servicios
    hacia un único directorio modula_proceso/EXPORTS_TIF.
    Retorna un resumen de los archivos copiados.
    """
    media_root = Path(settings.MEDIA_ROOT)

    # Definir rutas
    media_carga = media_root / 'modula_carga' / 'MOSAICS' / 'EXPORTS'
    media_servicios = media_root / 'modula_servicios' / 'AutoINVIAS'
    output_export = media_root / 'modula_proceso' / 'EXPORTS_TIF'
    output_export.mkdir(parents=True, exist_ok=True)

    extract_folders = [media_carga, media_servicios]
    resultados = []
    archivos_tif = []

    # Recolectar todos los archivos primero
    for folder in extract_folders:
        if not folder.exists():
            resultados.append({"folder": str(folder), "status": "no encontrado"})
            continue

        for root, dirs, files in os.walk(folder):
            for file in files:
                if file.lower().endswith('.tif'):
                    full_path = Path(root) / file
                    archivos_tif.append(full_path)

    # Iterar con tqdm mostrando progreso
    for full_path in tqdm(archivos_tif, desc="Copiando archivos .tif"):
        destino = output_export / full_path.name   
        try:
            shutil.copy(full_path, destino)
            resultados.append({
                "origen": str(full_path),
                "destino": str(destino),
                "status": "copiado"
            })
        except Exception as e:
            resultados.append({
                "origen": str(full_path),
                "destino": str(destino),
                "status": f"error: {e}"
            })

    return resultados


# import os
# import shutil
# from pathlib import Path
# from django.conf import settings
# from tqdm import tqdm   # 👈 Importar tqdm

# def mover_tifs():
#     """
#     Mueve archivos .tif desde modula_carga y modula_servicios
#     hacia modula_proceso/EXPORTS_TIF, preservando estructura.
#     Retorna un resumen de los archivos copiados.
#     """
#     media_root = Path(settings.MEDIA_ROOT)

#     # Definir rutas
#     media_carga = media_root / 'modula_carga' / 'MOSAICS' / 'EXPORTS'
#     media_servicios = media_root / 'modula_servicios' / 'AutoINVIAS'
#     output_export = media_root / 'modula_proceso' / 'EXPORTS_TIF'
#     output_export.mkdir(parents=True, exist_ok=True)

#     extract_folders = [media_carga, media_servicios]
#     resultados = []
#     archivos_tif = []

#     # Recolectar todos los archivos primero
#     for folder in extract_folders:
#         if not folder.exists():
#             resultados.append({"folder": str(folder), "status": "no encontrado"})
#             continue

#         for root, dirs, files in os.walk(folder):
#             for file in files:
#                 if file.lower().endswith('.tif'):
#                     full_path = Path(root) / file
#                     relative_path = Path(root).relative_to(folder)
#                     target_dir = output_export / relative_path
#                     archivos_tif.append((full_path, target_dir, file))

#     # Iterar con tqdm mostrando progreso
#     for full_path, target_dir, file in tqdm(archivos_tif, desc="Copiando archivos .tif"):
#         target_dir.mkdir(parents=True, exist_ok=True)
#         destino = target_dir / file
#         try:
#             shutil.copy(full_path, destino)
#             resultados.append({
#                 "origen": str(full_path),
#                 "destino": str(destino),
#                 "status": "copiado"
#             })
#         except Exception as e:
#             resultados.append({
#                 "origen": str(full_path),
#                 "destino": str(destino),
#                 "status": f"error: {e}"
#             })

#     return resultados
