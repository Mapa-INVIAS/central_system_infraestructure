import time, os, schedule, json, rasterio, traceback, ee, geemap
from django.shortcuts import render, redirect
from django.http import JsonResponse, FileResponse
from django.http import HttpResponse
from django.conf import settings
from google.cloud import storage
from google.cloud.storage import transfer_manager
from rasterio.features import shapes
from django.views.decorators.csrf import csrf_exempt
from .utils.maxent02 import MaxEntWorkflow  # importa tu clase
from .utils.downloadInputsMaxent import download_latest_exports

#================================================#
#                                                #
##            CENTRAL PROCESS SYSTEM            ##
#                                                #
#================================================#

import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio as rs
from tqdm import tqdm
from collections import defaultdict

## ========  Sample external data ======== ##

from geodatasets import get_path
import rasterio.features
import rasterio.warp


# ========================================= #
# Current conditions within the target bucket
# Tree Bucket

# This function scans the bucket and detects updates

def build_tree(blobs):
    tree = {}
    for blob in blobs:
        parts = blob.name.split("/")
        node = tree
        for part in parts[:-1]: 
            node = node.setdefault(part, {})
        node.setdefault("_files", []).append((parts[-1], blob))
    return tree

# 
def print_tree(node, indent=0):
    for key, value in sorted(node.items()):
        if key == "_files":
            for file, blob in sorted(value, key=lambda x: x[1].updated, reverse=True):
                print(" " * indent + f"||--** {file} || Date: {blob.updated}")
        else:
            print(" " * indent + f"|-* {key}")
            print_tree(value, indent + 4)


# Función de scanner
def list_blobs(bucket_name):
    # global contador
    # contador += 1

    """Lista blobs agrupados en árbol dinámico."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    tree = build_tree(blobs)
    print_tree(tree)
    # print(f'Ejecución número  {contador}')

bucket_name = "invias_mapa_vulnerabilidad_faunistica"

# schedule.every(1).minutes.do(list_blobs, bucket_name)
# contador = 0

# print('Activando programador de tareas')
# while True:
#     schedule.run_pending()
#     time.sleep(1)

list_blobs(bucket_name)

########################################################################
########################################################################
# Descarga GCS ultimo

def download_exports(request):
    try:
        result = download_latest_exports()
        return JsonResponse({"status": "ok", "result": result})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

########################################################################
########################################################################
# Crea los mosaicos


from django.contrib.admin.views.decorators import staff_member_required
from .tasks import run_mosaics_task
from celery.result import AsyncResult


@staff_member_required
def run_mosaics_page(request):
    """
    Página web para lanzar el procesamiento
    """
    if request.method == "POST":
        task = run_mosaics_task.delay()
        return redirect("mosaics_status_page", task_id=task.id)

    return render(request, "mosaic.html")



@staff_member_required
def mosaics_status_page(request, task_id):
    result = AsyncResult(task_id)

    return render(request, "mosaicstatus.html", {
        "task_id": task_id,
        "state": result.state,
        "result": result.result
    })


########################################################################
########################################################################

import threading
import traceback

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .utils.exportTiles import run_s2_export


@csrf_exempt
@require_POST
def run_s2(request):
    """
    Endpoint para lanzar el proceso S2 sin bloquear Django
    """

    def background_job():
        try:
            run_s2_export()
        except Exception:
            traceback.print_exc()

    thread = threading.Thread(
        target=background_job,
        daemon=True
    )
    thread.start()

    return JsonResponse({
        "status": "started",
        "message": "Proceso Sentinel-2 lanzado en segundo plano"
    })


########################################################################
########################################################################
# Complete download of bucket data
def download_bucket_with_transfer_manager(
    bucket_name, workers=8, max_results=None, prefix=None):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(max_results=max_results, prefix=prefix)
    blob_names = [blob.name for blob in blobs]

    destination_directory = settings.MEDIA_ROOT

    results = transfer_manager.download_many_to_path(
        bucket,
        blob_names,
        destination_directory=destination_directory,
        max_workers=workers,
    )

    for name, result in zip(blob_names, results):
        if isinstance(result, Exception):
            print(f"x Error al descargar {name}: {result}")
        else:
            ruta_final = os.path.join(destination_directory, name)
            print(f"✓ Descargado {name} -> {ruta_final}")

    print("🎉 Todos los archivos descargados con éxito")


print('si esta corriendo') 
# download_bucket_with_transfer_manager("invias")
# download_bucket_with_transfer_manager("invias_mapa_vulnerabilidad_faunistica")

# ==================================================================== #
# #################################################################### #

# Create your views here #
def demo_numpy(request):
    data = [1,2,3,4,5]
    mean_data = np.mean(data)
    std_data = np.std(data)
    return render(request, 'demo_numpy.html', 
                    {
                      'data': data, 
                      'mean_data': mean_data, 
                      'std_data': std_data
                    })

def demo_pandas(request):
    data = {
        'name': ['Jasmin', 'Nelson', 'Luis', 'Andres', 'Jairo'],
        'team': ['Analysis', 'Processing', 'Development', 'Manager', 'Arquitecture'],
    }
    data_frame = pd.DataFrame(data)
    result = data_frame.to_dict(orient='records')
    return render(request, 'demo_pandas.html', {'result': result})

def demo_geopandas(request):
    path_to_data = get_path("nybb")
    gdf = gpd.read_file(path_to_data)
    return render(request, 'demo_geopandas.html', {'gdf': gdf})

def demo_rasterio(request):
    with rasterio.open('.\static/backend\img\gadas.tif') as dataset:
        mask = dataset.dataset_mask()
        for geom, val in rasterio.features.shapes(
            mask, transform=dataset.transform):
            geom = rasterio.warp.transform_geom(
            dataset.crs, 'EPSG:4326', geom, precision=6)
            return render(request, 'demo_rasterio.html', {'geom': geom})

# ===#################################################################=== #

def demo_tqdm(request):
    # ps = []
    for i in tqdm(range(10), desc='Processing in view'):
        count = f'"Done processing. Check server logs for tqdm output", <h1>Iteration {i+1} of 10</h1>'
        time.sleep(0.5)
    return HttpResponse(count)

# ===#################################################################=== #
# ========          Import seguro de rpy2 conversion           ========== #

from rpy2.robjects import conversion
try:
    from rpy2.robjects.conversion import _converter as rpy2_converter  # para versiones nuevas
except ImportError:
    from rpy2.robjects import default_converter as rpy2_converter  # versiones antiguas

def demo_maxent(request, project_invias):
    
    """
    Vista para ejecutar el flujo completo de MaxEnt en un proyecto dado.
    Llama directamente a la clase MaxEntWorkflow y maneja el contexto de rpy2.
    """

    if request.method != "GET":
        return JsonResponse({"error": "Método no permitido. Usa GET."}, status=405)

    try:
        # Crear la instancia del flujo
        workflow = MaxEntWorkflow(project_name=project_invias)

        # Forzar el contexto de conversión de rpy2 (clave para evitar el error de contextvars)
        with conversion.localconverter(rpy2_converter):
            workflow.run()

        return JsonResponse({
            "status": "ok",
            "message": f"Proyecto {project_invias} procesado correctamente.",
            "result_path": os.path.join(settings.MEDIA_URL, "maxent_projects", project_invias, workflow.result_folder)
        })

    except Exception as e:
        traceback_str = traceback.format_exc()
        print(traceback_str)
        return JsonResponse({
            "status": "error",
            "message": str(e),
            "traceback": traceback_str
        }, status=500)


#========================================================#
# R libraries #
import rpy2.robjects as robjects
from rpy2.robjects import r, default_converter
from rpy2.robjects.conversion import localconverter
from django.http import HttpResponse

def demo_dismo(request):
    robjects.r('''
    set.seed(123)
    valores <- sample(1:100, 10)
    promedio <- mean(valores)
    desviacion <- sd(valores)
    ''')

    with localconverter(default_converter):
        valores = robjects.r['valores']
        promedio = robjects.r['promedio'][0]
        desviacion = robjects.r['desviacion'][0]

    resultado = f"""
    Lista de valores: {list(valores)}
    Promedio: {promedio}
    Desviación estándar: {desviacion}
    """

    # return HttpResponse(resultado)

    return render(request, 'R_cran/demo_dismo.html', {'resultado': resultado})


# https://copilot.microsoft.com/chats/Vjk6tHqi3JtriP2H1xZA3
#==========================================================#


# try:
#     ee.Initialize()
# except Exception as e:
#     ee.Authenticate()
#     ee.Initialize()

# Google Earth Engine #

# Inicializa Earth Engine
geemap.ee_initialize(project="complete-energy-448804-i2")

def demo_gee(request):
    # Crea el mapa sin inicializar Earth Engine (ya lo hicimos arriba)
    Map = geemap.Map(ee_initialize=False)
    
    # Puedes agregar capas aquí si lo deseas
    image = ee.Image('CGIAR/SRTM90_V4')
    vis_params = {'min': 0, 'max': 3000, 'palette': ['blue', 'green', 'red']}
    Map.addLayer(image, vis_params, 'SRTM Elevation')

    # Convierte el mapa en HTML
    map_html = Map.to_html()
    return render(request, 'gee/map.html', {'map_html': map_html})


# Arcgis demo
# def demo_arcgis(request):
#     layer_url = "https://storage.googleapis.com/invias/maps_invias/map.geojson"
#     return render(request, "gee/arcgis.html", {"layer_url": layer_url})

# import requests
# from django.http import JsonResponse
# # Arcgis demo
# def demo_arcgis(request):
#     layer_url = 'https://storage.googleapis.com/invias/maps_invias/map.geojson'
#     response = requests.get(layer_url)
#     if response.status_code == 200:
#         # return JsonResponse(response.json(), safe=False)
#         return render(request, 'gee/arcgis.html', {"layer_url": layer_url})
#     return JsonResponse({'error': 'No se pudo obtener el archivo'}, status=500)

# def demo_arcgis(request):
#     geojson_url = 'https://storage.googleapis.com/invias/maps_invias/map.geojson'
#     return render(request, 'gee/arcgis.html', {'geojson_url': geojson_url})


# import requests
# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# from django.views.decorators.http import require_GET
# from django.views.decorators.clickjacking import xframe_options_exempt

# @csrf_exempt
# @require_GET
# @xframe_options_exempt
# def demo_arcgis(request):
#     # URL del archivo GeoJSON en Google Cloud Storage
#     geojson_url = 'https://storage.googleapis.com/invias/maps_invias/map.geojson'

#     try:
#         response = requests.get(geojson_url)
#         if response.status_code == 200:
#             return JsonResponse(response.json(), safe=False)
#         else:
#             return JsonResponse({'error': 'No se pudo obtener el archivo'}, status=500)
#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=500)

import requests
from django.http import JsonResponse

def demo_arcgis(request, project_name):
    # url = "https://storage.googleapis.com/invias/maps_invias/geojson/mapa1.geojson"
    media_folder  = os.path.join(settings.MEDIA_ROOT, 'maxent_projects', project_name, 'RasterResult', 'salida.geojson')

    # response = requests.get(url)
    try:
        with open(media_folder, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # if response.status_code == 200:
                # data = response.json()
                # return JsonResponse(response.json(), safe=False)
            # print(data)
            # return render(request, 'gee/arcgis.html', {'data': data})
            return render(request, 'gee/arcgis.html', {'data': data})
        
    except Exception as e:
        return JsonResponse({"error": "No se pudo obtener el archivo str(e)"}, status=500)


def danger(request):
    # return render(request, 'danger.html')
    url = "https://storage.googleapis.com/invias/maps_invias/raster.tif"
    response = requests.get(url)
    if response.status_code == 200:
        return JsonResponse({"mensaje": "Archivo tiff leido"})
    return JsonResponse({"error": "No se pudo obtener el archivo"}, status=500)


def layer(request):
    url = "https://storage.googleapis.com/invias/maps_invias/raster.tif"
    return render(request, "layer.html", {"raster_url": url})


# from google.cloud import storage

# def leer_archivo(bucket_name, blob_name):
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(blob_name)
#     contenido = blob.download_as_text()
#     return contenido

# # Llamar la función y mostrar el resultado
# bucket_name = "invias"
# blob_name = "maps_invias/raster.tif"

# resultado = leer_archivo(bucket_name, blob_name)
# print("Contenido del archivo:")
# print(resultado)


# from google.cloud import storage
# from google.oauth2 import service_account

# # Ruta al archivo JSON
# ruta_credenciales = "C:\Program Files\Ampps\www\inviasvivo\inviasvivo\credentials\credentials.json"

# # Crear credenciales
# credentials = service_account.Credentials.from_service_account_file(ruta_credenciales)

# # Cliente con credenciales explícitas
# client = storage.Client(credentials=credentials)
# bucket = client.bucket("mi-bucket")
# blob = bucket.blob("https://storage.googleapis.com/invias/maps_invias/raster.tif")
# print(blob.download_as_text())


# def list_blobs(bucket_name):
#     """Lists all the blobs in the bucket."""
#     # bucket_name = "your-bucket-name"

#     storage_client = storage.Client()

#     # Note: Client.list_blobs requires at least package version 1.17.0.
#     blobs = storage_client.list_blobs(bucket_name)

#     # Note: The call returns a response only when the iterator is consumed.
#     for blob in blobs:
#         print(blob.name)


# list_blobs('invias')




# def mi_funcion():
#     print("La función se está ejecutando en este momento...")

# intervalo_en_segundos = 10 
# while True:
#     list_blobs()
#     time.sleep(intervalo_en_segundos)


# 
# def read_all_files_in_directory(bucket_name, prefix):
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)

#     blobs = bucket.list_blobs(prefix=prefix)

#     for blob in blobs:
#         print(f"\n=== Leyendo archivo: {blob.name} ===")

#         content = blob.download_as_text()
#         for line in content.splitlines():
#             print(line)

# read_all_files_in_directory("invias", "maps_invias/geojson/")



# def download(descarga_invias):
#     client = storage.Client()
#     bucket = client.bucket('invias')
#     # blob = bucket.blob('maps_invias/geojson/mapa1.geojson')
#     blob = bucket.blob(descarga_invias)
#     # content = blob.download_to_filename('nuevo_archivo.geojson')
#     content = blob.download_as_bytes()

#     ruta_destino = os.path.join(settings.MEDIA_ROOT, descarga_invias)
#     os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)

#     with open(ruta_destino, 'wb') as f:
#         f.write(content)

#     print("descargado con exito")


#     return ruta_destino


# download("maps_invias/geojson/mapa1.geojson")

# ========================================================================#
# ________________________________________________________________________#

# def download_bucket(bucket_name, prefix=None):
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blobs = bucket.list_blobs(prefix=prefix)

#     print(blobs)

#     for blob in blobs:
#         ruta = os.path.join(settings.MEDIA_ROOT, blob.name)
#         os.makedirs(os.path.dirname(ruta), exist_ok=True)

#         with open("maps_invias", "w") as f:
#             f.write(blob.download_to_filename(ruta))
#             print(ruta)



# print('si esta corriendo')       
# download_bucket('invias', prefix="maps_invias/geojson/")





# Tranformar de tiff a geojson

def tiff_geo(request, project_name):
    media_folder  = os.path.join(settings.MEDIA_ROOT, 'maxent_projects', project_name, 'RasterResult', 'resultado_maxent.tif')

    try:
        with rasterio.open(media_folder) as src:
            image = src.read(1)
            mask = image != 0

            results = (
                {'properties': {'value': v}, 'geometry': s}
                for s, v in shapes(image, mask=mask, transform=src.transform)
            )

            geoms = list(results)
            gdf = gpd.GeoDataFrame.from_features(geoms)

            output_file = os.path.join(settings.MEDIA_ROOT, 'maxent_projects', project_name, 'RasterResult', 'salida.geojson')

            gdf.to_file(output_file, driver="GeoJSON")

        return FileResponse(
            print('si funciona la descarga'),
            open(output_file, 'rb'),
            as_attachment=True,
            filename="salida.geojson",
            content_type="application/json"
        )
    
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
    



# Funciones de consumo de fuentes paralelas

def distances_way(request):
    data_url = "https://machine.domain.com/webadaptor/rest/services"
    response = request.get(data_url)
    return data_url
    
