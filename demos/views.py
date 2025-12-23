import time, os, schedule, json, rasterio, traceback, ee, geemap, requests, json, uuid
from django.shortcuts import render, redirect
from django.http import JsonResponse, FileResponse, HttpResponseNotAllowed
from django.http import HttpResponse
from django.conf import settings
from google.cloud import storage
from google.cloud.storage import transfer_manager
from rasterio.features import shapes
from django.views.decorators.csrf import csrf_exempt
from .utils.maxentModel02 import MaxEntWorkflow  # importa tu clase
from .utils.downloadInputsMaxent import download_latest_exports
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from .forms import sukubunForm

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
from io import BytesIO
from pathlib import Path
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

# list_blobs(bucket_name)


###############################################################################################################################
# =========================================================================================================================== #
#
# PREPROCESOS
#
# =========================================================================================================================== #
###############################################################################################################################


from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .utils.ee_init import init_ee
from .utils.exportTiles import run_s2_export


@csrf_exempt
@require_POST
def run_s2(request):
    try:
        init_ee()   

        body = json.loads(request.body.decode()) if request.body else {}

        result = run_s2_export(
            limit_zones=body.get("limit_zones"),
            dry_run_tiles=body.get("dry_run_tiles")
        )

        return JsonResponse(result, status=200)

    except Exception as e:
        return JsonResponse(
            {
                "status": "error",
                "detail": str(e),
                "hint": "Verifique permisos del Service Account en IAM"
            },
            status=500
        )


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


# from django.contrib.admin.views.decorators import staff_member_required
# from .tasks import run_mosaics_task
# from celery.result import AsyncResult


# @staff_member_required
# def run_mosaics_page(request):
#     """
#     Página web para lanzar el procesamiento
#     """
#     if request.method == "POST":
#         task = run_mosaics_task.delay()
#         return redirect("mosaics_status_page", task_id=task.id)

#     return render(request, "mosaic.html")



# @staff_member_required
# def mosaics_status_page(request, task_id):
#     result = AsyncResult(task_id)

#     return render(request, "mosaicstatus.html", {
#         "task_id": task_id,
#         "state": result.state,
#         "result": result.result
#     })


from .utils.makeMosaicInputs import full_mosaic_nacional

@csrf_exempt
@require_POST
def run_mosaic_nacional_view(request):
    payload = json.loads(request.body or "{}")

    exports_dir = Path(settings.MEDIA_ROOT) / "EXPORTS"

    results = full_mosaic_nacional(
        exports_dir=exports_dir,
        run_s2=payload.get("run_s2", True),
        run_hansen=payload.get("run_hansen", True),
        run_srtm=payload.get("run_srtm", True),
    )

    return JsonResponse({"status": "ok", "outputs": results})



########################################################################
########################################################################

# Complete download of bucket data
def download_bucket_with_transfer_manager(
    bucket_name, workers=8, max_results=None, prefix=None):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(max_results=max_results, prefix=prefix)
    blob_names = [blob.name for blob in blobs]

    # destination_directory = settings.MEDIA_ROOT

    destination_directory = os.path.join(settings.MEDIA_ROOT, 'preprocess')
    os.makedirs(destination_directory, exist_ok=True)

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


########################################################################
########################################################################

# Proceso de descarga de capas paralelas
from .utils.parallelServices import pipeline_process

from pathlib import Path
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def run_pipeline(request):
    base_dir = Path(settings.BASE_DIR) / "static" / "backend" / "geodata"
    input_name = base_dir / "CapaReferencia" / "Colombia.geojson"

    output_dir = Path(settings.MEDIA_ROOT) / "AutoINVIAS"

    if not input_name.exists():
        return JsonResponse(
            {"status": "error", "message": f"No se encontró {input_name}"}, status=404
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        pipeline_process(output_dir, input_name)
    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)}, status=500
        )

    return JsonResponse(
        {"status": "ok", "message": "Pipeline ejecutado"}
    )



# ==================================================================== #
# #################################################################### #
   
#========================================================#
# R libraries #
import rpy2.robjects as robjects
from rpy2.robjects import r, default_converter
from rpy2.robjects.conversion import localconverter
from django.http import HttpResponse



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

    
#########################################################################
# Funciones de consumo de fuentes paralelas

def distances_way(request):
    data_url = "https://machine.domain.com/webadaptor/rest/services"
    response = request.get(data_url)
    return data_url
    
# ================================================================== #
# login user
def sk_login(request):
    return render(request, "login.html")

# Sukubun database
def dbSukubun(request):
    if request.method == "POST":
        form = sukubunForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('db_sukubun')
    else:
        form = sukubunForm()
    return render(request, "sukubun.html", {"form": form})


# ===================================================================== #
#########################################################################
# K-riplay

from .utils.kripley02 import KRipley_HS

@csrf_exempt
@require_POST
def runHotRipley(request):

    try:
        # payload = json.loads(request.body.decode("utf-16"))

        if not request.body:
            return JsonResponse({"status": "error", "detail": "El cuerpo de la petición está vacío"}, status=400)

        try:
            payload = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError as e:
            return JsonResponse({"status": "error", "detail": f"JSON inválido: {str(e)}"}, status=400)

        # ============================
        # PARÁMETROS OBLIGATORIOS
        # ============================

        media_root = Path(settings.MEDIA_ROOT)
        uploads_folder = media_root / "uploads"

        excel_files = list(uploads_folder.glob("*.xlsx")) + list(uploads_folder.glob("*.xls"))

        if not excel_files:
            raise FileNotFoundError(f"No se encontró ningún archivo Excel en {uploads_folder}")

        excel_path = max(excel_files, key=lambda f: f.stat().st_mtime)

        print("Último Excel encontrado:", excel_path)

        print("excelpath")
        print(excel_path)

        roads_path  = media_root / "Vias_Total" /"Vias_Total.shp"

        if not excel_path.exists():
            raise FileNotFoundError(f"No existe el Excel: {excel_path}")

        if not roads_path.exists():
            raise FileNotFoundError(f"No existe el SHP de vías: {roads_path}")

        excel_sheet = payload.get("excel_sheet", "SUKUBUN")
        lat_field   = payload.get("lat_field", "y")
        lon_field   = payload.get("lon_field", "x")

        # ============================
        # PARÁMETROS NUMÉRICOS
        # ============================
        simplify_tolerance_m = float(payload.get("simplify_tolerance_m", 1.0))
        precision_scale      = float(payload.get("precision_scale", 0.001))
        segment_spacing_m    = float(payload.get("segment_spacing_m", 50.0))
        snap_tolerance_m     = float(payload.get("snap_tolerance_m", 90.0))
        r_start_m            = float(payload.get("r_start_m", 100.0))
        r_step_m             = float(payload.get("r_step_m", 500.0))

        n_sim_ripley  = int(payload.get("n_sim_ripley", 100))
        n_sim_hotspot = int(payload.get("n_sim_hotspot", 100))
        random_seed   = int(payload.get("random_seed", 321))

        hs_point_spacing_m   = float(payload.get("hs_point_spacing_m", 50.0))
        n_workers            = int(payload.get("n_workers", 2))
        max_hs_sample_points = payload.get("max_hs_sample_points")

        plot_png = bool(payload.get("plot_png", True))

        # ============================
        # OUTPUT
        # ============================
        run_id = uuid.uuid4().hex[:8]

        output_folder = (
            Path(settings.MEDIA_ROOT)
            / "kripley_runs"
            / run_id
        )
        output_folder.mkdir(parents=True, exist_ok=True)

        # ============================
        # NOMBRES DE ARCHIVOS
        # ============================
        export_csv_hotspots_name = "hotspots.csv"
        export_csv_ripley_name = "ripley_L.csv"
        export_shp_vias_colapsadas_name = "vias_simplificadas.shp"

        # ============================
        # EJECUCIÓN REAL
        # ============================
        KRipley_HS(
            excel_path,
            excel_sheet,
            lat_field,
            lon_field,
            roads_path,
            str(output_folder),
            simplify_tolerance_m,
            precision_scale,
            segment_spacing_m,
            snap_tolerance_m,
            r_start_m,
            r_step_m,
            n_sim_ripley,
            random_seed,
            n_sim_hotspot,
            hs_point_spacing_m,
            export_csv_hotspots_name,
            export_csv_ripley_name,
            export_shp_vias_colapsadas_name,
            plot_png,
            n_workers,
            max_hs_sample_points
        )

        return JsonResponse({
            "status": "ok",
            "run_id": run_id,
            "output_folder": str(output_folder),
            "outputs": {
                "ripley": export_csv_ripley_name,
                "hotspots": export_csv_hotspots_name,
                "vias": export_shp_vias_colapsadas_name,
                "metadata": "metadata.json"
            }
        })

    except Exception as e:
        return JsonResponse(
            {
                "status": "error",
                "detail": str(e)
            },
            status=500
        )



##########################################################################
# ====================================================================== #
# Red vial de colombia INVIAS

def generar_buffer_invias(request):

    url = "https://storage.googleapis.com/invias/maps_invias/dem_colombia/RedVialODAGOL_-7622711643947703228.geojson"
    carpeta_salida = Path(settings.MEDIA_ROOT) / "viasINVIAS"
    nombre_salida = "vias_invias.shp"

    carpeta_salida.mkdir(parents=True, exist_ok=True)
    shp_path = carpeta_salida / nombre_salida

    try:
        # Descargar y leer
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        gdf = gpd.read_file(BytesIO(response.content))

        # Reproyectar a metros
        gdf = gdf.to_crs(epsg=9377)

        # Buffer + dissolve
        gdf["geometry"] = gdf.buffer(200)
        gdf = gdf.dissolve()

        # Reproyectar a WGS84 y guardar shapefile
        gdf = gdf.to_crs(epsg=4326)
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        # Construir URL pública
        archivo_url = settings.MEDIA_URL + f"INVIAS/{nombre_salida}"

        return JsonResponse({
            "status": "ok",
            "mensaje": "Shapefile creado exitosamente",
            "archivo": archivo_url
        })

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "mensaje": str(e)
        }, status=500)


###############################################################################################################################
# =========================================================================================================================== #
#
# PROCESO
#
# =========================================================================================================================== #
###############################################################################################################################

from rpy2.robjects import conversion
try:
    from rpy2.robjects.conversion import _converter as rpy2_converter  # para versiones nuevas
except ImportError:
    from rpy2.robjects import default_converter as rpy2_converter  # versiones antiguas

from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter

@csrf_exempt
def demo_maxent(request):
    """
    Ejecuta MaxEnt para todas las regiones encontradas en MEDIA_ROOT/jackknife.
    Cada carpeta = una región = una corrida de MaxEnt.
    Uso: GET /demos/maxent/run_safe/
    """

    if request.method != "GET":
        return HttpResponseNotAllowed(["GET"])

    try:
        jackknife_root = os.path.join(settings.MEDIA_ROOT, "jacknife")

        if not os.path.isdir(jackknife_root):
            return JsonResponse(
                {"status": "error", "message": "No existe la carpeta jackknife"},
                status=404
            )

        # Detectar todas las regiones
        regiones = [
            d for d in os.listdir(jackknife_root)
            if os.path.isdir(os.path.join(jackknife_root, d))
        ]

        if not regiones:
            return JsonResponse(
                {"status": "error", "message": "No hay regiones dentro de jackknife"},
                status=400
            )

        resultados = {}

        # =============================== #
        # Ejecutar workflow SECUENCIAL    #
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

        return JsonResponse({
            "status": "ok",
            "regiones_procesadas": regiones,
            "resultados": resultados
        })

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }, status=500)


###############################################################################################################################
# =========================================================================================================================== #
#
# POSPROCESOS
#
# =========================================================================================================================== #
###############################################################################################################################

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