import time, os, json, rasterio, traceback, ee, requests, json
import geopandas as gpd
import logging
logger = logging.getLogger(__name__)
###############################################################
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from .forms import SukubunForm
from rasterio.features import shapes
from .models import SukubunData
###############################################################
from django.shortcuts import render, redirect
from django.http import JsonResponse, FileResponse, HttpResponseNotAllowed, HttpResponse
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.urls import reverse
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.http import require_POST
#################################################################
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter
#################################################################
from .utils.model.maxentModel02 import MaxEntWorkflow
from .utils.parallelServices import pipeline_process
from .utils.googleServices import gee_pipeline
from .utils.control import set_stop_pipeline

ULR = settings.BOT_URL
TOKEN = settings.BOT_TOKEN
CHAT_ID = settings.BOT_CHAT_ID

def send_telegram_message(msg):
    # Aquí tu implementación real de envío a Telegram
    print("Telegram:", msg)

def monitor_django(url="http://127.0.0.1:8000", interval=60):
    while True:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                print("Django OK")
            else:
                send_telegram_message(f"⚠️ Django respondió con código {r.status_code}")
        except requests.exceptions.RequestException:
            send_telegram_message("❌ Django no responde (posible caída)")
        time.sleep(interval)

if __name__ == "__main__":
    monitor_django()

def send_telegram_message(text, chat_id=None):
    token = TOKEN
    chat_id = CHAT_ID
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    response = requests.post(url, data=payload)
    return response.json()

def vector_dividido():
    # Generar lista de números del 1 al 100
    numeros = list(range(1, 1000))
    time.sleep(15)
    # Dividir cada número entre 5
    resultado = [n / 5 for n in numeros]
    return resultado

def proceso_largo():
    # Simulación de un proceso que tarda
    # time.sleep(5)
    vector_dividido()
    resultado = "Proceso terminado con éxito ✅"
    # Notificar al finalizar
    mensaje = f"La función proceso_largo finalizó: {resultado}\n\n[Enlace para continuar con el proceso](https://http://127.0.0.1:8000/demos/ejecutar/)" 
    send_telegram_message(mensaje)
    return resultado

def ejecutar_proceso(request):
    resultado = proceso_largo()
    return HttpResponse(f"Resultado: {resultado}")


#=========================================================================#
#=========================================================================#
#                                                                         #
##                      CENTRAL PROCESS SYSTEM                           ##
#                                                                         #
#=========================================================================#
#=========================================================================#

# login user
def sk_login(request):
    if request.method == "POST":
        username = request.POST['username']
        password = request.POST['password']
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('menu_automa')
        else:
            messages.error(request, 'Credenciales inválidas')
    return render(request, "login.html")

def sk_logout(request):
    logout(request)
    return redirect('db_login')

# Menu INVIASVIVO
@login_required(login_url='db_login', redirect_field_name=None)
def menu_auto(request):
    return render(request, 'menu.html')


# ======================================================================= #
#
# SISTEMA DE NOTIFICACIONES
#
# ======================================================================= #
###########################################################################

@login_required(login_url='db_login', redirect_field_name=None)
def safe_send_telegram(message: str):
    try:
        send_telegram_message(message)
    except requests.exceptions.ConnectionError:
        # Error específico de conexión
        print("Error: conexión interrumpida al enviar mensaje a Telegram.")
        return False
    except Exception as e:
        print(f"Error inesperado al enviar mensaje a Telegram: {e}")
        return False
    return True

###########################################################################
# ======================================================================= #
#
# PROCESO DE CARGA
#
# ======================================================================= #
###########################################################################

@login_required(login_url='db_login', redirect_field_name=None)
def inputs_gee(request):
    return render(request, 'inputs_gee.html')


@login_required(login_url='db_login', redirect_field_name=None)
def run_gee_pipeline(request):
    if request.method == 'POST':
        try:

            try:
                body = json.loads(request.body.decode())
            except json.JSONDecodeError:
                body = {}
            
            MAX_CONCURRENT = int(request.POST.get("MAX_CONCURRENT", body.get("MAX_CONCURRENT", 3)))
            PERIOD_AVERAGE = request.POST.get("PERIOD_AVERAGE", body.get("PERIOD_AVERAGE", 3))


            results = gee_pipeline(body, MAX_CONCURRENT, PERIOD_AVERAGE)

            resultado = "Proceso terminado con éxito ✅"
            # Notificar al finalizar 

            success_message = (
                f"Pipeline ha finalizado con éxito: {resultado}\n\n"
                f"[Enlace para continuar con el proceso](https://http://127.0.0.1:8000/demos/user/preprocess)" 
            )

            send_telegram_message(success_message)

            return JsonResponse({"status": "ok", "steps": results}, status=200)
        except Exception as e:

            error_message = (
                f"❌ Pipeline ha fallado por str{e}.\n\n"
                f"Intente de nuevo."
            )

            send_telegram_message(error_message)

            return JsonResponse({"status": "error", "detail": str(e)}, status=500)
    else:
        resultado = "Proceso terminado con éxito ✅"
        success_message = (
                f"Pipeline ha finalizado con éxito: {resultado}\n\n"
                f"[Enlace para continuar con el proceso](https://http://127.0.0.1:8000/demos/usuario/preprocess)" 
            )

        send_telegram_message(success_message)
        return JsonResponse({"status": "error", "detail": "Método no permitido"}, status=405)


@login_required(login_url='db_login', redirect_field_name=None)
def stop_pipeline(request):
    logger.warning("STOP solicitado desde UI")
    set_stop_pipeline(True)

    # Cancelar tareas activas en Earth Engine
    cancelled = 0
    try:
        for t in ee.batch.Task.list():
            if t.active():
                t.cancel()
                cancelled += 1

        success_message = (
            f"✅ Proceso cancelado con éxito.\n\n"
            f"Inicie nuevamente el proceso.\n\n"
            f"https://http://127.0.0.1:8000/demos/usuario/generador-insumos"
        )

        send_telegram_message(success_message)
        logger.warning(f"Se cancelaron {cancelled} tareas activas de Earth Engine")
    except Exception as e:
        logger.error(f"Error al cancelar tareas: {e}")

        error_message = (
            f"❌ Pipeline ha fallado por str{e}.\n\n"
            f"Intente de nuevo."
        )

        send_telegram_message(error_message)

    return JsonResponse({
        "status": "Ha detenido el proceso",
        "cancelled_tasks": cancelled
    })


###########################################################################
# ======================================================================= #
#
# SERVICIOS PARALELOS
#
# ======================================================================= #
###########################################################################

@login_required(login_url='db_login', redirect_field_name=None)
@csrf_exempt
def run_pipeline(request):
    base_dir = Path(settings.BASE_DIR) / "static" / "backend" / "geodata"
    input_name = base_dir / "CapaReferencia" / "Colombia.geojson"

    output_dir = Path(settings.MEDIA_ROOT, 'modula_servicios', 'autoINVIAS')

    if not input_name.exists():
        
        safe_send_telegram( 
            f"❌ Error: No se encontró el archivo\n\n {input_name}" 
        )

        return JsonResponse(
            {"status": "error", "message": f"No se encontró {input_name}"}, status=404
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # ============================================== #
    #                 Notificación                   #
    # ============================================== #

    link = reverse("preprocess_actions")
    domain = "http://127.0.0.1:8000/"
    full_url = f"{domain}{link}"

    try:
        pipeline_process(output_dir, input_name)

        message = (
            f"Los servicios paralelos han culminado.\n\n"
            f"Se ha generado el jacknife. \n\n"
            f"De forma exitosa. \n\n"
            f"Continue el proceso en ({full_url})"
        )

        safe_send_telegram(message)

    except Exception as e:

        error_message = (
            f"❌ El proceso pipeline falló.\n\n"
            f"Intente enviarlo nuevamente.\n\n"
            f"Si falla nuevamente notifique al personal técnico. \n\n"
            f"Error: {str(e)}"
        )

        safe_send_telegram(error_message)

        return JsonResponse(
            {"status": "error", "message": str(e)}, status=500
        )

    return JsonResponse(
        {"status": "ok", "message": "Pipeline ejecutado"}
    )


@login_required(login_url='db_login', redirect_field_name=None)
def stop_services(request):
    try:
        logger.warning("STOP de servicios desde UI")

        success_message = (
            f"✅ Proceso cancelado con éxito.\n\n"
            f"Inicie nuevamente el proceso.\n\n"
            f"https://http://127.0.0.1:8000/demos/usuario/servicios-paralelos"
        )

        send_telegram_message(success_message)
        set_stop_pipeline(True)

        return JsonResponse({"status": "Servicios detenidos"})
    
    except Exception as e:

        logger.error(f"❌ Error al detener los servicios: {e}")
        return JsonResponse(
            {
                "status": "error",
                "detail": str(e)
            }, status=500
        )


# try:
#     ee.Initialize()
# except Exception as e:
#     ee.Authenticate()
#     ee.Initialize()


###########################################################################
# ======================================================================= #
#
# FUNCIONES DE PREPROCESOS
#
# ======================================================================= #
###########################################################################

# Sukubun database
import os

@login_required(login_url='db_login', redirect_field_name=None)
def preprocess_actions(request):

    today = date.today() 
    tomorrow = today + timedelta(days=1) 
    last_sukubun = SukubunData.objects.order_by("-update_at").first()

    

    sukubun_files = SukubunData.objects.all()
    return render(request, 'parallel_services.html', {
        'sukubun': sukubun_files,
        'last_sukubun': last_sukubun,
        'today': today,
        'tomorrow': tomorrow,
        })


@login_required(login_url='db_login', redirect_field_name=None)
def dbSukubun(request):
    # Buscar el último registro actualizado
    sukubun = SukubunData.objects.order_by("-update_at").first()

    if request.method == "POST":
        form = SukubunForm(request.POST, request.FILES, instance=sukubun)
        if form.is_valid():
            form.save() 
            return redirect('preprocess_actions')
    else:
        form = SukubunForm(instance=sukubun)

    return render(request, "parallel_services.html", {"form": form})


###########################################################################
# ======================================================================= #
#
# FUNCIONES DE PROCESO
#
# ======================================================================= #
###########################################################################

from rpy2.robjects import conversion
try:
    from rpy2.robjects.conversion import _converter as rpy2_converter  # para versiones nuevas
except ImportError:
    from rpy2.robjects import default_converter as rpy2_converter  # versiones antiguas

from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter

from .utils.centralProcess import centralModelProcess
# @csrf_exempt
@login_required(login_url='db_login', redirect_field_name=None)
@require_POST

# def model_maxent(request):

#     if request.method != "POST":
#         return HttpResponseNotAllowed(["POST"])

#     try:
#         jackknife_root = os.path.join(settings.MEDIA_ROOT, "jacknife")

#         if not os.path.isdir(jackknife_root):
#             error_message = (
#                     f"❌ El sistema no encontro la carpeta jacknife verifique en \n\n" 
#                     f"el sistema si existe.\n\n"
#                     f"status=404"
#                 )

#             send_telegram_message(error_message)
#             return JsonResponse(
#                 {"status": "error", "message": "No existe la carpeta jacknife"},
#                 status=404
#             )

#         # Detectar todas las regiones
#         regiones = [
#             d for d in os.listdir(jackknife_root)
#             if os.path.isdir(os.path.join(jackknife_root, d))
#         ]

#         if not regiones:
#             return JsonResponse(
#                 {"status": "error", "message": "No hay regiones dentro de jacknife"},
#                 status=400
#             )

#         resultados = {}

#         # =============================== #
#         # Ejecutar workflow SECUENCIAL    #
#         # =============================== #
#         for region in regiones:
#             try:
#                 workflow = MaxEntWorkflow(project_name=region)
#                 # Garantizar contexto de rpy2 dentro del mismo thread
#                 with localconverter(default_converter):
#                     workflow.run()
#                 resultados[region] = "OK"
#             except Exception as e:

#                 error_message = (
#                     f"❌ Fallo el proceso de Maxent model. \n\n"
#                     f"Error: {str(e)}"
#                 )

#                 send_telegram_message(error_message)

#                 resultados[region] = f"ERROR: {str(e)}"

#         # ------------------------------- # 
#         #          Notificación           # 
#         # ------------------------------- #

#         enlace = reverse("process_actions")
#         dominio = "http://127.0.0.1:8000"
#         url_completa = f"{dominio}{enlace}"

#         mensaje = ( 
#             f"✅ El proceso MaxEnt finalizó.\n\n" 
#             f"Regiones procesadas: {', '.join(regiones)}\n" 
#             f"Resultados: {resultados}\n\n" 
#             f"Dirijase a descargar los resultados en ({url_completa})" )
        
#         send_telegram_message(mensaje)

#         return JsonResponse({
#             "status": "ok",
#             "regiones_procesadas": regiones,
#             "resultados": resultados
#         })

#     except Exception as e:

#         error_message = (
#                     f"❌ Fallo el proceso de Maxent model. \n\n"
#                     f"Error: {str(e)}"
#                 )

#         send_telegram_message(error_message)

#         return JsonResponse({
#             "status": "error",
#             "message": str(e),
#             "traceback": traceback.format_exc()
#         }, status=500)


def run_centralModelProcess(request):
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    
    try:
        centralModelProcess()
        return JsonResponse({
            'status': 'se ejecuto maxent'
        })
    
    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }, status=500)


###########################################################################
# ======================================================================= #
#
# FUNCIONES DE POSPROCESOS
#
# ======================================================================= #
###########################################################################
from .utils.generateMap import PostProcesamientoMaxEnt

def run_generateMap(request):

    today = date.today()

    base_dir = Path(settings.MEDIA_ROOT)
    # Directorios base
    maxent_dir = Path(base_dir, 'modula_proceso', 'maxent_invias')
    jacknife_dir = Path(base_dir, 'modula_proceso', 'jacknife')

    # Lista de regiones
    regiones = ["a_Amazonas", "b_Orinoquia", "c_Pacifico", "d_Caribe", "e_Andina"]

    # Parámetros
    min_area_m2 = 100
    campo_clase = "clase_prob"
    campo_rango = "rango_prob"

    resultados = []

    for region in regiones:
        try:

            print("================================")
            print(f"==Procesando región: {region}==")
            print("================================")

            ruta_raster_maxent = os.path.join(maxent_dir, region, "RasterResult", "resultado_maxent.tif")
            ruta_vias = os.path.join(jacknife_dir, region, "vias.shp")
            # carpeta_salida = os.path.join(base_dir,"nuevo_mapa", region)
            carpeta_salida = os.path.join(base_dir, 'modula_postproceso', region)
            # carpeta_salida = Path(base_dir, 'modula_postproceso', f'nuevo_mapa_{today}', region)
            # carpeta_salida.mkdir(parents=True, exist_ok=True)

            PostProcesamientoMaxEnt(
                ruta_raster_maxent,
                ruta_vias,
                carpeta_salida,
                min_area_m2,
                campo_clase,
                campo_rango
            )

            resultados.append({
                "region": region,
                "status": "ok",
                "output_dir": carpeta_salida
            })

        except Exception as e:
            resultados.append({
                "region": region,
                "status": "error",
                "message": str(e)
            })

    return JsonResponse({"resultados": resultados})


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


# ==================================================================== #
# ==================================================================== #

# User Visual interfaces

def process_actions(request):
    return render(request, 'new_map.html')

def generate_map(request):
    return render(request, 'data_map.html')

import os
import shutil
from django.http import FileResponse, Http404
import datetime

def listdownload_map(request):
    media_root = Path(settings.MEDIA_ROOT, 'modula_postproceso')

    submedia_directories    = []

    for directory in os.listdir(media_root):
        if not directory.strip():
            continue
        ruta = os.path.join(media_root, directory)
        if os.path.isdir(ruta):
            fecha_actualizada = datetime.datetime.fromtimestamp(os.path.getmtime(ruta))
            contenido = os.listdir(ruta)
            submedia_directories.append(
                {
                    'name': directory,
                    'update': fecha_actualizada.strftime("%Y-%m-%d"),
                    'uptime': fecha_actualizada.strftime("%H:%M:%S"),
                    'upcontent': contenido
                }
            )

    return render(request, 'downloadmap.html', {"submedia_directories": submedia_directories})


def download_map(request, name_subdir):
    media_root = Path(settings.MEDIA_ROOT, 'modula_postproceso')
    ruta = os.path.join(media_root, name_subdir)

    if not os.path.exists(ruta):
        raise Http404(f'No existe el directorio {name_subdir}')
    
    zip_file = f"/tmp/{name_subdir}.zip" 
    shutil.make_archive(zip_file.replace(".zip", ""), 'zip', ruta)
    
    return FileResponse(
        open(zip_file, 'rb'),
        as_attachment=True,
        filename=f'{name_subdir}.zip'
    )
