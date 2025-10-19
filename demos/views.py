import time, os
from django.shortcuts import render
from django.http import JsonResponse
from django.http import HttpResponse
from django.conf import settings

#====================#
### Demo libraries ###
#====================#
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio as rs
from tqdm import tqdm

## Sample external data
from geodatasets import get_path
import rasterio.features
import rasterio.warp

# Create your views here.
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

def demo_tqdm(request):
    # ps = []
    for i in tqdm(range(10), desc='Processing in view'):
        count = f'"Done processing. Check server logs for tqdm output", <h1>Iteration {i+1} of 10</h1>'
        time.sleep(0.5)
    return HttpResponse(count)


#########################
import traceback
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render
from django.conf import settings
from .utils.maxent02 import MaxEntWorkflow  # importa tu clase
import os
#########################

# Import seguro de rpy2 conversion
from rpy2.robjects import conversion
try:
    from rpy2.robjects.conversion import _converter as rpy2_converter  # para versiones nuevas
except ImportError:
    from rpy2.robjects import default_converter as rpy2_converter  # versiones antiguas

def demo_maxent(request, project_name):
    
    """
    Vista para ejecutar el flujo completo de MaxEnt en un proyecto dado.
    Llama directamente a la clase MaxEntWorkflow y maneja el contexto de rpy2.
    """

    if request.method != "GET":
        return JsonResponse({"error": "Método no permitido. Usa GET."}, status=405)

    try:
        # Crear la instancia del flujo
        workflow = MaxEntWorkflow(project_name=project_name)

        # Forzar el contexto de conversión de rpy2 (clave para evitar el error de contextvars)
        with conversion.localconverter(rpy2_converter):
            workflow.run()

        return JsonResponse({
            "status": "ok",
            "message": f"Proyecto {project_name} procesado correctamente.",
            "result_path": os.path.join(settings.MEDIA_URL, "maxent_projects", project_name, workflow.result_folder)
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


