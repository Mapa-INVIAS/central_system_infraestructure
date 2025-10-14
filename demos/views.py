import time
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
    with rasterio.open('.\static\img\gadas.tif') as dataset:
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

#========================================================#
# R libraries #
import rpy2.robjects as robjects
from django.shortcuts import render
import io
import sys
from rpy2.robjects.packages import importr

def demo_dismo(request):
    output_data = ""
    try:
        # Importar paquete stats
        stats = importr('stats')

        # Crear vector en R
        robjects.r('numeros <- c(10, 15, 20, 25, 30)')

        # Calcular media y desviación estándar usando funciones del paquete stats
        # media = robjects.r('mean(numeros)')[0]
        # desviacion = stats.sd(robjects.r('numeros'))[0]

        output_data = f"Lista de números: 10, 15, 20, 25, 30\nMedia: \nDesviación estándar:"

    except Exception as e:
        output_data = f"Error al ejecutar código R: {e}"

    return render(request, 'R_cran/demo_dismo.html', {'output_data': output_data})