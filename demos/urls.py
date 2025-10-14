from django.urls import path
from . import views

urlpatterns = [
    #===== Python libraries =====#
    path('numpy', views.demo_numpy, name='view_numpy'),
    path('geopandas', views.demo_geopandas),
    path('pandas', views.demo_pandas),
    path('rasterio', views.demo_rasterio),
    path('tqdm', views.demo_tqdm),
    #===== R libraries ======#
    path('rcran/dismo', views.demo_dismo),

    #===== Google Earth Engine =====#
    
]