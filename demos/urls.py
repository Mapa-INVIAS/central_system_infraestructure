from django.urls import path
from . import views

urlpatterns = [
    path('numpy', views.demo_numpy, name='view_numpy'),
    path('geopandas', views.demo_geopandas),
    path('pandas', views.demo_pandas),
    path('rasterio', views.demo_rasterio),
    path('tqdm', views.demo_tqdm),

]