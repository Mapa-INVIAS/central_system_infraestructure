from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    #===== Python libraries =====#
    path('numpy', views.demo_numpy, name='view_numpy'),
    path('geopandas', views.demo_geopandas),
    path('pandas', views.demo_pandas),
    path('rasterio', views.demo_rasterio),
    path('tqdm', views.demo_tqdm),
    path('maxent/<str:project_name>/', views.demo_maxent, name="ejecutar_maxent"),
    #===== R libraries ======#
    path('rcran/dismo', views.demo_dismo),

    #===== Google Earth Engine =====#
    
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)