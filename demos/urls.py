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
    path('gee/map/', views.demo_gee),
    path('arc-gis/<str:project_name>/', views.demo_arcgis, name='demo_arcgis'),
    path('real/danger', views.danger),
    #===============================#
    path('real/tiff', views.layer),
    path('exports', views.download_exports, name='download_exports'),

    path("run/", views.run_mosaics_page, name="run_mosaics_page"),
    path("status/<uuid:task_id>/", views.mosaics_status_page, name="mosaics_status_page"),
    path("s2/", views.run_s2, name="run_s2"),

    path('tiff-geo/<str:project_name>/', views.tiff_geo, name='tiff_geo'),
    
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)