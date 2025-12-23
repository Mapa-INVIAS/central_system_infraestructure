from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    #======= MAXENT model ===========#
    # path('maxent/<str:project_name>/', views.demo_maxent, name="ejecutar_maxent"),
    path('maxent/run/', views.demo_maxent, name="ejecutar_maxent"),
    #===== R libraries ======#
    #===== Google Earth Engine =====#
    path('gee/map/', views.demo_gee),
    # path('arc-gis/<str:project_name>/', views.demo_arcgis, name='demo_arcgis'),
    path('real/danger', views.danger),
    #===============================#
    path('real/tiff', views.layer),

    # Preprocesor functions and load data
    # ============================================================= #
    # Generador de insumos
    path("gee/run-s2/", views.run_s2),
    path('exports/gee-s2', views.download_exports, name='download_exports'),
    path('buffer-invias/', views.generar_buffer_invias, name='buffer_invias'),
    path('pipeline/run', views.run_pipeline, name="run_pipeline"),
    path('kripley/run', views.runHotRipley, name="run_kripley"),
    path('makemosaic/run', views.run_mosaic_nacional_view, name="run_mosaic"),
    ################################################################ 
    # path("run/", views.run_mosaics_page, name="run_mosaics_page"),
    # path("status/<uuid:task_id>/", views.mosaics_status_page, name="mosaics_status_page"),
    # 
    path("pipeline/", views.run_pipeline, name="run_pipeline"),
    # ============================================================== #

    path("db/login/", views.sk_login, name="db_login"),
    path("db/sukubun/", views.dbSukubun, name="db_sukubun"),

    path("tiff-geo/<str:project_name>/", views.tiff_geo, name="tiff_geo"),
    
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)