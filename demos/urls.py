from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    #########################################################
    #==============   Interfaces de acceso  ================#
    #########################################################
    path("", views.sk_login, name="db_login"),
    path("logout/", views.sk_logout, name="db_logout"),
    path("menu", views.menu_auto, name="menu_automa"),
    #########################################################
    #==============   Interfaces de carga  =================#
    #########################################################
    path('usuario/generador-insumos', views.inputs_gee, name="inputs_gee"),
    path('gee-pipeline/run', views.run_gee_pipeline, name="gee_pipeline_run"),
    path("detener/pipeline/", views.stop_pipeline, name="stop_pipeline"),
    #########################################################
    #==============   Interfaces de servicios  =============#
    #########################################################
    path('usuario/servicios-paralelos', views.preprocess_actions, name="preprocess_actions"),
    path('pipeline/run', views.run_pipeline, name="run_pipeline"),
    path("update/sukubun/", views.dbSukubun, name="db_sukubun"),
    path("detener/services/", views.stop_services, name="stop_services"),
    #########################################################
    #==============   Interfaces de modelo  ================#
    #########################################################
    path('usuario/modelo/', views.process_actions, name="process_actions"),
    # path('maxent/run/', views.model_maxent, name="ejecutar_maxent"),
    path('central/process', views.run_centralModelProcess, name="run_centralprocess"),
    #########################################################
    #============   Interfaces de posproceso  ==============#
    #########################################################
    path('usuario/nuevomapa/', views.generate_map, name="postprocess_actions"),
    path('generatemap/run/', views.run_generateMap, name="generate_map"),
    path("tiff-geo/<str:project_name>/", views.tiff_geo, name="tiff_geo"),
    #########################################################
    #=============   Interfaces de descarga  ===============#
    #########################################################

    ###################### INTERFACES ########################

    ####################### ACCIONES #########################

    
    # ==================== MAXENT model ================== #
    path('ejecutar/', views.ejecutar_proceso),
  

    
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)