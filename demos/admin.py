from django.contrib import admin
from .models import SukubunData


# Título en la barra azul
admin.site.site_header = 'Panel de administración INVIASVIVO'

# Título en la página principal (índice) del admin
admin.site.index_title = 'Bienvenido {{ user.username }} a la Administración'

# Título de la pestaña del navegador
admin.site.site_title = 'Mi Admin - INVIASVIVO'

# Register your models here.
admin.site.register(SukubunData)



