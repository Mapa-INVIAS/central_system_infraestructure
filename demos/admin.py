from django.contrib import admin
from .models import SukubunData

from django.contrib import admin
from django.utils.html import format_html

# Título en la barra azul
admin.site.site_header = 'Panel de administración INVIASVIVO'

# Título de la pestaña del navegador
admin.site.site_title = 'Administrador - INVIASVIVO'

# # # Título en la página principal (índice) del admin
# # admin.site.index_title = 'Bienvenido {{ user.username }} a la Administración'
# class MyAdminSite(admin.AdminSite):
#     index_title = "admin/custom_index.html"

# class MyAdminSite(admin.AdminSite):
    # site_header = "Administración"
    # site_title = "Panel de Administración"
    # index_title = "Bienvenido"

    # def each_context(self, request):
    #     context = super().each_context(request)
    #     context['index_title'] = format_html(
    #         "Bienvenido {} a la Administración",
    #         request.user.username
    #     )
    #     return context

# Usa tu AdminSite en lugar del default
# admin_site = MyAdminSite(name='myadmin')

class SukubunDataAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False
    
    def has_delete_permission(self, request, obj = None):
        return False

    def get_changelist_instance(self, request):
        cl = super().get_changelist_instance(request)
        cl.has_add_permission = False
        return cl
    
    def changeform_view(self, request, object_id=None, form_url='', extra_context=None):
        # Oculta el botón "Save and add another"
        extra_context = extra_context or {}
        extra_context['show_save_and_add_another'] = False
        return super().changeform_view(request, object_id, form_url, extra_context=extra_context)


# Register your models here.
admin.site.register(SukubunData, SukubunDataAdmin)
