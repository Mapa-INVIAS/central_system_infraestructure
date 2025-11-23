from django.urls import path
from . import views

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('menu', views.menu, name="menu_index"),
    path('grayfilter', views.gray_filter, name="filter_gray"),
    path('colorfilter', views.color_filter, name="filter_color"),
]

# if settings.DEBUG:
#     urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)