from django.shortcuts import render


# Create your views here.

def mapInterface(request):
    return render(request, 'map.html')



