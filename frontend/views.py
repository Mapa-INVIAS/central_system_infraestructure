from django.shortcuts import render


# Create your views here.
def landinPage(request):
    return render(request, 'landing.html')

def mapInterface(request):
    return render(request, 'map.html')



