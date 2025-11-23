import numpy as np
import matplotlib.pyplot as plt
from skimage import io, color, img_as_float
from django.shortcuts import render
from django.core.files.storage import default_storage
from django.conf import settings
import os
from numba import njit
from io import BytesIO
import base64
from scipy import ndimage
from django.http import HttpResponse

def menu(request):
    return render('menu', 'menu.html')


# Gray filter
@njit
def perona_malik(u, kappa, delta_t, option):
    nablaN = np.zeros_like(u)
    nablaS = np.zeros_like(u)
    nablaE = np.zeros_like(u)
    nablaW = np.zeros_like(u)

    nablaN[:-1, :] = u[1:, :] - u[:-1, :]
    nablaS[1:, :] = u[:-1, :] - u[1:, :]
    nablaE[:, :-1] = u[:, 1:] - u[:, :-1]
    nablaW[:, 1:] = u[:, :-1] - u[:, 1:]

    if option == 1:
        cN = np.exp(-(nablaN/kappa)**2)
        cS = np.exp(-(nablaS/kappa)**2)
        cE = np.exp(-(nablaE/kappa)**2)
        cW = np.exp(-(nablaW/kappa)**2)
    else:
        cN = 1 / (1 + (nablaN/kappa)**2)
        cS = 1 / (1 + (nablaS/kappa)**2)
        cE = 1 / (1 + (nablaE/kappa)**2)
        cW = 1 / (1 + (nablaW/kappa)**2)

    return u + delta_t * (cN * nablaN + cS * nablaS + cE * nablaE + cW * nablaW)


def gray_filter(request):
    if request.method == "POST":
        imagen = request.FILES["imagen"]
        ruta = default_storage.save(imagen.name, imagen)
        path = os.path.join(default_storage.location, ruta)

        # parámetros desde formulario
        n_iter = int(request.POST.get("n_iter", 20))
        delta_t = float(request.POST.get("delta_t", 0.15))
        kappa = float(request.POST.get("kappa", 25))
        option = int(request.POST.get("option", 1))

        img = io.imread(path)
        if img.ndim == 3:
            img = color.rgb2gray(img)
        img = img_as_float(img)
        # img = img[::4, ::4]
        if img.shape[0] > 1000 or img.shape[1] > 1000:
            img = img[::4, ::4]

        # Guardar imagen original
        original_filename = "original.png"
        original_path = os.path.join(settings.MEDIA_ROOT, original_filename)
        plt.imshow(img, cmap="gray")
        plt.axis("off")
        plt.savefig(original_path, bbox_inches="tight", pad_inches=0)
        plt.close()

        # Guardar la imagen original
        plt.imshow(img, cmap="gray")
        plt.axis("off")
        original_path = os.path.join(default_storage.location, "original.png")
        plt.savefig(original_path, bbox_inches="tight", pad_inches=0)
        plt.close()

        u = img.copy()
        for i in range(n_iter):
            u = perona_malik(u, kappa, delta_t, option)

        # Guardar resultado como archivo temporal
        plt.imshow(u, cmap="gray")
        plt.axis("off")
        result_path = os.path.join(default_storage.location, "resultado.png")
        plt.savefig(result_path, bbox_inches="tight", pad_inches=0)

        return render(request, "resultado_gray.html", {
            "resultado": "resultado.png",
            "original": "original.png"
        })

    return render(request, "filter_gray.html")

# Color filter
def color_filter(request):
    # if request.method == "POST" and request.FILES.get("image"):
    #     uploaded_file = request.FILES["image"]
    #     im = io.imread(uploaded_file)
    #     im = img_as_float(im)

    #     # Parámetros
    #     iterations = 50   # reducir para web
    #     delta = 0.15
    #     kappa = 0.1
    #     dd = np.sqrt(2)

    #     windows = [
    #         np.array([[0, 1, 0], [0, -1, 0], [0, 0, 0]], np.float64),
    #         np.array([[0, 0, 0], [0, -1, 0], [0, 1, 0]], np.float64),
    #         np.array([[0, 0, 0], [0, -1, 1], [0, 0, 0]], np.float64),
    #         np.array([[0, 0, 0], [1, -1, 0], [0, 0, 0]], np.float64),
    #         np.array([[0, 0, 1], [0, -1, 0], [0, 0, 0]], np.float64),
    #         np.array([[0, 0, 0], [0, -1, 0], [0, 0, 1]], np.float64),
    #         np.array([[0, 0, 0], [0, -1, 0], [1, 0, 0]], np.float64),
    #         np.array([[1, 0, 0], [0, -1, 0], [0, 0, 0]], np.float64),
    #     ]

    #     # -------------------------------
    #     # Difusión RGB canal por canal
    #     # -------------------------------
    #     u_rgb = im.copy()
    #     for c in range(3):
    #         channel = u_rgb[:, :, c]
    #         for r in range(iterations):
    #             nabla = [ndimage.convolve(channel, w) for w in windows]
    #             diff = [1. / (1 + (n / kappa) ** 2) for n in nabla]
    #             terms = [diff[i] * nabla[i] for i in range(4)]
    #             terms += [(1 / (dd ** 2)) * diff[i] * nabla[i] for i in range(4, 8)]
    #             channel = channel + delta * (sum(terms))
    #         u_rgb[:, :, c] = channel

    #     # -------------------------------
    #     # Difusión solo luminancia (Lab)
    #     # -------------------------------
    #     im_lab = color.rgb2lab(im)
    #     L = im_lab[:, :, 0]
    #     for r in range(iterations):
    #         nabla = [ndimage.convolve(L, w) for w in windows]
    #         diff = [1. / (1 + (n / kappa) ** 2) for n in nabla]
    #         terms = [diff[i] * nabla[i] for i in range(4)]
    #         terms += [(1 / (dd ** 2)) * diff[i] * nabla[i] for i in range(4, 8)]
    #         L = L + delta * (sum(terms))
    #     im_lab[:, :, 0] = L
    #     u_lab = color.lab2rgb(im_lab)

    #     # -------------------------------
    #     # Visualización en una sola figura
    #     # -------------------------------
    #     fig, axes = plt.subplots(1, 3, figsize=(12, 6))
    #     axes[0].imshow(im)
    #     axes[0].set_title("Original")
    #     axes[0].axis("off")

    #     axes[1].imshow(np.clip(u_rgb, 0, 1))
    #     axes[1].set_title("Difusión RGB canal por canal")
    #     axes[1].axis("off")

    #     axes[2].imshow(np.clip(u_lab, 0, 1))
    #     axes[2].set_title("Difusión luminancia (Lab)")
    #     axes[2].axis("off")

    #     plt.tight_layout()

    #     buf = BytesIO()
    #     plt.savefig(buf, format="png")
    #     plt.close(fig)
    #     image_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

    #     return render(request, "resultado_color.html", {"image": image_base64})

    # return render(request, "filter_color.html")


    if request.method == "POST" and request.FILES.get("image"):
        # Leer imagen subida
        uploaded_file = request.FILES["image"]
        im = io.imread(uploaded_file)
        im = img_as_float(im)

        # Parámetros
        iterations = 100   # reducir para web
        delta = 0.15
        kappa = 0.4
        dd = np.sqrt(2)

        windows = [
            np.array([[0, 1, 0], [0, -1, 0], [0, 0, 0]], np.float64),
            np.array([[0, 0, 0], [0, -1, 0], [0, 1, 0]], np.float64),
            np.array([[0, 0, 0], [0, -1, 1], [0, 0, 0]], np.float64),
            np.array([[0, 0, 0], [1, -1, 0], [0, 0, 0]], np.float64),
            np.array([[0, 0, 1], [0, -1, 0], [0, 0, 0]], np.float64),
            np.array([[0, 0, 0], [0, -1, 0], [0, 0, 1]], np.float64),
            np.array([[0, 0, 0], [0, -1, 0], [1, 0, 0]], np.float64),
            np.array([[1, 0, 0], [0, -1, 0], [0, 0, 0]], np.float64),
        ]

        # Difusión en luminancia
        im_lab = color.rgb2lab(im)
        L = im_lab[:, :, 0]
        for r in range(iterations):
            nabla = [ndimage.convolve(L, w) for w in windows]
            diff = [1. / (1 + (n / kappa) ** 2) for n in nabla]
            terms = [diff[i] * nabla[i] for i in range(4)]
            terms += [(1 / (dd ** 2)) * diff[i] * nabla[i] for i in range(4, 8)]
            L = L + delta * (sum(terms))
        im_lab[:, :, 0] = L
        u_lab = color.lab2rgb(im_lab)

        # Convertir resultado a PNG en memoria
        fig, ax = plt.subplots()
        ax.imshow(np.clip(u_lab, 0, 1))
        ax.axis("off")
        buf = BytesIO()
        plt.savefig(buf, format="png")
        plt.close(fig)
        image_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

        return render(request, "resultado_color.html", {"image": image_base64})

    return render(request, "filter_color.html")

    
