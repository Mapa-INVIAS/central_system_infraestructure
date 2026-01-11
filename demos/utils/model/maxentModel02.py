import os
import random
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from rasterio import open as rasterio_open
from rasterio.mask import mask
from django.conf import settings

from rpy2.robjects import r, globalenv
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import StrVector


# =====================================================
# CLASE MAXENT
# =====================================================
class MaxEntWorkflow:

    def __init__(
        self,
        project_name,
        input_basepath=None,  
        output_basepath=None,  
        raster_folder="rasterIN",
        crop_folder="Crop",
        output_folder="output",
        result_folder="RasterResult",
        hotspot_filename="atropellamiento.csv",
        output_sample_name="muestreo.csv",
        line_shp_name="vias.shp",
        buffer_dist=100 * np.sqrt(2),
        simplify_factor=30,
        n_points=10000,
        training_prob=0.8,
        replicates=3,
    ):
        self.project_name = project_name

        # Si no se pasan, usamos MEDIA_ROOT
        self.input_basepath = input_basepath or os.path.join(settings.MEDIA_ROOT, "jacknife")
        self.output_basepath = output_basepath or os.path.join(settings.MEDIA_ROOT, "maxent_invias")

        self.input_project_path = os.path.join(self.input_basepath, project_name)
        self.output_project_path = os.path.join(self.output_basepath, project_name)
        os.makedirs(self.output_project_path, exist_ok=True)

        # Parámetros restantes
        self.raster_folder = raster_folder
        self.crop_folder = crop_folder
        self.output_folder = output_folder
        self.result_folder = result_folder
        self.hotspot_filename = hotspot_filename
        self.output_sample_name = output_sample_name
        self.line_shp_name = line_shp_name
        self.buffer_dist = buffer_dist
        self.simplify_factor = simplify_factor
        self.n_points = n_points
        self.training_prob = training_prob
        self.replicates = replicates

    # =================================================
    def run(self):
        print(f"\n=== Procesando región: {self.project_name} ===")

        self.preparar_carpetas()
        self.recortar_rasteres()
        self.generar_puntos_aleatorios()
        self.ejecutar_maxent_en_r()

        print(f"=== Región {self.project_name} completada ===")

    # =================================================
    def preparar_carpetas(self):
        for carpeta in [self.crop_folder, self.output_folder, self.result_folder]:
            os.makedirs(
                os.path.join(self.output_project_path, carpeta),
                exist_ok=True
            )

    # =================================================
    def recortar_rasteres(self):
        shp_path = os.path.join(self.input_project_path, self.line_shp_name)
        if not os.path.exists(shp_path):
            raise FileNotFoundError(shp_path)

        lineas = gpd.read_file(shp_path).to_crs(epsg=3857)

        buffer_geom = (
            lineas.buffer(self.buffer_dist)
            .union_all()
            .simplify(self.simplify_factor, preserve_topology=True)
        )

        raster_dir = os.path.join(self.input_project_path, self.raster_folder)
        crop_dir = os.path.join(self.output_project_path, self.crop_folder)

        for tif in os.listdir(raster_dir):
            if not tif.lower().endswith(".tif"):
                continue

            with rasterio_open(os.path.join(raster_dir, tif)) as src:
                geom = (
                    gpd.GeoSeries([buffer_geom], crs="EPSG:3857")
                    .to_crs(src.crs)
                    .iloc[0]
                )

                out_img, out_tr = mask(
                    src,
                    [geom.__geo_interface__],
                    crop=True
                )

                meta = src.meta.copy()
                meta.update(
                    height=out_img.shape[1],
                    width=out_img.shape[2],
                    transform=out_tr,
                )

                with rasterio_open(
                    os.path.join(crop_dir, tif),
                    "w",
                    **meta
                ) as dst:
                    dst.write(out_img)

    # =================================================
    def generar_puntos_aleatorios(self):
        crop_dir = os.path.join(self.output_project_path, self.crop_folder)
        raster_files = [f for f in os.listdir(crop_dir) if f.endswith(".tif")]

        if not raster_files:
            raise RuntimeError("No hay rásteres recortados")

        with rasterio_open(os.path.join(crop_dir, raster_files[0])) as src:
            arr = src.read(1)
            valid = np.where(arr != src.nodata)

            n = min(self.n_points, len(valid[0]))
            idx = random.sample(list(zip(valid[0], valid[1])), n)
            xy = [src.xy(r, c) for r, c in idx]

        df = pd.DataFrame(xy, columns=["x", "y"])
        df.to_csv(
            os.path.join(self.output_project_path, self.output_sample_name),
            index=False,
        )

    # =================================================
    def ejecutar_maxent_en_r(self):


        # Preparar CRAN y paquetes
        utils = importr("utils")
        utils.chooseCRANmirror(ind=1)
        paquetes = ["raster", "dismo", "readr", "sp", "sf", "codetools", "rJava"]
        utils.install_packages(StrVector(paquetes))

        # Variables de entorno
        globalenv["basepath"] = self.output_project_path.replace("\\", "/")
        globalenv["cropFolder"] = self.crop_folder
        globalenv["outputFolder"] = self.output_folder
        globalenv["resultFolder"] = self.result_folder
        globalenv["hotspot"] = os.path.join(self.input_project_path, self.hotspot_filename).replace("\\", "/")
        globalenv["outputSample"] = self.output_sample_name
        globalenv["trainingProb"] = self.training_prob
        globalenv["replicates"] = self.replicates

        # Código R
        script_r = """
        library(raster)
        library(dismo)
        library(readr)
        library(sp)
        library(sf)
        library(codetools)
        library(rJava)
        .jinit()

        .jcall("java/lang/System", "S", "getProperty", "java.version")

        setwd(basepath)
        crop_path   <- file.path(basepath, cropFolder)
        output_path <- file.path(basepath, outputFolder)
        result_path <- file.path(basepath, resultFolder)

        raster_files <- list.files(crop_path, pattern="\\\\.tif$", full.names=TRUE)
        if (length(raster_files) == 0) stop("No hay rásteres recortados")

        clim <- stack(raster_files)

        # Background
        bg <- read_csv(file.path(basepath, outputSample))
        coordinates(bg) <- ~x + y

        # Ocurrencias
        occ_raw <- read_csv(hotspot)
        occ_clean <- occ_raw[!is.na(occ_raw$Longitude) & !is.na(occ_raw$Latitude), ]
        occ_clean <- occ_clean[!duplicated(occ_clean[c("Longitude","Latitude")]), ]
        coordinates(occ_clean) <- ~Longitude + Latitude

        n <- nrow(occ_clean)
        if (n < 5) stop("Muy pocos puntos de ocurrencia")

        # Selección entrenamiento
        sel <- sample(1:n, round(n * trainingProb))
        occ_train <- occ_clean[sel, ]
        occ_test  <- occ_clean[-sel, ]

        # Extraer valores de raster para presencia y background
        p <- extract(clim, occ_train)
        a <- extract(clim, bg)
        pa <- c(rep(1, nrow(p)), rep(0, nrow(a)))
        pder <- as.data.frame(rbind(p, a))

        # Modelo MaxEnt
        mod <- maxent(
            x = pder,
            p = pa,
            path = output_path,
            args = c("autofeature", "responsecurves", "jackknife",
                    paste0("replicates=", replicates))
        )

        # Predicción
        pred <- predict(mod, clim)
        writeRaster(pred, filename=file.path(result_path, "resultado_maxent.tif"), overwrite=TRUE)
        """

        try:
            r(script_r)
            print(f"[OK] MaxEnt ejecutado para {self.project_name}")
        except Exception as e:
            print(f"[ERROR] Error en ejecución R ({self.project_name}): {e}")


# =====================================================
# ORQUESTADOR
# =====================================================
def run_maxent_desde_jacknife():
    jacknife_root = os.path.join(settings.MEDIA_ROOT, "jacknife")
    output_root = os.path.join(settings.MEDIA_ROOT, "maxent_invias")

    regiones = [
        d for d in os.listdir(jacknife_root)
        if os.path.isdir(os.path.join(jacknife_root, d))
    ]

    if not regiones:
        raise RuntimeError("No hay regiones en jacknife")

    for region in regiones:
        wf = MaxEntWorkflow(
            project_name=region,
            input_basepath=jacknife_root,
            output_basepath=output_root,
        )
        wf.run()


# =====================================================
# EJECUCIÓN DIRECTA (opcional)
# =====================================================
# if __name__ == "__main__":
#     run_maxent_desde_jacknife()
