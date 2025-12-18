import os
import random
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from rasterio import open as rasterio_open
from rasterio.mask import mask
from django.conf import settings

# rpy2 imports
from rpy2 import robjects
from rpy2.robjects import r, globalenv
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import StrVector


class MaxEntWorkflow:
    def __init__(self,
                 project_name,
                 raster_folder="rasterIN",
                 crop_folder="Crop",
                 output_folder="output",
                 result_folder="RasterResult",
                 hotspot_filename="atropellamiento.csv",
                 output_sample_name="muestreo.csv",
                 line_shp_name="vias.shp",
                 buffer_dist=90,
                 simplify_factor=30,
                 n_points=10000,
                 training_prob=0.8,
                 replicates=3):

        self.basepath = os.path.join(settings.MEDIA_ROOT, "maxent_invias")
        self.project_name = project_name
        self.project_path = os.path.join(self.basepath, project_name)

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

        os.makedirs(self.project_path, exist_ok=True)

        self.jacknife_entries = {}

        for i in range(1,6):
        # while True:
            region_path = os.path.join(settings.MEDIA_ROOT, "jacknife", f"region{i}")
            if os.path.exists(region_path):
                # self.jacknife_entries[f"region{i}"] = os.listdir(region_path)
                self.jacknife_entries[f"region{i}"] = [
                    os.path.join(region_path, entry) for entry in os.listdir(region_path)
                ]
            else:
                self.jacknife_entries[f"region{i}"] = []

    # ----------------------
    # ETAPA 1: preparación
    # ----------------------
    def run(self):
        tqdm.write(f"Procesando proyecto: {self.project_name}")

        self.preparar_carpetas()
        self.recortar_rasteres()
        self.generar_puntos_aleatorios()
        self.ejecutar_maxent_en_r()

        tqdm.write(f"Proyecto {self.project_name} completado")

    def preparar_carpetas(self):
        for carpeta in [self.crop_folder, self.output_folder, self.result_folder]:
            path_carpeta = os.path.join(self.project_path, carpeta)
            os.makedirs(path_carpeta, exist_ok=True)

    def recortar_rasteres(self):
        ruta_shp = os.path.join(self.project_path, self.line_shp_name)
        if not os.path.exists(ruta_shp):
            return

        lineas = gpd.read_file(ruta_shp)
        lineas_m = lineas.to_crs(epsg=3857)
        buffer_geom = lineas_m.buffer(self.buffer_dist)
        buffer_union = buffer_geom.union_all()
        buffer_simpl = buffer_union.simplify(self.simplify_factor, preserve_topology=True)

        ruta_buffer = os.path.join(self.project_path, "buffer_vias.shp")
        gpd.GeoSeries([buffer_simpl], crs='EPSG:3857').to_file(ruta_buffer)

        carpeta_rasteres = os.path.join(self.project_path, self.raster_folder)
        raster_files = [f for f in os.listdir(carpeta_rasteres) if f.lower().endswith('.tif')]
        if not raster_files:
            return

        carpeta_crop = os.path.join(self.project_path, self.crop_folder)
        for tif in tqdm(raster_files, desc=f"Recortando {self.project_name}", leave=False):
            ruta_tif = os.path.join(carpeta_rasteres, tif)
            try:
                with rasterio_open(ruta_tif) as src:
                    buffer_geom_crs = gpd.GeoSeries([buffer_simpl], crs='EPSG:3857').to_crs(src.crs)
                    geoms = [buffer_geom_crs.iloc[0].__geo_interface__]
                    out_image, out_transform = mask(src, geoms, crop=True)
                    out_meta = src.meta.copy()
                    out_meta.update({
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform
                    })
                    ruta_guardar = os.path.join(carpeta_crop, tif)
                    with rasterio_open(ruta_guardar, "w", **out_meta) as dest:
                        dest.write(out_image)
            except Exception as e:
                print(f"Error recortando {tif}: {e}")

    def generar_puntos_aleatorios(self):
        carpeta_crop = os.path.join(self.project_path, self.crop_folder)
        raster_files = [f for f in os.listdir(carpeta_crop) if f.lower().endswith('.tif')]
        if not raster_files:
            return

        ruta_raster = os.path.join(carpeta_crop, raster_files[0])
        try:
            with rasterio_open(ruta_raster) as src:
                mask_arr = src.read(1)
                mask_valid = np.where(mask_arr != src.nodata)
                if len(mask_valid[0]) == 0:
                    return
                indices = list(zip(mask_valid[0], mask_valid[1]))
                n_puntos = min(self.n_points, len(indices))
                sampled_indices = random.sample(indices, n_puntos)
                coords = [src.xy(row, col) for row, col in sampled_indices]
                df = pd.DataFrame(coords, columns=['x', 'y'])
                ruta_csv = os.path.join(self.project_path, self.output_sample_name)
                df.to_csv(ruta_csv, index=False)
        except Exception as e:
            print(f"Error generando puntos aleatorios: {e}")

    # ----------------------
    # ETAPA 2: ejecución R
    # ----------------------
    def ejecutar_maxent_en_r(self):
        """Ejecuta el script R directamente desde Python con rpy2."""

        # Instalar paquetes necesarios si faltan
        utils = importr("utils")
        utils.chooseCRANmirror(ind=1)
        paquetes = ["raster", "dismo", "readr", "sp", "sf", "codetools", "rJava"]
        utils.install_packages(StrVector(paquetes))

        # Asignar variables de entorno
        globalenv["basepath"] = self.project_path.replace("\\", "/")
        globalenv["cropFolder"] = self.crop_folder
        globalenv["outputFolder"] = self.output_folder
        globalenv["resultFolder"] = self.result_folder
        globalenv["hotspot"] = self.hotspot_filename
        globalenv["outputSample"] = self.output_sample_name
        globalenv["trainingProb"] = self.training_prob
        globalenv["replicates"] = self.replicates

        # Código R embebido
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
        crop_path <- file.path(basepath, cropFolder)
        output_path <- file.path(basepath, outputFolder)
        result_path <- file.path(basepath, resultFolder)

        raster_files <- list.files(path = crop_path, pattern = "\\\\.tif$", full.names = TRUE)
        if (length(raster_files) == 0) stop("No se encontraron rásteres recortados.")

        clim <- stack(raster_files)
        bg <- read_csv(file.path(basepath, outputSample))
        coordinates(bg) <- ~x + y

        occ_raw <- read_csv(file.path(basepath, hotspot))
        occ_clean <- subset(occ_raw, !is.na(Latitude) & !is.na(Longitude))
        occ_clean <- occ_clean[!duplicated(occ_clean[c("Latitude","Longitude")]), ]
        coordinates(occ_clean) <- ~Longitude + Latitude

        numReg <- nrow(occ_clean)
        if (numReg < 5) stop("No hay suficientes puntos de ocurrencia.")

        numTrain <- round(numReg * trainingProb)
        selected <- sample(1:numReg, numTrain)
        occ_train <- occ_clean[selected, ]
        occ_test <- occ_clean[-selected, ]

        p <- extract(clim, occ_train)
        a <- extract(clim, bg)
        pa <- c(rep(1, nrow(p)), rep(0, nrow(a)))
        pder <- as.data.frame(rbind(p, a))

        mod <- maxent(x = pder, p = pa,
                      path = output_path,
                      args = c("autofeature", "responsecurves", "jackknife",
                               paste0("replicates=", replicates)))

        pred <- predict(mod, clim)
        writeRaster(pred, filename=file.path(result_path, "resultado_maxent.tif"), overwrite=TRUE)
        """

        try:
            r(script_r)
            print("Ejecución MaxEnt completada en R.")
        except Exception as e:
            print(f"Error en ejecución R: {e}")


