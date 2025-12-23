# -*- coding: utf-8 -*-

import os, json, math, warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from shapely.geometry import Point, LineString, MultiLineString, MultiPoint
from shapely.ops import unary_union, linemerge, nearest_points, split
from shapely import set_precision

import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor, as_completed
warnings.filterwarnings("ignore", category=UserWarning)

class KRipley_HS:

    def __init__(self,
                 excel_path,
                 excel_sheet,
                 lat_field,
                 lon_field,
                 roads_path,
                 output_folder,
                 simplify_tolerance_m,
                 precision_scale,
                 segment_spacing_m,
                 snap_tolerance_m,
                 r_start_m,
                 r_step_m,
                 n_sim_ripley,
                 random_seed,
                 n_sim_hotspot,
                 hs_point_spacing_m,
                 export_csv_hotspots_name,
                 export_csv_ripley_name,
                 export_shp_vias_colapsadas_name,
                 plot_png,
                 n_workers,
                 max_hs_sample_points):

        os.makedirs(output_folder, exist_ok=True)

        # --------------------------------------------------
        # EVENTOS
        # --------------------------------------------------

        ev = self.cargar_eventos_excel(excel_path, excel_sheet, lat_field, lon_field)
        ev = ev[ev.is_valid & ~ev.is_empty & ~ev.geometry.isna()].copy()
        ev = self.asegurar_crs_4326(ev)

        if len(ev) < 2:
            raise ValueError("Se requieren al menos dos eventos")

        lat0 = float(ev.geometry.y.mean())
        m_lat, m_lon = self.metros_por_grado(lat0)

        # --------------------------------------------------
        # VIAS
        # --------------------------------------------------

        vi = gpd.read_file(roads_path)
        vi = vi[vi.is_valid & ~vi.geometry.isna()].explode(index_parts=False).reset_index(drop=True)
        vi = self.asegurar_crs_4326(vi)

        # --------------------------------------------------
        # CONVERSION METROS A GRADOS
        # --------------------------------------------------

        simplify_deg = self.m_a_deg_conservador(simplify_tolerance_m, m_lat, m_lon, "min")
        segment_deg  = self.m_a_deg_conservador(segment_spacing_m,  m_lat, m_lon, "min")
        snap_deg     = self.m_a_deg_conservador(snap_tolerance_m,     m_lat, m_lon, "max")
        hs_step_deg  = self.m_a_deg_conservador(hs_point_spacing_m,   m_lat, m_lon, "min")

        # --------------------------------------------------
        # COLAPSAR RED
        # --------------------------------------------------

        cl = self.colapsar_y_simplificar_red_4326(
            vi=vi,
            simplify_deg=float(simplify_deg),
            precision_scale=float(precision_scale)
        )

        shp_out = os.path.join(output_folder, export_shp_vias_colapsadas_name)
        self.exportar_shp_4326(cl, shp_out)

        # --------------------------------------------------
        # SEGMENTAR RED
        # --------------------------------------------------

        cl_seg = self.segmentar_lineas_4326(
            gdf=cl,
            spacing_deg=float(segment_deg),
            meters_per_deg_lat=m_lat,
            meters_per_deg_lon=m_lon
        )

        D_m = float(cl_seg["length_m"].sum())
        if D_m <= 0:
            raise ValueError("Longitud total de red igual a cero")

        # --------------------------------------------------
        # SNAP EVENTOS
        # --------------------------------------------------

        snapped = self.snap_eventos_a_red_4326(ev, cl_seg, snap_deg)
        snapped = snapped[snapped.is_valid & ~snapped.is_empty & ~snapped.geometry.isna()].copy()

        if len(snapped) < 2:
            raise ValueError("Eventos insuficientes tras snap")

        # --------------------------------------------------
        # RADIOS
        # --------------------------------------------------

        if r_step_m is None or r_step_m <= 0:
            r_step_m = segment_spacing_m

        r_vals_m = np.arange(r_start_m, D_m + r_step_m, r_step_m)

        # --------------------------------------------------
        # RIPLEY K
        # --------------------------------------------------

        K_obs, sims = self.ripley_k_red_2d_fast_4326(
            cl_seg,
            snapped,
            r_vals_m,
            m_lat,
            m_lon,
            n_sim_ripley,
            random_seed
        )

        K_env_lo = np.quantile(sims, 0.025, axis=0)
        K_env_hi = np.quantile(sims, 0.975, axis=0)

        L_obs, L_lo, L_hi = self.calcular_L(K_obs, sims)

        if plot_png:
            self.graficar_L(r_vals_m, L_obs, L_lo, L_hi, r_step_m, output_folder)

        signif = r_vals_m[L_obs > L_hi]
        r_star = float(signif.min()) if len(signif) else None

        # --------------------------------------------------
        # HOTSPOTS
        # --------------------------------------------------

        if r_star is None:
            hs_csv = pd.DataFrame(columns=["Latitude", "Longitude", "HS", "HS_Intense", "UCL", "LCL"])
        else:
            hs_csv = self.hotspots_siriema_real_ci_4326(
                cl_seg,
                snapped,
                r_star,
                hs_point_spacing_m,
                hs_step_deg,
                m_lat,
                m_lon,
                n_sim_hotspot,
                random_seed,
                n_workers,
                max_hs_sample_points
            )

        # --------------------------------------------------
        # EXPORTS
        # --------------------------------------------------

        pd.DataFrame({
            "r_m": r_vals_m,
            "K_obs": K_obs,
            "K_env_lo": K_env_lo,
            "K_env_hi": K_env_hi,
            "L_obs": L_obs,
            "L_env_lo": L_lo,
            "L_env_hi": L_hi
        }).to_csv(os.path.join(output_folder, export_csv_ripley_name), index=False)

        hs_csv.to_csv(os.path.join(output_folder, export_csv_hotspots_name), index=False)

        # --------------------------------------------------
        # METADATA
        # --------------------------------------------------

        with open(os.path.join(output_folder, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump({
                "crs": "EPSG:4326",
                "lat0": lat0,
                "longitud_red_m": D_m,
                "r_star_m": r_star
            }, f, indent=2)

    # ==================================================
    # UTILIDADES
    # ==================================================

    def asegurar_crs_4326(self, gdf):
        return gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")

    def metros_por_grado(self, lat):
        return 111111.0, 111111.0 * max(1e-8, math.cos(math.radians(lat)))

    def m_a_deg_conservador(self, m, m_lat, m_lon, modo):
        dlat = m / m_lat
        dlon = m / m_lon
        return max(dlat, dlon) if modo == "max" else min(dlat, dlon)

    def cargar_eventos_excel(self, path, sheet, lat, lon):
        df = pd.read_excel(path, sheet_name=sheet).dropna(subset=[lat, lon])
        return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon], df[lat]), crs="EPSG:4326")

    # ==================================================
    # COLAPSAR DOBLE CALZADA (REEMPLAZO REFINADO)
    # ==================================================
    def colapsar_y_simplificar_red_4326(self, vi, simplify_deg, precision_scale):

        # --------------------------------------------------
        # Limpieza mínima, SIN tocar geometría
        # --------------------------------------------------
        gdf = vi.copy()
        gdf = gdf[gdf.geometry.notnull() & gdf.is_valid]
        gdf = gdf.explode(index_parts=False).reset_index(drop=True)

        # Aplanar a LineString sin modificar nada
        geoms = []
        for g in gdf.geometry.values:
            if g is None or g.is_empty:
                continue
            if isinstance(g, LineString):
                geoms.append(g)
            elif isinstance(g, MultiLineString):
                for p in g.geoms:
                    if p is not None and not p.is_empty and p.length > 0:
                        geoms.append(p)

        if not geoms:
            return gpd.GeoDataFrame({"id": []}, geometry=[], crs="EPSG:4326")

        gs = gpd.GeoSeries(geoms, crs="EPSG:4326")
        sindex = gs.sindex

        usados = np.zeros(len(gs), dtype=bool)
        resultado = []

        # Tolerancia SOLO para detectar paralelismo (NO modifica geometría)
        tol = float(simplify_deg) * 20.0

        # --------------------------------------------------
        # Eliminación CONSERVADORA de dobles calzadas
        # --------------------------------------------------
        for i in tqdm(range(len(gs)), total=len(gs), desc="Detectando dobles calzadas"):
            if usados[i]:
                continue

            g = gs.iloc[i]
            if g is None or g.is_empty:
                usados[i] = True
                continue

            ang_i = self._angulo_linea(g)
            len_i = float(g.length)

            minx, miny, maxx, maxy = g.bounds
            bbox = (minx - tol, miny - tol, maxx + tol, maxy + tol)
            candidatos = list(sindex.intersection(bbox))

            grupo = [i]

            for j in candidatos:
                if j == i or usados[j]:
                    continue

                gj = gs.iloc[j]
                if gj is None or gj.is_empty:
                    continue

                # Criterios estrictos
                if g.distance(gj) > tol:
                    continue

                ang_j = self._angulo_linea(gj)
                if abs(ang_i - ang_j) > 8.0:
                    continue

                len_j = float(gj.length)
                if min(len_i, len_j) / max(len_i, len_j) < 0.97:
                    continue

                grupo.append(j)

            # --------------------------------------------------
            # Decisión SIN RIESGO
            # --------------------------------------------------
            if len(grupo) == 1:
                resultado.append(g)
                usados[i] = True
                continue

            # Doble calzada clara → conservar UNA EXACTA
            idx_keep = max(grupo, key=lambda k: float(gs.iloc[k].length))
            resultado.append(gs.iloc[idx_keep])

            for k in grupo:
                usados[k] = True

        # --------------------------------------------------
        # Salida SIN ALTERAR TRAZADO
        # --------------------------------------------------
        out = gpd.GeoDataFrame(
            {"id": range(len(resultado))},
            geometry=resultado,
            crs="EPSG:4326"
        )

        return out




    def _angulo_linea(self, line):
        c = np.asarray(line.coords)
        if c.shape[0] < 2:
            return 0.0
        return abs(math.degrees(math.atan2(c[-1, 1] - c[0, 1], c[-1, 0] - c[0, 0]))) % 180.0

    def exportar_shp_4326(self, gdf, path):
        gdf[["id", "geometry"]].to_file(path, driver="ESRI Shapefile")

    # ==================================================
    # SEGMENTAR RED (AJUSTE EXCEPT split -> [p])
    # ==================================================

    def segmentar_lineas_4326(self, gdf, spacing_deg, meters_per_deg_lat, meters_per_deg_lon):

        segs = []
        ids = []

        for idx, row in tqdm(gdf.iterrows(), total=len(gdf), desc="Segmentando"):
            line = row.geometry
            if line is None or line.is_empty:
                continue

            parts = list(line.geoms) if isinstance(line, MultiLineString) else [line]

            for p in parts:
                if p.length <= spacing_deg:
                    segs.append(p)
                    ids.append(idx)
                    continue

                n = int(p.length // spacing_deg)
                pts = [Point(p.interpolate(i * spacing_deg)) for i in range(1, n)]

                try:
                    pieces = split(p, MultiPoint(pts))
                except Exception:
                    pieces = p  # fallback

                # Normalizar salida de split para que SIEMPRE sea lista iterable
                if isinstance(pieces, LineString):
                    parts_out = [pieces]
                elif hasattr(pieces, "geoms"):  # GeometryCollection / MultiLineString
                    parts_out = list(pieces.geoms)
                else:
                    parts_out = [p]

                for s in parts_out:
                    if s is not None and (not s.is_empty) and s.length > 0:
                        segs.append(s)
                        ids.append(idx)


        out = gpd.GeoDataFrame({"id_src": ids}, geometry=segs, crs="EPSG:4326")
        out["length_m"] = out.geometry.apply(lambda g: self.longitud_m_equivalente(g, meters_per_deg_lat, meters_per_deg_lon))
        out["offset_global_m"] = out["length_m"].cumsum() - out["length_m"]

        return out.reset_index(drop=True)

    def longitud_m_equivalente(self, line, m_lat, m_lon):
        c = np.asarray(line.coords)
        if c.shape[0] < 2:
            return 0.0
        dx = np.diff(c[:, 0]) * m_lon
        dy = np.diff(c[:, 1]) * m_lat
        return float(np.sqrt(dx * dx + dy * dy).sum())

    # ==================================================
    # SNAP
    # ==================================================

    def snap_eventos_a_red_4326(self, ev, cl_seg, snap_tol_deg):

        sindex = cl_seg.sindex
        out = []

        for _, r in tqdm(ev.iterrows(), total=len(ev), desc="Snap eventos"):
            p = r.geometry
            cand = list(sindex.query(p.buffer(snap_tol_deg)))
            if not cand:
                continue

            sub = cl_seg.iloc[cand]
            nearest = min(sub.itertuples(), key=lambda x: p.distance(x.geometry))
            _, np2 = nearest_points(p, nearest.geometry)

            if p.distance(np2) <= snap_tol_deg:
                d = r.to_dict()
                d["geometry"] = np2
                out.append(d)

        return gpd.GeoDataFrame(out, geometry="geometry", crs="EPSG:4326")

    # ==================================================
    # RIPLEY K
    # ==================================================

    def _distancias_2d_pares_m(self, coords, m_lat, m_lon):
        x = coords[:, 0] * m_lon
        y = coords[:, 1] * m_lat
        dx = x[:, None] - x[None, :]
        dy = y[:, None] - y[None, :]
        d = np.sqrt(dx * dx + dy * dy)
        iu = np.triu_indices(len(coords), 1)
        v = d[iu]
        v.sort()
        return v

    def _contar_pares_por_r(self, dist, r_vals):
        return np.searchsorted(dist, r_vals, side="right")

    def _generar_puntos_sobre_red_4326(self, n, cl_seg, rng):

        offs = cl_seg["offset_global_m"].values
        lens = cl_seg["length_m"].values
        geoms = cl_seg.geometry.values

        breaks = offs + lens
        D = breaks[-1]

        s = rng.uniform(0, D, n)
        idx = np.searchsorted(breaks, s, side="right")

        pts = np.zeros((n, 2))
        for i in range(n):
            seg = idx[i]
            frac = (s[i] - offs[seg]) / lens[seg] if lens[seg] > 0 else 0.0
            p = geoms[seg].interpolate(frac, normalized=True)
            pts[i] = [p.x, p.y]

        return pts

    def ripley_k_red_2d_fast_4326(self, cl_seg, snapped, r_vals, m_lat, m_lon, n_sim, seed):

        rng = np.random.default_rng(seed)
        n = len(snapped)
        D = cl_seg["length_m"].sum()

        obs = np.vstack([snapped.geometry.x, snapped.geometry.y]).T
        dv = self._distancias_2d_pares_m(obs, m_lat, m_lon)
        cnt = self._contar_pares_por_r(dv, r_vals)
        K_obs = (D / (n * (n - 1))) * (2.0 * cnt)

        sims = np.zeros((n_sim, len(r_vals)))
        for i in tqdm(range(n_sim), desc="Ripley"):
            pts = self._generar_puntos_sobre_red_4326(n, cl_seg, rng)
            dv = self._distancias_2d_pares_m(pts, m_lat, m_lon)
            cnt = self._contar_pares_por_r(dv, r_vals)
            sims[i] = (D / (n * (n - 1))) * (2.0 * cnt)

        return K_obs, sims

    # ==================================================
    # L FUNCTION
    # ==================================================

    def calcular_L(self, K_obs, sims):
        mean = sims.mean(axis=0)
        L_obs = K_obs - mean
        L_sims = sims - mean
        return L_obs, np.quantile(L_sims, 0.025, axis=0), np.quantile(L_sims, 0.975, axis=0)

    def graficar_L(self, r, L, L_lo, L_hi, step, folder):
        fig, ax = plt.subplots()
        ax.plot(r, L_hi)
        ax.plot(r, L_lo)
        ax.plot(r, L)
        ax.axhline(0)
        fig.savefig(os.path.join(folder, "L_en_red_2D_pegada.png"), dpi=150)
        plt.close(fig)

    # ==================================================
    # HOTSPOTS
    # ==================================================

    def hotspots_siriema_real_ci_4326(self,
                                     cl_seg,
                                     snapped,
                                     r_opt_m,
                                     hs_point_spacing_m,
                                     hs_step_deg,
                                     m_lat,
                                     m_lon,
                                     n_sim,
                                     seed,
                                     n_workers,
                                     max_hs_sample_points):

        r_deg = r_opt_m / m_lat
        sample_pts = self.generar_puntos_muestreo_en_red_4326(cl_seg, hs_step_deg)

        if max_hs_sample_points and len(sample_pts) > max_hs_sample_points:
            sample_pts = sample_pts.sample(max_hs_sample_points, random_state=seed)

        union_red = unary_union(cl_seg.geometry)

        H_obs = self.calcular_H_con_Ci_4326(
            sample_pts,
            snapped.geometry,
            union_red,
            r_deg,
            m_lat,
            m_lon,
            r_opt_m
        )

        H_sim = self.simular_H_con_Ci_4326(
            sample_pts,
            cl_seg,
            union_red,
            r_deg,
            r_opt_m,
            m_lat,
            m_lon,
            len(snapped),
            n_sim,
            seed,
            n_workers
        )

        HS, UCL, LCL = self.calcular_HS_UCL_LCL(H_obs, H_sim)
        mask = HS > UCL

        gdf = sample_pts.loc[mask].copy()
        gdf["HS"] = HS[mask]
        gdf["UCL"] = UCL[mask]
        gdf["LCL"] = LCL[mask]
        gdf["Longitude"] = gdf.geometry.x
        gdf["Latitude"] = gdf.geometry.y

        return pd.DataFrame(gdf[["Latitude", "Longitude", "HS", "UCL", "LCL"]])

    def generar_puntos_muestreo_en_red_4326(self, cl_seg, spacing_deg):

        pts = []
        for _, r in tqdm(cl_seg.iterrows(), total=len(cl_seg), desc="Puntos HS"):
            line = r.geometry
            L = line.length
            if L <= spacing_deg:
                pts.append(line.interpolate(0))
            else:
                n = int(L // spacing_deg)
                for i in range(n + 1):
                    pts.append(line.interpolate(min(i * spacing_deg, L)))

        return gpd.GeoDataFrame(geometry=pts, crs="EPSG:4326")

    def calcular_H_con_Ci_4326(self, sample_pts, events_geom, union_red, r_deg, m_lat, m_lon, r_opt_m):

        ev = gpd.GeoDataFrame(geometry=events_geom, crs="EPSG:4326")
        sidx = ev.sindex
        H = np.zeros(len(sample_pts))

        for i, r in tqdm(sample_pts.iterrows(), total=len(sample_pts), desc="H obs"):
            p = r.geometry
            circle = p.buffer(r_deg)
            cand = list(sidx.query(circle))
            n = ev.iloc[cand].geometry.within(circle).sum() if cand else 0
            Ci = self.longitud_geom_m_equivalente(union_red.intersection(circle), m_lat, m_lon)
            H[i] = n * (2.0 * r_opt_m / Ci) if Ci > 0 else 0.0

        return H

    def simular_H_con_Ci_4326(self,
                              sample_pts,
                              cl_seg,
                              union_red,
                              r_deg,
                              r_opt_m,
                              m_lat,
                              m_lon,
                              n_events,
                              n_sim,
                              seed,
                              n_workers):

        if n_workers <= 1:
            return self.simular_H_con_Ci_4326_serial(
                sample_pts, cl_seg, union_red, r_deg, r_opt_m, m_lat, m_lon, n_events, n_sim, seed
            )

        return self.simular_H_con_Ci_4326_parallel(
            sample_pts, cl_seg, union_red, r_deg, r_opt_m, m_lat, m_lon, n_events, n_sim, seed, n_workers
        )

    def simular_H_con_Ci_4326_serial(self,
                                     sample_pts,
                                     cl_seg,
                                     union_red,
                                     r_deg,
                                     r_opt_m,
                                     m_lat,
                                     m_lon,
                                     n_events,
                                     n_sim,
                                     seed):

        rng = np.random.default_rng(seed)

        offs = cl_seg["offset_global_m"].values
        lens = cl_seg["length_m"].values
        geoms = cl_seg.geometry.values
        breaks = offs + lens
        D = breaks[-1]

        circles = [g.buffer(r_deg) for g in sample_pts.geometry]
        H_sim = np.zeros((n_sim, len(sample_pts)))

        for s in tqdm(range(n_sim), desc="HS sim"):
            s_rand = rng.uniform(0, D, n_events)
            idx = np.searchsorted(breaks, s_rand, side="right")

            sim_pts = []
            for i in range(n_events):
                frac = (s_rand[i] - offs[idx[i]]) / lens[idx[i]]
                p = geoms[idx[i]].interpolate(frac, normalized=True)
                sim_pts.append(p)

            sim = gpd.GeoDataFrame(geometry=sim_pts, crs="EPSG:4326")
            sidx = sim.sindex

            for i, c in enumerate(circles):
                cand = list(sidx.query(c))
                n = sim.iloc[cand].geometry.within(c).sum() if cand else 0
                Ci = self.longitud_geom_m_equivalente(union_red.intersection(c), m_lat, m_lon)
                H_sim[s, i] = n * (2.0 * r_opt_m / Ci) if Ci > 0 else 0.0

        return H_sim

    def _worker_sim_block(self, args):

        (start_idx,
         n_block,
         seed,
         seg_offsets,
         seg_lengths,
         breaks,
         geoms,
         D,
         circles_wkb,
         r_opt_m,
         m_lat,
         m_lon,
         n_events,
         union_red_wkb) = args

        from shapely import wkb

        union_red = wkb.loads(union_red_wkb)
        circles = [wkb.loads(b) for b in circles_wkb]
        rng = np.random.default_rng(seed)

        block = np.zeros((n_block, len(circles)))

        for s in range(n_block):
            s_rand = rng.uniform(0, D, n_events)
            idx = np.searchsorted(breaks, s_rand, side="right")

            sim_pts = []
            for i in range(n_events):
                frac = (s_rand[i] - seg_offsets[idx[i]]) / seg_lengths[idx[i]]
                p = geoms[idx[i]].interpolate(frac, normalized=True)
                sim_pts.append(p)

            sim = gpd.GeoDataFrame(geometry=sim_pts, crs="EPSG:4326")
            sidx = sim.sindex

            for i, c in enumerate(circles):
                cand = list(sidx.query(c))
                n = sim.iloc[cand].geometry.within(c).sum() if cand else 0
                Ci = self.longitud_geom_m_equivalente(union_red.intersection(c), m_lat, m_lon)
                block[s, i] = n * (2.0 * r_opt_m / Ci) if Ci > 0 else 0.0

        return start_idx, block

    def simular_H_con_Ci_4326_parallel(self,
                                       sample_pts,
                                       cl_seg,
                                       union_red,
                                       r_deg,
                                       r_opt_m,
                                       meters_per_deg_lat,
                                       meters_per_deg_lon,
                                       n_events,
                                       n_sim,
                                       seed,
                                       n_workers):

        seg_offsets = cl_seg["offset_global_m"].values.astype(float)
        seg_lengths = cl_seg["length_m"].values.astype(float)
        geoms = cl_seg.geometry.values
        breaks = seg_offsets + seg_lengths
        D = float(breaks[-1])

        circles = [g.buffer(float(r_deg)) for g in sample_pts.geometry]

        from shapely import wkb
        circles_wkb = [wkb.dumps(c) for c in circles]
        union_red_wkb = wkb.dumps(union_red)

        n_sim = int(n_sim)
        n_workers = max(2, int(n_workers))
        chunk = max(1, int(math.ceil(n_sim / n_workers)))

        tasks = []
        start = 0
        while start < n_sim:
            n_block = min(chunk, n_sim - start)
            block_seed = int(seed) + int(start) * 97 + 13

            tasks.append((
                start, n_block, block_seed,
                seg_offsets, seg_lengths, breaks, geoms, D,
                circles_wkb, r_opt_m,
                meters_per_deg_lat, meters_per_deg_lon,
                n_events, union_red_wkb
            ))
            start += n_block

        H_sim = np.zeros((n_sim, len(sample_pts)), dtype=np.float32)

        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            futs = [ex.submit(self._worker_sim_block, t) for t in tasks]
            for fut in tqdm(as_completed(futs), total=len(futs), desc="Simulaciones HS (paralelo)"):
                start_idx, block = fut.result()
                H_sim[start_idx:start_idx + block.shape[0], :] = block

        return H_sim.astype(float)

    def longitud_geom_m_equivalente(self, geom, m_lat, m_lon):
        if geom is None or geom.is_empty:
            return 0.0
        if geom.geom_type == "LineString":
            return self.longitud_m_equivalente(geom, m_lat, m_lon)
        if geom.geom_type == "MultiLineString":
            return sum(self.longitud_m_equivalente(g, m_lat, m_lon) for g in geom.geoms)
        return 0.0

    def calcular_HS_UCL_LCL(self, H_obs, H_sim):
        mean = H_sim.mean(axis=0)
        HS = H_obs - mean
        lo = np.quantile(H_sim, 0.025, axis=0)
        hi = np.quantile(H_sim, 0.975, axis=0)
        return HS, hi, lo

    pass