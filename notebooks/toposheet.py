#!/usr/bin/env python3
"""
Toposheet-style map WITHOUT GDAL:
- Hillshade from DEM (matplotlib LightSource)
- Contours via matplotlib.contour (exported to GeoJSON manually)
- Roads + waterways from OSMnx
- Print layout: grid, scale bar, north arrow
Outputs:
  ./outputs/toposheet.pdf
  ./outputs/contours.geojson
"""

import json
import math
import os
from pathlib import Path

import numpy as np
import rasterio as rio
from matplotlib import pyplot as plt
from matplotlib.colors import LightSource
from pyproj import CRS, Transformer
from rasterio.transform import Affine

# Optional (but you said you have it)
try:
    from pydem.dem_processing import DEMProcessor

    HAS_PYDEM = True
except Exception:
    HAS_PYDEM = False

# OSMnx (depends on geopandas/shapely; fine—no osgeo.*)
import osmnx as ox

# -----------------------------
# CONFIG
# -----------------------------
DEM_FILENAME = "P5_PAN_CD_N25_000_E077_000_DEM_30m"
DEM_PATH = f"../data/00_raw/elevation/{DEM_FILENAME}.tif"
OUT_DIR = Path("../data/00_raw/elevation/toposheets") / DEM_FILENAME
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONTOUR_INTERVAL = 30.0  # meters
INDEX_INTERVAL = 90.0  # thicker lines every N meters
OSM_BUFFER_M = 1000  # buffer around DEM bbox for OSM features
TITLE = "Toposheet Map"
SUBTITLE = "Hillshade, 30 m Contours, Roads & Water"

# Fetch fewer, more important place types; control thinning
PLACE_TYPES = ["city", "town", "village"]
MIN_LABEL_SPACING_KM = 6.0
MAX_LABELS_PER_CATEGORY = {"city": 15, "town": 25, "village": 35}

# -----------------------------
# Helpers
# -----------------------------
def read_dem(path):
    src = rio.open(path)
    dem = src.read(1).astype("float32")
    # Treat no-data as NaN
    ndv = src.nodata
    if ndv is not None:
        dem = np.where(dem == ndv, np.nan, dem)
    return src, dem


def grid_xy(transform: Affine, width: int, height: int):
    """
    Make 2D arrays of x,y (map coords) at pixel centers using the full Affine.
    Works for any north-up geotransform (and oblique if needed).
    """
    cols = np.arange(width) + 0.5
    rows = np.arange(height) + 0.5
    C, R = np.meshgrid(cols, rows)
    X = transform.a * C + transform.b * R + transform.c
    Y = transform.d * C + transform.e * R + transform.f
    return X, Y


def hillshade_from_dem(dem, azimuth=315, altitude=45, vert_exag=1.0):
    ls = LightSource(azdeg=azimuth, altdeg=altitude)
    # Greyscale hillshade RGB [0..1]
    return ls.shade(dem, vert_exag=vert_exag, blend_mode="soft", cmap=plt.cm.Greys)


def contour_segments_from_dem(X, Y, Z, interval, min_points=2):
    """
    Contours via matplotlib (no GDAL).
    Returns: list[(level, [ [(x,y), ...], ... ])]
    """
    Z = np.asarray(Z, dtype=float)
    if not np.isfinite(Z).any():
        raise ValueError("DEM has no finite values to contour.")

    zmin = np.nanmin(Z)
    zmax = np.nanmax(Z)
    if zmin == zmax:
        raise ValueError("DEM is flat in the visible area; no contours possible.")

    start = np.floor(zmin / interval) * interval
    levels = np.arange(start, np.ceil(zmax / interval) * interval + interval, interval)

    fig, ax = plt.subplots()
    cs = ax.contour(X, Y, Z, levels=levels)
    plt.close(fig)

    results = []
    # cs.levels: list of contour levels
    # cs.allsegs: list-of-lists of Nx2 arrays of vertices for each level
    for lvl, seglist in zip(cs.levels, cs.allsegs):
        segments = []
        for arr in seglist:  # arr shape: (N, 2) -> x,y pairs
            if arr is None or len(arr) < min_points:
                continue
            # Filter non-finite vertices just in case
            mask = np.isfinite(arr).all(axis=1)
            coords = [(float(x), float(y)) for x, y in arr[mask]]
            if len(coords) >= min_points:
                segments.append(coords)
        if segments:
            results.append((float(lvl), segments))
    return results


def export_contours_geojson(contours, src_crs: CRS, out_path: Path):
    """
    Write contours to GeoJSON in EPSG:4326 (WGS84) without Fiona/GDAL.
    contours: list[(level, [segments as [(x,y),...] ])] in src_crs.
    """
    to_wgs = None
    if src_crs and not src_crs.is_geographic:
        to_wgs = Transformer.from_crs(src_crs, CRS.from_epsg(4326), always_xy=True)

    feats = []
    for level, segs in contours:
        for seg in segs:
            if to_wgs:
                coords = [to_wgs.transform(x, y) for x, y in seg]
            else:
                coords = seg  # already lon/lat
            feats.append(
                {
                    "type": "Feature",
                    "properties": {"elev": level},
                    "geometry": {"type": "LineString", "coordinates": coords},
                }
            )

    fc = {"type": "FeatureCollection", "features": feats}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f)


def project_gdf_vsafe(gdf, to_crs=None):
    """OSMnx project_gdf that works across versions (returns gdf, crs)."""
    if to_crs is None:
        proj = ox.projection.project_gdf(gdf)
    else:
        proj = ox.projection.project_gdf(gdf, to_crs=to_crs)
    if isinstance(proj, tuple):  # old OSMnx returned (gdf, crs)
        gdf_proj = proj[0]
        crs_proj = proj[1] if len(proj) > 1 else gdf_proj.crs
    else:  # new OSMnx returns just gdf
        gdf_proj = proj
        crs_proj = gdf_proj.crs
    return gdf_proj, crs_proj


def edges_gdf_from_graph(G):
    """Return edges GeoDataFrame across OSMnx versions."""
    out = ox.graph_to_gdfs(G, nodes=False, edges=True)
    if isinstance(out, tuple):  # old: (nodes, edges) or just (edges,)
        if len(out) == 2:
            return out[1]
        return out[0]
    return out  # new: just the edges gdf


def fetch_osm_layers(bbox_poly_ll, buffer_m, src_crs):
    """
    bbox_poly_ll: (minx, miny, maxx, maxy) in DEM CRS.
    Returns (roads_gdf, water_gdf) in DEM CRS.
    """
    import geopandas as gpd
    from pyproj import CRS
    from shapely.geometry import box

    crs_src = CRS.from_user_input(src_crs)

    minx, miny, maxx, maxy = bbox_poly_ll
    bbox_geom = gpd.GeoSeries([box(minx, miny, maxx, maxy)], crs=crs_src)

    # Project to a local metric CRS, buffer in meters, then back to WGS84 for OSMnx
    bbox_proj, crs_m = project_gdf_vsafe(bbox_geom)
    bbox_proj = bbox_proj.buffer(
        buffer_m
    ).total_bounds  # (minx, miny, maxx, maxy) in meters
    bbox_proj = gpd.GeoSeries([box(*bbox_proj)], crs=crs_m)
    bbox_ll, _ = project_gdf_vsafe(bbox_proj, to_crs=4326)
    west, south, east, north = bbox_ll.total_bounds

    # Roads (edges only)
    G = ox.graph_from_bbox(north, south, east, west, network_type="drive")
    roads = edges_gdf_from_graph(G)

    # Waterways
    tags = {"waterway": ["river", "stream", "canal", "drain"]}
    water = ox.geometries_from_bbox(north, south, east, west, tags)

    # Reproject to DEM CRS
    if roads is not None and not roads.empty:
        roads = roads.to_crs(crs_src)
    if water is not None and not water.empty:
        water = water.to_crs(crs_src)

    return roads, water


def fetch_osm_places(bbox_poly_ll, buffer_m, src_crs):
    """
    bbox_poly_ll: (minx, miny, maxx, maxy) in DEM CRS.
    Returns places GeoDataFrame (points) in DEM CRS.
    """
    import geopandas as gpd
    from pyproj import CRS
    from shapely.geometry import box

    crs_src = CRS.from_user_input(src_crs)

    minx, miny, maxx, maxy = bbox_poly_ll
    bbox_geom = gpd.GeoSeries([box(minx, miny, maxx, maxy)], crs=crs_src)

    # Buffer in a projected metric CRS, then go to WGS84 to query OSM
    bbox_proj, crs_m = project_gdf_vsafe(bbox_geom)
    bbox_proj = bbox_proj.buffer(buffer_m).total_bounds
    bbox_proj = gpd.GeoSeries([box(*bbox_proj)], crs=crs_m)
    bbox_ll, _ = project_gdf_vsafe(bbox_proj, to_crs=4326)
    west, south, east, north = bbox_ll.total_bounds

    # Places (reduced set)
    tags = {"place": PLACE_TYPES}
    places = ox.geometries_from_bbox(north, south, east, west, tags)

    # Keep only points (labels)
    if places is not None and not places.empty:
        places = places[places.geometry.type.isin(["Point", "MultiPoint"])]
        if not places.empty:
            places = places.to_crs(crs_src)
    return places


def _utm_epsg_from_lonlat(lon: float, lat: float) -> int:
    zone = int(math.floor((lon + 180) / 6) + 1)
    return (32600 if lat >= 0 else 32700) + zone  # 326 = north, 327 = south


def get_local_utm_crs(crs_src, extent):
    """Pick a local UTM CRS from map extent center."""
    crs_src = CRS.from_user_input(crs_src) if not isinstance(crs_src, CRS) else crs_src
    crs_wgs84 = CRS.from_epsg(4326)
    xmin, xmax, ymin, ymax = extent
    cx, cy = (xmin + xmax) * 0.5, (ymin + ymax) * 0.5
    lon, lat = Transformer.from_crs(crs_src, crs_wgs84, always_xy=True).transform(cx, cy)
    return CRS.from_epsg(_utm_epsg_from_lonlat(lon, lat))


def thin_places_gdf(places_gdf, crs_src, extent, min_spacing_km=5.0, max_per_cat=None):
    """Greedy thinning by min spacing (meters) with priority by type, capital, population."""
    if places_gdf is None or places_gdf.empty:
        return places_gdf
    crs_utm = get_local_utm_crs(crs_src, extent)
    gdf_m = places_gdf.to_crs(crs_utm)
    min_dist_m = float(min_spacing_km) * 1000.0
    priority_rank = {"city": 0, "town": 1, "village": 2, "hamlet": 3, "suburb": 4, "neighbourhood": 5, "locality": 6}

    # Build sortable items
    items = []
    for idx, row in gdf_m.iterrows():
        cat = str(row.get("place") or "")
        if PLACE_TYPES and cat and cat not in PLACE_TYPES:
            continue
        pop = row.get("population")
        try:
            pop_val = float(pop)
        except Exception:
            pop_val = 0.0
        is_capital = str(row.get("capital") or "").lower() in (
            "yes", "1", "2", "3", "4", "country", "state", "region", "province"
        )
        pri = priority_rank.get(cat, 999) - (1 if is_capital else 0)
        items.append((pri, -pop_val, idx, row.geometry, cat))
    items.sort()

    kept_idx, kept_geoms, per_cat = [], [], {}
    for pri, neg_pop, idx, geom, cat in items:
        if max_per_cat:
            cap = max_per_cat.get(cat, None)
            if cap is not None and per_cat.get(cat, 0) >= cap:
                continue
        ok = True
        for g in kept_geoms:
            if geom.distance(g) < min_dist_m:
                ok = False
                break
        if ok:
            kept_idx.append(idx)
            kept_geoms.append(geom)
            per_cat[cat] = per_cat.get(cat, 0) + 1

    return places_gdf.loc[kept_idx].copy()


def add_scalebar(ax, crs_src, extent, length_m=2000):
    """
    Draw a scalebar of length_m meters using a local UTM derived from map center.
    Works regardless of the DEM/map CRS; no OSMnx/GDAL needed.
    """
    xmin, xmax, ymin, ymax = extent
    # Place bar near bottom-left
    x0_plot = xmin + 0.10 * (xmax - xmin)
    y0_plot = ymin + 0.08 * (ymax - ymin)

    # Center point in map CRS
    cx = (xmin + xmax) * 0.5
    cy = (ymin + ymax) * 0.2

    # Ensure CRS objects
    crs_src = CRS.from_user_input(crs_src) if not isinstance(crs_src, CRS) else crs_src
    crs_wgs84 = CRS.from_epsg(4326)

    # Get center in lon/lat for UTM choice
    to_wgs = Transformer.from_crs(crs_src, crs_wgs84, always_xy=True)
    center_lon, center_lat = map(float, to_wgs.transform(cx, cy))

    # Local UTM at center
    utm_epsg = _utm_epsg_from_lonlat(center_lon, center_lat)
    crs_utm = CRS.from_epsg(utm_epsg)

    # Transformers
    wgs_to_utm = Transformer.from_crs(crs_wgs84, crs_utm, always_xy=True)
    utm_to_wgs = Transformer.from_crs(crs_utm, crs_wgs84, always_xy=True)
    wgs_to_src = Transformer.from_crs(crs_wgs84, crs_src, always_xy=True)

    # Anchor point for bar in lon/lat: project plot anchor -> lon/lat
    # (Use plot anchor so the bar sits at the chosen screen location)
    to_wgs_anchor = Transformer.from_crs(crs_src, crs_wgs84, always_xy=True)
    lon0, lat0 = map(float, to_wgs_anchor.transform(x0_plot, y0_plot))

    # Move length_m east in UTM, holding northing constant
    x0_m, y0_m = wgs_to_utm.transform(lon0, lat0)
    x1_m = float(x0_m + length_m)
    y1_m = float(y0_m)

    lon1, lat1 = utm_to_wgs.transform(x1_m, y1_m)

    # Back to map CRS for plotting
    px0, py0 = wgs_to_src.transform(lon0, lat0)
    px1, py1 = wgs_to_src.transform(lon1, lat1)

    # Draw bar
    ax.plot([px0, px1], [py0, py0], color="black", lw=3)
    ax.text(
        (px0 + px1) / 2.0,
        py0,
        f"{int(length_m/1000)} km",
        va="bottom",
        ha="center",
        fontsize=9,
        fontweight="bold",
    )


def add_north_arrow(ax, extent, size_frac=0.05):
    (xmin, xmax, ymin, ymax) = extent[0], extent[1], extent[2], extent[3]
    dx, dy = xmax - xmin, ymax - ymin
    x = xmin + 0.92 * dx
    y = ymin + 0.15 * dy
    size = size_frac * dy
    ax.annotate("N", (x, y + 1.2 * size), ha="center", fontsize=12, fontweight="bold")
    ax.arrow(
        x,
        y,
        0,
        size,
        width=size * 0.05,
        head_width=size * 0.2,
        head_length=size * 0.3,
        color="k",
        length_includes_head=True,
    )


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    src, dem = read_dem(DEM_PATH)
    crs_src = CRS.from_wkt(src.crs.to_wkt()) if src.crs else CRS.from_epsg(4326)
    X, Y = grid_xy(src.transform, src.width, src.height)
    extent = (src.bounds.left, src.bounds.right, src.bounds.bottom, src.bounds.top)

    # Optional: slope/aspect via PyDEM if you want to inspect/derive other terrain layers
    if HAS_PYDEM:
        try:
            _ = DEMProcessor(DEM_PATH).calc_slopes_directions()
        except Exception:
            pass

    # Hillshade
    hs = hillshade_from_dem(dem)

    # Contours (no GDAL)
    contours = contour_segments_from_dem(X, Y, dem, CONTOUR_INTERVAL)

    # Index flag for styling when plotting
    def is_index(level):
        return (round(level / INDEX_INTERVAL) * INDEX_INTERVAL) == level

    # Export contours as GeoJSON (in WGS84)
    export_contours_geojson(contours, crs_src, OUT_DIR / f"{DEM_FILENAME}_contours.geojson")

    # OSM layers
    bbox = (src.bounds.left, src.bounds.bottom, src.bounds.right, src.bounds.top)
    roads_gdf, water_gdf = fetch_osm_layers(bbox, OSM_BUFFER_M, crs_src)

    # ---- Plot ----
    fig = plt.figure(figsize=(11.7, 8.3))  # A4 landscape
    ax = plt.gca()

    # Hillshade backdrop (lighter via alpha)
    ax.imshow(hs, extent=extent, origin="upper", alpha=0.75)

    # Plot contours (thin + index)
    for level, segs in contours:
        lw = 0.8 if is_index(level) else 0.4
        alpha = 0.95 if is_index(level) else 0.6
        for seg in segs:
            xs, ys = zip(*seg)
            ax.plot(xs, ys, color="black", lw=lw, alpha=alpha)

    # OSM roads/water
    if roads_gdf is not None and not roads_gdf.empty:
        # roads_gdf.geometry is shapely; iterate for speed, avoid .plot() overhead
        for geom in roads_gdf.geometry:
            if geom is None:
                continue
            if geom.geom_type == "LineString":
                x, y = geom.xy
                # white casing + dark stroke to stand out
                ax.plot(x, y, lw=2.4, color="white", alpha=1.0, solid_capstyle="round")
                ax.plot(x, y, lw=1.2, color="#2b2b2b", alpha=1.0, solid_capstyle="round")
            elif geom.geom_type == "MultiLineString":
                for part in geom.geoms:
                    x, y = part.xy
                    ax.plot(x, y, lw=2.4, color="white", alpha=1.0, solid_capstyle="round")
                    ax.plot(x, y, lw=1.2, color="#2b2b2b", alpha=1.0, solid_capstyle="round")

    if water_gdf is not None and not water_gdf.empty:
        for geom in water_gdf.geometry:
            if geom is None:
                continue
            gtype = geom.geom_type
            if gtype == "LineString":
                x, y = geom.xy
                ax.plot(x, y, lw=0.8, color="#1f78b4", alpha=0.95)
            elif gtype == "MultiLineString":
                for part in geom.geoms:
                    x, y = part.xy
                    ax.plot(x, y, lw=0.8, color="#1f78b4", alpha=0.95)
            elif gtype in ("Polygon", "MultiPolygon"):
                # Optional: draw water areas lightly
                try:
                    from descartes import PolygonPatch  # optional

                    if gtype == "Polygon":
                        patch = PolygonPatch(
                            geom, fc="#1f78b4", ec="#1f78b4", alpha=0.25, lw=0
                        )
                        ax.add_patch(patch)
                    else:
                        for part in geom.geoms:
                            patch = PolygonPatch(
                                part, fc="#1f78b4", ec="#1f78b4", alpha=0.25, lw=0
                            )
                            ax.add_patch(patch)
                except Exception:
                    pass

    # Place names
    bbox = (src.bounds.left, src.bounds.bottom, src.bounds.right, src.bounds.top)
    places_gdf = fetch_osm_places(bbox, OSM_BUFFER_M, crs_src)
    if places_gdf is not None and not places_gdf.empty:
        # Thin labels to reduce clutter
        places_gdf = thin_places_gdf(
            places_gdf, crs_src, extent,
            min_spacing_km=MIN_LABEL_SPACING_KM,
            max_per_cat=MAX_LABELS_PER_CATEGORY,
        )

        import matplotlib.patheffects as patheffects
        halo = [patheffects.withStroke(linewidth=3.0, foreground="white")]
        # Style by category
        style = {
            "city": dict(size=12, weight="bold"),
            "town": dict(size=11, weight="bold"),
            "village": dict(size=10, weight="normal"),
        }
        order = [c for c in PLACE_TYPES if c in style]

        for cat in order:
            subset = places_gdf[places_gdf.get("place") == cat] if "place" in places_gdf.columns else places_gdf
            for _, row in subset.iterrows():
                geom = row.geometry
                name = row.get("name") or row.get("name:en")
                if not name or geom is None:
                    continue
                if geom.geom_type == "Point":
                    x, y = geom.x, geom.y
                    ax.text(
                        x, y, name,
                        ha="center", va="center",
                        fontsize=style.get(cat, dict(size=9))["size"],
                        fontweight=style.get(cat, dict(weight="normal"))["weight"],
                        color="black", path_effects=halo,
                    )
                elif geom.geom_type == "MultiPoint":
                    for p in geom.geoms:
                        x, y = p.x, p.y
                        ax.text(
                            x, y, name,
                            ha="center", va="center",
                            fontsize=style.get(cat, dict(size=9))["size"],
                            fontweight=style.get(cat, dict(weight="normal"))["weight"],
                            color="black", path_effects=halo,
                        )

    # Axes, grid, adornments
    ax.set_xlim([extent[0], extent[1]])
    ax.set_ylim([extent[2], extent[3]])
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")
    ax.grid(True, linestyle="--", alpha=0.3)

    ax.set_title(f"{TITLE}\n{SUBTITLE}", loc="left", fontsize=14, fontweight="bold")
    add_north_arrow(ax, extent, size_frac=0.07)
    add_scalebar(ax, crs_src, extent, length_m=2000)

    # Legend (simple)
    import matplotlib.lines as mlines

    roads_line = mlines.Line2D([], [], color="#555555", lw=0.8, label="Roads")
    water_line = mlines.Line2D([], [], color="#1f78b4", lw=0.8, label="Waterways")
    ax.legend(handles=[roads_line, water_line], loc="lower left")

    plt.tight_layout()
    out_pdf = OUT_DIR / f"{DEM_FILENAME}_toposheet.pdf"
    plt.savefig(out_pdf, dpi=300)
    plt.close(fig)

    print(f"Saved: {out_pdf}")
    print(f"Saved: {OUT_DIR / f'{DEM_FILENAME}_contours.geojson'}")
