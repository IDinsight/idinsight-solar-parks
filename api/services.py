"""
Geospatial processing services with PostgreSQL/PostGIS and file storage
"""
import json
import tempfile
import uuid
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from shapely import MultiPolygon
from shapely.geometry import Polygon, shape, mapping
from shapely.strtree import STRtree
from sklearn.cluster import DBSCAN
from sqlalchemy.orm import Session
from geoalchemy2.shape import from_shape, to_shape

from config import settings, AVAILABLE_LAYERS
from database import (
    ProjectModel,
    KhasraModel,
    LayerModel,
    LayerFeatureModel,
    ParcelModel,
    get_db,
)
from storage import file_storage
from models import (
    ClusteringRequest,
    ExportFormat,
    ExportType,
    KhasraStats,
    LayerConfig,
    LayerInfo,
    LayerType,
    ParcelStats,
    ProjectStatus,
)


# ============ Project CRUD Operations ============

def create_project(db: Session, name: str, location: str, description: Optional[str] = None) -> str:
    """Create a new project in the database"""
    project_id = str(uuid.uuid4())
    
    project = ProjectModel(
        id=project_id,
        name=name,
        location=location,
        description=description,
        status=ProjectStatus.CREATED,
    )
    
    db.add(project)
    db.commit()
    db.refresh(project)
    
    return project_id


def get_project(db: Session, project_id: str) -> Optional[ProjectModel]:
    """Get project by ID from database"""
    return db.query(ProjectModel).filter(ProjectModel.id == project_id).first()


def list_projects(db: Session) -> List[ProjectModel]:
    """List all projects from database"""
    return db.query(ProjectModel).order_by(ProjectModel.created_at.desc()).all()


def delete_project(db: Session, project_id: str) -> bool:
    """Delete a project and all associated data"""
    project = get_project(db, project_id)
    if not project:
        return False
    
    # Delete files
    file_storage.delete_project_files(project_id)
    
    # Delete from database (cascades to related tables)
    db.delete(project)
    db.commit()
    
    return True


def update_project_status(db: Session, project_id: str, status: ProjectStatus):
    """Update project status"""
    project = get_project(db, project_id)
    if project:
        project.status = status
        project.updated_at = datetime.utcnow()
        db.commit()


# ============ Geometry Utilities ============

def clean_non_polygons(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clean Geometry Collections and MultiPolygons by keeping only Polygons"""
    def _clean_geom(geom):
        if geom is None:
            return Polygon()

        if geom.geom_type == "Polygon":
            return geom

        polygons = []
        if hasattr(geom, "geoms"):
            for g in geom.geoms:
                if g.geom_type == "Polygon":
                    polygons.append(g)

        if not polygons:
            return Polygon()
        if len(polygons) == 1:
            return polygons[0]
        return MultiPolygon(polygons)

    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.apply(_clean_geom)
    return gdf


def difference_overlay_without_discard(
    gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Perform difference overlay without discarding rows that don't intersect"""
    overlay_gdf = gpd.overlay(
        gdf1.reset_index(names="original_index"),
        gdf2,
        how="difference",
        keep_geom_type=True,
    )

    gdf1_v2 = gdf1.copy()
    gdf1_v2["geometry"] = Polygon()
    gdf1_v2.loc[overlay_gdf["original_index"], "geometry"] = overlay_gdf.geometry.values
    gdf1_v2 = clean_non_polygons(gdf1_v2)

    return gdf1_v2


def ensure_multipolygon(geom):
    """Ensure geometry is a MultiPolygon for PostGIS storage"""
    if geom is None or geom.is_empty:
        return None
    if geom.geom_type == "Polygon":
        return MultiPolygon([geom])
    elif geom.geom_type == "MultiPolygon":
        return geom
    elif geom.geom_type == "GeometryCollection":
        polygons = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        if polygons:
            all_polys = []
            for p in polygons:
                if p.geom_type == "Polygon":
                    all_polys.append(p)
                else:
                    all_polys.extend(p.geoms)
            return MultiPolygon(all_polys) if all_polys else None
    return None


# ============ Khasra Processing ============

def process_khasra_upload(
    db: Session,
    file_content: bytes,
    filename: str,
    project_id: str,
    id_column: Optional[str] = None,
) -> Dict[str, Any]:
    """Process uploaded khasra file and store in database + files"""
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    # Read file
    file_extension = Path(filename).suffix.lower()
    
    with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmp_file:
        tmp_file.write(file_content)
        tmp_path = tmp_file.name

    try:
        if file_extension == ".kml":
            gdf = gpd.read_file(tmp_path, driver="KML", engine="pyogrio")
        elif file_extension in [".geojson", ".json"]:
            gdf = gpd.read_file(tmp_path, engine="pyogrio")
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    finally:
        Path(tmp_path).unlink()

    # Ensure CRS
    if gdf.crs is None:
        gdf = gdf.set_crs(settings.DEFAULT_CRS)
    
    gdf_4326 = gdf.to_crs(settings.DEFAULT_CRS)
    gdf_projected = gdf_4326.to_crs(settings.INDIA_PROJECTED_CRS)

    # Set up ID columns - try multiple common column names
    khasra_id_assigned = False
    
    # First priority: user-specified column
    if id_column and id_column in gdf_projected.columns:
        gdf_projected["Khasra ID"] = gdf_projected[id_column].astype(str)
        khasra_id_assigned = True
    
    # Second priority: try common ID column names
    if not khasra_id_assigned:
        common_id_columns = [
            "Name", "name", "NAME",
            "id", "ID", "Id",
            "khasra_id", "Khasra_ID", "KHASRA_ID", "khasra_no", "Khasra_No",
            "parcel_id", "Parcel_ID", "PARCEL_ID",
            "plot_id", "Plot_ID", "PLOT_ID",
            "feature_id", "Feature_ID", "FEATURE_ID",
            "fid", "FID",
            "OBJECTID", "ObjectID", "objectid",
        ]
        for col in common_id_columns:
            if col in gdf_projected.columns:
                gdf_projected["Khasra ID"] = gdf_projected[col].astype(str)
                khasra_id_assigned = True
                break
    
    # Last resort: auto-generate sequential IDs
    if not khasra_id_assigned:
        gdf_projected["Khasra ID"] = [f"KHASRA_{i+1:04d}" for i in range(len(gdf_projected))]

    # Create unique IDs (project-scoped unique identifier)
    gdf_projected["Khasra ID (Unique)"] = gdf_projected["Khasra ID"] + "_" + [
        str(i) for i in range(len(gdf_projected))
    ]

    # Calculate areas
    gdf_projected["Original Area (ha)"] = gdf_projected.geometry.area / 10_000

    # Prepare 4326 version for storage
    gdf_4326["Khasra ID"] = gdf_projected["Khasra ID"]
    gdf_4326["Khasra ID (Unique)"] = gdf_projected["Khasra ID (Unique)"]
    gdf_4326["Original Area (ha)"] = gdf_projected["Original Area (ha)"]

    # Save to file storage
    khasras_file_path = file_storage.save_geodataframe(
        gdf_4326, project_id, "khasras.parquet"
    )
    khasras_projected_file_path = file_storage.save_geodataframe(
        gdf_projected, project_id, "khasras_projected.parquet"
    )

    # Store in database (for querying)
    # First, delete any existing khasras for this project
    db.query(KhasraModel).filter(KhasraModel.project_id == project_id).delete()
    
    for idx, row in gdf_4326.iterrows():
        geom = ensure_multipolygon(row.geometry)
        if geom is None:
            continue
            
        khasra = KhasraModel(
            project_id=project_id,
            khasra_id=row["Khasra ID"],
            khasra_id_unique=row["Khasra ID (Unique)"],
            geometry=from_shape(geom, srid=4326),
            original_area_ha=row["Original Area (ha)"],
            properties={k: str(v) for k, v in row.drop(["geometry", "Khasra ID", "Khasra ID (Unique)", "Original Area (ha)"]).items() if pd.notna(v)},
        )
        db.add(khasra)

    # Update project
    bounds = gdf_4326.total_bounds
    project.khasras_file_path = khasras_file_path
    project.khasra_count = len(gdf_projected)
    project.total_area_ha = round(gdf_projected["Original Area (ha)"].sum(), 2)
    project.bounds_json = {
        "minx": round(bounds[0], 6),
        "miny": round(bounds[1], 6),
        "maxx": round(bounds[2], 6),
        "maxy": round(bounds[3], 6),
    }
    project.status = ProjectStatus.KHASRAS_UPLOADED
    project.updated_at = datetime.utcnow()
    
    db.commit()

    return {
        "khasra_count": len(gdf_projected),
        "total_area_ha": project.total_area_ha,
        "bounds": project.bounds_json,
        "crs": settings.DEFAULT_CRS,
    }


def get_khasras_gdf(db: Session, project_id: str, projected: bool = False) -> Optional[gpd.GeoDataFrame]:
    """Load khasras GeoDataFrame from file storage"""
    project = get_project(db, project_id)
    if not project or not project.khasras_file_path:
        return None
    
    if projected:
        gdf = file_storage.load_geodataframe(
            str(Path(project.khasras_file_path).parent / "khasras_projected.parquet")
        )
    else:
        gdf = file_storage.load_geodataframe(project.khasras_file_path)
    
    return gdf


# ============ Layer Processing ============

def process_custom_layer_upload(
    db: Session,
    file_content: bytes,
    filename: str,
    project_id: str,
    layer_name: str,
    is_unusable: bool = True,
) -> LayerInfo:
    """Process an uploaded custom layer file"""
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    gdf = get_khasras_gdf(db, project_id, projected=False)
    if gdf is None:
        raise ValueError("Khasras must be uploaded first")
    
    # Ensure khasras are projected to India CRS for intersection
    gdf = gdf.to_crs(settings.INDIA_PROJECTED_CRS)

    # Read the layer file
    file_extension = Path(filename).suffix.lower()
    
    with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmp_file:
        tmp_file.write(file_content)
        tmp_path = tmp_file.name

    try:
        if file_extension == ".kml":
            layer_gdf = gpd.read_file(tmp_path, driver="KML", engine="pyogrio")
        elif file_extension in [".geojson", ".json"]:
            layer_gdf = gpd.read_file(tmp_path, engine="pyogrio")
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    finally:
        Path(tmp_path).unlink()

    # Ensure CRS and project
    if layer_gdf.crs is None:
        layer_gdf = layer_gdf.set_crs(settings.DEFAULT_CRS)
    layer_gdf = layer_gdf.to_crs(settings.INDIA_PROJECTED_CRS)

    # Intersect with khasras
    layer_overlap_gdf = gpd.overlay(layer_gdf, gdf, how="intersection")
    layer_overlap_gdf = layer_overlap_gdf.dissolve(by="Khasra ID (Unique)").reset_index()

    # Calculate area
    area_col = f"{'Unusable' if is_unusable else 'Unavailable'} Area - {layer_name} (ha)"
    layer_overlap_gdf[area_col] = layer_overlap_gdf.area / 10_000

    # Save to file storage (backup)
    layer_file_path = file_storage.save_layer(
        layer_overlap_gdf.to_crs(settings.DEFAULT_CRS),
        project_id,
        layer_name,
    )

    # Store layer metadata in database
    layer = LayerModel(
        project_id=project_id,
        name=layer_name,
        layer_type=LayerType.CUSTOM.value,
        is_unusable=is_unusable,
        file_path=layer_file_path,
        feature_count=len(layer_overlap_gdf),
        total_area_ha=round(layer_overlap_gdf[area_col].sum(), 2),
        parameters={"area_col": area_col},
    )
    db.add(layer)
    db.flush()  # Get the layer.id before adding features

    # Store per-khasra layer features in database
    layer_overlap_4326 = layer_overlap_gdf.to_crs(settings.DEFAULT_CRS)
    for idx, row in layer_overlap_4326.iterrows():
        geom = row.geometry
        # Convert to MultiPolygon if needed
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        elif geom.geom_type == "GeometryCollection":
            polygons = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
            if polygons:
                all_polys = []
                for p in polygons:
                    if p.geom_type == "Polygon":
                        all_polys.append(p)
                    else:
                        all_polys.extend(p.geoms)
                geom = MultiPolygon(all_polys) if all_polys else None
            else:
                geom = None
        
        if geom is None or geom.is_empty:
            continue
        
        feature = LayerFeatureModel(
            layer_id=layer.id,
            khasra_id_unique=row["Khasra ID (Unique)"],
            geometry=from_shape(geom, srid=4326),
            area_ha=round(row[area_col], 4),
            properties={
                "khasra_id": row.get("Khasra ID", ""),
                "layer_name": layer_name,
                "is_unusable": is_unusable,
            },
        )
        db.add(feature)
    
    project.status = ProjectStatus.LAYERS_ADDED
    project.updated_at = datetime.utcnow()
    db.commit()

    return LayerInfo(
        layer_type=LayerType.CUSTOM.value,
        name=layer_name,
        description=f"Custom uploaded layer: {layer_name}",
        is_unusable=is_unusable,
        parameters={},
        area_ha=round(layer_overlap_gdf[area_col].sum(), 2),
        feature_count=len(layer_overlap_gdf),
    )


def get_project_layers(db: Session, project_id: str) -> List[LayerModel]:
    """Get all layers for a project"""
    return db.query(LayerModel).filter(LayerModel.project_id == project_id).all()


def load_layer_gdf(db: Session, project_id: str, layer_name: str) -> Optional[gpd.GeoDataFrame]:
    """Load a layer's GeoDataFrame from file storage"""
    layer = db.query(LayerModel).filter(
        LayerModel.project_id == project_id,
        LayerModel.name == layer_name
    ).first()
    
    if not layer or not layer.file_path:
        return None
    
    return file_storage.load_geodataframe(layer.file_path)


def get_layer_features_gdf(db: Session, project_id: str, layer_name: str) -> Optional[gpd.GeoDataFrame]:
    """Load a layer's features from the database as a GeoDataFrame"""
    layer = db.query(LayerModel).filter(
        LayerModel.project_id == project_id,
        LayerModel.name == layer_name
    ).first()
    
    if not layer:
        return None
    
    features = db.query(LayerFeatureModel).filter(
        LayerFeatureModel.layer_id == layer.id
    ).all()
    
    if not features:
        return None
    
    data = []
    for f in features:
        geom = to_shape(f.geometry)
        data.append({
            "khasra_id_unique": f.khasra_id_unique,
            "area_ha": f.area_ha,
            "geometry": geom,
            **(f.properties or {}),
        })
    
    gdf = gpd.GeoDataFrame(data, crs=settings.DEFAULT_CRS)
    return gdf


def get_layer_features_for_khasra(db: Session, project_id: str, khasra_id_unique: str) -> List[Dict]:
    """Get all layer features that intersect a specific khasra"""
    layers = get_project_layers(db, project_id)
    
    result = []
    for layer in layers:
        features = db.query(LayerFeatureModel).filter(
            LayerFeatureModel.layer_id == layer.id,
            LayerFeatureModel.khasra_id_unique == khasra_id_unique
        ).all()
        
        for f in features:
            result.append({
                "layer_name": layer.name,
                "layer_type": layer.layer_type,
                "is_unusable": layer.is_unusable,
                "area_ha": f.area_ha,
                "properties": f.properties,
            })
    
    return result


# ============ Area Calculations ============

def calculate_usable_areas(db: Session, project_id: str) -> gpd.GeoDataFrame:
    """Calculate usable and available areas after applying all layers"""
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    gdf = get_khasras_gdf(db, project_id, projected=True)
    if gdf is None:
        raise ValueError("Khasras must be uploaded first")

    available_gdf = gdf.copy()
    available_gdf["Original Area (ha)"] = available_gdf.geometry.area / 10_000

    # Get all layers
    layers = get_project_layers(db, project_id)

    # Apply unusable layers (cut out from geometry)
    for layer in layers:
        if layer.is_unusable:
            layer_gdf = file_storage.load_geodataframe(layer.file_path)
            if layer_gdf is not None and len(layer_gdf) > 0:
                layer_gdf = layer_gdf.to_crs(settings.INDIA_PROJECTED_CRS)
                available_gdf = difference_overlay_without_discard(available_gdf, layer_gdf)

    available_gdf["Usable Area (ha)"] = available_gdf.area / 10_000
    available_gdf["Unusable Area (ha)"] = (
        available_gdf["Original Area (ha)"] - available_gdf["Usable Area (ha)"]
    )
    available_gdf["Usable Area (%)"] = (
        available_gdf["Usable Area (ha)"] / available_gdf["Original Area (ha)"] * 100
    )
    available_gdf["Unusable Area (%)"] = (
        available_gdf["Unusable Area (ha)"] / available_gdf["Original Area (ha)"] * 100
    )

    # Apply unavailable layers
    for layer in layers:
        if not layer.is_unusable:
            layer_gdf = file_storage.load_geodataframe(layer.file_path)
            if layer_gdf is not None and len(layer_gdf) > 0:
                layer_gdf = layer_gdf.to_crs(settings.INDIA_PROJECTED_CRS)
                available_gdf = difference_overlay_without_discard(available_gdf, layer_gdf)

    available_gdf["Usable and Available Area (ha)"] = available_gdf.area / 10_000
    available_gdf["Usable but Unavailable Area (ha)"] = (
        available_gdf["Usable Area (ha)"] - available_gdf["Usable and Available Area (ha)"]
    )
    available_gdf["Usable and Available Area (%)"] = (
        available_gdf["Usable and Available Area (ha)"]
        / available_gdf["Original Area (ha)"]
        * 100
    )
    available_gdf["Usable but Unavailable Area (%)"] = (
        available_gdf["Usable but Unavailable Area (ha)"]
        / available_gdf["Original Area (ha)"]
        * 100
    )

    # Merge in layer-specific area columns
    for layer in layers:
        layer_gdf = file_storage.load_geodataframe(layer.file_path)
        if layer_gdf is not None and layer.parameters:
            area_col = layer.parameters.get("area_col")
            if area_col and area_col in layer_gdf.columns:
                merge_cols = ["Khasra ID (Unique)", area_col]
                if all(c in layer_gdf.columns for c in merge_cols):
                    available_gdf = available_gdf.merge(
                        layer_gdf[merge_cols],
                        on="Khasra ID (Unique)",
                        how="left",
                    )
                    available_gdf[area_col] = available_gdf[area_col].fillna(0)

    # Save stats file
    stats_file_path = file_storage.save_geodataframe(
        available_gdf.to_crs(settings.DEFAULT_CRS),
        project_id,
        "khasras_stats.parquet"
    )
    
    project.stats_file_path = stats_file_path
    project.updated_at = datetime.utcnow()
    db.commit()

    return available_gdf


# ============ Clustering ============

def build_optimised_distance_matrix(
    gdf: gpd.GeoDataFrame,
    max_distance_considered: int,
    n_jobs: int = -1,
) -> np.ndarray:
    """Build an optimized distance matrix for clustering"""
    geometries = gdf.geometry.values
    n = len(geometries)
    tree = STRtree(geometries)

    def _get_distances_for_geom(i, geom, geometries, tree, max_distance_considered):
        distances = []
        candidate_indices = tree.query(geom.buffer(max_distance_considered))
        for j in candidate_indices:
            if i == j:
                continue
            geom_candidate = geometries[j]
            d = geom.distance(geom_candidate)
            distances.append((i, j, d))
        return distances

    results = Parallel(n_jobs=n_jobs)(
        delayed(_get_distances_for_geom)(
            i, geom, geometries, tree, max_distance_considered
        )
        for i, geom in enumerate(geometries)
    )

    distance_matrix = np.full((n, n), 99999)
    for res_list in results:
        for i, j, d in res_list:
            distance_matrix[i, j] = d

    np.fill_diagonal(distance_matrix, 0)
    return distance_matrix


def format_cluster_labels(
    gdf: gpd.GeoDataFrame,
    cluster_id_col: str,
    distance_threshold: Optional[int] = None,
    area_col: str = "Usable Area (ha)",
) -> gpd.GeoDataFrame:
    """Format DBSCAN cluster labels into readable parcel IDs"""
    clustered_rows_df = gdf[gdf[cluster_id_col] != -1]
    
    cluster_labels_with_sizes_df = (
        clustered_rows_df.groupby(cluster_id_col)[area_col].sum()
    ).reset_index()
    
    ordered_cluster_labels_df = cluster_labels_with_sizes_df.sort_values(
        by=area_col, ascending=False
    )

    prefix = f"PARCEL_{distance_threshold}m_" if distance_threshold else "PARCEL_"
    formatted_ids = [f"{prefix}{i+1:04d}" for i in range(len(ordered_cluster_labels_df))]
    ordered_cluster_labels_df["formatted_ids"] = formatted_ids
    
    unclustered_label = f"{prefix}UNCLUSTERED" if distance_threshold else "PARCEL_UNCLUSTERED"
    cluster_mapping = {-1: unclustered_label}
    cluster_mapping.update(
        dict(
            zip(
                ordered_cluster_labels_df[cluster_id_col],
                ordered_cluster_labels_df["formatted_ids"],
            )
        )
    )

    gdf = gdf.copy()
    gdf[cluster_id_col] = gdf[cluster_id_col].map(cluster_mapping)
    return gdf


def cluster_khasras(
    db: Session,
    project_id: str,
    request: ClusteringRequest,
) -> Dict[str, Any]:
    """Cluster khasras into parcels using DBSCAN"""
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    # Load stats GDF or calculate if not exists
    if project.stats_file_path:
        stats_gdf = file_storage.load_geodataframe(project.stats_file_path)
        stats_gdf = stats_gdf.to_crs(settings.INDIA_PROJECTED_CRS)
    else:
        stats_gdf = calculate_usable_areas(db, project_id)

    gdf = get_khasras_gdf(db, project_id, projected=True)

    # Use original geometries for clustering (with stats)
    original_gdf_with_stats = stats_gdf.copy()
    original_gdf_with_stats = original_gdf_with_stats.set_index("Khasra ID (Unique)")
    original_gdf_with_stats["geometry"] = gdf.set_index("Khasra ID (Unique)")["geometry"]
    original_gdf_with_stats = original_gdf_with_stats.reset_index()

    # Build or load distance matrix
    if project.distance_matrix_path:
        distance_matrix = file_storage.load_numpy_array(project.distance_matrix_path)
    else:
        distance_matrix = build_optimised_distance_matrix(
            gdf=original_gdf_with_stats,
            max_distance_considered=settings.MAX_DISTANCE_CONSIDERED,
            n_jobs=-1,
        )
        distance_matrix_path = file_storage.save_numpy_array(
            distance_matrix, project_id, "distance_matrix.npy"
        )
        project.distance_matrix_path = distance_matrix_path

    # Perform clustering
    cluster_id_col = "Parcel ID"
    db_cluster = DBSCAN(
        eps=request.distance_threshold,
        min_samples=request.min_samples,
        metric="precomputed",
    )
    labels = db_cluster.fit_predict(distance_matrix)
    
    gdf_with_cluster_id = original_gdf_with_stats.copy()
    gdf_with_cluster_id[cluster_id_col] = labels
    
    gdf_with_cluster_id = format_cluster_labels(
        gdf=gdf_with_cluster_id,
        cluster_id_col=cluster_id_col,
        distance_threshold=request.distance_threshold,
        area_col="Usable Area (ha)",
    )

    # Save clustered GDF
    clustered_file_path = file_storage.save_geodataframe(
        gdf_with_cluster_id.to_crs(settings.DEFAULT_CRS),
        project_id,
        "khasras_clustered.parquet"
    )
    project.clustered_file_path = clustered_file_path

    # Aggregate to parcel level
    parcel_gdf = aggregate_to_parcels(gdf_with_cluster_id, cluster_id_col)
    
    parcels_file_path = file_storage.save_geodataframe(
        parcel_gdf.to_crs(settings.DEFAULT_CRS),
        project_id,
        "parcels.parquet"
    )
    project.parcels_file_path = parcels_file_path

    # Store parcels in database
    db.query(ParcelModel).filter(ParcelModel.project_id == project_id).delete()
    
    for _, row in parcel_gdf.iterrows():
        geom = ensure_multipolygon(row.geometry)
        parcel = ParcelModel(
            project_id=project_id,
            parcel_id=row[cluster_id_col],
            geometry=from_shape(geom, srid=4326) if geom else None,
            khasra_count=int(row.get("Khasra Count", 0)),
            khasra_ids=row.get("Khasra ID (Unique)", ""),
            original_area_ha=round(row.get("Original Area (ha)", 0), 2),
            usable_area_ha=round(row.get("Usable Area (ha)", 0), 2),
            usable_available_area_ha=round(row.get("Usable and Available Area (ha)", 0), 2),
            unusable_area_ha=round(row.get("Unusable Area (ha)", 0), 2),
            building_count=int(row.get("Building Count", 0)) if "Building Count" in row else 0,
        )
        db.add(parcel)

    project.status = ProjectStatus.CLUSTERED
    project.updated_at = datetime.utcnow()
    db.commit()

    # Build response
    clustered_count = len(gdf_with_cluster_id[~gdf_with_cluster_id[cluster_id_col].str.contains("UNCLUSTERED")])
    unclustered_count = len(gdf_with_cluster_id[gdf_with_cluster_id[cluster_id_col].str.contains("UNCLUSTERED")])

    parcels = []
    for _, row in parcel_gdf.iterrows():
        parcel_stats = ParcelStats(
            parcel_id=row[cluster_id_col],
            khasra_count=int(row.get("Khasra Count", 0)),
            khasra_ids=row.get("Khasra ID (Unique)", "").split(", "),
            original_area_ha=round(row.get("Original Area (ha)", 0), 2),
            usable_area_ha=round(row.get("Usable Area (ha)", 0), 2),
            usable_area_percent=round(row.get("Usable Area (%)", 0), 2),
            usable_available_area_ha=round(row.get("Usable and Available Area (ha)", 0), 2),
            usable_available_area_percent=round(row.get("Usable and Available Area (%)", 0), 2),
            unusable_area_ha=round(row.get("Unusable Area (ha)", 0), 2),
            building_count=int(row.get("Building Count", 0)) if "Building Count" in row else 0,
        )
        parcels.append(parcel_stats)

    return {
        "total_parcels": len(parcel_gdf),
        "clustered_khasras": clustered_count,
        "unclustered_khasras": unclustered_count,
        "parcels": parcels,
    }


def aggregate_to_parcels(
    gdf: gpd.GeoDataFrame,
    cluster_id_col: str,
) -> gpd.GeoDataFrame:
    """Aggregate khasra data to parcel level"""
    exclude_cols = [
        "Khasra ID",
        "geometry",
        "Unusable Area (%)",
        "Usable Area (%)",
        "Usable and Available Area (%)",
        "Usable but Unavailable Area (%)",
    ]
    
    numeric_cols = [
        col for col in gdf.select_dtypes(include=[np.number]).columns
        if col not in exclude_cols and col != cluster_id_col
    ]

    agg_dict = {col: "sum" for col in numeric_cols}
    pivot_df = gdf.groupby(cluster_id_col).agg(agg_dict).round(2).reset_index()

    count_df = gdf.groupby(cluster_id_col).agg(
        Khasra_Count=("Khasra ID (Unique)", "size"),
        Khasra_IDs=("Khasra ID (Unique)", lambda x: ", ".join(list(x))),
    ).reset_index()
    count_df.rename(
        columns={"Khasra_Count": "Khasra Count", "Khasra_IDs": "Khasra ID (Unique)"},
        inplace=True,
    )
    
    pivot_df = pivot_df.merge(count_df, on=cluster_id_col)

    # Recalculate percentages
    if "Original Area (ha)" in pivot_df.columns:
        if "Unusable Area (ha)" in pivot_df.columns:
            pivot_df["Unusable Area (%)"] = (
                pivot_df["Unusable Area (ha)"] / pivot_df["Original Area (ha)"] * 100
            ).round(2)
        if "Usable Area (ha)" in pivot_df.columns:
            pivot_df["Usable Area (%)"] = (
                pivot_df["Usable Area (ha)"] / pivot_df["Original Area (ha)"] * 100
            ).round(2)
        if "Usable and Available Area (ha)" in pivot_df.columns:
            pivot_df["Usable and Available Area (%)"] = (
                pivot_df["Usable and Available Area (ha)"]
                / pivot_df["Original Area (ha)"]
                * 100
            ).round(2)
        if "Usable but Unavailable Area (ha)" in pivot_df.columns:
            pivot_df["Usable but Unavailable Area (%)"] = (
                pivot_df["Usable but Unavailable Area (ha)"]
                / pivot_df["Original Area (ha)"]
                * 100
            ).round(2)

    # Add convex hull geometry
    convex_hull_geoms_gdf = (
        gdf.dissolve(by=cluster_id_col)
        .convex_hull.to_frame(name="geometry")
        .reset_index()
    )
    pivot_df = pivot_df.merge(convex_hull_geoms_gdf, on=cluster_id_col)
    parcel_gdf = gpd.GeoDataFrame(pivot_df, geometry="geometry", crs=gdf.crs)

    return parcel_gdf


# ============ Export Functions ============

def export_data(
    db: Session,
    project_id: str,
    export_type: ExportType,
    export_format: ExportFormat,
    include_statistics: bool = True,
) -> Tuple[bytes, str]:
    """Export project data in the specified format"""
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    location = project.location
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    gdfs_to_export = {}
    
    if export_type in [ExportType.KHASRAS, ExportType.ALL]:
        if project.khasras_file_path:
            gdf = file_storage.load_geodataframe(project.khasras_file_path)
            if gdf is not None:
                gdfs_to_export["khasras"] = gdf

    if export_type in [ExportType.KHASRAS_WITH_STATS, ExportType.ALL]:
        if project.stats_file_path:
            gdf = file_storage.load_geodataframe(project.stats_file_path)
            if gdf is not None:
                gdfs_to_export["khasras_with_stats"] = gdf

    if export_type in [ExportType.PARCELS, ExportType.ALL]:
        if project.parcels_file_path:
            gdf = file_storage.load_geodataframe(project.parcels_file_path)
            if gdf is not None:
                gdfs_to_export["parcels"] = gdf

    if export_type in [ExportType.LAYERS, ExportType.ALL]:
        layers = get_project_layers(db, project_id)
        for layer in layers:
            if layer.file_path:
                gdf = file_storage.load_geodataframe(layer.file_path)
                if gdf is not None:
                    gdfs_to_export[f"layer_{layer.name}"] = gdf

    if not gdfs_to_export:
        raise ValueError("No data available to export")

    # Ensure all are in WGS84
    for name, gdf in gdfs_to_export.items():
        if gdf.crs is not None and gdf.crs != "EPSG:4326":
            gdfs_to_export[name] = gdf.to_crs("EPSG:4326")

    # Export based on format
    if export_format == ExportFormat.GEOJSON:
        return export_to_geojson(gdfs_to_export, location, timestamp)
    elif export_format == ExportFormat.KML:
        return export_to_kml(gdfs_to_export, location, timestamp)
    elif export_format == ExportFormat.SHAPEFILE:
        return export_to_shapefile(gdfs_to_export, location, timestamp)
    elif export_format == ExportFormat.PARQUET:
        return export_to_parquet(gdfs_to_export, location, timestamp)
    elif export_format == ExportFormat.CSV:
        return export_to_csv(gdfs_to_export, location, timestamp)
    elif export_format == ExportFormat.EXCEL:
        return export_to_excel(gdfs_to_export, location, timestamp, include_statistics)
    else:
        raise ValueError(f"Unsupported export format: {export_format}")


def export_to_geojson(gdfs: Dict[str, gpd.GeoDataFrame], location: str, timestamp: str) -> Tuple[bytes, str]:
    if len(gdfs) == 1:
        name, gdf = list(gdfs.items())[0]
        filename = f"{location}_{name}_{timestamp}.geojson"
        content = gdf.to_json()
        return content.encode("utf-8"), filename
    else:
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, gdf in gdfs.items():
                content = gdf.to_json()
                zf.writestr(f"{name}.geojson", content)
        
        filename = f"{location}_export_{timestamp}.zip"
        return buffer.getvalue(), filename


def export_to_kml(gdfs: Dict[str, gpd.GeoDataFrame], location: str, timestamp: str) -> Tuple[bytes, str]:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, gdf in gdfs.items():
            with tempfile.NamedTemporaryFile(suffix=".kml", delete=False) as tmp:
                gdf.to_file(tmp.name, driver="KML", engine="pyogrio")
                with open(tmp.name, "rb") as f:
                    zf.writestr(f"{name}.kml", f.read())
                Path(tmp.name).unlink()
    
    filename = f"{location}_export_{timestamp}_kml.zip"
    return buffer.getvalue(), filename


def export_to_shapefile(gdfs: Dict[str, gpd.GeoDataFrame], location: str, timestamp: str) -> Tuple[bytes, str]:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, gdf in gdfs.items():
            with tempfile.TemporaryDirectory() as tmpdir:
                shp_path = Path(tmpdir) / f"{name}.shp"
                gdf.to_file(shp_path, driver="ESRI Shapefile")
                for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                    file_path = Path(tmpdir) / f"{name}{ext}"
                    if file_path.exists():
                        zf.write(file_path, f"{name}/{name}{ext}")
    
    filename = f"{location}_export_{timestamp}_shp.zip"
    return buffer.getvalue(), filename


def export_to_parquet(gdfs: Dict[str, gpd.GeoDataFrame], location: str, timestamp: str) -> Tuple[bytes, str]:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, gdf in gdfs.items():
            parquet_buffer = BytesIO()
            gdf.to_parquet(parquet_buffer)
            zf.writestr(f"{name}.parquet", parquet_buffer.getvalue())
    
    filename = f"{location}_export_{timestamp}.zip"
    return buffer.getvalue(), filename


def export_to_csv(gdfs: Dict[str, gpd.GeoDataFrame], location: str, timestamp: str) -> Tuple[bytes, str]:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, gdf in gdfs.items():
            df = gdf.drop(columns=["geometry"], errors="ignore")
            csv_content = df.to_csv(index=False)
            zf.writestr(f"{name}.csv", csv_content)
    
    filename = f"{location}_export_{timestamp}_csv.zip"
    return buffer.getvalue(), filename


def export_to_excel(gdfs: Dict[str, gpd.GeoDataFrame], location: str, timestamp: str, include_statistics: bool = True) -> Tuple[bytes, str]:
    buffer = BytesIO()
    
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for name, gdf in gdfs.items():
            df = gdf.drop(columns=["geometry"], errors="ignore")
            sheet_name = name[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        if include_statistics:
            summary_data = []
            for name, gdf in gdfs.items():
                summary_data.append({
                    "Layer": name,
                    "Feature Count": len(gdf),
                    "Total Area (ha)": gdf.geometry.area.sum() / 10_000 if "geometry" in gdf.columns else None,
                })
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
    
    filename = f"{location}_export_{timestamp}.xlsx"
    return buffer.getvalue(), filename
