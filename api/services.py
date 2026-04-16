"""
Geospatial processing services with PostgreSQL/PostGIS and file storage
"""

import json
import logging
import sys
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
import shapely
import simplekml
from config import settings
from database import (
    ClusteringRunModel,
    KhasraModel,
    LayerFeatureModel,
    LayerModel,
    ParcelModel,
    ProjectModel,
)
from geoalchemy2.shape import from_shape, to_shape
from joblib import Parallel, delayed
from models import (
    ClusteringRequest,
    ExportFormat,
    LayerInfo,
    LayerType,
    ParcelStats,
    ProjectStatus,
)
from openpyxl.styles import Alignment, Font, PatternFill
from shapely import MultiPolygon
from shapely.geometry import Polygon
from shapely.strtree import STRtree
from sklearn.cluster import DBSCAN
from sqlalchemy import func
from sqlalchemy.orm import Session
from storage import file_storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


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


def _repair_geometry(geom):
    """Repair a single geometry for robust overlay operations."""
    if geom is None or geom.is_empty:
        return None

    repaired = geom
    if not repaired.is_valid:
        try:
            repaired = shapely.make_valid(repaired)
        except Exception:
            repaired = repaired.buffer(0)

    if repaired is None or repaired.is_empty:
        return None

    if not repaired.is_valid:
        try:
            repaired = repaired.buffer(0)
        except Exception:
            return None

    if repaired.is_empty:
        return None

    return repaired


def sanitize_polygons_for_overlay(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Repair invalid geometries and keep only polygonal features for overlay."""
    if gdf is None or gdf.empty:
        return gdf

    cleaned = gdf.copy()
    cleaned.geometry = cleaned.geometry.apply(_repair_geometry)
    cleaned = gpd.GeoDataFrame(
        cleaned[cleaned.geometry.notna()].copy(),
        geometry="geometry",
        crs=gdf.crs,
    )
    cleaned = clean_non_polygons(cleaned)
    cleaned = gpd.GeoDataFrame(
        cleaned[~cleaned.geometry.is_empty].copy(),
        geometry="geometry",
        crs=gdf.crs,
    )
    return cleaned


def difference_overlay_without_discard(
    gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Perform difference overlay without discarding rows that don't intersect"""
    # Repair invalid geometries to avoid TopologyException during overlay
    # Only repair gdf1 in-place (don't drop rows, as index mapping is needed later)
    gdf1 = gdf1.copy()
    gdf1.geometry = gdf1.geometry.apply(_repair_geometry)
    gdf2 = sanitize_polygons_for_overlay(gdf2)
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


# ============ Project CRUD Operations ============


def create_project(
    db: Session, name: str, location: str, description: Optional[str] = None
) -> str:
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


def get_project(db: Session, project_id: str) -> ProjectModel:
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


def update_project(
    db: Session,
    project_id: str,
    name: Optional[str] = None,
    location: Optional[str] = None,
    description: Optional[str] = None,
) -> ProjectModel:
    """Update project details"""
    project = get_project(db, project_id)
    if not project:
        return None

    if name is not None:
        project.name = name
    if location is not None:
        project.location = location
    if description is not None:
        project.description = description

    project.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(project)

    return project


# ============ Khasras ============================


def process_khasra_upload(
    db: Session,
    file_content: bytes,
    filename: str,
    project_id: str,
    id_column: Optional[str] = None,
) -> Dict[str, Any]:
    """Process uploaded khasra file and store in database"""
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
            # remove z-dimension if present
            gdf.geometry = gdf.geometry.apply(
                lambda x: shapely.wkb.loads(shapely.wkb.dumps(x, output_dimension=2))
            )
        elif file_extension in [".geojson", ".json"]:
            gdf = gpd.read_file(tmp_path, engine="pyogrio")
        elif file_extension == ".parquet":
            gdf = gpd.read_parquet(tmp_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    finally:
        Path(tmp_path).unlink()

    # Ensure CRS
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    gdf_4326 = gdf.to_crs("EPSG:4326")
    gdf_projected = gdf_4326.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

    # Set up ID columns - try multiple common column names
    khasra_id_assigned = False

    # First priority: user-specified column
    if id_column and id_column in gdf_projected.columns:
        gdf_projected["Khasra ID"] = gdf_projected[id_column].astype(str)
        khasra_id_assigned = True

    # Second priority: try common ID column names
    if not khasra_id_assigned:
        common_id_columns = [
            "Name",
            "name",
            "NAME",
            "id",
            "ID",
            "Id",
            "khasra_id",
            "Khasra_ID",
            "KHASRA_ID",
            "khasra_no",
            "Khasra_No",
            "parcel_id",
            "Parcel_ID",
            "PARCEL_ID",
            "plot_id",
            "Plot_ID",
            "PLOT_ID",
            "feature_id",
            "Feature_ID",
            "FEATURE_ID",
            "fid",
            "FID",
            "OBJECTID",
            "ObjectID",
            "objectid",
        ]
        for col in common_id_columns:
            if col in gdf_projected.columns:
                gdf_projected["Khasra ID"] = gdf_projected[col].astype(str)
                khasra_id_assigned = True
                break

    # Last resort: auto-generate sequential IDs
    if not khasra_id_assigned:
        gdf_projected["Khasra ID"] = [
            f"KHASRA_{i+1:04d}" for i in range(len(gdf_projected))
        ]

    # Create unique IDs (project-scoped unique identifier)
    gdf_projected["Khasra ID (Unique)"] = (
        gdf_projected["Khasra ID"] + "_" + [str(i) for i in range(len(gdf_projected))]
    )

    # Calculate areas
    gdf_projected["Original Area (ha)"] = gdf_projected.geometry.area / 10_000

    # Prepare 4326 version for storage
    gdf_4326["Khasra ID"] = gdf_projected["Khasra ID"]
    gdf_4326["Khasra ID (Unique)"] = gdf_projected["Khasra ID (Unique)"]
    gdf_4326["Original Area (ha)"] = gdf_projected["Original Area (ha)"]

    # First, delete any existing khasras for this project
    db.query(KhasraModel).filter(KhasraModel.project_id == project_id).delete()

    # Invalidate distance matrix since khasras are being replaced
    if project.distance_matrix_path:
        try:
            file_storage.delete_file(project.distance_matrix_path)
            # Also delete metadata
            metadata_path = project.distance_matrix_path.replace(
                "distance_matrix.npy", "distance_matrix_metadata.json"
            )
            file_storage.delete_file(metadata_path)
        except Exception as e:
            logger.warning(f"Failed to delete distance matrix files: {e}")

        project.distance_matrix_path = None

    # Validate and repair invalid geometries at import time
    invalid_count = (~gdf_4326.geometry.is_valid).sum()
    if invalid_count > 0:
        logger.warning(
            f"Repairing {invalid_count} invalid khasra geometries out of {len(gdf_4326)}"
        )
        gdf_4326.geometry = gdf_4326.geometry.apply(_repair_geometry)
        # Also repair the projected version so area calculations are correct
        gdf_projected.geometry = gdf_projected.geometry.apply(_repair_geometry)
        # Recalculate areas after repair
        gdf_projected["Original Area (ha)"] = gdf_projected.geometry.area / 10_000
        gdf_4326["Original Area (ha)"] = gdf_projected["Original Area (ha)"]

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
            properties={
                k: str(v)
                for k, v in row.drop(
                    [
                        "geometry",
                        "Khasra ID",
                        "Khasra ID (Unique)",
                        "Original Area (ha)",
                    ]
                ).items()
                if pd.notna(v)
            },
        )
        db.add(khasra)

    # Update project
    bounds = gdf_4326.total_bounds
    project.khasra_count = len(gdf_projected)
    project.total_area_ha = float(round(gdf_projected["Original Area (ha)"].sum(), 2))
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
        "crs": "EPSG:4326",
    }


def get_khasras_gdf(
    db: Session, project_id: str, projected: bool = False
) -> Optional[gpd.GeoDataFrame]:
    """Load khasras GeoDataFrame from database

    Args:
        db: Database session
        project_id: Project ID
        projected: If True, return GDF in India projected CRS (EPSG:24378) for area calculations
    """
    # Query khasras from database with consistent ordering
    khasras = (
        db.query(KhasraModel)
        .filter(KhasraModel.project_id == project_id)
        .order_by(KhasraModel.khasra_id_unique)
        .all()
    )

    if not khasras:
        return None

    # Build GeoDataFrame from database records
    data = []
    for k in khasras:
        geom = to_shape(k.geometry)
        row = {
            "geometry": geom,
            "Khasra ID": k.khasra_id,
            "Khasra ID (Unique)": k.khasra_id_unique,
            "Original Area (ha)": k.original_area_ha,
            "parcel_id": k.parcel_id,
            "Building Count": k.building_count or 0,
        }
        # Add any additional properties
        if k.properties:
            row.update(k.properties)
        data.append(row)

    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")

    # Project to India CRS if requested (for area calculations)
    if projected:
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

    return gdf


def get_khasras_with_stats_gdf(
    db: Session, project_id: str
) -> Optional[gpd.GeoDataFrame]:
    """Load khasras GeoDataFrame with all calculated stats from database"""
    khasras = (
        db.query(KhasraModel)
        .filter(KhasraModel.project_id == project_id)
        .order_by(KhasraModel.khasra_id_unique)
        .all()
    )

    if not khasras:
        return None

    data = []
    for k in khasras:
        geom = to_shape(k.geometry)
        row = {
            "geometry": geom,
            "Khasra ID": k.khasra_id,
            "Khasra ID (Unique)": k.khasra_id_unique,
            "Original Area (ha)": k.original_area_ha or 0,
            "Usable Area (ha)": k.usable_area_ha or 0,
            "Unusable Area (ha)": k.unusable_area_ha or 0,
            "Usable and Available Area (ha)": k.usable_available_area_ha or 0,
            "Parcel ID": k.parcel_id,
            "Building Count": k.building_count or 0,
        }
        # Calculate percentages
        orig = row["Original Area (ha)"]
        if orig > 0:
            row["Usable Area (%)"] = round(row["Usable Area (ha)"] / orig * 100, 2)
            row["Unusable Area (%)"] = round(row["Unusable Area (ha)"] / orig * 100, 2)
            row["Usable and Available Area (%)"] = round(
                row["Usable and Available Area (ha)"] / orig * 100, 2
            )
        else:
            row["Usable Area (%)"] = 0
            row["Unusable Area (%)"] = 0
            row["Usable and Available Area (%)"] = 0

        # Add layer-specific areas
        if k.layer_areas:
            row.update(k.layer_areas)

        if k.properties:
            row.update(k.properties)
        data.append(row)

    return gpd.GeoDataFrame(data, crs="EPSG:4326")


def get_khasras(db: Session, project_id: str) -> Dict[str, Any]:
    """Get khasras for a project with GeoJSON geometries"""

    project = get_project(db, project_id)
    if not project:
        return {"exists": False}

    # Check if project has khasras and get count
    khasra_count = (
        db.query(KhasraModel).filter(KhasraModel.project_id == project_id).count()
    )

    if not khasra_count:
        return {"exists": False}

    # Use PostGIS ST_AsGeoJSON for efficient geometry conversion
    khasras_query = db.query(
        func.ST_AsGeoJSON(KhasraModel.geometry).label("geojson"),
        KhasraModel.khasra_id,
        KhasraModel.khasra_id_unique,
        KhasraModel.original_area_ha,
        KhasraModel.usable_area_ha,
        KhasraModel.unusable_area_ha,
        KhasraModel.usable_available_area_ha,
        KhasraModel.parcel_id,
        KhasraModel.building_count,
        KhasraModel.layer_areas,
        KhasraModel.properties,
    ).filter(KhasraModel.project_id == project_id)

    khasras_data = khasras_query.all()

    # Convert to GeoJSON features
    features = []
    for (
        geojson_str,
        khasra_id,
        khasra_id_unique,
        original_area_ha,
        usable_area_ha,
        unusable_area_ha,
        usable_available_area_ha,
        parcel_id,
        building_count,
        layer_areas,
        props,
    ) in khasras_data:
        geometry = json.loads(geojson_str) if geojson_str else None

        # Build properties dict with all available stats
        properties = {
            "khasra_id": khasra_id,
            "khasra_id_unique": khasra_id_unique,
            "original_area_ha": round(original_area_ha, 4)
            if original_area_ha
            else None,
            "usable_area_ha": round(usable_area_ha, 4) if usable_area_ha else None,
            "unusable_area_ha": round(unusable_area_ha, 4)
            if unusable_area_ha
            else None,
            "usable_available_area_ha": round(usable_available_area_ha, 4)
            if usable_available_area_ha
            else None,
            "parcel_id": parcel_id,
            "building_count": building_count or 0,
            **(props or {}),
        }

        # Calculate percentages if original_area_ha is available
        if original_area_ha and original_area_ha > 0:
            if usable_area_ha is not None:
                properties["usable_area_percent"] = round(
                    (usable_area_ha / original_area_ha) * 100, 2
                )
            if unusable_area_ha is not None:
                properties["unusable_area_percent"] = round(
                    (unusable_area_ha / original_area_ha) * 100, 2
                )
            if usable_available_area_ha is not None:
                properties["usable_available_area_percent"] = round(
                    (usable_available_area_ha / original_area_ha) * 100, 2
                )

        # Add layer-specific areas if available
        if layer_areas:
            properties.update(layer_areas)

        # Build a human-readable description for tooltips
        description_parts = []
        if original_area_ha:
            description_parts.append(f"Area: {round(original_area_ha, 2)} ha")
        if usable_area_ha is not None and original_area_ha:
            usable_pct = round((usable_area_ha / original_area_ha) * 100, 1)
            description_parts.append(
                f"Usable: {round(usable_area_ha, 2)} ha ({usable_pct}%)"
            )
        if unusable_area_ha is not None and original_area_ha:
            unusable_pct = round((unusable_area_ha / original_area_ha) * 100, 1)
            description_parts.append(
                f"Unusable: {round(unusable_area_ha, 2)} ha ({unusable_pct}%)"
            )

        properties["description"] = (
            " | ".join(description_parts)
            if description_parts
            else "No area calculated yet"
        )

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": properties,
            }
        )

    return {
        "exists": True,
        "count": len(features),
        "total_area_ha": project.total_area_ha,
        "uploaded_at": project.updated_at.isoformat() if project.updated_at else None,
        "geojson": {"type": "FeatureCollection", "features": features},
        "bounds": project.bounds_json,
    }


def delete_khasras(db: Session, project_id: str) -> bool:
    """Delete all khasras and dependent data for a project"""
    project = get_project(db, project_id)
    if not project:
        return False

    # Check if there are any khasras to delete
    khasra_count = (
        db.query(KhasraModel).filter(KhasraModel.project_id == project_id).count()
    )
    if khasra_count == 0:
        return False

    # Delete in proper order: parent tables first to avoid orphans

    # 1. Delete clustering runs (this cascades to parcels via FK constraint)
    db.query(ClusteringRunModel).filter(
        ClusteringRunModel.project_id == project_id
    ).delete()

    # 2. Delete all layers (this cascades to layer_features via FK constraint)
    db.query(LayerModel).filter(LayerModel.project_id == project_id).delete()

    # 3. Delete all khasras
    db.query(KhasraModel).filter(KhasraModel.project_id == project_id).delete()

    # Delete distance matrix file if it exists
    if project.distance_matrix_path:
        try:
            file_storage.delete_file(project.distance_matrix_path)
        except Exception as e:
            logger.warning(f"Failed to delete distance matrix file: {e}")

    # Reset project status and stats
    project.status = ProjectStatus.CREATED
    project.khasra_count = None
    project.total_area_ha = None
    project.bounds_json = None
    project.distance_matrix_path = None
    project.updated_at = datetime.utcnow()

    db.commit()

    return True


def delete_parcels(db: Session, project_id: str) -> bool:
    """Delete all parcels (clustering results) for a project"""
    project = get_project(db, project_id)
    if not project:
        return False

    # Check if there are any parcels to delete
    parcel_count = (
        db.query(ParcelModel).filter(ParcelModel.project_id == project_id).count()
    )
    if parcel_count == 0:
        return False

    # Delete all clustering runs (this will cascade delete parcels due to FK)
    db.query(ClusteringRunModel).filter(
        ClusteringRunModel.project_id == project_id
    ).delete()

    # Reset parcel_id on all khasras
    db.query(KhasraModel).filter(KhasraModel.project_id == project_id).update(
        {"parcel_id": None}
    )

    # Update project status - check if layers exist
    layer_count = (
        db.query(LayerModel).filter(LayerModel.project_id == project_id).count()
    )
    if layer_count > 0:
        project.status = ProjectStatus.LAYERS_ADDED
    else:
        project.status = ProjectStatus.KHASRAS_UPLOADED
    project.updated_at = datetime.utcnow()

    db.commit()

    return True


# ============ Layer Processing ============


def update_layer_status(db: Session, layer: LayerModel, status: str, details: str):
    """Update the status and details of a layer"""
    layer.status = status
    layer.details = details
    layer.updated_at = datetime.utcnow()
    db.commit()
    print(f"[LAYER STATUS] Layer '{layer.name}' {status.upper()}: {details}")


def format_success_message(feature_count: int, area_ha: float) -> str:
    """
    Generate standardized success message for layer processing.

    Args:
        feature_count: Number of features processed
        area_ha: Total area in hectares

    Returns:
        Formatted success message
    """
    return f"Processed {feature_count} features covering {area_ha} ha"


def format_success_no_data_message() -> str:
    """
    Generate standardized no-data message for layer processing.

    Returns:
        Formatted no-data message
    """
    return "No features found in project area"


def format_error_message(exception: Exception, context: str = "") -> str:
    """
    Generate user-friendly error message with technical details.

    Args:
        exception: The exception that occurred
        context: Optional context about what was being done when error occurred

    Returns:
        Formatted error message with user-friendly reason and technical details
    """
    # Categorize the exception
    if isinstance(exception, FileNotFoundError):
        user_reason = "Required data not available"
    elif isinstance(exception, ValueError):
        error_msg = str(exception).lower()
        if "not found" in error_msg or "must be uploaded" in error_msg:
            user_reason = "Invalid configuration or missing prerequisites"
        elif "no data found" in error_msg or "no rooftop data" in error_msg:
            user_reason = "Required data not available in this area"
        elif "authenticate" in error_msg or "credentials" in error_msg:
            user_reason = "External data source authentication failed"
        else:
            user_reason = "Invalid configuration or data format"
    elif (
        "authenticate" in str(exception).lower()
        or "credentials" in str(exception).lower()
    ):
        user_reason = "External data source authentication failed"
    elif any(
        keyword in str(exception).lower()
        for keyword in ["geometry", "invalid", "corrupt"]
    ):
        user_reason = "Processing failed due to data quality issues"
    else:
        user_reason = "An unexpected error occurred"

    # Build the message
    technical_details = str(exception)
    if context:
        return f"Processing failed: {user_reason}. Context: {context}. Technical details: {technical_details}"
    else:
        return (
            f"Processing failed: {user_reason}. Technical details: {technical_details}"
        )


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

    # Delete any existing layer with the same name to start fresh
    existing_layer = (
        db.query(LayerModel)
        .filter(LayerModel.project_id == project_id, LayerModel.name == layer_name)
        .first()
    )
    if existing_layer:
        db.delete(existing_layer)
        db.commit()

    # Create fresh layer record
    layer = LayerModel(
        project_id=project_id,
        name=layer_name,
        layer_type=LayerType.CUSTOM.value,
        is_unusable=is_unusable,
        status="in_progress",
        details="Initializing layer processing",
        parameters={},
    )
    db.add(layer)
    db.commit()

    try:
        update_layer_status(db, layer, "in_progress", "Loading khasra data")

        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")

        # Ensure khasras are projected to India CRS for intersection
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

        update_layer_status(db, layer, "in_progress", "Reading uploaded file")

        # Read the layer file
        file_extension = Path(filename).suffix.lower()

        with tempfile.NamedTemporaryFile(
            suffix=file_extension, delete=False
        ) as tmp_file:
            tmp_file.write(file_content)
            tmp_path = tmp_file.name

        try:
            if file_extension == ".kml":
                layer_gdf = gpd.read_file(tmp_path, driver="KML", engine="pyogrio")
            elif file_extension in [".geojson", ".json"]:
                layer_gdf = gpd.read_file(tmp_path, engine="pyogrio")
            elif file_extension == ".parquet":
                layer_gdf = gpd.read_parquet(tmp_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        finally:
            Path(tmp_path).unlink()

        update_layer_status(db, layer, "in_progress", "Projecting layer to India CRS")

        # Ensure CRS and project
        if layer_gdf.crs is None:
            layer_gdf = layer_gdf.set_crs("EPSG:4326")
        layer_gdf = layer_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

        update_layer_status(db, layer, "in_progress", "Intersecting layer with khasras")

        # Intersect with khasras
        layer_overlap_gdf = gpd.overlay(layer_gdf, gdf, how="intersection")
        layer_overlap_gdf = layer_overlap_gdf.dissolve(
            by="Khasra ID (Unique)"
        ).reset_index()

        update_layer_status(db, layer, "in_progress", "Calculating area statistics")

        # Calculate area
        area_col = (
            f"{'Unusable' if is_unusable else 'Unavailable'} Area - {layer_name} (ha)"
        )
        layer_overlap_gdf[area_col] = layer_overlap_gdf.area / 10_000

        update_layer_status(
            db, layer, "in_progress", "Storing custom layer in database"
        )

        # Update layer metadata
        layer.feature_count = len(layer_overlap_gdf)
        layer.total_area_ha = float(round(layer_overlap_gdf[area_col].sum(), 2))
        layer.parameters = {"area_col": area_col}
        db.flush()

        # Store per-khasra layer features in database
        layer_overlap_4326 = layer_overlap_gdf.to_crs("EPSG:4326")
        for idx, row in layer_overlap_4326.iterrows():
            geom = row.geometry
            # Convert to MultiPolygon if needed
            if geom.geom_type == "Polygon":
                geom = MultiPolygon([geom])
            elif geom.geom_type == "GeometryCollection":
                polygons = [
                    g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")
                ]
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

        # Mark as successful
        layer.status = "successful"
        layer.details = f"Layer processed successfully. {len(layer_overlap_gdf)} features, {layer.total_area_ha} ha total area."

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
            status="successful",
            details=layer.details,
        )

    except Exception as e:
        # Mark layer as failed
        layer.status = "failed"
        layer.details = format_error_message(e, "processing custom layer")
        db.commit()
        raise


def get_layers_metadata(db: Session, project_id: str) -> List[LayerModel]:
    """Get all layers for a project"""
    return db.query(LayerModel).filter(LayerModel.project_id == project_id).all()


def get_layers_geojson(db: Session, project_id: str) -> Dict[str, Any]:
    """Get all layer geometries as GeoJSON for map display using PostGIS ST_AsGeoJSON"""

    layers = get_layers_metadata(db, project_id)

    result = {}
    for layer in layers:
        # Use PostGIS ST_AsGeoJSON directly for much better performance
        # Query returns GeoJSON strings instead of converting geometries in Python
        features_query = (
            db.query(
                func.ST_AsGeoJSON(LayerFeatureModel.geometry).label("geojson"),
                LayerFeatureModel.khasra_id_unique,
                LayerFeatureModel.area_ha,
                LayerFeatureModel.properties,
            )
            .filter(LayerFeatureModel.layer_id == layer.id)
            # .limit(1000) # Limit to prevent hanging
        )  

        features_data = features_query.all()

        if not features_data:
            continue

        # Convert to GeoJSON features
        features = []
        for geojson_str, khasra_id_unique, area_ha, props in features_data:
            # Parse the GeoJSON geometry string from PostGIS
            geometry = json.loads(geojson_str) if geojson_str else None

            feature = {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "khasra_id_unique": khasra_id_unique,
                    "area_ha": area_ha,
                    "layer_name": layer.name,
                    "layer_type": layer.layer_type,
                    "is_unusable": layer.is_unusable,
                    **(props or {}),
                },
            }
            features.append(feature)

        result[layer.name] = {
            "type": "FeatureCollection",
            "features": features,
            "layer_info": {
                "name": layer.name,
                "layer_type": layer.layer_type,
                "is_unusable": layer.is_unusable,
                "total_area_ha": layer.total_area_ha,
                "feature_count": layer.feature_count,
            },
        }

    return result


def load_layer_gdf_by_id(db: Session, layer_id: int) -> Optional[gpd.GeoDataFrame]:
    """Load a layer's GeoDataFrame from database by layer ID"""
    features = (
        db.query(LayerFeatureModel).filter(LayerFeatureModel.layer_id == layer_id).all()
    )

    if not features:
        return None

    data = []
    for f in features:
        geom = to_shape(f.geometry)
        row = {
            "geometry": geom,
            "khasra_id_unique": f.khasra_id_unique,
            "area_ha": f.area_ha,
        }
        if f.properties:
            row.update(f.properties)
        data.append(row)

    return gpd.GeoDataFrame(data, crs="EPSG:4326")


# ============ Builtin Layer Processing ============


def get_landcover_shapes(
    landcover_data: np.ndarray,
    transform,
    class_name: str,
    class_value_lookup_dict: Dict[str, List[int]],
    raster_crs: str = "EPSG:4326",
    target_crs: str = "EPSG:24378",
) -> gpd.GeoDataFrame:
    """
    Extract vector shapes from landcover raster for a specific class.

    Args:
        landcover_data: Raster data array
        transform: Raster transform
        class_name: Name of landcover class (e.g., "Cropland", "Open surface water")
        class_value_lookup_dict: Mapping from class names to raster values
        raster_crs: Input CRS of raster
        target_crs: Target CRS for output

    Returns:
        GeoDataFrame with extracted shapes
    """
    from rasterio.features import shapes as rasterio_shapes
    from shapely.geometry import shape

    # Get array values for this class
    class_values = class_value_lookup_dict[class_name]

    # Create mask
    layer_mask = np.isin(landcover_data, class_values)

    # Extract vector shapes
    vector_shapes = [
        {"geometry": shape(geom), "properties": {"class": class_name}}
        for geom, class_value in rasterio_shapes(
            landcover_data, mask=layer_mask, transform=transform
        )
    ]

    # Handle empty case
    if not vector_shapes:
        return gpd.GeoDataFrame(columns=["geometry", "class"], crs=target_crs)

    # Create GeoDataFrame using from_features which handles GeoJSON-like dictionaries
    shapes_gdf = gpd.GeoDataFrame.from_features(vector_shapes, crs=raster_crs)
    shapes_gdf = shapes_gdf.to_crs(target_crs)

    return shapes_gdf


def load_landcover_class_mapping(legend_path: Path) -> Dict[str, List[int]]:
    """
    Load landcover class to value mapping from CSV legend.

    Returns:
        Dictionary mapping class names to list of raster values
    """
    import pandas as pd

    legend_df = pd.read_csv(legend_path)
    value_class_dict = legend_df.set_index("map_value")["class_b"].to_dict()

    # Invert to get class -> [values] mapping
    class_value_dict = {}
    for value, class_name in value_class_dict.items():
        if class_name not in class_value_dict:
            class_value_dict[class_name] = [value]
        else:
            class_value_dict[class_name].append(value)

    return class_value_dict


def process_settlement_layer(
    db: Session,
    project_id: str,
    building_buffer: int = 10,
    settlement_eps: int = 50,
    min_buildings: int = 5,
    create_only: bool = False,
) -> Tuple[LayerInfo, LayerInfo]:
    """
    Process buildings to create settlement and isolated building layers.

    This function:
    1. Loads building footprints from VIDA rooftop data
    2. Buffers buildings by building_buffer meters
    3. Clusters buildings using DBSCAN (eps=settlement_eps, min_samples=min_buildings)
    4. Creates convex hulls of clusters as settlements
    5. Saves both settlement layer and isolated buildings layer

    Args:
        db: Database session
        project_id: Project ID
        building_buffer: Buffer distance around buildings in meters
        settlement_eps: DBSCAN epsilon (max distance between buildings)
        min_buildings: Minimum buildings to form a settlement
        create_only: If True, only create layer records and return immediately

    Returns:
        Tuple of (settlements_layer_info, isolated_buildings_layer_info)
    """
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    if create_only:
        # Create placeholder layer records for async processing
        # Check if layers already exist to avoid duplicates
        existing_settlements = (
            db.query(LayerModel)
            .filter(
                LayerModel.project_id == project_id, LayerModel.name == "Settlements"
            )
            .first()
        )
        existing_isolated = (
            db.query(LayerModel)
            .filter(
                LayerModel.project_id == project_id,
                LayerModel.name == "Isolated Buildings",
            )
            .first()
        )

        if existing_settlements and existing_isolated:
            # Return existing placeholders
            return (
                LayerInfo(
                    layer_type=LayerType.SETTLEMENTS.value,
                    name=existing_settlements.name,
                    description="Settlement clusters from buildings",
                    is_unusable=existing_settlements.is_unusable,
                    parameters=existing_settlements.parameters or {},
                    status=existing_settlements.status,
                    details=existing_settlements.details,
                    area_ha=existing_settlements.total_area_ha,
                    feature_count=existing_settlements.feature_count,
                ),
                LayerInfo(
                    layer_type=LayerType.ISOLATED_BUILDINGS.value,
                    name=existing_isolated.name,
                    description="Buildings not part of settlements",
                    is_unusable=existing_isolated.is_unusable,
                    parameters=existing_isolated.parameters or {},
                    status=existing_isolated.status,
                    details=existing_isolated.details,
                    area_ha=existing_isolated.total_area_ha,
                    feature_count=existing_isolated.feature_count,
                ),
            )

        # Create new placeholder layers
        if not existing_settlements:
            settlements_layer = LayerModel(
                project_id=project_id,
                name="Settlements",
                layer_type=LayerType.SETTLEMENTS.value,
                is_unusable=True,
                status="in_progress",
                details="Queued for processing",
                parameters={
                    "building_buffer": building_buffer,
                    "settlement_eps": settlement_eps,
                    "min_buildings": min_buildings,
                },
            )
            db.add(settlements_layer)

        if not existing_isolated:
            isolated_layer = LayerModel(
                project_id=project_id,
                name="Isolated Buildings",
                layer_type=LayerType.ISOLATED_BUILDINGS.value,
                is_unusable=False,
                status="in_progress",
                details="Queued for processing",
                parameters={
                    "building_buffer": building_buffer,
                },
            )
            db.add(isolated_layer)

        db.commit()

        return (
            LayerInfo(
                layer_type=LayerType.SETTLEMENTS.value,
                name="Settlements",
                description="Settlement clusters from buildings",
                is_unusable=True,
                parameters={
                    "building_buffer": building_buffer,
                    "settlement_eps": settlement_eps,
                    "min_buildings": min_buildings,
                },
                status="in_progress",
                details="Queued for processing",
            ),
            LayerInfo(
                layer_type=LayerType.ISOLATED_BUILDINGS.value,
                name="Isolated Buildings",
                description="Buildings not part of settlements",
                is_unusable=False,
                parameters={"building_buffer": building_buffer},
                status="in_progress",
                details="Queued for processing",
            ),
        )

    # Actual processing (create_only=False): Delete old layers and start fresh
    existing_settlements = (
        db.query(LayerModel)
        .filter(LayerModel.project_id == project_id, LayerModel.name == "Settlements")
        .first()
    )
    if existing_settlements:
        db.delete(existing_settlements)

    existing_isolated = (
        db.query(LayerModel)
        .filter(
            LayerModel.project_id == project_id, LayerModel.name == "Isolated Buildings"
        )
        .first()
    )
    if existing_isolated:
        db.delete(existing_isolated)

    db.commit()

    # Create fresh layer records
    settlements_layer = LayerModel(
        project_id=project_id,
        name="Settlements",
        layer_type=LayerType.SETTLEMENTS.value,
        is_unusable=True,
        status="in_progress",
        details="Initializing settlement detection",
        parameters={
            "building_buffer": building_buffer,
            "settlement_eps": settlement_eps,
            "min_buildings": min_buildings,
        },
    )
    db.add(settlements_layer)

    isolated_layer = LayerModel(
        project_id=project_id,
        name="Isolated Buildings",
        layer_type=LayerType.ISOLATED_BUILDINGS.value,
        is_unusable=False,
        status="in_progress",
        details="Waiting for settlement detection",
        parameters={
            "building_buffer": building_buffer,
        },
    )
    db.add(isolated_layer)

    db.commit()

    try:
        update_layer_status(
            db, settlements_layer, "in_progress", "Loading khasras data"
        )

        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")

        # Project khasras
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
        gdf = sanitize_polygons_for_overlay(gdf)
        if gdf.empty:
            raise ValueError("No valid khasra geometries found after geometry repair")
        gdf_4326 = gdf.to_crs("EPSG:4326")

        update_layer_status(
            db, settlements_layer, "in_progress", "Importing rooftop utilities"
        )

        # Import VIDA utilities
        try:
            from gridsample.utils_rooftop import (
                download_VIDA_rooftops_data_by_s2,
                get_overlapping_s2_cell_ids,
            )
        except ImportError:
            raise ValueError(
                "gridsample package not installed. Install it with: pip install -e . in gridsample directory"
            )

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            "Finding overlapping rooftop data",
        )

        # Get S2 cell IDs that overlap the khasras
        s2_cell_ids = get_overlapping_s2_cell_ids(gdf_4326)

        if not s2_cell_ids:
            raise ValueError("No S2 cells found overlapping the khasras")

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            f"Downloading rooftop data for {len(s2_cell_ids)} tiles",
        )

        # Download rooftop data to shared folder (not per-project)
        shared_rooftop_dir = settings.DATA_DIR / "shared_vida_s2_rooftop_data"
        shared_rooftop_dir.mkdir(parents=True, exist_ok=True)

        download_VIDA_rooftops_data_by_s2(
            s2_cell_ids=s2_cell_ids,
            country_iso_code="IND",
            target_data_dir=shared_rooftop_dir,
        )

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            "Loading and combining rooftop data",
        )

        # Load and combine rooftop data from shared folder
        rooftop_gdf_list = []
        for s2_cell_id in s2_cell_ids:
            s2_rooftops_path = shared_rooftop_dir / f"{s2_cell_id}.parquet"
            if s2_rooftops_path.exists():
                rooftop_gdf = gpd.read_parquet(s2_rooftops_path)
                rooftop_gdf_list.append(rooftop_gdf)

        if not rooftop_gdf_list:
            raise ValueError("No rooftop data found for the given area")

        rooftop_gdf = pd.concat(rooftop_gdf_list, ignore_index=True)
        rooftop_gdf = rooftop_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            f"Filtering {len(rooftop_gdf)} buildings to khasras",
        )

        # Filter to only rooftops that intersect khasras
        rooftops_in_khasras = rooftop_gdf.sjoin(
            gdf, how="inner", predicate="intersects"
        )
        rooftops_in_khasras = rooftops_in_khasras.drop(
            columns=["index_right"], errors="ignore"
        )

        # Find khasra ID column (spatial join may add suffixes like _left/_right)
        khasra_id_col = None
        for col in rooftops_in_khasras.columns:
            if col == "Khasra ID (Unique)" or col.startswith("Khasra ID (Unique)_"):
                khasra_id_col = col
                break
            elif col == "khasra_id_unique" or col.startswith("khasra_id_unique_"):
                khasra_id_col = col
                break

        # Keep only geometry and khasra ID column
        if khasra_id_col:
            rooftops_in_khasras = rooftops_in_khasras[["geometry", khasra_id_col]]
            # Rename to consistent name for easier handling
            rooftops_in_khasras = rooftops_in_khasras.rename(
                columns={khasra_id_col: "khasra_id_unique"}
            )
            khasra_id_col = "khasra_id_unique"
        else:
            # If we can't find the khasra ID column, just keep geometry
            logger.warning(
                "Could not find khasra ID column after spatial join, building counts will not be tracked"
            )
            rooftops_in_khasras = rooftops_in_khasras[["geometry"]]

        if len(rooftops_in_khasras) == 0:
            raise ValueError("No buildings found within khasras")

        # Count buildings per khasra and update database
        if khasra_id_col:
            update_layer_status(
                db,
                settlements_layer,
                "in_progress",
                "Counting buildings per khasra",
            )

            # First, reset all building counts to 0 for this project
            db.query(KhasraModel).filter(KhasraModel.project_id == project_id).update(
                {KhasraModel.building_count: 0}
            )
            db.commit()

            building_counts = rooftops_in_khasras.groupby(khasra_id_col).size()

            # Update khasra building counts in database for khasras with buildings
            for khasra_id_unique, count in building_counts.items():
                khasra = (
                    db.query(KhasraModel)
                    .filter(
                        KhasraModel.project_id == project_id,
                        KhasraModel.khasra_id_unique == khasra_id_unique,
                    )
                    .first()
                )
                if khasra:
                    khasra.building_count = int(count)

            db.commit()
            logger.info(
                f"Reset building counts and updated {len(building_counts)} khasras with buildings"
            )

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            f"Buffering {len(rooftops_in_khasras)} buildings by {building_buffer}m",
        )

        # Buffer buildings
        buffered_buildings = rooftops_in_khasras.copy()
        buffered_buildings["geometry"] = buffered_buildings.buffer(building_buffer)
        buffered_buildings = sanitize_polygons_for_overlay(buffered_buildings)
        if buffered_buildings.empty:
            raise ValueError("No valid buffered building geometries found")

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            "Filtering to buildings that overlap the khasras",
        )

        # Get intersection with khasras
        buildings_overlap_gdf = gpd.overlay(buffered_buildings, gdf, how="intersection")
        buildings_overlap_gdf = sanitize_polygons_for_overlay(buildings_overlap_gdf)
        if buildings_overlap_gdf.empty:
            raise ValueError("No valid building overlaps found within khasras")

        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            f"Clustering {len(buildings_overlap_gdf)} buildings (max distance={settlement_eps}m, min buildings={min_buildings})",
        )

        # Cluster buildings using DBSCAN
        building_centroids = buildings_overlap_gdf.geometry.centroid
        X = np.array(list(zip(building_centroids.x, building_centroids.y)))

        clusterer = DBSCAN(eps=settlement_eps, min_samples=min_buildings, n_jobs=-1)
        building_cluster_ids = clusterer.fit_predict(X)
        buildings_overlap_gdf["settlement_id"] = building_cluster_ids

        # Separate settlement buildings from isolated buildings
        settlement_buildings_gdf = buildings_overlap_gdf[
            buildings_overlap_gdf["settlement_id"] != -1
        ].copy()
        isolated_buildings_gdf = buildings_overlap_gdf[
            buildings_overlap_gdf["settlement_id"] == -1
        ].copy()

        num_settlements = (
            len(settlement_buildings_gdf["settlement_id"].unique())
            if len(settlement_buildings_gdf) > 0
            else 0
        )
        update_layer_status(
            db,
            settlements_layer,
            "in_progress",
            f"Found {num_settlements} settlements and {len(isolated_buildings_gdf)} isolated buildings",
        )

        results = []

        # Process settlements (convex hull of clustered buildings)
        if len(settlement_buildings_gdf) > 0:
            update_layer_status(
                db,
                settlements_layer,
                "in_progress",
                "Calculating settlement boundaries",
            )

            settlements_gdf = settlement_buildings_gdf.dissolve(
                by="settlement_id"
            ).reset_index()
            settlements_gdf = settlements_gdf[["geometry", "settlement_id"]]
            settlements_gdf["geometry"] = settlements_gdf.convex_hull
            settlements_gdf = sanitize_polygons_for_overlay(settlements_gdf)

            # Intersect with khasras
            settlements_overlap_gdf = gpd.overlay(
                settlements_gdf, gdf, how="intersection"
            )
            settlements_overlap_gdf = sanitize_polygons_for_overlay(
                settlements_overlap_gdf
            )
            settlements_overlap_gdf = settlements_overlap_gdf.dissolve(
                by="Khasra ID (Unique)"
            ).reset_index()

            area_col = "Unusable Area - Settlements (ha)"
            settlements_overlap_gdf[area_col] = settlements_overlap_gdf.area / 10_000

            update_layer_status(
                db,
                settlements_layer,
                "in_progress",
                "Saving settlement layer to database",
            )

            # Save settlements layer
            settlements_info = _save_builtin_layer_with_status(
                db=db,
                layer=settlements_layer,
                layer_gdf=settlements_overlap_gdf,
                area_col=area_col,
            )
            results.append(settlements_info)
        else:
            settlements_layer.status = "successful"
            settlements_layer.details = format_success_no_data_message()
            settlements_layer.feature_count = 0
            settlements_layer.total_area_ha = 0.0
            db.commit()

            results.append(
                LayerInfo(
                    layer_type=LayerType.SETTLEMENTS.value,
                    name="Settlements",
                    description="No settlements found",
                    is_unusable=True,
                    parameters={},
                    area_ha=0.0,
                    feature_count=0,
                    status="successful",
                    details=format_success_no_data_message(),
                )
            )

        # Process isolated buildings
        update_layer_status(
            db, isolated_layer, "in_progress", "Processing isolated buildings"
        )

        if len(isolated_buildings_gdf) > 0:
            isolated_overlap_gdf = isolated_buildings_gdf.dissolve(
                by="Khasra ID (Unique)"
            ).reset_index()

            area_col = "Unavailable Area - Isolated Buildings (ha)"
            isolated_overlap_gdf[area_col] = isolated_overlap_gdf.area / 10_000

            update_layer_status(
                db,
                isolated_layer,
                "in_progress",
                "Saving isolated buildings layer to database",
            )

            # Save isolated buildings layer
            isolated_info = _save_builtin_layer_with_status(
                db=db,
                layer=isolated_layer,
                layer_gdf=isolated_overlap_gdf,
                area_col=area_col,
            )
            results.append(isolated_info)
        else:
            isolated_layer.status = "successful"
            isolated_layer.details = format_success_no_data_message()
            isolated_layer.feature_count = 0
            isolated_layer.total_area_ha = 0.0
            db.commit()

            results.append(
                LayerInfo(
                    layer_type=LayerType.ISOLATED_BUILDINGS.value,
                    name="Isolated Buildings",
                    description="No isolated buildings found",
                    is_unusable=False,
                    parameters={},
                    area_ha=0.0,
                    feature_count=0,
                    status="successful",
                    details=format_success_no_data_message(),
                )
            )

        # Update project status
        project.status = ProjectStatus.LAYERS_ADDED
        project.updated_at = datetime.utcnow()
        db.commit()

        return tuple(results)

    except Exception as e:
        # Mark both layers as failed
        settlements_layer.status = "failed"
        settlements_layer.details = format_error_message(e, "processing settlements")
        isolated_layer.status = "failed"
        isolated_layer.details = format_error_message(
            e, "processing isolated buildings"
        )
        db.commit()
        raise


def process_cropland_layer(
    db: Session,
    project_id: str,
    create_only: bool = False,
) -> LayerInfo:
    """
    Process cropland layer from landcover TIFF data.

    Steps:
    1. Load khasras and get bounding box
    2. Load landcover TIFF tiles that overlap project
    3. Extract cropland shapes using class_b="Cropland"
    4. Overlay with khasras to get intersection
    5. Save to database

    Args:
        db: Database session
        project_id: Project ID
        create_only: If True, only create layer record and return

    Returns:
        LayerInfo object
    """
    import rasterio
    import rasterio.mask

    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    if create_only:
        # Create placeholder layer record for async processing
        existing_layer = (
            db.query(LayerModel)
            .filter(LayerModel.project_id == project_id, LayerModel.name == "Cropland")
            .first()
        )

        if existing_layer:
            return LayerInfo(
                layer_type=LayerType.CROPLAND.value,
                name=existing_layer.name,
                description="Agricultural cropland areas",
                is_unusable=existing_layer.is_unusable,
                parameters=existing_layer.parameters or {},
                status=existing_layer.status,
                details=existing_layer.details,
                area_ha=existing_layer.total_area_ha,
                feature_count=existing_layer.feature_count,
            )

        # Create new placeholder layer
        cropland_layer = LayerModel(
            project_id=project_id,
            name="Cropland",
            layer_type=LayerType.CROPLAND.value,
            is_unusable=False,
            status="in_progress",
            details="Queued for processing",
            parameters={},
        )
        db.add(cropland_layer)
        db.commit()

        return LayerInfo(
            layer_type=LayerType.CROPLAND.value,
            name="Cropland",
            description="Agricultural cropland areas",
            is_unusable=False,
            parameters={},
            status="in_progress",
            details="Queued for processing",
        )

    # Actual processing (create_only=False): Delete old layer and start fresh
    existing_layer = (
        db.query(LayerModel)
        .filter(LayerModel.project_id == project_id, LayerModel.name == "Cropland")
        .first()
    )
    if existing_layer:
        db.delete(existing_layer)

    db.commit()

    # Create fresh layer record
    cropland_layer = LayerModel(
        project_id=project_id,
        name="Cropland",
        layer_type=LayerType.CROPLAND.value,
        is_unusable=False,
        status="in_progress",
        details="Loading khasras data",
        parameters={},
    )
    db.add(cropland_layer)
    db.commit()

    try:
        # Load khasras
        update_layer_status(db, cropland_layer, "in_progress", "Loading khasras data")
        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")

        # Project to target CRS
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
        gdf = sanitize_polygons_for_overlay(gdf)
        if gdf.empty:
            raise ValueError("No valid khasra geometries found after geometry repair")
        gdf_4326 = gdf.to_crs("EPSG:4326")

        # Load legend
        update_layer_status(db, cropland_layer, "in_progress", "Loading landcover data")
        legend_path = Path(settings.DATA_DIR) / "landcover" / "legend_processed.csv"
        class_value_dict = load_landcover_class_mapping(legend_path)

        # Load landcover TIFF and extract cropland shapes
        landcover_dir = Path(settings.DATA_DIR) / "landcover"

        # Find which TIFF files overlap the project bounds
        # For now, use 30N_070E_2020.tif (covers MP region)
        # TODO: Add logic to auto-detect overlapping tiles
        tiff_path = landcover_dir / "30N_070E_2020.tif"

        if not tiff_path.exists():
            raise FileNotFoundError(
                f"Landcover TIFF not found at {tiff_path}. "
                f"Please ensure the landcover data is available."
            )

        with rasterio.open(tiff_path) as src:
            update_layer_status(
                db, cropland_layer, "in_progress", "Extracting cropland from landcover"
            )

            # Mask to khasra bounds
            masked_data, masked_transform = rasterio.mask.mask(
                src, [gdf_4326.unary_union], crop=True
            )
            masked_data = np.squeeze(masked_data)

            # Extract cropland shapes
            cropland_shapes_gdf = get_landcover_shapes(
                landcover_data=masked_data,
                transform=masked_transform,
                class_name="Cropland",
                class_value_lookup_dict=class_value_dict,
                raster_crs=str(src.crs),
                target_crs=f"EPSG:{settings.INDIA_PROJECTED_CRS}",
            )

        cropland_shapes_gdf = sanitize_polygons_for_overlay(cropland_shapes_gdf)

        if cropland_shapes_gdf.empty:
            update_layer_status(
                db,
                cropland_layer,
                "successful",
                format_success_no_data_message(),
            )
            cropland_layer.feature_count = 0
            cropland_layer.total_area_ha = 0.0
            db.commit()

            return LayerInfo(
                layer_type=LayerType.CROPLAND.value,
                name="Cropland",
                description="Agricultural cropland areas",
                is_unusable=False,  # Cropland is unavailable (occupied), not unusable (unsuitable)
                parameters={},
                status="successful",
                details=format_success_no_data_message(),
                area_ha=0,
                feature_count=0,
            )

        # Overlay with khasras
        update_layer_status(
            db, cropland_layer, "in_progress", "Overlaying cropland onto khasras"
        )
        cropland_overlay_gdf = gpd.overlay(cropland_shapes_gdf, gdf, how="intersection")
        cropland_overlay_gdf = cropland_overlay_gdf.dissolve(
            by="Khasra ID (Unique)"
        ).reset_index()

        # Calculate areas
        area_col = "Unavailable Area - Cropland (ha)"
        cropland_overlay_gdf[area_col] = cropland_overlay_gdf.geometry.area / 10_000

        # Save to database using helper function
        update_layer_status(
            db,
            cropland_layer,
            "in_progress",
            "Saving cropland layer to database",
        )

        layer_info = _save_builtin_layer_with_status(
            db=db,
            layer=cropland_layer,
            layer_gdf=cropland_overlay_gdf,
            area_col=area_col,
        )

        # Update layer status
        cropland_layer.status = "successful"
        cropland_layer.details = format_success_message(
            cropland_layer.feature_count, cropland_layer.total_area_ha
        )
        db.commit()

        # Update project timestamp
        project.updated_at = datetime.utcnow()
        db.commit()

        return layer_info

    except Exception as e:
        cropland_layer.status = "failed"
        cropland_layer.details = format_error_message(e, "processing cropland layer")
        db.commit()
        raise


def process_water_layer(
    db: Session,
    project_id: str,
    create_only: bool = False,
) -> LayerInfo:
    """
    Process water layer from landcover TIFF data.

    Steps:
    1. Load khasras and get bounding box
    2. Load landcover TIFF tiles that overlap project
    3. Extract water shapes using class_b="Open surface water"
    4. Overlay with khasras to get intersection
    5. Save to database

    Args:
        db: Database session
        project_id: Project ID
        create_only: If True, only create layer record and return

    Returns:
        LayerInfo object
    """
    import rasterio
    import rasterio.mask

    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    if create_only:
        # Create placeholder layer record for async processing
        existing_layer = (
            db.query(LayerModel)
            .filter(LayerModel.project_id == project_id, LayerModel.name == "Water")
            .first()
        )

        if existing_layer:
            return LayerInfo(
                layer_type=LayerType.WATER.value,
                name=existing_layer.name,
                description="Open surface water bodies",
                is_unusable=existing_layer.is_unusable,
                parameters=existing_layer.parameters or {},
                status=existing_layer.status,
                details=existing_layer.details,
                area_ha=existing_layer.total_area_ha,
                feature_count=existing_layer.feature_count,
            )

        # Create new placeholder layer
        water_layer = LayerModel(
            project_id=project_id,
            name="Water",
            layer_type=LayerType.WATER.value,
            is_unusable=True,
            status="in_progress",
            details="Queued for processing",
            parameters={},
        )
        db.add(water_layer)
        db.commit()

        return LayerInfo(
            layer_type=LayerType.WATER.value,
            name="Water",
            description="Open surface water bodies",
            is_unusable=True,
            parameters={},
            status="in_progress",
            details="Queued for processing",
        )

    # Actual processing (create_only=False): Delete old layer and start fresh
    existing_layer = (
        db.query(LayerModel)
        .filter(LayerModel.project_id == project_id, LayerModel.name == "Water")
        .first()
    )
    if existing_layer:
        db.delete(existing_layer)

    db.commit()

    # Create fresh layer record
    water_layer = LayerModel(
        project_id=project_id,
        name="Water",
        layer_type=LayerType.WATER.value,
        is_unusable=True,
        status="in_progress",
        details="Loading khasras data",
        parameters={},
    )
    db.add(water_layer)
    db.commit()

    try:
        # Load khasras
        update_layer_status(db, water_layer, "in_progress", "Loading khasras data")
        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")

        # Project to target CRS
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
        gdf = sanitize_polygons_for_overlay(gdf)
        if gdf.empty:
            raise ValueError("No valid khasra geometries found after geometry repair")
        gdf_4326 = gdf.to_crs("EPSG:4326")

        # Load legend
        update_layer_status(db, water_layer, "in_progress", "Loading landcover data")
        legend_path = Path(settings.DATA_DIR) / "landcover" / "legend_processed.csv"
        class_value_dict = load_landcover_class_mapping(legend_path)

        # Load landcover TIFF and extract water shapes
        landcover_dir = Path(settings.DATA_DIR) / "landcover"

        # Find which TIFF files overlap the project bounds
        # For now, use 30N_070E_2020.tif (covers MP region)
        # TODO: Add logic to auto-detect overlapping tiles
        tiff_path = landcover_dir / "30N_070E_2020.tif"

        if not tiff_path.exists():
            raise FileNotFoundError(
                f"Landcover TIFF not found at {tiff_path}. "
                f"Please ensure the landcover data is available."
            )

        with rasterio.open(tiff_path) as src:
            update_layer_status(
                db, water_layer, "in_progress", "Extracting water from landcover"
            )

            # Mask to khasra bounds
            masked_data, masked_transform = rasterio.mask.mask(
                src, [gdf_4326.unary_union], crop=True
            )
            masked_data = np.squeeze(masked_data)

            # Extract water shapes
            water_shapes_gdf = get_landcover_shapes(
                landcover_data=masked_data,
                transform=masked_transform,
                class_name="Open surface water",
                class_value_lookup_dict=class_value_dict,
                raster_crs=str(src.crs),
                target_crs=f"EPSG:{settings.INDIA_PROJECTED_CRS}",
            )

        water_shapes_gdf = sanitize_polygons_for_overlay(water_shapes_gdf)

        if water_shapes_gdf.empty:
            update_layer_status(
                db, water_layer, "successful", format_success_no_data_message()
            )
            water_layer.feature_count = 0
            water_layer.total_area_ha = 0.0
            db.commit()

            return LayerInfo(
                layer_type=LayerType.WATER.value,
                name="Water",
                description="Open surface water bodies",
                is_unusable=True,
                parameters={},
                status="successful",
                details=format_success_no_data_message(),
                area_ha=0,
                feature_count=0,
            )

        # Overlay with khasras
        update_layer_status(
            db, water_layer, "in_progress", "Overlaying water onto khasras"
        )
        water_overlay_gdf = gpd.overlay(water_shapes_gdf, gdf, how="intersection")
        water_overlay_gdf = water_overlay_gdf.dissolve(
            by="Khasra ID (Unique)"
        ).reset_index()

        # Calculate areas
        area_col = "Unusable Area - Water (ha)"
        water_overlay_gdf[area_col] = water_overlay_gdf.geometry.area / 10_000

        # Save to database using helper function
        update_layer_status(
            db, water_layer, "in_progress", "Saving water layer to database"
        )

        layer_info = _save_builtin_layer_with_status(
            db=db,
            layer=water_layer,
            layer_gdf=water_overlay_gdf,
            area_col=area_col,
        )

        # Update layer status
        water_layer.status = "successful"
        water_layer.details = format_success_message(
            water_layer.feature_count, water_layer.total_area_ha
        )
        db.commit()

        # Update project timestamp
        project.updated_at = datetime.utcnow()
        db.commit()

        return layer_info

    except Exception as e:
        water_layer.status = "failed"
        water_layer.details = format_error_message(e, "processing water layer")
        db.commit()
        raise


def process_slopes_layer(
    db: Session,
    project_id: str,
    include_north_slopes: bool = True,
    include_other_slopes: bool = True,
    north_min_angle: float = 7.0,
    other_min_angle: float = 10.0,
    create_only: bool = False,
) -> List[LayerInfo]:
    """
    Process slopes layer from NASA ALOS PALSAR DEM data.

    Steps:
    1. Load khasras and get bounding box
    2. Search and download DEM tiles from NASA ALOS using ASF API
    3. Calculate slopes and aspects from DEM
    4. Extract steep slope areas based on angle thresholds
    5. Overlay with khasras to get intersection
    6. Save to database

    Args:
        db: Database session
        project_id: Project ID
        include_north_slopes: Include north-facing slopes (45-135° aspect)
        include_other_slopes: Include other-facing slopes  (<45° or >135° aspect)
        north_min_angle: Minimum angle for north slopes (degrees)
        other_min_angle: Minimum angle for other slopes (degrees)
        create_only: If True, only create layer record and return

    Returns:
        LayerInfo object
    """
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    if create_only:
        # Return placeholder LayerInfo objects for the layers that will be created
        result_layers = []

        if include_north_slopes:
            result_layers.append(
                LayerInfo(
                    layer_type=LayerType.SLOPE_NORTH.value,
                    name="Slopes - North Facing",
                    description="North-facing steep slopes (NE to NW, 45-135°)",
                    is_unusable=True,
                    parameters={"min_angle": north_min_angle},
                    status="in_progress",
                    details="Queued for processing",
                )
            )

        if include_other_slopes:
            result_layers.append(
                LayerInfo(
                    layer_type=LayerType.SLOPE_OTHER.value,
                    name="Slopes - Other Facing",
                    description="Other-facing steep slopes (S/E/W, <45° or >135°)",
                    is_unusable=True,
                    parameters={"min_angle": other_min_angle},
                    status="in_progress",
                    details="Queued for processing",
                )
            )

        return result_layers

    # Delete any existing slope layers to start fresh
    existing_north = (
        db.query(LayerModel)
        .filter(
            LayerModel.project_id == project_id,
            LayerModel.name == "Slopes - North Facing",
        )
        .first()
    )
    if existing_north:
        db.delete(existing_north)

    existing_other = (
        db.query(LayerModel)
        .filter(
            LayerModel.project_id == project_id,
            LayerModel.name == "Slopes - Other Facing",
        )
        .first()
    )
    if existing_other:
        db.delete(existing_other)

    # Also delete legacy "Slopes" layer if it exists
    legacy_slopes = (
        db.query(LayerModel)
        .filter(LayerModel.project_id == project_id, LayerModel.name == "Slopes")
        .first()
    )
    if legacy_slopes:
        db.delete(legacy_slopes)

    db.commit()

    # Create fresh layer records
    north_layer = None
    other_layer = None

    if include_north_slopes:
        north_layer = LayerModel(
            project_id=project_id,
            name="Slopes - North Facing",
            layer_type=LayerType.SLOPE_NORTH.value,
            is_unusable=True,
            status="in_progress",
            details="Starting processing",
            parameters={"min_angle": north_min_angle},
        )
        db.add(north_layer)

    if include_other_slopes:
        other_layer = LayerModel(
            project_id=project_id,
            name="Slopes - Other Facing",
            layer_type=LayerType.SLOPE_OTHER.value,
            is_unusable=True,
            status="in_progress",
            details="Starting processing",
            parameters={"min_angle": other_min_angle},
        )
        db.add(other_layer)

    db.flush()  # Ensure layers have IDs
    db.commit()

    # Load khasras and process slopes
    try:
        # Update status
        if north_layer:
            update_layer_status(db, north_layer, "in_progress", "Loading khasras data")
        if other_layer:
            update_layer_status(db, other_layer, "in_progress", "Loading khasras data")

        # Load khasras
        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")

        # Project to target CRS and get bounds
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
        gdf = sanitize_polygons_for_overlay(gdf)
        if gdf.empty:
            raise ValueError("No valid khasra geometries found after geometry repair")
        gdf_4326 = gdf.to_crs("EPSG:4326")

        # Get bounding box for DEM search
        xmin, ymin, xmax, ymax = gdf_4326.total_bounds
        from shapely.geometry import Polygon as ShapelyPolygon

        bbox_poly = ShapelyPolygon(
            [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin)]
        )
        wkt_string = bbox_poly.wkt

        # Search for DEM data using ASF
        if north_layer:
            update_layer_status(
                db, north_layer, "in_progress", "Searching for NASA elevation data"
            )
        if other_layer:
            update_layer_status(
                db, other_layer, "in_progress", "Searching for NASA elevation data"
            )

        try:
            import asf_search as asf
        except ImportError:
            raise ValueError(
                "asf_search package not installed. Install with: pip install asf_search"
            )

        results = asf.geo_search(
            platform=[asf.PLATFORM.ALOS],
            processingLevel=asf.PRODUCT_TYPE.RTC_HIGH_RES,
            intersectsWith=wkt_string,
        )

        if not results:
            raise ValueError("No DEM tiles found for the project area")

        # Use greedy algorithm to select minimum number of DEMs that cover maximum area
        # This approach from the notebook ensures we only download necessary tiles
        logger.info(f"Initial search found {len(results)} DEM tiles")

        from shapely.geometry import shape as shapely_shape

        # Create GeoDataFrame of DEM tiles
        dem_tiles = []
        for result in results:
            try:
                tile_geom_dict = result.geometry
                if tile_geom_dict:
                    tile_poly = shapely_shape(tile_geom_dict)
                    dem_tiles.append(
                        {
                            "result": result,
                            "geometry": tile_poly,
                            "sceneName": result.properties.get("sceneName", "unknown"),
                        }
                    )
            except Exception as e:
                logger.warning(f"Could not parse tile geometry: {e}")

        if not dem_tiles:
            raise ValueError("Could not parse any DEM tile geometries")

        dem_tiles_gdf = gpd.GeoDataFrame(dem_tiles, crs="EPSG:4326")
        # Reproject to projected CRS for accurate area calculations
        dem_tiles_gdf = dem_tiles_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
        khasras_projected = gdf.copy()  # Already in projected CRS

        # Greedy selection: pick DEM that covers most area, repeat until all covered
        selected_dems = []
        remaining_khasras = khasras_projected.copy()
        remaining_dems = dem_tiles_gdf.copy()

        while len(remaining_khasras) > 0 and len(remaining_dems) > 0:
            # Calculate coverage area for each remaining DEM
            remaining_dems["coverage_area"] = remaining_dems.geometry.apply(
                lambda dem_geom: remaining_khasras.intersection(dem_geom).area.sum()
            )

            # Find DEM with maximum coverage
            best_idx = remaining_dems["coverage_area"].idxmax()
            best_dem = remaining_dems.loc[best_idx]

            # Stop if no new coverage is added
            if best_dem["coverage_area"] == 0:
                break

            # Add to selected DEMs
            selected_dems.append(best_dem["result"])
            coverage_ha = best_dem["coverage_area"] / 10_000
            logger.info(
                f"Selected DEM {len(selected_dems)}: {best_dem['sceneName']} (covers {coverage_ha:.2f} ha)"
            )

            # Remove covered khasras and this DEM
            remaining_khasras = remaining_khasras[
                ~remaining_khasras.intersects(best_dem.geometry)
            ]
            remaining_dems = remaining_dems.drop(best_idx)

        if selected_dems:
            results = selected_dems
            uncovered_area_ha = (
                remaining_khasras.area.sum() / 10_000
                if len(remaining_khasras) > 0
                else 0
            )
            logger.info(
                f"Greedy algorithm selected {len(results)} DEM tiles (reduced from {len(dem_tiles)})"
            )
            if uncovered_area_ha > 0:
                logger.warning(f"Remaining uncovered area: {uncovered_area_ha:.2f} ha")
        else:
            logger.warning(f"Greedy selection failed, using all {len(results)} tiles")

        if north_layer:
            update_layer_status(
                db,
                north_layer,
                "in_progress",
                f"Selected {len(results)} data tiles, downloading...",
            )
        if other_layer:
            update_layer_status(
                db,
                other_layer,
                "in_progress",
                f"Selected {len(results)} data tiles, downloading...",
            )

        # Authenticate with NASA Earthdata
        if north_layer:
            update_layer_status(
                db,
                north_layer,
                "in_progress",
                "Authenticating with NASA Earthdata servers",
            )
        if other_layer:
            update_layer_status(
                db,
                other_layer,
                "in_progress",
                "Authenticating with NASA Earthdata servers",
            )

        if not settings.EARTHDATA_USERNAME or not settings.EARTHDATA_PASSWORD:
            raise ValueError(
                "NASA Earthdata credentials not configured. "
                "Please set EARTHDATA_USERNAME and EARTHDATA_PASSWORD environment variables."
            )

        try:
            session = asf.ASFSession().auth_with_creds(
                settings.EARTHDATA_USERNAME, settings.EARTHDATA_PASSWORD
            )
        except Exception as e:
            raise ValueError(f"Failed to authenticate with NASA Earthdata: {str(e)}")

        # Create DEM directory
        dem_dir = settings.DATA_DIR / "nasa_alos_palsar_dems"
        dem_dir.mkdir(parents=True, exist_ok=True)

        # Download DEM files
        dem_files = []
        for idx, result in enumerate(results):
            if north_layer:
                update_layer_status(
                    db,
                    north_layer,
                    "in_progress",
                    f"Downloading elevation data tile {idx+1} of {len(results)}",
                )
            if other_layer:
                update_layer_status(
                    db,
                    other_layer,
                    "in_progress",
                    f"Downloading elevation data tile {idx+1} of {len(results)}",
                )

            # Create subdirectory for this DEM
            dem_name = result.properties["sceneName"].replace(".zip", "")
            dem_subdir = dem_dir / dem_name
            dem_subdir.mkdir(parents=True, exist_ok=True)

            # Check if we already have the standardized .tif file
            dem_tif_path = dem_subdir / f"{dem_name}.tif"
            if dem_tif_path.exists():
                logger.info(f"DEM .tif already downloaded: {dem_tif_path.name}")
            else:
                # Need to download and process
                logger.info(f"Downloading new DEM: {dem_name}")
                import shutil
                import zipfile as zf

                # Check for existing zip files and validate them
                existing_zips = list(dem_subdir.glob("*.zip"))

                # Remove any corrupted/invalid zip files
                for existing_zip in existing_zips:
                    try:
                        with zf.ZipFile(existing_zip, "r") as test_zip:
                            # Try to read the file list to validate
                            test_zip.namelist()
                        logger.info(f"Existing valid zip found: {existing_zip.name}")
                    except (zf.BadZipFile, EOFError, OSError) as e:
                        logger.warning(
                            f"Found corrupted zip file {existing_zip.name}, deleting: {e}"
                        )
                        existing_zip.unlink()

                # Download the file (ASF will skip if valid file exists)
                result.download(path=str(dem_subdir), session=session)

                # Find the downloaded .zip file
                zip_files = list(dem_subdir.glob("*.zip"))

                if not zip_files:
                    raise ValueError(
                        f"No zip file found after download in {dem_subdir}"
                    )

                zip_path = zip_files[0]
                logger.info(f"Processing zip: {zip_path.name}")

                # Validate and extract
                logger.info(f"Extracting {zip_path.name}...")
                try:
                    with zf.ZipFile(zip_path, "r") as zip_ref:
                        # Log contents for debugging
                        file_list = zip_ref.namelist()
                        logger.info(f"Zip contains {len(file_list)} files")
                        logger.debug(f"First few files: {file_list[:5]}")
                        zip_ref.extractall(dem_subdir)
                except (zf.BadZipFile, EOFError) as e:
                    # Delete corrupted file and raise error
                    logger.error(f"Corrupted zip file detected: {e}")
                    zip_path.unlink()
                    raise ValueError(
                        f"Downloaded zip file is corrupted: {zip_path.name}. Please try again."
                    )

                # Find the DEM .tif file
                # ALOS structure: zip contains a folder, inside is {folder_name}.dem.tif
                # Look for files with '.dem.tif' extension first
                tif_files = list(dem_subdir.glob("**/*.dem.tif"))
                if not tif_files:
                    # Fallback: look for any .tif file
                    tif_files = list(dem_subdir.glob("**/*.tif"))

                if tif_files:
                    found_tif = tif_files[0]
                    logger.info(f"Found DEM file: {found_tif.relative_to(dem_subdir)}")

                    # Copy the file to standard location
                    if found_tif != dem_tif_path:
                        shutil.copy2(found_tif, dem_tif_path)
                        logger.info(f"Copied to standard location: {dem_tif_path.name}")

                    # Clean up: delete zip file and extracted folder
                    zip_path.unlink()
                    logger.info(f"Deleted zip file: {zip_path.name}")

                    # Delete extracted folder (if it's different from dem_subdir)
                    extracted_folder = found_tif.parent
                    if (
                        extracted_folder != dem_subdir
                        and extracted_folder.is_relative_to(dem_subdir)
                    ):
                        shutil.rmtree(extracted_folder)
                        logger.info(
                            f"Deleted extracted folder: {extracted_folder.name}"
                        )
                else:
                    # List what we did find for debugging
                    all_files = list(dem_subdir.glob("**/*"))
                    logger.error(
                        f"No .tif files found. Files in directory: {[f.name for f in all_files[:10]]}"
                    )
                    raise ValueError(
                        f"No .tif file found in extracted zip for {dem_name}"
                    )

            # Verify the TIF exists before adding to list
            if not dem_tif_path.exists():
                raise ValueError(f"Failed to prepare DEM file: {dem_tif_path}")

            dem_files.append((dem_subdir, dem_name))

        # Verify we have DEM files to process
        if not dem_files:
            raise ValueError(
                "Failed to download and extract any DEM files successfully"
            )

        # Process each DEM file to extract slopes - keep north and other separate
        if north_layer:
            update_layer_status(
                db, north_layer, "in_progress", "Calculating slopes from elevation data"
            )
        if other_layer:
            update_layer_status(
                db, other_layer, "in_progress", "Calculating slopes from elevation data"
            )

        north_slope_gdfs = []
        other_slope_gdfs = []
        dem_processing_errors = []

        for idx, (dem_subdir, dem_name) in enumerate(dem_files):
            if north_layer:
                update_layer_status(
                    db,
                    north_layer,
                    "in_progress",
                    f"Processing elevation data tile {idx+1} of {len(dem_files)}: {dem_name}",
                )
            if other_layer:
                update_layer_status(
                    db,
                    other_layer,
                    "in_progress",
                    f"Processing elevation data tile {idx+1} of {len(dem_files)}: {dem_name}",
                )

            dem_tif_path = dem_subdir / f"{dem_name}.tif"
            if not dem_tif_path.exists():
                error_msg = f"DEM file not found: {dem_tif_path}"
                logger.error(error_msg)
                dem_processing_errors.append(error_msg)
                continue

            # Process north-facing slopes
            if include_north_slopes:
                try:
                    slope_gdf = _extract_steep_slopes_from_dem(
                        dem_filepath=dem_tif_path,
                        dem_subdir=dem_subdir,
                        dem_filename=dem_name,
                        slope_type="north",
                        min_angle=north_min_angle,
                        output_crs=settings.INDIA_PROJECTED_CRS,
                    )
                    if len(slope_gdf) > 0:
                        north_slope_gdfs.append(slope_gdf)
                except Exception as e:
                    error_msg = f"Error processing {dem_name} (north slopes): {e}"
                    logger.error(error_msg)
                    dem_processing_errors.append(error_msg)

            # Process other-facing slopes
            if include_other_slopes:
                try:
                    slope_gdf = _extract_steep_slopes_from_dem(
                        dem_filepath=dem_tif_path,
                        dem_subdir=dem_subdir,
                        dem_filename=dem_name,
                        slope_type="other",
                        min_angle=other_min_angle,
                        output_crs=settings.INDIA_PROJECTED_CRS,
                    )
                    if len(slope_gdf) > 0:
                        other_slope_gdfs.append(slope_gdf)
                except Exception as e:
                    error_msg = f"Error processing {dem_name} (other slopes): {e}"
                    logger.error(error_msg)
                    dem_processing_errors.append(error_msg)

        # If we had errors processing DEMs and got no results, raise an error
        if dem_processing_errors and not north_slope_gdfs and not other_slope_gdfs:
            raise ValueError(
                f"Failed to process any DEM files successfully. Errors: {'; '.join(dem_processing_errors[:3])}"
            )

        # Check if we have any slopes at all
        if not north_slope_gdfs and not other_slope_gdfs:
            message = f"Processed {len(dem_files)} DEM tiles but no steep slopes found matching criteria"
            logger.info(message)

            # Mark layers as successful with no data
            if north_layer:
                north_layer.status = "successful"
                north_layer.details = format_success_no_data_message()
                north_layer.feature_count = 0
                north_layer.total_area_ha = 0.0
            if other_layer:
                other_layer.status = "successful"
                other_layer.details = format_success_no_data_message()
                other_layer.feature_count = 0
                other_layer.total_area_ha = 0.0

            db.commit()
            return []

        # Process each slope type as a separate layer
        result_layers = []

        # Process north-facing slopes
        if north_slope_gdfs and north_layer:
            update_layer_status(
                db, north_layer, "in_progress", "Combining north slope shapes"
            )
            north_slopes_gdf = pd.concat(north_slope_gdfs, ignore_index=True)
            north_slopes_gdf = sanitize_polygons_for_overlay(north_slopes_gdf)
            logger.info(f"Combined {len(north_slopes_gdf)} north slope polygons")

            # Overlay with khasras
            north_overlay_gdf = gpd.overlay(north_slopes_gdf, gdf, how="intersection")
            logger.info(
                f"North slopes after overlay: {len(north_overlay_gdf)} features"
            )

            if not north_overlay_gdf.empty:
                north_overlay_gdf = north_overlay_gdf.dissolve(
                    by="Khasra ID (Unique)"
                ).reset_index()
                area_col = "Unusable Area - North Slopes (ha)"
                north_overlay_gdf[area_col] = north_overlay_gdf.geometry.area / 10_000

                # Save to database
                layer_info = _save_builtin_layer_with_status(
                    db=db,
                    layer=north_layer,
                    layer_gdf=north_overlay_gdf,
                    area_col=area_col,
                )
                north_layer.status = "successful"
                north_layer.details = format_success_message(
                    north_layer.feature_count, north_layer.total_area_ha
                )
                result_layers.append(layer_info)
            else:
                north_layer.status = "successful"
                north_layer.details = format_success_no_data_message()
                north_layer.feature_count = 0
                north_layer.total_area_ha = 0.0

        # Process other-facing slopes
        if other_slope_gdfs and other_layer:
            update_layer_status(
                db, other_layer, "in_progress", "Combining other slope shapes"
            )
            other_slopes_gdf = pd.concat(other_slope_gdfs, ignore_index=True)
            other_slopes_gdf = sanitize_polygons_for_overlay(other_slopes_gdf)
            logger.info(f"Combined {len(other_slopes_gdf)} other slope polygons")

            # Overlay with khasras
            other_overlay_gdf = gpd.overlay(other_slopes_gdf, gdf, how="intersection")
            logger.info(
                f"Other slopes after overlay: {len(other_overlay_gdf)} features"
            )

            if not other_overlay_gdf.empty:
                other_overlay_gdf = other_overlay_gdf.dissolve(
                    by="Khasra ID (Unique)"
                ).reset_index()
                area_col = "Unusable Area - Other Slopes (ha)"
                other_overlay_gdf[area_col] = other_overlay_gdf.geometry.area / 10_000

                # Save to database
                layer_info = _save_builtin_layer_with_status(
                    db=db,
                    layer=other_layer,
                    layer_gdf=other_overlay_gdf,
                    area_col=area_col,
                )
                other_layer.status = "successful"
                other_layer.details = format_success_message(
                    other_layer.feature_count, other_layer.total_area_ha
                )
                result_layers.append(layer_info)
            else:
                other_layer.status = "successful"
                other_layer.details = format_success_no_data_message()
                other_layer.feature_count = 0
                other_layer.total_area_ha = 0.0

        # Mark layers as successful with no data if they were requested but not processed
        if include_north_slopes and north_layer and not north_slope_gdfs:
            north_layer.status = "successful"
            north_layer.details = "No north slopes found matching criteria"
            north_layer.feature_count = 0
            north_layer.total_area_ha = 0.0

        if include_other_slopes and other_layer and not other_slope_gdfs:
            other_layer.status = "successful"
            other_layer.details = "No other slopes found matching criteria"
            other_layer.feature_count = 0
            other_layer.total_area_ha = 0.0

        # Update project timestamp
        project.updated_at = datetime.utcnow()
        db.commit()

        return result_layers

    except Exception as e:
        # Error handling: If layers were created, mark them as failed
        db.rollback()

        # Try to update any layers that were created
        north_layer = (
            db.query(LayerModel)
            .filter(
                LayerModel.project_id == project_id,
                LayerModel.name == "Slopes - North Facing",
            )
            .first()
        )
        if north_layer:
            north_layer.status = "failed"
            north_layer.details = format_error_message(
                e, "processing north-facing slopes"
            )

        other_layer = (
            db.query(LayerModel)
            .filter(
                LayerModel.project_id == project_id,
                LayerModel.name == "Slopes - Other Facing",
            )
            .first()
        )
        if other_layer:
            other_layer.status = "failed"
            other_layer.details = format_error_message(
                e, "processing other-facing slopes"
            )

        db.commit()
        raise


def _extract_steep_slopes_from_dem(
    dem_filepath: Path,
    dem_subdir: Path,
    dem_filename: str,
    slope_type: str,
    min_angle: float,
    output_crs: int,
) -> gpd.GeoDataFrame:
    """
    Extract steep slope polygons from a DEM file.

    Args:
        dem_filepath: Path to DEM .tif file
        dem_subdir: Directory containing DEM and cached arrays
        dem_filename: Name of DEM file (without extension)
        slope_type: Either "north" or "other"
        min_angle: Minimum slope angle in degrees
        output_crs: Output CRS EPSG code

    Returns:
        GeoDataFrame of steep slope polygons
    """
    import rasterio
    from rasterio.features import shapes
    from shapely.geometry import shape

    if slope_type not in ["north", "other"]:
        raise ValueError("slope_type must be either 'north' or 'other'")

    # Try to load pre-calculated slopes and aspects
    slope_file = dem_subdir / f"{dem_filename}_magnitude.npy"
    aspect_file = dem_subdir / f"{dem_filename}_aspect.npy"

    if slope_file.exists() and aspect_file.exists():
        slope_pydem = np.load(slope_file)
        aspect_pydem = np.load(aspect_file)

        # Load transform from the DEM file
        with rasterio.open(dem_filepath) as src:
            transform = src.transform
            input_crs = src.crs
    else:
        # Calculate slopes and aspects using pydem
        try:
            from pydem.dem_processing import DEMProcessor
        except ImportError:
            raise ValueError(
                "pydem package not installed. Install with: pip install pydem"
            )

        dem_proc = DEMProcessor(str(dem_filepath))
        transform = dem_proc.transform

        with rasterio.open(dem_filepath) as src:
            input_crs = src.crs

        slope_pydem, aspect_pydem = dem_proc.calc_slopes_directions()

        # Save for future use
        np.save(slope_file, slope_pydem)
        np.save(aspect_file, aspect_pydem)

    # Convert from radians to degrees
    aspect = np.degrees(aspect_pydem)
    slope = np.degrees(slope_pydem)

    # Clean negative values
    aspect[aspect < 0] = 0
    slope[slope < 0] = 0

    # Log statistics for debugging
    logger.info(
        f"DEM {dem_filename} - Slope stats: min={slope.min():.2f}°, max={slope.max():.2f}°, mean={slope.mean():.2f}°"
    )
    logger.info(
        f"DEM {dem_filename} - Aspect stats: min={aspect.min():.2f}°, max={aspect.max():.2f}°, mean={aspect.mean():.2f}°"
    )

    # Apply slope/aspect filters based on type
    if slope_type == "north":
        # North-facing slopes: NE to NW (45-135°) with angle > min_angle
        slope_mask = np.where(
            (aspect >= 45) & (aspect < 135) & (slope > min_angle), True, False
        )
        logger.info(
            f"DEM {dem_filename} - North slopes > {min_angle}°: {slope_mask.sum()} pixels ({100*slope_mask.sum()/slope_mask.size:.2f}%)"
        )
    elif slope_type == "other":
        # Other-facing slopes: remaining directions with angle > min_angle
        slope_mask = np.where(
            ((aspect < 45) | (aspect >= 135)) & (slope > min_angle), True, False
        )
        logger.info(
            f"DEM {dem_filename} - Other slopes > {min_angle}°: {slope_mask.sum()} pixels ({100*slope_mask.sum()/slope_mask.size:.2f}%)"
        )

    # Extract vector shapes from raster mask
    # Convert boolean mask to uint8 for rasterio shapes
    slope_mask_uint8 = slope_mask.astype(np.uint8)

    vector_shapes = [
        {"geometry": shape(geom)}
        for geom, class_value in shapes(
            slope_mask_uint8, mask=slope_mask, transform=transform
        )
        if class_value
        == 1  # Only get the slope areas (value=1), not background (value=0)
    ]

    logger.info(
        f"DEM {dem_filename} - Extracted {len(vector_shapes)} slope polygons ({slope_type} type)"
    )

    if not vector_shapes:
        return gpd.GeoDataFrame(columns=["geometry"], crs=f"EPSG:{output_crs}")

    # Create GeoDataFrame with explicit geometry column
    slope_shapes_gdf = gpd.GeoDataFrame(vector_shapes, geometry="geometry")

    # Set CRS and transform to output CRS
    if input_crs:
        slope_shapes_gdf = slope_shapes_gdf.set_crs(input_crs)
    slope_shapes_gdf = slope_shapes_gdf.to_crs(f"EPSG:{output_crs}")

    return slope_shapes_gdf


def _save_builtin_layer_with_status(
    db: Session,
    layer: LayerModel,
    layer_gdf: gpd.GeoDataFrame,
    area_col: str,
) -> LayerInfo:
    """Helper function to save a builtin layer to database with status tracking"""

    # Update layer metadata
    layer.feature_count = len(layer_gdf)
    layer.total_area_ha = float(round(layer_gdf[area_col].sum(), 2))
    layer.parameters = {"area_col": area_col}
    db.flush()

    # Store per-khasra layer features in database
    layer_4326 = layer_gdf.to_crs("EPSG:4326")
    for idx, row in layer_4326.iterrows():
        geom = row.geometry
        # Convert to MultiPolygon if needed
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        elif geom.geom_type == "GeometryCollection":
            polygons = [
                g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")
            ]
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
            khasra_id_unique=row.get("Khasra ID (Unique)", ""),
            geometry=from_shape(geom, srid=4326),
            area_ha=round(row[area_col], 4),
            properties={
                "layer_name": layer.name,
                "is_unusable": layer.is_unusable,
            },
        )
        db.add(feature)

    # Mark as successful
    layer.status = "successful"
    layer.details = format_success_message(layer.feature_count, layer.total_area_ha)
    db.commit()

    return LayerInfo(
        layer_type=layer.layer_type,
        name=layer.name,
        description=f"Builtin layer: {layer.name}",
        is_unusable=layer.is_unusable,
        parameters=layer.parameters or {},
        area_ha=layer.total_area_ha,
        feature_count=layer.feature_count,
        status="successful",
        details=layer.details,
    )


# ============ Area Calculations ============


def calculate_usable_areas(db: Session, project_id: str) -> gpd.GeoDataFrame:
    """Calculate usable and available areas after applying all layers

    All data is loaded from the database, no file dependencies.
    """
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    gdf = get_khasras_gdf(db, project_id, projected=True)
    if gdf is None:
        raise ValueError("Khasras must be uploaded first")

    available_gdf = gdf.copy()
    available_gdf["Original Area (ha)"] = available_gdf.geometry.area / 10_000

    # Get all successful layers
    layers = [
        layer_model
        for layer_model in get_layers_metadata(db, project_id)
        if layer_model.status == "successful"
    ]

    # Apply unusable layers (cut out from geometry)
    for layer in layers:
        if layer.is_unusable:
            layer_gdf = load_layer_gdf_by_id(db, layer.id)
            if layer_gdf is not None and len(layer_gdf) > 0:
                layer_gdf = layer_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
                available_gdf = difference_overlay_without_discard(
                    available_gdf, layer_gdf
                )

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
            layer_gdf = load_layer_gdf_by_id(db, layer.id)
            if layer_gdf is not None and len(layer_gdf) > 0:
                layer_gdf = layer_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
                available_gdf = difference_overlay_without_discard(
                    available_gdf, layer_gdf
                )

    available_gdf["Usable and Available Area (ha)"] = available_gdf.area / 10_000
    available_gdf["Usable but Unavailable Area (ha)"] = (
        available_gdf["Usable Area (ha)"]
        - available_gdf["Usable and Available Area (ha)"]
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

    # Add layer-specific area columns from layer_features
    layer_area_columns = []
    for layer in layers:
        if layer.parameters:
            area_col = layer.parameters.get("area_col")
            if area_col:
                # Get per-khasra areas from layer_features
                features = (
                    db.query(LayerFeatureModel)
                    .filter(LayerFeatureModel.layer_id == layer.id)
                    .all()
                )

                khasra_areas = {}
                for f in features:
                    if f.khasra_id_unique:
                        if f.khasra_id_unique not in khasra_areas:
                            khasra_areas[f.khasra_id_unique] = 0
                        khasra_areas[f.khasra_id_unique] += f.area_ha or 0

                # Add column to available_gdf
                available_gdf[area_col] = available_gdf["Khasra ID (Unique)"].map(
                    lambda x: khasra_areas.get(x, 0)
                )
                layer_area_columns.append(area_col)

    # Update khasra records in database with calculated areas
    for _, row in available_gdf.iterrows():
        khasra_unique_id = row.get("Khasra ID (Unique)")
        if khasra_unique_id:
            khasra = (
                db.query(KhasraModel)
                .filter(
                    KhasraModel.project_id == project_id,
                    KhasraModel.khasra_id_unique == khasra_unique_id,
                )
                .first()
            )
            if khasra:
                khasra.original_area_ha = round(row.get("Original Area (ha)", 0), 4)
                khasra.usable_area_ha = round(row.get("Usable Area (ha)", 0), 4)
                khasra.unusable_area_ha = round(row.get("Unusable Area (ha)", 0), 4)
                khasra.usable_available_area_ha = round(
                    row.get("Usable and Available Area (ha)", 0), 4
                )

                # Store per-layer areas in JSONB field
                if layer_area_columns:
                    layer_areas_dict = {}
                    for col in layer_area_columns:
                        value = row.get(col, 0)
                        if pd.notna(value):
                            layer_areas_dict[col] = round(float(value), 4)
                    if layer_areas_dict:
                        khasra.layer_areas = layer_areas_dict

    project.updated_at = datetime.utcnow()
    db.commit()

    return available_gdf


def recalculate_areas_and_parcels(db: Session, project_id: str):
    """Helper function to recalculate areas and update parcels after layer changes"""
    try:
        # Recalculate usable areas
        logger.info(f"Recalculating areas for project {project_id} after layer change")
        calculate_usable_areas(db, project_id)

        # Check if parcels exist for this project
        parcels_exist = (
            db.query(ParcelModel).filter(ParcelModel.project_id == project_id).count()
            > 0
        )

        if parcels_exist:
            logger.info(f"Recalculating parcel areas for project {project_id}")
            # Get the latest clustering run
            clustering_run = (
                db.query(ClusteringRunModel)
                .filter(ClusteringRunModel.project_id == project_id)
                .order_by(ClusteringRunModel.created_at.desc())
                .first()
            )

            if clustering_run:
                # Re-aggregate khasra stats to parcel level
                khasras_gdf = get_khasras_with_stats_gdf(db, project_id)
                if khasras_gdf is not None and not khasras_gdf.empty:
                    # Group by parcel_id and aggregate
                    parcel_updates = {}
                    for parcel_id in khasras_gdf["Parcel ID"].unique():
                        if pd.isna(parcel_id) or "UNCLUSTERED" in str(parcel_id):
                            continue

                        parcel_khasras = khasras_gdf[
                            khasras_gdf["Parcel ID"] == parcel_id
                        ]

                        # Aggregate area columns
                        original_area = parcel_khasras["Original Area (ha)"].sum()
                        usable_area = parcel_khasras["Usable Area (ha)"].sum()
                        unusable_area = parcel_khasras["Unusable Area (ha)"].sum()
                        usable_available_area = parcel_khasras[
                            "Usable and Available Area (ha)"
                        ].sum()

                        # Aggregate layer-specific areas
                        layer_areas_dict = {}
                        for col in parcel_khasras.columns:
                            if col.endswith("(ha)") and col not in [
                                "Original Area (ha)",
                                "Usable Area (ha)",
                                "Unusable Area (ha)",
                                "Usable and Available Area (ha)",
                            ]:
                                layer_areas_dict[col] = round(
                                    float(parcel_khasras[col].sum()), 2
                                )

                        parcel_updates[parcel_id] = {
                            "original_area_ha": round(original_area, 2),
                            "usable_area_ha": round(usable_area, 2),
                            "unusable_area_ha": round(unusable_area, 2),
                            "usable_available_area_ha": round(usable_available_area, 2),
                            "layer_areas": layer_areas_dict
                            if layer_areas_dict
                            else None,
                        }

                    # Update parcel records
                    for parcel_id, updates in parcel_updates.items():
                        parcel = (
                            db.query(ParcelModel)
                            .filter(
                                ParcelModel.project_id == project_id,
                                ParcelModel.parcel_id == parcel_id,
                            )
                            .first()
                        )
                        if parcel:
                            parcel.original_area_ha = updates["original_area_ha"]
                            parcel.usable_area_ha = updates["usable_area_ha"]
                            parcel.unusable_area_ha = updates["unusable_area_ha"]
                            parcel.usable_available_area_ha = updates[
                                "usable_available_area_ha"
                            ]
                            parcel.layer_areas = updates["layer_areas"]

                            # Mark as modified for JSONB field
                            from sqlalchemy.orm import attributes

                            if updates["layer_areas"]:
                                attributes.flag_modified(parcel, "layer_areas")

                    db.commit()
                    logger.info(
                        f"Updated {len(parcel_updates)} parcels with new area calculations"
                    )

        logger.info(f"Successfully recalculated areas for project {project_id}")

    except Exception as e:
        logger.error(f"Error recalculating areas for project {project_id}: {str(e)}")
        # Don't raise - we want layer operations to succeed even if recalculation fails
        db.rollback()


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

    # Use threading backend to avoid pickling issues with STRtree
    # Limit to reasonable number of threads to avoid resource leaks
    n_jobs_safe = min(n_jobs, 4) if n_jobs > 0 else 4

    with Parallel(n_jobs=n_jobs_safe, backend="threading") as parallel:
        results = parallel(
            delayed(_get_distances_for_geom)(
                i, geom, geometries, tree, max_distance_considered
            )
            for i, geom in enumerate(geometries)
        )

    distance_matrix = np.full((n, n), 99999)
    for res_list in results:
        for i, j, d in res_list:
            distance_matrix[i, j] = d
            distance_matrix[j, i] = d  # Ensure matrix is symmetric

    np.fill_diagonal(distance_matrix, 0)

    # Log matrix statistics for debugging
    logger.info(f"Distance matrix shape: {distance_matrix.shape}")
    if n > 1:
        non_diagonal = distance_matrix[~np.eye(n, dtype=bool)]
        logger.info(f"Distance matrix min (non-diagonal): {np.min(non_diagonal)}")
        logger.info(f"Distance matrix max: {np.max(distance_matrix)}")
        logger.info(
            f"Number of finite distances (< 99999): {np.sum(distance_matrix < 99999) - n}"
        )
    else:
        logger.info("Single khasra - no pairwise distances to compute")

    return distance_matrix


def _create_distance_matrix_metadata(khasra_ids: List[str]) -> Dict[str, Any]:
    """Create metadata for distance matrix validation"""
    import hashlib
    from datetime import datetime

    ids_string = ",".join(khasra_ids)
    checksum = hashlib.sha256(ids_string.encode()).hexdigest()

    return {
        "khasra_ids": khasra_ids,
        "count": len(khasra_ids),
        "created_at": datetime.utcnow().isoformat(),
        "checksum": checksum,
    }


def _validate_distance_matrix_metadata(
    metadata: Dict[str, Any], current_khasra_ids: List[str]
) -> bool:
    """Validate that saved distance matrix matches current khasras

    Returns True if valid, False if matrix should be rebuilt
    """
    import hashlib

    # Check count
    if metadata.get("count") != len(current_khasra_ids):
        logger.info(
            f"Distance matrix invalid: count mismatch "
            f"(saved: {metadata.get('count')}, current: {len(current_khasra_ids)})"
        )
        return False

    # Quick checksum validation
    ids_string = ",".join(current_khasra_ids)
    current_checksum = hashlib.sha256(ids_string.encode()).hexdigest()

    if metadata.get("checksum") != current_checksum:
        logger.info("Distance matrix invalid: checksum mismatch")
        return False

    # Full ID comparison (safety check)
    saved_ids = metadata.get("khasra_ids", [])
    if saved_ids != current_khasra_ids:
        logger.info("Distance matrix invalid: khasra IDs don't match")
        return False

    logger.info("Distance matrix validation passed")
    return True


def format_cluster_labels(
    gdf: gpd.GeoDataFrame,
    cluster_id_col: str,
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

    prefix = "PARCEL_"
    # make list of ids [1, 2, 3, ...]
    df_length = len(ordered_cluster_labels_df)
    ids = np.arange(1, df_length + 1)
    # make list of ids with leading zeros ["001", "002", "003", ...]
    max_digits = len(str(df_length))
    formatted_ids = [prefix + str(id).zfill(max_digits) for id in ids]
    ordered_cluster_labels_df["formatted_ids"] = formatted_ids

    unclustered_label = "PARCEL_UNCLUSTERED"
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

    # Calculate usable areas (always from DB)
    stats_gdf = calculate_usable_areas(db, project_id)

    gdf = get_khasras_gdf(db, project_id, projected=True)

    # Use original geometries for clustering (with stats)
    original_gdf_with_stats = stats_gdf.copy()
    original_gdf_with_stats = original_gdf_with_stats.set_index("Khasra ID (Unique)")
    original_gdf_with_stats["geometry"] = gdf.set_index("Khasra ID (Unique)")[
        "geometry"
    ]
    original_gdf_with_stats = original_gdf_with_stats.reset_index()

    # Build or load distance matrix
    found_distance_matrix = False

    # Get ordered khasra IDs for validation
    current_khasra_ids = original_gdf_with_stats["Khasra ID (Unique)"].tolist()

    if project.distance_matrix_path:
        # Try to load existing matrix
        distance_matrix = file_storage.load_numpy_array(project.distance_matrix_path)

        # Try to load metadata
        metadata_path = project.distance_matrix_path.replace(
            "distance_matrix.npy", "distance_matrix_metadata.json"
        )
        metadata = file_storage.load_json(metadata_path)

        # Validate matrix and metadata
        if distance_matrix is not None and metadata is not None:
            if _validate_distance_matrix_metadata(metadata, current_khasra_ids):
                logger.info("Loaded existing distance matrix from storage")
                found_distance_matrix = True
            else:
                logger.info("Distance matrix validation failed, rebuilding")
                file_storage.delete_file(project.distance_matrix_path)
                if metadata_path:
                    file_storage.delete_file(metadata_path)
                project.distance_matrix_path = None
        elif distance_matrix is not None:
            # Backward compatibility: matrix exists but no metadata
            if distance_matrix.shape[0] == len(original_gdf_with_stats):
                logger.info("Loaded existing distance matrix (no metadata validation)")
                found_distance_matrix = True
            else:
                logger.info("Distance matrix shape mismatch, rebuilding")
                file_storage.delete_file(project.distance_matrix_path)
                project.distance_matrix_path = None

    if not found_distance_matrix:
        # Build new matrix
        distance_matrix = build_optimised_distance_matrix(
            gdf=original_gdf_with_stats,
            max_distance_considered=settings.MAX_DISTANCE_CONSIDERED,
            n_jobs=-1,
        )
        logger.info("Built new distance matrix")

        # Save matrix
        distance_matrix_path = file_storage.save_numpy_array(
            distance_matrix, project_id, "clustering/distance_matrix.npy"
        )

        # Save metadata
        metadata = _create_distance_matrix_metadata(current_khasra_ids)
        file_storage.save_json(
            metadata, project_id, "clustering/distance_matrix_metadata.json"
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

    # Filter out parcels with total area below minimum threshold (using raw cluster IDs)
    # Calculate total area per parcel (excluding -1 which is unclustered)
    parcel_areas = (
        gdf_with_cluster_id[gdf_with_cluster_id[cluster_id_col] != -1]
        .groupby(cluster_id_col)["Usable Area (ha)"]
        .sum()
    )

    # Identify parcels with area below minimum threshold
    small_parcels = parcel_areas[
        parcel_areas < request.min_parcel_area_ha
    ].index.tolist()

    # Mark khasras from small parcels as unclustered
    if small_parcels:
        logger.info(
            f"Dropping {len(small_parcels)} parcels with total area < {request.min_parcel_area_ha} hectares"
        )
        gdf_with_cluster_id.loc[
            gdf_with_cluster_id[cluster_id_col].isin(small_parcels), cluster_id_col
        ] = -1

    # Format cluster labels once, after filtering
    gdf_with_cluster_id = format_cluster_labels(
        gdf=gdf_with_cluster_id,
        cluster_id_col=cluster_id_col,
        area_col="Usable and Available Area (ha)",
    )

    # Update khasra records with parcel IDs
    for _, row in gdf_with_cluster_id.iterrows():
        khasra_unique_id = row.get("Khasra ID (Unique)")
        if khasra_unique_id:
            khasra = (
                db.query(KhasraModel)
                .filter(
                    KhasraModel.project_id == project_id,
                    KhasraModel.khasra_id_unique == khasra_unique_id,
                )
                .first()
            )
            if khasra:
                khasra.parcel_id = row[cluster_id_col]

    # Aggregate to parcel level
    parcel_gdf = aggregate_to_parcels(gdf_with_cluster_id, cluster_id_col)

    # Convert to WGS84 for database storage
    parcel_gdf_4326 = parcel_gdf.to_crs("EPSG:4326")

    # Calculate results summary
    clustered_count = len(
        gdf_with_cluster_id[
            ~gdf_with_cluster_id[cluster_id_col].str.contains("UNCLUSTERED")
        ]
    )
    unclustered_count = len(
        gdf_with_cluster_id[
            gdf_with_cluster_id[cluster_id_col].str.contains("UNCLUSTERED")
        ]
    )

    # Delete old clustering runs and parcels for this project
    db.query(ClusteringRunModel).filter(
        ClusteringRunModel.project_id == project_id
    ).delete()

    # Calculate total parcels excluding UNCLUSTERED
    total_parcels = len(
        parcel_gdf[~parcel_gdf[cluster_id_col].str.contains("UNCLUSTERED")]
    )

    # Create clustering run record
    clustering_run = ClusteringRunModel(
        project_id=project_id,
        distance_threshold=request.distance_threshold,
        min_samples=request.min_samples,
        max_distance_considered=settings.MAX_DISTANCE_CONSIDERED,
        min_parcel_area_ha=request.min_parcel_area_ha,
        total_parcels=total_parcels,
        clustered_khasras=clustered_count,
        unclustered_khasras=unclustered_count,
    )
    db.add(clustering_run)
    db.flush()  # Get the clustering_run.id

    # Identify layer-specific area columns for parcels
    standard_parcel_columns = {
        cluster_id_col,
        "Khasra Count",
        "Khasra ID (Unique)",
        "Original Area (ha)",
        "Usable Area (ha)",
        "Unusable Area (ha)",
        "Usable and Available Area (ha)",
        "Usable but Unavailable Area (ha)",
        "Unusable Area (%)",
        "Usable Area (%)",
        "Usable and Available Area (%)",
        "Usable but Unavailable Area (%)",
        "Building Count",
        "geometry",
    }
    layer_columns_in_parcels = [
        col
        for col in parcel_gdf_4326.columns
        if col not in standard_parcel_columns
        and pd.api.types.is_numeric_dtype(parcel_gdf_4326[col])
    ]

    # Store parcels in database
    for _, row in parcel_gdf_4326.iterrows():
        geom = ensure_multipolygon(row.geometry)

        # Extract layer-specific areas
        layer_areas_dict = {}
        for col in layer_columns_in_parcels:
            value = row.get(col)
            if pd.notna(value):
                layer_areas_dict[col] = round(float(value), 2)

        parcel = ParcelModel(
            clustering_run_id=clustering_run.id,
            project_id=project_id,
            parcel_id=row[cluster_id_col],
            geometry=from_shape(geom, srid=4326) if geom else None,
            khasra_count=int(row.get("Khasra Count", 0)),
            khasra_ids=row.get("Khasra ID (Unique)", ""),
            original_area_ha=round(row.get("Original Area (ha)", 0), 2),
            usable_area_ha=round(row.get("Usable Area (ha)", 0), 2),
            usable_available_area_ha=round(
                row.get("Usable and Available Area (ha)", 0), 2
            ),
            unusable_area_ha=round(row.get("Unusable Area (ha)", 0), 2),
            building_count=int(row.get("Building Count", 0))
            if "Building Count" in row
            else 0,
            layer_areas=layer_areas_dict if layer_areas_dict else None,
        )
        db.add(parcel)

    project.status = ProjectStatus.CLUSTERED
    project.updated_at = datetime.utcnow()
    db.commit()

    # Build response
    parcels = []
    for _, row in parcel_gdf.iterrows():
        parcel_stats = ParcelStats(
            parcel_id=row[cluster_id_col],
            khasra_count=int(row.get("Khasra Count", 0)),
            khasra_ids=row.get("Khasra ID (Unique)", "").split(", "),
            original_area_ha=round(row.get("Original Area (ha)", 0), 2),
            usable_area_ha=round(row.get("Usable Area (ha)", 0), 2),
            usable_area_percent=round(row.get("Usable Area (%)", 0), 2),
            usable_available_area_ha=round(
                row.get("Usable and Available Area (ha)", 0), 2
            ),
            usable_available_area_percent=round(
                row.get("Usable and Available Area (%)", 0), 2
            ),
            unusable_area_ha=round(row.get("Unusable Area (ha)", 0), 2),
            building_count=int(row.get("Building Count", 0))
            if "Building Count" in row
            else 0,
        )
        parcels.append(parcel_stats)

    return {
        "distance_threshold": request.distance_threshold,
        "min_samples": request.min_samples,
        "total_parcels": total_parcels,
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
        col
        for col in gdf.select_dtypes(include=[np.number]).columns
        if col not in exclude_cols and col != cluster_id_col
    ]

    agg_dict = {col: "sum" for col in numeric_cols}
    pivot_df = gdf.groupby(cluster_id_col).agg(agg_dict).round(2).reset_index()

    count_df = (
        gdf.groupby(cluster_id_col)
        .agg(
            Khasra_Count=("Khasra ID (Unique)", "size"),
            Khasra_IDs=("Khasra ID (Unique)", lambda x: ", ".join(list(x))),
        )
        .reset_index()
    )
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

    # Add convex hull geometry (repair invalid geometries before dissolve)
    gdf_for_dissolve = gdf.copy()
    gdf_for_dissolve.geometry = gdf_for_dissolve.geometry.apply(_repair_geometry)
    gdf_for_dissolve = gdf_for_dissolve[gdf_for_dissolve.geometry.notna()]
    convex_hull_geoms_gdf = (
        gdf_for_dissolve.dissolve(by=cluster_id_col)
        .convex_hull.to_frame(name="geometry")
        .reset_index()
    )
    pivot_df = pivot_df.merge(convex_hull_geoms_gdf, on=cluster_id_col)
    parcel_gdf = gpd.GeoDataFrame(pivot_df, geometry="geometry", crs=gdf.crs)

    return parcel_gdf


def get_parcels_gdf(
    db: Session, project_id: str
) -> Tuple[Optional[gpd.GeoDataFrame], Optional[Dict[str, Any]]]:
    """Load parcels GeoDataFrame from database, excluding UNCLUSTERED parcels.
    Returns (GeoDataFrame, clustering_params) tuple"""
    # Get the latest clustering run for this project
    clustering_run = (
        db.query(ClusteringRunModel)
        .filter(ClusteringRunModel.project_id == project_id)
        .order_by(ClusteringRunModel.created_at.desc())
        .first()
    )

    clustering_params = None
    if clustering_run:
        clustering_params = {
            "distance_threshold": clustering_run.distance_threshold,
            "min_samples": clustering_run.min_samples,
            "max_distance_considered": clustering_run.max_distance_considered,
            "min_parcel_area_ha": clustering_run.min_parcel_area_ha
            if clustering_run.min_parcel_area_ha is not None
            else 50.0,
            "total_parcels": clustering_run.total_parcels,
            "clustered_khasras": clustering_run.clustered_khasras,
            "unclustered_khasras": clustering_run.unclustered_khasras,
        }

    parcels = db.query(ParcelModel).filter(ParcelModel.project_id == project_id).all()

    if not parcels:
        return None, clustering_params

    data = []
    for p in parcels:
        # Skip UNCLUSTERED parcels
        if p.parcel_id and "UNCLUSTERED" in p.parcel_id:
            continue

        geom = to_shape(p.geometry) if p.geometry else None
        row = {
            "geometry": geom,
            "parcel_id": p.parcel_id,  # Use underscore for proper GeoJSON property name
            "khasra_count": p.khasra_count,
            "khasra_ids": p.khasra_ids,
            "original_area_ha": p.original_area_ha or 0,
            "usable_area_ha": p.usable_area_ha or 0,
            "unusable_area_ha": p.unusable_area_ha or 0,
            "usable_available_area_ha": p.usable_available_area_ha or 0,
            "building_count": p.building_count or 0,
        }
        # Add layer-specific areas
        if p.layer_areas:
            row.update(p.layer_areas)
        data.append(row)

    if not data:
        return None, clustering_params

    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
    # Filter out rows with no geometry
    gdf_filtered = gdf[gdf.geometry.notna()].copy()
    if len(gdf_filtered) == 0:
        return None, clustering_params
    return gdf_filtered, clustering_params
    return gdf_filtered


# ============ Export Functions ============


def export_data(
    db: Session,
    project_id: str,
    export_format: ExportFormat,
) -> Tuple[bytes, str]:
    """Export all project data in the specified format

    Always exports khasras (with stats), parcels, and all layers.
    All data is loaded from the database.
    """
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    location = project.location

    gdfs_to_export = {}

    # Always export khasras with stats
    gdf = get_khasras_with_stats_gdf(db, project_id)
    if gdf is not None:
        gdfs_to_export["khasras"] = gdf

    # Always export parcels if they exist
    gdf, _ = get_parcels_gdf(db, project_id)
    if gdf is not None:
        gdfs_to_export["parcels"] = gdf

    # Always export all layers
    layers = get_layers_metadata(db, project_id)
    for layer in layers:
        gdf = load_layer_gdf_by_id(db, layer.id)
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
        return export_to_geojson(gdfs_to_export, location)
    elif export_format == ExportFormat.KML:
        return export_to_kml(gdfs_to_export, location)
    elif export_format == ExportFormat.SHAPEFILE:
        return export_to_shapefile(gdfs_to_export, location)
    elif export_format == ExportFormat.PARQUET:
        return export_to_parquet(gdfs_to_export, location)
    elif export_format == ExportFormat.CSV:
        return export_to_csv(gdfs_to_export, location)
    elif export_format == ExportFormat.EXCEL:
        return export_to_excel(gdfs_to_export, location)
    else:
        raise ValueError(f"Unsupported export format: {export_format}")


def export_to_geojson(
    gdfs: Dict[str, gpd.GeoDataFrame], location: str
) -> Tuple[bytes, str]:
    if len(gdfs) == 1:
        name, gdf = list(gdfs.items())[0]
        filename = f"{location}_{name}.geojson"
        content = gdf.to_json()
        return content.encode("utf-8"), filename
    else:
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, gdf in gdfs.items():
                content = gdf.to_json()
                zf.writestr(f"{name}.geojson", content)

        filename = f"{location}_export.zip"
        return buffer.getvalue(), filename


def export_to_kml(
    gdfs: Dict[str, gpd.GeoDataFrame], location: str
) -> Tuple[bytes, str]:
    """Export data to KML/KMZ format.

    Creates a single KMZ file with all layers properly styled:
    - Khasras with default styling
    - Layers with color-coded styling
    - Parcels as boundaries with labels
    """
    kml = simplekml.Kml()

    # Define colors for different layer types (KML uses AABBGGRR format)
    # Alpha channel: b3 = 70% opacity (179/255)
    layer_colors = {
        "water": "b3e6d8ad",  # Light blue at 70% opacity
        "settlements": "b300008b",  # Dark red at 70% opacity
        "isolated buildings": "b300ffff",  # Yellow at 70% opacity
        "cropland": "b320a5da",  # Goldenrod at 70% opacity
        "slopes - north facing": "b3808080",  # Grey at 70% opacity
        "slopes - other facing": "b3d0d0d0",  # Light grey/white-ish at 70% opacity
        "slope": "b3808080",  # Grey (legacy fallback) at 70% opacity
    }

    # Process each GeoDataFrame
    for name, gdf in gdfs.items():
        # Create friendly folder names
        if "khasras" in name.lower():
            folder_name = "Khasras"
        else:
            folder_name = name.replace("_", " ").title()

        folder = kml.newfolder(name=folder_name)

        if "parcel" in name.lower():
            # Parcels: boundaries with labels
            for idx, row in gdf.iterrows():
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue

                parcel_name = str(row.get("parcel_id", f"Parcel {idx}"))

                try:
                    if geom.geom_type == "Polygon":
                        pol = folder.newpolygon(name=parcel_name)
                        pol.outerboundaryis = list(geom.exterior.coords)
                        pol.style.linestyle.color = (
                            "b3ffffff"  # White boundary at 70% opacity
                        )
                        pol.style.linestyle.width = 3
                        pol.style.polystyle.fill = 0  # No fill
                        # Disable polygon label (we'll use a point label instead)
                        pol.style.labelstyle.scale = 0
                    elif geom.geom_type == "MultiPolygon":
                        # Create a folder for this multipolygon
                        for poly_idx, poly in enumerate(geom.geoms):
                            pol = folder.newpolygon(name=f"{parcel_name}_{poly_idx}")
                            pol.outerboundaryis = list(poly.exterior.coords)
                            pol.style.linestyle.color = (
                                "b3ffffff"  # White boundary at 70% opacity
                            )
                            pol.style.linestyle.width = 3
                            pol.style.polystyle.fill = 0
                            # Disable polygon label (we'll use a point label instead)
                            pol.style.labelstyle.scale = 0
                    else:
                        continue

                    # Add comprehensive description with stats
                    desc = f"""
                    <b>Parcel ID:</b> {parcel_name}<br/>
                    <b>Khasra Count:</b> {row.get('khasra_count', 'N/A')}<br/>
                    <b>Original Area:</b> {row.get('original_area_ha', 0):.2f} ha<br/>
                    <b>Usable Area:</b> {row.get('usable_area_ha', 0):.2f} ha<br/>
                    <b>Usable & Available Area:</b> {row.get('usable_available_area_ha', 0):.2f} ha<br/>
                    <b>Unusable Area:</b> {row.get('unusable_area_ha', 0):.2f} ha<br/>
                    <b>Building Count:</b> {row.get('building_count', 0)}<br/>
                    """
                    pol.description = desc

                    # Add a point label at the centroid of the parcel
                    centroid = geom.centroid
                    usable_available_area = row.get('usable_available_area_ha', 0)
                    label_name = f"{parcel_name} ({usable_available_area:.2f} ha)"
                    pnt = folder.newpoint(
                        name=label_name, coords=[(centroid.x, centroid.y)]
                    )
                    pnt.style.labelstyle.scale = 1.2
                    pnt.style.labelstyle.color = simplekml.Color.white
                    pnt.style.iconstyle.scale = 0  # Hide the icon, show only the label
                    pnt.description = desc
                except Exception as e:
                    print(f"Error processing parcel {parcel_name}: {e}")
                    continue

        elif "layer" in name.lower():
            # Constraint layers with color coding
            layer_type = name.lower().replace("layer_", "")
            color = layer_colors.get(
                layer_type, "b3888888"
            )  # Default gray at 70% opacity

            for idx, row in gdf.iterrows():
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue

                feature_name = f"{layer_type.title()} {idx + 1}"

                try:
                    if geom.geom_type == "Polygon":
                        pol = folder.newpolygon(name=feature_name)
                        pol.outerboundaryis = list(geom.exterior.coords)
                        pol.style.polystyle.color = color
                        pol.style.polystyle.fill = 1
                        pol.style.linestyle.color = color
                        pol.style.linestyle.width = 1
                    elif geom.geom_type == "MultiPolygon":
                        for poly_idx, poly in enumerate(geom.geoms):
                            pol = folder.newpolygon(name=f"{feature_name}_{poly_idx}")
                            pol.outerboundaryis = list(poly.exterior.coords)
                            pol.style.polystyle.color = color
                            pol.style.polystyle.fill = 1
                            pol.style.linestyle.color = color
                            pol.style.linestyle.width = 1
                    else:
                        continue
                except Exception as e:
                    print(f"Error processing layer feature {feature_name}: {e}")
                    continue

        else:
            # Khasras with default styling
            for idx, row in gdf.iterrows():
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue

                khasra_name = str(
                    row.get("khasra_id_unique", row.get("khasra_id", f"Khasra {idx}"))
                )

                try:
                    if geom.geom_type == "Polygon":
                        pol = folder.newpolygon(name=khasra_name)
                        pol.outerboundaryis = list(geom.exterior.coords)
                        pol.style.polystyle.color = "b300ff00"  # Green at 70% opacity
                        pol.style.polystyle.fill = 1
                        pol.style.linestyle.color = (
                            "b300ff00"  # Green outline at 70% opacity
                        )
                        pol.style.linestyle.width = 1
                    elif geom.geom_type == "MultiPolygon":
                        for poly_idx, poly in enumerate(geom.geoms):
                            pol = folder.newpolygon(name=f"{khasra_name}_{poly_idx}")
                            pol.outerboundaryis = list(poly.exterior.coords)
                            pol.style.polystyle.color = (
                                "b300ff00"  # Green at 70% opacity
                            )
                            pol.style.polystyle.fill = 1
                            pol.style.linestyle.color = (
                                "b300ff00"  # Green outline at 70% opacity
                            )
                            pol.style.linestyle.width = 1
                    else:
                        continue

                    # Add comprehensive description with stats
                    desc_parts = [f"<b>Khasra ID:</b> {khasra_name}<br/>"]
                    if "original_area_ha" in row:
                        desc_parts.append(
                            f"<b>Original Area:</b> {row.get('original_area_ha', 0):.2f} ha<br/>"
                        )
                    if "usable_area_ha" in row:
                        desc_parts.append(
                            f"<b>Usable Area:</b> {row.get('usable_area_ha', 0):.2f} ha ({row.get('usable_area_percent', 0):.1f}%)<br/>"
                        )
                    if "usable_available_area_ha" in row:
                        desc_parts.append(
                            f"<b>Usable & Available:</b> {row.get('usable_available_area_ha', 0):.2f} ha ({row.get('usable_available_area_percent', 0):.1f}%)<br/>"
                        )
                    if "unusable_area_ha" in row:
                        desc_parts.append(
                            f"<b>Unusable Area:</b> {row.get('unusable_area_ha', 0):.2f} ha ({row.get('unusable_area_percent', 0):.1f}%)<br/>"
                        )
                    if "parcel_id" in row:
                        desc_parts.append(
                            f"<b>Parcel ID:</b> {row.get('parcel_id', 'N/A')}<br/>"
                        )

                    pol.description = "".join(desc_parts)
                except Exception as e:
                    print(f"Error processing khasra {khasra_name}: {e}")
                    continue

    # Save to KMZ (compressed KML in a ZIP file)
    buffer = BytesIO()
    kml_string = kml.kml()

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as kmz:
        kmz.writestr("doc.kml", kml_string)

    buffer.seek(0)
    filename = f"{location}_export.kmz"
    return buffer.getvalue(), filename


def export_to_shapefile(
    gdfs: Dict[str, gpd.GeoDataFrame], location: str
) -> Tuple[bytes, str]:
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

    filename = f"{location}_export_shp.zip"
    return buffer.getvalue(), filename


def export_to_parquet(
    gdfs: Dict[str, gpd.GeoDataFrame], location: str
) -> Tuple[bytes, str]:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, gdf in gdfs.items():
            parquet_buffer = BytesIO()
            gdf.to_parquet(parquet_buffer)
            zf.writestr(f"{name}.parquet", parquet_buffer.getvalue())

    filename = f"{location}_export.zip"
    return buffer.getvalue(), filename


def export_to_csv(
    gdfs: Dict[str, gpd.GeoDataFrame], location: str
) -> Tuple[bytes, str]:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, gdf in gdfs.items():
            df = gdf.drop(columns=["geometry"], errors="ignore")
            csv_content = df.to_csv(index=False)
            zf.writestr(f"{name}.csv", csv_content)

    filename = f"{location}_export_csv.zip"
    return buffer.getvalue(), filename


def export_to_excel(
    gdfs: Dict[str, gpd.GeoDataFrame],
    location: str,
) -> Tuple[bytes, str]:
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        # Sheet 1: Parcels (exclude UNCLUSTERED parcels)
        if "parcels" in gdfs:
            parcels_df = gdfs["parcels"].drop(columns=["geometry"], errors="ignore")
            parcels_df = parcels_df.sort_values(by="parcel_id")

            # rename columns to be more user-friendly in Excel
            parcels_df.rename(
                columns={
                    "parcel_id": "Parcel ID",
                    "khasra_count": "Khasra Count",
                    "khasra_ids": "Khasra IDs",
                    "original_area_ha": "Original Area (ha)",
                    "usable_area_ha": "Usable Area (ha)",
                    "usable_available_area_ha": "Usable and Available Area (ha)",
                    "unusable_area_ha": "Unusable Area (ha)",
                    "building_count": "Building Count",
                },
                inplace=True,
            )

            # Round all numeric columns to 2 decimal places
            numeric_columns = parcels_df.select_dtypes(include=["number"]).columns
            parcels_df[numeric_columns] = parcels_df[numeric_columns].round(2)

            parcels_df.to_excel(writer, sheet_name="Parcels", index=False)

        # Sheet 2: Khasras that are part of clustered parcels only
        if "khasras" in gdfs:
            khasras_df = gdfs["khasras"].drop(columns=["geometry"], errors="ignore")

            # Filter to only khasras that are part of clustered parcels (not UNCLUSTERED)
            if "Parcel ID" in khasras_df.columns:
                khasras_df = khasras_df[
                    ~khasras_df["Parcel ID"].str.contains("UNCLUSTERED", na=True)
                ]
                khasras_df = khasras_df.sort_values(
                    by=["Parcel ID", "Khasra ID (Unique)"]
                )

            # Round all numeric columns to 2 decimal places
            numeric_columns = khasras_df.select_dtypes(include=["number"]).columns
            khasras_df[numeric_columns] = khasras_df[numeric_columns].round(2)

        khasras_df.to_excel(writer, sheet_name="Khasras", index=False)
        # Auto-fit column widths with a maximum limit and format headers
        header_fill = PatternFill(
            start_color="1F4E78", end_color="1F4E78", fill_type="solid"
        )
        header_font = Font(color="FFFFFF", bold=True)
        header_alignment = Alignment(horizontal="left", vertical="center")

        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                column_width = 0
                column_letter = column[0].column_letter

                for cell in column:
                    try:
                        if cell.value:
                            column_width = max(column_width, len(str(cell.value)))
                    except Exception:
                        pass

                # Set width with padding, max 50 characters
                adjusted_width = min(column_width, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

            # Format header row (row 1)
            for cell in worksheet[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = header_alignment

            # Double the header row height
            worksheet.row_dimensions[1].height = 20

    filename = f"{location}_export.xlsx"
    return buffer.getvalue(), filename


# ============ Background Processing Functions ============


def process_settlement_layer_background(
    project_id: str,
    building_buffer: int = 10,
    settlement_eps: int = 50,
    min_buildings: int = 5,
):
    """Background task to process settlement layers"""
    from database import SessionLocal

    db = SessionLocal()
    try:
        process_settlement_layer(
            db=db,
            project_id=project_id,
            building_buffer=building_buffer,
            settlement_eps=settlement_eps,
            min_buildings=min_buildings,
            create_only=False,
        )
        # Automatically recalculate areas after layer is added
        recalculate_areas_and_parcels(db, project_id)
    except Exception as e:
        print(f"Error in background settlement layer processing: {e}")
        # The function already marks layers as failed in the database
    finally:
        db.close()


def process_cropland_layer_background(project_id: str):
    """Background task to process cropland layer"""
    from database import SessionLocal

    db = SessionLocal()
    try:
        process_cropland_layer(
            db=db,
            project_id=project_id,
            create_only=False,
        )
        # Automatically recalculate areas after layer is added
        recalculate_areas_and_parcels(db, project_id)
    except Exception as e:
        print(f"Error in background cropland layer processing: {e}")
        # The function already marks layer as failed in the database
    finally:
        db.close()


def process_water_layer_background(project_id: str):
    """Background task to process water layer"""
    from database import SessionLocal

    db = SessionLocal()
    try:
        process_water_layer(
            db=db,
            project_id=project_id,
            create_only=False,
        )
        # Automatically recalculate areas after layer is added
        recalculate_areas_and_parcels(db, project_id)
    except Exception as e:
        print(f"Error in background water layer processing: {e}")
        # The function already marks layer as failed in the database
    finally:
        db.close()


def process_slopes_layer_background(
    project_id: str,
    include_north_slopes: bool = True,
    include_other_slopes: bool = True,
    north_min_angle: float = 7.0,
    other_min_angle: float = 10.0,
):
    """Background task to process slopes layer"""
    from database import SessionLocal

    db = SessionLocal()
    try:
        process_slopes_layer(
            db=db,
            project_id=project_id,
            include_north_slopes=include_north_slopes,
            include_other_slopes=include_other_slopes,
            north_min_angle=north_min_angle,
            other_min_angle=other_min_angle,
            create_only=False,
        )
        # Automatically recalculate areas after layer is added
        recalculate_areas_and_parcels(db, project_id)
    except Exception as e:
        print(f"Error in background slopes layer processing: {e}")
        # The function already marks layer as failed in the database
    finally:
        db.close()


def process_custom_layer_background(
    file_content: bytes,
    filename: str,
    project_id: str,
    layer_name: str,
    is_unusable: bool = True,
):
    """Background task to process custom layer"""
    from database import SessionLocal

    db = SessionLocal()
    try:
        process_custom_layer_upload(
            db=db,
            file_content=file_content,
            filename=filename,
            project_id=project_id,
            layer_name=layer_name,
            is_unusable=is_unusable,
        )
        # Automatically recalculate areas after layer is added
        recalculate_areas_and_parcels(db, project_id)
    except Exception as e:
        print(f"Error in background custom layer processing: {e}")
        # The function already marks layer as failed in the database
    finally:
        db.close()
