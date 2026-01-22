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
from config import AVAILABLE_LAYERS, settings
from database import (
    KhasraModel,
    LayerFeatureModel,
    LayerModel,
    ParcelModel,
    ProjectModel,
    get_db,
)
from geoalchemy2.shape import from_shape, to_shape
from joblib import Parallel, delayed
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
from shapely import MultiPolygon
from shapely.geometry import Polygon, mapping, shape
from shapely.strtree import STRtree
from sklearn.cluster import DBSCAN
from sqlalchemy.orm import Session
from storage import file_storage

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

    # Store in database (primary storage)
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
        "crs": "EPSG:4326",
    }


def get_khasras_gdf(db: Session, project_id: str, projected: bool = False) -> Optional[gpd.GeoDataFrame]:
    """Load khasras GeoDataFrame from database
    
    Args:
        db: Database session
        project_id: Project ID
        projected: If True, return GDF in India projected CRS (EPSG:24378) for area calculations
    """
    # Query khasras from database
    khasras = db.query(KhasraModel).filter(KhasraModel.project_id == project_id).all()
    
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


def get_khasras_with_stats_gdf(db: Session, project_id: str) -> Optional[gpd.GeoDataFrame]:
    """Load khasras GeoDataFrame with all calculated stats from database"""
    khasras = db.query(KhasraModel).filter(KhasraModel.project_id == project_id).all()
    
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
        }
        # Calculate percentages
        orig = row["Original Area (ha)"]
        if orig > 0:
            row["Usable Area (%)"] = round(row["Usable Area (ha)"] / orig * 100, 2)
            row["Unusable Area (%)"] = round(row["Unusable Area (ha)"] / orig * 100, 2)
            row["Usable and Available Area (%)"] = round(row["Usable and Available Area (ha)"] / orig * 100, 2)
        else:
            row["Usable Area (%)"] = 0
            row["Unusable Area (%)"] = 0
            row["Usable and Available Area (%)"] = 0
        
        if k.properties:
            row.update(k.properties)
        data.append(row)
    
    return gpd.GeoDataFrame(data, crs="EPSG:4326")


def get_parcels_gdf(db: Session, project_id: str) -> Optional[gpd.GeoDataFrame]:
    """Load parcels GeoDataFrame from database"""
    parcels = db.query(ParcelModel).filter(ParcelModel.project_id == project_id).all()
    
    if not parcels:
        return None
    
    data = []
    for p in parcels:
        geom = to_shape(p.geometry) if p.geometry else None
        row = {
            "geometry": geom,
            "Parcel ID": p.parcel_id,
            "Khasra Count": p.khasra_count,
            "Khasra IDs": p.khasra_ids,
            "Original Area (ha)": p.original_area_ha or 0,
            "Usable Area (ha)": p.usable_area_ha or 0,
            "Unusable Area (ha)": p.unusable_area_ha or 0,
            "Usable and Available Area (ha)": p.usable_available_area_ha or 0,
            "Building Count": p.building_count or 0,
        }
        # Add layer-specific areas
        if p.layer_areas:
            row.update(p.layer_areas)
        data.append(row)
    
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
    # Filter out rows with no geometry
    gdf = gdf[gdf.geometry.notna()]
    return gdf if len(gdf) > 0 else None


# ============ Layer Processing ============

def update_layer_status(db: Session, layer: LayerModel, status: str, details: str):
    """Update the status and details of a layer"""
    layer.status = status
    layer.details = details
    layer.updated_at = datetime.utcnow()
    db.commit()
    print(f"[LAYER STATUS] Layer '{layer.name}' {status.upper()}: {details}")


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

    # Create layer record immediately with in_progress status
    layer = LayerModel(
        project_id=project_id,
        name=layer_name,
        layer_type=LayerType.CUSTOM.value,
        is_unusable=is_unusable,
        status="in_progress",
        details="Initializing layer processing...",
        parameters={},
    )
    db.add(layer)
    db.commit()
    
    try:
        update_layer_status(db, layer, "in_progress", "Loading khasras data...")
        
        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")
        
        # Ensure khasras are projected to India CRS for intersection
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

        update_layer_status(db, layer, "in_progress", "Reading uploaded file...")
        
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

        update_layer_status(db, layer, "in_progress", "Projecting layer to India CRS...")
        
        # Ensure CRS and project
        if layer_gdf.crs is None:
            layer_gdf = layer_gdf.set_crs("EPSG:4326")
        layer_gdf = layer_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")

        update_layer_status(db, layer, "in_progress", "Intersecting layer with khasras...")
        
        # Intersect with khasras
        layer_overlap_gdf = gpd.overlay(layer_gdf, gdf, how="intersection")
        layer_overlap_gdf = layer_overlap_gdf.dissolve(by="Khasra ID (Unique)").reset_index()

        update_layer_status(db, layer, "in_progress", "Calculating area statistics...")
        
        # Calculate area
        area_col = f"{'Unusable' if is_unusable else 'Unavailable'} Area - {layer_name} (ha)"
        layer_overlap_gdf[area_col] = layer_overlap_gdf.area / 10_000

        update_layer_status(db, layer, "in_progress", "Storing layer features in database...")
        
        # Update layer metadata
        layer.feature_count = len(layer_overlap_gdf)
        layer.total_area_ha = round(layer_overlap_gdf[area_col].sum(), 2)
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
        layer.details = f"Error: {str(e)}"
        db.commit()
        raise


def get_project_layers(db: Session, project_id: str) -> List[LayerModel]:
    """Get all layers for a project"""
    return db.query(LayerModel).filter(LayerModel.project_id == project_id).all()


def load_layer_gdf_by_id(db: Session, layer_id: int) -> Optional[gpd.GeoDataFrame]:
    """Load a layer's GeoDataFrame from database by layer ID"""
    features = db.query(LayerFeatureModel).filter(
        LayerFeatureModel.layer_id == layer_id
    ).all()
    
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


def load_layer_gdf(db: Session, project_id: str, layer_name: str) -> Optional[gpd.GeoDataFrame]:
    """Load a layer's GeoDataFrame from database (layer_features table)"""
    layer = db.query(LayerModel).filter(
        LayerModel.project_id == project_id,
        LayerModel.name == layer_name
    ).first()
    
    if not layer:
        return None
    
    return load_layer_gdf_by_id(db, layer.id)


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
    
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
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


# ============ Builtin Layer Processing ============

def process_settlement_layer(
    db: Session,
    project_id: str,
    building_buffer: int = 10,
    settlement_eps: int = 50,
    min_buildings: int = 5,
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
    
    Returns:
        Tuple of (settlements_layer_info, isolated_buildings_layer_info)
    """
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    # Create layer records immediately with in_progress status
    settlements_layer = LayerModel(
        project_id=project_id,
        name="Settlements",
        layer_type=LayerType.BUILTIN.value,
        is_unusable=True,
        status="in_progress",
        details="Initializing settlement detection...",
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
        layer_type=LayerType.BUILTIN.value,
        is_unusable=False,
        status="in_progress",
        details="Waiting for settlement detection...",
        parameters={
            "building_buffer": building_buffer,
        },
    )
    db.add(isolated_layer)
    db.commit()

    try:
        update_layer_status(db, settlements_layer, "in_progress", "Loading khasras data...")
        
        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is None:
            raise ValueError("Khasras must be uploaded first")
        
        # Project khasras
        gdf = gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
        gdf_4326 = gdf.to_crs("EPSG:4326")

        update_layer_status(db, settlements_layer, "in_progress", "Importing VIDA rooftop utilities...")
        
        # Import VIDA utilities
        try:
            from gridsample.utils_rooftop import (
                download_VIDA_rooftops_data_by_s2,
                get_overlapping_s2_cell_ids,
            )
        except ImportError:
            raise ValueError("gridsample package not installed. Install it with: pip install -e . in gridsample directory")

        update_layer_status(db, settlements_layer, "in_progress", "Finding overlapping S2 cells...")
        
        # Get S2 cell IDs that overlap the khasras
        s2_cell_ids = get_overlapping_s2_cell_ids(gdf_4326)
        
        if not s2_cell_ids:
            raise ValueError("No S2 cells found overlapping the khasras")

        update_layer_status(db, settlements_layer, "in_progress", f"Downloading rooftop data for {len(s2_cell_ids)} S2 cells...")
        
        # Download rooftop data to shared folder (not per-project)
        shared_rooftop_dir = settings.DATA_DIR / "shared_vida_s2_rooftop_data"
        shared_rooftop_dir.mkdir(parents=True, exist_ok=True)
        
        download_VIDA_rooftops_data_by_s2(
            s2_cell_ids=s2_cell_ids,
            country_iso_code="IND",
            target_data_dir=shared_rooftop_dir,
        )

        update_layer_status(db, settlements_layer, "in_progress", "Loading and combining rooftop data...")
        
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

        update_layer_status(db, settlements_layer, "in_progress", f"Filtering {len(rooftop_gdf)} buildings to khasras...")
        
        # Filter to only rooftops that intersect khasras
        rooftops_in_khasras = rooftop_gdf.sjoin(gdf, how="inner", predicate="intersects")
        rooftops_in_khasras = rooftops_in_khasras.drop(columns=["index_right"], errors="ignore")
        
        # Keep only geometry and necessary columns
        keep_cols = ["geometry"]
        print(rooftops_in_khasras.columns)
        if "khasra_id_unique" in rooftops_in_khasras.columns:
            keep_cols.append("khasra_id_unique")
        rooftops_in_khasras = rooftops_in_khasras[keep_cols]

        if len(rooftops_in_khasras) == 0:
            raise ValueError("No buildings found within khasras")

        update_layer_status(db, settlements_layer, "in_progress", f"Buffering {len(rooftops_in_khasras)} buildings by {building_buffer}m...")
        
        # Buffer buildings
        buffered_buildings = rooftops_in_khasras.copy()
        buffered_buildings["geometry"] = buffered_buildings.buffer(building_buffer)

        update_layer_status(db, settlements_layer, "in_progress", "Intersecting buildings with khasras...")
        
        # Get intersection with khasras
        buildings_overlap_gdf = gpd.overlay(buffered_buildings, gdf, how="intersection")

        update_layer_status(db, settlements_layer, "in_progress", f"Clustering {len(buildings_overlap_gdf)} buildings (eps={settlement_eps}m, min={min_buildings})...")
        
        # Cluster buildings using DBSCAN
        building_centroids = buildings_overlap_gdf.geometry.centroid
        X = np.array(list(zip(building_centroids.x, building_centroids.y)))
        
        clusterer = DBSCAN(eps=settlement_eps, min_samples=min_buildings, n_jobs=-1)
        building_cluster_ids = clusterer.fit_predict(X)
        buildings_overlap_gdf["settlement_id"] = building_cluster_ids

        # Separate settlement buildings from isolated buildings
        settlement_buildings_gdf = buildings_overlap_gdf[buildings_overlap_gdf["settlement_id"] != -1].copy()
        isolated_buildings_gdf = buildings_overlap_gdf[buildings_overlap_gdf["settlement_id"] == -1].copy()
        
        num_settlements = len(settlement_buildings_gdf["settlement_id"].unique()) if len(settlement_buildings_gdf) > 0 else 0
        update_layer_status(db, settlements_layer, "in_progress", f"Found {num_settlements} settlements, {len(isolated_buildings_gdf)} isolated buildings...")

        results = []

        # Process settlements (convex hull of clustered buildings)
        if len(settlement_buildings_gdf) > 0:
            update_layer_status(db, settlements_layer, "in_progress", "Creating settlement convex hulls...")
            
            settlements_gdf = settlement_buildings_gdf.dissolve(by="settlement_id").reset_index()
            settlements_gdf = settlements_gdf[["geometry", "settlement_id"]]
            settlements_gdf["geometry"] = settlements_gdf.convex_hull
            
            # Intersect with khasras
            settlements_overlap_gdf = gpd.overlay(settlements_gdf, gdf, how="intersection")
            settlements_overlap_gdf = settlements_overlap_gdf.dissolve(by="Khasra ID (Unique)").reset_index()
            
            area_col = "Unusable Area - Settlements (ha)"
            settlements_overlap_gdf[area_col] = settlements_overlap_gdf.area / 10_000
            
            update_layer_status(db, settlements_layer, "in_progress", "Saving settlement layer to database...")
            
            # Save settlements layer
            settlements_info = _save_builtin_layer_with_status(
                db=db,
                layer=settlements_layer,
                project_id=project_id,
                layer_gdf=settlements_overlap_gdf,
                area_col=area_col,
            )
            results.append(settlements_info)
        else:
            settlements_layer.status = "successful"
            settlements_layer.details = "No settlements found (no building clusters meeting criteria)"
            settlements_layer.feature_count = 0
            settlements_layer.total_area_ha = 0.0
            db.commit()
            
            results.append(LayerInfo(
                layer_type=LayerType.BUILTIN.value,
                name="Settlements",
                description="No settlements found",
                is_unusable=True,
                parameters={},
                area_ha=0.0,
                feature_count=0,
                status="successful",
                details="No settlements found (no building clusters meeting criteria)",
            ))

        # Process isolated buildings
        update_layer_status(db, isolated_layer, "in_progress", "Processing isolated buildings...")
        
        if len(isolated_buildings_gdf) > 0:
            isolated_overlap_gdf = isolated_buildings_gdf.dissolve(by="Khasra ID (Unique)").reset_index()
            
            area_col = "Unavailable Area - Isolated Buildings (ha)"
            isolated_overlap_gdf[area_col] = isolated_overlap_gdf.area / 10_000
            
            update_layer_status(db, isolated_layer, "in_progress", "Saving isolated buildings layer to database...")
            
            # Save isolated buildings layer
            isolated_info = _save_builtin_layer_with_status(
                db=db,
                layer=isolated_layer,
                project_id=project_id,
                layer_gdf=isolated_overlap_gdf,
                area_col=area_col,
            )
            results.append(isolated_info)
        else:
            isolated_layer.status = "successful"
            isolated_layer.details = "No isolated buildings found"
            isolated_layer.feature_count = 0
            isolated_layer.total_area_ha = 0.0
            db.commit()
            
            results.append(LayerInfo(
                layer_type=LayerType.BUILTIN.value,
                name="Isolated Buildings",
                description="No isolated buildings found",
                is_unusable=False,
                parameters={},
                area_ha=0.0,
                feature_count=0,
                status="successful",
                details="No isolated buildings found",
            ))

        # Update project status
        project.status = ProjectStatus.LAYERS_ADDED
        project.updated_at = datetime.utcnow()
        db.commit()

        return tuple(results)
        
    except Exception as e:
        # Mark both layers as failed
        settlements_layer.status = "failed"
        settlements_layer.details = f"Error: {str(e)}"
        isolated_layer.status = "failed"
        isolated_layer.details = f"Error: {str(e)}"
        db.commit()
        raise


def _save_builtin_layer_with_status(
    db: Session,
    layer: LayerModel,
    project_id: str,
    layer_gdf: gpd.GeoDataFrame,
    area_col: str,
) -> LayerInfo:
    """Helper function to save a builtin layer to database with status tracking"""
    
    # Update layer metadata
    layer.feature_count = len(layer_gdf)
    layer.total_area_ha = round(layer_gdf[area_col].sum(), 2)
    db.flush()

    # Store per-khasra layer features in database
    layer_4326 = layer_gdf.to_crs("EPSG:4326")
    for idx, row in layer_4326.iterrows():
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
    layer.details = f"Layer processed successfully. {layer.feature_count} features, {layer.total_area_ha} ha total area."
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


def _save_builtin_layer(
    db: Session,
    project_id: str,
    layer_name: str,
    layer_gdf: gpd.GeoDataFrame,
    area_col: str,
    is_unusable: bool,
    layer_type: str,
    parameters: Dict,
) -> LayerInfo:
    """Helper function to save a builtin layer to database"""
    
    # Store layer metadata in database
    layer = LayerModel(
        project_id=project_id,
        name=layer_name,
        layer_type=layer_type,
        is_unusable=is_unusable,
        feature_count=len(layer_gdf),
        total_area_ha=round(layer_gdf[area_col].sum(), 2),
        parameters=parameters,
        status="successful",
    )
    db.add(layer)
    db.flush()

    # Store per-khasra layer features in database
    layer_4326 = layer_gdf.to_crs("EPSG:4326")
    for idx, row in layer_4326.iterrows():
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
            khasra_id_unique=row.get("Khasra ID (Unique)", ""),
            geometry=from_shape(geom, srid=4326),
            area_ha=round(row[area_col], 4),
            properties={
                "layer_name": layer_name,
                "is_unusable": is_unusable,
            },
        )
        db.add(feature)

    return LayerInfo(
        layer_type=layer_type,
        name=layer_name,
        description=f"Builtin layer: {layer_name}",
        is_unusable=is_unusable,
        parameters=parameters,
        area_ha=round(layer_gdf[area_col].sum(), 2),
        feature_count=len(layer_gdf),
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
    layers = [layer_model for layer_model in get_project_layers(db, project_id) if layer_model.status == "successful"]

    # Apply unusable layers (cut out from geometry)
    for layer in layers:
        if layer.is_unusable:
            layer_gdf = load_layer_gdf_by_id(db, layer.id)
            if layer_gdf is not None and len(layer_gdf) > 0:
                layer_gdf = layer_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
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
            layer_gdf = load_layer_gdf_by_id(db, layer.id)
            if layer_gdf is not None and len(layer_gdf) > 0:
                layer_gdf = layer_gdf.to_crs(f"EPSG:{settings.INDIA_PROJECTED_CRS}")
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

    # Add layer-specific area columns from layer_features
    for layer in layers:
        if layer.parameters:
            area_col = layer.parameters.get("area_col")
            if area_col:
                # Get per-khasra areas from layer_features
                features = db.query(LayerFeatureModel).filter(
                    LayerFeatureModel.layer_id == layer.id
                ).all()
                
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

    # Update khasra records in database with calculated areas
    for _, row in available_gdf.iterrows():
        khasra_unique_id = row.get("Khasra ID (Unique)")
        if khasra_unique_id:
            khasra = db.query(KhasraModel).filter(
                KhasraModel.project_id == project_id,
                KhasraModel.khasra_id_unique == khasra_unique_id
            ).first()
            if khasra:
                khasra.original_area_ha = round(row.get("Original Area (ha)", 0), 4)
                khasra.usable_area_ha = round(row.get("Usable Area (ha)", 0), 4)
                khasra.unusable_area_ha = round(row.get("Unusable Area (ha)", 0), 4)
                khasra.usable_available_area_ha = round(row.get("Usable and Available Area (ha)", 0), 4)
    
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

    # Calculate usable areas (always from DB)
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

    # Update khasra records with parcel IDs
    for _, row in gdf_with_cluster_id.iterrows():
        khasra_unique_id = row.get("Khasra ID (Unique)")
        if khasra_unique_id:
            khasra = db.query(KhasraModel).filter(
                KhasraModel.project_id == project_id,
                KhasraModel.khasra_id_unique == khasra_unique_id
            ).first()
            if khasra:
                khasra.parcel_id = row[cluster_id_col]

    # Aggregate to parcel level
    parcel_gdf = aggregate_to_parcels(gdf_with_cluster_id, cluster_id_col)

    # Convert to WGS84 for database storage
    parcel_gdf_4326 = parcel_gdf.to_crs("EPSG:4326")

    # Store parcels in database
    db.query(ParcelModel).filter(ParcelModel.project_id == project_id).delete()
    
    for _, row in parcel_gdf_4326.iterrows():
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
    """Export project data in the specified format
    
    All data is loaded from the database.
    """
    project = get_project(db, project_id)
    if not project:
        raise ValueError(f"Project {project_id} not found")

    location = project.location
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    gdfs_to_export = {}
    
    if export_type in [ExportType.KHASRAS, ExportType.ALL]:
        gdf = get_khasras_gdf(db, project_id, projected=False)
        if gdf is not None:
            gdfs_to_export["khasras"] = gdf

    if export_type in [ExportType.KHASRAS_WITH_STATS, ExportType.ALL]:
        # Get khasras with calculated stats from DB
        gdf = get_khasras_with_stats_gdf(db, project_id)
        if gdf is not None:
            gdfs_to_export["khasras_with_stats"] = gdf

    if export_type in [ExportType.PARCELS, ExportType.ALL]:
        gdf = get_parcels_gdf(db, project_id)
        if gdf is not None:
            gdfs_to_export["parcels"] = gdf

    if export_type in [ExportType.LAYERS, ExportType.ALL]:
        layers = get_project_layers(db, project_id)
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
