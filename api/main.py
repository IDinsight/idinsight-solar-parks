"""
Solar Parks Analysis API - FastAPI Application

A FastAPI application for processing and analyzing solar park land parcels (khasras).
Provides endpoints for uploading khasra shapes, adding constraint layers, clustering,
and exporting results.
"""
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from io import BytesIO

from auth import (
    authenticate_user,
    create_access_token,
    fake_users_db,
    get_current_active_user,
)
from config import AVAILABLE_LAYERS, settings
from models import (
    AvailableLayersResponse,
    ClusteringRequest,
    ClusteringResponse,
    ErrorResponse,
    ExportFormat,
    ExportRequest,
    ExportResponse,
    ExportType,
    HealthCheckResponse,
    KhasraUploadResponse,
    LayerAddRequest,
    LayerInfo,
    LayerUploadResponse,
    ProjectCreate,
    ProjectListResponse,
    ProjectResponse,
    ProjectStatus,
    Token,
    User,
)
from services import (
    calculate_usable_areas,
    cluster_khasras,
    create_project,
    delete_project,
    export_data,
    get_project,
    list_projects,
    process_custom_layer_upload,
    process_khasra_upload,
)


# ============ App Initialization ============

app = FastAPI(
    title=settings.APP_NAME,
    description="""
## Solar Parks Analysis API

This API provides tools for analyzing land parcels (khasras) for solar park development.

### Features:
- **Upload khasra boundaries** from KML or GeoJSON files
- **Add constraint layers** (buildings, settlements, water, slopes, etc.)
- **Cluster khasras** into contiguous parcels
- **Calculate usable areas** after removing constraints
- **Export results** in multiple formats (GeoJSON, KML, Shapefile, Excel, etc.)

### Authentication:
All endpoints (except `/health` and `/auth/token`) require JWT authentication.
Use the `/auth/token` endpoint to obtain an access token.
    """,
    version=settings.APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ Health Check ============

@app.get(
    "/health",
    response_model=HealthCheckResponse,
    tags=["Health"],
    summary="Health check endpoint",
)
async def health_check():
    """Check if the API is running and healthy."""
    return HealthCheckResponse(
        status="healthy",
        version=settings.APP_VERSION,
        timestamp=datetime.utcnow(),
    )


# ============ Authentication Endpoints ============

@app.post(
    "/auth/token",
    response_model=Token,
    tags=["Authentication"],
    summary="Get access token",
)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Authenticate with username and password to receive a JWT access token.
    
    Default credentials:
    - Username: `admin`
    - Password: `solarparks2024`
    """
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")


@app.get(
    "/auth/me",
    response_model=User,
    tags=["Authentication"],
    summary="Get current user",
)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """Get information about the currently authenticated user."""
    return current_user


# ============ Project Endpoints ============

@app.post(
    "/projects",
    response_model=ProjectResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Projects"],
    summary="Create a new project",
)
async def create_new_project(
    project: ProjectCreate,
    current_user: User = Depends(get_current_active_user),
):
    """
    Create a new solar park analysis project.
    
    Each project can have its own khasra boundaries, layers, and clustering configuration.
    """
    project_id = create_project(
        name=project.name,
        location=project.location,
        description=project.description,
    )
    project_data = get_project(project_id)
    
    return ProjectResponse(
        id=project_data["id"],
        name=project_data["name"],
        location=project_data["location"],
        description=project_data["description"],
        status=project_data["status"],
        created_at=project_data["created_at"],
        updated_at=project_data["updated_at"],
        layers_added=project_data["layers_added"],
    )


@app.get(
    "/projects",
    response_model=ProjectListResponse,
    tags=["Projects"],
    summary="List all projects",
)
async def list_all_projects(
    current_user: User = Depends(get_current_active_user),
):
    """List all projects for the current user."""
    projects = list_projects()
    
    project_responses = []
    for p in projects:
        khasras_gdf = p.get("khasras_gdf_projected")
        project_responses.append(
            ProjectResponse(
                id=p["id"],
                name=p["name"],
                location=p["location"],
                description=p["description"],
                status=p["status"],
                created_at=p["created_at"],
                updated_at=p["updated_at"],
                khasra_count=len(khasras_gdf) if khasras_gdf is not None else None,
                total_area_ha=round(khasras_gdf["Original Area (ha)"].sum(), 2) if khasras_gdf is not None else None,
                layers_added=p["layers_added"],
            )
        )
    
    return ProjectListResponse(projects=project_responses, total=len(project_responses))


@app.get(
    "/projects/{project_id}",
    response_model=ProjectResponse,
    tags=["Projects"],
    summary="Get project details",
)
async def get_project_details(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """Get details about a specific project."""
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    khasras_gdf = project.get("khasras_gdf_projected")
    
    return ProjectResponse(
        id=project["id"],
        name=project["name"],
        location=project["location"],
        description=project["description"],
        status=project["status"],
        created_at=project["created_at"],
        updated_at=project["updated_at"],
        khasra_count=len(khasras_gdf) if khasras_gdf is not None else None,
        total_area_ha=round(khasras_gdf["Original Area (ha)"].sum(), 2) if khasras_gdf is not None else None,
        layers_added=project["layers_added"],
    )


@app.delete(
    "/projects/{project_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Projects"],
    summary="Delete a project",
)
async def delete_project_endpoint(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """Delete a project and all associated data."""
    if not delete_project(project_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    return None


# ============ Khasra Upload Endpoints ============

@app.post(
    "/projects/{project_id}/khasras",
    response_model=KhasraUploadResponse,
    tags=["Khasras"],
    summary="Upload khasra shapes",
)
async def upload_khasras(
    project_id: str,
    file: UploadFile = File(..., description="KML or GeoJSON file containing khasra boundaries"),
    id_column: Optional[str] = Form(None, description="Column name to use as Khasra ID"),
    current_user: User = Depends(get_current_active_user),
):
    """
    Upload khasra (land parcel) boundaries as KML or GeoJSON.
    
    The file should contain a single layer with polygon geometries representing khasras.
    Each khasra should have a unique ID (specify the column name if not 'Name').
    
    **Supported formats:**
    - KML (.kml)
    - GeoJSON (.geojson, .json)
    """
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    # Validate file type
    filename = file.filename.lower()
    if not any(filename.endswith(ext) for ext in [".kml", ".geojson", ".json"]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be KML or GeoJSON format",
        )
    
    try:
        content = await file.read()
        result = process_khasra_upload(
            file_content=content,
            filename=file.filename,
            project_id=project_id,
            id_column=id_column,
        )
        
        return KhasraUploadResponse(
            project_id=project_id,
            message=f"Successfully uploaded {result['khasra_count']} khasras",
            khasra_count=result["khasra_count"],
            total_area_ha=result["total_area_ha"],
            bounds=result["bounds"],
            crs=result["crs"],
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing file: {str(e)}",
        )


# ============ Layer Endpoints ============

@app.get(
    "/layers/available",
    response_model=AvailableLayersResponse,
    tags=["Layers"],
    summary="Get available layer types",
)
async def get_available_layers(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get information about available layer types and their configurable parameters.
    
    Each layer can be marked as:
    - **Unusable**: Area is completely unsuitable for solar (e.g., water, steep slopes)
    - **Unavailable**: Area is not currently available but could potentially be used (e.g., cropland)
    """
    return AvailableLayersResponse(layers=AVAILABLE_LAYERS)


@app.post(
    "/projects/{project_id}/layers",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Upload a custom layer",
)
async def upload_custom_layer(
    project_id: str,
    file: UploadFile = File(..., description="KML or GeoJSON file containing layer geometries"),
    layer_name: str = Form(..., description="Name for this layer"),
    is_unusable: bool = Form(True, description="If True, area is marked as unusable. If False, as unavailable."),
    current_user: User = Depends(get_current_active_user),
):
    """
    Upload a custom constraint layer (e.g., water bodies, forests, restricted areas).
    
    The layer will be intersected with khasra boundaries to calculate overlap areas.
    
    **Parameters:**
    - `layer_name`: A descriptive name for the layer
    - `is_unusable`: 
        - `True` = Areas are completely unsuitable (deducted from usable area)
        - `False` = Areas are unavailable but potentially usable (deducted from available area)
    """
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if project["khasras_gdf"] is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Khasras must be uploaded before adding layers",
        )
    
    # Validate file type
    filename = file.filename.lower()
    if not any(filename.endswith(ext) for ext in [".kml", ".geojson", ".json"]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be KML or GeoJSON format",
        )
    
    try:
        content = await file.read()
        layer_info = process_custom_layer_upload(
            file_content=content,
            filename=file.filename,
            project_id=project_id,
            layer_name=layer_name,
            is_unusable=is_unusable,
        )
        
        return LayerUploadResponse(
            project_id=project_id,
            message=f"Successfully added layer '{layer_name}'",
            layers_added=[layer_info],
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing layer: {str(e)}",
        )


@app.get(
    "/projects/{project_id}/layers",
    response_model=List[LayerInfo],
    tags=["Layers"],
    summary="List project layers",
)
async def list_project_layers(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """Get a list of all layers added to a project."""
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    layers = []
    for layer_name, layer_info in project["layers"].items():
        layer_gdf = project["layer_gdfs"].get(layer_name)
        area_col = layer_info.get("area_col")
        
        layers.append(
            LayerInfo(
                layer_type=layer_info.get("type", "custom").value if hasattr(layer_info.get("type"), "value") else str(layer_info.get("type", "custom")),
                name=layer_name,
                description=f"Layer: {layer_name}",
                is_unusable=layer_info.get("is_unusable", True),
                parameters={},
                area_ha=round(layer_gdf[area_col].sum(), 2) if layer_gdf is not None and area_col in layer_gdf.columns else None,
                feature_count=len(layer_gdf) if layer_gdf is not None else None,
            )
        )
    
    return layers


@app.post(
    "/projects/{project_id}/calculate-areas",
    tags=["Layers"],
    summary="Calculate usable areas",
)
async def calculate_areas(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """
    Calculate usable and available areas after applying all constraint layers.
    
    This step should be run after uploading khasras and adding all desired layers.
    """
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if project["khasras_gdf"] is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Khasras must be uploaded first",
        )
    
    try:
        stats_gdf = calculate_usable_areas(project_id)
        
        return {
            "project_id": project_id,
            "message": "Areas calculated successfully",
            "khasra_count": len(stats_gdf),
            "total_original_area_ha": round(stats_gdf["Original Area (ha)"].sum(), 2),
            "total_usable_area_ha": round(stats_gdf["Usable Area (ha)"].sum(), 2),
            "total_usable_available_area_ha": round(stats_gdf["Usable and Available Area (ha)"].sum(), 2),
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error calculating areas: {str(e)}",
        )


# ============ Clustering Endpoints ============

@app.post(
    "/projects/{project_id}/cluster",
    response_model=ClusteringResponse,
    tags=["Clustering"],
    summary="Cluster khasras into parcels",
)
async def cluster_khasras_endpoint(
    project_id: str,
    request: ClusteringRequest,
    current_user: User = Depends(get_current_active_user),
):
    """
    Cluster adjacent khasras into larger parcels using DBSCAN algorithm.
    
    **Parameters:**
    - `distance_threshold`: Maximum distance (meters) between khasra boundaries to be considered adjacent
    - `min_samples`: Minimum number of khasras required to form a cluster
    
    Khasras that don't meet the clustering criteria will be marked as "UNCLUSTERED".
    """
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if project["khasras_gdf"] is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Khasras must be uploaded first",
        )
    
    try:
        result = cluster_khasras(project_id, request)
        
        return ClusteringResponse(
            project_id=project_id,
            message=f"Successfully clustered khasras into {result['total_parcels']} parcels",
            distance_threshold=request.distance_threshold,
            total_parcels=result["total_parcels"],
            clustered_khasras=result["clustered_khasras"],
            unclustered_khasras=result["unclustered_khasras"],
            parcels=result["parcels"],
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clustering khasras: {str(e)}",
        )


# ============ Export/Download Endpoints ============

@app.post(
    "/projects/{project_id}/export",
    tags=["Export"],
    summary="Export project data",
)
async def export_project_data(
    project_id: str,
    export_type: ExportType = Query(ExportType.ALL, description="Type of data to export"),
    format: ExportFormat = Query(ExportFormat.GEOJSON, description="Export file format"),
    include_statistics: bool = Query(True, description="Include summary statistics (for Excel)"),
    current_user: User = Depends(get_current_active_user),
):
    """
    Export project data in various formats.
    
    **Export Types:**
    - `khasras`: Original khasra boundaries
    - `khasras_with_stats`: Khasras with calculated area statistics
    - `parcels`: Clustered parcel boundaries and statistics
    - `layers`: All constraint layers
    - `all`: Everything
    
    **Formats:**
    - `geojson`: GeoJSON format (good for web mapping)
    - `kml`: KML format (for Google Earth)
    - `shapefile`: ESRI Shapefile (for GIS software)
    - `parquet`: GeoParquet (efficient storage)
    - `csv`: CSV without geometry (for spreadsheets)
    - `excel`: Excel workbook with multiple sheets
    """
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    try:
        content, filename = export_data(
            project_id=project_id,
            export_type=export_type,
            export_format=format,
            include_statistics=include_statistics,
        )
        
        # Determine media type
        media_types = {
            ExportFormat.GEOJSON: "application/geo+json",
            ExportFormat.KML: "application/vnd.google-earth.kml+xml",
            ExportFormat.SHAPEFILE: "application/zip",
            ExportFormat.PARQUET: "application/octet-stream",
            ExportFormat.CSV: "application/zip",
            ExportFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        
        media_type = media_types.get(format, "application/octet-stream")
        
        # If it's a zip file, adjust media type
        if filename.endswith(".zip"):
            media_type = "application/zip"
        
        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error exporting data: {str(e)}",
        )


@app.get(
    "/projects/{project_id}/download/{data_type}",
    tags=["Export"],
    summary="Quick download endpoint",
)
async def quick_download(
    project_id: str,
    data_type: str,
    format: ExportFormat = Query(ExportFormat.GEOJSON),
    current_user: User = Depends(get_current_active_user),
):
    """
    Quick download endpoint for specific data types.
    
    **Data Types:**
    - `khasras`: Original khasra boundaries
    - `khasras_stats`: Khasras with area statistics
    - `parcels`: Clustered parcels
    - `layers`: All constraint layers
    """
    type_mapping = {
        "khasras": ExportType.KHASRAS,
        "khasras_stats": ExportType.KHASRAS_WITH_STATS,
        "parcels": ExportType.PARCELS,
        "layers": ExportType.LAYERS,
    }
    
    export_type = type_mapping.get(data_type)
    if not export_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid data type. Must be one of: {list(type_mapping.keys())}",
        )
    
    return await export_project_data(
        project_id=project_id,
        export_type=export_type,
        format=format,
        include_statistics=True,
        current_user=current_user,
    )


# ============ Statistics Endpoints ============

@app.get(
    "/projects/{project_id}/stats",
    tags=["Statistics"],
    summary="Get project statistics",
)
async def get_project_stats(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
):
    """Get summary statistics for a project."""
    project = get_project(project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    stats = {
        "project_id": project_id,
        "project_name": project["name"],
        "location": project["location"],
        "status": project["status"].value,
    }
    
    # Khasra stats
    khasras_gdf = project.get("khasras_gdf_projected")
    if khasras_gdf is not None:
        stats["khasras"] = {
            "count": len(khasras_gdf),
            "total_area_ha": round(khasras_gdf["Original Area (ha)"].sum(), 2),
        }
    
    # Stats after layer processing
    stats_gdf = project.get("stats_gdf")
    if stats_gdf is not None:
        stats["areas"] = {
            "original_area_ha": round(stats_gdf["Original Area (ha)"].sum(), 2),
            "usable_area_ha": round(stats_gdf["Usable Area (ha)"].sum(), 2),
            "usable_area_percent": round(
                stats_gdf["Usable Area (ha)"].sum() / stats_gdf["Original Area (ha)"].sum() * 100, 2
            ),
            "usable_available_area_ha": round(stats_gdf["Usable and Available Area (ha)"].sum(), 2),
            "usable_available_area_percent": round(
                stats_gdf["Usable and Available Area (ha)"].sum() / stats_gdf["Original Area (ha)"].sum() * 100, 2
            ),
        }
    
    # Parcel stats
    parcel_gdf = project.get("parcel_gdf")
    if parcel_gdf is not None:
        stats["parcels"] = {
            "total_count": len(parcel_gdf),
            "clustered_count": len(parcel_gdf[~parcel_gdf["Parcel ID"].str.contains("UNCLUSTERED")]),
            "unclustered_count": len(parcel_gdf[parcel_gdf["Parcel ID"].str.contains("UNCLUSTERED")]),
        }
    
    # Layer stats
    stats["layers"] = []
    for layer_name, layer_info in project["layers"].items():
        layer_gdf = project["layer_gdfs"].get(layer_name)
        area_col = layer_info.get("area_col")
        
        layer_stat = {
            "name": layer_name,
            "is_unusable": layer_info.get("is_unusable", True),
            "feature_count": len(layer_gdf) if layer_gdf is not None else 0,
        }
        
        if layer_gdf is not None and area_col and area_col in layer_gdf.columns:
            layer_stat["total_area_ha"] = round(layer_gdf[area_col].sum(), 2)
        
        stats["layers"].append(layer_stat)
    
    return stats


# ============ Run Server ============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
