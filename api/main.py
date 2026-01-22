"""
Solar Parks Analysis API - FastAPI Application

A FastAPI application for processing and analyzing solar park land parcels (khasras).
Provides endpoints for uploading khasra shapes, adding constraint layers, clustering,
and exporting results.

Now with PostgreSQL/PostGIS persistence and local file storage.
"""
from datetime import datetime, timedelta
from typing import List, Optional

from auth import (
    authenticate_user,
    create_access_token,
    fake_users_db,
    get_current_active_user,
)
from config import AVAILABLE_LAYERS, settings
from database import get_db, init_db
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.security import OAuth2PasswordRequestForm
from models import (
    AreaStatsInfo,
    AvailableLayersResponse,
    CalculateAreasResponse,
    ClusteringRequest,
    ClusteringResponse,
    ExportFormat,
    ExportRequest,
    ExportType,
    HealthCheckResponse,
    KhasraStatsInfo,
    KhasraUploadResponse,
    LayerInfo,
    LayerStatsInfo,
    LayerUploadResponse,
    ParcelStatsInfo,
    ProjectCreate,
    ProjectListResponse,
    ProjectResponse,
    ProjectStatsResponse,
    SettlementLayerRequest,
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
    get_project_layers,
    list_projects,
    process_custom_layer_upload,
    process_khasra_upload,
)
from sqlalchemy.orm import Session

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


# ============ Startup Event ============

@app.on_event("startup")
async def startup_event():
    """Initialize the database on startup."""
    init_db()


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
    - Password: `admin`
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
    db: Session = Depends(get_db),
):
    """
    Create a new solar park analysis project.
    
    Each project can have its own khasra boundaries, layers, and clustering configuration.
    """
    project_id = create_project(
        db=db,
        name=project.name,
        location=project.location,
        description=project.description,
    )
    project_data = get_project(db, project_id)
    
    return ProjectResponse(
        id=project_data.id,
        name=project_data.name,
        location=project_data.location,
        description=project_data.description,
        status=project_data.status,
        created_at=project_data.created_at,
        updated_at=project_data.updated_at,
        khasra_count=project_data.khasra_count,
        total_area_ha=project_data.total_area_ha,
        layers_added=[],
    )


@app.get(
    "/projects",
    response_model=ProjectListResponse,
    tags=["Projects"],
    summary="List all projects",
)
async def list_all_projects(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List all projects for the current user."""
    projects = list_projects(db)
    
    project_responses = []
    for p in projects:
        layers = get_project_layers(db, p.id)
        project_responses.append(
            ProjectResponse(
                id=p.id,
                name=p.name,
                location=p.location,
                description=p.description,
                status=p.status,
                created_at=p.created_at,
                updated_at=p.updated_at,
                khasra_count=p.khasra_count,
                total_area_ha=p.total_area_ha,
                layers_added=[layer.name for layer in layers],
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
    db: Session = Depends(get_db),
):
    """Get details about a specific project."""
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    layers = get_project_layers(db, project_id)
    
    return ProjectResponse(
        id=project.id,
        name=project.name,
        location=project.location,
        description=project.description,
        status=project.status,
        created_at=project.created_at,
        updated_at=project.updated_at,
        khasra_count=project.khasra_count,
        total_area_ha=project.total_area_ha,
        layers_added=[layer.name for layer in layers],
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
    db: Session = Depends(get_db),
):
    """Delete a project and all associated data."""
    if not delete_project(db, project_id):
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
    db: Session = Depends(get_db),
):
    """
    Upload khasra (land parcel) boundaries as KML or GeoJSON.
    
    The file should contain a single layer with polygon geometries representing khasras.
    Each khasra should have a unique ID (specify the column name if not 'Name').
    
    **Supported formats:**
    - KML (.kml)
    - GeoJSON (.geojson, .json)
    """
    project = get_project(db, project_id)
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
            db=db,
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
    db: Session = Depends(get_db),
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
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if not project.khasra_count or project.khasra_count == 0:
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
            db=db,
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


@app.post(
    "/projects/{project_id}/layers/settlements",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Generate settlement layer from buildings",
)
async def generate_settlement_layer(
    project_id: str,
    request: SettlementLayerRequest = SettlementLayerRequest(),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Automatically generate settlement and isolated building layers from VIDA rooftop data.
    
    This endpoint:
    1. Downloads building footprints from VIDA for the project area
    2. Buffers buildings by `building_buffer` meters
    3. Clusters buildings using DBSCAN to identify settlements
    4. Creates two layers:
       - **Settlements**: Convex hulls of building clusters (marked as unusable)
       - **Isolated Buildings**: Individual buildings not in settlements (marked as unavailable)
    """
    from services import process_settlement_layer
    
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if not project.khasra_count or project.khasra_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Khasras must be uploaded before generating settlement layers",
        )
    
    try:
        layer_infos = process_settlement_layer(
            db=db,
            project_id=project_id,
            building_buffer=request.building_buffer,
            settlement_eps=request.settlement_eps,
            min_buildings=request.min_buildings,
        )
        
        return LayerUploadResponse(
            project_id=project_id,
            message="Successfully generated settlement layers",
            layers_added=list(layer_infos),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error generating settlement layer: {str(e)}",
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
    db: Session = Depends(get_db),
):
    """
    Get a list of all layers added to a project.
    
    Each layer includes:
    - `status`: Processing status (in_progress, successful, failed)
    - `details`: Current processing step or completion message
    - `area_ha`: Total area in hectares (available when successful)
    - `feature_count`: Number of features (available when successful)
    """
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    layers_data = get_project_layers(db, project_id)
    layers = []
    for layer in layers_data:
        layers.append(
            LayerInfo(
                layer_type=layer.layer_type,
                name=layer.name,
                description=f"Layer: {layer.name}",
                is_unusable=layer.is_unusable,
                parameters=layer.parameters or {},
                area_ha=layer.total_area_ha,
                feature_count=layer.feature_count,
                status=layer.status,
                details=layer.details,
            )
        )
    
    return layers


@app.post(
    "/projects/{project_id}/calculate-areas",
    response_model=CalculateAreasResponse,
    tags=["Layers"],
    summary="Calculate usable areas",
)
async def calculate_areas(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Calculate usable and available areas after applying all constraint layers.
    
    This step should be run after uploading khasras and adding all desired layers.
    """
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if not project.khasra_count or project.khasra_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Khasras must be uploaded first",
        )
    
    try:
        stats_gdf = calculate_usable_areas(db, project_id)
        
        return CalculateAreasResponse(
            project_id=project_id,
            message="Areas calculated successfully",
            khasra_count=len(stats_gdf),
            total_original_area_ha=round(stats_gdf["Original Area (ha)"].sum(), 2),
            total_usable_area_ha=round(stats_gdf["Usable Area (ha)"].sum(), 2),
            total_usable_available_area_ha=round(stats_gdf["Usable and Available Area (ha)"].sum(), 2),
        )
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
    db: Session = Depends(get_db),
):
    """
    Cluster adjacent khasras into larger parcels using DBSCAN algorithm.
    
    **Parameters:**
    - `distance_threshold`: Maximum distance (meters) between khasra boundaries to be considered adjacent
    - `min_samples`: Minimum number of khasras required to form a cluster
    
    Khasras that don't meet the clustering criteria will be marked as "UNCLUSTERED".
    """
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    if not project.khasra_count or project.khasra_count == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Khasras must be uploaded first",
        )
    
    try:
        result = cluster_khasras(db, project_id, request)
        
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
    request: ExportRequest = ExportRequest(),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
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
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    try:
        content, filename = export_data(
            db=db,
            project_id=project_id,
            export_type=request.export_type,
            export_format=request.format,
            include_statistics=request.include_statistics,
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
        
        media_type = media_types.get(request.format, "application/octet-stream")
        
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


# ============ Statistics Endpoints ============

@app.get(
    "/projects/{project_id}/WIP_stats",
    response_model=ProjectStatsResponse,
    tags=["Statistics"],
    summary="Get project statistics",
)
async def get_project_stats(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Get summary statistics for a project."""
    from services import get_khasras_with_stats_gdf, get_parcels_gdf
    
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )
    
    # Build response
    khasras_info = None
    areas_info = None
    parcels_info = None
    layers_info = []
    
    # Khasra stats
    if project.khasra_count:
        khasras_info = KhasraStatsInfo(
            count=project.khasra_count,
            total_area_ha=project.total_area_ha,
        )
    
    # Area stats from khasras with calculated statistics
    stats_gdf = get_khasras_with_stats_gdf(db, project_id)
    if stats_gdf is not None and len(stats_gdf) > 0 and "Original Area (ha)" in stats_gdf.columns:
        original_total = stats_gdf["Original Area (ha)"].sum()
        usable_total = stats_gdf["Usable Area (ha)"].sum()
        usable_available_total = stats_gdf["Usable and Available Area (ha)"].sum()
        
        if original_total > 0:
            areas_info = AreaStatsInfo(
                original_area_ha=round(original_total, 2),
                usable_area_ha=round(usable_total, 2),
                usable_area_percent=round(usable_total / original_total * 100, 2),
                usable_available_area_ha=round(usable_available_total, 2),
                usable_available_area_percent=round(usable_available_total / original_total * 100, 2),
            )
    
    # Parcel stats from database
    parcel_gdf = get_parcels_gdf(db, project_id)
    if parcel_gdf is not None and len(parcel_gdf) > 0 and "parcel_id" in parcel_gdf.columns:
        total_count = len(parcel_gdf)
        unclustered_count = len(parcel_gdf[parcel_gdf["parcel_id"].str.contains("UNCLUSTERED", na=False)])
        clustered_count = total_count - unclustered_count
        
        parcels_info = ParcelStatsInfo(
            total_count=total_count,
            clustered_count=clustered_count,
            unclustered_count=unclustered_count,
        )
    
    # Layer stats
    layers = get_project_layers(db, project_id)
    for layer in layers:
        layers_info.append(
            LayerStatsInfo(
                name=layer.name,
                is_unusable=layer.is_unusable,
                feature_count=layer.feature_count or 0,
                total_area_ha=layer.total_area_ha,
            )
        )
    
    return ProjectStatsResponse(
        project_id=project_id,
        project_name=project.name,
        location=project.location,
        status=project.status.value if hasattr(project.status, 'value') else str(project.status),
        khasras=khasras_info,
        areas=areas_info,
        parcels=parcels_info,
        layers=layers_info,
    )


# ============ Run Server ============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
