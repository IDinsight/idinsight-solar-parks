"""
Solar Parks Analysis API - FastAPI Application

A FastAPI application for processing and analyzing solar park land parcels (khasras).
Provides endpoints for uploading khasra shapes, adding constraint layers, clustering,
and exporting results.

Now with PostgreSQL/PostGIS persistence and local file storage.
"""

import json
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import uvicorn
from auth import (
    authenticate_user,
    create_access_token,
    fake_users_db,
    get_current_active_user,
)
from config import AVAILABLE_LAYERS, settings
from database import LayerFeatureModel, LayerModel, SessionLocal, get_db, init_db
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
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
    HealthCheckResponse,
    KhasraStatsInfo,
    KhasraUploadResponse,
    LayerInfo,
    LayerStatsInfo,
    LayerType,
    LayerUploadResponse,
    ParcelStatsInfo,
    ProjectCreate,
    ProjectListResponse,
    ProjectResponse,
    ProjectStatsResponse,
    SettlementLayerRequest,
    SlopesLayerRequest,
    Token,
    User,
)
from services import (
    calculate_usable_areas,
    cluster_khasras,
    create_project,
    delete_khasras,
    delete_parcels,
    delete_project,
    export_data,
    get_khasras,
    get_khasras_with_stats_gdf,
    get_layers_geojson,
    get_layers_metadata,
    get_parcels_gdf,
    get_project,
    list_projects,
    process_cropland_layer,
    process_cropland_layer_background,
    process_custom_layer_background,
    process_khasra_upload,
    process_settlement_layer,
    process_settlement_layer_background,
    process_slopes_layer,
    process_slopes_layer_background,
    process_water_layer,
    process_water_layer_background,
)
from sqlalchemy import delete as sql_delete
from sqlalchemy.orm import Session

# ============ Lifespan Event Handler ============


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events."""
    # Startup
    init_db()
    yield
    # Shutdown (if needed in the future)


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
    lifespan=lifespan,
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
async def health_check_endpoint():
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
async def login_for_access_toke_endpoint(
    form_data: OAuth2PasswordRequestForm = Depends(),
):
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
async def read_users_me_endpoint(current_user: User = Depends(get_current_active_user)):
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
async def create_project_endpoint(
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
async def list_projects_endpoint(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List all projects for the current user."""
    projects = list_projects(db)

    project_responses = []
    for p in projects:
        layers = get_layers_metadata(db, p.id)
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
async def get_project_details_endpoint(
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

    layers = get_layers_metadata(db, project_id)

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


@app.get(
    "/projects/{project_id}/khasras",
    response_model=Dict[str, Any],
    tags=["Khasras"],
    summary="Get khasra summary for a project",
)
async def get_khasras_endpoint(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get summary information about khasras uploaded for a project.

    Returns count, total area, and upload date if khasras exist.
    """
    summary = get_khasras(db, project_id)
    return summary


@app.post(
    "/projects/{project_id}/khasras",
    response_model=KhasraUploadResponse,
    tags=["Khasras"],
    summary="Upload khasra shapes",
)
async def upload_khasras_endpoint(
    project_id: str,
    file: UploadFile = File(
        ..., description="KML or GeoJSON file containing khasra boundaries"
    ),
    id_column: Optional[str] = Form(
        None, description="Column name to use as Khasra ID"
    ),
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
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing file: {str(e)}",
        )


@app.delete(
    "/projects/{project_id}/khasras",
    tags=["Khasras"],
    summary="Delete khasras for a project",
)
async def delete_khasras_endpoint(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Delete all khasras for a project.

    **Warning:** This will also delete all dependent data including:
    - Settlement layers
    - Building layers
    - Clustering results
    - Generated statistics

    The project status will be reset to CREATED.
    """
    success = delete_khasras(db, project_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found or no khasras to delete",
        )
    return {"message": "Khasras and all dependent data deleted successfully"}


# ============ Layer Endpoints ============


@app.get(
    "/layers/available_builtin",
    response_model=AvailableLayersResponse,
    tags=["Layers"],
    summary="Get available layer types",
)
async def get_available_layers_endpoint(
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
    "/projects/{project_id}/layers/custom_upload",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Upload a custom layer",
)
async def upload_custom_layer_endpoint(
    project_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(
        ..., description="KML or GeoJSON file containing layer geometries"
    ),
    layer_name: str = Form(..., description="Name for this layer"),
    is_unusable: bool = Form(
        True,
        description="If True, area is marked as unusable. If False, as unavailable.",
    ),
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
        
        # Create placeholder layer record immediately
        temp_db = SessionLocal()
        try:
            layer = LayerModel(
                project_id=project_id,
                name=layer_name,
                layer_type=LayerType.CUSTOM.value,
                is_unusable=is_unusable,
                status="in_progress",
                details="Queued for processing...",
                parameters={},
            )
            temp_db.add(layer)
            temp_db.commit()
            
            layer_info = LayerInfo(
                layer_type=LayerType.CUSTOM.value,
                name=layer_name,
                description=f"Custom uploaded layer: {layer_name}",
                is_unusable=is_unusable,
                parameters={},
                status="in_progress",
                details="Queued for processing...",
            )
            temp_db.close()
        except Exception as e:
            temp_db.close()
            raise
        
        # Schedule background processing
        background_tasks.add_task(
            process_custom_layer_background,
            file_content=content,
            filename=file.filename,
            project_id=project_id,
            layer_name=layer_name,
            is_unusable=is_unusable,
        )

        return LayerUploadResponse(
            project_id=project_id,
            message=f"Layer '{layer_name}' processing started. Poll /projects/{project_id}/layers for status updates.",
            layers_added=[layer_info],
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error initializing layer: {str(e)}",
        )


@app.post(
    "/projects/{project_id}/layers/settlements",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Generate settlement layer from buildings",
)
async def generate_settlement_layer_endpoint(
    project_id: str,
    background_tasks: BackgroundTasks,
    request: SettlementLayerRequest = SettlementLayerRequest(),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Automatically generate settlement and isolated building layers from VIDA rooftop data.

    This endpoint returns immediately and processes layers in the background.
    Poll `/projects/{project_id}/layers` to check processing status.

    This endpoint:
    1. Downloads building footprints from VIDA for the project area
    2. Buffers buildings by `building_buffer` meters
    3. Clusters buildings using DBSCAN to identify settlements
    4. Creates two layers:
       - **Settlements**: Convex hulls of building clusters (marked as unusable)
       - **Isolated Buildings**: Individual buildings not in settlements (marked as unavailable)
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
            detail="Khasras must be uploaded before generating settlement layers",
        )

    # Create placeholder layer records immediately

    temp_db = SessionLocal()
    try:
        layer_infos = process_settlement_layer(
            db=temp_db,
            project_id=project_id,
            building_buffer=request.building_buffer,
            settlement_eps=request.settlement_eps,
            min_buildings=request.min_buildings,
            create_only=True,  # Only create layer records, don't process yet
        )
        temp_db.close()
    except Exception as e:
        temp_db.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error initializing settlement layers: {str(e)}",
        )

    # Schedule background processing
    background_tasks.add_task(
        process_settlement_layer_background,
        project_id=project_id,
        building_buffer=request.building_buffer,
        settlement_eps=request.settlement_eps,
        min_buildings=request.min_buildings,
    )

    return LayerUploadResponse(
        project_id=project_id,
        message="Settlement layer processing started. Poll /projects/{project_id}/layers for status updates.",
        layers_added=list(layer_infos),
    )


@app.post(
    "/projects/{project_id}/layers/cropland",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Generate cropland layer from landcover data",
)
async def generate_cropland_layer_endpoint(
    project_id: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Automatically generate cropland layer from landcover TIFF data.

    This endpoint returns immediately and processes the layer in the background.
    Poll `/projects/{project_id}/layers` to check processing status.

    This endpoint:
    1. Loads landcover TIFF data for the project area
    2. Extracts cropland polygons (class="Cropland")
    3. Overlays with khasras to get intersection
    4. Saves cropland layer (marked as unusable)
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
            detail="Khasras must be uploaded before generating cropland layer",
        )

    # Create placeholder layer record
    temp_db = SessionLocal()
    try:
        layer_info = process_cropland_layer(
            db=temp_db,
            project_id=project_id,
            create_only=True,
        )
        temp_db.close()
    except Exception as e:
        temp_db.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error initializing cropland layer: {str(e)}",
        )

    # Schedule background processing
    background_tasks.add_task(
        process_cropland_layer_background,
        project_id=project_id,
    )

    return LayerUploadResponse(
        project_id=project_id,
        message="Cropland layer processing started. Poll /projects/{project_id}/layers for status updates.",
        layers_added=[layer_info],
    )


@app.post(
    "/projects/{project_id}/layers/water",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Generate water layer from landcover data",
)
async def generate_water_layer_endpoint(
    project_id: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Automatically generate water layer from landcover TIFF data.

    This endpoint returns immediately and processes the layer in the background.
    Poll `/projects/{project_id}/layers` to check processing status.

    This endpoint:
    1. Loads landcover TIFF data for the project area
    2. Extracts water body polygons (class="Open surface water")
    3. Overlays with khasras to get intersection
    4. Saves water layer (marked as unusable)
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
            detail="Khasras must be uploaded before generating water layer",
        )

    # Create placeholder layer record
    temp_db = SessionLocal()
    try:
        layer_info = process_water_layer(
            db=temp_db,
            project_id=project_id,
            create_only=True,
        )
        temp_db.close()
    except Exception as e:
        temp_db.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error initializing water layer: {str(e)}",
        )

    # Schedule background processing
    background_tasks.add_task(
        process_water_layer_background,
        project_id=project_id,
    )

    return LayerUploadResponse(
        project_id=project_id,
        message="Water layer processing started. Poll /projects/{project_id}/layers for status updates.",
        layers_added=[layer_info],
    )


@app.post(
    "/projects/{project_id}/layers/slopes",
    response_model=LayerUploadResponse,
    tags=["Layers"],
    summary="Generate slopes layer from NASA ALOS DEM data",
)
async def generate_slopes_layer_endpoint(
    project_id: str,
    background_tasks: BackgroundTasks,
    request: SlopesLayerRequest = SlopesLayerRequest(),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Automatically generate slopes layer from NASA ALOS PALSAR DEM data.

    This endpoint returns immediately and processes the layer in the background.
    Poll `/projects/{project_id}/layers` to check processing status.

    This endpoint:
    1. Downloads DEM tiles from NASA ALOS using ASF API for the project area
    2. Calculates slope angles and aspects from DEM data
    3. Extracts steep slope areas based on configurable thresholds:
       - North-facing slopes (45-135° aspect): Minimum angle configurable (default 7°)
       - Other slopes (remaining aspects): Minimum angle configurable (default 10°)
    4. Overlays with khasras to get intersection
    5. Saves slopes layer (marked as unusable)

    **Parameters:**
    - `include_north_slopes`: Include north-facing steep slopes (NE to NW, 45-135°)
    - `include_other_slopes`: Include other steep slopes (south/east/west facing)
    - `north_min_angle`: Minimum slope angle for north-facing slopes (degrees)
    - `other_min_angle`: Minimum slope angle for other-facing slopes (degrees)
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
            detail="Khasras must be uploaded before generating slopes layer",
        )

    # Create placeholder layer records
    temp_db = SessionLocal()
    try:
        layer_infos = process_slopes_layer(
            db=temp_db,
            project_id=project_id,
            include_north_slopes=request.include_north_slopes,
            include_other_slopes=request.include_other_slopes,
            north_min_angle=request.north_min_angle,
            other_min_angle=request.other_min_angle,
            create_only=True,
        )
        temp_db.close()
    except Exception as e:
        temp_db.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error initializing slopes layers: {str(e)}",
        )

    # Schedule background processing
    background_tasks.add_task(
        process_slopes_layer_background,
        project_id=project_id,
        include_north_slopes=request.include_north_slopes,
        include_other_slopes=request.include_other_slopes,
        north_min_angle=request.north_min_angle,
        other_min_angle=request.other_min_angle,
    )

    layer_names = ", ".join([layer.name for layer in layer_infos]) if layer_infos else "slopes"
    return LayerUploadResponse(
        project_id=project_id,
        message=f"Slopes processing started ({layer_names}). Poll /projects/{{project_id}}/layers for status updates.",
        layers_added=layer_infos,
    )


@app.get(
    "/projects/{project_id}/layers",
    response_model=List[LayerInfo],
    tags=["Layers"],
    summary="List project layers",
)
async def get_layers_metadata_endpoint(
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

    layers_data = get_layers_metadata(db, project_id)
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


@app.delete(
    "/projects/{project_id}/layers/{layer_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Layers"],
    summary="Delete a layer",
)
async def delete_layer_endpoint(
    project_id: str,
    layer_name: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Delete a layer and all its features from a project.
    """
    
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )

    # Find the layer
    layer = (
        db.query(LayerModel)
        .filter(LayerModel.project_id == project_id, LayerModel.name == layer_name)
        .first()
    )
    
    if not layer:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Layer '{layer_name}' not found",
        )

    # Delete layer features first (cascade should handle this, but being explicit)
    db.execute(sql_delete(LayerFeatureModel).where(LayerFeatureModel.layer_id == layer.id))

    # Delete the layer
    db.delete(layer)
    db.commit()

    # Automatically recalculate areas after layer deletion
    try:
        from services import recalculate_areas_and_parcels
        recalculate_areas_and_parcels(db, project_id)
    except Exception as e:
        # Log but don't fail the deletion if recalculation fails
        print(f"Warning: Failed to recalculate areas after layer deletion: {e}")

    return None


@app.get(
    "/projects/{project_id}/layers/geojson",
    response_model=Dict[str, Any],
    tags=["Layers"],
    summary="Get project layers as GeoJSON",
)
async def get_layers_geojson_endpoint(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get all layer geometries for a project as GeoJSON features.
    Returns a dictionary with layer names as keys and GeoJSON FeatureCollections as values.
    """

    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )

    return get_layers_geojson(db, project_id)


@app.post(
    "/projects/{project_id}/calculate-areas",
    response_model=CalculateAreasResponse,
    tags=["Layers"],
    summary="Calculate usable areas",
)
async def calculate_areas_endpoint(
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
            total_usable_available_area_ha=round(
                stats_gdf["Usable and Available Area (ha)"].sum(), 2
            ),
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
        error_details = traceback.format_exc()
        print(f"[CLUSTERING ERROR] {error_details}", flush=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clustering khasras: {str(e)}",
        )


@app.get(
    "/projects/{project_id}/parcels/geojson",
    tags=["Clustering"],
    summary="Get parcel boundaries as GeoJSON",
)
async def get_parcels_geojson(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Get parcel boundaries as GeoJSON with parcel IDs and statistics.
    
    Returns the clustered parcel geometries that can be displayed on a map.
    Each feature includes parcel_id, khasra_count, and area statistics.
    """
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )

    try:
        parcel_gdf, clustering_params = get_parcels_gdf(db, project_id)
        
        if parcel_gdf is None or len(parcel_gdf) == 0:
            return {
                "type": "FeatureCollection",
                "features": [],
                "clusteringParams": clustering_params
            }
        
        # Convert to GeoJSON
        geojson = json.loads(parcel_gdf.to_json())
        # Add clustering params to response
        geojson["clusteringParams"] = clustering_params
        
        return geojson
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving parcel geometries: {str(e)}",
        )


@app.delete(
    "/projects/{project_id}/parcels",
    tags=["Clustering"],
    summary="Delete clustering results",
)
async def delete_parcels_endpoint(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Delete all clustering results (parcels) for a project.
    
    This resets the project back to the state before clustering,
    allowing you to re-run clustering with different parameters.
    """
    project = get_project(db, project_id)
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project {project_id} not found",
        )

    try:
        success = delete_parcels(db, project_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No clustering results found to delete",
            )
        
        return {
            "message": "Successfully deleted clustering results",
            "project_id": project_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting parcels: {str(e)}",
        )


# ============ Export/Download Endpoints ============


@app.post(
    "/projects/{project_id}/export",
    tags=["Export"],
    summary="Export project data",
)
async def export_project_data_endpoint(
    project_id: str,
    request: ExportRequest = ExportRequest(),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """
    Export all project data in various formats.
    
    Always exports khasras (with statistics), parcels (if clustered), and all constraint layers.

    **Formats:**
    - `geojson`: Single file or ZIP with multiple GeoJSON files
    - `kml`: Single KMZ file with all data as separate folders
    - `shapefile`: ZIP file with shapefiles for each layer
    - `parquet`: ZIP file with parquet files for each layer
    - `csv`: ZIP file with CSV files for each layer (geometry as WKT)
    - `excel`: Single Excel workbook with multiple sheets and statistics
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
        import traceback
        error_details = traceback.format_exc()
        print(f"[EXPORT ERROR] {error_details}", flush=True)
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
async def get_project_stats_endpoint(
    project_id: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Get summary statistics for a project."""

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
    if (
        stats_gdf is not None
        and len(stats_gdf) > 0
        and "Original Area (ha)" in stats_gdf.columns
    ):
        original_total = stats_gdf["Original Area (ha)"].sum()
        usable_total = stats_gdf["Usable Area (ha)"].sum()
        usable_available_total = stats_gdf["Usable and Available Area (ha)"].sum()

        if original_total > 0:
            areas_info = AreaStatsInfo(
                original_area_ha=round(original_total, 2),
                usable_area_ha=round(usable_total, 2),
                usable_area_percent=round(usable_total / original_total * 100, 2),
                usable_available_area_ha=round(usable_available_total, 2),
                usable_available_area_percent=round(
                    usable_available_total / original_total * 100, 2
                ),
            )

    # Parcel stats from database
    parcel_gdf, _ = get_parcels_gdf(db, project_id)
    if (
        parcel_gdf is not None
        and len(parcel_gdf) > 0
        and "parcel_id" in parcel_gdf.columns
    ):
        total_count = len(parcel_gdf)
        unclustered_count = len(
            parcel_gdf[parcel_gdf["parcel_id"].str.contains("UNCLUSTERED", na=False)]
        )
        clustered_count = total_count - unclustered_count

        parcels_info = ParcelStatsInfo(
            total_count=total_count,
            clustered_count=clustered_count,
            unclustered_count=unclustered_count,
        )

    # Layer stats
    layers = get_layers_metadata(db, project_id)
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
        status=project.status.value
        if hasattr(project.status, "value")
        else str(project.status),
        khasras=khasras_info,
        areas=areas_info,
        parcels=parcels_info,
        layers=layers_info,
    )


# ============ Run Server ============

if __name__ == "__main__":
    

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
