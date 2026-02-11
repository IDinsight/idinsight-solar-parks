"""
Pydantic models for request/response validation
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# ============ Authentication Models ============

class Token(BaseModel):
    """JWT token response"""
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Token payload data"""
    username: Optional[str] = None


class User(BaseModel):
    """User model"""
    username: str
    disabled: Optional[bool] = False


class UserInDB(User):
    """User model with password hash"""
    hashed_password: str


# ============ Project Models ============

class ProjectStatus(str, Enum):
    """Project processing status"""
    CREATED = "created"
    KHASRAS_UPLOADED = "khasras_uploaded"
    LAYERS_ADDED = "layers_added"
    CLUSTERED = "clustered"
    COMPLETED = "completed"
    ERROR = "error"


class ProjectCreate(BaseModel):
    """Create a new project"""
    name: str = Field(..., min_length=1, max_length=100, description="Project name")
    location: str = Field(..., min_length=1, max_length=100, description="Location/District name")
    description: Optional[str] = Field(None, max_length=500, description="Project description")


class ProjectUpdate(BaseModel):
    """Update an existing project"""
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="Project name")
    location: Optional[str] = Field(None, min_length=1, max_length=100, description="Location/District name")
    description: Optional[str] = Field(None, max_length=500, description="Project description")


class ProjectResponse(BaseModel):
    """Project response model"""
    id: str
    name: str
    location: str
    description: Optional[str]
    status: ProjectStatus
    created_at: datetime
    updated_at: datetime
    khasra_count: Optional[int] = None
    total_area_ha: Optional[float] = None
    layers_added: List[str] = []


class ProjectListResponse(BaseModel):
    """List of projects response"""
    projects: List[ProjectResponse]
    total: int


# ============ Khasra/Parcel Models ============

class KhasraUploadResponse(BaseModel):
    """Response after uploading khasra shapes"""
    project_id: str
    message: str
    khasra_count: int
    total_area_ha: float
    bounds: Dict[str, float]  # minx, miny, maxx, maxy
    crs: str


class KhasraStats(BaseModel):
    """Statistics for a single khasra"""
    khasra_id: str
    khasra_id_unique: str
    original_area_ha: float
    usable_area_ha: Optional[float] = None
    unusable_area_ha: Optional[float] = None
    usable_area_percent: Optional[float] = None


# ============ Layer Models ============

class LayerType(str, Enum):
    """Available layer types"""
    ISOLATED_BUILDINGS = "isolated_buildings"
    SETTLEMENTS = "settlements"
    WATER = "water"
    CROPLAND = "cropland"
    SLOPE_NORTH = "slope_north"
    SLOPE_OTHER = "slope_other"
    CUSTOM = "custom"
    BUILTIN = "builtin"


class LayerStatus(str, Enum):
    """Layer processing status"""
    IN_PROGRESS = "in_progress"
    SUCCESSFUL = "successful"
    FAILED = "failed"


class LayerParameter(BaseModel):
    """Parameter for layer configuration"""
    name: str
    value: Any


class LayerConfig(BaseModel):
    """Configuration for adding a layer"""
    layer_type: LayerType
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    is_unusable: bool = Field(True, description="If True, area is marked as unusable. If False, as unavailable.")


class LayerAddRequest(BaseModel):
    """Request to add a layer"""
    layers: List[LayerConfig]


class LayerInfo(BaseModel):
    """Information about a layer"""
    layer_type: str
    name: str
    description: str
    is_unusable: bool
    parameters: Dict[str, Any]
    area_ha: Optional[float] = None
    feature_count: Optional[int] = None
    status: Optional[str] = None  # in_progress, successful, failed
    details: Optional[str] = None  # Current processing step or error message


class LayerUploadResponse(BaseModel):
    """Response after adding layers"""
    project_id: str
    message: str
    layers_added: List[LayerInfo]


class AvailableLayersResponse(BaseModel):
    """Response with available layer configurations"""
    layers: Dict[str, Any]


# ============ Clustering Models ============

class ClusteringMethod(str, Enum):
    """Clustering method options"""
    DBSCAN = "dbscan"


class ClusteringRequest(BaseModel):
    """Request for clustering khasras into parcels"""
    distance_threshold: int = Field(
        default=25,
        description="Maximum distance between khasras to be in the same cluster (meters)"
    )
    min_samples: int = Field(
        default=2,
        ge=1,
        description="Minimum number of khasras to form a cluster"
    )
    min_parcel_area_ha: float = Field(
        default=100.0,
        ge=0,
        description="Minimum total usable area (hectares) for a parcel to be included"
    )
    method: ClusteringMethod = ClusteringMethod.DBSCAN


class ParcelStats(BaseModel):
    """Statistics for a parcel (cluster of khasras)"""
    parcel_id: str
    khasra_count: int
    khasra_ids: List[str]
    original_area_ha: float
    usable_area_ha: float
    usable_area_percent: float
    usable_available_area_ha: float
    usable_available_area_percent: float
    unusable_area_ha: float
    building_count: int
    
    # Layer-specific unusable areas
    settlements_area_ha: Optional[float] = None
    water_area_ha: Optional[float] = None
    slope_area_ha: Optional[float] = None
    cropland_area_ha: Optional[float] = None
    buildings_area_ha: Optional[float] = None


class ClusteringResponse(BaseModel):
    """Response after clustering"""
    project_id: str
    message: str
    distance_threshold: int
    total_parcels: int
    clustered_khasras: int
    unclustered_khasras: int
    parcels: List[ParcelStats]


# ============ Settlement Layer Models ============

class SettlementLayerRequest(BaseModel):
    """Request to generate settlement layers from VIDA rooftop data"""
    building_buffer: int = Field(
        default=10,
        ge=0,
        description="Buffer distance around buildings in meters"
    )
    settlement_eps: int = Field(
        default=50,
        description="DBSCAN epsilon - max distance between buildings in a settlement (meters)"
    )
    min_buildings: int = Field(
        default=5,
        ge=2,
        description="Minimum number of buildings to form a settlement"
    )


# ============ Slopes Layer Models ============

class SlopesLayerRequest(BaseModel):
    """Request to generate slopes layer from NASA ALOS PALSAR DEM data"""
    include_north_slopes: bool = Field(
        default=True,
        description="Include north-facing steep slopes (NE to NW, 45-135°, >7° angle)"
    )
    include_other_slopes: bool = Field(
        default=True,
        description="Include other steep slopes (south/east/west facing, >10° angle)"
    )
    north_min_angle: float = Field(
        default=7.0,
        ge=0,
        le=90,
        description="Minimum slope angle for north-facing slopes (degrees)"
    )
    other_min_angle: float = Field(
        default=10.0,
        ge=0,
        le=90,
        description="Minimum slope angle for other-facing slopes (degrees)"
    )


# ============ Calculate Areas Models ============

class CalculateAreasResponse(BaseModel):
    """Response after calculating usable areas"""
    project_id: str
    message: str
    khasra_count: int
    total_original_area_ha: float
    total_usable_area_ha: float
    total_usable_available_area_ha: float


# ============ Statistics Models ============

class KhasraStatsInfo(BaseModel):
    """Khasra statistics info"""
    count: int
    total_area_ha: Optional[float] = None


class AreaStatsInfo(BaseModel):
    """Area statistics after layer processing"""
    original_area_ha: float
    usable_area_ha: float
    usable_area_percent: float
    usable_available_area_ha: float
    usable_available_area_percent: float


class ParcelStatsInfo(BaseModel):
    """Parcel statistics info"""
    total_count: int
    clustered_count: int
    unclustered_count: int


class LayerStatsInfo(BaseModel):
    """Layer statistics info"""
    name: str
    is_unusable: bool
    feature_count: int
    total_area_ha: Optional[float] = None


class ProjectStatsResponse(BaseModel):
    """Response with project statistics"""
    project_id: str
    project_name: str
    location: str
    status: str
    khasras: Optional[KhasraStatsInfo] = None
    areas: Optional[AreaStatsInfo] = None
    parcels: Optional[ParcelStatsInfo] = None
    layers: List[LayerStatsInfo] = []


# ============ Export/Download Models ============

class ExportFormat(str, Enum):
    """Export file formats"""
    GEOJSON = "geojson"
    KML = "kml"
    SHAPEFILE = "shapefile"
    PARQUET = "parquet"
    CSV = "csv"
    EXCEL = "excel"


class ExportRequest(BaseModel):
    """Request to export data - always exports all project data (khasras, parcels, layers)"""
    format: ExportFormat = ExportFormat.GEOJSON


class ExportResponse(BaseModel):
    """Response with download information"""
    project_id: str
    download_url: str
    filename: str
    format: ExportFormat
    file_size_bytes: Optional[int] = None
    expires_at: Optional[datetime] = None


# ============ Error Models ============

class ErrorResponse(BaseModel):
    """Error response model"""
    detail: str
    error_code: Optional[str] = None


# ============ Health Check Models ============

class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str = "healthy"
    version: str
    timestamp: datetime
