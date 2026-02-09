/**
 * API type definitions matching the backend models
 */

export interface User {
    username: string
    disabled?: boolean
}

export interface LoginRequest {
    username: string
    password: string
}

export interface TokenResponse {
    access_token: string
    token_type: string
}

export interface ProjectStatus {
    CREATED: 'created'
    KHASRAS_UPLOADED: 'khasras_uploaded'
    LAYERS_ADDED: 'layers_added'
    CLUSTERED: 'clustered'
    COMPLETED: 'completed'
    ERROR: 'error'
}

export interface Project {
    id: string
    name: string
    location: string
    description?: string
    status: string
    created_at: string
    updated_at: string
    khasra_count?: number
    total_area_ha?: number
    layers_added: string[]
}

export interface ProjectCreate {
    name: string
    location: string
    description?: string
}

export interface ProjectListResponse {
    projects: Project[]
    total: number
}

export interface KhasraUploadResponse {
    project_id: string
    message: string
    khasra_count: number
    total_area_ha: number
    bounds: {
        minx: number
        miny: number
        maxx: number
        maxy: number
    }
    crs: string
}

export interface KhasraSummary {
    exists: boolean
    count?: number
    total_area_ha?: number
    uploaded_at?: string
    geojson?: {
        type: string
        features: any[]
    }
    bounds?: {
        minx: number
        miny: number
        maxx: number
        maxy: number
    }
}


export interface LayerInfo {
    layer_type: string
    name: string
    description: string
    is_unusable: boolean
    parameters: Record<string, any>
    area_ha?: number
    feature_count?: number
    status: string
    details: string
}

export interface LayerUploadResponse {
    project_id: string
    message: string
    layers_added: LayerInfo[]
}

export interface CalculateAreasResponse {
    project_id: string
    message: string
    khasra_count: number
    total_original_area_ha: number
    total_usable_area_ha: number
    total_usable_available_area_ha: number
}

export interface ClusteringRequest {
    distance_threshold: number
    min_samples?: number
    min_parcel_area_ha?: number
}

export interface ParcelInfo {
    parcel_id: string
    khasra_count: number
    original_area_ha: number
    usable_area_ha: number
    usable_available_area_ha: number
}

export interface ClusteringResponse {
    project_id: string
    message: string
    distance_threshold: number
    total_parcels: number
    clustered_khasras: number
    unclustered_khasras: number
    parcels: ParcelInfo[]
}

export enum ExportFormat {
    GEOJSON = 'geojson',
    KML = 'kml',
    SHAPEFILE = 'shapefile',
    PARQUET = 'parquet',
    CSV = 'csv',
    EXCEL = 'excel',
}

export interface ExportRequest {
    format: ExportFormat
    include_statistics?: boolean
}

export interface AvailableLayer {
    name: string
    description: string
    required: boolean
    parameters: Record<string, any>
}

export interface AvailableLayersResponse {
    layers: Record<string, AvailableLayer>
}

export interface SettlementLayerRequest {
    building_buffer?: number
    settlement_eps?: number
    min_buildings?: number
}
