/**
 * API service functions
 */
import apiClient from './client'
import type {
    TokenResponse,
    User,
    ProjectCreate,
    Project,
    ProjectListResponse,
    KhasraUploadResponse,
    KhasraSummary,
    LayerUploadResponse,
    LayerInfo,
    CalculateAreasResponse,
    ClusteringRequest,
    ClusteringResponse,
    ExportRequest,
    AvailableLayersResponse,
    SettlementLayerRequest,
} from './types'

// ============ Authentication ============

export async function login(username: string, password: string): Promise<TokenResponse> {
    const formData = new FormData()
    formData.append('username', username)
    formData.append('password', password)

    const response = await apiClient.post<TokenResponse>('/auth/token', formData, {
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
    })
    return response.data
}

export async function getCurrentUser(): Promise<User> {
    const response = await apiClient.get<User>('/auth/me')
    return response.data
}

// ============ Projects ============

export async function createProject(data: ProjectCreate): Promise<Project> {
    const response = await apiClient.post<Project>('/projects', data)
    return response.data
}

export async function listProjects(): Promise<ProjectListResponse> {
    const response = await apiClient.get<ProjectListResponse>('/projects')
    return response.data
}

export async function getProject(projectId: string): Promise<Project> {
    const response = await apiClient.get<Project>(`/projects/${projectId}`)
    return response.data
}

export async function deleteProject(projectId: string): Promise<void> {
    await apiClient.delete(`/projects/${projectId}`)
}

// ============ Khasras ============

export async function getKhasrasSummary(projectId: string): Promise<KhasraSummary> {
    const response = await apiClient.get<KhasraSummary>(`/projects/${projectId}/khasras`)
    return response.data
}

export async function uploadKhasras(
    projectId: string,
    file: File,
    idColumn?: string
): Promise<KhasraUploadResponse> {
    const formData = new FormData()
    formData.append('file', file)
    if (idColumn) {
        formData.append('id_column', idColumn)
    }

    const response = await apiClient.post<KhasraUploadResponse>(
        `/projects/${projectId}/khasras`,
        formData,
        {
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }
    )
    return response.data
}

export async function deleteKhasras(projectId: string): Promise<void> {
    await apiClient.delete(`/projects/${projectId}/khasras`)
}


// ============ Layers ============

export async function getAvailableLayers(): Promise<AvailableLayersResponse> {
    const response = await apiClient.get<AvailableLayersResponse>('/layers/available_builtin')
    return response.data
}

export async function uploadCustomLayer(
    projectId: string,
    file: File,
    layerName: string,
    isUnusable: boolean
): Promise<LayerUploadResponse> {
    const formData = new FormData()
    formData.append('file', file)
    formData.append('layer_name', layerName)
    formData.append('is_unusable', String(isUnusable))

    const response = await apiClient.post<LayerUploadResponse>(
        `/projects/${projectId}/layers/custom_upload`,
        formData,
        {
            headers: {
                'Content-Type': 'multipart/form-data',
            },
        }
    )
    return response.data
}

export async function generateSettlementLayer(
    projectId: string,
    request?: SettlementLayerRequest
): Promise<LayerUploadResponse> {
    const response = await apiClient.post<LayerUploadResponse>(
        `/projects/${projectId}/layers/settlements`,
        request || {}
    )
    return response.data
}

export async function generateCroplandLayer(projectId: string): Promise<LayerUploadResponse> {
    const response = await apiClient.post<LayerUploadResponse>(
        `/projects/${projectId}/layers/cropland`,
        {}
    )
    return response.data
}

export async function generateWaterLayer(projectId: string): Promise<LayerUploadResponse> {
    const response = await apiClient.post<LayerUploadResponse>(
        `/projects/${projectId}/layers/water`,
        {}
    )
    return response.data
}

export async function listProjectLayers(projectId: string): Promise<LayerInfo[]> {
    const response = await apiClient.get<LayerInfo[]>(`/projects/${projectId}/layers`)
    return response.data
}

export async function deleteLayer(projectId: string, layerName: string): Promise<void> {
    await apiClient.delete(`/projects/${projectId}/layers/${encodeURIComponent(layerName)}`)
}

export async function getProjectLayersGeoJSON(projectId: string): Promise<Record<string, any>> {
    const response = await apiClient.get<Record<string, any>>(`/projects/${projectId}/layers/geojson`)
    return response.data
}


export async function calculateAreas(projectId: string): Promise<CalculateAreasResponse> {
    const response = await apiClient.post<CalculateAreasResponse>(
        `/projects/${projectId}/calculate-areas`
    )
    return response.data
}

// ============ Clustering ============

export async function clusterKhasras(
    projectId: string,
    request: ClusteringRequest
): Promise<ClusteringResponse> {
    const response = await apiClient.post<ClusteringResponse>(
        `/projects/${projectId}/cluster`,
        request
    )
    return response.data
}

export async function getParcelsGeoJSON(projectId: string): Promise<any> {
    const response = await apiClient.get(`/projects/${projectId}/parcels/geojson`)
    return response.data
}

export async function deleteParcels(projectId: string): Promise<void> {
    await apiClient.delete(`/projects/${projectId}/parcels`)
}

// ============ Export ============

export async function exportData(projectId: string, request: ExportRequest): Promise<Blob> {
    const response = await apiClient.post(`/projects/${projectId}/export`, request, {
        responseType: 'blob',
    })
    return response.data
}
