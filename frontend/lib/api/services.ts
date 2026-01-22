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

// ============ Layers ============

export async function getAvailableLayers(): Promise<AvailableLayersResponse> {
    const response = await apiClient.get<AvailableLayersResponse>('/layers/available')
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
        `/projects/${projectId}/layers`,
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

export async function listProjectLayers(projectId: string): Promise<LayerInfo[]> {
    const response = await apiClient.get<LayerInfo[]>(`/projects/${projectId}/layers`)
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

// ============ Export ============

export async function exportData(projectId: string, request: ExportRequest): Promise<Blob> {
    const response = await apiClient.post(`/projects/${projectId}/export`, request, {
        responseType: 'blob',
    })
    return response.data
}
