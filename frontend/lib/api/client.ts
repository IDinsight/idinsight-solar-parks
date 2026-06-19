/**
 * Axios API client with authentication support
 */
import axios, { AxiosError, InternalAxiosRequestConfig } from 'axios'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export const apiClient = axios.create({
    baseURL: API_BASE_URL,
    timeout: 120000,  // 120 second timeout
    headers: {
        'Content-Type': 'application/json',
    },
})

// Request interceptor to add auth token
apiClient.interceptors.request.use(
    (config: InternalAxiosRequestConfig) => {
        const token = localStorage.getItem('access_token')
        if (token && config.headers) {
            config.headers.Authorization = `Bearer ${token}`
        }
        return config
    },
    (error: AxiosError) => Promise.reject(error)
)

// Response interceptor for error handling
apiClient.interceptors.response.use(
    (response) => response,
    async (error: AxiosError) => {
        if (error.response?.status === 401) {
            localStorage.removeItem('access_token')
            if (typeof window !== 'undefined' && !window.location.pathname.startsWith('/map/')) {
                window.location.href = '/login'
            }
        }
        return Promise.reject(error)
    }
)

export default apiClient

export const publicApiClient = axios.create({
    baseURL: API_BASE_URL,
    timeout: 120000,
    headers: {
        'Content-Type': 'application/json',
    },
})
