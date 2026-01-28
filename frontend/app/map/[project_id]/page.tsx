"use client"

import { useState, useEffect } from "react"
import { useParams, useRouter } from "next/navigation"
import MapContainer from "@/components/map-container"
import * as api from "@/lib/api/services"
import type { Project } from "@/lib/api/types"
import { ArrowLeft, Loader2 } from "lucide-react"

export default function FullScreenMapPage() {
    const params = useParams()
    const router = useRouter()
    const projectId = params.project_id as string

    const [project, setProject] = useState<Project | null>(null)
    const [khasrasData, setKhasrasData] = useState<any>(null)
    const [parcelsData, setParcelsData] = useState<any>(null)
    const [layersData, setLayersData] = useState<Record<string, any>>({})
    const [isLoading, setIsLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        if (!projectId) return

        const loadData = async () => {
            try {
                setIsLoading(true)
                setError(null)

                // Load project details
                const projectData = await api.getProject(projectId)
                setProject(projectData)

                // Load khasras
                try {
                    const khasrasSummary = await api.getKhasrasSummary(projectId)
                    if (khasrasSummary.geojson) {
                        setKhasrasData(khasrasSummary.geojson)
                    }
                } catch (e) {
                    console.warn("No khasras data available")
                }

                // Load parcels
                try {
                    const parcels = await api.getParcelsGeoJSON(projectId)
                    setParcelsData(parcels)
                } catch (e) {
                    console.warn("No parcels data available")
                }

                // Load layers
                try {
                    const layers = await api.getProjectLayersGeoJSON(projectId)
                    setLayersData(layers)
                } catch (e) {
                    console.warn("No layers data available")
                }

            } catch (err) {
                console.error("Error loading map data:", err)
                setError(err instanceof Error ? err.message : "Failed to load map data")
            } finally {
                setIsLoading(false)
            }
        }

        loadData()
    }, [projectId])

    if (isLoading) {
        return (
            <div className="h-screen w-screen flex items-center justify-center bg-slate-50">
                <div className="text-center">
                    <Loader2 className="h-8 w-8 animate-spin text-blue-600 mx-auto mb-3" />
                    <p className="text-slate-600">Loading map data...</p>
                </div>
            </div>
        )
    }

    if (error) {
        return (
            <div className="h-screen w-screen flex items-center justify-center bg-slate-50">
                <div className="text-center max-w-md">
                    <div className="text-red-500 text-5xl mb-4">⚠️</div>
                    <h2 className="text-xl font-semibold text-slate-900 mb-2">Failed to Load Map</h2>
                    <p className="text-slate-600 mb-4">{error}</p>
                    <button
                        onClick={() => router.push("/dashboard")}
                        className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                    >
                        Back to Dashboard
                    </button>
                </div>
            </div>
        )
    }

    // Calculate map center from khasras or parcels
    let mapCenter: [number, number] = [23.0, 77.0] // Default center for India
    let mapZoom = 10

    if (khasrasData?.features?.[0]?.geometry?.coordinates) {
        const coords = khasrasData.features[0].geometry.coordinates[0][0][0]
        if (coords && Array.isArray(coords) && coords.length >= 2) {
            mapCenter = [coords[1], coords[0]]
            mapZoom = 13
        }
    }

    return (
        <div className="h-screen w-screen flex flex-col">
            {/* Header Bar */}
            <div className="bg-white border-b border-slate-200 px-4 py-3 flex items-center justify-between shadow-sm">
                <div className="flex items-center gap-3">
                    <button
                        onClick={() => router.push(`/workflow/${projectId}`)}
                        className="flex items-center gap-2 px-3 py-2 text-sm text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
                    >
                        <ArrowLeft className="h-4 w-4" />
                        Go to Project
                    </button>
                    <div className="h-6 w-px bg-slate-300" />
                    <div>
                        <h1 className="text-lg font-semibold text-slate-900">
                            {project?.name || "Project Map"}
                        </h1>
                        {project?.location && (
                            <p className="text-xs text-slate-500">{project.location}</p>
                        )}
                    </div>
                </div>
            </div>

            {/* Full Screen Map */}
            <div className="flex-1 relative">
                <MapContainer
                    projectId={projectId}
                    data={khasrasData}
                    center={mapCenter}
                    zoom={mapZoom}
                    parcelsData={parcelsData}
                    layersData={layersData}
                />
            </div>
        </div>
    )
}
