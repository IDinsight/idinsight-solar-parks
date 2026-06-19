"use client"

import { useState, useEffect } from "react"
import { useParams, useRouter } from "next/navigation"
import MapContainer from "@/components/map-container"
import * as api from "@/lib/api/services"
import type { Project } from "@/lib/api/types"
import { ArrowLeft, Loader2, Globe, Lock, LogIn, Check, Copy } from "lucide-react"
import { getWorkflowPageForProject } from "@/lib/utils/project-navigation"
import { toast } from "sonner"

export default function FullScreenMapPage() {
    const params = useParams()
    const router = useRouter()
    const projectId = params.project_id as string

    const [project, setProject] = useState<Project | null>(null)
    const [khasrasData, setKhasrasData] = useState<any>(null)
    const [parcelsData, setParcelsData] = useState<any>(null)
    const [layersData, setLayersData] = useState<Record<string, any>>({})
    const [isLoading, setIsLoading] = useState(true)
    const [authedView, setAuthedView] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [isPrivate, setIsPrivate] = useState(false)
    const [isTogglingVisibility, setIsTogglingVisibility] = useState(false)
    const [copied, setCopied] = useState(false)

    useEffect(() => {
        if (!projectId) return

        const loadData = async () => {
            try {
                setIsLoading(true)
                setError(null)
                setIsPrivate(false)

                // Try authenticated first if user appears logged in
                const hasToken = typeof window !== 'undefined' && !!localStorage.getItem('access_token')

                if (hasToken) {
                    try {
                        const projectData = await api.getProject(projectId)
                        setProject(projectData)
                        setAuthedView(true)

                        try {
                            const khasrasSummary = await api.getKhasrasSummary(projectId)
                            if (khasrasSummary.geojson) setKhasrasData(khasrasSummary.geojson)
                        } catch (e) { console.warn("No khasras data available") }

                        try {
                            const parcels = await api.getParcelsGeoJSON(projectId)
                            setParcelsData(parcels)
                        } catch (e) { console.warn("No parcels data available") }

                        try {
                            const layers = await api.getProjectLayersGeoJSON(projectId)
                            setLayersData(layers)
                        } catch (e) { console.warn("No layers data available") }

                        return
                    } catch (e: any) {
                        // Auth failed — fall through to public path
                        // Prevent the 401 interceptor redirect by clearing token
                        localStorage.removeItem('access_token')
                        localStorage.removeItem('access_token_expiry')
                    }
                }

                // Public path (no auth)
                try {
                    const projectData = await api.getPublicProject(projectId)
                    setProject(projectData)
                    setAuthedView(false)

                    try {
                        const khasrasSummary = await api.getPublicKhasrasSummary(projectId)
                        if (khasrasSummary.geojson) setKhasrasData(khasrasSummary.geojson)
                    } catch (e) { console.warn("No khasras data available") }

                    try {
                        const parcels = await api.getPublicParcelsGeoJSON(projectId)
                        setParcelsData(parcels)
                    } catch (e) { console.warn("No parcels data available") }

                    try {
                        const layers = await api.getPublicLayersGeoJSON(projectId)
                        setLayersData(layers)
                    } catch (e) { console.warn("No layers data available") }

                } catch (err: any) {
                    if (err?.response?.status === 404) {
                        setIsPrivate(true)
                    } else {
                        setError(err instanceof Error ? err.message : "Failed to load map data")
                    }
                }
            } finally {
                setIsLoading(false)
            }
        }

        loadData()
    }, [projectId])

    const handleToggleVisibility = async () => {
        if (!project) return
        setIsTogglingVisibility(true)
        try {
            const updated = await api.updateProjectVisibility(projectId, !project.is_public)
            setProject(updated)
            if (updated.is_public) {
                const mapUrl = `${window.location.origin}/map/${projectId}`
                toast.success("Map is now public", {
                    description: mapUrl,
                    action: {
                        label: "Copy Link",
                        onClick: () => navigator.clipboard.writeText(mapUrl),
                    },
                })
            } else {
                toast.success("Map is now private")
            }
        } catch (err) {
            toast.error("Failed to update visibility")
        } finally {
            setIsTogglingVisibility(false)
        }
    }

    const handleCopyLink = async () => {
        const mapUrl = `${window.location.origin}/map/${projectId}`
        await navigator.clipboard.writeText(mapUrl)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
    }

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

    if (isPrivate) {
        return (
            <div className="h-screen w-screen flex items-center justify-center bg-slate-50">
                <div className="text-center max-w-md">
                    <Lock className="h-12 w-12 text-slate-400 mx-auto mb-4" />
                    <h2 className="text-xl font-semibold text-slate-900 mb-2">This map is private</h2>
                    <p className="text-slate-600 mb-6">You need to log in to view this map, or ask the owner to make it public.</p>
                    <button
                        onClick={() => router.push("/login")}
                        className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                    >
                        <LogIn className="h-4 w-4" />
                        Log in
                    </button>
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

    let mapCenter: [number, number] = [23.0, 77.0]
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
                    {authedView ? (
                        <button
                            onClick={() => {
                                const targetPage = getWorkflowPageForProject(project)
                                router.push(`/workflow/${projectId}?page=${targetPage}`)
                            }}
                            className="flex items-center gap-2 px-3 py-2 text-sm text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
                        >
                            <ArrowLeft className="h-4 w-4" />
                            Go to Project
                        </button>
                    ) : (
                        <button
                            onClick={() => router.push("/login")}
                            className="flex items-center gap-2 px-3 py-2 text-sm text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
                        >
                            <LogIn className="h-4 w-4 text-blue-600" />
                            Login to Edit
                        </button>
                    )}
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

                {/* Share controls (authenticated only) */}
                {authedView && (
                    <div className="flex items-center gap-3 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
                        <div className="flex items-center gap-1.5 text-sm text-slate-600">
                            {project?.is_public ? (
                                <Globe className="h-4 w-4 text-green-600" />
                            ) : (
                                <Lock className="h-4 w-4 text-slate-400" />
                            )}
                            <span>Currently {project?.is_public ? "Public" : "Private"}</span>
                        </div>
                        <button
                            onClick={handleToggleVisibility}
                            disabled={isTogglingVisibility}
                            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                                project?.is_public
                                    ? "bg-slate-200 text-slate-700 hover:bg-slate-300"
                                    : "bg-blue-600 text-white hover:bg-blue-700"
                            }`}
                        >
                            {isTogglingVisibility ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            ) : null}
                            {project?.is_public ? "Make Private" : "Make Public"}
                        </button>
                        {project?.is_public && (
                            <button
                                onClick={handleCopyLink}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md bg-slate-200 text-slate-700 hover:bg-slate-300 transition-colors"
                            >
                                {copied ? <Check className="h-3.5 w-3.5 text-green-600" /> : <Copy className="h-3.5 w-3.5" />}
                                {copied ? "Copied!" : "Copy Link"}
                            </button>
                        )}
                    </div>
                )}
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
