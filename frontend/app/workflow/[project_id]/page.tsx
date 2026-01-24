"use client"

import { useState, useEffect } from "react"
import { useRouter, useParams } from "next/navigation"
import { ProtectedRoute } from "@/components/protected-route"
import { useProjectStore } from "@/lib/stores/project"
import UploadSection from "@/components/upload-section"
import ClusteringSection from "@/components/clustering-section"
import MapContainer from "@/components/map-container"
import * as api from "@/lib/api/services"
import { ExportFormat } from "@/lib/api/types"
import { ChevronLeft, ChevronRight, ArrowLeft, AlertCircle, Map, Copy, ExternalLink, Check, FileSpreadsheet } from "lucide-react"

/**
 * Animated ellipsis component for loading states
 * Cycles through . .. ... every 500ms
 */
function AnimatedEllipsis() {
    const [dots, setDots] = useState(".")

    useEffect(() => {
        const interval = setInterval(() => {
            setDots(prev => {
                if (prev === ".") return ".."
                if (prev === "..") return "..."
                return "."
            })
        }, 500)

        return () => clearInterval(interval)
    }, [])

    return <span className="inline-block w-4">{dots}</span>
}

export default function WorkflowPage() {
    return (
        <ProtectedRoute>
            <WorkflowContent />
        </ProtectedRoute>
    )
}

function WorkflowContent() {
    const router = useRouter()
    const params = useParams()
    const projectId = params.project_id as string
    const { currentProject, setCurrentProject, updateProject } = useProjectStore()

    // Workflow state
    const [currentPage, setCurrentPage] = useState(1)
    const [isProcessing, setIsProcessing] = useState(false)
    const [error, setError] = useState<string | null>(null)

    // Step 1: Khasra upload state
    const [khasraFile, setKhasraFile] = useState<File | null>(null)
    const [khasraGeoJSON, setKhasraGeoJSON] = useState<any>(null)
    const [khasraIdColumn, setKhasraIdColumn] = useState<string>("")
    const [isKhasraUploadComplete, setIsKhasraUploadComplete] = useState(false)

    // Step 2: Constraint layers state
    const [constraintLayersGeoJSON, setConstraintLayersGeoJSON] = useState<Record<string, any> | null>(null)
    const [allProjectLayers, setAllProjectLayers] = useState<any[]>([])
    const [activeProcessingLayer, setActiveProcessingLayer] = useState<string | null>(null)

    // Settlement layer configuration
    const [settlementLayerParams, setSettlementLayerParams] = useState({
        building_buffer: 10,
        settlement_eps: 50,
        min_buildings: 5,
    })
    const [settlementLayerStatus, setSettlementLayerStatus] = useState<any>(null)

    // Step 3: Clustering state
    const [isClusteringComplete, setIsClusteringComplete] = useState(false)
    const [clusteringResult, setClusteringResult] = useState<any>(null)
    const [clusteringParams, setClusteringParams] = useState<{ distance_threshold: number, min_samples: number } | null>(null)
    const [parcelGeoJSON, setParcelGeoJSON] = useState<any>(null)

    // Map visualization state
    const [mapCenter, setMapCenter] = useState<[number, number]>([20, 0])
    const [mapZoom, setMapZoom] = useState(5)

    // Map link state
    const [mapLinkCopied, setMapLinkCopied] = useState(false)

    /**
     * Fetch and update constraint layers whenever we're on page 2 or later
     * Checks settlement layer status and loads GeoJSON for visualization
     */
    useEffect(() => {
        const fetchConstraintLayers = async () => {
            if (currentProject?.id && currentPage >= 2) {
                try {
                    const layers = await api.listProjectLayers(currentProject.id)
                    setAllProjectLayers(layers)

                    // Update settlement layer status
                    const settlementsLayer = layers.find((l: any) => l.name === "Settlements")
                    const isolatedBuildingsLayer = layers.find((l: any) => l.name === "Isolated Buildings")
                    if (settlementsLayer || isolatedBuildingsLayer) {
                        setSettlementLayerStatus({
                            settlements: settlementsLayer,
                            isolated: isolatedBuildingsLayer,
                            processing: settlementsLayer?.status === "in_progress" || isolatedBuildingsLayer?.status === "in_progress"
                        })
                    }

                    const layersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
                    setConstraintLayersGeoJSON(layersGeoJSON)
                } catch (error) {
                    console.error("Error fetching constraint layers:", error)
                }
            }
        }
        fetchConstraintLayers()
    }, [currentProject?.id, currentPage])

    /**
     * Poll for layer processing status updates every 5 seconds
     * Stops polling when processing is complete and refreshes all data
     */
    useEffect(() => {
        if (!currentProject?.id || !activeProcessingLayer) return

        const pollInterval = setInterval(async () => {
            try {
                const layers = await api.listProjectLayers(currentProject.id)
                setAllProjectLayers(layers)

                if (activeProcessingLayer === "Settlements") {
                    const settlementsLayer = layers.find((l: any) => l.name === "Settlements")
                    const isolatedBuildingsLayer = layers.find((l: any) => l.name === "Isolated Buildings")

                    setSettlementLayerStatus({
                        settlements: settlementsLayer,
                        isolated: isolatedBuildingsLayer,
                        processing: settlementsLayer?.status === "in_progress" || isolatedBuildingsLayer?.status === "in_progress"
                    })

                    // Stop polling when both layers are complete
                    const isProcessingComplete = settlementsLayer?.status !== "in_progress" && isolatedBuildingsLayer?.status !== "in_progress"
                    if (isProcessingComplete) {
                        setActiveProcessingLayer(null)

                        // Refresh all data after processing completes
                        const layersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
                        setConstraintLayersGeoJSON(layersGeoJSON)

                        const project = await api.getProject(currentProject.id)
                        updateProject(project)
                    }
                }
            } catch (error) {
                console.error("Error polling layer status:", error)
            }
        }, 5000) // Poll every 5 seconds

        return () => clearInterval(pollInterval)
    }, [currentProject?.id, activeProcessingLayer, updateProject])


    /**
     * Load initial project state on mount and determine starting page
     * Loads the project from the URL parameter
     */
    useEffect(() => {
        if (!projectId) {
            router.push('/dashboard')
            return
        }
        
        // Load the project from URL parameter if not current or different
        if (!currentProject || currentProject.id !== projectId) {
            const loadProjectFromUrl = async () => {
                try {
                    const project = await api.getProject(projectId)
                    setCurrentProject(project)
                } catch (error) {
                    console.error('Failed to load project from URL:', error)
                    router.push('/dashboard')
                }
            }
            loadProjectFromUrl()
            return
        }

        const loadInitialProjectState = async () => {
            try {
                const project = await api.getProject(currentProject.id)
                updateProject(project)

                // Determine starting page based on project completion status
                if (project.khasra_count && project.khasra_count > 0) {
                    setIsKhasraUploadComplete(true)

                    // Load khasra GeoJSON data for clustering
                    try {
                        const khasraSummary = await api.getKhasrasSummary(currentProject.id)
                        if (khasraSummary.geojson) {
                            setKhasraGeoJSON(khasraSummary.geojson)

                            // Center map on khasra bounds if available
                            if (khasraSummary.bounds) {
                                const centerLng = (khasraSummary.bounds.minx + khasraSummary.bounds.maxx) / 2
                                const centerLat = (khasraSummary.bounds.miny + khasraSummary.bounds.maxy) / 2
                                setMapCenter([centerLat, centerLng])
                                setMapZoom(10)
                            }
                        }
                    } catch (error) {
                        console.error('Failed to load khasra GeoJSON:', error)
                    }

                    // Determine which page to start on
                    const hasLayers = project.layers_added && project.layers_added.length > 0
                    const isClustered = project.status === 'clustered' || project.status === 'completed'

                    if (hasLayers && isClustered) {
                        // Project is fully complete - go to export page
                        setIsClusteringComplete(true)

                        try {
                            const parcelsGeoJSON = await api.getParcelsGeoJSON(currentProject.id)
                            setParcelGeoJSON(parcelsGeoJSON)

                            if (parcelsGeoJSON?.clusteringParams) {
                                setClusteringParams({
                                    distance_threshold: parcelsGeoJSON.clusteringParams.distance_threshold,
                                    min_samples: parcelsGeoJSON.clusteringParams.min_samples
                                })
                                setClusteringResult({
                                    total_parcels: parcelsGeoJSON.clusteringParams.total_parcels,
                                    clustered_khasras: parcelsGeoJSON.clusteringParams.clustered_khasras,
                                    unclustered_khasras: parcelsGeoJSON.clusteringParams.unclustered_khasras
                                })
                            }
                        } catch (error) {
                            console.error('Failed to load parcel GeoJSON:', error)
                        }

                        setCurrentPage(4)
                    } else if (hasLayers) {
                        // Layers added but not clustered yet - go to layers page
                        setCurrentPage(2)
                    } else {
                        // Only khasras added - stay on upload page
                        setCurrentPage(1)
                    }
                } else {
                    // No khasras - stay on upload page
                    setCurrentPage(1)
                }
            } catch (error) {
                console.error('Failed to load project:', error)
            }
        }

        loadInitialProjectState()
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []) // Only run once on mount

    /**
     * Handle khasra file upload and process boundaries
     * Centers map on uploaded data and moves to layer selection page
     */
    const handleKhasraUpload = async (file: File, geoJSONData: any, uniqueIdColumn: string) => {
        if (!currentProject) return

        setIsProcessing(true)
        setError(null)

        try {
            const uploadResponse = await api.uploadKhasras(currentProject.id, file, uniqueIdColumn)

            setKhasraFile(file)
            setKhasraGeoJSON(geoJSONData)
            setKhasraIdColumn(uniqueIdColumn)
            setIsKhasraUploadComplete(true)

            // Center map on uploaded data using response bounds
            if (uploadResponse.bounds) {
                const centerLng = (uploadResponse.bounds.minx + uploadResponse.bounds.maxx) / 2
                const centerLat = (uploadResponse.bounds.miny + uploadResponse.bounds.maxy) / 2
                setMapCenter([centerLat, centerLng])
                setMapZoom(10)
            }

            // Refresh project data
            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)

            // Don't auto-advance - wait for user to click Next
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to upload khasras')
            console.error("Error uploading khasras:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    /**
     * Initiate settlement layer generation with configured parameters
     * Sets initial processing state and starts background job
     * Polling effect will track progress automatically
     */
    const handleGenerateSettlementLayers = async () => {
        if (!currentProject) return

        setActiveProcessingLayer("Settlements")
        setError(null)

        // Set initial processing status for immediate UI feedback
        setSettlementLayerStatus({
            settlements: { status: "in_progress", details: "Queued for processing..." },
            isolated: { status: "in_progress", details: "Queued for processing..." },
            processing: true
        })

        try {
            await api.generateSettlementLayer(currentProject.id, settlementLayerParams)
            // Polling effect will automatically track progress
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to generate settlement layers')
            console.error("Error generating settlement layers:", error)
            setActiveProcessingLayer(null)
            setSettlementLayerStatus(null)
        }
    }

    /**
     * Delete both settlement layers (Settlements and Isolated Buildings)
     * Requires user confirmation and refreshes all data after deletion
     */
    const handleDeleteSettlementLayers = async () => {
        if (!currentProject) return

        const confirmed = confirm(
            "Are you sure you want to delete the Settlement layers? " +
            "This will remove both Settlements and Isolated Buildings layers."
        )
        if (!confirmed) return

        setIsProcessing(true)
        setError(null)

        try {
            // Delete both settlement sub-layers
            await api.deleteLayer(currentProject.id, "Settlements")
            await api.deleteLayer(currentProject.id, "Isolated Buildings")

            setSettlementLayerStatus(null)

            // Refresh all layers and GeoJSON data
            const updatedLayers = await api.listProjectLayers(currentProject.id)
            setAllProjectLayers(updatedLayers)

            const updatedLayersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
            setConstraintLayersGeoJSON(updatedLayersGeoJSON)

            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to delete layers')
            console.error("Error deleting settlement layers:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    /**
     * Run DBSCAN clustering to group khasras into parcels
     * Uses distance threshold and minimum samples parameters
     */
    const handleRunClustering = async (distanceThreshold: number, minSamples: number = 2) => {
        if (!currentProject) return

        setIsProcessing(true)
        setError(null)

        try {
            const result = await api.clusterKhasras(currentProject.id, {
                distance_threshold: distanceThreshold,
                min_samples: minSamples,
            })

            setClusteringResult(result)
            setIsClusteringComplete(true)
            setClusteringParams({
                distance_threshold: distanceThreshold,
                min_samples: minSamples
            })

            // Fetch parcel geometries for map display
            const parcelsGeoJSON = await api.getParcelsGeoJSON(currentProject.id)
            setParcelGeoJSON(parcelsGeoJSON)

            // Extract clustering params from response (should match what we just sent)
            if (parcelsGeoJSON?.clusteringParams) {
                setClusteringParams({
                    distance_threshold: parcelsGeoJSON.clusteringParams.distance_threshold,
                    min_samples: parcelsGeoJSON.clusteringParams.min_samples
                })
            }

            // Refresh project data with clustering status
            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)

        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to run clustering')
            console.error("Error running clustering:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    /**
     * Delete clustering results and reset to pre-clustering state
     */
    const handleDeleteClustering = async () => {
        if (!currentProject) return

        setIsProcessing(true)
        setError(null)

        try {
            await api.deleteParcels(currentProject.id)

            // Reset clustering state
            setIsClusteringComplete(false)
            setClusteringResult(null)
            setClusteringParams(null)
            setParcelGeoJSON(null)

            // Refresh project data
            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)

        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to delete clustering')
            console.error("Error deleting clustering:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    /**
     * Export project data in specified format (KML, Excel, etc.)
     * Creates download link and triggers browser download
     */
    const handleExportData = async (format: ExportFormat) => {
        if (!currentProject) return

        setIsProcessing(true)

        try {
            const exportBlob = await api.exportData(currentProject.id, {
                format: format,
                include_statistics: true,
            })

            // Map export formats to file extensions
            const fileExtensions: Record<ExportFormat, string> = {
                [ExportFormat.GEOJSON]: 'zip',  // Multiple files
                [ExportFormat.KML]: 'kmz',
                [ExportFormat.SHAPEFILE]: 'zip',
                [ExportFormat.PARQUET]: 'zip',
                [ExportFormat.CSV]: 'zip',
                [ExportFormat.EXCEL]: 'xlsx',
            }

            // Trigger browser download
            const downloadUrl = window.URL.createObjectURL(exportBlob)
            const downloadLink = document.createElement('a')
            downloadLink.href = downloadUrl
            downloadLink.download = `${currentProject.name}_export.${fileExtensions[format]}`
            document.body.appendChild(downloadLink)
            downloadLink.click()
            document.body.removeChild(downloadLink)
            window.URL.revokeObjectURL(downloadUrl)
        } catch (error: any) {
            alert(error.response?.data?.detail || 'Failed to export data')
        } finally {
            setIsProcessing(false)
        }
    }

    /**
     * Copy full-screen map URL to clipboard
     */
    const handleCopyMapLink = async () => {
        if (!currentProject) return

        const mapUrl = `${window.location.origin}/map/${currentProject.id}`

        try {
            await navigator.clipboard.writeText(mapUrl)
            setMapLinkCopied(true)
            setTimeout(() => setMapLinkCopied(false), 2000)
        } catch (error) {
            console.error('Failed to copy link:', error)
            alert('Failed to copy link to clipboard')
        }
    }

    /**
     * Open full-screen map in new tab
     */
    const handleOpenMapInNewTab = () => {
        if (!currentProject) return

        const mapUrl = `/map/${currentProject.id}`
        window.open(mapUrl, '_blank')
    }

    if (!currentProject) {
        return <div>Loading...</div>
    }

    // Determine which pages user can navigate to based on completion status
    const canProceedToLayerSelection = isKhasraUploadComplete
    const areSettlementLayersComplete = (
        settlementLayerStatus?.settlements?.status === "successful" &&
        settlementLayerStatus?.isolated?.status === "successful"
    )
    const canProceedToClustering = isKhasraUploadComplete && areSettlementLayersComplete
    const canProceedToExport = isKhasraUploadComplete && areSettlementLayersComplete && isClusteringComplete

    return (
        <main className="min-h-screen bg-slate-50 flex flex-col">
            {/* Header */}
            <div className="bg-white border-b border-slate-200 shadow-sm">
                <div className="container mx-auto max-w-6xl px-6 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                            <button
                                onClick={() => router.push('/dashboard')}
                                className="p-2 hover:bg-slate-100 rounded-lg transition-colors"
                            >
                                <ArrowLeft className="w-5 h-5 text-slate-600" />
                            </button>
                            <div>
                                <h1 className="text-xl font-bold text-slate-900">{currentProject.name}</h1>
                                <p className="text-sm text-slate-600">{currentProject.location}</p>
                            </div>
                        </div>
                        <div className="text-right">
                            <p className="text-sm font-medium text-slate-900">
                                {currentProject.khasra_count || 0} Khasras
                            </p>
                            {currentProject.total_area_ha && (
                                <p className="text-xs text-slate-600">
                                    {currentProject.total_area_ha.toFixed(2)} ha
                                </p>
                            )}
                        </div>
                    </div>
                </div>
            </div>

            <div className="flex-1 flex flex-col container mx-auto max-w-6xl px-6 py-12">
                {/* Error Display */}
                {error && (
                    <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
                        <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                        <div className="flex-1">
                            <p className="text-sm text-red-800 font-medium">Error</p>
                            <p className="text-sm text-red-600 mt-1">{error}</p>
                        </div>
                        <button
                            onClick={() => setError(null)}
                            className="text-red-600 hover:text-red-800"
                        >
                            ×
                        </button>
                    </div>
                )}

                {/* Page Indicator */}
                <div className="flex gap-4 mb-6">
                    {[
                        { number: 1, label: "Upload Khasras" },
                        { number: 2, label: "Add Layers" },
                        { number: 3, label: "Clustering" },
                        { number: 4, label: "Export" },
                    ].map((step) => (
                        <div
                            key={step.number}
                            className={`flex-1 py-2 px-4 rounded-lg text-center font-semibold transition-all ${currentPage === step.number
                                ? "bg-blue-600 text-white"
                                : currentPage > step.number
                                    ? "bg-green-100 text-green-700"
                                    : "bg-slate-200 text-slate-500"
                                }`}
                        >
                            <div className="text-sm">Step {step.number}</div>
                            <div className="text-xs mt-1">{step.label}</div>
                        </div>
                    ))}
                </div>

                {/* Content Area */}
                <div className="bg-white rounded-lg p-8 mb-8 flex-1 flex flex-col min-h-0">
                    {/* Page 1: Upload */}
                    {currentPage === 1 && (
                        <div className="flex flex-col flex-1 min-h-0">
                            <div className="mb-6">
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 1: Upload Khasra Boundaries</h2>
                                <p className="text-base text-slate-600">Upload your KML or GeoJSON file containing land parcel boundaries</p>
                            </div>
                            <div className="flex-1 min-h-0">
                                <UploadSection
                                    onFileUpload={handleKhasraUpload}
                                    onKhasraDeleted={() => {
                                        // Reset all workflow state when khasras are deleted
                                        setIsKhasraUploadComplete(false)
                                        setConstraintLayersGeoJSON(null)
                                        setAllProjectLayers([])
                                        setSettlementLayerStatus(null)
                                        setActiveProcessingLayer(null)
                                        setIsClusteringComplete(false)
                                        setClusteringResult(null)
                                        setClusteringParams(null)
                                        setParcelGeoJSON(null)
                                    }}
                                    isProcessing={isProcessing}
                                />
                            </div>
                        </div>
                    )}

                    {/* Page 2: Layer Selection */}
                    {currentPage === 2 && (
                        <div className="flex flex-col flex-1 min-h-0">
                            <div className="mb-6">
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 2: Add Layers</h2>
                                <p className="text-base text-slate-600">Add unusable land layers to exclude from solar park areas</p>
                            </div>

                            <div className="flex gap-8 flex-1 min-h-0">
                                <div className="w-80 flex-shrink-0 space-y-6 overflow-y-auto">
                                    {/* Settlements & Buildings Layer */}
                                    <div className="border border-slate-200 rounded-lg p-4">
                                        <h4 className="font-semibold text-slate-900 mb-2">Settlements & Buildings</h4>
                                        <p className="text-xs text-slate-600 mb-4">
                                            Automatically detect settlements and isolated buildings from VIDA rooftop data
                                        </p>

                                        {!settlementLayerStatus || (settlementLayerStatus.settlements?.status === "failed" && settlementLayerStatus.isolated?.status === "failed") ? (
                                            <>
                                                {/* Parameters */}
                                                <div className="space-y-3 mb-4">
                                                    <div>
                                                        <label className="text-xs font-medium text-slate-700">Building Buffer (m)</label>
                                                        <input
                                                            type="text"
                                                            inputMode="numeric"
                                                            value={settlementLayerParams.building_buffer}
                                                            onChange={(e) => {
                                                                const value = e.target.value.replace(/[^0-9]/g, '');
                                                                const num = value === '' ? 0 : Math.max(0, Number(value));
                                                                setSettlementLayerParams({ ...settlementLayerParams, building_buffer: num });
                                                            }}
                                                            className="w-full mt-1 px-3 py-2 text-sm border border-slate-300 rounded-md"
                                                        />
                                                    </div>
                                                    <div>
                                                        <label className="text-xs font-medium text-slate-700">Max Inter-building Distance (m)</label>
                                                        <input
                                                            type="text"
                                                            inputMode="numeric"
                                                            value={settlementLayerParams.settlement_eps}
                                                            onChange={(e) => {
                                                                const value = e.target.value.replace(/[^0-9]/g, '');
                                                                const num = value === '' ? 0 : Math.max(0, Number(value));
                                                                setSettlementLayerParams({ ...settlementLayerParams, settlement_eps: num });
                                                            }}
                                                            className="w-full mt-1 px-3 py-2 text-sm border border-slate-300 rounded-md"
                                                        />
                                                    </div>
                                                    <div>
                                                        <label className="text-xs font-medium text-slate-700">Min Buildings in Settlement</label>
                                                        <input
                                                            type="text"
                                                            inputMode="numeric"
                                                            value={settlementLayerParams.min_buildings}
                                                            onChange={(e) => {
                                                                const value = e.target.value.replace(/[^0-9]/g, '');
                                                                const num = value === '' ? 1 : Math.max(1, Number(value));
                                                                setSettlementLayerParams({ ...settlementLayerParams, min_buildings: num });
                                                            }}
                                                            className="w-full mt-1 px-3 py-2 text-sm border border-slate-300 rounded-md"
                                                        />
                                                    </div>
                                                </div>

                                                <button
                                                    onClick={handleGenerateSettlementLayers}
                                                    disabled={isProcessing || activeProcessingLayer === "Settlements"}
                                                    className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-semibold rounded-lg transition-colors"
                                                >
                                                    Run Layer
                                                </button>
                                            </>
                                        ) : settlementLayerStatus.processing ? (
                                            <div className="space-y-3">
                                                {/* Processing Status */}
                                                <div className="space-y-2">
                                                    <div className="flex items-center gap-2">
                                                        <div className="w-4 h-4 rounded-full border-2 border-blue-600 border-t-transparent animate-spin flex-shrink-0" />
                                                        <span className="text-sm font-medium text-blue-700">Settlements</span>
                                                    </div>
                                                    <p className="text-xs text-slate-600 ml-6">
                                                        {settlementLayerStatus.settlements?.details || "Processing"}
                                                        <AnimatedEllipsis />
                                                    </p>
                                                </div>
                                                <div className="space-y-2">
                                                    <div className="flex items-center gap-2">
                                                        <div className="w-4 h-4 rounded-full border-2 border-blue-600 border-t-transparent animate-spin flex-shrink-0" />
                                                        <span className="text-sm font-medium text-blue-700">Isolated Buildings</span>
                                                    </div>
                                                    <p className="text-xs text-slate-600 ml-6">
                                                        {settlementLayerStatus.isolated?.details || "Processing"}
                                                        <AnimatedEllipsis />
                                                    </p>
                                                </div>
                                            </div>
                                        ) : (
                                            <>
                                                {/* Completed Status */}
                                                <div className="space-y-2 mb-4">
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-4 h-4 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0">
                                                                <span className="text-white text-xs">✓</span>
                                                            </div>
                                                            <span className="text-sm font-medium text-green-700">Settlements</span>
                                                        </div>
                                                        <span className="text-xs text-slate-600">
                                                            {settlementLayerStatus.settlements?.feature_count} features, {settlementLayerStatus.settlements?.area_ha?.toFixed(2)} ha
                                                        </span>
                                                    </div>
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-4 h-4 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0">
                                                                <span className="text-white text-xs">✓</span>
                                                            </div>
                                                            <span className="text-sm font-medium text-green-700">Isolated Buildings</span>
                                                        </div>
                                                        <span className="text-xs text-slate-600">
                                                            {settlementLayerStatus.isolated?.feature_count} features, {settlementLayerStatus.isolated?.area_ha?.toFixed(2)} ha
                                                        </span>
                                                    </div>
                                                </div>

                                                <button
                                                    onClick={handleDeleteSettlementLayers}
                                                    disabled={isProcessing}
                                                    className="w-full px-4 py-2 border border-red-600 hover:bg-red-100  text-red-600 disabled:bg-gray-100 disabled:text-gray-400 disabled:border-gray-400 text-sm font-semibold rounded-lg transition-colors"
                                                >
                                                    Delete Layer
                                                </button>
                                            </>
                                        )}
                                    </div>

                                    {/* Placeholder for future layers */}
                                    <div className="border border-slate-200 border-dashed rounded-lg p-4 text-center text-slate-400">
                                        <p className="text-sm">More layers coming soon...</p>
                                    </div>
                                </div>
                                <div className="flex-1 min-h-0 flex flex-col">
                                    {khasraGeoJSON ? (
                                        <div className="flex-1 min-h-0">
                                            <MapContainer
                                                data={khasraGeoJSON}
                                                center={mapCenter}
                                                zoom={mapZoom}
                                                layersData={constraintLayersGeoJSON || undefined}
                                            />
                                        </div>
                                    ) : (
                                        <div className="flex-1 min-h-0 bg-slate-100 flex items-center justify-center rounded-lg border-2 border-dashed border-slate-300">
                                            <p className="text-slate-500">No data available</p>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Page 3: Clustering */}
                    {currentPage === 3 && (
                        <div className="flex flex-col flex-1 min-h-0">
                            <div className="mb-6">
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 3: Cluster Khasras into Parcels</h2>
                                <p className="text-base text-slate-600">
                                    Group adjacent khasras into contiguous parcels for solar development
                                </p>
                            </div>
                            <div className="flex gap-8 flex-1 min-h-0">
                                <div className="w-80 flex-shrink-0 overflow-y-auto">
                                        <ClusteringSection
                                            data={khasraGeoJSON}
                                            isProcessing={isProcessing}
                                            clusteringComplete={isClusteringComplete}
                                            clusteringParams={clusteringParams}
                                            clusteringResult={clusteringResult}
                                            onClusteringComplete={(result: any) => {
                                                handleRunClustering(result.distanceThreshold, result.minSamples)
                                            }}
                                            onClusteringDeleted={handleDeleteClustering}
                                        />
                                    </div>
                                    <div className="flex-1 min-h-0 flex flex-col">
                                        {khasraGeoJSON ? (
                                            <div className="flex-1 min-h-0">
                                                <MapContainer
                                                    data={khasraGeoJSON}
                                                    center={mapCenter}
                                                    zoom={mapZoom}
                                                    parcelsData={parcelGeoJSON}
                                                    layersData={constraintLayersGeoJSON || undefined}
                                                />
                                            </div>
                                        ) : (
                                            <div className="flex-1 min-h-0 bg-slate-100 flex items-center justify-center rounded-lg border-2 border-dashed border-slate-300">
                                                <p className="text-slate-500">No data available</p>
                                            </div>
                                        )}
                                    </div>
                            </div>
                        </div>
                    )}

                    {/* Page 4: Export */}
                    {currentPage === 4 && (
                        <div className="flex flex-col flex-1 min-h-0">
                            <div className="mb-6">
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 4: Export Results</h2>
                                <p className="text-base text-slate-600">Download your analysis results in various formats</p>
                            </div>

                            <div className="flex gap-8 flex-1 min-h-0">
                                <div className="w-80 flex-shrink-0 space-y-6 overflow-y-auto">
                                    {/* Summary Stats */}
                                    {clusteringResult && (
                                        <div className="space-y-3 p-4 bg-blue-50 rounded-lg border border-blue-200">
                                            <h3 className="text-sm font-semibold text-slate-900">Summary</h3>
                                            <div className="space-y-2">
                                                <div className="flex justify-between items-center">
                                                    <span className="text-xs text-slate-600">Total Parcels</span>
                                                    <span className="text-lg font-bold text-blue-600">{clusteringResult.total_parcels}</span>
                                                </div>
                                                <div className="flex justify-between items-center">
                                                    <span className="text-xs text-slate-600">Clustered Khasras</span>
                                                    <span className="text-lg font-bold text-green-600">{clusteringResult.clustered_khasras}</span>
                                                </div>
                                                <div className="flex justify-between items-center">
                                                    <span className="text-xs text-slate-600">Unclustered</span>
                                                    <span className="text-lg font-bold text-slate-600">{clusteringResult.unclustered_khasras}</span>
                                                </div>
                                            </div>
                                        </div>
                                    )}

                                    {/* Download Section */}
                                    <div>
                                        <h3 className="text-base font-semibold text-slate-900 mb-4">Download Files</h3>
                                        <div className="space-y-4">
                                            {/* KML */}
                                            <button
                                                onClick={() => handleExportData(ExportFormat.KML)}
                                                disabled={isProcessing}
                                                className="w-full p-6 border-2 border-slate-200 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-all text-left"
                                            >
                                                <div className="flex items-start gap-4">
                                                    <Map className="w-8 h-8 text-blue-600 flex-shrink-0" />
                                                    <div>
                                                        <h4 className="text-base font-semibold text-slate-900">Download KMZ</h4>
                                                        <p className="text-xs text-slate-600 mt-1">All layers for Google Earth</p>
                                                        <p className="text-xs text-slate-500 mt-1">Includes khasras, parcels, and constraint layers</p>
                                                    </div>
                                                </div>
                                            </button>

                                            {/* Excel */}
                                            <button
                                                onClick={() => handleExportData(ExportFormat.EXCEL)}
                                                disabled={isProcessing}
                                                className="w-full p-6 border-2 border-slate-200 rounded-lg hover:border-emerald-500 hover:bg-emerald-50 transition-all text-left"
                                            >
                                                <div className="flex items-start gap-4">
                                                    <FileSpreadsheet className="w-8 h-8 text-emerald-600 flex-shrink-0" />
                                                    <div>
                                                        <h4 className="text-base font-semibold text-slate-900">Download Excel</h4>
                                                        <p className="text-xs text-slate-600 mt-1">Complete statistics workbook</p>
                                                        <p className="text-xs text-slate-500 mt-1">Multiple sheets with detailed analysis</p>
                                                    </div>
                                                </div>
                                            </button>
                                        </div>
                                    </div>
                                </div>

                                <div className="flex-1 min-h-0 flex flex-col relative">
                                    {/* Floating Map Controls */}
                                    <div className="absolute top-3 right-3 z-[1000] bg-white rounded-lg shadow-lg border border-slate-200 px-4 py-2">
                                        <div className="flex items-center gap-3">
                                            <span className="text-xs font-medium text-slate-600">Online Map:</span>
                                            <button
                                                onClick={handleOpenMapInNewTab}
                                                className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-xs font-medium rounded transition-colors"
                                            >
                                                <ExternalLink className="w-3.5 h-3.5" />
                                                Full Screen
                                            </button>
                                            <button
                                                onClick={handleCopyMapLink}
                                                className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-700 text-xs font-medium rounded transition-colors"
                                            >
                                                {mapLinkCopied ? (
                                                    <>
                                                        <Check className="w-3.5 h-3.5" />
                                                        Copied!
                                                    </>
                                                ) : (
                                                    <>
                                                        <Copy className="w-3.5 h-3.5" />
                                                        Copy Link
                                                    </>
                                                )}
                                            </button>
                                        </div>
                                    </div>

                                    {/* Map */}
                                    {khasraGeoJSON ? (
                                        <div className="flex-1 min-h-0">
                                            <MapContainer
                                                data={khasraGeoJSON}
                                                center={mapCenter}
                                                zoom={mapZoom}
                                                parcelsData={parcelGeoJSON}
                                                layersData={constraintLayersGeoJSON || undefined}
                                            />
                                        </div>
                                    ) : (
                                        <div className="flex-1 min-h-0 bg-slate-100 flex items-center justify-center rounded-lg border-2 border-dashed border-slate-300">
                                            <p className="text-slate-500">No data available</p>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )}
                </div>

                {/* Navigation Buttons */}
                <div className="flex gap-4 justify-between">
                    {currentPage > 1 && (
                        <button
                            onClick={() => setCurrentPage((prev) => Math.max(1, prev - 1))}
                            disabled={isProcessing}
                            className="flex items-center gap-2 px-6 py-3 border border-blue-600 hover:bg-blue-100 disabled:bg-slate-400 text-blue-600 font-semibold rounded-lg transition-colors"
                        >
                            <ChevronLeft className="w-5 h-5" />
                            Previous
                        </button>
                    )}

                    {currentPage < 4 && (
                        <button
                            onClick={() => setCurrentPage((prev) => Math.min(4, prev + 1))}
                            disabled={
                                isProcessing ||
                                (currentPage === 1 && !canProceedToLayerSelection) ||
                                (currentPage === 2 && !canProceedToClustering) ||
                                (currentPage === 3 && !canProceedToExport)
                            }
                            className="flex items-center gap-2 px-6 py-3 border border-blue-600 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors ml-auto"
                        >
                            Next
                            <ChevronRight className="w-5 h-5" />
                        </button>
                    )}
                </div>
            </div>
        </main>
    )
}
