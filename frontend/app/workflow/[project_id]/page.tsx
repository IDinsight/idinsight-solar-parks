"use client"

import { useState, useEffect } from "react"
import { useRouter, useParams, useSearchParams } from "next/navigation"
import { ProtectedRoute } from "@/components/protected-route"
import { useProjectStore } from "@/lib/stores/project"
import { useMapStore } from "@/lib/stores/map"
import UploadSection from "@/components/upload-section"
import ClusteringSection from "@/components/clustering-section"
import MapContainer, { LAYER_COLORS } from "@/components/map-container"
import * as api from "@/lib/api/services"
import { ExportFormat } from "@/lib/api/types"
import { ChevronLeft, ChevronRight, ArrowLeft, AlertCircle, Map, Globe, ExternalLink, Link, FileSpreadsheet, Trash2, AlertTriangle } from "lucide-react"


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
    const searchParams = useSearchParams()
    const projectId = params.project_id as string
    const { currentProject, setCurrentProject, updateProject } = useProjectStore()
    const { clearProjectMap } = useMapStore()

    // Workflow state - initialize from URL if available
    const [currentPage, setCurrentPage] = useState(() => {
        const pageFromUrl = searchParams.get('page')
        return pageFromUrl ? parseInt(pageFromUrl) : 1
    })
    const [isProcessing, setIsProcessing] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [isLoadingInitialData, setIsLoadingInitialData] = useState(true)

    // Step 1: Khasra upload state
    const [khasraFile, setKhasraFile] = useState<File | null>(null)
    const [khasraGeoJSON, setKhasraGeoJSON] = useState<any>(null)
    const [khasraIdColumn, setKhasraIdColumn] = useState<string>("")
    const [isKhasraUploadComplete, setIsKhasraUploadComplete] = useState(false)

    // Step 2: Constraint layers state
    const [constraintLayersGeoJSON, setConstraintLayersGeoJSON] = useState<Record<string, any> | null>(null)
    const [allProjectLayers, setAllProjectLayers] = useState<any[]>([])
    const [activeProcessingLayers, setActiveProcessingLayers] = useState<string[]>([])

    // Settlement layer configuration
    const [settlementLayerParams, setSettlementLayerParams] = useState({
        building_buffer: 10,
        settlement_eps: 50,
        min_buildings: 5,
    })
    const [settlementLayerStatus, setSettlementLayerStatus] = useState<any>(null)
    const [croplandLayerStatus, setCroplandLayerStatus] = useState<any>(null)
    const [waterLayerStatus, setWaterLayerStatus] = useState<any>(null)

    // Slopes layer configuration
    const [slopesLayerParams, setSlopesLayerParams] = useState({
        include_north_slopes: true,
        include_other_slopes: true,
        north_min_angle: 7.0,
        other_min_angle: 10.0,
    })
    const [northSlopesLayerStatus, setNorthSlopesLayerStatus] = useState<any>(null)
    const [otherSlopesLayerStatus, setOtherSlopesLayerStatus] = useState<any>(null)

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

    // Delete modal states
    const [showDeleteSettlementModal, setShowDeleteSettlementModal] = useState(false)
    const [showDeleteCroplandModal, setShowDeleteCroplandModal] = useState(false)
    const [showDeleteWaterModal, setShowDeleteWaterModal] = useState(false)
    const [showDeleteSlopesModal, setShowDeleteSlopesModal] = useState(false)
    const [isDeletingLayer, setIsDeletingLayer] = useState(false)

    /**
     * Update current page and persist to URL
     */
    const updateCurrentPage = (page: number) => {
        setCurrentPage(page)
        router.replace(`/workflow/${projectId}?page=${page}`, { scroll: false })
    }

    /**
     * Calculate optimal zoom level based on geographic bounds
     * Uses a simple heuristic: larger areas need lower zoom levels
     */
    const calculateZoomLevel = (bounds: { minx: number, maxx: number, miny: number, maxy: number }): number => {
        const lngDiff = bounds.maxx - bounds.minx
        const latDiff = bounds.maxy - bounds.miny
        const maxDiff = Math.max(lngDiff, latDiff)

        // Approximate zoom levels based on angular extent
        // These values are tuned for typical project sizes
        if (maxDiff > 2) return 8   // Very large area (>200km)
        if (maxDiff > 1) return 9   // Large area (~100-200km)
        if (maxDiff > 0.5) return 10 // Medium area (~50-100km)
        if (maxDiff > 0.2) return 11 // Smaller area (~20-50km)
        if (maxDiff > 0.1) return 12 // Small area (~10-20km)
        if (maxDiff > 0.05) return 13 // Very small area (~5-10km)
        return 14 // Tiny area (<5km)
    }

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

                    // Update cropland layer status
                    const croplandLayer = layers.find((l: any) => l.name === "Cropland")
                    if (croplandLayer) {
                        setCroplandLayerStatus(croplandLayer)
                    }

                    // Update water layer status
                    const waterLayer = layers.find((l: any) => l.name === "Water")
                    if (waterLayer) {
                        setWaterLayerStatus(waterLayer)
                    }

                    // Update slopes layer status
                    const northSlopesLayer = layers.find((l: any) => l.name === "Slopes - North Facing")
                    if (northSlopesLayer) {
                        setNorthSlopesLayerStatus(northSlopesLayer)
                    }
                    const otherSlopesLayer = layers.find((l: any) => l.name === "Slopes - Other Facing")
                    if (otherSlopesLayer) {
                        setOtherSlopesLayerStatus(otherSlopesLayer)
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
     * Debug: Track activeProcessingLayers changes
     */
    useEffect(() => {
        console.log('[DEBUG] activeProcessingLayers changed to:', activeProcessingLayers)
    }, [activeProcessingLayers])

    /**
     * Poll for layer processing status updates every 5 seconds
     * Stops polling when processing is complete and refreshes all data
     */
    useEffect(() => {
        if (!currentProject?.id || activeProcessingLayers.length === 0) return

        console.log('[POLLING] Starting poll for layers:', activeProcessingLayers)

        const pollInterval = setInterval(async () => {
            try {
                console.log('[POLLING] Checking status for:', activeProcessingLayers)
                const layers = await api.listProjectLayers(currentProject.id)
                setAllProjectLayers(layers)

                let layersToRemove: string[] = []
                let shouldRefreshData = false

                if (activeProcessingLayers.includes("Settlements")) {
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
                        layersToRemove.push("Settlements")
                        shouldRefreshData = true
                    }
                }

                if (activeProcessingLayers.includes("Cropland")) {
                    const croplandLayer = layers.find((l: any) => l.name === "Cropland")

                    console.log('[CROPLAND] Found layer:', croplandLayer)
                    console.log('[CROPLAND] Status:', croplandLayer?.status)

                    setCroplandLayerStatus(croplandLayer)

                    if (croplandLayer?.status !== "in_progress") {
                        console.log('[CROPLAND] Processing complete! Removing from active layers.')
                        layersToRemove.push("Cropland")
                        shouldRefreshData = true
                    }
                }

                if (activeProcessingLayers.includes("Water")) {
                    const waterLayer = layers.find((l: any) => l.name === "Water")

                    setWaterLayerStatus(waterLayer)

                    if (waterLayer?.status !== "in_progress") {
                        layersToRemove.push("Water")
                        shouldRefreshData = true
                    }
                }

                if (activeProcessingLayers.includes("Slopes")) {
                    const northSlopesLayer = layers.find((l: any) => l.name === "Slopes - North Facing")
                    const otherSlopesLayer = layers.find((l: any) => l.name === "Slopes - Other Facing")

                    setNorthSlopesLayerStatus(northSlopesLayer)
                    setOtherSlopesLayerStatus(otherSlopesLayer)

                    // Check if both slope layers are done processing
                    const northDone = !northSlopesLayer || northSlopesLayer?.status !== "in_progress"
                    const otherDone = !otherSlopesLayer || otherSlopesLayer?.status !== "in_progress"

                    if (northDone && otherDone) {
                        layersToRemove.push("Slopes")
                        shouldRefreshData = true
                    }
                }

                // Remove completed layers from active processing
                if (layersToRemove.length > 0) {
                    setActiveProcessingLayers(prev => prev.filter(layer => !layersToRemove.includes(layer)))
                }

                // Refresh data if any layer completed
                if (shouldRefreshData) {
                    const layersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
                    setConstraintLayersGeoJSON(layersGeoJSON)
                    const project = await api.getProject(currentProject.id)
                    updateProject(project)
                }
            } catch (error) {
                console.error("Error polling layer status:", error)
            }
        }, 5000) // Poll every 5 seconds

        return () => clearInterval(pollInterval)
    }, [currentProject?.id, activeProcessingLayers, updateProject])


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
            setIsLoadingInitialData(true)
            try {
                const project = await api.getProject(currentProject.id)
                updateProject(project)

                // Load project state data based on completion status
                if (project.khasra_count && project.khasra_count > 0) {
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
                                setMapZoom(calculateZoomLevel(khasraSummary.bounds))
                            }
                        }
                    } catch (error) {
                        console.error('Failed to load khasra GeoJSON:', error)
                    }

                    // Check if project has clustering complete and load parcel data
                    const isClustered = project.status === 'clustered' || project.status === 'completed'
                    if (isClustered) {
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
                    }

                    // Set upload complete only AFTER all data is loaded
                    setIsKhasraUploadComplete(true)
                }
            } catch (error) {
                console.error('Failed to load project:', error)
            } finally {
                setIsLoadingInitialData(false)
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
                setMapZoom(calculateZoomLevel(uploadResponse.bounds))
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

        setActiveProcessingLayers(prev => [...prev, "Settlements"])
        setError(null)

        // Set initial processing status for immediate UI feedback
        setSettlementLayerStatus({
            settlements: { status: "in_progress", details: "Queued for processing" },
            isolated: { status: "in_progress", details: "Queued for processing" },
            processing: true
        })

        try {
            await api.generateSettlementLayer(currentProject.id, settlementLayerParams)
            // Polling effect will automatically track progress
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to generate settlement layers')
            console.error("Error generating settlement layers:", error)
            setActiveProcessingLayers(prev => prev.filter(l => l !== "Settlements"))
            setSettlementLayerStatus(null)
        }
    }

    /**
     * Delete both settlement layers (Settlements and Isolated Buildings)
     * Requires user confirmation and refreshes all data after deletion
     */
    const handleDeleteSettlementLayers = async () => {
        if (!currentProject) return

        setIsDeletingLayer(true)
        setError(null)

        try {
            // Delete both settlement sub-layers
            await api.deleteLayer(currentProject.id, "Settlements")
            await api.deleteLayer(currentProject.id, "Isolated Buildings")

            setSettlementLayerStatus(null)
            setShowDeleteSettlementModal(false)

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
            setIsDeletingLayer(false)
        }
    }

    /**
     * Generate cropland layer from landcover data
     */
    const handleGenerateCroplandLayer = async () => {
        if (!currentProject) return

        console.log('[CROPLAND] Starting generation, adding "Cropland" to activeProcessingLayers')
        setActiveProcessingLayers(prev => [...prev, "Cropland"])
        setError(null)

        setCroplandLayerStatus({
            status: "in_progress",
            details: "Queued for processing"
        })

        try {
            await api.generateCroplandLayer(currentProject.id)
            console.log('[CROPLAND] API call successful, polling should now start')
            // Polling effect will automatically track progress
        } catch (error: any) {
            console.error('[CROPLAND] API error:', error)
            setError(error.response?.data?.detail || 'Failed to generate cropland layer')
            console.error("Error generating cropland layer:", error)
            setActiveProcessingLayers(prev => prev.filter(l => l !== "Cropland"))
            setCroplandLayerStatus(null)
        }
    }

    /**
     * Delete cropland layer
     */
    const handleDeleteCroplandLayer = async () => {
        if (!currentProject) return

        setIsDeletingLayer(true)
        setError(null)

        try {
            await api.deleteLayer(currentProject.id, "Cropland")
            setCroplandLayerStatus(null)
            setShowDeleteCroplandModal(false)

            // Refresh all layers and GeoJSON data
            const updatedLayers = await api.listProjectLayers(currentProject.id)
            setAllProjectLayers(updatedLayers)

            const updatedLayersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
            setConstraintLayersGeoJSON(updatedLayersGeoJSON)

            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to delete cropland layer')
            console.error("Error deleting cropland layer:", error)
        } finally {
            setIsDeletingLayer(false)
        }
    }

    /**
     * Generate water layer from landcover data
     */
    const handleGenerateWaterLayer = async () => {
        if (!currentProject) return

        setActiveProcessingLayers(prev => [...prev, "Water"])
        setError(null)

        setWaterLayerStatus({
            status: "in_progress",
            details: "Queued for processing"
        })

        try {
            await api.generateWaterLayer(currentProject.id)
            // Polling effect will automatically track progress
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to generate water layer')
            console.error("Error generating water layer:", error)
            setActiveProcessingLayers(prev => prev.filter(l => l !== "Water"))
            setWaterLayerStatus(null)
        }
    }

    /**
     * Delete water layer
     */
    const handleDeleteWaterLayer = async () => {
        if (!currentProject) return

        setIsDeletingLayer(true)
        setError(null)

        try {
            await api.deleteLayer(currentProject.id, "Water")
            setWaterLayerStatus(null)
            setShowDeleteWaterModal(false)

            // Refresh all layers and GeoJSON data
            const updatedLayers = await api.listProjectLayers(currentProject.id)
            setAllProjectLayers(updatedLayers)

            const updatedLayersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
            setConstraintLayersGeoJSON(updatedLayersGeoJSON)

            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to delete water layer')
            console.error("Error deleting water layer:", error)
        } finally {
            setIsDeletingLayer(false)
        }
    }

    /**
     * Generate slopes layer from NASA ALOS DEM data
     */
    const handleGenerateSlopesLayer = async () => {
        if (!currentProject) return

        setActiveProcessingLayers(prev => [...prev, "Slopes"])
        setError(null)

        setNorthSlopesLayerStatus({ status: "in_progress", details: "Queued for processing" })
        setOtherSlopesLayerStatus({ status: "in_progress", details: "Queued for processing" })

        try {
            await api.generateSlopesLayer(currentProject.id, slopesLayerParams)
            // Polling effect will automatically track progress
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to generate slopes layer')
            console.error("Error generating slopes layer:", error)
            setActiveProcessingLayers(prev => prev.filter(l => l !== "Slopes"))
            setNorthSlopesLayerStatus(null)
            setOtherSlopesLayerStatus(null)
        }
    }

    /**
     * Delete slopes layers
     */
    const handleDeleteSlopesLayer = async () => {
        if (!currentProject) return

        setIsDeletingLayer(true)
        setError(null)

        try {
            // Delete both slope layers if they exist
            if (northSlopesLayerStatus) {
                await api.deleteLayer(currentProject.id, "Slopes - North Facing")
            }
            if (otherSlopesLayerStatus) {
                await api.deleteLayer(currentProject.id, "Slopes - Other Facing")
            }

            setNorthSlopesLayerStatus(null)
            setOtherSlopesLayerStatus(null)
            setShowDeleteSlopesModal(false)

            const updatedLayersGeoJSON = await api.getProjectLayersGeoJSON(currentProject.id)
            setConstraintLayersGeoJSON(updatedLayersGeoJSON)

            const updatedProject = await api.getProject(currentProject.id)
            updateProject(updatedProject)
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to delete slopes layers')
            console.error("Error deleting slopes layers:", error)
        } finally {
            setIsDeletingLayer(false)
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

    // Show loading state while initial data is being fetched
    if (isLoadingInitialData) {
        return (
            <main className="min-h-screen bg-slate-50 flex items-center justify-center">
                <div className="text-center">
                    <div className="inline-block animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mb-4"></div>
                    <p className="text-slate-700 text-lg font-medium mb-2">Loading project data...</p>
                    <p className="text-slate-500 text-sm">This may take a moment for large projects</p>
                </div>
            </main>
        )
    }

    // Determine which pages user can navigate to based on completion status
    const canProceedToLayerSelection = isKhasraUploadComplete
    const areSettlementLayersComplete = (
        settlementLayerStatus?.settlements?.status === "successful" &&
        settlementLayerStatus?.isolated?.status === "successful"
    )

    // Check if ALL requested layers have completed successfully
    const areAllRequestedLayersComplete = () => {
        // Settlement layers are always checked
        if (!areSettlementLayersComplete) {
            return false
        }

        // Check cropland layer if it was requested
        if (croplandLayerStatus && croplandLayerStatus.status !== "successful") {
            return false
        }

        // Check water layer if it was requested
        if (waterLayerStatus && waterLayerStatus.status !== "successful") {
            return false
        }

        // Check north slopes layer if it was requested
        if (northSlopesLayerStatus && northSlopesLayerStatus.status !== "successful") {
            return false
        }

        // Check other slopes layer if it was requested
        if (otherSlopesLayerStatus && otherSlopesLayerStatus.status !== "successful") {
            return false
        }

        return true
    }

    const canProceedToClustering = isKhasraUploadComplete && areAllRequestedLayersComplete()
    const canProceedToExport = isKhasraUploadComplete && areAllRequestedLayersComplete() && isClusteringComplete

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
                        { number: 3, label: "Cluster" },
                        { number: 4, label: "Export" },
                    ].map((step) => {
                        const canNavigateToStep =
                            step.number === 1 ||
                            (step.number === 2 && canProceedToLayerSelection) ||
                            (step.number === 3 && canProceedToClustering) ||
                            (step.number === 4 && canProceedToExport)

                        return (
                            <button
                                key={step.number}
                                onClick={() => canNavigateToStep && updateCurrentPage(step.number)}
                                disabled={!canNavigateToStep}
                                className={`flex-1 py-2 px-4 rounded-lg text-center font-semibold transition-all ${currentPage === step.number
                                    ? "bg-blue-600 text-white"
                                    : currentPage > step.number
                                        ? "bg-green-100 text-green-700 hover:bg-green-200 cursor-pointer"
                                        : canNavigateToStep
                                            ? "bg-slate-200 text-slate-700 hover:bg-slate-300 cursor-pointer"
                                            : "bg-slate-200 text-slate-500 cursor-not-allowed"
                                    }`}
                            >
                                <div className="text-sm">Step {step.number}</div>
                                <div className="text-xs mt-1">{step.label}</div>
                            </button>
                        )
                    })}
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
                                        setActiveProcessingLayers([])
                                        setIsClusteringComplete(false)
                                        setClusteringResult(null)
                                        setClusteringParams(null)
                                        setParcelGeoJSON(null)

                                        // Clear layer completion states
                                        setCroplandLayerStatus(null)
                                        setWaterLayerStatus(null)
                                        setNorthSlopesLayerStatus(null)
                                        setOtherSlopesLayerStatus(null)

                                        // Reset map state (center, zoom, layer visibility)
                                        clearProjectMap(projectId)
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
                                        <div className="flex items-start justify-between mb-2">
                                            <h4 className="font-semibold text-slate-900">Settlements & Buildings</h4>
                                            {settlementLayerStatus && !(settlementLayerStatus.settlements?.status === "failed" && settlementLayerStatus.isolated?.status === "failed") && !settlementLayerStatus.processing && (
                                                <button
                                                    onClick={() => setShowDeleteSettlementModal(true)}
                                                    disabled={isDeletingLayer}
                                                    className="p-1 hover:bg-red-50 rounded transition-colors disabled:opacity-50"
                                                    title="Delete Layer"
                                                >
                                                    <Trash2 className="w-4 h-4 text-red-600" />
                                                </button>
                                            )}
                                        </div>
                                        <p className="text-xs text-slate-600 mb-4">
                                            Automatically detect settlements and isolated buildings from VIDA rooftop data
                                        </p>

                                        {!settlementLayerStatus || (settlementLayerStatus.settlements?.status === "failed" && settlementLayerStatus.isolated?.status === "failed") ? (
                                            <>
                                                {/* Show alert if failed */}
                                                {(settlementLayerStatus?.settlements?.status === "failed" || settlementLayerStatus?.isolated?.status === "failed") && (
                                                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded flex items-center gap-2">
                                                        <AlertCircle className="w-4 h-4 text-red-600 flex-shrink-0" />
                                                        <span className="text-sm text-red-700">Failed to generate settlement layers. Please try again.</span>
                                                    </div>
                                                )}
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
                                                    disabled={isProcessing || activeProcessingLayers.includes("Settlements")}
                                                    className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-semibold rounded-lg transition-colors"
                                                >
                                                    {(settlementLayerStatus?.settlements?.status === "failed" || settlementLayerStatus?.isolated?.status === "failed") ? "Retry" : "Add Layer"}
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
                                                <div className="space-y-2">
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: LAYER_COLORS['Settlements'] }}>
                                                                <span className="text-white text-xs">✓</span>
                                                            </div>
                                                            <span className="text-sm font-medium" style={{ color: LAYER_COLORS['Settlements'] }}>Settlements</span>
                                                        </div>
                                                        <span className="text-xs text-slate-600">
                                                            {settlementLayerStatus.settlements?.area_ha?.toFixed(2)} ha
                                                        </span>
                                                    </div>
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: LAYER_COLORS['Isolated Buildings'] }}>
                                                                <span className="text-white text-xs">✓</span>
                                                            </div>
                                                            <span className="text-sm font-medium" style={{ color: LAYER_COLORS['Isolated Buildings'] }}>Isolated Buildings</span>
                                                        </div>
                                                        <span className="text-xs text-slate-600">
                                                            {settlementLayerStatus.isolated?.area_ha?.toFixed(2)} ha
                                                        </span>
                                                    </div>
                                                </div>
                                            </>
                                        )}
                                    </div>

                                    {/* Cropland Layer */}
                                    <div className="border border-slate-200 rounded-lg p-4">
                                        <div className="flex items-start justify-between mb-2">
                                            <h4 className="font-semibold text-slate-900">Cropland</h4>
                                            {croplandLayerStatus && croplandLayerStatus?.status !== "failed" && croplandLayerStatus?.status !== "in_progress" && (
                                                <button
                                                    onClick={() => setShowDeleteCroplandModal(true)}
                                                    disabled={isDeletingLayer}
                                                    className="p-1 hover:bg-red-50 rounded transition-colors disabled:opacity-50"
                                                    title="Delete Layer"
                                                >
                                                    <Trash2 className="w-4 h-4 text-red-600" />
                                                </button>
                                            )}
                                        </div>
                                        <p className="text-xs text-slate-600 mb-4">
                                            Automatically detect agricultural cropland from landcover data
                                        </p>

                                        {!croplandLayerStatus || croplandLayerStatus?.status === "failed" ? (
                                            <>
                                                {/* Show alert if failed */}
                                                {croplandLayerStatus?.status === "failed" && (
                                                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded flex items-center gap-2">
                                                        <AlertCircle className="w-4 h-4 text-red-600 flex-shrink-0" />
                                                        <span className="text-sm text-red-700">Failed to generate cropland layer. Please try again.</span>
                                                    </div>
                                                )}
                                                <button
                                                    onClick={handleGenerateCroplandLayer}
                                                    disabled={isProcessing || activeProcessingLayers.includes("Cropland")}
                                                    className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-semibold rounded-lg transition-colors"
                                                >
                                                    {croplandLayerStatus?.status === "failed" ? "Retry" : "Add Layer"}
                                                </button>
                                            </>
                                        ) : croplandLayerStatus?.status === "in_progress" ? (
                                            <div className="space-y-3">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-4 h-4 rounded-full border-2 border-blue-600 border-t-transparent animate-spin flex-shrink-0" />
                                                    <span className="text-sm font-medium text-blue-700">Processing</span>
                                                </div>
                                                <p className="text-xs text-slate-600 ml-6">
                                                    {croplandLayerStatus?.details || "Processing"}
                                                    <AnimatedEllipsis />
                                                </p>
                                            </div>
                                        ) : (
                                            <div className="space-y-2">
                                                <div className="flex items-center justify-between">
                                                    <div className="flex items-center gap-2">
                                                        <div className="w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: LAYER_COLORS['Cropland'] }}>
                                                            <span className="text-white text-xs">✓</span>
                                                        </div>
                                                        <span className="text-sm font-medium" style={{ color: LAYER_COLORS['Cropland'] }}>Cropland</span>
                                                    </div>
                                                    <span className="text-xs text-slate-600">
                                                        {croplandLayerStatus?.area_ha?.toFixed(2)} ha
                                                    </span>
                                                </div>
                                            </div>
                                        )}
                                    </div>

                                    {/* Water Layer */}
                                    <div className="border border-slate-200 rounded-lg p-4">
                                        <div className="flex items-start justify-between mb-2">
                                            <h4 className="font-semibold text-slate-900">Water</h4>
                                            {waterLayerStatus && waterLayerStatus?.status !== "failed" && waterLayerStatus?.status !== "in_progress" && (
                                                <button
                                                    onClick={() => setShowDeleteWaterModal(true)}
                                                    disabled={isDeletingLayer}
                                                    className="p-1 hover:bg-red-50 rounded transition-colors disabled:opacity-50"
                                                    title="Delete Layer"
                                                >
                                                    <Trash2 className="w-4 h-4 text-red-600" />
                                                </button>
                                            )}
                                        </div>
                                        <p className="text-xs text-slate-600 mb-4">
                                            Automatically detect water bodies from landcover data
                                        </p>

                                        {!waterLayerStatus || waterLayerStatus?.status === "failed" ? (
                                            <>
                                                {/* Show alert if failed */}
                                                {waterLayerStatus?.status === "failed" && (
                                                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded flex items-center gap-2">
                                                        <AlertCircle className="w-4 h-4 text-red-600 flex-shrink-0" />
                                                        <span className="text-sm text-red-700">Failed to generate water layer. Please try again.</span>
                                                    </div>
                                                )}
                                                <button
                                                    onClick={handleGenerateWaterLayer}
                                                    disabled={isProcessing || activeProcessingLayers.includes("Water")}
                                                    className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-semibold rounded-lg transition-colors"
                                                >
                                                    {waterLayerStatus?.status === "failed" ? "Retry" : "Add Layer"}
                                                </button>
                                            </>
                                        ) : waterLayerStatus?.status === "in_progress" ? (
                                            <div className="space-y-3">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-4 h-4 rounded-full border-2 border-blue-600 border-t-transparent animate-spin flex-shrink-0" />
                                                    <span className="text-sm font-medium text-blue-700">Processing</span>
                                                </div>
                                                <p className="text-xs text-slate-600 ml-6">
                                                    {waterLayerStatus?.details || "Processing"}
                                                    <AnimatedEllipsis />
                                                </p>
                                            </div>
                                        ) : (
                                            <div className="space-y-2">
                                                <div className="flex items-center justify-between">
                                                    <div className="flex items-center gap-2">
                                                        <div className="w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: LAYER_COLORS['Water'] }}>
                                                            <span className="text-white text-xs">✓</span>
                                                        </div>
                                                        <span className="text-sm font-medium" style={{ color: LAYER_COLORS['Water'] }}>Water</span>
                                                    </div>
                                                    <span className="text-xs text-slate-600">
                                                        {waterLayerStatus?.area_ha?.toFixed(2)} ha
                                                    </span>
                                                </div>
                                            </div>
                                        )}
                                    </div>

                                    {/* Slopes Layer */}
                                    <div className="border border-slate-200 rounded-lg p-4">
                                        <div className="flex items-start justify-between mb-2">
                                            <h4 className="font-semibold text-slate-900">Slopes</h4>
                                            {((northSlopesLayerStatus && northSlopesLayerStatus?.status !== "failed" && northSlopesLayerStatus?.status !== "in_progress") ||
                                                (otherSlopesLayerStatus && otherSlopesLayerStatus?.status !== "failed" && otherSlopesLayerStatus?.status !== "in_progress")) && (
                                                    <button
                                                        onClick={() => setShowDeleteSlopesModal(true)}
                                                        disabled={isDeletingLayer}
                                                        className="p-1 hover:bg-red-50 rounded transition-colors disabled:opacity-50"
                                                        title="Delete Layer"
                                                    >
                                                        <Trash2 className="w-4 h-4 text-red-600" />
                                                    </button>
                                                )}
                                        </div>
                                        <p className="text-xs text-slate-600 mb-4">
                                            Automatically detect steep slopes from NASA ALOS DEM data
                                        </p>

                                        {(!northSlopesLayerStatus && !otherSlopesLayerStatus) ||
                                            northSlopesLayerStatus?.status === "failed" ||
                                            otherSlopesLayerStatus?.status === "failed" ? (
                                            <>
                                                {/* Show alert if failed */}
                                                {(northSlopesLayerStatus?.status === "failed" || otherSlopesLayerStatus?.status === "failed") && (
                                                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded flex items-center gap-2">
                                                        <AlertCircle className="w-4 h-4 text-red-600 flex-shrink-0" />
                                                        <span className="text-sm text-red-700">Failed to generate slopes layer. Please try again.</span>
                                                    </div>
                                                )}

                                                {/* Parameters */}
                                                <div className="space-y-3 mb-4">
                                                    <div className="flex items-center gap-2">
                                                        <input
                                                            type="checkbox"
                                                            checked={slopesLayerParams.include_north_slopes}
                                                            onChange={(e) => setSlopesLayerParams({ ...slopesLayerParams, include_north_slopes: e.target.checked })}
                                                            className="w-4 h-4 text-blue-600 rounded"
                                                        />
                                                        <label className="text-xs font-medium text-slate-700">Include North Slopes (45-135°)</label>
                                                    </div>
                                                    {slopesLayerParams.include_north_slopes && (
                                                        <div className="ml-6">
                                                            <label className="text-xs font-medium text-slate-700">Min Angle (degrees)</label>
                                                            <input
                                                                type="number"
                                                                step="0.5"
                                                                min="0"
                                                                max="90"
                                                                value={slopesLayerParams.north_min_angle}
                                                                onChange={(e) => setSlopesLayerParams({ ...slopesLayerParams, north_min_angle: parseFloat(e.target.value) || 0 })}
                                                                className="w-full mt-1 px-3 py-2 text-sm border border-slate-300 rounded-md"
                                                            />
                                                        </div>
                                                    )}
                                                    <div className="flex items-center gap-2">
                                                        <input
                                                            type="checkbox"
                                                            checked={slopesLayerParams.include_other_slopes}
                                                            onChange={(e) => setSlopesLayerParams({ ...slopesLayerParams, include_other_slopes: e.target.checked })}
                                                            className="w-4 h-4 text-blue-600 rounded"
                                                        />
                                                        <label className="text-xs font-medium text-slate-700">Include Other Slopes</label>
                                                    </div>
                                                    {slopesLayerParams.include_other_slopes && (
                                                        <div className="ml-6">
                                                            <label className="text-xs font-medium text-slate-700">Min Angle (degrees)</label>
                                                            <input
                                                                type="number"
                                                                step="0.5"
                                                                min="0"
                                                                max="90"
                                                                value={slopesLayerParams.other_min_angle}
                                                                onChange={(e) => setSlopesLayerParams({ ...slopesLayerParams, other_min_angle: parseFloat(e.target.value) || 0 })}
                                                                className="w-full mt-1 px-3 py-2 text-sm border border-slate-300 rounded-md"
                                                            />
                                                        </div>
                                                    )}
                                                </div>

                                                <button
                                                    onClick={handleGenerateSlopesLayer}
                                                    disabled={isProcessing || activeProcessingLayers.includes("Slopes") || (!slopesLayerParams.include_north_slopes && !slopesLayerParams.include_other_slopes)}
                                                    className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-semibold rounded-lg transition-colors"
                                                >
                                                    {(northSlopesLayerStatus?.status === "failed" || otherSlopesLayerStatus?.status === "failed") ? "Retry" : "Add Layer"}
                                                </button>
                                            </>
                                        ) : (northSlopesLayerStatus?.status === "in_progress" || otherSlopesLayerStatus?.status === "in_progress") ? (
                                            <div className="space-y-3">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-4 h-4 rounded-full border-2 border-blue-600 border-t-transparent animate-spin flex-shrink-0" />
                                                    <span className="text-sm font-medium text-blue-700">Processing</span>
                                                </div>
                                                <p className="text-xs text-slate-600 ml-6">
                                                    {northSlopesLayerStatus?.details || otherSlopesLayerStatus?.details || "Processing"}
                                                    <AnimatedEllipsis />
                                                </p>
                                            </div>
                                        ) : (
                                            <div className="space-y-2">
                                                {northSlopesLayerStatus && (
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: LAYER_COLORS['Slopes - North Facing'] }}>
                                                                <span className="text-white text-xs">✓</span>
                                                            </div>
                                                            <span className="text-sm font-medium" style={{ color: LAYER_COLORS['Slopes - North Facing'] }}>North Facing</span>
                                                        </div>
                                                        <span className="text-xs text-slate-600">
                                                            {northSlopesLayerStatus?.area_ha?.toFixed(2)} ha
                                                        </span>
                                                    </div>
                                                )}
                                                {otherSlopesLayerStatus && (
                                                    <div className="flex items-center justify-between">
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-4 h-4 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: LAYER_COLORS['Slopes - Other Facing'] }}>
                                                                <span className="text-white text-xs">✓</span>
                                                            </div>
                                                            <span className="text-sm font-medium" style={{ color: LAYER_COLORS['Slopes - Other Facing'] }}>Other Facing</span>
                                                        </div>
                                                        <span className="text-xs text-slate-600">
                                                            {otherSlopesLayerStatus?.area_ha?.toFixed(2)} ha
                                                        </span>
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>

                                </div>
                                <div className="flex-1 min-h-0 flex flex-col">
                                    {khasraGeoJSON ? (
                                        <div className="flex-1 min-h-0">
                                            <MapContainer
                                                projectId={currentProject.id}
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
                                    Group adjacent khasras into contiguous parcels for solar development. Only parcels larger than 50 hectares will be included in the final results.
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
                                                projectId={currentProject.id}
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
                                        <div className="space-y-4">
                                            {/* Fullscreen Online Map */}
                                            <div className="w-full p-6 border-2 border-slate-200 rounded-lg rounded-lg px-4 py-4">

                                                <div className="flex items-start gap-4">
                                                    <Globe className="w-8 h-8 text-blue-600 flex-shrink-0" />
                                                    <div>
                                                        <h4 className="text-base font-semibold text-slate-900">Online Map</h4>
                                                        <p className="text-xs text-slate-600 mt-1">Full screen version of the online map</p>
                                                        <div className="flex items-start justify-start gap-3 mt-4">
                                                            <button
                                                                onClick={handleOpenMapInNewTab}
                                                                className="flex items-center gap-1.5 p-3 bg-blue-600 hover:bg-blue-700 text-white text-xs font-medium rounded transition-colors"
                                                            >
                                                                <ExternalLink className="w-3.5 h-3.5" />
                                                                Open
                                                            </button>
                                                            <button
                                                                onClick={handleCopyMapLink}
                                                                className="flex items-center gap-1.5 p-3 bg-slate-100 hover:bg-slate-200 text-slate-700 text-xs font-medium rounded transition-colors"
                                                            >
                                                                {mapLinkCopied ? (
                                                                    <>
                                                                        <Link className="w-3.5 h-3.5" />
                                                                        Copied
                                                                    </>
                                                                ) : (
                                                                    <>
                                                                        <Link className="w-3.5 h-3.5" />
                                                                        Copy
                                                                    </>
                                                                )}
                                                            </button>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
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
                                    {/* Map */}
                                    {khasraGeoJSON ? (
                                        <div className="flex-1 min-h-0">
                                            <MapContainer
                                                projectId={currentProject.id}
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
                            onClick={() => updateCurrentPage(Math.max(1, currentPage - 1))}
                            disabled={isProcessing}
                            className="flex items-center gap-2 px-6 py-3 border border-blue-600 hover:bg-blue-100 disabled:bg-slate-400 disabled:border-slate-400 disabled:text-white text-blue-600 font-semibold rounded-lg transition-colors"
                        >
                            <ChevronLeft className="w-5 h-5" />
                            Previous
                        </button>
                    )}

                    {currentPage < 4 && (
                        <button
                            onClick={() => updateCurrentPage(Math.min(4, currentPage + 1))}
                            disabled={
                                isProcessing ||
                                (currentPage === 1 && !canProceedToLayerSelection) ||
                                (currentPage === 2 && !canProceedToClustering) ||
                                (currentPage === 3 && !canProceedToExport)
                            }
                            className="flex items-center gap-2 px-6 py-3 border border-blue-600 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-400 disabled:border-slate-400 text-white font-semibold rounded-lg transition-colors ml-auto"
                        >
                            Next
                            <ChevronRight className="w-5 h-5" />
                        </button>
                    )}
                </div>
            </div>

            {/* Delete Settlement Layers Modal */}
            {showDeleteSettlementModal && (
                <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[1000]">
                    <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 shadow-xl relative z-[1001]">
                        <div className="flex items-center gap-3 mb-4">
                            <AlertTriangle className="w-6 h-6 text-red-600" />
                            <h3 className="text-lg font-semibold text-slate-900">Delete Settlement Layers?</h3>
                        </div>
                        <div className="mb-6">
                            <p className="text-slate-700 mb-3">
                                This will permanently delete both the Settlements and Isolated Buildings layers.
                            </p>
                            <p className="text-red-600 font-medium text-sm">This action cannot be undone.</p>
                        </div>
                        <div className="flex gap-3">
                            <button
                                onClick={() => setShowDeleteSettlementModal(false)}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-900 font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleDeleteSettlementLayers}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-red-600 hover:bg-red-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                {isDeletingLayer ? "Deleting..." : "Delete Layers"}
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Delete Cropland Layer Modal */}
            {showDeleteCroplandModal && (
                <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[1000]">
                    <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 shadow-xl relative z-[1001]">
                        <div className="flex items-center gap-3 mb-4">
                            <AlertTriangle className="w-6 h-6 text-red-600" />
                            <h3 className="text-lg font-semibold text-slate-900">Delete Cropland Layer?</h3>
                        </div>
                        <div className="mb-6">
                            <p className="text-slate-700 mb-3">
                                This will permanently delete the Cropland layer.
                            </p>
                            <p className="text-red-600 font-medium text-sm">This action cannot be undone.</p>
                        </div>
                        <div className="flex gap-3">
                            <button
                                onClick={() => setShowDeleteCroplandModal(false)}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-900 font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleDeleteCroplandLayer}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-red-600 hover:bg-red-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                {isDeletingLayer ? "Deleting..." : "Delete Layer"}
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Delete Water Layer Modal */}
            {showDeleteWaterModal && (
                <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[1000]">
                    <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 shadow-xl relative z-[1001]">
                        <div className="flex items-center gap-3 mb-4">
                            <AlertTriangle className="w-6 h-6 text-red-600" />
                            <h3 className="text-lg font-semibold text-slate-900">Delete Water Layer?</h3>
                        </div>
                        <div className="mb-6">
                            <p className="text-slate-700 mb-3">
                                This will permanently delete the Water layer.
                            </p>
                            <p className="text-red-600 font-medium text-sm">This action cannot be undone.</p>
                        </div>
                        <div className="flex gap-3">
                            <button
                                onClick={() => setShowDeleteWaterModal(false)}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-900 font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleDeleteWaterLayer}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-red-600 hover:bg-red-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                {isDeletingLayer ? "Deleting..." : "Delete Layer"}
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Delete Slopes Layer Modal */}
            {showDeleteSlopesModal && (
                <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[1000]">
                    <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 shadow-xl relative z-[1001]">
                        <div className="flex items-center gap-3 mb-4">
                            <AlertTriangle className="w-6 h-6 text-red-600" />
                            <h3 className="text-lg font-semibold text-slate-900">Delete Slopes Layers?</h3>
                        </div>
                        <div className="mb-6">
                            <p className="text-slate-700 mb-3">
                                This will permanently delete all slopes layers (north-facing and other-facing).
                            </p>
                            <p className="text-red-600 font-medium text-sm">This action cannot be undone.</p>
                        </div>
                        <div className="flex gap-3">
                            <button
                                onClick={() => setShowDeleteSlopesModal(false)}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-900 font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={handleDeleteSlopesLayer}
                                disabled={isDeletingLayer}
                                className="flex-1 px-4 py-2 bg-red-600 hover:bg-red-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
                            >
                                {isDeletingLayer ? "Deleting..." : "Delete Layers"}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </main>
    )
}
