"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { ProtectedRoute } from "@/components/protected-route"
import { useProjectStore } from "@/lib/stores/project"
import UploadSection from "@/components/upload-section"
import LayerSelector from "@/components/layer-selector"
import ClusteringSection from "@/components/clustering-section"
import MapContainer from "@/components/map-container"
import * as api from "@/lib/api/services"
import { ExportFormat, ExportType } from "@/lib/api/types"
import { ChevronLeft, ChevronRight, Download, ArrowLeft, AlertCircle } from "lucide-react"

export default function WorkflowPage() {
    return (
        <ProtectedRoute>
            <WorkflowContent />
        </ProtectedRoute>
    )
}

function WorkflowContent() {
    const router = useRouter()
    const { currentProject, updateProject } = useProjectStore()

    const [currentPage, setCurrentPage] = useState(1)
    const [isProcessing, setIsProcessing] = useState(false)
    const [error, setError] = useState<string | null>(null)

    // Step 1: Upload khasras
    const [khasraFile, setKhasraFile] = useState<File | null>(null)
    const [khasraData, setKhasraData] = useState<any>(null)
    const [idColumn, setIdColumn] = useState<string>("")
    const [uploadComplete, setUploadComplete] = useState(false)

    // Step 2: Layers
    const [availableLayers, setAvailableLayers] = useState<string[]>(["Buildings & Settlements"])
    const [selectedLayers, setSelectedLayers] = useState<string[]>([])
    const [layersComplete, setLayersComplete] = useState(false)
    const [layerData, setLayerData] = useState<any>(null)
    const [layersGeoJSON, setLayersGeoJSON] = useState<Record<string, any> | null>(null)

    // Step 3: Clustering
    const [clusteringComplete, setClusteringComplete] = useState(false)
    const [clusterResult, setClusterResult] = useState<any>(null)

    // Map state
    const [mapCenter, setMapCenter] = useState<[number, number]>([20, 0])
    const [mapZoom, setMapZoom] = useState(5)

    // Fetch layers GeoJSON when on page 2 or later
    useEffect(() => {
        const fetchLayersGeoJSON = async () => {
            if (currentProject?.id && currentPage >= 2) {
                try {
                    const layers = await api.getProjectLayersGeoJSON(currentProject.id)
                    setLayersGeoJSON(layers)
                } catch (error) {
                    console.error("Error fetching layers GeoJSON:", error)
                }
            }
        }
        fetchLayersGeoJSON()
    }, [currentProject?.id, currentPage, layersComplete])


    useEffect(() => {
        if (!currentProject) {
            router.push('/dashboard')
            return
        }

        // Only load project state on initial mount
        const loadInitialState = async () => {
            try {
                const project = await api.getProject(currentProject.id)
                updateProject(project)

                // Determine which page to start on based on project status
                if (project.khasra_count && project.khasra_count > 0) {
                    setUploadComplete(true)
                    if (project.layers_added && project.layers_added.length > 0) {
                        setLayersComplete(true)
                        // Check if clustering is done
                        if (project.status === 'clustered' || project.status === 'completed') {
                            setClusteringComplete(true)
                            setCurrentPage(3) // Start at clustering page, user can proceed to export
                        } else {
                            setCurrentPage(3)
                        }
                    } else {
                        setCurrentPage(2)
                    }
                }
            } catch (error) {
                console.error('Failed to load project:', error)
            }
        }

        loadInitialState()
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []) // Only run once on mount

    const handleFileUpload = async (file: File, data: any, uniqueIdColumn: string) => {
        if (!currentProject) return

        setIsProcessing(true)
        setError(null)

        try {
            // Upload khasras to backend
            const response = await api.uploadKhasras(currentProject.id, file, uniqueIdColumn)

            setKhasraFile(file)
            setKhasraData(data)
            setIdColumn(uniqueIdColumn)
            setUploadComplete(true)

            // Update map center from bounds
            if (response.bounds) {
                const centerLng = (response.bounds.minx + response.bounds.maxx) / 2
                const centerLat = (response.bounds.miny + response.bounds.maxy) / 2
                setMapCenter([centerLat, centerLng])
                setMapZoom(10)
            }

            // Refresh project data
            const project = await api.getProject(currentProject.id)
            updateProject(project)

            setCurrentPage(2)
        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to upload khasras')
            console.error("Error uploading khasras:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    const handleLayerChange = (layer: string) => {
        setSelectedLayers((prev) =>
            prev.includes(layer) ? prev.filter((l) => l !== layer) : [...prev, layer]
        )
    }

    const handleAddLayers = async () => {
        if (!currentProject || selectedLayers.length === 0) return

        setIsProcessing(true)
        setError(null)

        try {
            // Generate settlement layers which creates both buildings and settlements
            if (selectedLayers.includes("Buildings & Settlements")) {
                const layerResult = await api.generateSettlementLayer(currentProject.id, {
                    building_buffer: 10,
                    settlement_eps: 50,
                    min_buildings: 5,
                })

                // Store layer info for display
                setLayerData(layerResult)
            }

            // Calculate areas after adding layers
            await api.calculateAreas(currentProject.id)

            setLayersComplete(true)

            // Refresh project data
            const project = await api.getProject(currentProject.id)
            updateProject(project)

        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to add layers')
            console.error("Error adding layers:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    const handleRunClustering = async (distanceThreshold: number, minSamples: number = 2) => {
        if (!currentProject) return

        setIsProcessing(true)
        setError(null)

        try {
            const result = await api.clusterKhasras(currentProject.id, {
                distance_threshold: distanceThreshold,
                min_samples: minSamples,
            })

            setClusterResult(result)
            setClusteringComplete(true)

            // Refresh project data
            const project = await api.getProject(currentProject.id)
            updateProject(project)

        } catch (error: any) {
            setError(error.response?.data?.detail || 'Failed to run clustering')
            console.error("Error clustering:", error)
        } finally {
            setIsProcessing(false)
        }
    }

    const handleExport = async (format: ExportFormat, type: ExportType) => {
        if (!currentProject) return

        setIsProcessing(true)

        try {
            const blob = await api.exportData(currentProject.id, {
                export_type: type,
                format: format,
                include_statistics: true,
            })

            // Create download link
            const url = window.URL.createObjectURL(blob)
            const a = document.createElement('a')
            a.href = url

            // Determine file extension
            const extensions: Record<ExportFormat, string> = {
                [ExportFormat.GEOJSON]: 'geojson',
                [ExportFormat.KML]: 'kml',
                [ExportFormat.SHAPEFILE]: 'zip',
                [ExportFormat.PARQUET]: 'parquet',
                [ExportFormat.CSV]: 'zip',
                [ExportFormat.EXCEL]: 'xlsx',
            }

            // if export type is all, use zip extension unless it's excel
            if (type === ExportType.ALL && format !== ExportFormat.EXCEL) {
                extensions[format] = 'zip'
            }

            a.download = `${currentProject.name}-${type}.${extensions[format]}`
            document.body.appendChild(a)
            a.click()
            document.body.removeChild(a)
            window.URL.revokeObjectURL(url)
        } catch (error: any) {
            alert(error.response?.data?.detail || 'Failed to export data')
        } finally {
            setIsProcessing(false)
        }
    }

    if (!currentProject) {
        return <div>Loading...</div>
    }

    const canProceedToPage2 = uploadComplete
    const canProceedToPage3 = uploadComplete && layersComplete
    const canProceedToPage4 = uploadComplete && layersComplete && clusteringComplete

    return (
        <main className="min-h-screen bg-slate-50">
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

            <div className="container mx-auto max-w-6xl px-6 py-12">
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
                <div className="flex gap-4 mb-12">
                    {[
                        { number: 1, label: "Upload Khasras" },
                        { number: 2, label: "Add Layers" },
                        { number: 3, label: "Clustering" },
                        { number: 4, label: "Export" },
                    ].map((step) => (
                        <div
                            key={step.number}
                            className={`flex-1 py-4 px-6 rounded-lg text-center font-semibold transition-all ${currentPage === step.number
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
                <div className="bg-white rounded-lg p-8 mb-8 min-h-96">
                    {/* Page 1: Upload */}
                    {currentPage === 1 && (
                        <div className="space-y-8">
                            <div>
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 1: Upload Khasra Boundaries</h2>
                                <p className="text-base text-slate-600">Upload your KML or GeoJSON file containing land parcel boundaries</p>
                            </div>
                            <UploadSection onFileUpload={handleFileUpload} isProcessing={isProcessing} />
                        </div>
                    )}

                    {/* Page 2: Layer Selection */}
                    {currentPage === 2 && (
                        <div className="space-y-8">
                            <div>
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 2: Add Constraint Layers</h2>
                                <p className="text-base text-slate-600">Select which constraint layers to overlay on your khasras</p>
                            </div>

                            {/* Layer Results Banner */}
                            {layersComplete && layerData && (
                                <div className="p-6 bg-green-50 border-2 border-green-200 rounded-lg">
                                    <div className="flex items-center gap-3 mb-4">
                                        <div className="w-8 h-8 bg-green-500 rounded-full flex items-center justify-center">
                                            <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                                            </svg>
                                        </div>
                                        <h3 className="text-lg font-semibold text-green-900">Layers Added Successfully!</h3>
                                    </div>
                                    <div className="space-y-2">
                                        {layerData.layers_added?.map((layer: any, idx: number) => (
                                            <div key={idx} className="text-sm text-green-800">
                                                <span className="font-semibold">{layer.name}:</span> {layer.feature_count} features, {layer.area_ha?.toFixed(2)} ha
                                            </div>
                                        ))}
                                    </div>
                                    <p className="text-sm text-green-700 mt-4">
                                        ✓ Constraint layers have been added to the map. Click Next to proceed to clustering.
                                    </p>
                                </div>
                            )}

                            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                                <div className="lg:col-span-1">
                                    <div className="space-y-4">
                                        <h3 className="font-semibold text-slate-900">Available Layers</h3>
                                        {availableLayers.map((layer) => (
                                            <label
                                                key={layer}
                                                className="flex items-center gap-3 cursor-pointer hover:bg-slate-50 p-3 rounded-lg transition-colors"
                                            >
                                                <input
                                                    type="checkbox"
                                                    checked={selectedLayers.includes(layer)}
                                                    onChange={() => handleLayerChange(layer)}
                                                    // disabled={layersComplete}
                                                    className="w-4 h-4 text-blue-600 rounded"
                                                />
                                                <span className="text-sm text-slate-700 font-medium">{layer}</span>
                                            </label>
                                        ))}

                                        {selectedLayers.length === 0 && !layersComplete && (
                                            <p className="text-xs text-amber-600 bg-amber-50 p-3 rounded border border-amber-200">
                                                Select at least one layer to continue
                                            </p>
                                        )}

                                        <button
                                            onClick={handleAddLayers}
                                            disabled={isProcessing || selectedLayers.length === 0}
                                            className="w-full px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white font-semibold rounded-lg transition-colors"
                                        >
                                            {isProcessing ? 'Processing...' : layersComplete ? 'Rerun Layers' : 'Add Layers'}
                                        </button>
                                    </div>
                                </div>
                                <div className="lg:col-span-2 h-[500px]">
                                    {khasraData ? (
                                        <MapContainer
                                            data={khasraData}
                                            selectedLayers={selectedLayers}
                                            center={mapCenter}
                                            zoom={mapZoom}
                                            clusters={[]}
                                            layersData={layersGeoJSON || undefined}
                                        />
                                    ) : (
                                        <div className="w-full h-full bg-slate-100 flex items-center justify-center rounded-lg border-2 border-dashed border-slate-300">
                                            <p className="text-slate-500">No data available</p>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Page 3: Clustering */}
                    {currentPage === 3 && (
                        <div className="space-y-8">
                            <div>
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 3: Cluster Khasras into Parcels</h2>
                                <p className="text-base text-slate-600">
                                    Group adjacent khasras into contiguous parcels for solar development
                                </p>
                            </div>
                            <div className="space-y-6">
                                {/* Clustering Results Banner */}
                                {clusteringComplete && clusterResult && (
                                    <div className="p-6 bg-green-50 border-2 border-green-200 rounded-lg">
                                        <div className="flex items-center gap-3 mb-4">
                                            <div className="w-8 h-8 bg-green-500 rounded-full flex items-center justify-center">
                                                <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                                                </svg>
                                            </div>
                                            <h3 className="text-lg font-semibold text-green-900">Clustering Complete!</h3>
                                        </div>
                                        <div className="grid grid-cols-3 gap-4">
                                            <div>
                                                <p className="text-2xl font-bold text-green-700">{clusterResult.total_parcels}</p>
                                                <p className="text-sm text-green-600">Parcels Created</p>
                                            </div>
                                            <div>
                                                <p className="text-2xl font-bold text-green-700">{clusterResult.clustered_khasras}</p>
                                                <p className="text-sm text-green-600">Khasras Clustered</p>
                                            </div>
                                            <div>
                                                <p className="text-2xl font-bold text-slate-600">{clusterResult.unclustered_khasras}</p>
                                                <p className="text-sm text-slate-600">Unclustered</p>
                                            </div>
                                        </div>
                                        <p className="text-sm text-green-700 mt-4">
                                            ✓ Parcel boundaries have been created and are displayed on the map. Click Next to export results.
                                        </p>
                                    </div>
                                )}

                                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                                    <div className="lg:col-span-1">
                                        <ClusteringSection
                                            data={khasraData}
                                            isProcessing={isProcessing}
                                            clusteringComplete={clusteringComplete}
                                            onClusteringComplete={(result: any) => {
                                                handleRunClustering(result.distanceThreshold, result.minSamples)
                                            }}
                                        />
                                    </div>
                                    <div className="lg:col-span-2 h-[500px]">
                                        {khasraData ? (
                                            <MapContainer
                                                data={khasraData}
                                                selectedLayers={selectedLayers}
                                                center={mapCenter}
                                                zoom={mapZoom}
                                                clusters={clusterResult?.parcels || []}
                                                layersData={layersGeoJSON || undefined}
                                            />
                                        ) : (
                                            <div className="w-full h-full bg-slate-100 flex items-center justify-center rounded-lg border-2 border-dashed border-slate-300">
                                                <p className="text-slate-500">No data available</p>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}

                    {/* Page 4: Export */}
                    {currentPage === 4 && (
                        <div className="space-y-8">
                            <div>
                                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 4: Export Results</h2>
                                <p className="text-base text-slate-600">Download your analysis results in various formats</p>
                            </div>

                            {clusterResult && (
                                <div className="grid grid-cols-3 gap-4 mb-8 p-6 bg-blue-50 rounded-lg">
                                    <div className="text-center">
                                        <p className="text-3xl font-bold text-blue-600">{clusterResult.total_parcels}</p>
                                        <p className="text-sm text-slate-600 mt-1">Total Parcels</p>
                                    </div>
                                    <div className="text-center">
                                        <p className="text-3xl font-bold text-green-600">{clusterResult.clustered_khasras}</p>
                                        <p className="text-sm text-slate-600 mt-1">Clustered Khasras</p>
                                    </div>
                                    <div className="text-center">
                                        <p className="text-3xl font-bold text-slate-600">{clusterResult.unclustered_khasras}</p>
                                        <p className="text-sm text-slate-600 mt-1">Unclustered</p>
                                    </div>
                                </div>
                            )}

                            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 max-w-2xl mx-auto">
                                {/* KML */}
                                <button
                                    onClick={() => handleExport(ExportFormat.KML, ExportType.ALL)}
                                    disabled={isProcessing}
                                    className="p-8 border-2 border-slate-200 rounded-lg hover:border-green-500 hover:bg-green-50 transition-all text-center space-y-4"
                                >
                                    <Download className="w-12 h-12 text-green-600 mx-auto" />
                                    <div>
                                        <h3 className="text-xl font-semibold text-slate-900">Download KML</h3>
                                        <p className="text-sm text-slate-600 mt-2">All layers for Google Earth</p>
                                        <p className="text-xs text-slate-500 mt-1">Includes khasras, parcels, and constraint layers</p>
                                    </div>
                                </button>

                                {/* Excel */}
                                <button
                                    onClick={() => handleExport(ExportFormat.EXCEL, ExportType.ALL)}
                                    disabled={isProcessing}
                                    className="p-8 border-2 border-slate-200 rounded-lg hover:border-emerald-500 hover:bg-emerald-50 transition-all text-center space-y-4"
                                >
                                    <Download className="w-12 h-12 text-emerald-600 mx-auto" />
                                    <div>
                                        <h3 className="text-xl font-semibold text-slate-900">Download Excel</h3>
                                        <p className="text-sm text-slate-600 mt-2">Complete statistics workbook</p>
                                        <p className="text-xs text-slate-500 mt-1">Multiple sheets with detailed analysis</p>
                                    </div>
                                </button>
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
                            className="flex items-center gap-2 px-6 py-3 bg-slate-600 hover:bg-slate-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors"
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
                                (currentPage === 1 && !canProceedToPage2) ||
                                (currentPage === 2 && !canProceedToPage3) ||
                                (currentPage === 3 && !canProceedToPage4)
                            }
                            className="flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors ml-auto"
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
