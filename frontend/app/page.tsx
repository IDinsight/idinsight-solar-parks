"use client"

import { useState } from "react"
import UploadSection from "@/components/upload-section"
import LayerSelector from "@/components/layer-selector"
import ClusteringSection from "@/components/clustering-section"
import MapContainer from "@/components/map-container"
import { ChevronLeft, ChevronRight, Download } from "lucide-react"

const AVAILABLE_LAYERS = ["Buildings", "Settlements", "Crops", "Water", "Slopes"]

export default function Home() {
  const [currentPage, setCurrentPage] = useState(1)
  const [kmzData, setKmzData] = useState<any>(null)
  const [uniqueIdColumn, setUniqueIdColumn] = useState<string>("")
  const [selectedLayers, setSelectedLayers] = useState<string[]>(AVAILABLE_LAYERS)
  const [isProcessing, setIsProcessing] = useState(false)
  const [mapCenter, setMapCenter] = useState<[number, number]>([20, 0])
  const [mapZoom, setMapZoom] = useState(5)
  const [clusters, setClusters] = useState<any[]>([])

  const handleFileUpload = async (file: File, data: any, idColumn: string) => {
    setIsProcessing(true)
    try {
      setUniqueIdColumn(idColumn)
      setKmzData(data)

      // Calculate bounds for centering map
      if (data.features.length > 0) {
        const coords = data.features.flatMap((f: any) => {
          if (f.geometry.type === "Point") return [f.geometry.coordinates]
          if (f.geometry.type === "LineString") return f.geometry.coordinates
          if (f.geometry.type === "Polygon") return f.geometry.coordinates[0]
          return []
        })

        if (coords.length > 0) {
          const lngs = coords.map((c) => c[0])
          const lats = coords.map((c) => c[1])
          const centerLng = (Math.min(...lngs) + Math.max(...lngs)) / 2
          const centerLat = (Math.min(...lats) + Math.max(...lats)) / 2
          setMapCenter([centerLat, centerLng])
          setMapZoom(6)
        }
      }

      setCurrentPage(2)
    } catch (error) {
      console.error("Error processing KMZ file:", error)
      alert("Error processing KMZ file.")
    } finally {
      setIsProcessing(false)
    }
  }

  const handleLayerChange = (layer: string) => {
    setSelectedLayers((prev) => (prev.includes(layer) ? prev.filter((l) => l !== layer) : [...prev, layer]))
  }

  const handleClusteringComplete = (clusterData: any[]) => {
    setClusters(clusterData)
  }

  const canProceedToPage2 = !!kmzData
  const canProceedToPage3 = !!kmzData && selectedLayers.length > 0

  return (
    <main className="min-h-screen bg-slate-50">
      <div className="container mx-auto max-w-6xl px-6 py-12">
        {/* Header */}
        <div className="mb-12">
          <h1 className="text-4xl font-bold text-slate-900 mb-3">Solar Park Parcel Identification</h1>
          <p className="text-lg text-slate-600">
            Overlay unusable layers and cluster khasras to create usable parcels for solar park development.
          </p>
        </div>

        {/* Page Indicator */}
        <div className="flex gap-4 mb-12">
          {[
            { number: 1, label: "Load data" },
            { number: 2, label: "Add layers" },
            { number: 3, label: "Clustering" },
            { number: 4, label: "Download" },
          ].map((step) => (
            <div
              key={step.number}
              className={`flex-1 py-4 px-6 rounded-lg text-center font-semibold transition-all ${
                currentPage === step.number
                  ? "bg-blue-600 text-white"
                  : currentPage > step.number
                    ? "bg-slate-300 text-slate-500"
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
                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 1: Upload KML File</h2>
                <p className="text-base text-slate-600">Start by uploading your KML file from Google Earth</p>
              </div>
              <UploadSection onFileUpload={handleFileUpload} isProcessing={isProcessing} />
            </div>
          )}

          {/* Page 2: Layer Selection */}
          {currentPage === 2 && (
            <div className="space-y-8">
              <div>
                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 2: Select Layers</h2>
                <p className="text-base text-slate-600">Choose which layers to display on the map</p>
              </div>
              <LayerSelector
                layers={AVAILABLE_LAYERS}
                selectedLayers={selectedLayers}
                onLayerChange={handleLayerChange}
                kmzData={kmzData}
                mapCenter={mapCenter}
                mapZoom={mapZoom}
                clusters={clusters}
              />
            </div>
          )}

          {/* Page 3: Clustering */}
          {currentPage === 3 && (
            <div className="space-y-8">
              <div>
                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 3: Grouping Khasras</h2>
                <p className="text-base text-slate-600">
                  Group khasras by combining those closer than the given distance threshold
                </p>
              </div>
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
                <div className="lg:col-span-1">
                  <ClusteringSection data={kmzData} onClusteringComplete={handleClusteringComplete} />
                </div>
                <div className="lg:col-span-2 h-[500px]">
                  <MapContainer
                    data={kmzData}
                    selectedLayers={selectedLayers}
                    center={mapCenter}
                    zoom={mapZoom}
                    clusters={clusters}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Page 4: Download Results */}
          {currentPage === 4 && (
            <div className="space-y-8">
              <div>
                <h2 className="text-3xl font-bold text-slate-900 mb-2">Step 4: Download Results</h2>
                <p className="text-base text-slate-600">Download your clustering results in various formats</p>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* KMZ Download */}
                <button
                  onClick={() => {
                    const element = document.createElement("a")
                    element.href = "#"
                    element.download = "clustered-parcels.kmz"
                    element.click()
                  }}
                  className="p-8 border-2 border-slate-200 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-all text-center space-y-4"
                >
                  <Download className="w-12 h-12 text-blue-600 mx-auto" />
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900">Download KMZ</h3>
                    <p className="text-sm text-slate-600 mt-2">Google Earth format with clustering results</p>
                  </div>
                </button>

                {/* Excel Download */}
                <button
                  onClick={() => {
                    const element = document.createElement("a")
                    element.href = "#"
                    element.download = "clustered-parcels.xlsx"
                    element.click()
                  }}
                  className="p-8 border-2 border-slate-200 rounded-lg hover:border-green-500 hover:bg-green-50 transition-all text-center space-y-4"
                >
                  <Download className="w-12 h-12 text-green-600 mx-auto" />
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900">Download Excel</h3>
                    <p className="text-sm text-slate-600 mt-2">Spreadsheet with cluster details and statistics</p>
                  </div>
                </button>
              </div>

              <div className="p-6 bg-slate-100 rounded-lg border border-slate-300">
                <h4 className="font-semibold text-slate-900 mb-3">Files Generated:</h4>
                <ul className="space-y-2 text-sm text-slate-700">
                  <li>✓ clustered-parcels.kmz - Clustered boundaries in KMZ format</li>
                  <li>✓ clustered-parcels.xlsx - Cluster metadata and statistics</li>
                </ul>
              </div>
            </div>
          )}
        </div>

        {/* Navigation Buttons */}
        {currentPage > 1 && (
          <div className="flex gap-4 justify-between">
            <button
              onClick={() => setCurrentPage((prev) => Math.max(1, prev - 1))}
              disabled={currentPage === 1}
              className="flex items-center gap-2 px-6 py-3 bg-slate-600 hover:bg-slate-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors"
            >
              <ChevronLeft className="w-5 h-5" />
              Previous
            </button>

            <button
              onClick={() => setCurrentPage((prev) => Math.min(4, prev + 1))}
              disabled={
                (currentPage === 2 && !canProceedToPage3) ||
                currentPage === 4 ||
                (currentPage === 1 && !canProceedToPage2)
              }
              className="flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors"
            >
              Next
              <ChevronRight className="w-5 h-5" />
            </button>
          </div>
        )}
      </div>
    </main>
  )
}
