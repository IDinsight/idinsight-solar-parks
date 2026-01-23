"use client"

import type React from "react"

import { useEffect, useRef, useState } from "react"
import { Upload, FileUp, AlertTriangle, Trash2 } from "lucide-react"
import MapComponent from "./map-container"
import { getKhasrasSummary, deleteKhasras } from "@/lib/api/services"
import { useProjectStore } from "@/lib/stores/project"
import type { KhasraSummary } from "@/lib/api/types"

interface UploadSectionProps {
  onFileUpload: (file: File, data: any, uniqueIdColumn: string) => void
  onKhasraDeleted?: () => void
  isProcessing: boolean
}

export default function UploadSection({ onFileUpload, onKhasraDeleted, isProcessing }: UploadSectionProps) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [previewData, setPreviewData] = useState<any>(null)
  const [selectedIdColumn, setSelectedIdColumn] = useState<string>("")
  const [columns, setColumns] = useState<string[]>([])
  const [mapCenter, setMapCenter] = useState<[number, number]>([20, 0])
  const [mapZoom, setMapZoom] = useState(2)
  const [existingKhasras, setExistingKhasras] = useState<KhasraSummary | null>(null)
  const [showDeleteModal, setShowDeleteModal] = useState(false)
  const [isDeleting, setIsDeleting] = useState(false)
  const { currentProject, updateProject } = useProjectStore()

  // Check for existing khasras on mount
  useEffect(() => {
    const checkExistingKhasras = async () => {
      if (currentProject?.id) {
        try {
          const summary = await getKhasrasSummary(currentProject.id)
          if (summary.exists) {
            setExistingKhasras(summary)
          }
        } catch (error) {
          console.error("Error checking existing khasras:", error)
        }
      }
    }
    checkExistingKhasras()
  }, [currentProject?.id])

  const handleDeleteKhasras = async () => {
    if (!currentProject?.id) return

    setIsDeleting(true)
    try {
      await deleteKhasras(currentProject.id)
      setExistingKhasras(null)
      setShowDeleteModal(false)
      // Refresh project to get updated status
      if (updateProject) {
        updateProject({ ...currentProject, status: "created", khasra_count: 0 })
      }
      // Notify parent component that khasras were deleted
      if (onKhasraDeleted) {
        onKhasraDeleted()
      }
    } catch (error) {
      console.error("Error deleting khasras:", error)
      alert("Failed to delete khasras. Please try again.")
    } finally {
      setIsDeleting(false)
    }
  }


  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      const fileName = file.name.toLowerCase()
      if (fileName.endsWith(".kml") || fileName.endsWith(".geojson") || fileName.endsWith(".json")) {
        parseFilePreview(file)
      } else {
        alert("Please select a valid KML or GeoJSON file")
      }
    }
  }

  const handleDragAndDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.stopPropagation()

    const file = e.dataTransfer.files?.[0]
    if (file) {
      const fileName = file.name.toLowerCase()
      if (fileName.endsWith(".kml") || fileName.endsWith(".geojson") || fileName.endsWith(".json")) {
        parseFilePreview(file)
      } else {
        alert("Please drop a valid KML or GeoJSON file")
      }
    }
  }

  const parseFilePreview = async (file: File) => {
    const fileName = file.name.toLowerCase()

    if (fileName.endsWith(".kml")) {
      parseKMLPreview(file)
    } else if (fileName.endsWith(".geojson") || fileName.endsWith(".json")) {
      parseGeoJSONPreview(file)
    }
  }

  const parseGeoJSONPreview = async (file: File) => {
    try {
      const content = await file.text()
      const geojson = JSON.parse(content)

      if (!geojson.features || !Array.isArray(geojson.features)) {
        alert("Invalid GeoJSON file. Must contain a 'features' array.")
        return
      }

      // Extract all unique column names from properties
      const allColumns = new Set<string>()
      geojson.features.forEach((f: any) => {
        if (f.properties) {
          Object.keys(f.properties).forEach((col) => allColumns.add(col))
        }
      })

      const columnList = Array.from(allColumns).sort()
      setColumns(columnList)

      // Calculate center from features
      const coords = geojson.features.flatMap((f: any) => {
        if (!f.geometry) return []
        if (f.geometry.type === "Point") return [f.geometry.coordinates]
        if (f.geometry.type === "LineString") return f.geometry.coordinates
        if (f.geometry.type === "Polygon") return f.geometry.coordinates[0]
        if (f.geometry.type === "MultiPolygon") return f.geometry.coordinates[0][0]
        return []
      })

      if (coords.length > 0) {
        const lngs = coords.map((c: number[]) => c[0])
        const lats = coords.map((c: number[]) => c[1])
        const centerLng = (Math.min(...lngs) + Math.max(...lngs)) / 2
        const centerLat = (Math.min(...lats) + Math.max(...lats)) / 2
        setMapCenter([centerLat, centerLng])
        setMapZoom(10)
      }

      setPreviewData({
        type: "FeatureCollection",
        features: geojson.features,
        file,
      })
    } catch (error) {
      console.error("Error parsing GeoJSON file:", error)
      alert("Error parsing GeoJSON file. Please ensure it is valid GeoJSON format.")
    }
  }

  const parseKMLPreview = async (file: File) => {
    try {
      // Read KML file directly as text
      const kmlContent = await file.text()

      if (kmlContent) {
        const parser = new DOMParser()
        const kmlDoc = parser.parseFromString(kmlContent, "application/xml")

        const placemarks = Array.from(kmlDoc.querySelectorAll("Placemark"))
        const features = placemarks
          .map((pm: any) => {
            const name = pm.querySelector("name")?.textContent || "Unnamed"
            const description = pm.querySelector("description")?.textContent || ""

            // Parse extended data if available
            const extDataElements = pm.querySelectorAll("ExtendedData Data")
            const extData: Record<string, string> = {}
            extDataElements.forEach((el: any) => {
              const key = el.getAttribute("name")
              const value = el.querySelector("value")?.textContent || ""
              if (key) extData[key] = value
            })

            const pointEl = pm.querySelector("Point")
            const lineEl = pm.querySelector("LineString")
            const polygonEl = pm.querySelector("Polygon")

            let geometry = null
            if (pointEl) {
              const coords = pointEl.querySelector("coordinates")?.textContent?.trim().split(",") || []
              geometry = {
                type: "Point",
                coordinates: [Number.parseFloat(coords[0] || 0), Number.parseFloat(coords[1] || 0)],
              }
            } else if (lineEl) {
              const coordsText = lineEl.querySelector("coordinates")?.textContent?.trim() || ""
              const coords = coordsText.split(/\s+/).map((c: string) => {
                const [lng, lat] = c.split(",")
                return [Number.parseFloat(lng), Number.parseFloat(lat)]
              })
              geometry = {
                type: "LineString",
                coordinates: coords,
              }
            } else if (polygonEl) {
              const coordsText =
                polygonEl.querySelector("outerBoundaryIs LinearRing coordinates")?.textContent?.trim() || ""
              const coords = coordsText.split(/\s+/).map((c: string) => {
                const [lng, lat] = c.split(",")
                return [Number.parseFloat(lng), Number.parseFloat(lat)]
              })
              geometry = {
                type: "Polygon",
                coordinates: [coords],
              }
            }

            return {
              type: "Feature",
              geometry,
              properties: {
                name,
                description,
                ...extData,
              },
            }
          })
          .filter((f) => f.geometry)

        // Extract all unique column names from properties
        const allColumns = new Set<string>()
        features.forEach((f: any) => {
          Object.keys(f.properties).forEach((col) => allColumns.add(col))
        })

        const columnList = Array.from(allColumns).sort()
        setColumns(columnList)

        // Calculate center from features
        const coords = features.flatMap((f: any) => {
          if (f.geometry.type === "Point") return [f.geometry.coordinates]
          if (f.geometry.type === "LineString") return f.geometry.coordinates
          if (f.geometry.type === "Polygon") return f.geometry.coordinates[0]
          return []
        })

        if (coords.length > 0) {
          const lngs = coords.map((c: number[]) => c[0])
          const lats = coords.map((c: number[]) => c[1])
          const centerLng = (Math.min(...lngs) + Math.max(...lngs)) / 2
          const centerLat = (Math.min(...lats) + Math.max(...lats)) / 2
          setMapCenter([centerLat, centerLng])
          setMapZoom(10)
        }

        setPreviewData({
          type: "FeatureCollection",
          features,
          file,
        })
      }
    } catch (error) {
      console.error("Error parsing KML file:", error)
      alert("Error parsing KML file. Please ensure it is a valid KML file.")
    }
  }

  const handleConfirm = async () => {
    if (previewData && selectedIdColumn) {
      await onFileUpload(previewData.file, previewData, selectedIdColumn)
      
      // After successful upload, clear preview and refresh existing khasras
      setPreviewData(null)
      setColumns([])
      setSelectedIdColumn("")
      
      // Fetch the newly uploaded khasras to show in "pre-existing" mode
      if (currentProject?.id) {
        try {
          const summary = await getKhasrasSummary(currentProject.id)
          if (summary.exists) {
            setExistingKhasras(summary)
          }
        } catch (error) {
          console.error("Error fetching uploaded khasras:", error)
        }
      }
    }
  }

  if (previewData) {
    const sampleFeatures = previewData.features.slice(0, 5)

    return (
      <div className="grid grid-cols-2 gap-8 h-full">
        {/* Left side: Data Preview and Actions */}
        <div className="flex flex-col gap-6">
          <div>
            <h3 className="text-lg font-semibold text-slate-900 mb-4">Data Preview</h3>
            <div className="overflow-x-auto rounded-lg border border-slate-200">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-300 bg-slate-50">
                    {columns.map((col) => (
                      <th
                        key={col}
                        className={`px-4 py-3 text-left font-semibold text-slate-900 ${col === selectedIdColumn ? "bg-blue-100" : ""
                          }`}
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sampleFeatures.map((feature: any, idx: number) => (
                    <tr key={idx} className="border-b border-slate-200 hover:bg-slate-50">
                      {columns.map((col) => (
                        <td
                          key={col}
                          className={`px-4 py-3 text-slate-700 ${col === selectedIdColumn ? "bg-blue-50 font-semibold" : ""
                            }`}
                        >
                          {feature.properties[col] || "-"}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-xs text-slate-500 mt-3">
              Showing {Math.min(5, sampleFeatures.length)} of {previewData.features.length} features
            </p>
          </div>

          <div className="bg-slate-50 rounded-lg p-6 border border-slate-200">
            <label className="block text-sm font-semibold text-slate-900 mb-3">
              Select Unique ID Column for Shapes
            </label>
            <select
              value={selectedIdColumn}
              onChange={(e) => setSelectedIdColumn(e.target.value)}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg bg-white text-slate-900 font-medium focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="" disabled>
                Please select...
              </option>
              {columns.map((col) => (
                <option key={col} value={col}>
                  {col}
                </option>
              ))}
            </select>
            <p className="text-xs text-slate-500 mt-2">
              This column will be used to uniquely identify each shape during clustering
            </p>
          </div>

          <div className="flex gap-4">
            <button
              onClick={() => {
                setPreviewData(null)
                setColumns([])
                setSelectedIdColumn("")
              }}
              className="flex-1 px-4 py-3 bg-slate-200 hover:bg-slate-300 text-slate-900 font-semibold rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleConfirm}
              disabled={!selectedIdColumn || isProcessing}
              className="flex-1 px-4 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isProcessing ? 'Uploading...' : 'Confirm'}
            </button>
          </div>
        </div>

        {/* Right side: Map Preview */}
        <div>
          <div className="rounded-lg overflow-hidden bg-slate-50 w-full h-[550px]">
            <MapComponent
              data={previewData}
              selectedLayers={["Buildings", "Settlements", "Crops", "Water", "Slopes", "Other"]}
              center={mapCenter}
              zoom={mapZoom}
            />
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full">
      <h2 className="text-lg font-semibold text-slate-900 mb-4 flex items-center gap-2">
        <FileUp className="w-5 h-5 text-blue-600" />
        Upload Khasra Boundaries
      </h2>

      {/* Existing Khasras Display */}
      {existingKhasras && existingKhasras.exists && (
        <div className="grid grid-cols-2 gap-8 h-full">
          {/* Left side: Info and Actions */}
          <div className="flex flex-col gap-6">
            {/* Info Banner */}
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-blue-900 mb-4">Khasras Already Uploaded</h3>
              <div className="text-sm text-blue-800 space-y-2">
                <p>
                  <span className="font-medium">Shape Count:</span> {existingKhasras.count}
                </p>
                <p>
                  <span className="font-medium">Total Area:</span> {existingKhasras.total_area_ha?.toFixed(2)} hectares
                </p>
                {existingKhasras.uploaded_at && (
                  <p>
                    <span className="font-medium">Uploaded on:</span>{" "}
                    {new Date(existingKhasras.uploaded_at).toLocaleString()}
                  </p>
                )}
              </div>
            </div>

            {/* Delete Action */}
            <div className="border border-red-200 rounded-lg p-6">
              <h3 className="text-sm font-semibold text-red-900 mb-3">Delete Khasras</h3>
              <p className="text-xs text-red-700 mb-4">
                To upload new khasras, you must first delete the existing ones. This will reset your project and remove all layers, clustering results, and statistics.
              </p>
              <button
                onClick={() => setShowDeleteModal(true)}
                className="w-full flex items-center justify-center gap-2 px-4 py-3 border border-red-600 hover:bg-red-100  text-red-600 disabled:bg-gray-100 disabled:text-gray-400 disabled:border-gray-400 font-semibold rounded-lg transition-colors"
              >
                <Trash2 className="w-4 h-4" />
                Delete All Khasras
              </button>
            </div>
          </div>

          {/* Right side: Map */}
          <div>
            <div className="rounded-lg overflow-hidden bg-slate-50 w-full h-[550px] relative z-0">
              {existingKhasras.geojson && (
                <MapComponent
                  data={existingKhasras.geojson}
                  selectedLayers={[]}
                  center={
                    existingKhasras.bounds
                      ? [
                          (existingKhasras.bounds.miny + existingKhasras.bounds.maxy) / 2,
                          (existingKhasras.bounds.minx + existingKhasras.bounds.maxx) / 2,
                        ]
                      : [20, 0]
                  }
                  zoom={existingKhasras.bounds ? 12 : 2}
                />
              )}
            </div>
          </div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteModal && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[100]">
          <div className="bg-white rounded-lg p-6 max-w-md w-full mx-4 shadow-xl relative z-[101]">
            <div className="flex items-center gap-3 mb-4">
              <AlertTriangle className="w-6 h-6 text-red-600" />
              <h3 className="text-lg font-semibold text-slate-900">Delete Khasras?</h3>
            </div>
            <div className="mb-6">
              <p className="text-slate-700 mb-3">
                This will permanently delete all khasras and reset your project. The following data will also be removed:
              </p>
              <ul className="list-disc list-inside space-y-1 text-sm text-slate-600">
                <li>Settlement layers</li>
                <li>Building layers</li>
                <li>Clustering results</li>
                <li>Generated statistics and exports</li>
              </ul>
              <p className="text-red-600 font-medium text-sm mt-3">This action cannot be undone.</p>
            </div>
            <div className="flex gap-3">
              <button
                onClick={() => setShowDeleteModal(false)}
                disabled={isDeleting}
                className="flex-1 px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-900 font-medium rounded-lg transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleDeleteKhasras}
                disabled={isDeleting}
                className="flex-1 px-4 py-2 bg-red-600 hover:bg-red-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
              >
                {isDeleting ? "Deleting..." : "Delete Everything"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Upload Area - only show if no existing khasras */}
      {!existingKhasras?.exists && (
        <>
          <div
            onDragOver={(e) => e.preventDefault()}
            onDrop={handleDragAndDrop}
            className="border-2 border-dashed border-slate-300 rounded-lg p-12 text-center hover:border-blue-500 hover:bg-blue-50 transition-colors cursor-pointer"
            onClick={() => fileInputRef.current?.click()}
          >
            <Upload className="w-12 h-12 text-slate-400 mx-auto mb-4" />
            <p className="text-base text-slate-600 font-medium">
              Drag & drop your KML or GeoJSON file here
            </p>
            <p className="text-sm text-slate-500 mt-2">or click to browse</p>
          </div>

          <input
            ref={fileInputRef}
            type="file"
            accept=".kml,.geojson,.json"
            onChange={handleFileChange}
            className="hidden"
            disabled={isProcessing}
          />

          <p className="text-xs text-slate-500 mt-4">✓ Supports .kml and .geojson files</p>
        </>
      )}
    </div>
  )
}
