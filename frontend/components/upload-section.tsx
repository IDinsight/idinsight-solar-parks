"use client"

import type React from "react"

import { useEffect, useRef, useState } from "react"
import { FileUp, AlertTriangle, Trash2 } from "lucide-react"
import MapComponent from "./map-container"
import { getKhasrasSummary, deleteKhasras } from "@/lib/api/services"
import { useProjectStore } from "@/lib/stores/project"
import type { KhasraSummary } from "@/lib/api/types"
import apiClient from "@/lib/api/client"

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
      if (fileName.endsWith(".kml") || fileName.endsWith(".geojson") || fileName.endsWith(".json") || fileName.endsWith(".parquet")) {
        parseFilePreview(file)
      } else {
        alert("Please select a valid KML, GeoJSON, or Parquet file")
      }
    }
  }

  const handleDragAndDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.stopPropagation()

    const file = e.dataTransfer.files?.[0]
    if (file) {
      const fileName = file.name.toLowerCase()
      if (fileName.endsWith(".kml") || fileName.endsWith(".geojson") || fileName.endsWith(".json") || fileName.endsWith(".parquet")) {
        parseFilePreview(file)
      } else {
        alert("Please drop a valid KML, GeoJSON, or Parquet file")
      }
    }
  }

  const parseFilePreview = async (file: File) => {
    const fileName = file.name.toLowerCase()

    if (fileName.endsWith(".kml")) {
      parseKMLPreview(file)
    } else if (fileName.endsWith(".geojson") || fileName.endsWith(".json")) {
      parseGeoJSONPreview(file)
    } else if (fileName.endsWith(".parquet")) {
      parseParquetPreview(file)
    }
  }

  // Helper function to extract unique columns from features
  const extractColumns = (features: any[]): string[] => {
    const allColumns = new Set<string>()
    features.forEach((f: any) => {
      if (f.properties) {
        Object.keys(f.properties).forEach((col) => allColumns.add(col))
      }
    })
    return Array.from(allColumns).sort()
  }

  // Helper function to calculate map center from features
  const calculateMapCenter = (features: any[]): void => {
    const coords = features.flatMap((f: any) => {
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
  }

  // Helper function to set preview data with feature limit
  const setLimitedPreview = (allFeatures: any[], file: File, maxPreview: number = 1000): void => {
    const totalCount = allFeatures.length
    const previewFeatures = allFeatures.slice(0, maxPreview)

    const columnList = extractColumns(previewFeatures)
    setColumns(columnList)

    calculateMapCenter(previewFeatures)

    setPreviewData({
      type: "FeatureCollection",
      features: previewFeatures,
      file,
      total_count: totalCount,
      preview_count: previewFeatures.length,
    })
  }

  const parseParquetPreview = async (file: File) => {
    try {
      if (!currentProject?.id) {
        alert("No project selected")
        return
      }

      // Call backend preview endpoint
      const formData = new FormData()
      formData.append("file", file)

      const response = await apiClient.post(
        `/projects/${currentProject.id}/khasras/preview`,
        formData,
        {
          headers: {
            "Content-Type": "multipart/form-data",
          },
        }
      )

      const data = response.data

      // Set columns from API response
      setColumns(data.columns || [])

      // Calculate map center from features
      if (data.features && data.features.length > 0) {
        calculateMapCenter(data.features)
      }

      // Set preview data
      setPreviewData({
        type: "FeatureCollection",
        features: data.features || [],
        file,
        total_count: data.total_count,
        preview_count: data.preview_count,
      })
    } catch (error) {
      console.error("Error previewing parquet file:", error)
      alert(`Error previewing parquet file: ${error instanceof Error ? error.message : 'Unknown error'}`)
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

      setLimitedPreview(geojson.features, file)
    } catch (error) {
      console.error("Error parsing GeoJSON file:", error)
      alert("Error parsing GeoJSON file. Please ensure it is valid GeoJSON format.")
    }
  }

  const parseKMLPreview = async (file: File) => {
    try {
      const kmlContent = await file.text()
      const parser = new DOMParser()
      const kmlDoc = parser.parseFromString(kmlContent, "application/xml")

      // Check for XML parsing errors
      const parserError = kmlDoc.querySelector("parsererror")
      if (parserError) {
        console.error("XML parsing error:", parserError.textContent)
        alert("Error parsing KML file: Invalid XML format. " + parserError.textContent)
        return
      }

      const placemarks = Array.from(kmlDoc.querySelectorAll("Placemark"))

      if (placemarks.length === 0) {
        console.error("No Placemarks found in KML file")
        alert("Error: No Placemarks found in KML file. Please ensure it is a valid KML file with geometry data.")
        return
      }

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

          const geometry = parseKMLGeometry(pm)

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

      setLimitedPreview(features, file)
    } catch (error) {
      console.error("Error parsing KML file:", error)
      const errorMessage = error instanceof Error ? error.message : "Unknown error"
      alert(`Error parsing KML file: ${errorMessage}\n\nPlease check the browser console for more details.`)
    }
  }

  // Helper function to parse KML geometry from a Placemark element
  const parseKMLGeometry = (placemark: any): any => {
    const pointEl = placemark.querySelector("Point")
    const lineEl = placemark.querySelector("LineString")
    const polygonEl = placemark.querySelector("Polygon")

    if (pointEl) {
      return parseKMLPoint(pointEl)
    } else if (lineEl) {
      return parseKMLLineString(lineEl)
    } else if (polygonEl) {
      return parseKMLPolygon(polygonEl)
    }
    return null
  }

  // Helper function to parse KML Point
  const parseKMLPoint = (pointEl: any): any => {
    const coordsText = pointEl.querySelector("coordinates")?.textContent?.trim() || ""
    const coords = coordsText.split(",").map((c: string) => c.trim())
    // KML coordinates can have altitude (lng,lat,alt), we only need lng,lat
    if (coords.length >= 2) {
      return {
        type: "Point",
        coordinates: [Number.parseFloat(coords[0]), Number.parseFloat(coords[1])],
      }
    }
    return null
  }

  // Helper function to parse KML LineString
  const parseKMLLineString = (lineEl: any): any => {
    const coordsText = lineEl.querySelector("coordinates")?.textContent?.trim() || ""
    const coords = parseKMLCoordinates(coordsText)
    if (coords.length > 0) {
      return {
        type: "LineString",
        coordinates: coords,
      }
    }
    return null
  }

  // Helper function to parse KML Polygon
  const parseKMLPolygon = (polygonEl: any): any => {
    const coordsText =
      polygonEl.querySelector("outerBoundaryIs LinearRing coordinates")?.textContent?.trim() || ""
    const coords = parseKMLCoordinates(coordsText)
    if (coords.length > 0) {
      return {
        type: "Polygon",
        coordinates: [coords],
      }
    }
    return null
  }

  // Helper function to parse KML coordinate string
  const parseKMLCoordinates = (coordsText: string): number[][] => {
    return coordsText
      .split(/[\s\n]+/)
      .filter((c: string) => c.trim())
      .map((c: string) => {
        const parts = c.split(",").map((p: string) => p.trim())
        // Take only lng,lat, ignore altitude if present
        return [Number.parseFloat(parts[0]), Number.parseFloat(parts[1])]
      })
      .filter((c: number[]) => !isNaN(c[0]) && !isNaN(c[1]))
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
          {/* Warning banner for limited preview */}
          {previewData.total_count && previewData.preview_count && previewData.preview_count < previewData.total_count && (
            <div className="bg-amber-50 border border-amber-300 rounded-lg p-4 flex items-start gap-3">
              <AlertTriangle className="w-5 h-5 text-amber-600 flex-shrink-0 mt-0.5" />
              <div className="text-sm">
                <p className="font-semibold text-amber-900 mb-1">Limited Preview</p>
                <p className="text-amber-800">
                  Showing {previewData.preview_count.toLocaleString()} of {previewData.total_count.toLocaleString()} rows for preview. All rows will be processed when you confirm the upload.
                </p>
              </div>
            </div>
          )}
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
              projectId={currentProject?.id || ""}
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

      {/* Existing Khasras Display */}
      {existingKhasras && existingKhasras.exists && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Left side: Info and Actions */}
          <div className="lg:col-span-1 space-y-6">
            {/* Info Banner */}
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-blue-900 mb-4">Khasras Already Uploaded</h3>
              <div className="text-sm text-blue-800 space-y-2">
                <p>
                  <span className="font-medium">Shape count:</span> {existingKhasras.count}
                </p>
                <p>
                  <span className="font-medium">Total area:</span> {existingKhasras.total_area_ha?.toFixed(2)} hectares
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
          <div className="lg:col-span-2 h-[500px]">
            <div className="rounded-lg overflow-hidden bg-slate-50 w-full h-full relative z-0">
              {existingKhasras.geojson && (
                <MapComponent
                  projectId={currentProject?.id || ""}
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
            <FileUp className="w-12 h-12 text-slate-400 mx-auto mb-4" />
            <p className="text-base text-slate-600 font-medium">
              Drag & drop your KML, GeoJSON, or Parquet file here
            </p>
            <p className="text-sm text-slate-500 mt-2">or click to browse</p>
          </div>

          <input
            ref={fileInputRef}
            type="file"
            accept=".kml,.geojson,.json,.parquet"
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
