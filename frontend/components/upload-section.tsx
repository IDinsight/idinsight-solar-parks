"use client"

import type React from "react"

import { useRef, useState } from "react"
import { Upload, FileUp } from "lucide-react"
import MapComponent from "./map-container"

interface UploadSectionProps {
  onFileUpload: (file: File, data: any, uniqueIdColumn: string) => void
  isProcessing: boolean
}

export default function UploadSection({ onFileUpload, isProcessing }: UploadSectionProps) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [previewData, setPreviewData] = useState<any>(null)
  const [selectedIdColumn, setSelectedIdColumn] = useState<string>("")
  const [columns, setColumns] = useState<string[]>([])
  const [mapCenter, setMapCenter] = useState<[number, number]>([20, 0])
  const [mapZoom, setMapZoom] = useState(2)

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file && file.name.endsWith(".kml")) {
      parseKMLPreview(file)
    } else {
      alert("Please select a valid KML file")
    }
  }

  const handleDragAndDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.stopPropagation()

    const file = e.dataTransfer.files?.[0]
    if (file && file.name.endsWith(".kml")) {
      parseKMLPreview(file)
    } else {
      alert("Please drop a valid KML file")
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
              const coords = coordsText.split(/\s+/).map((c) => {
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
              const coords = coordsText.split(/\s+/).map((c) => {
                const [lng, lat] = c.split(",")
                return [Number.parseFloat(lng), Number.parseFloat(lat)]
              })
              geometry = {
                type: "Polygon",
                coordinates: [coords],
              }
            }

            let layer = "Other"
            const text = `${name} ${description}`.toLowerCase()
            if (text.includes("building")) layer = "Buildings"
            else if (text.includes("settlement") || text.includes("city") || text.includes("town"))
              layer = "Settlements"
            else if (text.includes("crop") || text.includes("agriculture") || text.includes("farm")) layer = "Crops"
            else if (text.includes("water") || text.includes("river") || text.includes("lake")) layer = "Water"
            else if (text.includes("slope") || text.includes("elevation") || text.includes("terrain")) layer = "Slopes"

            return {
              type: "Feature",
              geometry,
              properties: {
                name,
                description,
                layer,
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
        setSelectedIdColumn(columnList[0] || "name")

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

  const handleConfirm = () => {
    if (previewData && selectedIdColumn) {
      onFileUpload(previewData.file, previewData, selectedIdColumn)
    }
  }

  if (previewData) {
    const sampleFeatures = previewData.features.slice(0, 5)

    return (
      <div className="grid grid-cols-2 gap-8 h-full">
        <div>
          <h3 className="text-lg font-semibold text-slate-900 mb-4">Map Preview</h3>
          <div className="rounded-lg overflow-hidden bg-slate-50 w-full h-[550px]">
            <MapComponent
              data={previewData}
              selectedLayers={["Buildings", "Settlements", "Crops", "Water", "Slopes", "Other"]}
              center={mapCenter}
              zoom={mapZoom}
            />
          </div>
        </div>

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
                        className={`px-4 py-3 text-left font-semibold text-slate-900 ${
                          col === selectedIdColumn ? "bg-blue-100" : ""
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
                          className={`px-4 py-3 text-slate-700 ${
                            col === selectedIdColumn ? "bg-blue-50 font-semibold" : ""
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
              className="flex-1 px-4 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
            >
              Confirm & Continue
            </button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full">
      <h2 className="text-lg font-semibold text-slate-900 mb-4 flex items-center gap-2">
        <FileUp className="w-5 h-5 text-blue-600" />
        Upload KML File
      </h2>

      <div
        onDragOver={(e) => e.preventDefault()}
        onDrop={handleDragAndDrop}
        className="border-2 border-dashed border-slate-300 rounded-lg p-12 text-center hover:border-blue-500 hover:bg-blue-50 transition-colors cursor-pointer"
        onClick={() => fileInputRef.current?.click()}
      >
        <Upload className="w-12 h-12 text-slate-400 mx-auto mb-4" />
        <p className="text-base text-slate-600 font-medium">Drag & drop your KML file here</p>
        <p className="text-sm text-slate-500 mt-2">or click to browse</p>
      </div>

      <input
        ref={fileInputRef}
        type="file"
        accept=".kml"
        onChange={handleFileChange}
        className="hidden"
        disabled={isProcessing}
      />

      <p className="text-xs text-slate-500 mt-4">✓ Supports .kml files from Google Earth</p>
    </div>
  )
}
