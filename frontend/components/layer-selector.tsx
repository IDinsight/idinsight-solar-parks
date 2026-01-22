"use client"

import { Layers, Loader2 } from "lucide-react"
import { useState } from "react"
import MapContainer from "@/components/map-container"

interface LayerSelectorProps {
  layers: string[]
  selectedLayers: string[]
  onLayerChange: (layer: string) => void
  kmzData: any
  mapCenter: [number, number]
  mapZoom: number
  clusters: any[]
}

const LAYER_COLORS: Record<string, string> = {
  Buildings: "bg-red-500",
  Settlements: "bg-yellow-500",
  Crops: "bg-green-500",
  Water: "bg-blue-500",
  Slopes: "bg-purple-500",
}

export default function LayerSelector({
  layers,
  selectedLayers,
  onLayerChange,
  kmzData,
  mapCenter,
  mapZoom,
  clusters,
}: LayerSelectorProps) {
  const [isProcessing, setIsProcessing] = useState(false)
  const [processedLayers, setProcessedLayers] = useState<string[]>([])
  const [layersAdded, setLayersAdded] = useState(false)

  const handleAddLayers = async () => {
    if (selectedLayers.length === 0) return

    setIsProcessing(true)
    setProcessedLayers([])

    // Process each layer with a 1.2 second delay for visual ticker effect
    for (const layer of selectedLayers) {
      await new Promise((resolve) => setTimeout(resolve, 1200))
      setProcessedLayers((prev) => [...prev, layer])
    }

    setIsProcessing(false)
    setLayersAdded(true)
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
      <div className="lg:col-span-1 space-y-6">
        {!isProcessing && !layersAdded ? (
          <div className="w-full">
            <h2 className="text-lg font-semibold text-slate-900 mb-6 flex items-center gap-2">
              <Layers className="w-5 h-5 text-blue-600" />
              Layer Selection
            </h2>

            <div className="space-y-3 mb-8">
              {layers.map((layer) => (
                <label
                  key={layer}
                  className="flex items-center gap-4 cursor-pointer hover:bg-slate-50 p-3 rounded-lg transition-colors"
                >
                  <input
                    type="checkbox"
                    checked={selectedLayers.includes(layer)}
                    onChange={() => onLayerChange(layer)}
                    className="w-4 h-4 text-blue-600 rounded"
                  />
                  <span className={`w-3 h-3 rounded-full flex-shrink-0 ${LAYER_COLORS[layer] || "bg-gray-500"}`} />
                  <span className="text-sm text-slate-700 font-medium">{layer}</span>
                </label>
              ))}
            </div>

            <p className="text-xs text-slate-500 mb-6">Select which layers to overlay on the map</p>

            <button
              onClick={handleAddLayers}
              disabled={selectedLayers.length === 0}
              className="w-full px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors"
            >
              Add Layers
            </button>
          </div>
        ) : isProcessing ? (
          <div className="w-full">
            <h2 className="text-lg font-semibold text-slate-900 mb-6 flex items-center gap-2">
              <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
              Processing Layers
            </h2>
            <div className="space-y-3">
              {selectedLayers.map((layer) => (
                <div key={layer} className="flex items-center gap-3 p-3 rounded-lg bg-slate-50">
                  {processedLayers.includes(layer) ? (
                    <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0">
                      <span className="text-white text-xs">✓</span>
                    </div>
                  ) : (
                    <div className="w-5 h-5 rounded-full border-2 border-blue-600 border-t-transparent animate-spin flex-shrink-0" />
                  )}
                  <span
                    className={`text-sm ${processedLayers.includes(layer) ? "text-green-600 font-medium" : "text-slate-600"}`}
                  >
                    {layer}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="w-full">
            <h2 className="text-lg font-semibold text-slate-900 mb-6 flex items-center gap-2">
              <Layers className="w-5 h-5 text-green-600" />
              Layers Added
            </h2>
            <div className="space-y-3">
              {selectedLayers.map((layer) => (
                <div key={layer} className="flex items-center gap-3 p-3 rounded-lg bg-slate-50">
                  <div className="w-5 h-5 rounded-full bg-green-500 flex items-center justify-center flex-shrink-0">
                    <span className="text-white text-xs">✓</span>
                  </div>
                  <span className={`w-3 h-3 rounded-full flex-shrink-0 ${LAYER_COLORS[layer] || "bg-gray-500"}`} />
                  <span className="text-sm text-green-600 font-medium">{layer}</span>
                </div>
              ))}
            </div>
            <p className="text-xs text-slate-500 mt-6">Layers have been added to the map. Click Next to continue.</p>
          </div>
        )}
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
  )
}
