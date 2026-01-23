"use client"
import dynamic from "next/dynamic"
import { useEffect, useState } from "react"
import type { FeatureCollection } from "geojson"

// Import Leaflet CSS
import "leaflet/dist/leaflet.css"

interface MapProps {
  data: any
  selectedLayers: string[]
  center: [number, number]
  zoom: number
  clusters?: any[]
  layersData?: Record<string, any>
}

// Define colors for different layer types
const LAYER_COLORS: Record<string, string> = {
  'Buildings': '#e74c3c',       // Red
  'Settlements': '#f39c12',     // Orange
  'Crops': '#27ae60',           // Green
  'Water': '#3498db',           // Blue
  'Slopes': '#9b59b6',          // Purple
  'Other': '#95a5a6',           // Gray
  'khasras': '#2c3e50',         // Dark gray (default for khasras)
}

// Create the entire map as a single dynamic component to avoid SSR issues
const LeafletMap = dynamic(
  () => import("react-leaflet").then((mod) => {
    const { MapContainer, TileLayer, GeoJSON, useMap } = mod
    const L = require("leaflet")

    // Helper component to handle map resize and fit bounds
    function MapController({ geoJsonData, layersGeoJson }: { 
      geoJsonData: FeatureCollection | null
      layersGeoJson: Array<{ data: FeatureCollection, color: string }> 
    }) {
      const map = useMap()
      
      useEffect(() => {
        if (!map || !map.getContainer()) return
        
        // Invalidate size multiple times with delays
        const timeouts = [0, 100, 250, 500, 1000].map((delay) =>
          setTimeout(() => {
            try {
              if (map && map.getContainer()) {
                map.invalidateSize()
              }
            } catch (e) {
              // Ignore errors during cleanup
            }
          }, delay)
        )

        // Also handle window resize
        const handleResize = () => {
          try {
            if (map && map.getContainer()) {
              map.invalidateSize()
            }
          } catch (e) {
            // Ignore errors
          }
        }
        window.addEventListener("resize", handleResize)

        return () => {
          timeouts.forEach(clearTimeout)
          window.removeEventListener("resize", handleResize)
        }
      }, [map])

      // Fit bounds when geoJsonData changes
      useEffect(() => {
        if (!map || !map.getContainer()) return
        
        try {
          let allBounds: any = null
          
          // Add khasra bounds
          if (geoJsonData && geoJsonData.features.length > 0) {
            const geoJsonLayer = L.geoJSON(geoJsonData)
            allBounds = geoJsonLayer.getBounds()
          }
          
          // Add layer bounds
          if (layersGeoJson && Array.isArray(layersGeoJson)) {
            layersGeoJson.forEach(layer => {
              if (layer.data && layer.data.features && layer.data.features.length > 0) {
                const layerBounds = L.geoJSON(layer.data).getBounds()
                if (allBounds) {
                  allBounds.extend(layerBounds)
                } else {
                  allBounds = layerBounds
                }
              }
            })
          }
          
          if (allBounds && allBounds.isValid()) {
            // Use requestAnimationFrame to ensure DOM is ready
            requestAnimationFrame(() => {
              setTimeout(() => {
                try {
                  if (map && map.getContainer()) {
                    map.fitBounds(allBounds, { padding: [20, 20], maxZoom: 16 })
                  }
                } catch (e) {
                  // Ignore errors during cleanup
                }
              }, 300)
            })
          }
        } catch (e) {
          console.error("Error fitting bounds:", e)
        }
      }, [map, geoJsonData, layersGeoJson])

      return null
    }

    // Return the actual map component
    return function MapInner({ center, zoom, geoJsonData, layersGeoJson }: { 
      center: [number, number]
      zoom: number
      geoJsonData: FeatureCollection | null
      layersGeoJson: Array<{ data: FeatureCollection, color: string, name: string }>
    }) {
      // Style function for khasras (base layer)
      const khasraStyle = () => ({
        color: '#2c3e50',
        weight: 2,
        opacity: 0.6,
        fillOpacity: 0.1,
      })

      // Style function for layers
      const layerStyle = (color: string) => () => ({
        color: color,
        weight: 2,
        opacity: 0.8,
        fillColor: color,
        fillOpacity: 0.4,
      })

      return (
        <MapContainer 
          center={center} 
          zoom={zoom} 
          style={{ width: "100%", height: "100%" }} 
          scrollWheelZoom={true}
          key="main-map" // Stable key to prevent recreation
        >
          <MapController geoJsonData={geoJsonData} layersGeoJson={layersGeoJson} />
          <TileLayer
            attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {/* Render khasras first (bottom layer) */}
          {geoJsonData && geoJsonData.features.length > 0 && (
            <GeoJSON 
              key={`khasras-${geoJsonData.features.length}`} 
              data={geoJsonData} 
              style={khasraStyle} 
            />
          )}
          {/* Render layers on top */}
          {layersGeoJson.map((layer, index) => (
            layer.data && layer.data.features.length > 0 && (
              <GeoJSON 
                key={`layer-${layer.name}-${layer.data.features.length}`} 
                data={layer.data} 
                style={layerStyle(layer.color)}
              />
            )
          ))}
        </MapContainer>
      )
    }
  }),
  { 
    ssr: false,
    loading: () => (
      <div className="w-full h-full bg-slate-50 flex items-center justify-center rounded-lg">
        <p className="text-slate-500">Loading map...</p>
      </div>
    )
  }
)

export default function MapComponent({ data, selectedLayers, center, zoom, layersData }: MapProps) {
  const [geoJsonData, setGeoJsonData] = useState<FeatureCollection | null>(null)
  const [layersGeoJson, setLayersGeoJson] = useState<Array<{ data: FeatureCollection, color: string, name: string }>>([])

  useEffect(() => {
    if (data?.features && Array.isArray(data.features)) {
      // Show all features without filtering by layer
      const geoJson: FeatureCollection = {
        type: "FeatureCollection",
        features: data.features,
      }
      setGeoJsonData(geoJson)
    } else {
      setGeoJsonData(null)
    }
  }, [data])

  // Process layers data
  useEffect(() => {
    if (!layersData) {
      setLayersGeoJson([])
      return
    }

    try {
      const processedLayers: Array<{ data: FeatureCollection, color: string, name: string }> = []

      Object.entries(layersData).forEach(([layerName, layerInfo]: [string, any]) => {
        // Check if this layer should be displayed based on selectedLayers
        if (selectedLayers.length > 0 && !selectedLayers.includes(layerName)) {
          return
        }

        if (layerInfo?.features && Array.isArray(layerInfo.features) && layerInfo.features.length > 0) {
          // Get color based on layer type or name
          const layerType = layerInfo.layer_info?.layer_type || layerName
          const color = LAYER_COLORS[layerType] || LAYER_COLORS[layerName] || LAYER_COLORS['Other']

          processedLayers.push({
            data: {
              type: "FeatureCollection",
              features: layerInfo.features,
            },
            color,
            name: layerName,
          })
        }
      })

      setLayersGeoJson(processedLayers)
    } catch (e) {
      console.error("Error processing layers:", e)
      setLayersGeoJson([])
    }
  }, [layersData, selectedLayers])

  if (!data) {
    return (
      <div className="w-full h-full bg-slate-50 flex items-center justify-center rounded-lg">
        <p className="text-slate-500">Upload a KML file to display the map</p>
      </div>
    )
  }

  return (
    <div className="w-full h-full rounded-lg overflow-hidden">
      <LeafletMap center={center} zoom={zoom} geoJsonData={geoJsonData} layersGeoJson={layersGeoJson} />
    </div>
  )
}
