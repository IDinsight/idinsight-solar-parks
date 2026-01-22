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
}

// Create the entire map as a single dynamic component to avoid SSR issues
const LeafletMap = dynamic(
  () => import("react-leaflet").then((mod) => {
    const { MapContainer, TileLayer, GeoJSON, useMap } = mod
    const L = require("leaflet")

    // Helper component to handle map resize and fit bounds
    function MapController({ geoJsonData }: { geoJsonData: FeatureCollection | null }) {
      const map = useMap()
      
      useEffect(() => {
        if (!map) return
        
        // Invalidate size multiple times with delays
        const timeouts = [0, 100, 250, 500, 1000].map((delay) =>
          setTimeout(() => {
            map.invalidateSize()
          }, delay)
        )

        // Also handle window resize
        const handleResize = () => {
          map.invalidateSize()
        }
        window.addEventListener("resize", handleResize)

        return () => {
          timeouts.forEach(clearTimeout)
          window.removeEventListener("resize", handleResize)
        }
      }, [map])

      // Fit bounds when geoJsonData changes
      useEffect(() => {
        if (!map || !geoJsonData || geoJsonData.features.length === 0) return
        
        const geoJsonLayer = L.geoJSON(geoJsonData)
        const bounds = geoJsonLayer.getBounds()
        
        if (bounds.isValid()) {
          setTimeout(() => {
            map.fitBounds(bounds, { padding: [20, 20] })
          }, 200)
        }
      }, [map, geoJsonData])

      return null
    }

    // Return the actual map component
    return function MapInner({ center, zoom, geoJsonData }: { 
      center: [number, number]
      zoom: number
      geoJsonData: FeatureCollection | null 
    }) {
      return (
        <MapContainer 
          center={center} 
          zoom={zoom} 
          style={{ width: "100%", height: "100%" }} 
          scrollWheelZoom={true}
        >
          <MapController geoJsonData={geoJsonData} />
          <TileLayer
            attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {geoJsonData && <GeoJSON data={geoJsonData} />}
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

export default function MapComponent({ data, selectedLayers, center, zoom }: MapProps) {
  const [geoJsonData, setGeoJsonData] = useState<FeatureCollection | null>(null)

  useEffect(() => {
    if (data?.features) {
      // Show all features without filtering by layer
      const geoJson: FeatureCollection = {
        type: "FeatureCollection",
        features: data.features,
      }
      setGeoJsonData(geoJson)
    }
  }, [data])

  if (!data) {
    return (
      <div className="w-full h-full bg-slate-50 flex items-center justify-center rounded-lg">
        <p className="text-slate-500">Upload a KML file to display the map</p>
      </div>
    )
  }

  return (
    <div className="w-full h-full rounded-lg overflow-hidden">
      <LeafletMap center={center} zoom={zoom} geoJsonData={geoJsonData} />
    </div>
  )
}
