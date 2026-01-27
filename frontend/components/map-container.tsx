"use client"
import dynamic from "next/dynamic"
import { useEffect, useState } from "react"
import type { FeatureCollection } from "geojson"

// Import Leaflet CSS
import "leaflet/dist/leaflet.css"

interface MapProps {
  data: any
  selectedLayers?: string[]
  center: [number, number]
  zoom: number
  parcelsData?: any
  layersData?: Record<string, any>
}

// Define colors for different layer types
export const LAYER_COLORS: Record<string, string> = {
  'Isolated Buildings': '#97498ed1',
  'Settlements': '#b500008b',
  'Cropland': '#988400ae',
  'Water': '#00d9ffc4',
  'Slopes': '#9c9c9cbf',
  'Other': '#fcffffd3',
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
    return function MapInner({ center, zoom, geoJsonData, layersGeoJson, parcelsGeoJson }: {
      center: [number, number]
      zoom: number
      geoJsonData: FeatureCollection | null
      layersGeoJson: Array<{ data: FeatureCollection, color: string, name: string }>
      parcelsGeoJson: FeatureCollection | null
    }) {
      // Style function for khasras (base layer)
      const khasraStyle = () => ({
        color: '#2c3e50',
        weight: 2,
        opacity: 0.6,
        fillOpacity: 0.1,
      })

      // Function to add tooltips to khasras
      const onEachKhasra = (feature: any, layer: any) => {
        if (feature.properties) {
          const props = feature.properties

          // Build tooltip content with all available stats
          let tooltipContent = `<strong>Khasra: ${props.khasra_id_unique || props.khasra_id || 'N/A'}</strong><br/>`

          if (props.original_area_ha !== null && props.original_area_ha !== undefined) {
            tooltipContent += `Original Area: ${props.original_area_ha.toFixed(4)} ha<br/>`
          }

          if (props.usable_area_ha !== null && props.usable_area_ha !== undefined) {
            tooltipContent += `Usable Area: ${props.usable_area_ha.toFixed(4)} ha`
            if (props.usable_area_percent !== null && props.usable_area_percent !== undefined) {
              tooltipContent += ` (${props.usable_area_percent.toFixed(1)}%)`
            }
            tooltipContent += `<br/>`
          }

          if (props.usable_available_area_ha !== null && props.usable_available_area_ha !== undefined) {
            tooltipContent += `Usable & Available: ${props.usable_available_area_ha.toFixed(4)} ha`
            if (props.usable_available_area_percent !== null && props.usable_available_area_percent !== undefined) {
              tooltipContent += ` (${props.usable_available_area_percent.toFixed(1)}%)`
            }
            tooltipContent += `<br/>`
          }

          if (props.unusable_area_ha !== null && props.unusable_area_ha !== undefined) {
            tooltipContent += `Unusable Area: ${props.unusable_area_ha.toFixed(4)} ha`
            if (props.unusable_area_percent !== null && props.unusable_area_percent !== undefined) {
              tooltipContent += ` (${props.unusable_area_percent.toFixed(1)}%)`
            }
            tooltipContent += `<br/>`
          }

          if (props.parcel_id) {
            tooltipContent += `Parcel: ${props.parcel_id}<br/>`
          }

          // Add tooltip on hover
          layer.bindTooltip(tooltipContent, {
            permanent: false,
            direction: 'top',
            className: 'khasra-tooltip'
          })
        }
      }

      // Style function for layers
      const layerStyle = (color: string) => () => ({
        color: color,
        weight: 2,
        opacity: 0.8,
        fillColor: color,
        fillOpacity: 0.4,
      })

      // Style function for parcels
      const parcelStyle = () => ({
        color: '#ff6b35',
        weight: 3,
        opacity: 0.9,
        fillOpacity: 0,
        dashArray: '5, 5',
      })

      // Function to add labels to parcels
      const onEachParcel = (feature: any, layer: any) => {
        if (feature.properties && feature.properties.parcel_id) {
          const parcelId = feature.properties.parcel_id
          const khasraCount = feature.properties.khasra_count || 0
          const usableAreaHa = feature.properties.usable_area_ha || 0

          // Add tooltip on hover
          layer.bindTooltip(
            `<strong>${parcelId}</strong><br/>` +
            `Khasras: ${khasraCount}<br/>` +
            `Usable Area: ${usableAreaHa.toFixed(2)} ha`,
            { permanent: false, direction: 'top' }
          )

          // Add permanent label in the center of the parcel
          layer.on('add', function () {
            const bounds = layer.getBounds()
            const center = bounds.getCenter()

            const label = L.marker(center, {
              icon: L.divIcon({
                className: 'parcel-label',
                html: `<div style="
                  background: rgba(255, 255, 255, 0.85);
                  color: #334155;
                  padding: 3px 7px;
                  border-radius: 3px;
                  font-weight: 600;
                  font-size: 11px;
                  box-shadow: 0 1px 3px rgba(0,0,0,0.2);
                  white-space: nowrap;
                  text-align: center;
                  border: 1px solid rgba(100, 116, 139, 0.3);
                  transform: translate(-50%, -50%);
                ">${parcelId}</div>`,
                iconSize: undefined,
                iconAnchor: [0, 0],
              }),
              interactive: false, // Make label non-interactive so it doesn't block clicks
            })

            label.addTo(layer._map)

            // Store reference to remove label when layer is removed
            layer._label = label
          })

          layer.on('remove', function () {
            if (layer._label) {
              layer._map.removeLayer(layer._label)
            }
          })
        }
      }

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
              onEachFeature={onEachKhasra}
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
          {/* Render parcel boundaries on top with labels */}
          {parcelsGeoJson && parcelsGeoJson.features && parcelsGeoJson.features.length > 0 && (
            <GeoJSON
              key={`parcels-${parcelsGeoJson.features.length}`}
              data={parcelsGeoJson}
              style={parcelStyle}
              onEachFeature={onEachParcel}
            />
          )}
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

export default function MapComponent({ data, selectedLayers, center, zoom, parcelsData, layersData }: MapProps) {
  const [geoJsonData, setGeoJsonData] = useState<FeatureCollection | null>(null)
  const [layersGeoJson, setLayersGeoJson] = useState<Array<{ data: FeatureCollection, color: string, name: string }>>([])
  const [parcelsGeoJson, setParcelsGeoJson] = useState<FeatureCollection | null>(null)

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
      const layersToCheck = selectedLayers || []

      Object.entries(layersData).forEach(([layerName, layerInfo]: [string, any]) => {
        // Check if this layer should be displayed based on selectedLayers
        if (layersToCheck.length > 0 && !layersToCheck.includes(layerName)) {
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

  // Process parcels data
  useEffect(() => {
    if (parcelsData?.features && Array.isArray(parcelsData.features)) {
      setParcelsGeoJson({
        type: "FeatureCollection",
        features: parcelsData.features,
      })
    } else {
      setParcelsGeoJson(null)
    }
  }, [parcelsData])

  if (!data) {
    return (
      <div className="w-full h-full bg-slate-50 flex items-center justify-center rounded-lg">
        <p className="text-slate-500">Upload a KML file to display the map</p>
      </div>
    )
  }

  return (
    <div className="w-full h-full rounded-lg overflow-hidden">
      <LeafletMap center={center} zoom={zoom} geoJsonData={geoJsonData} layersGeoJson={layersGeoJson} parcelsGeoJson={parcelsGeoJson} />
    </div>
  )
}
