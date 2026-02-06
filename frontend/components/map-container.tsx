"use client"
import dynamic from "next/dynamic"
import { useEffect, useState } from "react"
import type { FeatureCollection } from "geojson"
import { useMapStore } from "@/lib/stores/map"

// Import Leaflet CSS
import "leaflet/dist/leaflet.css"

interface MapProps {
  projectId: string
  data: any
  selectedLayers?: string[]
  center?: [number, number]
  zoom?: number
  parcelsData?: any
  layersData?: Record<string, any>
  forceAutoFit?: boolean  // Force auto-fit even if view state exists (e.g., for preview mode)
}

interface VisibleLayers {
  khasras: boolean
  parcels: boolean
  layers: Record<string, boolean>
}

// Define colors for different layer types
export const LAYER_COLORS: Record<string, string> = {
  'Isolated Buildings': '#ff83f1',
  'Settlements': '#cf0000',
  'Cropland': '#ab9928',
  'Water': '#00d9ff',
  'Slopes - North Facing': '#191919',  // Grey
  'Slopes - Other Facing': '#cfcfcf',  // Light grey/white-ish
  'Other': '#ffffff',
}

// Define layer rendering order (bottom to top)
// Lower index = rendered first (bottom), higher index = rendered last (top)
const LAYER_ORDER: Record<string, number> = {
  'Slopes - Other Facing': 1,
  'Slopes - North Facing': 2,
  'Water': 3,
  'Cropland': 4,
  'Settlements': 5,
  'Isolated Buildings': 6,
  // Khasras and parcels are rendered separately after all constraint layers
}

// Get sort order for a layer (unknown layers go to position 100)
function getLayerOrder(layerName: string): number {
  return LAYER_ORDER[layerName] ?? 100
}

// Create the entire map as a single dynamic component to avoid SSR issues
const LeafletMap = dynamic(
  () => import("react-leaflet").then((mod) => {
    const { MapContainer, TileLayer, GeoJSON, useMap, LayersControl } = mod
    const L = require("leaflet")

    // Helper component to handle map resize and fit bounds
    function MapController({ geoJsonData, layersGeoJson, shouldAutoFit }: {
      geoJsonData: FeatureCollection | null
      layersGeoJson: Array<{ data: FeatureCollection, color: string }>
      shouldAutoFit: boolean
    }) {
      const map = useMap()
      const { useState } = require("react")
      const [hasAutoFitted, setHasAutoFitted] = useState(false)

      // Reset hasAutoFitted when data changes to allow recentering
      useEffect(() => {
        setHasAutoFitted(false)
      }, [geoJsonData])

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

      // Fit bounds when geoJsonData changes (only once if shouldAutoFit is true)
      useEffect(() => {
        if (!map || !map.getContainer()) return
        if (!shouldAutoFit || hasAutoFitted) return

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
                    setHasAutoFitted(true)
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
      }, [map, geoJsonData, layersGeoJson, shouldAutoFit, hasAutoFitted])

      return null
    }

    // Legend component
    function MapLegend({ visibleLayers, setVisibleLayers, layersGeoJson, hasParcels, isExpanded, setIsExpanded }: {
      visibleLayers: VisibleLayers
      setVisibleLayers: React.Dispatch<React.SetStateAction<VisibleLayers>>
      layersGeoJson: Array<{ data: FeatureCollection, color: string, name: string }>
      hasParcels: boolean
      isExpanded: boolean
      setIsExpanded: (expanded: boolean) => void
    }) {

      return (
        <div style={{
          position: 'absolute',
          top: '10px',
          right: '10px',
          zIndex: 1000,
          backgroundColor: 'rgba(255, 255, 255, 0.95)',
          padding: '12px',
          borderRadius: '6px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
          minWidth: '180px',
          maxWidth: '220px',
        }}>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: isExpanded ? '8px' : '0',
              cursor: 'pointer',
            }}
            onClick={() => setIsExpanded(!isExpanded)}
          >
            <div style={{ fontWeight: 600, fontSize: '13px', color: '#334155' }}>
              Map Layers
            </div>
            <div style={{ fontSize: '12px', color: '#64748b' }}>
              {isExpanded ? '▼' : '▶'}
            </div>
          </div>

          {isExpanded && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
              {/* Khasras toggle */}
              <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '12px' }}>
                <input
                  type="checkbox"
                  checked={visibleLayers.khasras}
                  onChange={(e) => setVisibleLayers(prev => ({ ...prev, khasras: e.target.checked }))}
                  style={{ cursor: 'pointer' }}
                />
                <div style={{ width: '12px', height: '12px', border: '2px solid #000000', borderRadius: '2px' }} />
                <span style={{ color: '#334155' }}>Khasras</span>
              </label>

              {/* Parcels toggle - only show if parcels exist */}
              {hasParcels && (
                <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '12px' }}>
                  <input
                    type="checkbox"
                    checked={visibleLayers.parcels}
                    onChange={(e) => setVisibleLayers(prev => ({ ...prev, parcels: e.target.checked }))}
                    style={{ cursor: 'pointer' }}
                  />
                  <div style={{ width: '12px', height: '12px', border: '2px dashed #000000', borderRadius: '2px' }} />
                  <span style={{ color: '#334155' }}>Parcels</span>
                </label>
              )}

              {/* Constraint layers toggles */}
              {layersGeoJson.map((layer) => (
                <label key={layer.name} style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '12px' }}>
                  <input
                    type="checkbox"
                    checked={visibleLayers.layers[layer.name] !== false}
                    onChange={(e) => setVisibleLayers(prev => ({
                      ...prev,
                      layers: { ...prev.layers, [layer.name]: e.target.checked }
                    }))}
                    style={{ cursor: 'pointer' }}
                  />
                  <div style={{ width: '12px', height: '12px', backgroundColor: layer.color, borderRadius: '2px' }} />
                  <span style={{ color: '#334155' }}>{layer.name}</span>
                </label>
              ))}
            </div>
          )}
        </div>
      )
    }

    // Helper component to track map view changes
    function MapViewTracker({ onViewChange, onBaseLayerChange }: {
      onViewChange: (center: [number, number], zoom: number) => void
      onBaseLayerChange: (layer: 'satellite' | 'street') => void
    }) {
      const map = useMap()

      useEffect(() => {
        if (!map) return

        const handleMoveEnd = () => {
          const center = map.getCenter()
          const zoom = map.getZoom()
          onViewChange([center.lat, center.lng], zoom)
        }

        const handleBaseLayerChange = (e: any) => {
          const layerName = e.name
          if (layerName === 'Satellite') {
            onBaseLayerChange('satellite')
          } else if (layerName === 'Street Map') {
            onBaseLayerChange('street')
          }
        }

        map.on('moveend', handleMoveEnd)
        map.on('zoomend', handleMoveEnd)
        map.on('baselayerchange', handleBaseLayerChange)

        return () => {
          map.off('moveend', handleMoveEnd)
          map.off('zoomend', handleMoveEnd)
          map.off('baselayerchange', handleBaseLayerChange)
        }
      }, [map, onViewChange, onBaseLayerChange])

      return null
    }

    // Return the actual map component
    return function MapInner({ center, zoom, geoJsonData, layersGeoJson, parcelsGeoJson, baseLayer, visibleLayers, setVisibleLayers, legendExpanded, setLegendExpanded, onViewChange, onBaseLayerChange, shouldAutoFit }: {
      center: [number, number]
      zoom: number
      geoJsonData: FeatureCollection | null
      layersGeoJson: Array<{ data: FeatureCollection, color: string, name: string }>
      parcelsGeoJson: FeatureCollection | null
      baseLayer: 'satellite' | 'street'
      visibleLayers: VisibleLayers
      setVisibleLayers: React.Dispatch<React.SetStateAction<VisibleLayers>>
      legendExpanded: boolean
      setLegendExpanded: (expanded: boolean) => void
      onViewChange: (center: [number, number], zoom: number) => void
      onBaseLayerChange: (layer: 'satellite' | 'street') => void
      shouldAutoFit: boolean
    }) {
      const hasParcels = parcelsGeoJson && parcelsGeoJson.features && parcelsGeoJson.features.length > 0
      // Style function for khasras (gray outline)
      const khasraOutlineStyle = () => ({
        color: '#424242',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0.2,
      })

      // Style function for khasras (white line on top)
      const khasraStyle = () => ({
        color: '#e1e1e1',
        weight: 1,
        opacity: 0.8,
        fillOpacity: 0.2,
      })

      // Function to add tooltips to khasras
      const onEachKhasra = (feature: any, layer: any) => {
        if (feature.properties) {
          const props = feature.properties

          // Build tooltip content with all available stats
          let tooltipContent = `<strong>Khasra: ${props.khasra_id_unique || props.khasra_id || 'N/A'}</strong><br/>`

          if (props.original_area_ha !== null && props.original_area_ha !== undefined) {
            tooltipContent += `Original Area: ${props.original_area_ha.toFixed(1)} ha<br/>`
          }

          if (props.usable_area_ha !== null && props.usable_area_ha !== undefined) {
            tooltipContent += `Usable Area: ${props.usable_area_ha.toFixed(1)} ha`
            if (props.usable_area_percent !== null && props.usable_area_percent !== undefined) {
              tooltipContent += ` (${props.usable_area_percent.toFixed(1)}%)`
            }
            tooltipContent += `<br/>`
          }

          if (props.usable_available_area_ha !== null && props.usable_available_area_ha !== undefined) {
            tooltipContent += `Usable & Available: ${props.usable_available_area_ha.toFixed(1)} ha`
            if (props.usable_available_area_percent !== null && props.usable_available_area_percent !== undefined) {
              tooltipContent += ` (${props.usable_available_area_percent.toFixed(1)}%)`
            }
            tooltipContent += `<br/>`
          }

          if (props.unusable_area_ha !== null && props.unusable_area_ha !== undefined) {
            tooltipContent += `Unusable Area: ${props.unusable_area_ha.toFixed(1)} ha`
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
            className: 'khasra-tooltip',
            sticky: true, // Follow mouse cursor
          })

          // Explicitly handle mouseout to ensure tooltip closes
          layer.on('mouseout', function () {
            layer.closeTooltip()
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

      // Style function for parcels (white outline)
      const parcelOutlineStyle = () => ({
        color: '#ffffff',
        weight: 5,
        opacity: 1,
        fillOpacity: 0,
        dashArray: '5, 3',
      })

      // Style function for parcels (black line on top)
      const parcelStyle = () => ({
        color: '#000000',
        weight: 2,
        opacity: 0.8,
        fillOpacity: 0,
        dashArray: '5, 3',
      })

      // Function to add labels to parcels
      const onEachParcel = (feature: any, layer: any) => {
        if (feature.properties && feature.properties.parcel_id) {
          const parcelId = feature.properties.parcel_id
          const khasraCount = feature.properties.khasra_count || 0
          const originalAreaHa = feature.properties.original_area_ha || 0
          const usableAreaHa = feature.properties.usable_area_ha || 0
          const usableAvailableAreaHa = feature.properties.usable_available_area_ha || 0

          // Calculate percentages
          const usablePercent = originalAreaHa > 0 ? (usableAreaHa / originalAreaHa) * 100 : 0
          const usableAvailablePercent = originalAreaHa > 0 ? (usableAvailableAreaHa / originalAreaHa) * 100 : 0

          // Add tooltip on hover
          layer.bindTooltip(
            `<strong>${parcelId}</strong><br/>` +
            `Khasras: ${khasraCount}<br/>` +
            `Original Area: ${originalAreaHa.toFixed(1)} ha<br/>` +
            `Usable Area: ${usableAreaHa.toFixed(1)} ha (${usablePercent.toFixed(1)}%)<br/>` +
            `Usable + Available: ${usableAvailableAreaHa.toFixed(1)} ha (${usableAvailablePercent.toFixed(1)}%)`,
            { permanent: false, direction: 'top', sticky: true }
          )

          // Explicitly handle mouseout to ensure tooltip closes
          layer.on('mouseout', function () {
            layer.closeTooltip()
          })

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
        <div style={{ position: 'relative', width: '100%', height: '100%' }}>
          <MapContainer
            center={center}
            zoom={zoom}
            style={{ width: "100%", height: "100%" }}
            scrollWheelZoom={true}
            key="main-map" // Stable key to prevent recreation
          >
            <MapController geoJsonData={geoJsonData} layersGeoJson={layersGeoJson} shouldAutoFit={shouldAutoFit} />
            <MapViewTracker onViewChange={onViewChange} onBaseLayerChange={onBaseLayerChange} />
            <LayersControl position="bottomright">
              <LayersControl.BaseLayer checked={baseLayer === 'satellite'} name="Satellite">
                <TileLayer
                  attribution='&copy; <a href="https://www.esri.com">Esri</a>, Maxar, Earthstar Geographics'
                  url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                  maxZoom={19}
                />
              </LayersControl.BaseLayer>
              <LayersControl.BaseLayer checked={baseLayer === 'street'} name="Street Map">
                <TileLayer
                  attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                  maxZoom={19}
                />
              </LayersControl.BaseLayer>
            </LayersControl>
            {/* Render constraint layers first (bottom) */}
            {layersGeoJson.map((layer) => (
              layer.data && layer.data.features.length > 0 && visibleLayers.layers[layer.name] !== false && (
                <GeoJSON
                  key={`layer-${layer.name}-${layer.data.features.length}`}
                  data={layer.data}
                  style={layerStyle(layer.color)}
                />
              )
            ))}
            {/* Render parcel white outline first */}
            {visibleLayers.parcels && parcelsGeoJson && parcelsGeoJson.features && parcelsGeoJson.features.length > 0 && (
              <GeoJSON
                key={`parcels-outline-${parcelsGeoJson.features.length}`}
                data={parcelsGeoJson}
                style={parcelOutlineStyle}
              />
            )}
            {/* Render parcel boundaries in the middle */}
            {visibleLayers.parcels && parcelsGeoJson && parcelsGeoJson.features && parcelsGeoJson.features.length > 0 && (
              <GeoJSON
                key={`parcels-${parcelsGeoJson.features.length}`}
                data={parcelsGeoJson}
                style={parcelStyle}
                onEachFeature={onEachParcel}
              />
            )}
            {/* Render khasra gray outline first */}
            {visibleLayers.khasras && geoJsonData && geoJsonData.features.length > 0 && (
              <GeoJSON
                key={`khasras-outline-${geoJsonData.features.length}`}
                data={geoJsonData}
                style={khasraOutlineStyle}
              />
            )}
            {/* Render khasras last (top layer for hover) */}
            {visibleLayers.khasras && geoJsonData && geoJsonData.features.length > 0 && (
              <GeoJSON
                key={`khasras-${geoJsonData.features.length}`}
                data={geoJsonData}
                style={khasraStyle}
                onEachFeature={onEachKhasra}
              />
            )}
          </MapContainer>
          <MapLegend
            visibleLayers={visibleLayers}
            setVisibleLayers={setVisibleLayers}
            layersGeoJson={layersGeoJson}
            hasParcels={hasParcels}
            isExpanded={legendExpanded}
            setIsExpanded={setLegendExpanded}
          />
        </div>
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

export default function MapComponent({ projectId, data, selectedLayers, center, zoom, parcelsData, layersData, forceAutoFit }: MapProps) {
  const [geoJsonData, setGeoJsonData] = useState<FeatureCollection | null>(null)
  const [layersGeoJson, setLayersGeoJson] = useState<Array<{ data: FeatureCollection, color: string, name: string }>>([])
  const [parcelsGeoJson, setParcelsGeoJson] = useState<FeatureCollection | null>(null)
  const [layerFeatureCounts, setLayerFeatureCounts] = useState<Record<string, number>>({})

  // Get map store and initialize project map state if needed
  const {
    getMapState,
    initializeProjectMap,
    setViewState,
    setVisibleLayers: storeSetVisibleLayers,
    setBaseLayer,
    setLegendExpanded: storeSetLegendExpanded,
  } = useMapStore()

  // Initialize map state for this project on mount
  useEffect(() => {
    if (projectId) {
      initializeProjectMap(projectId, {
        viewState: {
          center: center || [20, 77],
          zoom: zoom || 5,
        },
      })
    }
  }, [projectId, initializeProjectMap])

  // Get current map state or use defaults
  const mapState = getMapState(projectId)
  const baseLayer = mapState?.mapState.baseLayer || 'satellite'
  const legendExpanded = mapState?.mapState.legendExpanded ?? true
  const visibleLayers = mapState?.visibleLayers || {
    khasras: true,
    parcels: true,
    layers: {},
  }

  // Use stored view state with fallback to props
  const mapCenter = mapState?.viewState.center || center || [20, 77]
  const mapZoom = mapState?.viewState.zoom || zoom || 5

  // Only auto-fit if we don't have stored view state (first time viewing this project) OR if forceAutoFit is true
  const shouldAutoFit = forceAutoFit || (!mapState?.viewState.center && !mapState?.viewState.zoom)

  // Handle visible layer changes
  const handleSetVisibleLayers = (updater: React.SetStateAction<VisibleLayers>) => {
    if (!projectId) return

    const currentLayers = getMapState(projectId)?.visibleLayers || {
      khasras: true,
      parcels: true,
      layers: {},
    }
    const newLayers = typeof updater === 'function' ? updater(currentLayers) : updater
    storeSetVisibleLayers(projectId, newLayers)
  }

  // Handle legend expand/collapse
  const handleSetLegendExpanded = (expanded: boolean) => {
    if (projectId) {
      storeSetLegendExpanded(projectId, expanded)
    }
  }

  // Handle view changes (zoom/pan)
  const handleViewChange = (newCenter: [number, number], newZoom: number) => {
    if (projectId) {
      setViewState(projectId, { center: newCenter, zoom: newZoom })
    }
  }

  // Handle base layer changes
  const handleBaseLayerChange = (layer: 'satellite' | 'street') => {
    if (projectId) {
      setBaseLayer(projectId, layer)
    }
  }

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
      const newLayerNames = new Set<string>()

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
          newLayerNames.add(layerName)
        }
      })

      // Sort layers by rendering order (bottom to top)
      processedLayers.sort((a, b) => getLayerOrder(a.name) - getLayerOrder(b.name))

      setLayersGeoJson(processedLayers)

      // Track feature counts to detect when layers are recreated
      const newFeatureCounts: Record<string, number> = {}
      processedLayers.forEach(layer => {
        newFeatureCounts[layer.name] = layer.data.features.length
      })

      // Ensure all processed layers are visible in the store (new layers default to visible)
      if (projectId && processedLayers.length > 0) {
        const currentVisibleLayers = getMapState(projectId)?.visibleLayers || {
          khasras: true,
          parcels: true,
          layers: {},
        }

        // Check if any layers are new or need to be set to visible
        let needsUpdate = false
        const updatedLayers = { ...currentVisibleLayers.layers }

        newLayerNames.forEach((layerName) => {
          const isNewLayer = !(layerName in updatedLayers)
          const hasNewData = layerFeatureCounts[layerName] !== newFeatureCounts[layerName]

          // Set to visible if it's a new layer OR if the layer has been recreated with different data
          if (isNewLayer || hasNewData) {
            updatedLayers[layerName] = true
            needsUpdate = true
          }
        })

        if (needsUpdate) {
          storeSetVisibleLayers(projectId, {
            ...currentVisibleLayers,
            layers: updatedLayers,
          })
        }
      }

      // Update tracked feature counts
      setLayerFeatureCounts(newFeatureCounts)
    } catch (e) {
      console.error("Error processing layers:", e)
      setLayersGeoJson([])
    }
  }, [layersData, selectedLayers, projectId, getMapState, storeSetVisibleLayers])

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
      <LeafletMap
        center={mapCenter}
        zoom={mapZoom}
        geoJsonData={geoJsonData}
        layersGeoJson={layersGeoJson}
        parcelsGeoJson={parcelsGeoJson}
        baseLayer={baseLayer}
        visibleLayers={visibleLayers}
        setVisibleLayers={handleSetVisibleLayers}
        legendExpanded={legendExpanded}
        setLegendExpanded={handleSetLegendExpanded}
        onViewChange={handleViewChange}
        onBaseLayerChange={handleBaseLayerChange}
        shouldAutoFit={shouldAutoFit}
      />
    </div>
  )
}
