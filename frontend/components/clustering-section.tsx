"use client"
import { useState } from "react"

interface ClusteringSectionProps {
  data: any
  onClusteringComplete: (clusters: any[]) => void
}

export default function ClusteringSection({ data, onClusteringComplete }: ClusteringSectionProps) {
  const [distanceThreshold, setDistanceThreshold] = useState(25)
  const [isProcessing, setIsProcessing] = useState(false)
  const [clusterResult, setClusterResult] = useState<any>(null)

  const getFeatureCentroid = (feature: any): [number, number] | null => {
    if (feature.geometry?.type === "Point") {
      return feature.geometry.coordinates
    } else if (feature.geometry?.type === "Polygon") {
      const coordinates = feature.geometry.coordinates[0]
      if (coordinates.length === 0) return null
      let sumLng = 0,
        sumLat = 0
      coordinates.forEach(([lng, lat]: [number, number]) => {
        sumLng += lng
        sumLat += lat
      })
      return [sumLng / coordinates.length, sumLat / coordinates.length]
    } else if (feature.geometry?.type === "LineString") {
      const coordinates = feature.geometry.coordinates
      if (coordinates.length === 0) return null
      let sumLng = 0,
        sumLat = 0
      coordinates.forEach(([lng, lat]: [number, number]) => {
        sumLng += lng
        sumLat += lat
      })
      return [sumLng / coordinates.length, sumLat / coordinates.length]
    }
    return null
  }

  const distance = (p1: [number, number], p2: [number, number]): number => {
    const dlng = p1[0] - p2[0]
    const dlat = p1[1] - p2[1]
    return Math.sqrt(dlng * dlng + dlat * dlat)
  }

  const distanceBasedClustering = (features: any[], threshold: number) => {
    const visited = new Set<number>()
    const clusters: any[] = []

    for (let i = 0; i < features.length; i++) {
      if (visited.has(i)) continue

      const cluster: any[] = []
      const queue: number[] = [i]

      while (queue.length > 0) {
        const idx = queue.shift()!
        if (visited.has(idx)) continue

        visited.add(idx)
        const centroid1 = getFeatureCentroid(features[idx])
        if (!centroid1) continue

        cluster.push({
          feature: features[idx],
          centroid: centroid1,
          index: idx,
        })

        for (let j = 0; j < features.length; j++) {
          if (visited.has(j) || j === idx) continue
          const centroid2 = getFeatureCentroid(features[j])
          if (!centroid2) continue

          if (distance(centroid1, centroid2) <= threshold) {
            queue.push(j)
          }
        }
      }

      if (cluster.length > 0) {
        const clusterCentroid: [number, number] = [
          cluster.reduce((sum, c) => sum + c.centroid[0], 0) / cluster.length,
          cluster.reduce((sum, c) => sum + c.centroid[1], 0) / cluster.length,
        ]
        clusters.push({
          id: clusters.length,
          features: cluster,
          centroid: clusterCentroid,
          size: cluster.length,
        })
      }
    }

    return clusters
  }

  const handleRunClustering = async () => {
    if (!data?.features || data.features.length === 0) {
      alert("No features to cluster. Please upload a KML file first.")
      return
    }

    setIsProcessing(true)

    // Simulate processing delay
    setTimeout(() => {
      const mockTotalClusters = Math.ceil(data.features.length / 5)
      const mockResult = {
        totalClusters: mockTotalClusters,
        totalFeatures: data.features.length,
        averageClusterSize: (data.features.length / mockTotalClusters).toFixed(1),
        processingTime: "2.3s",
        status: "success",
      }

      setClusterResult(mockResult)
      onClusteringComplete([])
      setIsProcessing(false)
    }, 1500)
  }

  return (
    <div className="w-full space-y-6">
      <div className="space-y-6">
        <div className="p-6">
          <label className="block text-sm font-semibold text-slate-900 mb-3">
            Distance Threshold (m): <span className="text-amber-600 font-bold">{distanceThreshold.toFixed(0)}</span>
          </label>
          <input
            type="range"
            min="0"
            max="50"
            step="5"
            value={distanceThreshold}
            onChange={(e) => setDistanceThreshold(Number(e.target.value))}
            className="w-full h-2 bg-slate-300 rounded-lg appearance-none cursor-pointer"
          />
          <p className="text-xs text-slate-500 mt-3">Lower = more clusters, Higher = fewer clusters</p>
        </div>

        <button
          onClick={handleRunClustering}
          disabled={!data || isProcessing}
          className="w-full px-4 py-3 bg-amber-600 hover:bg-amber-700 disabled:bg-slate-400 text-white font-semibold rounded-lg transition-colors"
        >
          {isProcessing ? "Processing..." : "Run Clustering"}
        </button>

        {clusterResult && (
          <div className="space-y-6">
            <div className="p-6 bg-green-50 rounded-lg border border-green-200">
              <div className="flex items-center gap-3 mb-4">
                <div className="w-6 h-6 bg-green-500 rounded-full flex items-center justify-center">
                  <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                </div>
                <p className="text-sm font-semibold text-green-900">Clustering Complete</p>
              </div>
              <div className="space-y-3 text-sm text-green-800">
                <p>
                  <span className="font-medium">Total Clusters:</span> {clusterResult.totalClusters}
                </p>
                <p>
                  <span className="font-medium">Total Features:</span> {clusterResult.totalFeatures}
                </p>
                <p>
                  <span className="font-medium">Average Cluster Size:</span> {clusterResult.averageClusterSize}
                </p>
                <p>
                  <span className="font-medium">Processing Time:</span> {clusterResult.processingTime}
                </p>
              </div>
            </div>

            <p className="text-xs text-slate-500 text-center p-4 bg-slate-50 rounded-lg">
              Clustering results will be processed in the backend. The actual cluster geometries will be displayed on
              the map when available.
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
