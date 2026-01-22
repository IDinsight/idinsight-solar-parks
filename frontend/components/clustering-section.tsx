"use client"
import { useState } from "react"
import { Loader2, CheckCircle } from "lucide-react"

interface ClusteringSectionProps {
  data: any
  isProcessing?: boolean
  clusteringComplete?: boolean
  onClusteringComplete: (result: { distanceThreshold: number, minSamples: number }) => void
}

export default function ClusteringSection({
  data,
  isProcessing = false,
  clusteringComplete = false,
  onClusteringComplete
}: ClusteringSectionProps) {
  const [distanceThreshold, setDistanceThreshold] = useState(25)
  const [minSamples, setMinSamples] = useState(2)

  const handleRunClustering = async () => {
    if (!data?.features || data.features.length === 0) {
      alert("No data available for clustering")
      return
    }

    onClusteringComplete({
      distanceThreshold,
      minSamples,
    })
  }

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-slate-900 mb-4">Clustering Parameters</h3>

        <div className="space-y-5">
          {/* Distance Threshold */}
          <div>
            <label htmlFor="distance" className="block text-sm font-medium text-slate-700 mb-2">
              Distance Threshold (meters)
            </label>
            <input
              id="distance"
              type="number"
              value={distanceThreshold}
              onChange={(e) => setDistanceThreshold(Number(e.target.value))}
              min={1}
              max={500}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="text-xs text-slate-500 mt-2">
              Maximum distance between khasra boundaries to be considered adjacent
            </p>
          </div>

          {/* Min Samples */}
          <div>
            <label htmlFor="minSamples" className="block text-sm font-medium text-slate-700 mb-2">
              Minimum Khasras per Cluster
            </label>
            <input
              id="minSamples"
              type="number"
              value={minSamples}
              onChange={(e) => setMinSamples(Number(e.target.value))}
              min={1}
              max={20}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="text-xs text-slate-500 mt-2">
              Minimum number of khasras required to form a cluster
            </p>
          </div>
        </div>
      </div>

      {/* Run Button */}
      <button
        onClick={handleRunClustering}
        disabled={isProcessing || clusteringComplete}
        className="w-full px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white font-semibold rounded-lg transition-colors flex items-center justify-center gap-2"
      >
        {isProcessing ? (
          <>
            <Loader2 className="w-5 h-5 animate-spin" />
            Running Clustering...
          </>
        ) : clusteringComplete ? (
          <>
            <CheckCircle className="w-5 h-5" />
            Clustering Complete
          </>
        ) : (
          'Run Clustering'
        )}
      </button>

      {/* Info Box */}
      <div className="p-4 bg-slate-50 border border-slate-200 rounded-lg">
        <h4 className="font-semibold text-slate-900 text-sm mb-2">About Clustering</h4>
        <ul className="text-xs text-slate-600 space-y-1">
          <li>• Adjacent khasras are grouped into parcels</li>
          <li>• Khasras must be within the distance threshold</li>
          <li>• Clusters must have minimum number of khasras</li>
          <li>• Unclustered khasras will be marked separately</li>
        </ul>
      </div>
    </div>
  )
}
