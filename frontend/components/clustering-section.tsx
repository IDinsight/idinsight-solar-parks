"use client"
import { useState } from "react"
import { Loader2, CheckCircle, Trash2 } from "lucide-react"

interface ClusteringSectionProps {
  data: any
  isProcessing?: boolean
  clusteringComplete?: boolean
  clusteringParams?: { distance_threshold: number, min_samples: number, min_parcel_area_ha?: number } | null
  clusteringResult?: { total_parcels: number, clustered_khasras: number, unclustered_khasras: number } | null
  onClusteringComplete: (result: { distanceThreshold: number, minParcelArea: number }) => void
  onClusteringDeleted?: () => void
}

export default function ClusteringSection({
  data,
  isProcessing = false,
  clusteringComplete = false,
  clusteringParams = null,
  clusteringResult = null,
  onClusteringComplete,
  onClusteringDeleted
}: ClusteringSectionProps) {
  const [distanceThreshold, setDistanceThreshold] = useState(10)
  const [minParcelArea, setMinParcelArea] = useState(100)
  const [showDeleteModal, setShowDeleteModal] = useState(false)
  const [isDeleting, setIsDeleting] = useState(false)

  const handleRunClustering = async () => {
    if (!data?.features || data.features.length === 0) {
      alert("No data available for clustering")
      return
    }

    onClusteringComplete({
      distanceThreshold,
      minParcelArea,
    })
  }

  const handleDeleteClustering = async () => {
    setIsDeleting(true)
    try {
      if (onClusteringDeleted) {
        await onClusteringDeleted()
      }
      setShowDeleteModal(false)
    } catch (error) {
      console.error("Error deleting clustering:", error)
      alert("Failed to delete clustering. Please try again.")
    } finally {
      setIsDeleting(false)
    }
  }

  // If clustering is complete, show the parameters used and delete button
  if (clusteringComplete) {
    return (
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center gap-2 mb-4">
          <CheckCircle className="w-5 h-5 text-green-600" />
          <h3 className="text-lg font-semibold text-slate-900">Clustering Complete</h3>
        </div>

        {/* Display clustering parameters and statistics */}
        <div className="p-4 bg-green-50 border border-green-200 rounded-lg space-y-4">
          {/* Parameters Used */}
          {clusteringParams && (
            <div>
              <h4 className="font-semibold text-green-900 text-sm mb-2">Parameters Used</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-slate-600">Distance Threshold:</span>
                  <span className="font-medium text-slate-900">{clusteringParams.distance_threshold} meters</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-600">Minimum Parcel Area:</span>
                  <span className="font-medium text-slate-900">{clusteringParams.min_parcel_area_ha ?? 50} hectares</span>
                </div>
              </div>
            </div>
          )}
          
          {/* Clustering Results Stats */}
          {clusteringResult && (
            <div className={clusteringParams ? "pt-3 border-t border-green-300" : ""}>
              <h4 className="font-semibold text-green-900 text-sm mb-3">Clustering Results</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-slate-600">Total Parcels:</span>
                  <span className="font-bold text-green-700">{clusteringResult.total_parcels}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-600">Clustered Khasras:</span>
                  <span className="font-bold text-green-700">{clusteringResult.clustered_khasras}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-600">Unclustered Khasras:</span>
                  <span className="font-bold text-slate-600">{clusteringResult.unclustered_khasras}</span>
                </div>
              </div>
            </div>
          )}
          
          {/* Fallback if no data available */}
          {!clusteringResult && !clusteringParams && (
            <p className="text-sm text-green-900">
              Clustering has been completed.
            </p>
          )}
        </div>

        {/* Delete Action */}
        <div className="border border-red-200 rounded-lg p-6">
          <h3 className="text-sm font-semibold text-red-900 mb-3">Delete Clustering</h3>
          <p className="text-xs text-red-700 mb-4">
            This will delete all parcel clustering results. You can re-run clustering with different parameters afterwards.
          </p>
          <button
            onClick={() => setShowDeleteModal(true)}
            disabled={isDeleting}
            className="w-full flex items-center justify-center gap-2 px-4 py-3 border border-red-600 hover:bg-red-100 text-red-600 disabled:bg-gray-100 disabled:text-gray-400 disabled:border-gray-400 font-semibold rounded-lg transition-colors"
          >
            <Trash2 className="w-4 h-4" />
            Delete Clustering Results
          </button>
        </div>

        {/* Delete Confirmation Modal */}
        {showDeleteModal && (
          <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-[9999]">
            <div className="bg-white p-6 rounded-lg shadow-xl max-w-md w-full mx-4 relative z-[10000]">
              <h3 className="text-lg font-semibold text-slate-900 mb-2">Delete Clustering Results?</h3>
              <p className="text-sm text-slate-600 mb-4">
                This will delete all parcel clustering results. You can re-run clustering with different parameters afterwards.
              </p>
              <p className="text-red-600 font-medium text-sm mb-6">This action cannot be undone.</p>
              <div className="flex gap-3 justify-end">
                <button
                  onClick={() => setShowDeleteModal(false)}
                  disabled={isDeleting}
                  className="px-4 py-2 border border-slate-300 rounded-lg hover:bg-slate-50 transition-colors disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  onClick={handleDeleteClustering}
                  disabled={isDeleting}
                  className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors flex items-center gap-2 disabled:opacity-50"
                >
                  {isDeleting ? (
                    <>
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Deleting...
                    </>
                  ) : (
                    <>
                      Delete
                    </>
                  )}
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    )
  }

  // Original UI for running clustering
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
              type="text"
              inputMode="numeric"
              value={distanceThreshold}
              onChange={(e) => {
                const value = e.target.value.replace(/[^0-9]/g, '');
                const num = value === '' ? 0 : Math.max(1, Number(value));
                setDistanceThreshold(num);
              }}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="text-xs text-slate-500 mt-2">
              Maximum distance between khasra boundaries to be considered adjacent
            </p>
          </div>

          {/* Minimum Parcel Area */}
          <div>
            <label htmlFor="minParcelArea" className="block text-sm font-medium text-slate-700 mb-2">
              Minimum Parcel Area (hectares)
            </label>
            <input
              id="minParcelArea"
              type="text"
              inputMode="decimal"
              value={minParcelArea}
              onChange={(e) => {
                const value = e.target.value.replace(/[^0-9.]/g, '');
                const num = value === '' ? 0 : Math.max(0, Number(value));
                setMinParcelArea(num);
              }}
              className="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="text-xs text-slate-500 mt-2">
              Parcels with total usable area below this threshold will be dropped
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
          <li>• Clusters must have at least 2 khasras</li>
          <li>• Parcels below minimum area threshold will be excluded</li>
          <li>• Unclustered khasras will be marked separately</li>
        </ul>
      </div>
    </div>
  )
}
