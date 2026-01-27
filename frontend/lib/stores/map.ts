/**
 * Map store using Zustand - persists map state across page navigations
 */
import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export interface MapViewState {
  center: [number, number]
  zoom: number
}

export interface VisibleLayers {
  khasras: boolean
  parcels: boolean
  layers: Record<string, boolean>
}

export interface MapState {
  baseLayer: 'satellite' | 'street'
  legendExpanded: boolean
}

interface ProjectMapState {
  viewState: MapViewState
  visibleLayers: VisibleLayers
  mapState: MapState
}

interface MapStoreState {
  // Map state keyed by project ID
  projectMaps: Record<string, ProjectMapState>

  // Getters
  getMapState: (projectId: string) => ProjectMapState | undefined

  // Setters
  setViewState: (projectId: string, viewState: Partial<MapViewState>) => void
  setVisibleLayers: (projectId: string, visibleLayers: Partial<VisibleLayers>) => void
  setMapState: (projectId: string, mapState: Partial<MapState>) => void
  setBaseLayer: (projectId: string, baseLayer: 'satellite' | 'street') => void
  setLegendExpanded: (projectId: string, expanded: boolean) => void

  // Initialize map state for a project if it doesn't exist
  initializeProjectMap: (projectId: string, initialState?: Partial<ProjectMapState>) => void

  // Clear state for a project
  clearProjectMap: (projectId: string) => void
}

const DEFAULT_PROJECT_MAP_STATE: ProjectMapState = {
  viewState: {
    center: [20, 77],
    zoom: 5,
  },
  visibleLayers: {
    khasras: true,
    parcels: true,
    layers: {},
  },
  mapState: {
    baseLayer: 'satellite',
    legendExpanded: true,
  },
}

export const useMapStore = create<MapStoreState>()(
  persist(
    (set, get) => ({
      projectMaps: {},

      getMapState: (projectId: string) => {
        return get().projectMaps[projectId]
      },

      initializeProjectMap: (projectId: string, initialState?: Partial<ProjectMapState>) => {
        set((state) => {
          // Don't reinitialize if already exists
          if (state.projectMaps[projectId]) {
            return state
          }

          return {
            projectMaps: {
              ...state.projectMaps,
              [projectId]: {
                ...DEFAULT_PROJECT_MAP_STATE,
                ...initialState,
                viewState: {
                  ...DEFAULT_PROJECT_MAP_STATE.viewState,
                  ...initialState?.viewState,
                },
                visibleLayers: {
                  ...DEFAULT_PROJECT_MAP_STATE.visibleLayers,
                  ...initialState?.visibleLayers,
                  layers: {
                    ...DEFAULT_PROJECT_MAP_STATE.visibleLayers.layers,
                    ...initialState?.visibleLayers?.layers,
                  },
                },
                mapState: {
                  ...DEFAULT_PROJECT_MAP_STATE.mapState,
                  ...initialState?.mapState,
                },
              },
            },
          }
        })
      },

      setViewState: (projectId: string, viewState: Partial<MapViewState>) => {
        set((state) => ({
          projectMaps: {
            ...state.projectMaps,
            [projectId]: {
              ...(state.projectMaps[projectId] || DEFAULT_PROJECT_MAP_STATE),
              viewState: {
                ...(state.projectMaps[projectId]?.viewState || DEFAULT_PROJECT_MAP_STATE.viewState),
                ...viewState,
              },
            },
          },
        }))
      },

      setVisibleLayers: (projectId: string, visibleLayers: Partial<VisibleLayers>) => {
        set((state) => ({
          projectMaps: {
            ...state.projectMaps,
            [projectId]: {
              ...(state.projectMaps[projectId] || DEFAULT_PROJECT_MAP_STATE),
              visibleLayers: {
                ...(state.projectMaps[projectId]?.visibleLayers || DEFAULT_PROJECT_MAP_STATE.visibleLayers),
                ...visibleLayers,
                layers: {
                  ...(state.projectMaps[projectId]?.visibleLayers?.layers || {}),
                  ...visibleLayers.layers,
                },
              },
            },
          },
        }))
      },

      setMapState: (projectId: string, mapState: Partial<MapState>) => {
        set((state) => ({
          projectMaps: {
            ...state.projectMaps,
            [projectId]: {
              ...(state.projectMaps[projectId] || DEFAULT_PROJECT_MAP_STATE),
              mapState: {
                ...(state.projectMaps[projectId]?.mapState || DEFAULT_PROJECT_MAP_STATE.mapState),
                ...mapState,
              },
            },
          },
        }))
      },

      setBaseLayer: (projectId: string, baseLayer: 'satellite' | 'street') => {
        get().setMapState(projectId, { baseLayer })
      },

      setLegendExpanded: (projectId: string, expanded: boolean) => {
        get().setMapState(projectId, { legendExpanded: expanded })
      },

      clearProjectMap: (projectId: string) => {
        set((state) => {
          const { [projectId]: _, ...rest } = state.projectMaps
          return { projectMaps: rest }
        })
      },
    }),
    {
      name: 'map-storage',
    }
  )
)
