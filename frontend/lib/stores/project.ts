/**
 * Project store using Zustand
 */
import { create } from 'zustand'
import type { Project } from '@/lib/api/types'

interface ProjectState {
    currentProject: Project | null
    projects: Project[]
    setCurrentProject: (project: Project | null) => void
    setProjects: (projects: Project[]) => void
    addProject: (project: Project) => void
    updateProject: (project: Project) => void
    removeProject: (projectId: string) => void
}

export const useProjectStore = create<ProjectState>((set) => ({
    currentProject: null,
    projects: [],

    setCurrentProject: (project) => set({ currentProject: project }),

    setProjects: (projects) => set({ projects }),

    addProject: (project) =>
        set((state) => ({
            projects: [project, ...state.projects],
        })),

    updateProject: (project) =>
        set((state) => ({
            projects: state.projects.map((p) => (p.id === project.id ? project : p)),
            currentProject:
                state.currentProject?.id === project.id ? project : state.currentProject,
        })),

    removeProject: (projectId) =>
        set((state) => ({
            projects: state.projects.filter((p) => p.id !== projectId),
            currentProject: state.currentProject?.id === projectId ? null : state.currentProject,
        })),
}))
