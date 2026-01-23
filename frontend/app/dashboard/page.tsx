"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { ProtectedRoute } from "@/components/protected-route"
import { useAuthStore } from "@/lib/stores/auth"
import { useProjectStore } from "@/lib/stores/project"
import * as api from "@/lib/api/services"
import { Plus, FolderOpen, LogOut, Sun, Trash2, Calendar, MapPin } from "lucide-react"

export default function DashboardPage() {
    return (
        <ProtectedRoute>
            <DashboardContent />
        </ProtectedRoute>
    )
}

function DashboardContent() {
    const router = useRouter()
    const { user, logout } = useAuthStore()
    const { projects, setProjects, setCurrentProject, removeProject } = useProjectStore()
    const [isLoading, setIsLoading] = useState(true)
    const [showCreateModal, setShowCreateModal] = useState(false)
    const [newProject, setNewProject] = useState({ name: '', location: '', description: '' })
    const [isCreating, setIsCreating] = useState(false)

    useEffect(() => {
        loadProjects()
    }, [])

    const loadProjects = async () => {
        try {
            const response = await api.listProjects()
            setProjects(response.projects)
        } catch (error) {
            console.error('Failed to load projects:', error)
        } finally {
            setIsLoading(false)
        }
    }

    const handleCreateProject = async (e: React.FormEvent) => {
        e.preventDefault()
        setIsCreating(true)

        try {
            const project = await api.createProject(newProject)
            setProjects([project, ...projects])
            setNewProject({ name: '', location: '', description: '' })
            setShowCreateModal(false)
        } catch (error: any) {
            alert(error.response?.data?.detail || 'Failed to create project')
        } finally {
            setIsCreating(false)
        }
    }

    const handleSelectProject = (project: any) => {
        setCurrentProject(project)
        router.push(`/workflow/${project.id}`)
    }

    const handleDeleteProject = async (projectId: string, e: React.MouseEvent) => {
        e.stopPropagation()
        if (!confirm('Are you sure you want to delete this project? This action cannot be undone.')) {
            return
        }

        try {
            await api.deleteProject(projectId)
            removeProject(projectId)
        } catch (error: any) {
            alert(error.response?.data?.detail || 'Failed to delete project')
        }
    }

    const handleLogout = () => {
        logout()
        router.push('/login')
    }

    return (
        <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-slate-100">
            {/* Header */}
            <header className="bg-white border-b border-slate-200 shadow-sm">
                <div className="container mx-auto px-6 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <div className="bg-blue-600 p-2 rounded-lg">
                                <Sun className="w-6 h-6 text-white" />
                            </div>
                            <div>
                                <h1 className="text-xl font-bold text-slate-900">Solar Park Analysis</h1>
                                <p className="text-xs text-slate-600">Parcel Identification System</p>
                            </div>
                        </div>
                        <div className="flex items-center gap-4">
                            <div className="text-right">
                                <p className="text-sm font-medium text-slate-900">{user?.username}</p>
                                <p className="text-xs text-slate-500">Logged in</p>
                            </div>
                            <button
                                onClick={handleLogout}
                                className="flex items-center gap-2 px-4 py-2 text-slate-700 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
                            >
                                <LogOut className="w-4 h-4" />
                                <span className="text-sm font-medium">Logout</span>
                            </button>
                        </div>
                    </div>
                </div>
            </header>

            {/* Main Content */}
            <main className="container mx-auto px-6 py-12 max-w-7xl">
                {/* Title and Create Button */}
                <div className="flex items-center justify-between mb-8">
                    <div>
                        <h2 className="text-3xl font-bold text-slate-900 mb-2">Projects</h2>
                        <p className="text-slate-600">Select a project or create a new one to get started</p>
                    </div>
                    <button
                        onClick={() => setShowCreateModal(true)}
                        className="flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg shadow-md hover:shadow-lg transition-all"
                    >
                        <Plus className="w-5 h-5" />
                        New Project
                    </button>
                </div>

                {/* Projects Grid */}
                {isLoading ? (
                    <div className="flex items-center justify-center py-20">
                        <div className="text-center">
                            <div className="w-12 h-12 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
                            <p className="text-slate-600">Loading projects...</p>
                        </div>
                    </div>
                ) : projects.length === 0 ? (
                    <div className="text-center py-20">
                        <FolderOpen className="w-16 h-16 text-slate-300 mx-auto mb-4" />
                        <h3 className="text-xl font-semibold text-slate-900 mb-2">No projects yet</h3>
                        <p className="text-slate-600 mb-6">Create your first project to start analyzing solar parcels</p>
                        <button
                            onClick={() => setShowCreateModal(true)}
                            className="inline-flex items-center gap-2 px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
                        >
                            <Plus className="w-5 h-5" />
                            Create Project
                        </button>
                    </div>
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                        {projects.map((project) => (
                            <div
                                key={project.id}
                                onClick={() => handleSelectProject(project)}
                                className="bg-white rounded-xl shadow-md hover:shadow-xl transition-all cursor-pointer border border-slate-200 hover:border-blue-300 p-6 group"
                            >
                                <div className="flex items-start justify-between mb-4">
                                    <div className="flex-1">
                                        <h3 className="text-lg font-semibold text-slate-900 group-hover:text-blue-600 transition-colors mb-1">
                                            {project.name}
                                        </h3>
                                        <div className="flex items-center gap-1 text-sm text-slate-500">
                                            <MapPin className="w-3 h-3" />
                                            {project.location}
                                        </div>
                                    </div>
                                    <button
                                        onClick={(e) => handleDeleteProject(project.id, e)}
                                        className="p-2 text-slate-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                                        title="Delete project"
                                    >
                                        <Trash2 className="w-4 h-4" />
                                    </button>
                                </div>

                                {project.description && (
                                    <p className="text-sm text-slate-600 mb-4 line-clamp-2">{project.description}</p>
                                )}

                                <div className="space-y-2 mb-4">
                                    <div className="flex items-center justify-between text-sm">
                                        <span className="text-slate-600">Status</span>
                                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${project.status === 'completed' ? 'bg-green-100 text-green-700' :
                                                project.status === 'error' ? 'bg-red-100 text-red-700' :
                                                    'bg-blue-100 text-blue-700'
                                            }`}>
                                            {project.status}
                                        </span>
                                    </div>
                                    {project.khasra_count !== null && (
                                        <div className="flex items-center justify-between text-sm">
                                            <span className="text-slate-600">Khasras</span>
                                            <span className="font-semibold text-slate-900">{project.khasra_count || 0}</span>
                                        </div>
                                    )}
                                    {project.total_area_ha !== null && (
                                        <div className="flex items-center justify-between text-sm">
                                            <span className="text-slate-600">Total Area</span>
                                            <span className="font-semibold text-slate-900">{project.total_area_ha?.toFixed(2)} ha</span>
                                        </div>
                                    )}
                                </div>

                                <div className="pt-4 border-t border-slate-100 flex items-center gap-2 text-xs text-slate-500">
                                    <Calendar className="w-3 h-3" />
                                    {new Date(project.created_at).toLocaleDateString()}
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </main>

            {/* Create Project Modal */}
            {showCreateModal && (
                <div className="fixed inset-0 bg-white/30 backdrop-blur-xs flex items-center justify-center p-4 z-50">
                    <div className="bg-white rounded-2xl shadow-2xl max-w-md w-full p-8">
                        <h3 className="text-2xl font-bold text-slate-900 mb-6">Create New Project</h3>
                        <form onSubmit={handleCreateProject} className="space-y-5">
                            <div>
                                <label htmlFor="name" className="block text-sm font-medium text-slate-700 mb-2">
                                    Project Name *
                                </label>
                                <input
                                    id="name"
                                    type="text"
                                    value={newProject.name}
                                    onChange={(e) => setNewProject({ ...newProject, name: e.target.value })}
                                    className="w-full px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    placeholder="e.g., Sagar District Solar"
                                    required
                                />
                            </div>
                            <div>
                                <label htmlFor="location" className="block text-sm font-medium text-slate-700 mb-2">
                                    Location *
                                </label>
                                <input
                                    id="location"
                                    type="text"
                                    value={newProject.location}
                                    onChange={(e) => setNewProject({ ...newProject, location: e.target.value })}
                                    className="w-full px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                    placeholder="e.g., Madhya Pradesh"
                                    required
                                />
                            </div>
                            <div>
                                <label htmlFor="description" className="block text-sm font-medium text-slate-700 mb-2">
                                    Description (optional)
                                </label>
                                <textarea
                                    id="description"
                                    value={newProject.description}
                                    onChange={(e) => setNewProject({ ...newProject, description: e.target.value })}
                                    className="w-full px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 resize-none"
                                    rows={3}
                                    placeholder="Brief description of the project..."
                                />
                            </div>
                            <div className="flex gap-3 pt-4">
                                <button
                                    type="button"
                                    onClick={() => {
                                        setShowCreateModal(false)
                                        setNewProject({ name: '', location: '', description: '' })
                                    }}
                                    className="flex-1 px-4 py-3 bg-slate-200 hover:bg-slate-300 text-slate-700 font-semibold rounded-lg transition-colors"
                                >
                                    Cancel
                                </button>
                                <button
                                    type="submit"
                                    disabled={isCreating}
                                    className="flex-1 px-4 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white font-semibold rounded-lg transition-colors"
                                >
                                    {isCreating ? 'Creating...' : 'Create Project'}
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
            )}
        </div>
    )
}
