/**
 * Utility functions for project navigation based on project status
 */

interface Project {
  status?: string
  khasra_count?: number
}

/**
 * Determine the appropriate workflow page based on project status
 * Returns the latest completed step for the project
 */
export function getWorkflowPageForProject(project: Project | null | undefined): number {
  if (!project) return 1

  // Determine page based on project status
  if (project.status === 'clustered' || project.status === 'completed') {
    return 4 // Export page
  } else if (project.status === 'layers_added') {
    return 3 // Clustering page
  } else if (project.status === 'khasras_uploaded') {
    return 2 // Layers page
  } else if (project.khasra_count && project.khasra_count > 0) {
    // Fallback: if khasras exist, go to layers page
    return 2
  }

  return 1 // Default to upload page
}
