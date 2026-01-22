/**
 * Authentication store using Zustand
 */
import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { login as apiLogin, getCurrentUser } from '@/lib/api/services'
import type { User } from '@/lib/api/types'

interface AuthState {
    user: User | null
    token: string | null
    isAuthenticated: boolean
    isLoading: boolean
    login: (username: string, password: string) => Promise<void>
    logout: () => void
    checkAuth: () => Promise<void>
}

export const useAuthStore = create<AuthState>()(
    persist(
        (set) => ({
            user: null,
            token: null,
            isAuthenticated: false,
            isLoading: false,

            login: async (username: string, password: string) => {
                set({ isLoading: true })
                try {
                    const response = await apiLogin(username, password)
                    localStorage.setItem('access_token', response.access_token)

                    const user = await getCurrentUser()
                    set({
                        user,
                        token: response.access_token,
                        isAuthenticated: true,
                        isLoading: false,
                    })
                } catch (error) {
                    set({ isLoading: false })
                    throw error
                }
            },

            logout: () => {
                localStorage.removeItem('access_token')
                set({
                    user: null,
                    token: null,
                    isAuthenticated: false,
                })
            },

            checkAuth: async () => {
                const token = localStorage.getItem('access_token')
                if (!token) {
                    set({ isAuthenticated: false })
                    return
                }

                try {
                    const user = await getCurrentUser()
                    set({
                        user,
                        token,
                        isAuthenticated: true,
                    })
                } catch (error) {
                    localStorage.removeItem('access_token')
                    set({
                        user: null,
                        token: null,
                        isAuthenticated: false,
                    })
                }
            },
        }),
        {
            name: 'auth-storage',
            partialize: (state) => ({ token: state.token }),
        }
    )
)
