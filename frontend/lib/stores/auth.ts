/**
 * Authentication store using Zustand
 */

import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { login as apiLogin, getCurrentUser } from '@/lib/api/services'
import type { User } from '@/lib/api/types'

// Helper to decode JWT and get expiry
function getTokenExpiry(token: string): number | null {
    try {
        const payload = JSON.parse(atob(token.split('.')[1]))
        // exp is in seconds since epoch
        return typeof payload.exp === 'number' ? payload.exp * 1000 : null
    } catch {
        return null
    }
}


interface AuthState {
    user: User | null
    token: string | null
    tokenExpiry: number | null
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
            tokenExpiry: null,
            isAuthenticated: false,
            isLoading: false,

            login: async (username: string, password: string) => {
                set({ isLoading: true })
                try {
                    const response = await apiLogin(username, password)
                    const expiry = getTokenExpiry(response.access_token)
                    localStorage.setItem('access_token', response.access_token)
                    if (expiry) {
                        localStorage.setItem('access_token_expiry', expiry.toString())
                    } else {
                        localStorage.removeItem('access_token_expiry')
                    }
                    const user = await getCurrentUser()
                    set({
                        user,
                        token: response.access_token,
                        tokenExpiry: expiry,
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
                localStorage.removeItem('access_token_expiry')
                set({
                    user: null,
                    token: null,
                    tokenExpiry: null,
                    isAuthenticated: false,
                })
            },

            checkAuth: async () => {
                const token = localStorage.getItem('access_token')
                const expiryStr = localStorage.getItem('access_token_expiry')
                const expiry = expiryStr ? parseInt(expiryStr, 10) : null
                if (!token || !expiry || Date.now() > expiry) {
                    localStorage.removeItem('access_token')
                    localStorage.removeItem('access_token_expiry')
                    set({
                        user: null,
                        token: null,
                        tokenExpiry: null,
                        isAuthenticated: false,
                    })
                    return
                }
                try {
                    const user = await getCurrentUser()
                    set({
                        user,
                        token,
                        tokenExpiry: expiry,
                        isAuthenticated: true,
                    })
                } catch (error) {
                    localStorage.removeItem('access_token')
                    localStorage.removeItem('access_token_expiry')
                    set({
                        user: null,
                        token: null,
                        tokenExpiry: null,
                        isAuthenticated: false,
                    })
                }
            },
        }),
        {
            name: 'auth-storage',
            partialize: (state) => ({ token: state.token, tokenExpiry: state.tokenExpiry }),
        }
    )
)

