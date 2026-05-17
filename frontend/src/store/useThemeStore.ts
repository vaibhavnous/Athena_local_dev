import { create } from 'zustand'

const THEME_STORAGE_KEY = 'athena.theme'

function loadPersistedTheme(): 'dark' | 'light' {
  if (typeof window === 'undefined') return 'dark'
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY)
    return stored === 'light' ? 'light' : 'dark'
  } catch (error) {
    console.warn('[ThemeStore] Failed to load persisted theme:', error)
    return 'dark'
  }
}

function persistTheme(theme: string) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme)
  } catch (error) {
    console.warn('[ThemeStore] Failed to persist theme:', error)
  }
}

interface ThemeState {
  theme: 'dark' | 'light'
  setTheme: (theme: 'dark' | 'light') => void
  toggleTheme: () => void
}

const useThemeStore = create<ThemeState>((set) => ({
  theme: loadPersistedTheme(),

  setTheme: (theme) => {
    persistTheme(theme)
    set({ theme })
  },

  toggleTheme: () =>
    set((state) => {
      const newTheme = state.theme === 'dark' ? 'light' : 'dark'
      persistTheme(newTheme)
      return { theme: newTheme }
    })
}))

export default useThemeStore
