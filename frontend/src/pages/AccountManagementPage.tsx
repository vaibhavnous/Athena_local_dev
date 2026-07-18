import React, { useEffect, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import {
  AlertTriangle,
  CheckCircle2,
  Edit2,
  Eye,
  EyeOff,
  Loader2,
  Save,
  Trash2,
  User,
  UserCheck,
  UserPlus,
  UserX,
  X
} from 'lucide-react'
import {
  createAuthUser,
  deleteAuthUser,
  getAuthUsers,
  setAuthUserStatus,
  updateAuthUser,
  type AuthUser,
  type UserType
} from '../api/athenaApi'
import { PageFrame, PageHeader } from '../components/shared/DashboardLayout'
import { useAuth } from '../context/AuthContext'

type IconType = React.ComponentType<{ size?: number; className?: string }>

interface UserFormState {
  username: string
  email: string
  password: string
  userType: UserType
}
const STATUS_MESSAGE_DISMISS_MS = 4000

function AccountManagementPage() {
  const [users, setUsers] = useState<AuthUser[]>([])
  const [showCreateForm, setShowCreateForm] = useState(false)
  const [editingUser, setEditingUser] = useState<AuthUser | null>(null)
  const [confirmDeleteUid, setConfirmDeleteUid] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [successMessage, setSuccessMessage] = useState('')
  const [isLoadingUsers, setIsLoadingUsers] = useState(false)

  const { user: currentUser } = useAuth()
  const isPrimaryAdmin = Boolean(currentUser?.canManageAccounts)

  useEffect(() => {
    if (isPrimaryAdmin) {
      fetchUsers()
    }
  }, [isPrimaryAdmin])

  useEffect(() => {
    if (!successMessage) {
      return
    }

    const timeoutId = window.setTimeout(() => {
      setSuccessMessage('')
    }, STATUS_MESSAGE_DISMISS_MS)

    return () => window.clearTimeout(timeoutId)
  }, [successMessage])

  const getValidationMessage = (currentForm: UserFormState, requirePassword = false) => {
    const normalizedUsername = currentForm.username.trim()
    const normalizedEmail = currentForm.email.trim()
    const emailLocalPart = normalizedEmail.split('@')[0] ?? ''
    const shouldValidatePassword = requirePassword || currentForm.password.length > 0

    if (normalizedUsername.length < 2) {
      return 'User name must be at least 2 characters long'
    }

    if (!/[a-z]/i.test(normalizedUsername)) {
      return 'User name must contain at least one alphabet'
    }

    if (!normalizedEmail) {
      return 'Email is required'
    }

    if (!/[a-z]/i.test(emailLocalPart)) {
      return 'Email must contain at least one alphabet before @'
    }

    if (requirePassword && !currentForm.password) {
      return 'Password is required'
    }

    if (shouldValidatePassword && currentForm.password.length < 12) {
      return 'Password must be at least 12 characters long'
    }

    if (
      shouldValidatePassword &&
      (!/[a-z]/i.test(currentForm.password) ||
        !/[0-9]/.test(currentForm.password) ||
        !/[^a-z0-9]/i.test(currentForm.password))
    ) {
      return 'Password must contain alphabet, number, and special character'
    }

    if (currentForm.userType !== 'Admin' && currentForm.userType !== 'Client') {
      return 'User type is required'
    }

    return ''
  }

  const fetchUsers = async () => {
    setIsLoadingUsers(true)
    setErrorMessage('')

    try {
      const response = await getAuthUsers()
      setUsers(response.users)
    } catch (error: any) {
      setErrorMessage(error?.message ?? 'Unable to load users.')
    } finally {
      setIsLoadingUsers(false)
    }
  }

  const startEditing = (user: AuthUser) => {
    setEditingUser(user)
    setShowCreateForm(false)
    setConfirmDeleteUid('')
    setErrorMessage('')
    setSuccessMessage('')
  }

  const handleCreateUser = async (form: UserFormState) => {
    setErrorMessage('')
    setSuccessMessage('')

    const validationMessage = getValidationMessage(form, true)

    if (validationMessage) {
      throw new Error(validationMessage)
    }

    const response = await createAuthUser({
      username: form.username.trim(),
      email: form.email.trim(),
      password: form.password,
      userType: form.userType
    })

    setUsers((current) => [response.user, ...current])
    setShowCreateForm(false)
    setSuccessMessage(`Account created for ${response.user.email}`)
  }

  const cancelEditing = () => {
    setEditingUser(null)
  }

  const handleUpdateUser = async (uid: string, form: UserFormState) => {
    setErrorMessage('')
    setSuccessMessage('')

    const validationMessage = getValidationMessage(form)

    if (validationMessage) {
      throw new Error(validationMessage)
    }

    const response = await updateAuthUser(uid, {
      username: form.username.trim(),
      email: form.email.trim(),
      userType: form.userType,
      ...(form.password ? { password: form.password } : {})
    })

    setUsers((current) =>
      current.map((user) => (user.uid === uid ? response.user : user))
    )
    setSuccessMessage(`Updated ${response.user.username}`)
    cancelEditing()
  }

  const handleToggleUser = async (user: AuthUser) => {
    setErrorMessage('')
    setSuccessMessage('')
    setConfirmDeleteUid('')

    try {
      const response = await setAuthUserStatus(user.uid, !(user.isActive ?? true))

      setUsers((current) =>
        current.map((item) => (item.uid === user.uid ? response.user : item))
      )
      setSuccessMessage(
        `${response.user.username} ${response.user.isActive ? 'enabled' : 'disabled'}`
      )
    } catch (error: any) {
      setErrorMessage(error?.message ?? 'Unable to update user status.')
    }
  }

  const handleDeleteUser = async (user: AuthUser) => {
    setErrorMessage('')
    setSuccessMessage('')

    try {
      await deleteAuthUser(user.uid)
      setUsers((current) => current.filter((item) => item.uid !== user.uid))
      setSuccessMessage(`Removed ${user.email}`)
      setConfirmDeleteUid('')

      if (editingUser?.uid === user.uid) {
        cancelEditing()
      }
    } catch (error: any) {
      setErrorMessage(error?.message ?? 'Unable to remove user.')
    }
  }

  if (!isPrimaryAdmin) {
    return (
      <PageFrame>
        <PageHeader
          eyebrow="Accounts"
          title="Account management."
          description="Manage Astra-Data users and access roles."
          icon={UserPlus}
        />

        <div className="card max-w-3xl p-6">
          <div className="mb-4 flex h-10 w-10 items-center justify-center rounded-lg border border-accent-red/30 bg-red-950/20">
            <AlertTriangle size={18} className="text-accent-red" />
          </div>
          <h2 className="text-sm font-semibold text-white">Access restricted</h2>
          <p className="mt-2 text-sm text-gray-500">
            Only the primary admin account can manage Astra-Data accounts.
          </p>
        </div>
      </PageFrame>
    )
  }

  return (
    <PageFrame>
      <PageHeader
        eyebrow="Accounts"
        title="Account management."
        description="Create users, update account details, and control access status."
        icon={UserPlus}
        actions={
          <button
            type="button"
            onClick={() => {
              setEditingUser(null)
              setConfirmDeleteUid('')
              setShowCreateForm(true)
            }}
            className="btn-primary flex items-center gap-2"
          >
            <UserPlus size={14} />
            Add New User
          </button>
        }
      />

      {successMessage && (
        <StatusMessage tone="success" message={successMessage} />
      )}

      {errorMessage && (
        <StatusMessage tone="error" message={errorMessage} />
      )}

      {isLoadingUsers ? (
        <div className="card flex flex-col items-center gap-3 p-12">
          <Loader2 size={24} className="animate-spin text-accent-blue" />
          <p className="text-sm text-gray-500">Loading users...</p>
        </div>
      ) : users.length === 0 ? (
        <div className="card flex flex-col items-center gap-3 p-12 text-center">
          <div className="flex h-14 w-14 items-center justify-center rounded-xl border border-bg-border bg-bg-base">
            <User size={24} className="text-gray-600" />
          </div>
          <p className="text-sm font-medium text-gray-400">No users found</p>
          <p className="max-w-xs text-xs text-gray-600">
            Click <span className="text-accent-blue">Add New User</span> to create an Astra-Data account.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {users.map((user) => {
            const isPrimaryRow = user.canManageAccounts

            return (
              <UserCard
                key={user.uid}
                user={user}
                isPrimaryRow={isPrimaryRow}
                confirmDelete={confirmDeleteUid === user.uid}
                onEdit={() => startEditing(user)}
                onToggleStatus={() => handleToggleUser(user)}
                onAskDelete={() => {
                  setEditingUser(null)
                  setConfirmDeleteUid(user.uid)
                }}
                onCancelDelete={() => setConfirmDeleteUid('')}
                onConfirmDelete={() => handleDeleteUser(user)}
              />
            )
          })}
        </div>
      )}

      <AnimatePresence>
        {showCreateForm && (
          <UserFormPanel
            title="Add User"
            description="Create an Astra-Data account"
            initialForm={{
              username: '',
              email: '',
              password: '',
              userType: 'Client'
            }}
            passwordRequired
            onSave={handleCreateUser}
            onClose={() => setShowCreateForm(false)}
          />
        )}
        {editingUser && (
          <UserFormPanel
            key={editingUser.uid}
            title="Edit User"
            description="Update Astra-Data account details"
            initialForm={{
              username: editingUser.username,
              email: editingUser.email,
              password: '',
              userType: editingUser.userType
            }}
            isPrimaryRow={editingUser.canManageAccounts}
            onSave={(form) => handleUpdateUser(editingUser.uid, form)}
            onClose={cancelEditing}
          />
        )}
      </AnimatePresence>
    </PageFrame>
  )
}

function UserFormPanel({
  title,
  description,
  initialForm,
  passwordRequired = false,
  isPrimaryRow = false,
  onSave,
  onClose
}: {
  title: string
  description: string
  initialForm: UserFormState
  passwordRequired?: boolean
  isPrimaryRow?: boolean
  onSave: (form: UserFormState) => Promise<void>
  onClose: () => void
}) {
  const [form, setForm] = useState<UserFormState>(initialForm)
  const [errorMessage, setErrorMessage] = useState('')
  const [saving, setSaving] = useState(false)
  const [showPassword, setShowPassword] = useState(false)
  const submitLabel = passwordRequired ? 'Save' : 'Update'

  const set = (key: keyof UserFormState, value: string) => {
    setForm((current) => ({
      ...current,
      [key]: value
    }))
    setErrorMessage('')
  }

  const handleSave = async () => {
    setSaving(true)
    setErrorMessage('')

    try {
      await onSave(form)
    } catch (error: any) {
      const message =
        error?.message === 'Network Error'
          ? 'Unable to reach account API. Please make sure the backend is running on http://localhost:8000.'
          : error?.message ?? 'Account save failed. Please try again.'

      setErrorMessage(message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      <motion.div
        initial={{ x: '100%' }}
        animate={{ x: 0 }}
        exit={{ x: '100%' }}
        transition={{ type: 'spring', stiffness: 300, damping: 30 }}
        className="fixed right-0 top-0 z-50 flex h-full w-full max-w-lg flex-col border-l border-bg-border bg-bg-card shadow-2xl"
      >
        <div className="flex flex-shrink-0 items-center justify-between border-b border-bg-border px-6 py-4">
          <div>
            <h2 className="text-lg font-bold text-white">{title}</h2>
            <p className="mt-0.5 text-xs text-gray-500">{description}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="flex h-8 w-8 items-center justify-center rounded-lg text-gray-400 transition-colors hover:bg-bg-border hover:text-white"
          >
            <X size={16} />
          </button>
        </div>

        <div className="flex-1 space-y-5 overflow-y-auto p-6">
          <FormField label="User Name" required>
            <input
              type="text"
              className="input-field"
              placeholder="Full name"
              value={form.username}
              onChange={(e) => set('username', e.target.value)}
            />
          </FormField>

          <FormField label="Email" required>
            <input
              type="email"
              className="input-field"
              placeholder="user@company.com"
              value={form.email}
              disabled={isPrimaryRow}
              onChange={(e) => set('email', e.target.value)}
            />
          </FormField>

          <FormField
            label="Password"
            required={passwordRequired}
            hint={passwordRequired ? undefined : 'Leave blank to keep current password'}
          >
            <div className="relative">
              <input
                type={showPassword ? 'text' : 'password'}
                className="login-password-input input-field pr-9"
                placeholder={passwordRequired ? 'Password' : 'New password'}
                value={form.password}
                onChange={(e) => set('password', e.target.value)}
              />
              <button
                type="button"
                onClick={() => setShowPassword((current) => !current)}
                className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-500 transition-colors hover:text-gray-300"
              >
                {showPassword ? <EyeOff size={14} /> : <Eye size={14} />}
              </button>
            </div>
          </FormField>

          <FormField label="User Type" required>
            <UserTypeControl
              value={form.userType}
              disabled={isPrimaryRow}
              lockUnselected={!passwordRequired}
              onChange={(value) => set('userType', value)}
            />
          </FormField>

          {errorMessage && (
            <div className="flex items-start gap-2 rounded-lg border border-accent-red/30 bg-red-950/20 p-3">
              <AlertTriangle size={14} className="mt-0.5 flex-shrink-0 text-accent-red" />
              <p className="text-xs text-accent-red">{errorMessage}</p>
            </div>
          )}
        </div>

        <div className="flex flex-shrink-0 gap-3 border-t border-bg-border px-6 py-4">
          <button
            type="button"
            onClick={onClose}
            disabled={saving}
            className="btn-secondary flex-1 disabled:cursor-not-allowed disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="btn-primary flex flex-1 items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {saving ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Saving...
              </>
            ) : (
              <>
                <Save size={14} />
                {submitLabel}
              </>
            )}
          </button>
        </div>
      </motion.div>
    </>
  )
}

function UserCard({
  user,
  isPrimaryRow,
  confirmDelete,
  onEdit,
  onToggleStatus,
  onAskDelete,
  onCancelDelete,
  onConfirmDelete
}: {
  user: AuthUser
  isPrimaryRow: boolean
  confirmDelete: boolean
  onEdit: () => void
  onToggleStatus: () => void
  onAskDelete: () => void
  onCancelDelete: () => void
  onConfirmDelete: () => void
}) {
  const isActive = user.isActive ?? true

  return (
    <div className="card flex items-start gap-4 p-5">
      <div className="mt-0.5 flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-lg border border-accent-blue/20 bg-accent-blue/10">
        <User size={18} className="text-accent-blue" />
      </div>

      <div className="min-w-0 flex-1">
        <div className="mb-1 flex flex-wrap items-center gap-2">
          <span className="truncate text-sm font-semibold text-white">
            {user.username || 'Unnamed User'}
          </span>
          <Badge
            tone={isActive ? 'blue' : 'red'}
            icon={isActive ? UserCheck : UserX}
            label={isActive ? 'Enabled' : 'Disabled'}
          />
        </div>

        <div className="mt-2 grid gap-x-6 gap-y-1 md:grid-cols-2">
          <Detail label="Email" value={user.email} />
          <Detail label="Access" value={isPrimaryRow ? 'Primary admin' : user.userType} />
        </div>

      </div>

      <div className="flex flex-shrink-0 items-center gap-2">
        {confirmDelete ? (
          <div className="flex items-center gap-1">
            <button
              type="button"
              onClick={onConfirmDelete}
              className="px-2 text-xs font-semibold text-accent-red hover:text-red-300"
            >
              Delete
            </button>
            <button
              type="button"
              onClick={onCancelDelete}
              className="px-1 text-xs text-gray-500 hover:text-gray-300"
            >
              Cancel
            </button>
          </div>
        ) : (
          <>
            <ToggleSwitch
              checked={isActive}
              onChange={onToggleStatus}
              disabled={isPrimaryRow}
              label={isActive ? 'Disable user' : 'Enable user'}
            />
            <IconButton label="Edit" icon={Edit2} onClick={onEdit} />
            <IconButton
              label="Delete"
              icon={Trash2}
              onClick={onAskDelete}
              disabled={isPrimaryRow}
              tone="danger"
            />
          </>
        )}
      </div>
    </div>
  )
}

function UserTypeControl({
  value,
  disabled,
  lockUnselected,
  onChange
}: {
  value: UserType
  disabled: boolean
  lockUnselected: boolean
  onChange: (value: UserType) => void
}) {
  const options: UserType[] = ['Client', 'Admin']

  return (
    <div className="grid grid-cols-2 gap-3">
      {options.map((option) => {
        const isSelected = value === option
        const isDisabled = disabled || (lockUnselected && !isSelected)

        return (
          <button
            key={option}
            type="button"
            aria-pressed={isSelected}
            disabled={isDisabled}
            onClick={() => onChange(option)}
            className={`rounded-lg border px-4 py-3 text-sm font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${
              isSelected
                ? 'border-accent-blue bg-accent-blue/20 text-accent-blue'
                : 'border-bg-border bg-bg-base text-gray-500 hover:border-accent-blue/40 hover:text-gray-300'
            }`}
          >
            {option}
          </button>
        )
      })}
    </div>
  )
}

function ToggleSwitch({
  checked,
  onChange,
  disabled = false,
  label
}: {
  checked: boolean
  onChange: () => void
  disabled?: boolean
  label: string
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label={label}
      title={label}
      onClick={onChange}
      disabled={disabled}
      className={`relative h-6 w-11 rounded-full border transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${
        checked
          ? 'border-accent-blue/40 bg-accent-blue/20'
          : 'border-bg-border bg-bg-base'
      }`}
    >
      <span
        className={`absolute left-0 top-1/2 h-4 w-4 -translate-y-1/2 rounded-full transition-transform ${
          checked
            ? 'translate-x-6 bg-accent-blue'
            : 'translate-x-1 bg-gray-500'
        }`}
      />
    </button>
  )
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline gap-1.5 overflow-hidden">
      <span className="flex-shrink-0 text-xs text-gray-500">{label}:</span>
      <span className="truncate text-xs text-gray-300">{value}</span>
    </div>
  )
}

function Badge({
  label,
  tone,
  icon: Icon
}: {
  label: string
  tone: 'blue' | 'green' | 'red'
  icon?: IconType
}) {
  const toneClass =
    tone === 'green'
      ? 'border-green-500/30 bg-green-950/20 text-green-400'
      : tone === 'red'
        ? 'border-accent-red/30 bg-red-950/20 text-accent-red'
        : 'border-accent-blue/20 bg-accent-blue/10 text-accent-blue'

  return (
    <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs ${toneClass}`}>
      {Icon && <Icon size={12} />}
      {label}
    </span>
  )
}

function FormField({
  label,
  children,
  hint,
  required
}: {
  label: string
  children: React.ReactNode
  hint?: string
  required?: boolean
}) {
  return (
    <div>
      <label className="label">
        {label}
        {required && <span className="ml-0.5 text-accent-red">*</span>}
      </label>
      {children}
      {hint && <p className="mt-1 text-xs text-gray-600">{hint}</p>}
    </div>
  )
}

function IconButton({
  label,
  icon: Icon,
  onClick,
  disabled = false,
  tone = 'default',
  muted = false
}: {
  label: string
  icon: IconType
  onClick: () => void
  disabled?: boolean
  tone?: 'default' | 'danger' | 'warning' | 'success'
  muted?: boolean
}) {
  const toneClass =
    tone === 'danger'
      ? 'text-gray-500 hover:bg-red-950/20 hover:text-accent-red'
      : tone === 'warning'
        ? 'text-gray-500 hover:bg-amber-950/20 hover:text-accent-amber'
        : tone === 'success'
          ? 'text-gray-500 hover:bg-green-950/20 hover:text-green-400'
          : muted
            ? 'text-gray-500 hover:bg-bg-border hover:text-gray-300'
            : 'text-gray-400 hover:bg-bg-border hover:text-white'

  return (
    <button
      type="button"
      title={label}
      aria-label={label}
      onClick={onClick}
      disabled={disabled}
      className={`flex h-8 w-8 items-center justify-center rounded-lg transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${toneClass}`}
    >
      <Icon size={14} />
    </button>
  )
}

function StatusMessage({ tone, message }: { tone: 'success' | 'error'; message: string }) {
  const isSuccess = tone === 'success'

  return (
    <div
      className={`flex items-start gap-2 rounded-lg border p-3 ${
        isSuccess
          ? 'border-accent-blue/30 bg-accent-blue/10'
          : 'border-accent-red/30 bg-red-950/20'
      }`}
    >
      {isSuccess ? (
        <CheckCircle2 size={14} className="mt-0.5 flex-shrink-0 text-accent-blue" />
      ) : (
        <AlertTriangle size={14} className="mt-0.5 flex-shrink-0 text-accent-red" />
      )}
      <p className={`text-xs ${isSuccess ? 'text-accent-blue' : 'text-accent-red'}`}>
        {message}
      </p>
    </div>
  )
}

export default AccountManagementPage
