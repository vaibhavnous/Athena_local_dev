// @ts-nocheck
import React from 'react'
import { useNavigate } from 'react-router-dom'
import NewRunModal from '../components/shared/NewRunModal'

function NewRunPage() {
  const navigate = useNavigate()

  return (
    <div className="h-full min-h-0 overflow-hidden">
      <NewRunModal
        isOpen
        pageMode
        onClose={() => navigate('/app')}
      />
    </div>
  )
}

export default NewRunPage
