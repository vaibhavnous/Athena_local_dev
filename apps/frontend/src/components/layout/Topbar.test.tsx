import React from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import Topbar from './Topbar'

jest.mock('../../store/useThemeStore', () => ({
  __esModule: true,
  default: () => ({ theme: 'dark', toggleTheme: jest.fn() }),
}))

jest.mock('../shared/NewRunModal', () => () => null)

test('opens mobile navigation from the top bar', () => {
  const onOpenNavigation = jest.fn()
  render(<Topbar onOpenNavigation={onOpenNavigation} />)

  fireEvent.click(screen.getByRole('button', { name: 'Open navigation' }))

  expect(onOpenNavigation).toHaveBeenCalledTimes(1)
})
