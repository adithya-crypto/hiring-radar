import './globals.css'
import React from 'react'

export const metadata = { title: 'Hiring Radar', description: 'Active hiring tracker' }

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-zinc-50 text-zinc-900">
        <div className="mx-auto max-w-5xl p-6">
          <header className="mb-8">
            <h1 className="text-2xl font-bold">Hiring Radar</h1>
            <p className="text-sm text-zinc-600">Live view of companies actively hiring SDEs</p>
          </header>
          {children}
        </div>
      </body>
    </html>
  )
}
