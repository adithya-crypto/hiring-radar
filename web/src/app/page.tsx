'use client'
import React, { useEffect, useMemo, useState } from 'react'

const API = process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000'

type Score = {
  id?: number
  company_id: number
  role_family: string
  score: number
  computed_at?: string
  details_json?: { open_now?: number; new_last_4w?: number }
  evidence_urls?: string[]
  open_count?: number
  company_name?: string
}

type Company = { id: number; name: string; ticker?: string | null }

type Posting = {
  id: number
  title: string
  location?: string | null
  department?: string | null
  apply_url?: string | null
  created_at?: string
  updated_at?: string
  role_family?: string | null
}

type TimeFilter = 'all' | '24h' | '48h' | '72h' | '7d' | '14d' | '30d'
type ViewMode = 'top' | 'new'

// --- US location detection helpers (unchanged) ---
const STATE_ABBRS = new Set([
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN',
  'MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA',
  'WA','WV','WI','WY','DC'
])
const STATE_NAMES = [
  'Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia',
  'Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts',
  'Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey',
  'New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island',
  'South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia',
  'Wisconsin','Wyoming','District of Columbia','Washington, DC','D.C.','DC'
]
function isUS(loc?: string | null): boolean {
  if (!loc) return false
  const raw = loc.trim()
  const L = raw.toLowerCase()
  if (L.includes('united states') || L.includes('united states of america') || /\b(u\.s\.a?|usa)\b/i.test(raw)) return true
  if (/\b(us|u\.s\.a?)\b.*\bremote\b/i.test(raw) || /\bremote\b.*\b(us|u\.s\.a?)\b/i.test(raw) || /\bus-?remote\b/i.test(raw)) return true
  const abbrMatch = /(?:^|,|\s)\b([A-Z]{2})\b(?:\s|,|$)/.exec(raw)
  if (abbrMatch && STATE_ABBRS.has(abbrMatch[1])) { if (!/canada/i.test(raw)) return true }
  for (const name of STATE_NAMES) {
    const re = new RegExp(`\\b${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i')
    if (re.test(raw)) return true
  }
  return false
}

export default function Page() {
  const [scores, setScores] = useState<Score[]>([])
  const [companies, setCompanies] = useState<Record<number, Company>>({})
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [minScore, setMinScore] = useState<number>(0)
  const [companyQuery, setCompanyQuery] = useState<string>('')
  const [debouncedQuery, setDebouncedQuery] = useState<string>('')
  const [error, setError] = useState<string | null>(null)

  const [mode, setMode] = useState<ViewMode>('top')
  const [newDays, setNewDays] = useState<7 | 14 | 30>(7)

  // Evidence modal
  const [evidenceOpen, setEvidenceOpen] = useState(false)
  const [evidenceFor, setEvidenceFor] = useState<string>('')
  const [evidenceLinks, setEvidenceLinks] = useState<string[]>([])

  // Company panel
  const [companyPanelOpen, setCompanyPanelOpen] = useState(false)
  const [panelCompanyId, setPanelCompanyId] = useState<number | null>(null)
  const [panelTimeFilter, setPanelTimeFilter] = useState<TimeFilter>('all')
  const [panelLoading, setPanelLoading] = useState(false)
  const [panelPostings, setPanelPostings] = useState<Posting[]>([])
  const [usOnly, setUsOnly] = useState<boolean>(true)

  // --- Debounce search for smoother UX ---
  useEffect(() => {
    const t = setTimeout(() => setDebouncedQuery(companyQuery.trim().toLowerCase()), 200)
    return () => clearTimeout(t)
  }, [companyQuery])

  const panelCompanyName = useMemo(
    () => (panelCompanyId ? (companies[panelCompanyId]?.name || String(panelCompanyId)) : ''),
    [panelCompanyId, companies]
  )

  async function fetchTop() {
    setBusy(true); setError(null)
    try {
      const [sRes, cRes] = await Promise.all([
        fetch(`${API}/active_top?role_family=SDE&limit=50`),
        fetch(`${API}/companies`),
      ])
      if (!sRes.ok) throw new Error(`GET /active_top ${sRes.status}`)
      if (!cRes.ok) throw new Error(`GET /companies ${cRes.status}`)
      const [scoresJson, companiesJson] = await Promise.all([sRes.json(), cRes.json()])
      const cmap: Record<number, Company> = {}
      ;(companiesJson as Company[]).forEach((c) => (cmap[c.id] = c))
      setCompanies(cmap)
      setScores((scoresJson as Score[]).sort((a, b) => b.score - a.score))
    } catch (e: any) {
      setError(e?.message || 'Failed to load data')
    } finally { setBusy(false); setLoading(false) }
  }

  async function fetchTopNew(days = 7) {
    setBusy(true); setError(null)
    try {
      const [sRes, cRes] = await Promise.all([
        fetch(`${API}/active_top_new?role_family=SDE&days=${days}&limit=50`),
        fetch(`${API}/companies`),
      ])
      if (!sRes.ok) throw new Error(`GET /active_top_new ${sRes.status}`)
      if (!cRes.ok) throw new Error(`GET /companies ${cRes.status}`)
      const [scoresJson, companiesJson] = await Promise.all([sRes.json(), cRes.json()])
      const cmap: Record<number, Company> = {}
      ;(companiesJson as Company[]).forEach((c) => (cmap[c.id] = c))
      setCompanies(cmap)
      setScores((scoresJson as Score[]).sort((a, b) => b.score - a.score))
    } catch (e: any) {
      setError(e?.message || 'Failed to load data')
    } finally { setBusy(false); setLoading(false) }
  }

  useEffect(() => { fetchTop() }, [])

  const filtered = scores.filter((s) => {
    const meetsScore = s.score >= minScore
    const name = companies[s.company_id]?.name?.toLowerCase() || ''
    const meetsQuery = debouncedQuery ? name.includes(debouncedQuery) : true
    return meetsScore && meetsQuery
  })

  function openEvidence(companyId: number) {
    const name = companies[companyId]?.name || String(companyId)
    const row = scores.find((r) => r.company_id === companyId)
    setEvidenceFor(name)
    setEvidenceLinks(row?.evidence_urls || [])
    setEvidenceOpen(true)
  }

  async function openCompanyPanel(companyId: number, tf: TimeFilter = 'all') {
    setPanelCompanyId(companyId)
    setPanelTimeFilter(tf)
    setCompanyPanelOpen(true)
    await fetchCompanyPostings(companyId, tf)
  }

  async function fetchCompanyPostings(companyId: number, tf: TimeFilter) {
    setPanelLoading(true)
    try {
      const url = new URL(`${API}/companies/${companyId}/postings`)
      url.searchParams.set('role_family', 'SDE')
      switch (tf) {
        case '24h': url.searchParams.set('since_hours', '24'); break
        case '48h': url.searchParams.set('since_hours', '48'); break
        case '72h': url.searchParams.set('since_hours', '72'); break
        case '7d': url.searchParams.set('since_days', '7'); break
        case '14d': url.searchParams.set('since_days', '14'); break
        case '30d': url.searchParams.set('since_days', '30'); break
        case 'all':
        default: break
      }
      const r = await fetch(url.toString())
      if (!r.ok) throw new Error(`GET /companies/${companyId}/postings ${r.status}`)
      const data = (await r.json()) as Posting[]
      setPanelPostings(data)
    } catch (e) {
      setPanelPostings([])
    } finally { setPanelLoading(false) }
  }

  // --- Presentational helpers ---
  const badge = (txt: string, tone: 'zinc' | 'emerald' | 'amber' | 'blue' = 'zinc') => {
    const map: Record<string,string> = {
      zinc: 'bg-zinc-100 text-zinc-700',
      emerald: 'bg-emerald-100 text-emerald-700',
      amber: 'bg-amber-100 text-amber-800',
      blue: 'bg-blue-100 text-blue-700',
    }
    return <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs ${map[tone]} whitespace-nowrap`}>{txt}</span>
  }

  const shimmerRow = (i: number) => (
    <tr key={`skeleton-${i}`} className="animate-pulse">
      <td className="p-3"><div className="h-4 w-40 rounded bg-zinc-200" /></td>
      <td className="p-3"><div className="h-4 w-10 rounded bg-zinc-200" /></td>
      <td className="p-3"><div className="h-4 w-14 rounded bg-zinc-200" /></td>
      <td className="p-3"><div className="h-4 w-14 rounded bg-zinc-200" /></td>
      <td className="p-3"><div className="h-8 w-16 rounded bg-zinc-200" /></td>
    </tr>
  )

  const panelPostingsFiltered = useMemo(() => {
    if (!usOnly) return panelPostings
    return panelPostings.filter(p => isUS(p.location))
  }, [panelPostings, usOnly])

  return (
    <main className="mx-auto max-w-6xl p-6">
      {/* Page header */}
      <div className="mb-6 flex flex-wrap items-center gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">Hiring Radar</h1>
        {mode === 'new'
          ? badge(`New (last ${newDays}d)`, 'amber')
          : badge('Top companies', 'blue')}
        <span className="text-sm text-zinc-500">Live view of companies actively hiring SDEs</span>
      </div>

      {/* Controls card */}
      <div className="mb-6 rounded-2xl border bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-center gap-3">
          {/* Segmented control */}
          <div className="flex rounded-xl border p-1">
            <button
              className={`rounded-lg px-3 py-1.5 text-sm ${mode==='top' ? 'bg-zinc-900 text-white' : 'text-zinc-700 hover:bg-zinc-50'}`}
              onClick={async () => { setMode('top'); await fetchTop() }}
              disabled={busy || mode==='top'}
            >
              Top (overall)
            </button>
            <button
              className={`rounded-lg px-3 py-1.5 text-sm ${mode==='new' ? 'bg-zinc-900 text-white' : 'text-zinc-700 hover:bg-zinc-50'}`}
              onClick={async () => { setMode('new'); await fetchTopNew(newDays) }}
              disabled={busy || mode==='new'}
            >
              New
            </button>
          </div>

          {/* New window picker */}
          {mode === 'new' && (
            <select
              className="rounded-lg border px-2 py-1.5 text-sm"
              value={newDays}
              onChange={async (e) => {
                const d = Number(e.target.value) as 7|14|30
                setNewDays(d)
                await fetchTopNew(d)
              }}
            >
              <option value={7}>Last 7 days</option>
              <option value={14}>Last 14 days</option>
              <option value={30}>Last 30 days</option>
            </select>
          )}

          {/* Actions */}
          <div className="mx-2 h-6 w-px bg-zinc-200" />
          <button
            className="rounded-lg bg-black px-3 py-2 text-sm text-white disabled:opacity-60"
            disabled={busy}
            onClick={async () => {
              setBusy(true); setError(null)
              try {
                const r = await fetch(`${API}/tasks/ingest`, { method: 'POST' })
                if (!r.ok) throw new Error(`POST /tasks/ingest ${r.status}`)
                mode === 'new' ? await fetchTopNew(newDays) : await fetchTop()
              } catch (e: any) { setError(e?.message || 'Ingest failed') }
              finally { setBusy(false) }
            }}
          >
            {busy ? 'Running…' : 'Run Ingest'}
          </button>
          <button
            className="rounded-lg bg-zinc-900 px-3 py-2 text-sm text-white disabled:opacity-60"
            disabled={busy}
            onClick={async () => {
              setBusy(true); setError(null)
              try {
                const r = await fetch(`${API}/tasks/forecast`, { method: 'POST' })
                if (!r.ok) throw new Error(`POST /tasks/forecast ${r.status}`)
                mode === 'new' ? await fetchTopNew(newDays) : await fetchTop()
              } catch (e: any) { setError(e?.message || 'Forecast failed') }
              finally { setBusy(false) }
            }}
          >
            {busy ? 'Computing…' : 'Run Forecast'}
          </button>
          <button
            className="rounded-lg border px-3 py-2 text-sm disabled:opacity-60"
            disabled={busy}
            onClick={async () => mode==='new' ? await fetchTopNew(newDays) : await fetchTop()}
          >
            Refresh
          </button>

          {/* Right-aligned filters */}
          <div className="ml-auto flex items-center gap-3">
            <div className="relative">
              <input
                type="text"
                value={companyQuery}
                onChange={(e) => setCompanyQuery(e.target.value)}
                placeholder="Search company…"
                className="w-56 rounded-lg border px-3 py-2 text-sm pr-8"
              />
              <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 text-zinc-400">⌘K</span>
            </div>
            <div className="flex items-center gap-2">
              <label className="text-sm text-zinc-600" htmlFor="minScore">Min score</label>
              <input
                id="minScore"
                type="number"
                value={minScore}
                min={0}
                max={100}
                onChange={(e) => setMinScore(Number(e.target.value))}
                className="w-24 rounded-lg border px-2 py-2 text-sm"
              />
            </div>
          </div>
        </div>

        {/* Inline error alert */}
        {error && (
          <div className="mt-3 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
            <div className="flex items-center justify-between">
              <span>⚠️ {error}</span>
              <button className="rounded px-2 py-1 hover:bg-red-100" onClick={() => setError(null)}>Dismiss</button>
            </div>
          </div>
        )}
      </div>

      {/* Results card */}
      <div className="overflow-hidden rounded-2xl border bg-white shadow-sm">
        <div className="max-h-[70vh] overflow-auto">
          <table className="w-full table-fixed">
            <thead className="sticky top-0 z-10 bg-white/95 backdrop-blur text-left text-xs font-medium text-zinc-600">
              <tr className="border-b">
                <th className="p-3">Company</th>
                <th className="w-24 p-3">Score</th>
                <th className="w-32 p-3">Open SDE</th>
                <th className="w-32 p-3">New (28d)</th>
                <th className="w-28 p-3">Evidence</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {loading
                ? Array.from({ length: 10 }).map((_, i) => shimmerRow(i))
                : filtered.length > 0
                  ? filtered.map((s) => (
                      <tr key={`${s.company_id}-${s.role_family}`} className="hover:bg-zinc-50">
                        <td className="p-3">
                          <div className="flex items-center gap-2">
                            <button
                              className="truncate text-sm font-medium text-blue-600 hover:underline"
                              onClick={() => openCompanyPanel(s.company_id, 'all')}
                              title="View openings"
                            >
                              {companies[s.company_id]?.name || s.company_id}
                            </button>
                            {s.score >= 20 && badge('Active', 'emerald')}
                          </div>
                          <div className="mt-0.5 text-xs text-zinc-500">
                            {s.computed_at ? new Date(s.computed_at).toLocaleString() : ''}
                          </div>
                        </td>
                        <td className="p-3 align-top">{s.score}</td>
                        <td className="p-3 align-top">{s.details_json?.open_now ?? s.open_count ?? '—'}</td>
                        <td className="p-3 align-top">{s.details_json?.new_last_4w ?? '—'}</td>
                        <td className="p-3 align-top">
                          {(s.evidence_urls?.length ?? 0) > 0 ? (
                            <button
                              className="rounded-lg border px-2 py-1 text-xs hover:bg-zinc-50"
                              onClick={() => openEvidence(s.company_id)}
                            >
                              View
                            </button>
                          ) : (
                            <span className="text-zinc-400 text-xs">—</span>
                          )}
                        </td>
                      </tr>
                    ))
                  : (
                    <tr>
                      <td className="p-6 text-center text-zinc-500" colSpan={5}>
                        No companies match your filters.
                      </td>
                    </tr>
                  )
              }
            </tbody>
          </table>
        </div>
      </div>

      {/* Evidence modal */}
      {evidenceOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="w-full max-w-lg rounded-2xl bg-white shadow-xl">
            <div className="flex items-center justify-between border-b p-4">
              <h3 className="text-base font-semibold">Evidence — {evidenceFor}</h3>
              <button className="rounded px-2 py-1 text-sm hover:bg-zinc-100" onClick={() => setEvidenceOpen(false)}>
                Close
              </button>
            </div>
            <div className="p-4">
              {evidenceLinks.length > 0 ? (
                <ul className="list-disc space-y-2 pl-5">
                  {evidenceLinks.map((u, i) => (
                    <li key={i} className="break-all text-sm">
                      <a href={u} target="_blank" rel="noreferrer" className="text-blue-600 hover:underline">
                        {u}
                      </a>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-zinc-500">No recent apply links available.</p>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Company slide-over */}
      {companyPanelOpen && (
        <div className="fixed inset-0 z-40">
          <div className="absolute inset-0 bg-black/40" onClick={() => setCompanyPanelOpen(false)} />
          <div className="absolute right-0 top-0 flex h-full w-full max-w-2xl flex-col bg-white shadow-2xl">
            {/* Header */}
            <div className="flex items-center justify-between border-b p-4">
              <div>
                <h3 className="text-base font-semibold">{panelCompanyName}</h3>
                <p className="text-xs text-zinc-500">Open SDE roles</p>
              </div>
              <button className="rounded px-2 py-1 text-sm hover:bg-zinc-100" onClick={() => setCompanyPanelOpen(false)}>
                Close
              </button>
            </div>

            {/* Filters */}
            <div className="flex items-center gap-2 border-b p-3">
              <span className="text-xs text-zinc-600">Posted/updated:</span>
              {(['all','24h','48h','72h','7d','14d','30d'] as TimeFilter[]).map((tf) => (
                <button
                  key={tf}
                  className={`rounded-lg px-2 py-1 text-xs border ${
                    panelTimeFilter === tf ? 'bg-zinc-900 text-white' : 'bg-white hover:bg-zinc-50'
                  }`}
                  onClick={async () => {
                    if (panelCompanyId == null) return
                    setPanelTimeFilter(tf)
                    await fetchCompanyPostings(panelCompanyId, tf)
                  }}
                >
                  {tf.toUpperCase()}
                </button>
              ))}
              <label className="ml-auto flex items-center gap-2 text-xs">
                <input type="checkbox" checked={usOnly} onChange={(e) => setUsOnly(e.target.checked)} />
                US only
              </label>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-y-auto p-4">
              {panelLoading ? (
                <div className="space-y-3">
                  {Array.from({ length: 6 }).map((_, i) => (
                    <div key={i} className="animate-pulse rounded-xl border p-3">
                      <div className="h-4 w-48 rounded bg-zinc-200" />
                      <div className="mt-2 h-3 w-72 rounded bg-zinc-200" />
                    </div>
                  ))}
                </div>
              ) : panelPostingsFiltered.length === 0 ? (
                <p className="text-sm text-zinc-500">No postings match this filter.</p>
              ) : (
                <ul className="space-y-3">
                  {panelPostingsFiltered.map((p) => (
                    <li key={p.id} className="rounded-xl border p-3 hover:bg-zinc-50">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0">
                          <a
                            href={p.apply_url || '#'}
                            target="_blank"
                            rel="noreferrer"
                            className={`line-clamp-2 font-medium ${p.apply_url ? 'text-blue-600 hover:underline' : 'text-zinc-700'}`}
                          >
                            {p.title || 'Untitled role'}
                          </a>
                          <div className="mt-1 text-xs text-zinc-600">
                            {p.department ? `${p.department} · ` : ''}{p.location || 'Location n/a'}
                          </div>
                        </div>
                        <div className="shrink-0 text-right text-[11px] text-zinc-500 whitespace-nowrap">
                          {p.updated_at
                            ? new Date(p.updated_at).toLocaleString()
                            : (p.created_at ? new Date(p.created_at).toLocaleString() : '')}
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  )
}
