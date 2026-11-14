import { useEffect, useMemo, useState } from 'react'
import {
  Activity, ArrowDown, ArrowDownUp, Clock3, Database, GitBranch,
  Search, ShieldCheck, UsersRound,
} from 'lucide-react'
import {
  Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'
import { Badge } from '../components/ui/badge'
import { Button } from '../components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { ApiError } from '../services/apiClient'
import { dashboardApi, type DashboardData, type PipelineActivity } from '../services/dashboardApi'

type Range = 7 | 30 | 90
type SortKey = 'run_id' | 'entities' | 'source_records' | 'master_records' | 'matches' | 'accepted_rate' | 'status' | 'started_at'

const PAGE_SIZE = 5
const DECISION_LABELS = [
  { key: 'EXACT', label: 'Exact duplicate pairs', color: '#16a37a' },
  { key: 'STRONG_PROBABLE_MATCH', label: 'Strong probable matches', color: '#3978d4' },
  { key: 'REVIEW_REQUIRED', label: 'Review required', color: '#d9942e' },
]

export function DashboardPage({ searchQuery, onSearchChange }: {
  searchQuery: string
  onSearchChange: (value: string) => void
}) {
  const [range, setRange] = useState<Range>(30)
  const [data, setData] = useState<DashboardData | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [entity, setEntity] = useState<'all' | 'customer' | 'product'>('all')
  const [sortKey, setSortKey] = useState<SortKey>('started_at')
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')
  const [page, setPage] = useState(1)
  const [reloadId, setReloadId] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    setError(null)
    dashboardApi.getDashboardData(range, controller.signal)
      .then((response) => setData(response))
      .catch((cause: unknown) => {
        if (cause instanceof DOMException && cause.name === 'AbortError') return
        setError(cause instanceof ApiError ? cause.message : 'Dashboard data could not be loaded.')
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [range, reloadId])

  const filteredRuns = useMemo(() => {
    if (!data) return []
    const normalizedQuery = searchQuery.trim().toLocaleLowerCase()
    return [...data.runs]
      .filter((run) => entity === 'all' || run.entities.includes(entity))
      .filter((run) => !normalizedQuery || `${run.run_id} ${run.status} ${run.entities.join(' ')}`.toLocaleLowerCase().includes(normalizedQuery))
      .sort((left, right) => {
        const value = (run: PipelineActivity): string | number => {
          if (sortKey === 'matches') return run.exact_duplicate_pairs + (run.probable_match_decisions.STRONG_PROBABLE_MATCH ?? 0)
          if (sortKey === 'accepted_rate') return run.source_records ? run.accepted_records / run.source_records : -1
          if (sortKey === 'entities') return run.entities.join(', ')
          return run[sortKey as keyof PipelineActivity] as string | number
        }
        const first = value(left)
        const second = value(right)
        const result = typeof first === 'number' && typeof second === 'number'
          ? first - second
          : String(first).localeCompare(String(second))
        return sortDirection === 'asc' ? result : -result
      })
  }, [data, entity, searchQuery, sortDirection, sortKey])

  const pageCount = Math.max(1, Math.ceil(filteredRuns.length / PAGE_SIZE))
  const pageRuns = filteredRuns.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)
  const kpis = data?.kpis
  const connectionLabel = error ? 'API unavailable' : loading ? 'Connecting...' : data ? 'Live database' : 'API unavailable'
  const connectionTone = error ? 'amber' : loading ? 'slate' : 'blue'
  const qualityRows = kpis ? [
    ...DECISION_LABELS.map((item) => ({ ...item, count: item.key === 'EXACT' ? kpis.exact_duplicate_pairs : kpis.probable_match_decisions[item.key] ?? 0 })),
    { key: 'REJECTED', label: 'Validation rejected', color: '#d65d54', count: kpis.rejected_records },
  ] : []
  const chartData = data?.activity.map((run) => ({
    label: run.run_id,
    runId: run.run_id,
    startedAt: run.started_at,
    processed: run.source_records,
    mastered: run.master_records,
  })) ?? []

  function sortBy(key: SortKey) {
    if (sortKey === key) setSortDirection((current) => current === 'asc' ? 'desc' : 'asc')
    else {
      setSortKey(key)
      setSortDirection('desc')
    }
    setPage(1)
  }

  function updateSearch(value: string) {
    onSearchChange(value)
    setPage(1)
  }

  return (
    <main className="page-content">
      <div className="page-heading">
        <div>
          <div className="eyebrow"><span className="live-dot" /> WORKSPACE OVERVIEW</div>
          <h1>Data operations overview</h1>
          <p>Persisted pipeline activity and entity-resolution results.</p>
        </div>
        <div className="heading-actions">
          <Badge tone={connectionTone} className="demo-badge" aria-live="polite">
            <span className={data && !error && !loading ? 'connected-dot' : error ? 'connection-dot-error' : 'connection-dot-idle'} />
            {connectionLabel}
          </Badge>
          <label className="date-select"><Clock3 size={15} /><select aria-label="Dashboard date range" value={range} onChange={(event) => { setRange(Number(event.target.value) as Range); setPage(1) }}><option value={7}>Last 7 days</option><option value={30}>Last 30 days</option><option value={90}>Last 90 days</option></select><span className="chevron-mark" /></label>
          <Button variant="outline" className="refresh-button" onClick={() => setReloadId((current) => current + 1)} disabled={loading}><Activity size={15} /> Refresh data</Button>
        </div>
      </div>

      {error && <div className="error-banner" role="alert"><span>{error}</span><Button variant="outline" size="sm" onClick={() => setReloadId((current) => current + 1)}>Retry</Button></div>}
      {loading && !data ? <LoadingState /> : data && <>
        <section className="metrics-grid" aria-label="Database-backed metrics">
          <MetricCard title="Source records" value={formatCount(kpis?.source_records)} icon={Database} iconTone="blue" caption="Persisted across runs in this period" />
          <MetricCard title="Master records" value={formatCount(kpis?.master_records)} icon={UsersRound} iconTone="green" caption="Created by survivorship" />
          <MetricCard title="Accepted rate" value={formatPercent(kpis?.accepted_rate_percent)} icon={ShieldCheck} iconTone="teal" caption={`${formatCount(kpis?.accepted_records)} accepted · ${formatCount(kpis?.rejected_records)} rejected`} />
          <MetricCard title="Confirmed duplicate pairs" value={formatCount(kpis ? kpis.exact_duplicate_pairs + (kpis.probable_match_decisions.STRONG_PROBABLE_MATCH ?? 0) : undefined)} icon={GitBranch} iconTone="amber" caption={`${formatCount(kpis?.exact_duplicate_pairs)} exact · ${formatCount(kpis?.probable_match_decisions.STRONG_PROBABLE_MATCH)} strong probable`} />
        </section>

        <section className="dashboard-grid">
          <Card className="trend-card">
            <CardHeader className="chart-card-header">
              <div><CardTitle>Processing activity</CardTitle><p className="card-subtitle">Source and master records by persisted run</p></div>
              <div className="range-tabs" role="group" aria-label="Chart date range">
                {([7, 30, 90] as const).map((days) => <button key={days} className={range === days ? 'range-tab range-tab-active' : 'range-tab'} onClick={() => { setRange(days); setPage(1) }}>{days}d</button>)}
              </div>
            </CardHeader>
            <CardContent className="chart-content">
              {chartData.length ? <>
                <div className="chart-legend"><span><i className="legend-dot processed-dot" /> Source records</span><span><i className="legend-dot mastered-dot" /> Master records</span><span className="chart-total"><strong>{formatCount(kpis?.source_records)}</strong> persisted in period</span></div>
                <div className="activity-chart">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData} margin={{ top: 12, right: 12, bottom: 0, left: -14 }}>
                      <defs>
                        <linearGradient id="processedFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#4785df" stopOpacity={0.17} /><stop offset="100%" stopColor="#4785df" stopOpacity={0.01} /></linearGradient>
                        <linearGradient id="masteredFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#20a57d" stopOpacity={0.12} /><stop offset="100%" stopColor="#20a57d" stopOpacity={0.01} /></linearGradient>
                      </defs>
                      <CartesianGrid stroke="#edf0f3" vertical={false} />
                      <XAxis dataKey="runId" axisLine={false} tickLine={false} tick={{ fill: '#8a96a5', fontSize: 9 }} tickFormatter={shortRunId} dy={10} />
                      <YAxis axisLine={false} tickLine={false} tick={{ fill: '#8a96a5', fontSize: 10 }} tickFormatter={(value: number) => value.toLocaleString()} />
                      <Tooltip content={<ChartTooltip />} />
                      <Area type="monotone" dataKey="processed" name="Source records" stroke="#4785df" strokeWidth={2.3} fill="url(#processedFill)" activeDot={{ r: 4, strokeWidth: 0 }} isAnimationActive={false} />
                      <Area type="monotone" dataKey="mastered" name="Master records" stroke="#20a57d" strokeWidth={2.3} fill="url(#masteredFill)" activeDot={{ r: 4, strokeWidth: 0 }} isAnimationActive={false} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </> : <EmptyState title="No pipeline activity in this period" message="Completed and in-progress runs will appear here when persisted in the configured database." />}
            </CardContent>
          </Card>

          <Card className="quality-card">
            <CardHeader><div><CardTitle>Data quality overview</CardTitle><p className="card-subtitle">Persisted validation and match decisions</p></div></CardHeader>
            <CardContent className="quality-content">
              <div className="quality-score-line"><div><strong>{formatPercent(kpis?.accepted_rate_percent)}</strong><small>Accepted source records</small></div></div>
              <div className="quality-track" aria-label="Accepted source record rate"><span style={{ width: `${kpis?.accepted_rate_percent ?? 0}%` }} /></div>
              <div className="quality-breakdown">
                {qualityRows.map((item) => <div className="quality-row" key={item.key}><span className="quality-label"><i style={{ backgroundColor: item.color }} />{item.label}</span><strong>{item.count.toLocaleString()}</strong></div>)}
              </div>
              {data.review_data_available
                ? <div className="availability-note">Pending review items: {formatCount(kpis?.pending_reviews ?? undefined)}</div>
                : <div className="availability-note">Pending review totals unavailable: this database has no review-items table.</div>}
              {data.quarantine_data_available
                ? <div className="availability-note">Quarantined records: {formatCount(kpis?.quarantine_records ?? undefined)}</div>
                : <div className="availability-note">Quarantine totals unavailable: this database has no quarantine table.</div>}
            </CardContent>
          </Card>
        </section>

        <Card className="runs-card">
          <CardHeader className="runs-card-header">
            <div><div className="runs-title-row"><CardTitle>Recent pipeline runs</CardTitle><span className="run-count">{data.total_runs.toLocaleString()}</span></div><p className="card-subtitle">Persisted runs started in the selected date range</p></div>
            <div className="table-controls">
              <label className="table-search"><Search size={15} /><input value={searchQuery} onChange={(event) => updateSearch(event.target.value)} placeholder="Filter runs..." aria-label="Filter pipeline runs" />{searchQuery && <button aria-label="Clear search" onClick={() => updateSearch('')}>×</button>}</label>
              <label className="entity-select-wrap"><span className="sr-only">Filter by entity</span><select value={entity} onChange={(event) => { setEntity(event.target.value as typeof entity); setPage(1) }}><option value="all">All entities</option><option value="customer">Customers</option><option value="product">Products</option></select><span className="chevron-mark" /></label>
            </div>
          </CardHeader>
          <CardContent className="table-content">
            <div className="table-scroll">
              <table className="runs-table">
                <thead><tr>
                  <SortableHeader label="Run ID" sortKey="run_id" current={sortKey} direction={sortDirection} onSort={sortBy} />
                  <SortableHeader label="Entity" sortKey="entities" current={sortKey} direction={sortDirection} onSort={sortBy} />
                  <SortableHeader label="Source records" sortKey="source_records" current={sortKey} direction={sortDirection} onSort={sortBy} align="right" />
                  <SortableHeader label="Master records" sortKey="master_records" current={sortKey} direction={sortDirection} onSort={sortBy} align="right" />
                  <SortableHeader label="Matched pairs" sortKey="matches" current={sortKey} direction={sortDirection} onSort={sortBy} align="right" />
                  <SortableHeader label="Accepted" sortKey="accepted_rate" current={sortKey} direction={sortDirection} onSort={sortBy} align="right" />
                  <SortableHeader label="Status" sortKey="status" current={sortKey} direction={sortDirection} onSort={sortBy} />
                  <SortableHeader label="Started" sortKey="started_at" current={sortKey} direction={sortDirection} onSort={sortBy} />
                </tr></thead>
                <tbody>
                  {pageRuns.map((run) => {
                    const matches = run.exact_duplicate_pairs + (run.probable_match_decisions.STRONG_PROBABLE_MATCH ?? 0)
                    const entities = run.entities.map((value) => value === 'customer' ? 'Customers' : 'Products').join(' + ')
                    return <tr key={run.run_id}>
                      <td><span className="run-id">{run.run_id}</span></td>
                      <td><span className="entity-name"><span className="entity-mark" />{entities || 'Unknown'}</span></td>
                      <td className="numeric-cell">{run.source_records.toLocaleString()}</td>
                      <td className="numeric-cell">{run.master_records.toLocaleString()}</td>
                      <td className="numeric-cell">{matches.toLocaleString()}</td>
                      <td className="numeric-cell">{formatPercent(run.source_records ? run.accepted_records * 100 / run.source_records : null)}</td>
                      <td><Badge tone={run.status === 'COMPLETED' ? 'green' : run.status === 'RUNNING' ? 'blue' : 'amber'}><span className="status-indicator" />{run.status}</Badge></td>
                      <td><span className="started-at">{formatDate(run.started_at)}</span></td>
                    </tr>
                  })}
                  {pageRuns.length === 0 && <tr><td className="empty-cell" colSpan={8}><EmptyState title={data.runs.length ? 'No runs match these filters' : 'No pipeline runs found'} message={data.runs.length ? 'Try another search or entity.' : 'Run the existing pipeline to populate this dashboard.'} action={data.runs.length ? <Button variant="ghost" size="sm" onClick={() => { updateSearch(''); setEntity('all') }}>Clear filters</Button> : undefined} /></td></tr>}
                </tbody>
              </table>
            </div>
            <div className="table-footer"><span>Showing <strong>{filteredRuns.length ? (page - 1) * PAGE_SIZE + 1 : 0}–{Math.min(page * PAGE_SIZE, filteredRuns.length)}</strong> of <strong>{filteredRuns.length.toLocaleString()}</strong> loaded runs{data.total_runs > data.runs.length ? ` · ${data.total_runs.toLocaleString()} total` : ''}</span><div className="pagination"><button disabled={page === 1} onClick={() => setPage((current) => current - 1)} aria-label="Previous page">‹</button><span>Page <strong>{page}</strong> of <strong>{pageCount}</strong></span><button disabled={page >= pageCount} onClick={() => setPage((current) => current + 1)} aria-label="Next page">›</button></div></div>
          </CardContent>
        </Card>
        <div className="data-footnote">Counts are persisted artifacts grouped by pipeline run. Repeated inputs in separate runs are counted separately.</div>
      </>}
    </main>
  )
}

function formatCount(value?: number | null) {
  return value == null ? '—' : value.toLocaleString()
}

function formatPercent(value?: number | null) {
  return value == null ? '—' : `${value.toFixed(2)}%`
}

function formatDate(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

function shortRunId(value: string) {
  return value.length > 14 ? `${value.slice(0, 10)}…` : value
}

function MetricCard({ title, value, icon: Icon, iconTone, caption }: {
  title: string; value: string; icon: typeof Database; iconTone: string; caption: string
}) {
  return <Card className="metric-card"><CardContent className="metric-content"><div className="metric-top"><span className="metric-title">{title}</span><span className={`metric-icon metric-icon-${iconTone}`}><Icon size={17} strokeWidth={1.9} /></span></div><div className="metric-value-row"><strong className="metric-value">{value}</strong></div><div className="metric-caption">{caption}</div></CardContent></Card>
}

function SortableHeader({ label, sortKey, current, direction, onSort, align }: {
  label: string; sortKey: SortKey; current: SortKey; direction: 'asc' | 'desc'; onSort: (key: SortKey) => void; align?: 'right'
}) {
  const active = sortKey === current
  return <th className={align === 'right' ? 'th-right' : ''}><button className={`sort-button ${active ? 'sort-button-active' : ''}`} onClick={() => onSort(sortKey)}>{label}{active ? direction === 'asc' ? <ArrowDown size={12} className="sort-ascending" /> : <ArrowDown size={12} /> : <ArrowDownUp size={12} className="sort-muted" />}</button></th>
}

function ChartTooltip({ active, payload }: { active?: boolean; payload?: { color: string; name: string; value: number; payload?: { runId: string; startedAt: string } }[] }) {
  if (!active || !payload?.length) return null
  const point = payload[0]?.payload
  return <div className="chart-tooltip"><strong>{point?.runId}</strong><small>{point?.startedAt ? formatDate(point.startedAt) : ''}</small>{payload.map((item) => <div key={item.name}><i style={{ backgroundColor: item.color }} />{item.name}<b>{item.value.toLocaleString()}</b></div>)}</div>
}

function LoadingState() {
  return <div className="loading-dashboard" aria-live="polite"><div className="loading-line loading-line-wide" /><div className="loading-line" /><div className="loading-panels"><span /><span /><span /><span /></div><p>Loading persisted dashboard data...</p></div>
}

function EmptyState({ title, message, action }: { title: string; message: string; action?: React.ReactNode }) {
  return <div className="empty-state"><Database size={20} /><strong>{title}</strong><span>{message}</span>{action}</div>
}
