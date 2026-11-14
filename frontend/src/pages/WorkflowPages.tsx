import { useEffect, useState, type ReactNode } from 'react'
import {
  AlertCircle, Check, ChevronLeft, ChevronRight,
  Download, Eye, FileJson2, LoaderCircle, RefreshCw, Search, Upload, X,
} from 'lucide-react'
import { Badge } from '../components/ui/badge'
import { Button } from '../components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card'
import { ApiError, apiGet, apiPost, apiUpload, apiUrl } from '../services/apiClient'
import './workflow.css'

const LIMIT = 25

type Entity = 'customer' | 'product'
type PageData<T> = { items: T[]; total: number; limit: number; offset: number }
type RunRow = {
  run_id: string; started_at: string; completed_at: string | null; status: string
  customer_input_count: number; product_input_count: number; accepted_count: number
  rejected_count: number; master_record_count: number; error_count: number; report_available?: boolean
}
type SourceRecord = {
  source_record_id: string; entity_type: Entity; source_system: string | null
  validation_status: string; original_record: Record<string, unknown>; normalized_record: Record<string, unknown> | null
}
type DuplicateItem = {
  pair_id: string; run_id: string; entity_type: Entity; record_id_1: string; record_id_2: string
  decision: string; confidence_score: number | null; matching_evidence: unknown
  conflicting_evidence: unknown; source_records: SourceRecord[]
}
type ReviewItem = {
  review_id: string; run_id: string; entity_type: Entity; record_id_1: string; record_id_2: string
  confidence_score: number | null; matching_evidence: unknown; conflicting_evidence: unknown
  decision_explanation: string; review_status: string; created_at: string; reviewer_notes: string | null
}
type QuarantineItem = {
  quarantine_id: string; run_id: string; entity_type: Entity; source_record_id: string | null
  source_file: string; source_record_number: number; original_record: Record<string, unknown>
  validation_issues: { error_code: string; field_name: string; message: string }[]
  quarantine_status: string; quarantined_at: string
}
type MasterItem = {
  run_id: string; master_id: string; entity_type: Entity; merge_method: string; created_at: string
  merged_fields: Record<string, unknown>; contributing_source_record_ids: string[]
  contributing_source_systems: string[]; field_lineage?: LineageItem[]; source_records?: SourceRecord[]
}
type LineageItem = {
  field_name: string; selected_value: unknown; selected_source_record_id: string | null
  selected_source_system: string | null; selection_rule: string; rejected_values: unknown[]
}
type UploadResult = {
  upload_id: string; entity_type: Entity; filename: string; source_format: string
  total_records: number; accepted_records: number; rejected_records: number
  schema_drift_records: number; error_counts_by_code: Record<string, number>
  issues: { record_id: unknown; field_name: string; error_code: string; message: string }[]
  issues_truncated: boolean
}
type ExecutionResult = { run_id: string; status: string; result: Record<string, unknown>; report_url: string }
type RunDetail = RunRow & Record<string, unknown>

function useRemote<T>(path: string, refreshId = 0) {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(Boolean(path))
  const [error, setError] = useState('')
  const [localRefreshId, setLocalRefreshId] = useState(0)

  useEffect(() => {
    if (!path) {
      setData(null)
      setError('')
      setLoading(false)
      return
    }
    const controller = new AbortController()
    setLoading(true)
    setError('')
    apiGet<T>(path, controller.signal)
      .then(setData)
      .catch((cause: unknown) => {
        if (cause instanceof DOMException && cause.name === 'AbortError') return
        setError(cause instanceof ApiError ? cause.message : 'The request could not be completed.')
      })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [path, refreshId, localRefreshId])

  return { data, loading, error, refresh: () => setLocalRefreshId((current) => current + 1) }
}

function useRuns() {
  const resource = useRemote<PageData<RunRow>>('/api/v1/pipeline-runs?limit=100&offset=0')
  const [runId, setRunId] = useState('')
  useEffect(() => {
    if (!runId && resource.data?.items.length) setRunId(resource.data.items[0].run_id)
  }, [resource.data, runId])
  return { ...resource, runId, setRunId, runs: resource.data?.items ?? [] }
}

function queryString(values: Record<string, string | number | undefined | null>) {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(values)) if (value !== undefined && value !== null && value !== '') query.set(key, String(value))
  return query.toString()
}

function PageHeader({ title, description, children }: { title: string; description: string; children?: ReactNode }) {
  return <div className="workflow-heading"><div><div className="eyebrow">MERGEMIND AI WORKSPACE</div><h1>{title}</h1><p>{description}</p></div><div className="workflow-heading-actions">{children}</div></div>
}

function RunSelector({ runs, runId, onChange, loading }: {
  runs: RunRow[]; runId: string; onChange: (value: string) => void; loading?: boolean
}) {
  return <label className="workflow-select-label">Pipeline run<select value={runId} onChange={(event) => onChange(event.target.value)} disabled={loading || !runs.length}><option value="">Select a run</option>{runs.map((run) => <option key={run.run_id} value={run.run_id}>{run.run_id} · {run.status}</option>)}</select></label>
}

function RemoteState({ loading, error, empty, onRetry, children }: {
  loading: boolean; error: string; empty?: { title: string; message: string }; onRetry?: () => void; children?: ReactNode
}) {
  if (loading) return <div className="workflow-state"><LoaderCircle className="spin" size={22} /><strong>Loading persisted data...</strong></div>
  if (error) return <div className="workflow-error" role="alert"><AlertCircle size={18} /><span>{error}</span>{onRetry && <Button variant="outline" size="sm" onClick={onRetry}>Retry</Button>}</div>
  if (empty) return <div className="workflow-state"><Search size={20} /><strong>{empty.title}</strong><span>{empty.message}</span></div>
  return <>{children}</>
}

function Pager({ total, offset, limit, onChange }: { total: number; offset: number; limit: number; onChange: (value: number) => void }) {
  const page = Math.floor(offset / limit) + 1
  const pageCount = Math.max(1, Math.ceil(total / limit))
  return <div className="workflow-pager"><span>{total ? `${offset + 1}–${Math.min(offset + limit, total)} of ${total}` : '0 results'}</span><div><button disabled={offset <= 0} onClick={() => onChange(Math.max(0, offset - limit))} aria-label="Previous page"><ChevronLeft size={16} /></button><span>Page {page} / {pageCount}</span><button disabled={offset + limit >= total} onClick={() => onChange(offset + limit)} aria-label="Next page"><ChevronRight size={16} /></button></div></div>
}

function DataIngestionPage({ onNavigate }: { onNavigate: (path: string) => void }) {
  const [results, setResults] = useState<Partial<Record<Entity, UploadResult>>>({})
  const [busyEntity, setBusyEntity] = useState<Entity | ''>('')
  const [error, setError] = useState('')
  const [runId, setRunId] = useState(() => new URLSearchParams(window.location.search).get('run_id') ?? '')
  const [replaceExisting, setReplaceExisting] = useState(false)
  const [executing, setExecuting] = useState(false)
  const [execution, setExecution] = useState<ExecutionResult | null>(null)

  async function uploadFile(entity: Entity, file?: File) {
    if (!file) return
    setError('')
    setExecution(null)
    const extensionOkay = /\.(csv|json|jsonl|ndjson)$/i.test(file.name)
    if (!extensionOkay) { setError('Choose a CSV, JSON array, or JSON Lines file.'); return }
    if (file.size > 25 * 1024 * 1024) { setError('File exceeds the 25 MB upload limit.'); return }
    setBusyEntity(entity)
    try {
      const validation = await apiUpload<UploadResult>('/api/v1/ingestion/uploads', entity, file)
      setResults((current) => ({ ...current, [entity]: validation }))
    } catch (cause) {
      setError(cause instanceof ApiError ? cause.message : 'File validation failed.')
    } finally { setBusyEntity('') }
  }

  async function execute() {
    if (executing || (!results.customer && !results.product)) return
    setExecuting(true)
    setError('')
    setExecution(null)
    try {
      const response = await apiPost<ExecutionResult>('/api/v1/pipeline-runs', {
        customer_upload_id: results.customer?.upload_id ?? null,
        product_upload_id: results.product?.upload_id ?? null,
        run_id: runId.trim() || null,
        replace_existing: replaceExisting,
      }, { 'Idempotency-Key': crypto.randomUUID() })
      setExecution(response)
    } catch (cause) {
      setError(cause instanceof ApiError ? cause.message : 'Pipeline execution failed.')
    } finally { setExecuting(false) }
  }

  return <main className="page-content workflow-page">
    <PageHeader title="Data ingestion" description="Validate source files, then execute the existing MergeMind pipeline.">
      <Badge tone="blue"><span className="connected-dot" /> Backend-connected</Badge>
    </PageHeader>
    {error && <div className="workflow-error" role="alert"><AlertCircle size={18} /><span>{error}</span></div>}
    <div className="upload-grid">
      {(['customer', 'product'] as const).map((entity) => {
        const result = results[entity]
        return <Card className="workflow-card" key={entity}>
          <CardHeader><div><CardTitle>{entity === 'customer' ? 'Customer data' : 'Product data'}</CardTitle><p className="card-subtitle">{entity === 'customer' ? 'CSV, JSON, or JSON Lines' : 'CSV, JSON, or JSON Lines'}</p></div><span className="workflow-icon"><Upload size={18} /></span></CardHeader>
          <CardContent>
            <label className="file-drop"><input type="file" accept=".csv,.json,.jsonl,.ndjson" onChange={(event) => void uploadFile(entity, event.target.files?.[0])} disabled={Boolean(busyEntity) || executing} /><Upload size={20} /><strong>{busyEntity === entity ? 'Uploading and validating...' : result?.filename ?? `Choose ${entity} file`}</strong><span>Maximum 25 MB · 100,000 records</span></label>
            {result && <div className="validation-summary"><div className="summary-stats"><span><strong>{result.total_records.toLocaleString()}</strong>Total</span><span><strong>{result.accepted_records.toLocaleString()}</strong>Valid</span><span className={result.rejected_records ? 'summary-warn' : ''}><strong>{result.rejected_records.toLocaleString()}</strong>Rejected</span></div>
              {result.schema_drift_records > 0 && <p className="schema-note">Schema drift reported on {result.schema_drift_records} rows; permissive validation retained those fields.</p>}
              {result.issues.length > 0 && <details className="issue-list"><summary>Validation issues ({Object.values(result.error_counts_by_code).reduce((sum, count) => sum + count, 0)})</summary><ul>{result.issues.slice(0, 12).map((issue, index) => <li key={`${issue.record_id}-${issue.field_name}-${index}`}><strong>{issue.error_code}</strong> · {String(issue.record_id ?? 'Row')} · {issue.message}</li>)}</ul>{result.issues_truncated && <small>Only the first 1,000 issues are returned by the API.</small>}</details>}
            </div>}
          </CardContent>
        </Card>
      })}
    </div>
    <Card className="workflow-card execute-card"><CardHeader><div><CardTitle>Pipeline execution</CardTitle><p className="card-subtitle">Uses the configured backend database and existing pipeline stages.</p></div></CardHeader><CardContent>
      <div className="execute-fields"><label>Run ID (optional)<input value={runId} onChange={(event) => setRunId(event.target.value)} maxLength={128} placeholder="Generated when empty" /></label><label className="checkbox-field"><input type="checkbox" checked={replaceExisting} onChange={(event) => setReplaceExisting(event.target.checked)} />Replace only this run ID</label><Button onClick={() => void execute()} disabled={executing || Boolean(busyEntity) || (!results.customer && !results.product)}>{executing ? <><LoaderCircle className="spin" size={15} /> Running pipeline...</> : <><RefreshCw size={15} /> Execute pipeline</>}</Button></div>
      {replaceExisting && <p className="warning-note">Replacement deletes artifacts for the specified run ID only. Other runs remain untouched.</p>}
      {execution && <div className="execution-result" role="status"><div><Badge tone={execution.status === 'COMPLETED' ? 'green' : 'amber'}>{execution.status}</Badge><strong>Run {execution.run_id}</strong></div><div className="summary-stats"><span><strong>{numberAt(execution.result, 'customer_metrics.records_ingested', 0).toLocaleString()}</strong>Customers</span><span><strong>{numberAt(execution.result, 'product_metrics.records_ingested', 0).toLocaleString()}</strong>Products</span><span><strong>{numberAt(execution.result, 'rejected_record_count', 0).toLocaleString()}</strong>Rejected</span><span><strong>{numberAt(execution.result, 'database_persistence_counts.master_record_rows', 0).toLocaleString()}</strong>Masters</span></div><a className="workflow-link" href={apiUrl(execution.report_url)} target="_blank" rel="noreferrer"><FileJson2 size={15} />Open execution report</a><Button variant="outline" size="sm" onClick={() => onNavigate('/')}>View dashboard</Button></div>}
    </CardContent></Card>
  </main>
}

function numberAt(value: Record<string, unknown>, path: string, fallback: number): number {
  const result = path.split('.').reduce<unknown>((current, key) => current && typeof current === 'object' ? (current as Record<string, unknown>)[key] : undefined, value)
  return typeof result === 'number' ? result : fallback
}

function DuplicateExplorerPage({ searchQuery }: { searchQuery: string }) {
  const runs = useRuns()
  const [entity, setEntity] = useState('')
  const [decision, setDecision] = useState('')
  const [offset, setOffset] = useState(0)
  const [selected, setSelected] = useState<DuplicateItem | null>(null)
  const query = queryString({ entity_type: entity, decision, search: searchQuery, limit: LIMIT, offset })
  const path = runs.runId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/duplicate-decisions?${query}` : ''
  const data = useRemote<PageData<DuplicateItem>>(path)
  useEffect(() => { setOffset(0) }, [entity, decision, searchQuery, runs.runId])
  return <main className="page-content workflow-page">
    <PageHeader title="Duplicate explorer" description="Inspect persisted exact and probable pair decisions, evidence, and source records."><RunSelector runs={runs.runs} runId={runs.runId} onChange={(value) => { runs.setRunId(value); setSelected(null) }} loading={runs.loading} /></PageHeader>
    <div className="workflow-toolbar"><label>Entity<select value={entity} onChange={(event) => setEntity(event.target.value)}><option value="">All entities</option><option value="customer">Customers</option><option value="product">Products</option></select></label><label>Decision<select value={decision} onChange={(event) => setDecision(event.target.value)}><option value="">All decisions</option><option value="EXACT">Exact</option><option value="IDENTITY_CONFLICT">Identity conflict</option><option value="STRONG_PROBABLE_MATCH">Strong probable</option><option value="REVIEW_REQUIRED">Review required</option><option value="NO_MATCH">No match</option></select></label><span className="toolbar-note">Search uses the global search field.</span></div>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Pair decisions</CardTitle><p className="card-subtitle">Confidence and evidence come directly from the persisted match decision.</p></div><span className="run-count">{data.data?.total ?? 0}</span></CardHeader><CardContent className="table-content"><RemoteState loading={runs.loading || data.loading} error={runs.error || data.error} empty={runs.runId && data.data && !data.data.items.length ? { title: 'No duplicate decisions found', message: 'Choose another run or adjust the entity, decision, or search filters.' } : undefined} onRetry={data.refresh}><div className="table-scroll"><table className="workflow-table"><thead><tr><th>Entity</th><th>Source pair</th><th>Decision</th><th>Confidence</th><th>Evidence</th><th /></tr></thead><tbody>{data.data?.items.map((item) => <tr key={item.pair_id}><td>{entityName(item.entity_type)}</td><td className="mono-cell">{item.record_id_1}<br />{item.record_id_2}</td><td><Badge tone={decisionTone(item.decision)}>{item.decision}</Badge></td><td>{formatConfidence(item.confidence_score)}</td><td>{evidenceSummary(item.matching_evidence)}</td><td><Button variant="outline" size="sm" onClick={() => setSelected(item)}><Eye size={14} />Compare</Button></td></tr>)}</tbody></table></div><Pager total={data.data?.total ?? 0} offset={offset} limit={LIMIT} onChange={setOffset} /></RemoteState></CardContent></Card>
    {selected && <ComparisonPanel item={selected} onClose={() => setSelected(null)} />}
  </main>
}

function ComparisonPanel({ item, onClose }: { item: DuplicateItem; onClose: () => void }) {
  const sources = item.source_records ?? []
  const records = sources.map((source) => source.original_record)
  const fields = [...new Set(records.flatMap((record) => Object.keys(record)))].sort()
  return <div className="detail-backdrop" role="presentation" onClick={(event) => { if (event.target === event.currentTarget) onClose() }}><section className="detail-drawer" role="dialog" aria-modal="true" aria-label="Duplicate comparison"><div className="drawer-header"><div><div className="eyebrow">{item.entity_type.toUpperCase()} PAIR</div><h2>{item.record_id_1} <span>vs</span> {item.record_id_2}</h2></div><Button variant="ghost" size="icon" aria-label="Close comparison" onClick={onClose}><X size={18} /></Button></div><div className="drawer-body"><div className="detail-pills"><Badge tone={decisionTone(item.decision)}>{item.decision}</Badge><span>Confidence {formatConfidence(item.confidence_score)}</span></div><h3>Source record comparison</h3><div className="comparison-scroll"><table className="comparison-table"><thead><tr><th>Field</th>{sources.map((source) => <th key={source.source_record_id}>{source.source_record_id}<small>{source.source_system}</small></th>)}</tr></thead><tbody>{fields.map((field) => <tr key={field}><th>{field}</th>{records.map((record, index) => <td key={`${field}-${index}`}>{displayValue(record[field])}</td>)}</tr>)}</tbody></table></div><h3>Matching evidence</h3><JsonBlock value={item.matching_evidence} /><h3>Conflicting evidence</h3><JsonBlock value={item.conflicting_evidence} /></div></section></div>
}

function ReviewQueuePage({ searchQuery }: { searchQuery: string }) {
  const runs = useRuns()
  const [entity, setEntity] = useState('')
  const [status, setStatus] = useState('PENDING')
  const [offset, setOffset] = useState(0)
  const [refreshId, setRefreshId] = useState(0)
  const [selectedId, setSelectedId] = useState('')
  const [actionError, setActionError] = useState('')
  const [reviewerId, setReviewerId] = useState('')
  const [reviewerNotes, setReviewerNotes] = useState('')
  const query = queryString({ entity_type: entity, status: status === 'ALL' ? '' : status, limit: LIMIT, offset })
  const basePath = runs.runId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/reviews` : ''
  const data = useRemote<PageData<ReviewItem>>(basePath ? `${basePath}?${query}` : '', refreshId)
  const detail = useRemote<ReviewItem & { history: Record<string, unknown>[]; source_records: SourceRecord[] }>(selectedId ? `${basePath}/${encodeURIComponent(selectedId)}` : '', refreshId)
  const filtered = (data.data?.items ?? []).filter((item) => !searchQuery || `${item.review_id} ${item.record_id_1} ${item.record_id_2} ${item.decision_explanation}`.toLowerCase().includes(searchQuery.toLowerCase()))
  useEffect(() => { setOffset(0) }, [entity, status, runs.runId, searchQuery])

  async function decide(item: ReviewItem, decision: string) {
    if ((decision === 'ACCEPTED_MATCH' || decision === 'REJECTED_MATCH') && !window.confirm(`Confirm ${decision === 'ACCEPTED_MATCH' ? 'accepting' : 'rejecting'} this proposed match? This decision does not merge master records.`)) return
    setActionError('')
    try {
      await apiPost(`${basePath}/${encodeURIComponent(item.review_id)}/decision`, {
        decision,
        reviewer_id: reviewerId.trim() || null,
        notes: reviewerNotes.trim() || null,
      })
      setRefreshId((value) => value + 1)
    } catch (cause) { setActionError(cause instanceof ApiError ? cause.message : 'Review action failed.') }
  }

  return <main className="page-content workflow-page"><PageHeader title="Review queue" description="Make auditable decisions on uncertain pairs. Accepting a pair does not merge records."><RunSelector runs={runs.runs} runId={runs.runId} onChange={(value) => { runs.setRunId(value); setSelectedId('') }} loading={runs.loading} /></PageHeader>
    {actionError && <div className="workflow-error" role="alert"><AlertCircle size={17} />{actionError}</div>}
    <div className="workflow-toolbar"><label>Entity<select value={entity} onChange={(event) => setEntity(event.target.value)}><option value="">All entities</option><option value="customer">Customers</option><option value="product">Products</option></select></label><label>Status<select value={status} onChange={(event) => setStatus(event.target.value)}><option value="PENDING">Pending</option><option value="DEFERRED">Deferred</option><option value="ACCEPTED_MATCH">Accepted</option><option value="REJECTED_MATCH">Rejected</option><option value="ALL">All statuses</option></select></label><label>Reviewer (optional)<input value={reviewerId} onChange={(event) => setReviewerId(event.target.value)} maxLength={120} /></label><label>Decision notes (optional)<input value={reviewerNotes} onChange={(event) => setReviewerNotes(event.target.value)} maxLength={2000} /></label><Button variant="outline" size="sm" onClick={() => setRefreshId((value) => value + 1)}><RefreshCw size={14} />Refresh</Button></div>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Review items</CardTitle><p className="card-subtitle">Decision and evidence are recorded in review history.</p></div><span className="run-count">{filtered.length}</span></CardHeader><CardContent className="table-content"><RemoteState loading={runs.loading || data.loading} error={runs.error || data.error} empty={runs.runId && data.data && !filtered.length ? { title: 'No review items match', message: 'There are no items for this run and filter selection.' } : undefined} onRetry={data.refresh}><div className="table-scroll"><table className="workflow-table"><thead><tr><th>Entity</th><th>Pair</th><th>Confidence</th><th>Explanation</th><th>Status</th><th>Actions</th></tr></thead><tbody>{filtered.map((item) => <tr key={item.review_id}><td>{entityName(item.entity_type)}</td><td className="mono-cell">{item.record_id_1}<br />{item.record_id_2}</td><td>{formatConfidence(item.confidence_score)}</td><td className="explanation-cell">{item.decision_explanation}</td><td><Badge tone={statusTone(item.review_status)}>{item.review_status}</Badge></td><td className="action-cell"><Button variant="ghost" size="sm" onClick={() => setSelectedId(item.review_id)}>Inspect</Button>{(item.review_status === 'PENDING' || item.review_status === 'DEFERRED') && <><Button variant="outline" size="sm" onClick={() => void decide(item, 'ACCEPTED_MATCH')}><Check size={13} />Accept</Button><Button variant="outline" size="sm" onClick={() => void decide(item, 'REJECTED_MATCH')}><X size={13} />Reject</Button>{item.review_status === 'PENDING' && <Button variant="ghost" size="sm" onClick={() => void decide(item, 'DEFERRED')}>Defer</Button>}</>}</td></tr>)}</tbody></table></div><Pager total={data.data?.total ?? 0} offset={offset} limit={LIMIT} onChange={setOffset} /></RemoteState></CardContent></Card>
    {selectedId && <DetailDialog title={`Review ${selectedId}`} loading={detail.loading} error={detail.error} data={detail.data} onClose={() => setSelectedId('')}><h3>Decision history</h3><HistoryList items={detail.data?.history ?? []} timeKey="changed_at" labelKey="new_status" /><h3>Pair source records</h3><SourceCards records={detail.data?.source_records ?? []} /></DetailDialog>}
  </main>
}

function QuarantinePage({ searchQuery }: { searchQuery: string }) {
  const runs = useRuns()
  const [entity, setEntity] = useState('')
  const [status, setStatus] = useState('QUARANTINED')
  const [errorCode, setErrorCode] = useState('')
  const [offset, setOffset] = useState(0)
  const [refreshId, setRefreshId] = useState(0)
  const [selectedId, setSelectedId] = useState('')
  const [actionError, setActionError] = useState('')
  const [resolutionNotes, setResolutionNotes] = useState('')
  const query = queryString({ entity_type: entity, status: status === 'ALL' ? '' : status, error_code: errorCode, limit: LIMIT, offset })
  const base = runs.runId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/quarantine` : ''
  const data = useRemote<PageData<QuarantineItem>>(base ? `${base}?${query}` : '', refreshId)
  const detail = useRemote<QuarantineItem & { history: Record<string, unknown>[] }>(selectedId ? `${base}/${encodeURIComponent(selectedId)}` : '', refreshId)
  const filtered = (data.data?.items ?? []).filter((item) => !searchQuery || JSON.stringify(item).toLowerCase().includes(searchQuery.toLowerCase()))
  useEffect(() => { setOffset(0) }, [entity, status, errorCode, runs.runId, searchQuery])

  async function resolve(item: QuarantineItem, action: 'RESOLVE' | 'DISCARD') {
    const message = action === 'DISCARD' ? 'Discard this quarantined row? The source record will remain stored.' : 'Mark this record resolved for external follow-up? It will not be reprocessed.'
    if (!window.confirm(message)) return
    setActionError('')
    try {
      await apiPost(`${base}/${encodeURIComponent(item.quarantine_id)}/resolution`, {
        action,
        resolution_action: action === 'RESOLVE' ? 'MANUAL_FOLLOW_UP' : null,
        notes: resolutionNotes.trim() || null,
      })
      setRefreshId((value) => value + 1)
    } catch (cause) { setActionError(cause instanceof ApiError ? cause.message : 'Quarantine action failed.') }
  }

  return <main className="page-content workflow-page"><PageHeader title="Quarantine" description="Inspect validation-rejected source rows and record an auditable disposition."><RunSelector runs={runs.runs} runId={runs.runId} onChange={(value) => { runs.setRunId(value); setSelectedId('') }} loading={runs.loading} /></PageHeader>
    {actionError && <div className="workflow-error" role="alert"><AlertCircle size={17} />{actionError}</div>}
    <div className="workflow-toolbar"><label>Entity<select value={entity} onChange={(event) => setEntity(event.target.value)}><option value="">All entities</option><option value="customer">Customers</option><option value="product">Products</option></select></label><label>Status<select value={status} onChange={(event) => setStatus(event.target.value)}><option value="QUARANTINED">Quarantined</option><option value="RESOLVED">Resolved</option><option value="DISCARDED">Discarded</option><option value="ALL">All statuses</option></select></label><label>Validation code<input value={errorCode} onChange={(event) => { setErrorCode(event.target.value); setOffset(0) }} placeholder="e.g. EMPTY_REQUIRED_FIELD" /></label><label>Resolution notes (optional)<input value={resolutionNotes} onChange={(event) => setResolutionNotes(event.target.value)} maxLength={2000} /></label><Button variant="outline" size="sm" onClick={() => setRefreshId((value) => value + 1)}><RefreshCw size={14} />Refresh</Button></div>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Quarantined records</CardTitle><p className="card-subtitle">Resolve means external follow-up only; it does not repair or reprocess data.</p></div><span className="run-count">{filtered.length}</span></CardHeader><CardContent className="table-content"><RemoteState loading={runs.loading || data.loading} error={runs.error || data.error} empty={runs.runId && data.data && !filtered.length ? { title: 'No quarantined records match', message: 'This run may have no rejected rows, or no rows match these filters.' } : undefined} onRetry={data.refresh}><div className="table-scroll"><table className="workflow-table"><thead><tr><th>Record</th><th>Entity</th><th>Source</th><th>Validation issues</th><th>Status</th><th /></tr></thead><tbody>{filtered.map((item) => <tr key={item.quarantine_id}><td className="mono-cell">{item.source_record_id ?? item.quarantine_id}<small>row {item.source_record_number}</small></td><td>{entityName(item.entity_type)}</td><td>{item.source_file}</td><td>{item.validation_issues.map((issue) => issue.error_code).join(', ')}</td><td><Badge tone={statusTone(item.quarantine_status)}>{item.quarantine_status}</Badge></td><td className="action-cell"><Button variant="ghost" size="sm" onClick={() => setSelectedId(item.quarantine_id)}>Inspect</Button>{item.quarantine_status === 'QUARANTINED' && <><Button variant="outline" size="sm" onClick={() => void resolve(item, 'RESOLVE')}>Resolve</Button><Button variant="outline" size="sm" onClick={() => void resolve(item, 'DISCARD')}>Discard</Button></>}</td></tr>)}</tbody></table></div><Pager total={data.data?.total ?? 0} offset={offset} limit={LIMIT} onChange={setOffset} /></RemoteState></CardContent></Card>
    {selectedId && <DetailDialog title={`Quarantine ${selectedId}`} loading={detail.loading} error={detail.error} data={detail.data} onClose={() => setSelectedId('')}><h3>Original source values</h3><JsonBlock value={detail.data?.original_record} /><h3>Validation issues</h3><IssueList issues={detail.data?.validation_issues ?? []} /><h3>Resolution history</h3><HistoryList items={detail.data?.history ?? []} timeKey="changed_at" labelKey="new_status" /></DetailDialog>}
  </main>
}

function MasterRecordsPage({ searchQuery }: { searchQuery: string }) {
  const runs = useRuns()
  const [entity, setEntity] = useState('')
  const [offset, setOffset] = useState(0)
  const [selectedId, setSelectedId] = useState('')
  const path = runs.runId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/masters?${queryString({ entity_type: entity, search: searchQuery, limit: LIMIT, offset })}` : ''
  const data = useRemote<PageData<MasterItem>>(path)
  const detail = useRemote<MasterItem>(selectedId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/masters/${encodeURIComponent(selectedId)}` : '')
  useEffect(() => { setOffset(0) }, [entity, searchQuery, runs.runId])
  return <main className="page-content workflow-page"><PageHeader title="Master records" description="Search merged records, inspect contributing sources, and follow survivorship selections."><RunSelector runs={runs.runs} runId={runs.runId} onChange={(value) => { runs.setRunId(value); setSelectedId('') }} loading={runs.loading} /></PageHeader>
    <div className="workflow-toolbar"><label>Entity<select value={entity} onChange={(event) => { setEntity(event.target.value); setOffset(0) }}><option value="">All entities</option><option value="customer">Customers</option><option value="product">Products</option></select></label><span className="toolbar-note">Use the global search field for master IDs or merged values.</span></div>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Master records</CardTitle><p className="card-subtitle">Merged values and contributing source counts come from persisted masters.</p></div><span className="run-count">{data.data?.total ?? 0}</span></CardHeader><CardContent className="table-content"><RemoteState loading={runs.loading || data.loading} error={runs.error || data.error} empty={runs.runId && data.data && !data.data.items.length ? { title: 'No master records found', message: 'Try another run, entity, or search term.' } : undefined} onRetry={data.refresh}><div className="table-scroll"><table className="workflow-table"><thead><tr><th>Master ID</th><th>Entity</th><th>Merge method</th><th>Contributing sources</th><th>Created</th><th /></tr></thead><tbody>{data.data?.items.map((item) => <tr key={item.master_id}><td className="mono-cell">{item.master_id}</td><td>{entityName(item.entity_type)}</td><td><Badge tone="slate">{item.merge_method}</Badge></td><td>{item.contributing_source_record_ids?.length ?? 0}</td><td>{formatDate(item.created_at)}</td><td><Button variant="outline" size="sm" onClick={() => setSelectedId(item.master_id)}><Eye size={14} />Inspect</Button></td></tr>)}</tbody></table></div><Pager total={data.data?.total ?? 0} offset={offset} limit={LIMIT} onChange={setOffset} /></RemoteState></CardContent></Card>
    {selectedId && <DetailDialog title={`Master ${selectedId}`} loading={detail.loading} error={detail.error} data={detail.data} onClose={() => setSelectedId('')}><h3>Merged fields</h3><JsonBlock value={detail.data?.merged_fields} /><p className="detail-meta">Method: {detail.data?.merge_method} · Sources: {detail.data?.contributing_source_record_ids?.join(', ')}</p><h3>Contributing source records</h3><SourceCards records={detail.data?.source_records ?? []} /><h3>Field decisions</h3><LineageTable items={detail.data?.field_lineage ?? []} /></DetailDialog>}
  </main>
}

function DataLineagePage({ searchQuery }: { searchQuery: string }) {
  const runs = useRuns()
  const [masterId, setMasterId] = useState('')
  const [offset, setOffset] = useState(0)
  const masterQuery = queryString({ search: searchQuery, limit: 100, offset })
  const masters = useRemote<PageData<MasterItem>>(runs.runId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/masters?${masterQuery}` : '')
  const lineage = useRemote<{ run_id: string; master_id: string; fields: LineageItem[] }>(masterId ? `/api/v1/pipeline-runs/${encodeURIComponent(runs.runId)}/masters/${encodeURIComponent(masterId)}/lineage` : '')
  useEffect(() => { setOffset(0); setMasterId('') }, [runs.runId, searchQuery])
  useEffect(() => { if (!masterId && masters.data?.items.length) setMasterId(masters.data.items[0].master_id) }, [masterId, masters.data])
  return <main className="page-content workflow-page"><PageHeader title="Data lineage" description="Trace every selected master field to its source and survivorship rule."><RunSelector runs={runs.runs} runId={runs.runId} onChange={(value) => { runs.setRunId(value); setMasterId('') }} loading={runs.loading} /></PageHeader>
    <div className="workflow-toolbar"><label>Master record<select value={masterId} onChange={(event) => setMasterId(event.target.value)} disabled={!masters.data?.items.length}><option value="">Select a master</option>{masters.data?.items.map((master) => <option key={master.master_id} value={master.master_id}>{master.master_id} · {entityName(master.entity_type)}</option>)}</select></label><span className="toolbar-note">Global search filters master IDs and merged field values.</span></div>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Field-level lineage</CardTitle><p className="card-subtitle">Selected source, rule, and rejected alternatives.</p></div></CardHeader><CardContent><RemoteState loading={runs.loading || masters.loading || lineage.loading} error={runs.error || masters.error || lineage.error} empty={runs.runId && masters.data && !masters.data.items.length ? { title: 'No master records found', message: 'Lineage appears after a pipeline creates master records.' } : undefined} onRetry={lineage.refresh}>{lineage.data?.fields.length ? <LineageTable items={lineage.data.fields} /> : <div className="workflow-state"><Search size={19} /><strong>Select a master to inspect its field lineage.</strong></div>}</RemoteState></CardContent></Card>
    {masters.data && masters.data.total > masters.data.limit && <Pager total={masters.data.total} offset={offset} limit={100} onChange={(value) => { setOffset(value); setMasterId('') }} />}
  </main>
}

function PipelineRunsPage({ searchQuery, onNavigate }: { searchQuery: string; onNavigate: (path: string) => void }) {
  const [status, setStatus] = useState('')
  const [offset, setOffset] = useState(0)
  const [selectedId, setSelectedId] = useState('')
  const [refreshId, setRefreshId] = useState(0)
  const query = queryString({ status, limit: LIMIT, offset })
  const runs = useRemote<PageData<RunRow>>(`/api/v1/pipeline-runs?${query}`, refreshId)
  const filtered = (runs.data?.items ?? []).filter((run) => !searchQuery || run.run_id.toLowerCase().includes(searchQuery.toLowerCase()))
  const detail = useRemote<RunDetail>(selectedId ? `/api/v1/pipeline-runs/${encodeURIComponent(selectedId)}` : '')
  const errors = useRemote<PageData<Record<string, unknown>>>(selectedId ? `/api/v1/pipeline-runs/${encodeURIComponent(selectedId)}/errors?limit=100` : '')
  useEffect(() => { setOffset(0) }, [status, searchQuery])
  return <main className="page-content workflow-page"><PageHeader title="Pipeline runs" description="Inspect persisted run results, stage metrics, validation findings, and operational errors."><Button variant="outline" onClick={() => onNavigate('/ingestion')}><Upload size={15} />New ingestion</Button></PageHeader>
    <div className="workflow-toolbar"><label>Status<select value={status} onChange={(event) => { setStatus(event.target.value); setOffset(0) }}><option value="">All statuses</option><option value="RUNNING">Running</option><option value="COMPLETED">Completed</option><option value="FAILED">Failed</option></select></label><Button variant="outline" size="sm" onClick={() => setRefreshId((value) => value + 1)}><RefreshCw size={14} />Refresh</Button></div>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Run history</CardTitle><p className="card-subtitle">Use the same run ID and same uploaded bytes to resume; replacement affects only that ID.</p></div><span className="run-count">{runs.data?.total ?? 0}</span></CardHeader><CardContent className="table-content"><RemoteState loading={runs.loading} error={runs.error} empty={runs.data && !filtered.length ? { title: 'No pipeline runs found', message: 'Upload a source file to execute the pipeline.' } : undefined} onRetry={runs.refresh}><div className="table-scroll"><table className="workflow-table"><thead><tr><th>Run ID</th><th>Status</th><th>Customer input</th><th>Product input</th><th>Accepted</th><th>Rejected</th><th>Masters</th><th>Started</th><th /></tr></thead><tbody>{filtered.map((run) => <tr key={run.run_id}><td className="mono-cell">{run.run_id}</td><td><Badge tone={statusTone(run.status)}>{run.status}</Badge></td><td>{run.customer_input_count.toLocaleString()}</td><td>{run.product_input_count.toLocaleString()}</td><td>{run.accepted_count.toLocaleString()}</td><td>{run.rejected_count.toLocaleString()}</td><td>{run.master_record_count.toLocaleString()}</td><td>{formatDate(run.started_at)}</td><td><Button variant="outline" size="sm" onClick={() => setSelectedId(run.run_id)}><Eye size={14} />Details</Button></td></tr>)}</tbody></table></div><Pager total={runs.data?.total ?? 0} offset={offset} limit={LIMIT} onChange={setOffset} /></RemoteState></CardContent></Card>
    {selectedId && <DetailDialog title={`Pipeline run ${selectedId}`} loading={detail.loading || errors.loading} error={detail.error || errors.error} data={detail.data} onClose={() => setSelectedId('')}><div className="detail-inline-actions"><a className="workflow-link" href={apiUrl(`/api/v1/reports/${encodeURIComponent(selectedId)}/download`)}><Download size={15} />Download JSON report</a><Button variant="outline" size="sm" onClick={() => onNavigate(`/ingestion?run_id=${encodeURIComponent(selectedId)}`)}><RefreshCw size={14} />Rerun or replace this ID</Button></div><h3>Stage metrics and durations</h3><JsonBlock value={detail.data?.stage_metrics ?? {}} /><h3>Data-quality issues ({Array.isArray(detail.data?.data_quality_issues) ? detail.data.data_quality_issues.length : 0})</h3><JsonBlock value={detail.data?.data_quality_issues ?? []} /><h3>Operational errors ({errors.data?.total ?? 0})</h3><JsonBlock value={errors.data?.items ?? []} /><h3>Unresolved survivorship conflicts</h3><JsonBlock value={detail.data?.unresolved_conflicts ?? []} /></DetailDialog>}
  </main>
}

function ReportsPage({ searchQuery }: { searchQuery: string }) {
  const [offset, setOffset] = useState(0)
  const [days, setDays] = useState(90)
  const reports = useRemote<PageData<{ run_id: string; report_url: string }>>(`/api/v1/reports?limit=${LIMIT}&offset=${offset}`)
  const summary = useRemote<Record<string, unknown>>(`/api/v1/reports/summary?days=${days}`)
  const filtered = (reports.data?.items ?? []).filter((item) => !searchQuery || item.run_id.toLowerCase().includes(searchQuery.toLowerCase()))
  useEffect(() => { setOffset(0) }, [searchQuery])
  const kpis = summary.data?.kpis as Record<string, unknown> | undefined
  const probable = kpis?.probable_match_decisions as Record<string, number> | undefined
  return <main className="page-content workflow-page"><PageHeader title="Reports and analytics" description="Download persisted execution reports and review database-backed summary metrics."><label className="workflow-select-label">Period<select value={days} onChange={(event) => setDays(Number(event.target.value))}><option value={7}>7 days</option><option value={30}>30 days</option><option value={90}>90 days</option><option value={365}>365 days</option></select></label></PageHeader>
    <RemoteState loading={summary.loading} error={summary.error} onRetry={summary.refresh}><section className="metrics-grid reports-metrics"><SmallMetric title="Source records" value={kpis?.source_records} /><SmallMetric title="Accepted" value={kpis?.accepted_records} /><SmallMetric title="Rejected" value={kpis?.rejected_records} /><SmallMetric title="Master records" value={kpis?.master_records} /><SmallMetric title="Exact pairs" value={kpis?.exact_duplicate_pairs} /><SmallMetric title="Strong probable" value={probable?.STRONG_PROBABLE_MATCH} /></section></RemoteState>
    <Card className="workflow-card"><CardHeader><div><CardTitle>Execution reports</CardTitle><p className="card-subtitle">JSON files are served through run-ID validation; server paths are not exposed.</p></div><span className="run-count">{reports.data?.total ?? 0}</span></CardHeader><CardContent className="table-content"><RemoteState loading={reports.loading} error={reports.error} empty={reports.data && !filtered.length ? { title: 'No reports available', message: 'Reports are created when pipeline runs complete.' } : undefined} onRetry={reports.refresh}><div className="table-scroll"><table className="workflow-table"><thead><tr><th>Run ID</th><th>Report format</th><th /></tr></thead><tbody>{filtered.map((item) => <tr key={item.run_id}><td className="mono-cell">{item.run_id}</td><td><Badge tone="slate">JSON</Badge></td><td><a className="workflow-link" href={apiUrl(item.report_url)}><Download size={14} />Download report</a><a className="workflow-link secondary-link" href={apiUrl(`/api/v1/reports/${encodeURIComponent(item.run_id)}`)} target="_blank" rel="noreferrer"><Eye size={14} />View JSON</a></td></tr>)}</tbody></table></div><Pager total={reports.data?.total ?? 0} offset={offset} limit={LIMIT} onChange={setOffset} /></RemoteState></CardContent></Card>
  </main>
}

function SmallMetric({ title, value }: { title: string; value: unknown }) {
  return <Card className="metric-card"><CardContent className="metric-content"><div className="metric-title">{title}</div><strong className="metric-value">{typeof value === 'number' ? value.toLocaleString() : '—'}</strong></CardContent></Card>
}

function DetailDialog({ title, loading, error, data, onClose, children }: {
  title: string; loading: boolean; error: string; data: unknown; onClose: () => void; children: ReactNode
}) {
  return <div className="detail-backdrop" role="presentation" onClick={(event) => { if (event.target === event.currentTarget) onClose() }}><section className="detail-drawer" role="dialog" aria-modal="true" aria-label={title}><div className="drawer-header"><div><div className="eyebrow">RECORD DETAIL</div><h2>{title}</h2></div><Button variant="ghost" size="icon" aria-label="Close detail" onClick={onClose}><X size={18} /></Button></div><div className="drawer-body"><RemoteState loading={loading} error={error} empty={!loading && !error && !data ? { title: 'Record unavailable', message: 'The selected record could not be found.' } : undefined}>{children}</RemoteState></div></section></div>
}

function JsonBlock({ value }: { value: unknown }) {
  return <pre className="json-block">{JSON.stringify(value ?? null, null, 2)}</pre>
}

function SourceCards({ records }: { records: SourceRecord[] }) {
  if (!records.length) return <p className="muted-copy">No source-record details are available.</p>
  return <div className="source-cards">{records.map((record) => <section key={record.source_record_id} className="source-card"><div><strong>{record.source_record_id}</strong><Badge tone={record.validation_status === 'ACCEPTED' ? 'green' : 'amber'}>{record.validation_status}</Badge></div><small>{record.source_system ?? 'Unknown source'} · {entityName(record.entity_type)}</small><JsonBlock value={record.original_record} /></section>)}</div>
}

function LineageTable({ items }: { items: LineageItem[] }) {
  return <div className="table-scroll"><table className="workflow-table lineage-table"><thead><tr><th>Field</th><th>Selected value</th><th>Source record</th><th>Source system</th><th>Selection rule</th><th>Rejected values</th></tr></thead><tbody>{items.map((item) => <tr key={item.field_name}><td><strong>{item.field_name}</strong></td><td>{displayValue(item.selected_value)}</td><td className="mono-cell">{item.selected_source_record_id ?? '—'}</td><td>{item.selected_source_system ?? '—'}</td><td>{item.selection_rule}</td><td>{item.rejected_values?.length ? <JsonBlock value={item.rejected_values} /> : '—'}</td></tr>)}</tbody></table></div>
}

function IssueList({ issues }: { issues: { error_code: string; field_name: string; message: string }[] }) {
  if (!issues.length) return <p className="muted-copy">No validation issues.</p>
  return <ul className="issue-list-detail">{issues.map((issue, index) => <li key={`${issue.error_code}-${issue.field_name}-${index}`}><Badge tone="amber">{issue.error_code}</Badge><strong>{issue.field_name}</strong><span>{issue.message}</span></li>)}</ul>
}

function HistoryList({ items, timeKey, labelKey }: { items: Record<string, unknown>[]; timeKey: string; labelKey: string }) {
  if (!items.length) return <p className="muted-copy">No history events recorded.</p>
  return <ol className="history-list">{items.map((item, index) => <li key={`${String(item[timeKey])}-${index}`}><Badge tone="slate">{String(item[labelKey] ?? 'Updated')}</Badge><time>{formatDate(String(item[timeKey] ?? ''))}</time>{item.reviewer_id != null && item.reviewer_id !== '' && <span>{String(item.reviewer_id)}</span>}{item.reviewer_notes != null && item.reviewer_notes !== '' && <p>{String(item.reviewer_notes)}</p>}{item.resolution_notes != null && item.resolution_notes !== '' && <p>{String(item.resolution_notes)}</p>}</li>)}</ol>
}

function formatConfidence(value: number | null) {
  if (value == null) return '—'
  const percentage = value <= 1 ? value * 100 : value
  return `${percentage.toFixed(1)}%`
}

function evidenceSummary(value: unknown) {
  if (!value || typeof value !== 'object') return '—'
  const record = value as Record<string, unknown>
  const attributes = record.matching_attributes
  return Array.isArray(attributes) ? attributes.join(', ') || 'No matching attributes' : 'Inspect evidence'
}

function displayValue(value: unknown) {
  if (value == null || value === '') return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function entityName(value: Entity) {
  return value === 'customer' ? 'Customer' : 'Product'
}

function statusTone(value: string): 'green' | 'amber' | 'blue' | 'slate' {
  if (['COMPLETED', 'ACCEPTED_MATCH', 'RESOLVED'].includes(value)) return 'green'
  if (['FAILED', 'REJECTED_MATCH', 'DISCARDED', 'QUARANTINED'].includes(value)) return 'amber'
  if (['RUNNING', 'PENDING'].includes(value)) return 'blue'
  return 'slate'
}

function decisionTone(value: string): 'green' | 'amber' | 'blue' | 'slate' {
  if (value === 'EXACT' || value === 'STRONG_PROBABLE_MATCH') return 'green'
  if (value === 'REVIEW_REQUIRED') return 'amber'
  if (value === 'NO_MATCH') return 'slate'
  return 'blue'
}

function formatDate(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

export function WorkflowPage({ path, searchQuery, onNavigate }: {
  path: string
  searchQuery: string
  onNavigate: (path: string) => void
}) {
  switch (path) {
    case '/ingestion': return <DataIngestionPage onNavigate={onNavigate} />
    case '/duplicates': return <DuplicateExplorerPage searchQuery={searchQuery} />
    case '/reviews': return <ReviewQueuePage searchQuery={searchQuery} />
    case '/quarantine': return <QuarantinePage searchQuery={searchQuery} />
    case '/masters': return <MasterRecordsPage searchQuery={searchQuery} />
    case '/lineage': return <DataLineagePage searchQuery={searchQuery} />
    case '/runs': return <PipelineRunsPage searchQuery={searchQuery} onNavigate={onNavigate} />
    case '/reports': return <ReportsPage searchQuery={searchQuery} />
    default: return <main className="page-content workflow-page"><PageHeader title="Page not found" description="Choose a MergeMind workspace view from the navigation." /><Button variant="outline" onClick={() => onNavigate('/')}>Dashboard</Button></main>
  }
}
