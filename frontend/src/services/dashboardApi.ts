import { apiGet } from './apiClient'

export type EntityType = 'customer' | 'product'

export interface DashboardKpis {
  source_records: number
  accepted_records: number
  rejected_records: number
  master_records: number
  exact_duplicate_groups: number
  exact_duplicate_pairs: number
  probable_match_decisions: Record<string, number>
  pending_reviews: number | null
  quarantine_records: number | null
  accepted_rate_percent: number | null
}

export interface PipelineActivity {
  run_id: string
  started_at: string
  completed_at: string | null
  status: string
  source_records: number
  accepted_records: number
  rejected_records: number
  master_records: number
  entities: EntityType[]
  exact_duplicate_pairs: number
  probable_match_decisions: Record<string, number>
}

export interface DashboardData {
  database_status: 'available'
  review_data_available: boolean
  quarantine_data_available: boolean
  generated_at: string
  days: number
  kpis: DashboardKpis
  total_runs: number
  limit: number
  offset: number
  runs: PipelineActivity[]
  activity: PipelineActivity[]
}

export interface DashboardApi {
  getDashboardData(days: number, signal?: AbortSignal): Promise<DashboardData>
}

export const dashboardApi: DashboardApi = {
  getDashboardData(days, signal) {
    const query = new URLSearchParams({ days: String(days), limit: '100', offset: '0' })
    return apiGet<DashboardData>(`/api/v1/dashboard?${query}`, signal)
  },
}