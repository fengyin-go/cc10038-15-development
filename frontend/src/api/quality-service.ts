/** 分来源质量校验接口：控制台读取、样本核对、告警核对与来源覆盖清单。 */
import { fetchJson, request } from './client'
import type {
  ConclusionSummary,
  QualityOverview,
  SampleResult,
  SourceQuality,
} from '@/data/types'

export async function loadQualityOverview(viewName = '班报-跨区'): Promise<QualityOverview> {
  return await fetchJson<QualityOverview>(
    `/api/pipeline/quality/overview?view_name=${encodeURIComponent(viewName)}`,
  )
}

export async function runQualitySample(
  sourceId: string,
  logs: string[],
  timeWindow = 'manual',
): Promise<SampleResult> {
  const response = await request('/api/pipeline/quality/sample', {
    method: 'POST',
    body: JSON.stringify({
      source_id: sourceId,
      time_window: timeWindow,
      raw_logs: logs.map((body) => ({ body })),
      note: '控制台样本核对',
    }),
  })
  if (!response.ok) throw new Error('样本核对失败')
  return (await response.json()) as SampleResult
}

export async function checkSourceAlerts(sourceId: string): Promise<{
  ok: boolean
  message: string
  passed?: boolean
  alerts?: { 级别: string; 字段: string; 内容: string }[]
}> {
  const response = await request(
    `/api/pipeline/quality/alerts/check?source_id=${encodeURIComponent(sourceId)}`,
    { method: 'POST' },
  )
  return await response.json()
}

export async function loadConclusion(conclusionId: string): Promise<ConclusionSummary> {
  return await fetchJson<ConclusionSummary>(
    `/api/pipeline/quality/conclusions/${encodeURIComponent(conclusionId)}`,
  )
}

async function post(path: string): Promise<{ ok: boolean; message: string; [key: string]: unknown }> {
  const response = await request(path, { method: 'POST' })
  return await response.json()
}

export async function refreshAggregation(): Promise<{ ok: boolean; message: string; [key: string]: unknown }> {
  const response = await request('/api/pipeline/aggregation/refresh', {
    method: 'POST',
    body: JSON.stringify({
      view_name: '班报-跨区',
      dim_field: 'level',
      metric_field: 'time',
      agg_func: 'count',
      report_scope: '班报-跨区',
    }),
  })
  return await response.json()
}

export async function publishAggregation(): Promise<{ ok: boolean; message: string; [key: string]: unknown }> {
  return await post('/api/pipeline/aggregation/班报-跨区/publish')
}

export type { QualityOverview, SourceQuality }
