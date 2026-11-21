/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

export type MissingStat = { 缺失数: number; 缺失率: number }

export type ConclusionSummary = {
  结论编号: string
  id: number
  来源标识: string
  采集区域: string
  批次编号: string
  结论版本: number
  格式版本: string
  格式名称: string
  规则名称: string
  规则版本: number
  判定依据: string
  必填字段: string[]
  样本数: number
  分母: number
  缺失统计: Record<string, MissingStat>
  阈值: {
    required_fields: string[]
    max_missing_rate: number
    per_field: Record<string, number>
    min_records: number
  }
  触发阈值: { 字段: string; 阈值: number; 实测: number; 缺失数: number; 分母: number }[]
  越线字段: string[]
  是否合格: boolean
  结论: string
  结论说明: string
  缺失行: MissingRow[]
  评估时间: string
}

export type MissingRow = {
  标准化记录ID: number
  原始记录ID: string
  来源批次: string
  缺失字段: string[]
  缺失值: Record<string, string>
  原始正文摘录: string
}

export type SourceQuality = {
  来源标识: string
  采集区域: string
  接入协议: string | null
  源状态: string
  覆盖状态: string | null
  覆盖清单版本: string | null
  规则: {
    id: number
    规则名称: string
    规则版本: number
    阈值参数: string
    阈值: ConclusionSummary['阈值']
  } | null
  最新结论: ConclusionSummary | null
  告警核对: { 核对结果: string; 核对时间: string; 结论编号: string } | null
}

export type CoverageEntry = {
  来源标识: string
  采集区域: string
  结论编号: string | null
  格式版本: string
  判定依据: string
  样本数: number
  日志数: number
  合格日志数?: number
  未计入日志数?: number
  记录缺口?: number
  原因?: string
  触发阈值?: ConclusionSummary['触发阈值']
  缺失行?: MissingRow[]
}

export type Coverage = {
  视图名称: string
  覆盖版本: number
  生成时间: string
  发布状态?: string
  发布时间?: string
  数据源范围: string[]
  来源总数: number
  计入来源数: number
  未计入来源数: number
  覆盖完整: boolean
  口径: string
  计入来源: CoverageEntry[]
  未计入来源: CoverageEntry[]
}

export type QualityOverview = {
  view_name: string
  sources: SourceQuality[]
  view: {
    id: number | null
    视图名称: string | null
    status: string | null
    published: boolean
    published_at: string | null
    合格日志数: number | null
    计入来源数: number | null
    未计入来源数: number | null
    覆盖版本: number | null
    覆盖完整: boolean | null
    coverage: Coverage | null
  } | null
  history: ConclusionSummary[]
  alert_checks: {
    来源标识: string
    采集区域: string
    结论编号: string
    核对结果: string
    告警条数: number
    核对时间: string
  }[]
  summary: { 来源数: number; 合格来源: number; 越线来源: number; 未核对来源: number }
}

export type SampleResult = {
  ok: boolean
  message: string
  source_id: string
  batch_no: string
  结论编号?: string
  conclusion: ConclusionSummary | null
  missing_rows: MissingRow[]
  triggered: ConclusionSummary['触发阈值']
}
