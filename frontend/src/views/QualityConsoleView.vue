<template>
  <section class="page quality-console">
    <header class="page-head">
      <div>
        <h2>分来源质量校验</h2>
        <p class="page-desc">
          每处数据按本处现行口径重算必填字段缺失率；聚合只统计合格来源，发布结果回写来源覆盖清单。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="quality-refresh" type="button" :disabled="busy" @click="refreshView">
          刷新聚合视图
        </button>
        <button class="btn primary" data-testid="quality-publish" type="button" :disabled="busy" @click="publishView">
          发布班报结果
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="quality-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="quality-notice">{{ notice }}</p>

    <div class="stat-row">
      <article v-for="item in cards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <section class="panel">
      <header class="panel-head">
        <h3>来源判定依据</h3>
        <span class="muted">同一格式版本下，各区仍按各自登记的阈值判定</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>来源</th><th>区域</th><th>格式版本</th><th>现行口径</th>
            <th>最新结论</th><th>必填字段缺失率</th><th>覆盖状态</th><th>告警核对</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="source in overview?.sources ?? []" :key="source.来源标识" :data-testid="`quality-source-${source.来源标识}`">
            <td>{{ source.来源标识 }}</td>
            <td>{{ source.采集区域 }}</td>
            <td>{{ source.最新结论?.格式版本 ?? '—' }}</td>
            <td class="cell-rule">
              <span v-if="source.规则">
                {{ source.规则.规则名称 }} v{{ source.规则.规则版本 }}
                <small>{{ source.规则.阈值参数 }}</small>
              </span>
              <span v-else class="muted">未登记来源口径</span>
            </td>
            <td>
              <span :class="['verdict', verdictClass(source.最新结论?.结论)]">
                {{ source.最新结论?.结论 ?? '未核对' }}
              </span>
            </td>
            <td class="cell-missing">
              <template v-if="source.最新结论">
                <span
                  v-for="(stat, field) in source.最新结论.缺失统计"
                  :key="field"
                  :class="['missing-chip', stat.缺失数 ? 'has-missing' : '']"
                >
                  {{ field }} {{ (stat.缺失率 * 100).toFixed(1) }}%
                  <em v-if="fieldIn(source.最新结论.越线字段, field)">越线</em>
                </span>
                <small class="muted">分母 {{ source.最新结论.分母 }}</small>
              </template>
              <span v-else class="muted">—</span>
            </td>
            <td>
              <span v-if="source.覆盖状态" :class="['verdict', source.覆盖状态 === '已计入' ? 'ok' : 'bad']">
                {{ source.覆盖状态 }}
              </span>
              <span v-else class="muted">未发布</span>
            </td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`quality-check-${source.来源标识}`" @click="checkAlerts(source.来源标识)">
                告警核对
              </button>
              <button
                v-if="source.最新结论"
                class="link"
                type="button"
                @click="showConclusion(source.最新结论.结论编号)"
              >
                查看结论
              </button>
            </td>
          </tr>
          <tr v-if="!(overview?.sources ?? []).length">
            <td colspan="8" class="empty-state">暂无来源数据，请先登记数据源并采集样本</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>班报来源组成</h3>
        <span class="muted">
          {{ overview?.view?.published ? `已于 ${overview?.view?.published_at} 发布` : '尚未发布' }}
          <template v-if="overview?.view?.覆盖版本">· 覆盖清单 v{{ overview?.view?.覆盖版本 }}</template>
        </span>
      </header>
      <div class="coverage-grid">
        <article class="coverage-card included">
          <h4>计入来源（{{ overview?.view?.coverage?.计入来源数 ?? 0 }}）</h4>
          <ul>
            <li v-for="item in overview?.view?.coverage?.计入来源 ?? []" :key="item.来源标识">
              <strong>{{ item.来源标识 }}</strong>
              <span>{{ item.采集区域 }}</span>
              <span>{{ item.合格日志数 }} / {{ item.日志数 }} 条</span>
              <span class="muted">{{ item.格式版本 }} · {{ item.结论编号 }}</span>
            </li>
            <li v-if="!(overview?.view?.coverage?.计入来源 ?? []).length" class="muted">暂无</li>
          </ul>
        </article>
        <article class="coverage-card excluded">
          <h4>未计入来源（{{ overview?.view?.coverage?.未计入来源数 ?? 0 }}）</h4>
          <ul>
            <li v-for="item in overview?.view?.coverage?.未计入来源 ?? []" :key="item.来源标识">
              <strong>{{ item.来源标识 }}</strong>
              <span>{{ item.采集区域 }}</span>
              <span>{{ item.未计入日志数 }} 条未计入</span>
              <span class="reason">{{ item.原因 }}</span>
              <button class="link" type="button" @click="showConclusion(item.结论编号 ?? '')">
                结论 {{ item.结论编号 }}
              </button>
            </li>
            <li v-if="!(overview?.view?.coverage?.未计入来源 ?? []).length" class="muted">无，覆盖完整</li>
          </ul>
        </article>
      </div>
      <p class="coverage-foot">
        合格日志数 {{ overview?.view?.合格日志数 ?? 0 }} 条 ·
        {{ overview?.view?.覆盖完整 ? '来源覆盖完整' : '存在未计入来源，报告需附校验条目' }}
      </p>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>样本核对</h3>
        <span class="muted">给某处发样本后按该处现行口径重算，不与其他来源共用分母</span>
      </header>
      <div class="sample-form">
        <label>
          <span>数据源</span>
          <select v-model="sampleSource" data-testid="sample-source">
            <option v-for="source in overview?.sources ?? []" :key="source.来源标识" :value="source.来源标识">
              {{ source.采集区域 }} · {{ source.来源标识 }}
            </option>
          </select>
        </label>
        <label class="sample-logs">
          <span>样本日志（每行一条，竖线分隔 time|level|message|host）</span>
          <textarea v-model="sampleLogs" rows="5" data-testid="sample-logs" />
        </label>
        <button class="btn primary" type="button" :disabled="busy || !sampleSource" @click="runSample">
          发送样本并核对
        </button>
      </div>
      <p v-if="sampleOutcome" class="sample-outcome" data-testid="sample-outcome">
        {{ sampleOutcome }}
      </p>
    </section>

    <section class="panel">
      <header class="panel-head">
        <h3>校验结论历史</h3>
        <span class="muted">每条结论冻结出报时的分母、阈值和缺失行</span>
      </header>
      <table class="data-table">
        <thead>
          <tr><th>结论编号</th><th>来源</th><th>批次</th><th>格式版本</th><th>结论</th><th>分母</th><th>评估时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in overview?.history ?? []" :key="item.结论编号">
            <td><button class="link" type="button" @click="showConclusion(item.结论编号)">{{ item.结论编号 }}</button></td>
            <td>{{ item.采集区域 }} · {{ item.来源标识 }}</td>
            <td>{{ item.批次编号 }}</td>
            <td>{{ item.格式版本 }}</td>
            <td><span :class="['verdict', verdictClass(item.结论)]">{{ item.结论 }}</span></td>
            <td>{{ item.分母 }}</td>
            <td>{{ item.评估时间 }}</td>
          </tr>
          <tr v-if="!(overview?.history ?? []).length">
            <td colspan="7" class="empty-state">尚无校验结论</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="detail" class="modal-backdrop" data-testid="conclusion-modal" @click.self="detail = null">
      <div class="modal conclusion-modal">
        <header class="panel-head">
          <h3>校验结论 {{ detail.结论编号 }}</h3>
          <button class="btn ghost" type="button" @click="detail = null">关闭</button>
        </header>
        <dl class="conclusion-meta">
          <div><dt>来源</dt><dd>{{ detail.采集区域 }} · {{ detail.来源标识 }}</dd></div>
          <div><dt>批次</dt><dd>{{ detail.批次编号 }}</dd></div>
          <div><dt>结论</dt><dd><span :class="['verdict', verdictClass(detail.结论)]">{{ detail.结论 }}</span></dd></div>
          <div><dt>分母</dt><dd>{{ detail.分母 }} 条</dd></div>
          <div><dt>格式版本</dt><dd>{{ detail.格式版本 }}（{{ detail.格式名称 || '未登记格式名' }}）</dd></div>
          <div><dt>规则</dt><dd>{{ detail.规则名称 }} v{{ detail.规则版本 }}</dd></div>
        </dl>
        <p class="basis">{{ detail.判定依据 }}</p>
        <table class="data-table">
          <thead><tr><th>必填字段</th><th>缺失数</th><th>缺失率</th><th>阈值</th></tr></thead>
          <tbody>
            <tr v-for="field in detail.必填字段" :key="field">
              <td>{{ field }}</td>
              <td>{{ detail.缺失统计[field]?.缺失数 ?? 0 }}</td>
              <td>{{ ((detail.缺失统计[field]?.缺失率 ?? 0) * 100).toFixed(1) }}%</td>
              <td>{{ thresholdFor(detail, field) }}</td>
            </tr>
          </tbody>
        </table>
        <h4>缺失行（{{ detail.缺失行.length }}）</h4>
        <table class="data-table">
          <thead><tr><th>记录ID</th><th>批次</th><th>缺失字段</th><th>原始正文摘录</th></tr></thead>
          <tbody>
            <tr v-for="row in detail.缺失行" :key="row.标准化记录ID">
              <td>{{ row.标准化记录ID }}</td>
              <td>{{ row.来源批次 }}</td>
              <td>{{ row.缺失字段.join('、') }}</td>
              <td class="excerpt">{{ row.原始正文摘录 }}</td>
            </tr>
            <tr v-if="!detail.缺失行.length"><td colspan="4" class="empty-state">无缺失行</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  checkSourceAlerts,
  loadConclusion,
  loadQualityOverview,
  publishAggregation,
  refreshAggregation,
  runQualitySample,
} from '@/api/quality-service'
import type { ConclusionSummary, QualityOverview } from '@/data/types'

const overview = ref<QualityOverview | null>(null)
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)
const detail = ref<ConclusionSummary | null>(null)
const sampleSource = ref('')
const sampleLogs = ref('')
const sampleOutcome = ref('')

const cards = computed(() => {
  const summary = overview.value?.summary
  return [
    { label: '来源总数', value: summary?.来源数 ?? 0 },
    { label: '合格来源', value: summary?.合格来源 ?? 0 },
    { label: '越线来源', value: summary?.越线来源 ?? 0 },
    { label: '合格日志数', value: overview.value?.view?.合格日志数 ?? 0 },
  ]
})

function verdictClass(verdict?: string | null): string {
  if (verdict === '合格') return 'ok'
  if (verdict === '越线' || verdict === '无样本' || verdict === '未核对') return 'bad'
  return 'warn'
}

function fieldIn(fields: string[] | undefined, field: string): boolean {
  return (fields ?? []).includes(field)
}

function thresholdFor(conclusion: ConclusionSummary, field: string): string {
  const limit = conclusion.阈值?.per_field?.[field] ?? conclusion.阈值?.max_missing_rate ?? 0
  return `${(limit * 100).toFixed(1)}%`
}

async function reload() {
  overview.value = await loadQualityOverview()
  if (!sampleSource.value && overview.value?.sources.length) {
    sampleSource.value = overview.value.sources[0].来源标识
  }
}

onMounted(async () => {
  try {
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '质量校验数据读取失败'
  }
})

async function runTask(task: () => Promise<void>) {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    await task()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  } finally {
    busy.value = false
  }
}

async function refreshView() {
  await runTask(async () => {
    const result = await refreshAggregation()
    notice.value = String(result.message ?? '聚合视图已刷新')
  })
}

async function publishView() {
  await runTask(async () => {
    const result = await publishAggregation()
    notice.value = String(result.message ?? '班报已发布')
  })
}

async function checkAlerts(identifier: string) {
  await runTask(async () => {
    const result = await checkSourceAlerts(identifier)
    notice.value = result.message
  })
}

async function runSample() {
  await runTask(async () => {
    const logs = sampleLogs.value
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
    if (!logs.length) throw new Error('请先填写至少一行样本日志')
    const result = await runQualitySample(sampleSource.value, logs)
    const conclusion = result.conclusion
    sampleOutcome.value = conclusion
      ? `${result.message}：分母 ${conclusion.分母}，${conclusion.结论说明}`
      : result.message
    if (conclusion) sampleLogs.value = ''
  })
}

async function showConclusion(conclusionId: string) {
  if (!conclusionId) return
  try {
    detail.value = await loadConclusion(conclusionId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '校验结论读取失败'
  }
}
</script>

<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; margin-bottom: 14px; }
.panel-head { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; }
.panel-head h3 { font-size: 14px; margin: 0 0 8px; }
.panel-head h4, h4 { font-size: 13px; margin: 14px 0 6px; }
.muted { color: var(--muted); font-size: 12px; }
.notice-text { color: #0b6b3a; font-size: 13px; }
.cell-rule span { display: flex; flex-direction: column; font-size: 12px; }
.cell-missing { display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }
.missing-chip { border: 1px solid var(--border); border-radius: 10px; padding: 1px 8px; font-size: 12px; }
.missing-chip.has-missing { border-color: #f0a5a5; background: #fff4f4; }
.missing-chip em { font-style: normal; color: #b42318; margin-left: 4px; }
.verdict { border-radius: 10px; padding: 1px 8px; font-size: 12px; }
.verdict.ok { background: #e7f7ee; color: #0b6b3a; }
.verdict.bad { background: #fdecec; color: #b42318; }
.verdict.warn { background: #fff7e6; color: #92400e; }
.coverage-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.coverage-card { border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; }
.coverage-card.included { background: #f7fdf9; }
.coverage-card.excluded { background: #fffafa; }
.coverage-card ul { list-style: none; padding: 0; margin: 0; }
.coverage-card li { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; padding: 6px 0; border-bottom: 1px dashed #e5e7eb; font-size: 13px; }
.coverage-card li:last-child { border-bottom: none; }
.coverage-card .reason { color: #b42318; }
.coverage-foot { font-size: 12px; color: var(--muted); margin: 8px 0 0; }
.sample-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
.sample-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.sample-form select, .sample-form textarea { border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; font-size: 13px; }
.sample-logs { flex: 1; min-width: 320px; }
.sample-outcome { margin: 10px 0 0; font-size: 13px; }
.conclusion-modal { width: min(760px, 96vw); }
.conclusion-meta { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 8px 0; }
.conclusion-meta dt { font-size: 12px; color: var(--muted); }
.conclusion-meta dd { margin: 0; font-size: 13px; }
.basis { font-size: 12px; color: var(--muted); background: #f8fafc; padding: 8px; border-radius: 6px; }
.excerpt { max-width: 280px; word-break: break-all; font-size: 12px; }
</style>
