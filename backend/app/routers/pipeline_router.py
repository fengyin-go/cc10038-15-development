"""流水线路由：暴露跨模块数据采集流水线的 HTTP 接口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import source_quality
from app.services.pipeline import pipeline
from app.store import store

router = APIRouter(prefix="/api/pipeline", tags=["采集流水线"])


class SourceRegisterPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    protocol: str = "http"
    region: str = "default"


class IngestBatchPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)


class ParseBatchPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    parser_rule_name: str | None = None


class DedupPayload(BaseModel):
    dedup_key_fields: list[str] = Field(..., min_length=1)
    window: str = "batch"
    retain_strategy: str = "keep_first"


class AggregationPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    dim_field: str = Field(..., min_length=1)
    metric_field: str = "count"
    agg_func: str = "count"
    enforce_quality: bool = True
    source_id: str | None = None
    batch_no: str | None = None
    report_scope: str | None = None


class ExportPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    export_format: str = "csv"
    filters: dict[str, str] | None = None


class WatermarkPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    batch_no: str = Field(..., min_length=1)
    watermark_value: str = Field(..., min_length=1)


class ReplayPayload(BaseModel):
    history_batch_range: str = Field(..., min_length=1)
    new_parser_rule: str = Field(..., min_length=1)


class SchemaEvolvePayload(BaseModel):
    version: str = Field(..., min_length=1)
    data_source: str = Field(..., min_length=1)
    field_defs: list[str] = Field(..., min_length=1)
    backward_compatible: bool = True


class FullPipelinePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)
    parser_rule: str | None = None


class QualityRulePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    required_fields: list[str] = Field(..., min_length=1)
    max_missing_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    per_field: dict[str, float] | None = None
    severity: str = "高"
    rule_name: str | None = None
    format_version: str | None = None


class QualityEvaluatePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    batch_no: str | None = None
    scope: str = "latest"
    note: str | None = None


class QualitySamplePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)
    parser_rule: str | None = None
    note: str | None = None


@router.post("/source/register")
def pipeline_register_source(payload: SourceRegisterPayload) -> dict[str, Any]:
    return pipeline.register_source(payload.source_id, payload.protocol, payload.region)


@router.post("/source/validate")
def pipeline_validate_source(source_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.validate_source(source_id)


@router.post("/batch/start")
def pipeline_start_batch(payload: IngestBatchPayload) -> dict[str, Any]:
    return pipeline.start_ingest_batch(payload.source_id, payload.time_window, payload.raw_logs)


@router.post("/batch/parse")
def pipeline_parse_batch(payload: ParseBatchPayload) -> dict[str, Any]:
    return pipeline.parse_batch(payload.batch_no, payload.parser_rule_name)


@router.post("/batch/recover")
def pipeline_recover_batch(batch_no: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.recover_batch(batch_no)


@router.post("/dedup/run")
def pipeline_run_dedup(payload: DedupPayload) -> dict[str, Any]:
    return pipeline.run_dedup(payload.dedup_key_fields, payload.window, payload.retain_strategy)


@router.post("/validate")
def pipeline_validate_entities(
    batch_no: str | None = Query(default=None),
    source_id: str | None = Query(default=None),
    scope: str = Query(default="latest"),
) -> dict[str, Any]:
    return pipeline.validate_entities(batch_no, source_id=source_id, scope=scope)


@router.get("/quality/overview")
def quality_overview(view_name: str = Query(default="班报-跨区")) -> dict[str, Any]:
    """分来源质量校验控制台：现行口径、最新结论、覆盖状态和告警核对。"""
    return source_quality.overview(view_name)


@router.get("/quality/rules")
def quality_rules_list() -> dict[str, Any]:
    rules = [
        {
            "id": row.get("id"),
            "规则名称": row.get("规则名称"),
            "适用数据源": row.get("适用数据源"),
            "规则版本": row.get("规则版本"),
            "threshold": source_quality.rule_threshold(row),
            "格式版本": row.get("格式版本"),
            "status": row.get("status"),
        }
        for row in store.rows("quality_check")
        if row.get("校验类型") == "必填字段缺失率"
    ]
    return {"items": rules, "total": len(rules)}


@router.post("/quality/rules")
def quality_rule_configure(payload: QualityRulePayload) -> dict[str, Any]:
    """登记或调整数据源的现行口径，规则版本自增，历史结论保持原样。"""
    rule = source_quality.configure_rule(
        payload.source_id,
        payload.required_fields,
        payload.max_missing_rate,
        per_field=payload.per_field,
        severity=payload.severity,
        rule_name=payload.rule_name,
        format_version=payload.format_version,
    )
    return {
        "ok": True,
        "message": f"{payload.source_id} 现行口径已登记 v{rule.get('规则版本')}",
        "rule": rule,
    }


@router.post("/quality/evaluate")
def quality_evaluate(payload: QualityEvaluatePayload) -> dict[str, Any]:
    """按数据源现行口径重算缺失率并追加一条结论。"""
    return source_quality.evaluate_source_quality(
        payload.source_id,
        batch_no=payload.batch_no,
        scope=payload.scope,
        note=payload.note,
    )


@router.get("/quality/conclusions")
def quality_conclusions(
    source_id: str | None = Query(default=None),
    batch_no: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    rows = [
        row for row in store.rows(source_quality.CONCLUSION_MODULE)
        if (source_id is None or row.get("来源标识") == source_id)
        and (batch_no is None or row.get("批次编号") == batch_no)
    ]
    rows = sorted(rows, key=lambda row: int(row.get("id", 0)), reverse=True)[:limit]
    items = [source_quality.conclusion_summary(row) for row in rows]
    return {"items": items, "total": len(items)}


@router.get("/quality/conclusions/{identifier}")
def quality_conclusion_detail(identifier: str) -> dict[str, Any]:
    """按结论编号或 id 读取校验条目：分母、阈值、缺失行都在这里冻结。"""
    conclusion = source_quality.get_conclusion(identifier)
    if conclusion is None:
        raise HTTPException(status_code=404, detail=f"校验结论 {identifier} 不存在")
    return conclusion


@router.get("/quality/alerts")
def quality_alerts(source_id: str | None = Query(default=None)) -> dict[str, Any]:
    rows = [
        row for row in store.rows(source_quality.ALERT_MODULE)
        if source_id is None or row.get("来源标识") == source_id
    ]
    rows = sorted(rows, key=lambda row: int(row.get("id", 0)), reverse=True)
    return {
        "items": rows,
        "total": len(rows),
        "pending_sources": sorted({
            sid for sid in source_quality._source_ids_in_scope()
            if source_quality.latest_conclusion(sid) is None
            or source_quality.latest_alert_check(sid) is None
        }),
    }


@router.post("/quality/alerts/check")
def quality_alert_check(
    source_id: str = Query(..., min_length=1),
    batch_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """对单个数据源做告警核对，可按区域独立执行。"""
    return source_quality.run_alert_check(source_id, batch_no)


@router.post("/quality/sample")
def quality_run_sample(payload: QualitySamplePayload) -> dict[str, Any]:
    """采集一份样本并重算本来源缺失率，用于分区域核对是否越线。"""
    result = pipeline.run_quality_sample(
        payload.source_id,
        payload.time_window,
        payload.raw_logs,
        parser_rule=payload.parser_rule,
        note=payload.note,
    )
    return result


@router.get("/aggregation/{view_name}/coverage")
def aggregation_coverage(view_name: str) -> dict[str, Any]:
    """读取发布结果回写的来源覆盖清单。"""
    views = [
        row for row in store.rows("aggregation_view")
        if row.get("视图名称") == view_name
    ]
    if not views:
        raise HTTPException(status_code=404, detail=f"聚合视图 {view_name} 不存在")
    latest = max(views, key=lambda row: int(row.get("id", 0)))
    return {
        "view_name": view_name,
        "view_id": latest.get("id"),
        "覆盖版本": latest.get("覆盖版本"),
        "published": latest.get("published", False),
        "published_at": latest.get("published_at"),
        "合格日志数": latest.get("合格日志数"),
        "来源覆盖清单引用": latest.get("来源覆盖清单引用"),
        "coverage": latest.get("来源覆盖清单"),
    }


@router.post("/aggregation/refresh")
def pipeline_refresh_aggregation(payload: AggregationPayload) -> dict[str, Any]:
    return pipeline.refresh_aggregation(
        payload.view_name,
        payload.dim_field,
        payload.metric_field,
        payload.agg_func,
        enforce_quality=payload.enforce_quality,
        filters={
            key: value for key, value in {
                "source_id": payload.source_id,
                "batch_no": payload.batch_no,
            }.items() if value
        },
        report_scope=payload.report_scope,
    )


@router.post("/aggregation/{view_name}/publish")
def aggregation_publish(view_name: str) -> dict[str, Any]:
    """发布聚合结果：回写来源覆盖清单，并给出报告使用方的校验条目索引。"""
    return pipeline.publish_aggregation(view_name)


@router.post("/export/build")
def pipeline_build_export(payload: ExportPayload) -> dict[str, Any]:
    return pipeline.build_export(payload.view_name, payload.export_format, payload.filters)


@router.post("/watermark/advance")
def pipeline_advance_watermark(payload: WatermarkPayload) -> dict[str, Any]:
    return pipeline.advance_watermark(payload.source_id, payload.batch_no, payload.watermark_value)


@router.post("/replay/batch")
def pipeline_replay_batch(payload: ReplayPayload) -> dict[str, Any]:
    return pipeline.replay_batch(payload.history_batch_range, payload.new_parser_rule)


@router.post("/schema/evolve")
def pipeline_evolve_schema(payload: SchemaEvolvePayload) -> dict[str, Any]:
    return pipeline.evolve_schema(
        payload.version, payload.data_source, payload.field_defs, payload.backward_compatible
    )


@router.post("/full")
def pipeline_run_full(payload: FullPipelinePayload) -> dict[str, Any]:
    return pipeline.run_full_pipeline(
        payload.source_id, payload.time_window, payload.raw_logs, payload.parser_rule
    )


@router.get("/snapshot")
def pipeline_get_snapshot() -> dict[str, Any]:
    """获取所有模块的数据快照，用于跨模块数据流验证。"""
    from app.services.base import BaseModuleService
    from app.services import SERVICES
    snapshots: dict[str, Any] = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()
    return {"modules": snapshots}