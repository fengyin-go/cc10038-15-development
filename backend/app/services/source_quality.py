"""分来源质量校验：按数据源现行口径计算必填字段缺失率并生成可追溯的校验结论。

与旧的“字段集非空”校验不同，这里把阈值挂在数据源上（quality_check 规则），
每次重算只追加一条新结论，旧结论保留出报时的分母、阈值和缺失行。
聚合视图与导出只读取“最新结论”做来源准入，并把结论快照写入覆盖清单。
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from typing import Any

from app.store import store

CONCLUSION_MODULE = "quality_conclusion"
ALERT_MODULE = "quality_alert_check"

DEFAULT_THRESHOLD: dict[str, Any] = {
    "required_fields": [],
    "max_missing_rate": 0.0,
    "per_field": {},
    "min_records": 1,
}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _rate(missing: int, denominator: int) -> float:
    return round(missing / denominator, 4) if denominator else 0.0


def _pct(value: float) -> str:
    return f"{round(value * 100, 1)}%"


def _json_obj(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        loaded = json.loads(str(raw or ""))
    except (json.JSONDecodeError, TypeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    try:
        loaded = json.loads(str(raw or ""))
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(loaded, list):
        return [str(item).strip() for item in loaded if str(item).strip()]
    return []


def _normalize_threshold(raw: Any) -> dict[str, Any]:
    """把规则里的阈值参数解析成统一结构。

    支持三种写法：结构化 dict、JSON 文本、以及种子/表单里的可读文本
    （例如“必填 time、level；缺失率≤20%”）。解析失败时退回默认口径，
    保证旧规则不会让校验接口直接报错。
    """
    data: dict[str, Any] = {}
    if isinstance(raw, dict):
        data = dict(raw)
    elif isinstance(raw, str) and raw.strip().startswith("{"):
        data = _json_obj(raw)
    else:
        text = str(raw or "")
        percent = re.search(r"(?:≤|<=|max[^0-9]*)\s*([0-9]+(?:\.[0-9]+)?)\s*%", text)
        required = re.search(r"必填[：:\s]*([^;；]+)", text)
        if percent:
            data["max_missing_rate"] = _as_float(percent.group(1), 0.0) / 100
        if required:
            names = re.split(r"[、,，/\s]+", required.group(1))
            data["required_fields"] = [n for n in names if n and n not in ("字段", "：")]

    threshold = copy.deepcopy(DEFAULT_THRESHOLD)
    threshold["required_fields"] = [
        str(f).strip() for f in (data.get("required_fields") or []) if str(f).strip()
    ]
    threshold["max_missing_rate"] = max(0.0, _as_float(data.get("max_missing_rate"), 0.0))
    threshold["min_records"] = max(1, int(_as_float(data.get("min_records"), 1)))
    per_field = data.get("per_field") or {}
    if isinstance(per_field, dict):
        threshold["per_field"] = {
            str(field): max(0.0, _as_float(limit, threshold["max_missing_rate"]))
            for field, limit in per_field.items()
        }
    return threshold


def _threshold_text(threshold: dict[str, Any]) -> str:
    fields = "、".join(threshold.get("required_fields") or []) or "自动识别"
    return f"必填 {fields}；缺失率≤{_pct(threshold.get('max_missing_rate', 0.0))}"


def _threshold_signature(threshold: dict[str, Any]) -> str:
    raw = json.dumps(threshold, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _batch_map() -> dict[str, dict[str, Any]]:
    return {str(b.get("批次编号")): b for b in store.rows("ingest_batch")}


def entity_source(entity: dict[str, Any], batches: dict[str, dict[str, Any]] | None = None) -> str:
    """标准化记录归属的数据源：优先取解析时写入的 source_id，旧数据回查批次。"""
    source_id = str(entity.get("source_id") or "")
    if source_id:
        return source_id
    batches = batches or _batch_map()
    batch = batches.get(str(entity.get("来源批次") or ""))
    return str(batch.get("所属数据源") or "") if batch else ""


def source_entry(source_id: str) -> dict[str, Any] | None:
    for row in store.rows("source_registry"):
        if str(row.get("来源标识")) == source_id:
            return row
    return None


def source_region(source_id: str) -> str:
    row = source_entry(source_id)
    return str(row.get("采集区域") or "-") if row else "-"


def _source_ids_in_scope() -> list[str]:
    batches = _batch_map()
    sources = {
        entity_source(entity, batches)
        for entity in store.rows("parsed_entity")
    }
    return sorted(source for source in sources if source)


# ---------- 校验规则 ----------

def resolve_rule(
    source_id: str,
    format_version: str | None = None,
) -> dict[str, Any] | None:
    """为本数据源挑一条已生效规则：来源精确匹配优先，其次“全部”。"""
    active = [r for r in store.rows("quality_check") if r.get("status") == "已生效"]
    exact = [r for r in active if str(r.get("适用数据源") or "") == source_id]
    shared = [
        r for r in active
        if str(r.get("适用数据源") or "") in ("全部", "全部数据源", "*", "")
    ]
    candidates = exact or shared
    if format_version:
        matched = [r for r in candidates if str(r.get("格式版本") or "") == format_version]
        candidates = matched or candidates
    if not candidates:
        return None
    return max(candidates, key=lambda r: (int(r.get("规则版本", 1) or 1), int(r.get("id", 0))))


def rule_threshold(rule: dict[str, Any] | None) -> dict[str, Any]:
    if not rule:
        return copy.deepcopy(DEFAULT_THRESHOLD)
    if isinstance(rule.get("threshold"), dict):
        return _normalize_threshold(rule["threshold"])
    return _normalize_threshold(rule.get("阈值参数"))


def configure_rule(
    source_id: str,
    required_fields: list[str],
    max_missing_rate: float = 0.0,
    *,
    per_field: dict[str, float] | None = None,
    severity: str = "高",
    rule_name: str | None = None,
    format_version: str | None = None,
) -> dict[str, Any]:
    """为数据源登记/更新“本处现行口径”，规则版本递增，旧结论不动。"""
    threshold = _normalize_threshold({
        "required_fields": required_fields,
        "max_missing_rate": max_missing_rate,
        "per_field": per_field or {},
    })
    rows = store.rows("quality_check")
    existing = next(
        (
            row for row in rows
            if str(row.get("适用数据源") or "") == source_id
            and row.get("status") == "已生效"
        ),
        None,
    )
    version = int(existing.get("规则版本", 1)) + 1 if existing else 1
    name = rule_name or f"{source_region(source_id)}-班报必填字段完整性"
    values = {
        "规则名称": name,
        "校验类型": "必填字段缺失率",
        "阈值参数": _threshold_text(threshold),
        "适用数据源": source_id,
        "严重级别": severity,
        "status": "已生效",
        "pending": False,
        "abnormal": False,
        "规则版本": version,
        "threshold": threshold,
        "口径签名": _threshold_signature(threshold),
        "格式版本": format_version or "",
        "updated_at": _now_iso(),
    }
    if existing is None:
        existing = {"id": _next_id("quality_check")}
        rows.append(existing)
    existing.update(values)
    return existing


# ---------- 缺失率计算 ----------

def _scope_entities(source_id: str, batch_no: str | None, scope: str) -> tuple[list[dict[str, Any]], str]:
    batches = _batch_map()
    entities = [
        entity for entity in store.rows("parsed_entity")
        if entity_source(entity, batches) == source_id
    ]
    if batch_no:
        return [e for e in entities if str(e.get("来源批次")) == batch_no], batch_no

    entity_batches = {str(e.get("来源批次")) for e in entities}
    batches_for_source = [
        b for b in store.rows("ingest_batch")
        if str(b.get("所属数据源")) == source_id and str(b.get("批次编号")) in entity_batches
    ]
    if scope == "all" or not batches_for_source:
        return entities, "(全部批次)"
    latest = max(batches_for_source, key=lambda b: int(b.get("id", 0)))
    latest_no = str(latest.get("批次编号"))
    return [e for e in entities if str(e.get("来源批次")) == latest_no], latest_no


def _required_fields(
    entities: list[dict[str, Any]],
    rule: dict[str, Any] | None,
    threshold: dict[str, Any],
) -> list[str]:
    if threshold.get("required_fields"):
        return list(threshold["required_fields"])

    versions = {str(e.get("schema_version") or "") for e in entities if e.get("schema_version")}
    for schema in reversed(store.rows("schema_version")):
        if str(schema.get("版本号")) in versions:
            fields = _json_list(schema.get("字段定义"))
            if fields:
                return fields

    if rule:
        fields = _json_list(rule.get("输出字段列表"))
        if fields and fields[0] not in ("字段解析规则样例",):
            return fields

    union: list[str] = []
    for entity in entities:
        for field in _json_obj(entity.get("标准化字段集")):
            if field not in union:
                union.append(field)
    return union


def _format_meta(entities: list[dict[str, Any]], rule: dict[str, Any] | None) -> tuple[str, str]:
    versions = sorted({
        str(e.get("schema_version") or e.get("格式版本") or "unknown")
        for e in entities
    })
    format_version = "、".join(versions) if versions else "unknown"
    format_name = str((rule or {}).get("格式名称") or "")
    for adapter in reversed(store.rows("format_adapter")):
        if adapter.get("status") != "已启用":
            continue
        adapter_version = str(adapter.get("版本") or "")
        if not versions or adapter_version in versions:
            if not format_name:
                format_name = str(adapter.get("输入格式名") or "")
            break
    if not format_name and rule:
        format_name = str(rule.get("输入格式") or "")
    return format_version, format_name


def evaluate_source_quality(
    source_id: str,
    *,
    batch_no: str | None = None,
    scope: str = "latest",
    note: str | None = None,
) -> dict[str, Any]:
    """按本数据源现行口径重算缺失率，追加一条不可变结论。"""
    source = source_entry(source_id)
    if source is None:
        return {"ok": False, "message": f"数据源 {source_id} 未登记"}

    entities, resolved_batch = _scope_entities(source_id, batch_no, scope)
    rule = resolve_rule(source_id)
    threshold = rule_threshold(rule)
    format_version, format_name = _format_meta(entities, rule)
    required = _required_fields(entities, rule, threshold)

    denominator = len(entities)
    stats: dict[str, dict[str, Any]] = {
        field: {"缺失数": 0, "缺失率": 0.0} for field in required
    }
    missing_rows: list[dict[str, Any]] = []
    raw_by_id = {str(r.get("id")): r for r in store.rows("raw_records")}
    if denominator:
        for entity in entities:
            fields = _json_obj(entity.get("标准化字段集"))
            missing = [
                field for field in required
                if not str(fields.get(field) or "").strip()
            ]
            if not missing:
                continue
            for field in missing:
                stats[field]["缺失数"] += 1
            if len(missing_rows) < 200:
                raw = raw_by_id.get(str(entity.get("原始记录ID")))
                missing_rows.append({
                    "标准化记录ID": entity.get("id"),
                    "原始记录ID": entity.get("原始记录ID"),
                    "来源批次": entity.get("来源批次"),
                    "缺失字段": missing,
                    "缺失值": {field: fields.get(field, "") for field in missing},
                    "原始正文摘录": str((raw or {}).get("原始正文") or "")[:160],
                })

    for field in required:
        stats[field]["缺失率"] = _rate(stats[field]["缺失数"], denominator)

    triggered: list[dict[str, Any]] = []
    broken_fields: list[str] = []
    for field in required:
        limit = threshold["per_field"].get(field, threshold["max_missing_rate"])
        actual = stats[field]["缺失率"]
        if denominator and actual > limit + 1e-9:
            broken_fields.append(field)
            triggered.append({
                "字段": field,
                "阈值": limit,
                "实测": actual,
                "缺失数": stats[field]["缺失数"],
                "分母": denominator,
            })

    if denominator == 0:
        verdict = "无样本"
        qualified = False
        reason = "本数据源没有可评估的标准化记录"
    elif denominator < threshold["min_records"]:
        verdict = "样本不足"
        qualified = False
        reason = f"样本数 {denominator} 小于最小样本 {threshold['min_records']}"
    elif broken_fields:
        verdict = "越线"
        qualified = False
        reason = "；".join(
            f"字段 {item['字段']} 缺失率 {_pct(item['实测'])} > 阈值 {_pct(item['阈值'])}"
            for item in triggered
        )
    else:
        verdict = "合格"
        qualified = True
        reason = "全部必填字段缺失率未超过本处阈值"

    rule_name = str(rule.get("规则名称")) if rule else "默认口径（未配置来源规则）"
    rule_version = int(rule.get("规则版本", 1)) if rule else 0
    basis = (
        f"规则「{rule_name}」v{rule_version}｜适用来源 {source_id}（{source_region(source_id)}）"
        f"｜阈值 {_threshold_text(threshold)}｜格式 {format_name or '-'} / {format_version}"
    )

    rows = store.rows(CONCLUSION_MODULE)
    conclusion_id = _next_id(CONCLUSION_MODULE)
    versions_for_source = [
        int(row.get("结论版本", 1))
        for row in rows
        if row.get("来源标识") == source_id and row.get("批次编号") == resolved_batch
    ]
    total_missing = sum(item["缺失数"] for item in stats.values())
    conclusion = {
        "id": conclusion_id,
        "结论编号": f"QC-{conclusion_id:04d}",
        "status": verdict,
        "pending": False,
        "abnormal": not qualified,
        "来源标识": source_id,
        "采集区域": source_region(source_id),
        "批次编号": resolved_batch,
        "数据范围": scope if not batch_no else "指定批次",
        "结论版本": max(versions_for_source, default=0) + 1,
        "格式版本": format_version,
        "格式名称": format_name,
        "解析规则版本": "、".join(sorted({
            str(entity.get("解析规则版本") or "unknown") for entity in entities
        })) or "unknown",
        "规则ID": rule.get("id") if rule else None,
        "规则名称": rule_name,
        "规则版本": rule_version,
        "口径版本": f"{source_id}@r{rule_version}",
        "口径签名": _threshold_signature(threshold),
        "判定依据": basis,
        "必填字段": required,
        "样本数": denominator,
        "分母": denominator,
        "缺失统计": stats,
        "缺失总数": total_missing,
        "总缺失率": _rate(total_missing, denominator * len(required)) if required else 0.0,
        "阈值": threshold,
        "触发阈值": triggered,
        "越线字段": broken_fields,
        "是否合格": qualified,
        "结论": verdict,
        "结论说明": reason,
        "缺失行": missing_rows,
        "评估时间": _now_iso(),
        "备注": note or "",
    }
    rows.append(conclusion)

    # 标准化明细只记录“最近一次结论”，历史结论仍按结论编号保留完整快照。
    for entity in entities:
        entity["质量结论编号"] = conclusion["结论编号"]
        entity["来源校验结论"] = verdict
        entity["来源缺失字段"] = broken_fields

    return {"ok": True, "message": f"{source_id} 已按现行口径重算", "conclusion": conclusion}


def latest_conclusion(
    source_id: str,
    batch_no: str | None = None,
) -> dict[str, Any] | None:
    rows = [
        row for row in store.rows(CONCLUSION_MODULE)
        if str(row.get("来源标识")) == source_id
        and (batch_no is None or str(row.get("批次编号")) == batch_no)
    ]
    return max(rows, key=lambda row: int(row.get("id", 0))) if rows else None


def get_conclusion(identifier: str | int) -> dict[str, Any] | None:
    for row in store.rows(CONCLUSION_MODULE):
        if str(row.get("id")) == str(identifier) or str(row.get("结论编号")) == str(identifier):
            return row
    return None


def conclusion_summary(conclusion: dict[str, Any] | None) -> dict[str, Any] | None:
    if conclusion is None:
        return None
    return {
        "结论编号": conclusion.get("结论编号"),
        "id": conclusion.get("id"),
        "来源标识": conclusion.get("来源标识"),
        "采集区域": conclusion.get("采集区域"),
        "批次编号": conclusion.get("批次编号"),
        "结论版本": conclusion.get("结论版本"),
        "格式版本": conclusion.get("格式版本"),
        "格式名称": conclusion.get("格式名称"),
        "规则名称": conclusion.get("规则名称"),
        "规则版本": conclusion.get("规则版本"),
        "规则ID": conclusion.get("规则ID"),
        "口径版本": conclusion.get("口径版本"),
        "判定依据": conclusion.get("判定依据"),
        "必填字段": conclusion.get("必填字段"),
        "样本数": conclusion.get("样本数"),
        "分母": conclusion.get("分母"),
        "缺失统计": conclusion.get("缺失统计"),
        "阈值": conclusion.get("阈值"),
        "触发阈值": conclusion.get("触发阈值"),
        "越线字段": conclusion.get("越线字段"),
        "是否合格": conclusion.get("是否合格"),
        "结论": conclusion.get("结论"),
        "结论说明": conclusion.get("结论说明"),
        "缺失行": conclusion.get("缺失行", []),
        "评估时间": conclusion.get("评估时间"),
    }


def conclusion_snapshot(conclusion: dict[str, Any]) -> dict[str, Any]:
    """覆盖清单里保存的结论快照：分母、阈值、缺失行在出报时冻结。"""
    snapshot = conclusion_summary(conclusion) or {}
    snapshot["口径版本"] = conclusion.get("口径版本")
    snapshot["规则名称"] = conclusion.get("规则名称")
    snapshot["规则版本"] = conclusion.get("规则版本")
    snapshot["参数指纹"] = conclusion.get("口径签名")
    return copy.deepcopy(snapshot)


# ---------- 覆盖清单 ----------

def _empty_coverage(view_name: str, coverage_version: int) -> dict[str, Any]:
    return {
        "视图名称": view_name,
        "覆盖版本": coverage_version,
        "生成时间": _now_iso(),
        "数据源范围": [],
        "来源总数": 0,
        "计入来源数": 0,
        "未计入来源数": 0,
        "覆盖完整": True,
        "口径": "按数据源最新校验结论准入，仅统计校验通过日志",
        "计入来源": [],
        "未计入来源": [],
    }


def _base_coverage_entry(source_id: str, conclusion: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "来源标识": source_id,
        "采集区域": source_region(source_id),
        "结论编号": conclusion.get("结论编号") if conclusion else None,
        "格式版本": conclusion.get("格式版本") if conclusion else "-",
        "判定依据": conclusion.get("判定依据") if conclusion else "尚未生成本来源校验结论",
        "样本数": conclusion.get("样本数") if conclusion else 0,
        "阈值": conclusion.get("阈值") if conclusion else None,
        "缺失统计": conclusion.get("缺失统计") if conclusion else {},
    }


def build_rejected_coverage(
    view_name: str,
    source_ids: list[str],
    total_logs: dict[str, int],
    qualified_logs: dict[str, int],
    coverage_version: int = 1,
) -> dict[str, Any]:
    """严格准入：最新结论不合格或缺失的来源全部排除，并写明原因。"""
    coverage = _empty_coverage(view_name, coverage_version)
    coverage["数据源范围"] = sorted(source_ids)
    for source_id in sorted(source_ids):
        conclusion = latest_conclusion(source_id)
        entry = _base_coverage_entry(source_id, conclusion)
        if conclusion is None:
            entry.update({
                "日志数": total_logs.get(source_id, 0),
                "未计入日志数": total_logs.get(source_id, 0),
                "原因": "缺少校验结论（未按本处口径核对）",
            })
            coverage["未计入来源"].append(entry)
            continue
        if not conclusion.get("是否合格"):
            reason = f"校验{conclusion.get('结论')}"
            if conclusion.get("越线字段"):
                reason += f"：字段 {'、'.join(conclusion['越线字段'])} 缺失率超出阈值"
            elif conclusion.get("结论") == "无样本":
                reason += "：没有可评估样本"
            entry.update({
                "日志数": total_logs.get(source_id, 0),
                "未计入日志数": total_logs.get(source_id, 0),
                "原因": reason,
                "触发阈值": conclusion.get("触发阈值", []),
                "缺失行": copy.deepcopy(conclusion.get("缺失行", [])),
                "结论快照": conclusion_snapshot(conclusion),
            })
            coverage["未计入来源"].append(entry)
            continue
        entry.update({
            "日志数": total_logs.get(source_id, 0),
            "合格日志数": qualified_logs.get(source_id, 0),
            "记录缺口": total_logs.get(source_id, 0) - qualified_logs.get(source_id, 0),
            "结论快照": conclusion_snapshot(conclusion),
        })
        coverage["计入来源"].append(entry)

    coverage["来源总数"] = len(source_ids)
    coverage["计入来源数"] = len(coverage["计入来源"])
    coverage["未计入来源数"] = len(coverage["未计入来源"])
    coverage["覆盖完整"] = coverage["未计入来源数"] == 0
    return coverage


def build_permissive_coverage(
    view_name: str,
    source_ids: list[str],
    total_logs: dict[str, int],
    qualified_logs: dict[str, int],
    coverage_version: int = 1,
) -> dict[str, Any]:
    """历史视图的兼容口径：不启用来源准入，所有来源计入但保留结论引用。"""
    coverage = _empty_coverage(view_name, coverage_version)
    coverage["口径"] = "未启用来源准入（历史视图），全部来源计入"
    coverage["数据源范围"] = sorted(source_ids)
    for source_id in sorted(source_ids):
        conclusion = latest_conclusion(source_id)
        entry = _base_coverage_entry(source_id, conclusion)
        entry.update({
            "日志数": total_logs.get(source_id, 0),
            "合格日志数": qualified_logs.get(source_id, 0),
            "结论快照": conclusion_snapshot(conclusion) if conclusion else None,
        })
        coverage["计入来源"].append(entry)
    coverage["来源总数"] = len(source_ids)
    coverage["计入来源数"] = len(coverage["计入来源"])
    coverage["覆盖完整"] = False
    return coverage


def writeback_coverage(coverage: dict[str, Any], view_name: str) -> list[dict[str, Any]]:
    """把发布结果的来源覆盖情况回写到数据源登记，便于报告使用方核对。"""
    written: list[dict[str, Any]] = []
    for key, status in (("计入来源", "已计入"), ("未计入来源", "未计入")):
        for entry in coverage.get(key, []):
            source = source_entry(str(entry.get("来源标识")))
            if source is None:
                continue
            source["覆盖状态"] = status
            source["覆盖清单版本"] = f"{view_name}@{coverage.get('覆盖版本', 1)}"
            source["覆盖结论编号"] = entry.get("结论编号")
            source["覆盖更新时间"] = coverage.get("生成时间") or _now_iso()
            if status == "未计入":
                source["未计入原因"] = entry.get("原因")
            else:
                source.pop("未计入原因", None)
            written.append({
                "来源标识": source.get("来源标识"),
                "覆盖状态": status,
                "覆盖清单版本": source.get("覆盖清单版本"),
                "结论编号": source.get("覆盖结论编号"),
            })
    return written


# ---------- 告警核对 ----------

def run_alert_check(source_id: str, batch_no: str | None = None) -> dict[str, Any]:
    """按数据源做告警核对；南区可独立先行核对，不受跨区班报缺口影响。"""
    conclusion = latest_conclusion(source_id, batch_no)
    if conclusion is None:
        return {
            "ok": False,
            "message": f"{source_id} 尚无校验结论，请先重算缺失率",
            "source_id": source_id,
        }

    alerts: list[dict[str, Any]] = []
    for item in conclusion.get("触发阈值", []):
        alerts.append({
            "级别": "严重" if item["实测"] - item["阈值"] >= 0.2 else "警告",
            "字段": item["字段"],
            "内容": (
                f"{source_region(source_id)} {source_id} 字段 {item['字段']} "
                f"缺失率 {_pct(item['实测'])} 超过阈值 {_pct(item['阈值'])}"
                f"（{item['缺失数']}/{item['分母']}）"
            ),
            "阈值": item["阈值"],
            "实测": item["实测"],
            "分母": item["分母"],
        })
    if not alerts and conclusion.get("结论") in ("无样本", "样本不足"):
        alerts.append({
            "级别": "警告",
            "字段": "-",
            "内容": f"{source_id} {conclusion.get('结论说明')}",
            "阈值": None,
            "实测": None,
            "分母": conclusion.get("分母", 0),
        })

    result = {
        "id": _next_id(ALERT_MODULE),
        "来源标识": source_id,
        "采集区域": source_region(source_id),
        "结论编号": conclusion.get("结论编号"),
        "核对结果": "通过" if not alerts else "告警",
        "告警条数": len(alerts),
        "告警明细": alerts,
        "核对时间": _now_iso(),
        "status": "通过" if not alerts else "告警",
        "pending": False,
        "abnormal": bool(alerts),
    }
    store.rows(ALERT_MODULE).append(result)
    return {
        "ok": True,
        "message": (
            f"{source_region(source_id)}告警核对完成："
            f"{'未触发告警' if not alerts else f'触发 {len(alerts)} 条告警'}"
        ),
        "source_id": source_id,
        "conclusion_id": conclusion.get("结论编号"),
        "passed": not alerts,
        "alerts": alerts,
        "checked_at": result["核对时间"],
    }


def latest_alert_check(source_id: str) -> dict[str, Any] | None:
    rows = [r for r in store.rows(ALERT_MODULE) if r.get("来源标识") == source_id]
    return max(rows, key=lambda row: int(row.get("id", 0))) if rows else None


# ---------- 控制台汇总 ----------

def overview(view_name: str = "班报-跨区") -> dict[str, Any]:
    source_ids = _source_ids_in_scope()
    sources: list[dict[str, Any]] = []
    for source_id in source_ids:
        entry = source_entry(source_id) or {}
        conclusion = latest_conclusion(source_id)
        rule = resolve_rule(source_id)
        alert = latest_alert_check(source_id)
        sources.append({
            "来源标识": source_id,
            "采集区域": source_region(source_id),
            "接入协议": entry.get("接入协议"),
            "源状态": entry.get("status"),
            "覆盖状态": entry.get("覆盖状态"),
            "覆盖清单版本": entry.get("覆盖清单版本"),
            "规则": {
                "id": rule.get("id"),
                "规则名称": rule.get("规则名称"),
                "规则版本": rule.get("规则版本"),
                "阈值参数": rule.get("阈值参数"),
                "阈值": rule_threshold(rule),
            } if rule else None,
            "最新结论": conclusion_summary(conclusion),
            "告警核对": {
                "核对结果": alert.get("核对结果"),
                "核对时间": alert.get("核对时间"),
                "结论编号": alert.get("结论编号"),
            } if alert else None,
        })

    views = [v for v in store.rows("aggregation_view") if v.get("视图名称") == view_name]
    latest_view = max(views, key=lambda row: int(row.get("id", 0))) if views else None
    coverage = latest_view.get("来源覆盖清单") if latest_view else None

    history = [
        conclusion_summary(row)
        for row in sorted(
            store.rows(CONCLUSION_MODULE),
            key=lambda row: int(row.get("id", 0)),
            reverse=True,
        )
    ]
    checks = [
        {
            "来源标识": row.get("来源标识"),
            "采集区域": row.get("采集区域"),
            "结论编号": row.get("结论编号"),
            "核对结果": row.get("核对结果"),
            "告警条数": row.get("告警条数"),
            "核对时间": row.get("核对时间"),
        }
        for row in sorted(
            store.rows(ALERT_MODULE),
            key=lambda row: int(row.get("id", 0)),
            reverse=True,
        )[:10]
    ]
    return {
        "view_name": view_name,
        "sources": sources,
        "view": {
            "id": latest_view.get("id"),
            "视图名称": latest_view.get("视图名称"),
            "status": latest_view.get("status"),
            "published": latest_view.get("published", False),
            "published_at": latest_view.get("published_at"),
            "合格日志数": latest_view.get("合格日志数"),
            "计入来源数": latest_view.get("计入来源数"),
            "未计入来源数": latest_view.get("未计入来源数"),
            "覆盖版本": latest_view.get("覆盖版本"),
            "覆盖完整": (coverage or {}).get("覆盖完整"),
            "coverage": coverage,
        } if latest_view else None,
        "history": history,
        "alert_checks": checks,
        "summary": {
            "来源数": len(sources),
            "合格来源": sum(1 for s in sources if (s["最新结论"] or {}).get("是否合格")),
            "越线来源": sum(
                1 for s in sources
                if s["最新结论"] and not s["最新结论"].get("是否合格")
            ),
            "未核对来源": sum(1 for s in sources if not s["最新结论"]),
        },
    }
