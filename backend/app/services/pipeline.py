"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any

from app.services import source_quality
from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 来源专属规则优先：同一来源登记了多条时取最新发布的那条。
        batches_now = store.rows("ingest_batch")
        batch_source = next(
            (b.get("所属数据源") for b in batches_now if b.get("批次编号") == batch_no),
            "",
        )
        if parser_rule_name is None and batch_source:
            applicable = [
                p for p in parsers
                if p.get("status") == "已发布"
                and (
                    str(p.get("适用数据源") or "") == batch_source
                    or batch_source in str(p.get("适用数据源") or "")
                )
            ]
            if applicable:
                rule = max(applicable, key=lambda p: int(p.get("id", 0)))

        # 获取当前 schema_version：优先选本批次来源适用的已发布版本
        schema_versions = store.rows("schema_version")
        batch_source = next(
            (b.get("所属数据源") for b in store.rows("ingest_batch")
             if b.get("批次编号") == batch_no),
            "",
        )
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") != "已发布":
                continue
            applies = str(sv.get("适用数据源") or "")
            if batch_source and (batch_source in applies or "全部" in applies):
                schema_ver = sv
                break
        if schema_ver is None:
            for sv in schema_versions:
                if sv.get("status") == "已发布":
                    schema_ver = sv
                    break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "source_id": raw.get("source_id") or "",
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "format_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重：只处理当前已按来源口径判定通过的记录。
        # 来源整体越线的记录即使单条字段完整，也不进入去重结果集。
        batches = {str(b.get("批次编号")): b for b in store.rows("ingest_batch")}
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            rec_source = source_quality.entity_source(rec, batches)
            latest = source_quality.latest_conclusion(rec_source) if rec_source else None
            if latest is not None and not latest.get("是否合格"):
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(
        self,
        batch_no: str | None = None,
        source_id: str | None = None,
        scope: str = "latest",
        note: str | None = None,
    ) -> dict[str, Any]:
        """按数据源现行口径校验标准化明细。

        先为涉及的每个数据源重算缺失率并追加一条分来源结论，再逐条记录
        按该来源的必填字段判定是否通过；来源整体越线的记录仍会保留
        自己的状态，但在聚合/导出阶段按结论排除。
        """
        parsed = store.rows("parsed_entity")
        batches = {str(b.get("批次编号")): b for b in store.rows("ingest_batch")}
        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if str(p.get("来源批次")) == batch_no]
        if source_id:
            target = [
                p for p in target
                if source_quality.entity_source(p, batches) == source_id
            ]

        involved: list[str] = []
        for entity in target:
            sid = source_quality.entity_source(entity, batches)
            if sid and sid not in involved:
                involved.append(sid)

        # 批次已解析但没有待校验记录时，仍然刷新一次来源结论。
        if not involved and batch_no:
            fallback = source_quality.entity_source(
                next((p for p in parsed if str(p.get("来源批次")) == batch_no), {}),
                batches,
            )
            if fallback:
                involved.append(fallback)
        if source_id and source_id not in involved:
            involved.append(source_id)

        conclusions: dict[str, dict[str, Any]] = {}
        for sid in involved:
            # 指定批次时按该批次口径重算，避免把历史上已合格的结论覆盖掉。
            outcome = source_quality.evaluate_source_quality(
                sid,
                batch_no=batch_no if batch_no else None,
                scope="batch" if batch_no else scope,
                note=note,
            )
            if outcome.get("ok"):
                conclusions[sid] = outcome["conclusion"]

        passed = 0
        failed = 0
        for entity in target:
            sid = source_quality.entity_source(entity, batches)
            conclusion = conclusions.get(sid)
            required = (
                conclusion.get("必填字段", [])
                if conclusion
                else source_quality.rule_threshold(
                    source_quality.resolve_rule(sid)
                )["required_fields"]
            )
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            if not isinstance(fields_data, dict) or not fields_data:
                is_valid, missing = False, ["标准化字段集不可解析"]
            elif not required:
                is_valid = all(str(v).strip() for v in fields_data.values())
                missing = [
                    key for key, value in fields_data.items()
                    if not str(value or "").strip()
                ]
            else:
                missing = [
                    field for field in required
                    if not str(fields_data.get(field) or "").strip()
                ]
                is_valid = not missing

            entity["来源标识"] = sid
            if conclusion:
                entity["结论编号"] = conclusion.get("结论编号")
                entity["来源校验结论"] = conclusion.get("结论")
                entity["来源判定依据"] = conclusion.get("判定依据")
            entity["缺失字段"] = missing

            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["abnormal"] = False
                entity["validated_at"] = _now_iso()
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["pending"] = False
                entity["abnormal"] = True
                entity["fail_reason"] = (
                    f"必填字段缺失：{'、'.join(missing)}" if required else "字段集为空"
                )
                failed += 1

        return {
            "ok": True,
            "message": "校验完成",
            "passed": passed,
            "failed": failed,
            "sources": list(conclusions),
            "conclusions": {
                sid: {
                    "结论编号": item.get("结论编号"),
                    "结论": item.get("结论"),
                    "是否合格": item.get("是否合格"),
                    "缺失统计": item.get("缺失统计"),
                    "触发阈值": item.get("触发阈值"),
                    "判定依据": item.get("判定依据"),
                }
                for sid, item in conclusions.items()
            },
            "quality_rule": ", ".join(
                str(item.get("规则名称")) for item in conclusions.values()
            ) or None,
            "per_source_threshold": True,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
        enforce_quality: bool = True,
        filters: dict[str, str] | None = None,
        report_scope: str | None = None,
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。

        读取每个数据源的最新分来源结论，只有结论合格且记录自身校验通过的
        日志才计入；未计入的来源写进来源覆盖清单并回写数据源登记。
        历史视图可用 ``enforce_quality=False`` 保留旧的“全部通过记录”口径。
        """
        parsed = store.rows("parsed_entity")
        batches = {str(b.get("批次编号")): b for b in store.rows("ingest_batch")}
        results: dict[str, dict[str, Any]] = {}

        # 视图名与班报范围同名时自动按班报范围取数。
        if report_scope is None and any(
            str(e.get("报表范围")) == view_name for e in parsed
        ):
            report_scope = view_name

        # 聚合范围：显式过滤条件 > 本来源最新结论对应的批次 > 全部记录。
        filters = {k: str(v) for k, v in (filters or {}).items() if v}
        filter_sources = {filters["source_id"]} if filters.get("source_id") else set()
        filter_batches = {filters["batch_no"]} if filters.get("batch_no") else set()
        in_scope = [
            e for e in parsed
            if (not filter_sources or source_quality.entity_source(e, batches) in filter_sources)
            and (not filter_batches or str(e.get("来源批次")) in filter_batches)
            and (str(e.get("报表范围")) == report_scope if report_scope
                 else not e.get("报表范围"))
        ]
        scoped_sources = sorted({
            sid for sid in (source_quality.entity_source(e, batches) for e in in_scope) if sid
        })

        if report_scope and not filter_sources and not filter_batches:
            # 班报有明确范围时，各来源取该范围内自己最新的一次结论批次；
            # 如果最新结论来自班报之外的新样本，则退回该来源在班报内的最新批次，
            # 不会把未纳入班报的样本混进本期数据。
            agg_scope = f"{report_scope}（按各来源最新结论批次）"
            kept: list[dict[str, Any]] = []
            for source_id in scoped_sources:
                conclusion = source_quality.latest_conclusion(source_id)
                latest_batch = str((conclusion or {}).get("批次编号") or "")
                batches_for_source = {
                    str(e.get("来源批次"))
                    for e in store.rows("parsed_entity")
                    if source_quality.entity_source(e, batches) == source_id
                    and str(e.get("报表范围")) == report_scope
                }
                if latest_batch in batches_for_source:
                    target_batch = latest_batch
                else:
                    ids = {
                        bid: int(batches.get(bid, {}).get("id", 0))
                        for bid in batches_for_source
                    }
                    target_batch = max(ids, key=lambda key: ids[key]) if ids else ""
                kept.extend(
                    e for e in in_scope
                    if source_quality.entity_source(e, batches) == source_id
                    and str(e.get("来源批次")) == target_batch
                )
            in_scope = kept
        elif not filter_sources and not filter_batches:
            # 未指定班报范围时退回“最近一个批次”，避免把历史批次重复计入。
            batch_ids = {
                str(e.get("来源批次")): int(batches.get(str(e.get("来源批次")), {}).get("id", 0))
                for e in in_scope if e.get("来源批次")
            }
            if batch_ids:
                latest = max(batch_ids, key=lambda key: batch_ids[key])
                in_scope = [e for e in in_scope if str(e.get("来源批次")) == latest]
                agg_scope = latest
        elif filter_batches:
            agg_scope = "、".join(sorted(filter_batches))

        source_ids = sorted({
            sid for sid in (source_quality.entity_source(e, batches) for e in in_scope) if sid
        })
        qualified_by_source: dict[str, list[dict[str, Any]]] = {}
        total_logs: dict[str, int] = {}
        qualified_counts: dict[str, int] = {}
        rejected_records: list[dict[str, Any]] = []

        for entity in in_scope:
            sid = source_quality.entity_source(entity, batches)
            if not sid:
                continue
            total_logs[sid] = total_logs.get(sid, 0) + 1
            conclusion = source_quality.latest_conclusion(sid)
            source_ok = bool(conclusion and conclusion.get("是否合格"))
            if not enforce_quality:
                source_ok = True
            if source_ok and entity.get("status") == "校验通过":
                qualified_by_source.setdefault(sid, []).append(entity)
                qualified_counts[sid] = qualified_counts.get(sid, 0) + 1
            elif enforce_quality:
                rejected_records.append({
                    "记录ID": entity.get("id"),
                    "来源标识": sid,
                    "来源批次": entity.get("来源批次"),
                    "结论编号": (conclusion or {}).get("结论编号"),
                    "拒绝原因": (
                        "来源最新结论不合格"
                        if not source_ok
                        else "记录自身校验未通过"
                    ),
                })

        for entity in (e for rows in qualified_by_source.values() for e in rows):
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(fields_data, dict):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        previous_versions = [
            int(a.get("覆盖版本", 0)) for a in aggs if a.get("视图名称") == view_name
        ]
        coverage_version = max(previous_versions, default=0) + 1
        if enforce_quality:
            coverage = source_quality.build_rejected_coverage(
                view_name, source_ids, total_logs, qualified_counts, coverage_version,
            )
        else:
            coverage = source_quality.build_permissive_coverage(
                view_name, source_ids, total_logs, qualified_counts, coverage_version,
            )

        qualified_total = sum(qualified_counts.values())
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": coverage["未计入来源数"] > 0 and enforce_quality,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(in_scope),
            "聚合范围": agg_scope,
            "enforce_quality": enforce_quality,
            "合格日志数": qualified_total,
            "计入来源数": coverage["计入来源数"],
            "未计入来源数": coverage["未计入来源数"],
            "覆盖版本": coverage_version,
            "来源覆盖清单": coverage,
            "来源覆盖清单引用": f"{view_name}@v{coverage_version}",
            "排除说明": coverage.get("口径"),
            "含未计入来源": coverage["未计入来源数"] > 0 and enforce_quality,
            "排除记录": rejected_records[:200],
        }
        aggs.append(agg_entry)
        coverage_writes = source_quality.writeback_coverage(coverage, view_name)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
            "qualified_logs": qualified_total,
            "coverage_version": coverage_version,
            "coverage_complete": coverage["覆盖完整"],
            "excluded_sources": [
                {
                    "来源标识": item.get("来源标识"),
                    "采集区域": item.get("采集区域"),
                    "结论编号": item.get("结论编号"),
                    "原因": item.get("原因"),
                    "触发阈值": item.get("触发阈值"),
                }
                for item in coverage["未计入来源"]
            ],
            "coverage_writes": coverage_writes,
        }

    # ---------- 阶段5b：样本核对 ----------
    def run_quality_sample(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """采集一份样本并立即按本来源现行口径核对。

        每个数据源独立重算，不合并到同一分母，便于分区域分别判断是否越线。
        """
        source = source_quality.source_entry(source_id)
        if source is None:
            self.register_source(source_id, "http", "default")
        if source_quality.source_entry(source_id).get("status") != "已验证":
            self.validate_source(source_id)

        batches = store.rows("ingest_batch")
        active = [
            b for b in batches
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中")
        ]
        for batch in active:
            # 同一来源的旧样本批次直接结束，避免把两次样本混在一个分母里。
            batch["status"] = "已完成"
            batch["pending"] = False

        # 该来源若已纳入某份班报，新样本沿用同一报表范围，补齐后可直接重算班报组成。
        report_scopes = {
            str(entity.get("报表范围"))
            for entity in store.rows("parsed_entity")
            if source_quality.entity_source(entity) == source_id and entity.get("报表范围")
        }

        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return batch_result
        batch_no = batch_result["batch"]["批次编号"]
        parse_result = self.parse_batch(batch_no, parser_rule)
        if not parse_result.get("ok"):
            return parse_result
        if report_scopes:
            for entity in store.rows("parsed_entity"):
                if str(entity.get("来源批次")) == batch_no and not entity.get("报表范围"):
                    entity["报表范围"] = next(iter(report_scopes)) if len(report_scopes) == 1 else sorted(report_scopes)[0]

        # 结论重算后立刻按同一口径落每条记录的校验状态，
        # 让聚合/导出只需同时看“来源结论 + 记录状态”即可。
        self.validate_entities(batch_no=batch_no, note=note)
        conclusion = source_quality.latest_conclusion(source_id, batch_no)
        verdict = {"ok": True, "conclusion": conclusion} if conclusion else {
            "ok": False, "message": "样本核对未产出结论",
        }
        conclusion = verdict.get("conclusion") if verdict.get("ok") else None
        return {
            "ok": bool(conclusion),
            "message": (
                f"{source_id} 样本核对完成：{conclusion.get('结论')}"
                if conclusion else verdict.get("message", "样本核对失败")
            ),
            "source_id": source_id,
            "batch_no": batch_no,
            "parse": {
                "success": parse_result.get("success"),
                "failure": parse_result.get("failure"),
            },
            "conclusion": source_quality.conclusion_summary(conclusion) if conclusion else None,
            "missing_rows": conclusion.get("缺失行", []) if conclusion else [],
            "triggered": conclusion.get("触发阈值", []) if conclusion else [],
        }

    # ---------- 阶段5c：发布结果 ----------
    def publish_aggregation(self, view_name: str) -> dict[str, Any]:
        """发布聚合结果：冻结来源覆盖清单，回写数据源登记，生成报告索引。"""
        views = [
            row for row in store.rows("aggregation_view")
            if row.get("视图名称") == view_name
        ]
        if not views:
            return {"ok": False, "message": f"聚合视图 {view_name} 不存在"}
        view = max(views, key=lambda row: int(row.get("id", 0)))
        coverage = view.get("来源覆盖清单")
        if not coverage:
            return {"ok": False, "message": f"聚合视图 {view_name} 尚未生成来源覆盖清单"}

        published_at = _now_iso()
        view["published"] = True
        view["published_at"] = published_at
        view["status"] = "已生效"
        coverage["发布状态"] = "已发布"
        coverage["发布时间"] = published_at
        coverage["报告可复核"] = len(coverage.get("未计入来源", [])) > 0
        view["来源覆盖清单"] = coverage

        writes = source_quality.writeback_coverage(coverage, view_name)
        index = [
            {
                "来源标识": item.get("来源标识"),
                "采集区域": item.get("采集区域"),
                "结论编号": item.get("结论编号"),
                "是否计入": True,
                "格式版本": item.get("格式版本"),
            }
            for item in coverage.get("计入来源", [])
        ] + [
            {
                "来源标识": item.get("来源标识"),
                "采集区域": item.get("采集区域"),
                "结论编号": item.get("结论编号"),
                "是否计入": False,
                "格式版本": item.get("格式版本"),
                "原因": item.get("原因"),
                "触发阈值": item.get("触发阈值"),
            }
            for item in coverage.get("未计入来源", [])
        ]
        return {
            "ok": True,
            "message": (
                f"{view_name} 已发布，来源覆盖 {coverage.get('计入来源数')}/"
                f"{coverage.get('来源总数')}"
            ),
            "view_name": view_name,
            "published_at": published_at,
            "coverage_version": coverage.get("覆盖版本"),
            "coverage_reference": view.get("来源覆盖清单引用"),
            "coverage_complete": coverage.get("覆盖完整"),
            "qualified_logs": view.get("合格日志数"),
            "excluded_sources": [
                item for item in index if not item["是否计入"]
            ],
            "conclusion_index": index,
            "coverage_writes": writes,
            "coverage": coverage,
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据，并带上来源覆盖清单。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}
        coverage = view.get("来源覆盖清单") if view else None
        excluded = (coverage or {}).get("未计入来源", [])
        conclusion_refs = [
            {
                "来源标识": item.get("来源标识"),
                "采集区域": item.get("采集区域"),
                "结论编号": item.get("结论编号"),
                "计入": True,
            }
            for item in (coverage or {}).get("计入来源", [])
        ] + [
            {
                "来源标识": item.get("来源标识"),
                "采集区域": item.get("采集区域"),
                "结论编号": item.get("结论编号"),
                "计入": False,
                "原因": item.get("原因"),
            }
            for item in excluded
        ]

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "已完成",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": bool(excluded),
            "results": results_data,
            "record_count": len(results_data),
            "导出时间": _now_iso(),
            "exported_at": _now_iso(),
            "合格日志数": view.get("合格日志数") if view else 0,
            "来源覆盖清单引用": view.get("来源覆盖清单引用") if view else None,
            "来源覆盖清单": copy.deepcopy(coverage) if coverage else None,
            "校验条目引用": conclusion_refs,
            "未计入来源": [
                {
                    "来源标识": item.get("来源标识"),
                    "采集区域": item.get("采集区域"),
                    "结论编号": item.get("结论编号"),
                    "原因": item.get("原因"),
                }
                for item in excluded
            ],
        }
        exports.append(export_entry)

        # 记录血缘：每个来源一条，报告使用方可按结论编号回查校验条目。
        lineage_entries: list[dict[str, Any]] = []
        lineage_sources = conclusion_refs or [
            {"来源标识": "unknown", "采集区域": "-", "结论编号": None, "计入": True}
        ]
        for ref in lineage_sources:
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": view_name,
                "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
                "标准化版本": view_name,
                "下游消费标识": f"export-{export_id}",
                "记录时间": _now_iso(),
                "status": "已记录" if ref.get("计入") else "验证异常",
                "pending": False,
                "abnormal": not ref.get("计入", True),
                "export_id": str(export_id),
                "export_format": export_format,
                "来源标识": ref.get("来源标识"),
                "采集区域": ref.get("采集区域"),
                "校验结论编号": ref.get("结论编号"),
                "是否计入": ref.get("计入", True),
                "来源覆盖清单引用": export_entry["来源覆盖清单引用"],
                "未计入原因": ref.get("原因"),
            }
            lineages.append(lineage_entry)
            lineage_entries.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entries[0] if lineage_entries else None,
            "lineage_count": len(lineage_entries),
            "coverage_complete": (coverage or {}).get("覆盖完整", False) if coverage else False,
            "excluded_sources": export_entry["未计入来源"],
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()