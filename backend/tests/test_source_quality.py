"""分来源质量校验的业务行为测试。

覆盖：按来源的必填字段缺失率、现行口径版本、阈值触发、南北两区同格式不同
判定依据、原因结论冻结、聚合来源准入、覆盖清单回写、导出可追溯，以及北区
补齐后的班报来源组成。
"""
from __future__ import annotations

import json

from app.services import source_quality
from app.store import store


def _bulletin_logs(region: str, with_host: bool = True, missing_host_rows: int = 0) -> list[dict]:
    logs = []
    for index, (level, message) in enumerate(
        [("INFO", "gateway ok"), ("WARN", "disk 81%"), ("ERROR", "timeout"), ("INFO", "request ok")]
    ):
        drop_host = not with_host or index < missing_host_rows
        host = "" if drop_host else f"{region}-host-{index}"
        logs.append({
            "body": f"2026-11-19T08:0{index}:00|{level}|{region} {message}|{host}",
        })
    return logs


class TestPerSourceMissingRate:
    def test_south_full_sample_passes(self, pipeline):
        result = pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        assert result["ok"] is True
        conclusion = result["conclusion"]
        assert conclusion["结论"] == "合格"
        assert conclusion["分母"] == 4
        assert conclusion["必填字段"] == ["time", "level", "message", "host"]
        assert conclusion["越线字段"] == []

    def test_north_missing_host_crosses_threshold(self, pipeline):
        result = pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        conclusion = result["conclusion"]
        assert conclusion["结论"] == "越线"
        assert conclusion["越线字段"] == ["host"]
        assert conclusion["缺失统计"]["host"]["缺失数"] == 4
        assert conclusion["缺失统计"]["host"]["缺失率"] == 1.0
        assert conclusion["触发阈值"][0]["阈值"] == 0.1
        assert len(conclusion["缺失行"]) == 4
        assert all(item["缺失字段"] == ["host"] for item in conclusion["缺失行"])

    def test_missing_rate_recomputed_per_source(self, pipeline):
        """南区跑一份缺 host 的样本，北区不受影响，各自按本处口径重算。"""
        pipeline.run_quality_sample("bulletin-north", "2026-11-19", _bulletin_logs("north"))
        north_before = source_quality.latest_conclusion("bulletin-north")
        pipeline.run_quality_sample(
            "bulletin-south", "2026-11-19", _bulletin_logs("south", with_host=False)
        )
        south = source_quality.latest_conclusion("bulletin-south")
        north_after = source_quality.latest_conclusion("bulletin-north")
        assert south["来源标识"] == "bulletin-south"
        assert south["越线字段"] == ["host"]
        assert north_after["id"] == north_before["id"]

    def test_threshold_is_per_field_overridable(self, pipeline):
        source_quality.configure_rule(
            "bullet-threshold-src", ["time", "level", "message", "host"],
            max_missing_rate=0.4,
        )
        pipeline.register_source("bullet-threshold-src", "http", "南区")
        pipeline.validate_source("bullet-threshold-src")
        result = pipeline.run_quality_sample(
            "bullet-threshold-src", "2026-11-19", _bulletin_logs("south", with_host=False)
        )
        # 4 条里 4 条缺 host，缺失率 100% > 40%
        assert result["conclusion"]["结论"] == "越线"

        source_quality.configure_rule(
            "bullet-threshold-src", ["time", "level", "message", "host"],
            max_missing_rate=1.0,
        )
        result = pipeline.run_quality_sample(
            "bullet-threshold-src", "2026-11-20", _bulletin_logs("south", with_host=False)
        )
        assert result["conclusion"]["结论"] == "合格"

    def test_conclusion_records_format_and_threshold(self, pipeline):
        result = pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        conclusion = result["conclusion"]
        assert conclusion["格式版本"] == "v1.0"
        assert "缺失率≤10.0%" in conclusion["判定依据"]
        assert "bulletin-south" in conclusion["判定依据"]
        assert conclusion["规则名称"]
        assert conclusion["阈值"]["required_fields"] == ["time", "level", "message", "host"]


class TestSameFormatTwoBases:
    def test_two_regions_same_schema_but_separate_basis(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        south = source_quality.latest_conclusion("bulletin-south")
        north = source_quality.latest_conclusion("bulletin-north")
        assert south["格式版本"] == north["格式版本"] == "v1.0"
        assert south["规则ID"] != north["规则ID"]
        assert south["判定依据"] != north["判定依据"]
        assert "南区" in south["判定依据"]
        assert "北区" in north["判定依据"]

    def test_rule_rollover_keeps_history(self, pipeline):
        first = pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )["conclusion"]
        source_quality.configure_rule(
            "bulletin-north", ["time", "level", "message", "host"], max_missing_rate=0.6
        )
        second = pipeline.run_quality_sample(
            "bulletin-north", "2026-11-20", _bulletin_logs("north", missing_host_rows=2)
        )["conclusion"]
        assert second["结论"] == "合格"
        assert second["缺失统计"]["host"]["缺失率"] == 0.5
        assert second["规则版本"] == 2

        frozen = source_quality.get_conclusion(first["结论编号"])
        assert frozen["分母"] == 4
        assert frozen["越线字段"] == ["host"]
        assert len(frozen["缺失行"]) == 4
        assert frozen["规则版本"] == 1
        assert frozen["阈值"]["max_missing_rate"] == 0.1


class TestAggregationCoverage:
    def test_aggregation_counts_only_qualified_source(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        result = pipeline.refresh_aggregation("班报-跨区", "level", "time", "count")
        assert result["qualified_logs"] == 4
        assert result["coverage_complete"] is False
        assert [item["来源标识"] for item in result["excluded_sources"]] == ["bulletin-north"]
        assert result["view"]["results"]["INFO"]["count"] == 2
        # 北区 4 条全部排除，计入的只有南区 4 条
        assert sum(item["count"] for item in result["view"]["results"].values()) == 4

    def test_coverage_lists_both_sources_with_reason(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        view = pipeline.refresh_aggregation("班报-跨区", "level", "time", "count")["view"]
        coverage = view["来源覆盖清单"]
        assert coverage["来源总数"] == 2
        assert coverage["计入来源数"] == 1
        assert coverage["未计入来源数"] == 1
        north = coverage["未计入来源"][0]
        assert north["来源标识"] == "bulletin-north"
        assert north["采集区域"] == "北区"
        assert "host" in north["原因"]
        assert north["结论快照"]["分母"] == 4

    def test_publish_writes_back_registry(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        pipeline.refresh_aggregation("班报-跨区", "level", "time", "count")
        published = pipeline.publish_aggregation("班报-跨区")
        assert published["ok"] is True
        assert published["coverage_complete"] is False
        assert published["excluded_sources"][0]["来源标识"] == "bulletin-north"

        registry = {row["来源标识"]: row for row in store.rows("source_registry")}
        assert registry["bulletin-south"]["覆盖状态"] == "已计入"
        assert registry["bulletin-north"]["覆盖状态"] == "未计入"
        assert registry["bulletin-north"]["未计入原因"]

        index = {item["来源标识"]: item for item in published["conclusion_index"]}
        north_conclusion = source_quality.get_conclusion(index["bulletin-north"]["结论编号"])
        assert north_conclusion["是否合格"] is False
        assert north_conclusion["缺失行"]

    def test_export_references_conclusions(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        pipeline.refresh_aggregation("班报-跨区", "level", "time", "count")
        result = pipeline.build_export("班报-跨区", "csv")
        refs = result["export"]["校验条目引用"]
        assert {item["来源标识"] for item in refs} == {"bulletin-south", "bulletin-north"}
        excluded = [item for item in refs if not item["计入"]]
        assert excluded and excluded[0]["来源标识"] == "bulletin-north"
        assert result["coverage_complete"] is False
        assert result["lineage_count"] == 2
        lineage = [row for row in store.rows("data_lineage") if row.get("来源标识")]
        assert any(row["是否计入"] is False for row in lineage)


class TestAlertCheck:
    def test_south_alert_check_independent(self, pipeline):
        """南区先自查，不依赖北区班报是否完整。"""
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        result = source_quality.run_alert_check("bulletin-south")
        assert result["ok"] is True
        assert result["passed"] is True
        assert result["alerts"] == []

    def test_north_alert_check_reports_missing_host(self, pipeline):
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        result = source_quality.run_alert_check("bulletin-north")
        assert result["passed"] is False
        assert result["alerts"]
        assert result["alerts"][0]["字段"] == "host"
        assert "北区" in result["alerts"][0]["内容"]

    def test_alert_check_without_conclusion(self, pipeline):
        pipeline.register_source("no-conclusion-src", "http", "东区")
        result = source_quality.run_alert_check("no-conclusion-src")
        assert result["ok"] is False


class TestCoverageAfterRepair:
    def test_north_repaired_joins_bulletin(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        broken = pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        assert broken["conclusion"]["结论"] == "越线"

        repaired = pipeline.run_quality_sample(
            "bulletin-north", "2026-11-20", _bulletin_logs("north", with_host=True)
        )
        assert repaired["conclusion"]["结论"] == "合格"

        view = pipeline.refresh_aggregation("班报-跨区", "level", "time", "count")
        coverage = view["view"]["来源覆盖清单"]
        assert coverage["覆盖完整"] is True
        assert coverage["计入来源数"] == 2
        assert coverage["未计入来源数"] == 0
        assert coverage["来源总数"] == 2
        assert view["qualified_logs"] == 8
        assert {item["来源标识"] for item in coverage["计入来源"]} == {
            "bulletin-south", "bulletin-north",
        }

        # 北区仍有 2 条历史缺口记录，来源整体合格但记录缺口仍要可见。
        north = next(
            item for item in coverage["计入来源"] if item["来源标识"] == "bulletin-north"
        )
        assert north["记录缺口"] == 0  # 聚合只取最新批次

    def test_not_counted_source_remains_visible_in_overview(self, pipeline):
        pipeline.run_quality_sample("bulletin-south", "2026-11-19", _bulletin_logs("south"))
        pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        pipeline.refresh_aggregation("班报-跨区", "level", "time", "count")
        pipeline.publish_aggregation("班报-跨区")

        overview = source_quality.overview("班报-跨区")
        assert overview["summary"]["来源数"] == 2
        assert overview["summary"]["越线来源"] == 1
        north = next(item for item in overview["sources"] if item["来源标识"] == "bulletin-north")
        assert north["最新结论"]["结论"] == "越线"
        assert north["覆盖状态"] == "未计入"
        assert overview["view"]["覆盖完整"] is False
        # 两个来源各自独立重算，北区的越线不会把南区拉下水。
        south = next(item for item in overview["sources"] if item["来源标识"] == "bulletin-south")
        assert south["最新结论"]["结论"] == "合格"
        assert south["最新结论"]["判定依据"] != north["最新结论"]["判定依据"]

    def test_conclusion_detail_endpoint_shape(self, pipeline):
        result = pipeline.run_quality_sample(
            "bulletin-north", "2026-11-19", _bulletin_logs("north", with_host=False)
        )
        conclusion = source_quality.get_conclusion(result["conclusion"]["结论编号"])
        payload = json.loads(json.dumps(conclusion, ensure_ascii=False, default=str))
        assert payload["缺失行"][0]["缺失字段"] == ["host"]
        assert payload["分母"] == 4
