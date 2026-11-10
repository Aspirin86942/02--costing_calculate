use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use calamine::{open_workbook_auto, DataType, Reader};
use costing_calculate::application::{execute, RunOperation, RunOutcome, RunRequest};
use costing_core::PipelineName;
use rust_xlsxwriter::Workbook;

#[test]
fn complete_sk_export_preserves_costs_and_reports_actual_sheet_rows() {
    let root = std::env::temp_dir().join(format!(
        "costing-full-sk-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let input = root.join("sk-synthetic.xlsx");
    let config: costing_calculate::config::FileConfigV1 =
        toml::from_str(costing_calculate::config::DEFAULT_CONFIG).unwrap();
    let products = &config.pipeline(PipelineName::Sk).product_order;
    let selected = products.first().unwrap();
    assert!(!products.iter().any(|product| product.code == "UNLISTED-SK"));
    write_complete_sk_export(&input, &(selected.code.clone(), selected.name.clone()));

    for (month, detail_rows, qty_rows, analysis_rows) in [
        (None, 20, 2, 1),
        (Some("2026-02"), 10, 1, 0),
        (Some("2030-01"), 0, 0, 0),
    ] {
        for check_only in [false, true] {
            let suffix = format!("{}-{check_only}", month.unwrap_or("all"));
            let output = root.join(format!("output-{suffix}.xlsx"));
            let manifest = root.join(format!("manifest-{suffix}.json"));
            let outcome = execute(RunRequest {
                pipeline: PipelineName::Sk,
                input: Some(input.clone()),
                output: (!check_only).then(|| output.clone()),
                month_start: month.map(str::to_string),
                month_end: month.map(str::to_string),
                check_only,
                benchmark: false,
                summary_output: Some(manifest.clone()),
                redact_paths: true,
                config: None,
                operation: RunOperation::Execute,
            });
            let RunOutcome::Succeeded(summary) = outcome else {
                panic!("full SK export failed: {outcome:?}");
            };
            assert_eq!(summary.run_counts["reader_rows"], 25);
            assert_eq!(summary.run_counts["detail_rows"], detail_rows);
            assert_eq!(summary.run_counts["qty_sheet_rows"], qty_rows);
            assert_eq!(summary.run_counts["work_order_rows"], analysis_rows);
            assert_eq!(summary.error_log_count, qty_rows);
            assert_eq!(
                summary
                    .issue_type_counts
                    .get("MATERIAL_DETAIL_MISMATCH")
                    .copied()
                    .unwrap_or(0),
                qty_rows,
            );
            assert!(!summary
                .issue_type_counts
                .contains_key("TOTAL_COST_MISMATCH"));
            let metric = summary
                .quality_metrics
                .iter()
                .find(|metric| metric.metric == "工单异常分析输出行数")
                .unwrap();
            assert_eq!(metric.value, analysis_rows.to_string());
            let audit: serde_json::Value =
                serde_json::from_slice(&std::fs::read(manifest).unwrap()).unwrap();
            assert_eq!(audit["run_counts"]["work_order_rows"], analysis_rows);
            assert_eq!(summary.output_written, !check_only);
            if check_only {
                assert!(!output.exists());
                continue;
            }
            let mut workbook = open_workbook_auto(&output).unwrap();
            assert_eq!(
                workbook.sheet_names(),
                [
                    "成本计算单总表",
                    "成本计算单数量聚合维度",
                    "成本分析工单维度"
                ]
            );
            for (name, rows) in [
                ("成本计算单总表", detail_rows),
                ("成本计算单数量聚合维度", qty_rows),
                ("成本分析工单维度", analysis_rows),
            ] {
                assert_eq!(workbook.worksheet_range(name).unwrap().height() - 1, rows);
            }
            let quantity = workbook.worksheet_range("成本计算单数量聚合维度").unwrap();
            let headers = quantity.rows().next().unwrap();
            let column = |name: &str| {
                headers
                    .iter()
                    .position(|cell| cell.as_string().as_deref() == Some(name))
                    .unwrap()
            };
            for row in quantity.rows().skip(1) {
                for (name, amount) in [
                    ("本期完工金额", 100.0),
                    ("本期完工直接材料合计完工金额", 40.0),
                    ("本期完工直接人工合计完工金额", 10.0),
                    ("本期完工制造费用合计完工金额", 40.0),
                    ("本期完工委外加工费合计完工金额", 5.0),
                    ("本期完工软件费用合计完工金额", 5.0),
                    ("软件费用单位完工成本", 0.5),
                ] {
                    assert_eq!(row[column(name)].as_f64(), Some(amount));
                }
                assert_eq!(
                    row[column("数据校验状态")].as_string().as_deref(),
                    Some("需复核")
                );
            }
            let analysis = workbook.worksheet_range("成本分析工单维度").unwrap();
            let headers = analysis.rows().next().unwrap();
            for row in analysis.rows().skip(1) {
                for (name, amount) in [
                    ("直接材料合计完工金额", 40.0),
                    ("直接材料单位完工成本", 4.0),
                    ("软件费用合计完工金额", 5.0),
                    ("软件费用单位完工成本", 0.5),
                ] {
                    let index = headers
                        .iter()
                        .position(|cell| cell.as_string().as_deref() == Some(name))
                        .unwrap();
                    assert_eq!(row[index].as_f64(), Some(amount));
                }
                let reason = headers
                    .iter()
                    .position(|cell| cell.as_string().as_deref() == Some("复核原因"))
                    .unwrap();
                assert!(row[reason]
                    .as_string()
                    .unwrap()
                    .contains("材料子项合计与ERP汇总不一致"));
            }
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

fn write_complete_sk_export(path: &Path, selected: &(String, String)) {
    let headers = [
        ("年期", "年期"),
        ("需求单据", "需求单据"),
        ("需求单据行号", "需求单据行号"),
        ("成本中心编码", "成本中心编码"),
        ("成本中心名称", "成本中心名称"),
        ("产品编码", "产品编码"),
        ("产品名称", "产品名称"),
        ("规格型号", "规格型号"),
        ("生产类型", "生产类型"),
        ("单据类型", "单据类型"),
        ("工单编号", "工单编号"),
        ("工单行号", "工单行号"),
        ("业务状态", "业务状态"),
        ("供应商编码", "供应商编码"),
        ("供应商名称", "供应商名称"),
        ("BOM版本", "BOM版本"),
        ("批号", "批号"),
        ("计划跟踪号", "计划跟踪号"),
        ("辅助属性", "辅助属性"),
        ("基本单位", "基本单位"),
        ("计划产量", "计划产量"),
        ("成本项目编码", "成本项目编码"),
        ("成本项目名称", "成本项目名称"),
        ("子项物料编码", "子项物料编码"),
        ("子项物料名称", "子项物料名称"),
        ("子项物料规格型号", "子项物料规格型号"),
        ("子项物料基本单位", "子项物料基本单位"),
        ("费用项目编码", "费用项目编码"),
        ("费用项目名称", "费用项目名称"),
        ("期初在产品", "数量"),
        ("期初在产品", "金额"),
        ("期初调整", "数量"),
        ("期初调整", "金额"),
        ("本期投入", "数量"),
        ("本期投入", "金额"),
        ("累计投入", "数量"),
        ("累计投入", "金额"),
        ("期末在产品", "数量"),
        ("期末在产品", "金额"),
        ("期末调整", "数量"),
        ("期末调整", "金额"),
        ("期末调整后", "数量"),
        ("期末调整后", "金额"),
        ("本期完工", "数量"),
        ("本期完工", "单耗"),
        ("本期完工", "单位成本"),
        ("本期完工", "金额"),
        ("在产品分配标准", "在产品分配标准"),
        ("累计合格品入库", "数量"),
        ("累计合格品入库", "金额"),
        ("累计不良品入库", "数量"),
        ("累计不良品入库", "金额"),
        ("累计废品入库", "数量"),
        ("累计废品入库", "金额"),
        ("累计完工", "数量"),
        ("累计完工", "单耗"),
        ("累计完工", "单位成本"),
        ("累计完工", "金额"),
    ];
    assert_eq!(headers.len(), 58);
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    sheet.set_name("成本计算单").unwrap();
    sheet.write_string(0, 0, "核算体系:合成测试").unwrap();
    sheet.write_string(1, 0, "币别:人民币").unwrap();
    for (index, (top, bottom)) in headers.into_iter().enumerate() {
        sheet.write_string(2, index as u16, top).unwrap();
        sheet.write_string(3, index as u16, bottom).unwrap();
    }
    for (group, (code, name)) in [
        (selected.0.as_str(), selected.1.as_str()),
        ("UNLISTED-SK", "合成池外产品"),
    ]
    .into_iter()
    .enumerate()
    {
        let first_row = 4 + group as u32 * 12;
        sheet
            .write_string(first_row, 0, format!("2026年{:02}期", group + 1))
            .unwrap();
        sheet.write_string(first_row, 4, "集成车间").unwrap();
        sheet.write_string(first_row, 5, code).unwrap();
        sheet.write_string(first_row, 6, name).unwrap();
        sheet
            .write_string(first_row, 9, "汇报入库-普通生产")
            .unwrap();
        sheet
            .write_string(first_row, 10, format!("SYNTHETIC-WO-{group}"))
            .unwrap();
        sheet.write_number(first_row, 11, 1).unwrap();
        sheet.write_string(first_row, 19, "个").unwrap();
        sheet.write_number(first_row, 43, 10).unwrap();
        sheet.write_number(first_row, 46, 100).unwrap();
        // 其他金额分组刻意不同，防止宽表列序误把投入或调整金额当成本期完工金额。
        for column in [30, 32, 34, 36, 38, 40, 42, 49, 51, 53, 57] {
            sheet.write_number(first_row, column, 999).unwrap();
        }
        for (offset, item, amount, material) in [
            (1, "直接材料", 40, ""),
            (2, "", 25, "SYNTHETIC-MATERIAL-1"),
            (3, "", 35, "SYNTHETIC-MATERIAL-2"),
            (4, "直接人工", 10, ""),
            (5, "制造费用_其他", 10, ""),
            (6, "制造费用-人工", 10, ""),
            (7, "制造费用_机物料及低耗", 10, ""),
            (8, "制造费用_折旧", 5, ""),
            (9, "制造费用_水电费", 5, ""),
            (10, "委外加工费", 5, ""),
            (11, "软件费用", 5, ""),
        ] {
            let row = first_row + offset;
            sheet.write_string(row, 22, item).unwrap();
            sheet.write_string(row, 23, material).unwrap();
            sheet.write_string(row, 28, "合成费用项目").unwrap();
            sheet.write_number(row, 46, amount).unwrap();
        }
    }
    sheet.write_string(28, 0, "合计").unwrap();
    workbook.save(path).unwrap();
}
