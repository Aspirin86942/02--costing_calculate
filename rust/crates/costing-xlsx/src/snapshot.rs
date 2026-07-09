use std::collections::BTreeMap;

use costing_core::model::{CellValue, RawWorkbook, ReaderSnapshot};

pub fn build_reader_snapshot(raw: &RawWorkbook) -> ReaderSnapshot {
    let headers = flatten_header_rows(&raw.header_rows);
    let mut null_counts = BTreeMap::new();
    for (idx, header) in headers.iter().enumerate() {
        let count = raw
            .rows
            .iter()
            .filter(|row| matches!(row.get(idx), None | Some(CellValue::Blank)))
            .count();
        null_counts.insert(header.clone(), count);
    }
    ReaderSnapshot {
        sheet_name: raw.sheet_name.clone(),
        row_count: raw.rows.len(),
        column_count: headers.len(),
        headers,
        null_counts,
    }
}

pub fn flatten_header_rows(header_rows: &[Vec<String>; 2]) -> Vec<String> {
    let width = header_rows[0].len().max(header_rows[1].len());
    (0..width)
        .map(|idx| {
            let top = header_rows[0]
                .get(idx)
                .map(String::as_str)
                .unwrap_or("")
                .trim();
            let bottom = header_rows[1]
                .get(idx)
                .map(String::as_str)
                .unwrap_or("")
                .trim();
            match (top.is_empty(), bottom.is_empty()) {
                (true, true) => format!("column_{idx}"),
                (false, true) => top.to_string(),
                (true, false) => bottom.to_string(),
                (false, false) if top == bottom => top.to_string(),
                (false, false) => bottom.to_string(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use costing_core::model::{CellValue, RawWorkbook};

    use super::*;

    #[test]
    fn snapshot_counts_blank_cells_by_flattened_header() {
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec!["产品".to_string(), "金额".to_string(), "".to_string()],
                vec!["产品编码".to_string(), "".to_string(), "日期".to_string()],
            ],
            rows: vec![
                vec![
                    CellValue::Text("A".to_string()),
                    CellValue::Blank,
                    CellValue::DateLike("2025-01-02 00:00:00".to_string()),
                ],
                vec![
                    CellValue::Blank,
                    CellValue::Decimal("10".parse().unwrap()),
                    CellValue::Blank,
                ],
            ],
        };
        let snapshot = build_reader_snapshot(&raw);
        assert_eq!(snapshot.headers, vec!["产品编码", "金额", "日期"]);
        assert_eq!(snapshot.null_counts["产品编码"], 1);
        assert_eq!(snapshot.null_counts["金额"], 1);
        assert_eq!(snapshot.null_counts["日期"], 1);
    }
}
