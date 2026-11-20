use std::collections::VecDeque;
use std::io::{Cursor, Read, Seek, Write};
use std::thread;

use calamine::{Data, Range, Reader, Xlsx};
use costing_core::model::{CellValue, RawWorkbook};
use quick_xml::events::Event;
use quick_xml::Reader as XmlReader;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

use super::{normalize_data_cell, raw_from_range};

const SHEET: &str = "xl/worksheets/sheet1.xml";
const BLOCK: usize = 1024 * 1024;

pub(super) fn read(source: impl Read + Seek) -> Option<RawWorkbook> {
    let workers = thread::available_parallelism().ok()?.get().min(12);
    if workers < 2 {
        return None;
    }
    read_blocks(source, 16 * BLOCK, BLOCK, workers)
}

// Keep a small ZIP containing the original styles, strings and relationships.
// Each worker adds one block of complete rows and uses the same Calamine reader.
fn read_blocks(
    source: impl Read + Seek,
    minimum: usize,
    block: usize,
    workers: usize,
) -> Option<RawWorkbook> {
    let mut archive = ZipArchive::new(source).ok()?;
    let size = archive.by_name(SHEET).ok()?.size();
    if archive.len() > 32 || size < minimum as u64 {
        return None;
    }
    // Small sheets need fewer live Calamine ranges and allocator arenas.
    let workers = workers.min(size.div_ceil(32 * 1024 * 1024).clamp(2, 12) as usize);
    let mut template = ZipWriter::new(Cursor::new(Vec::new()));
    let mut metadata_size = 0u64;
    for i in 0..archive.len() {
        let file = archive.by_index(i).ok()?;
        let name = file.name().ok()?;
        if name == SHEET {
            continue;
        }
        metadata_size = metadata_size.checked_add(file.size())?;
        if name.starts_with("xl/worksheets/") || metadata_size > 1024 * 1024 {
            return None;
        }
        template.raw_copy_file(file).ok()?;
    }
    let template = template.finish().ok()?.into_inner();
    let mut sheet = archive.by_name(SHEET).ok()?;
    let mut header = Vec::new();
    (&mut sheet).take(64 * 1024).read_to_end(&mut header).ok()?;
    let mut xml = XmlReader::from_reader(header.as_slice());
    loop {
        match xml.read_event().ok()? {
            Event::Start(tag) if tag.name().as_ref() == "sheetData" => break,
            Event::Eof | Event::DocType(_) => return None,
            _ => {}
        }
    }
    if xml.encoding().name() != "UTF-8" {
        return None;
    }
    let offset = header.len() - xml.into_inner().len();
    let (prefix, buffered_rows) = header.split_at(offset);
    let mut input = buffered_rows.chain(sheet);
    let mut carry = Vec::new();
    let mut eof = false;
    let mut next = || -> Option<Option<Vec<u8>>> {
        if eof {
            return Some(None);
        }
        let count = (&mut input)
            .take(block as u64)
            .read_to_end(&mut carry)
            .ok()?;
        eof = count < block;
        let end = carry
            .windows(6)
            .rposition(|s| s == b"</row>")
            .map_or(0, |p| p + 6);
        let tail = carry.split_off(end);
        if eof {
            if !tail.trim_ascii_start().starts_with(b"</sheetData>") {
                return None;
            }
        } else if end == 0 {
            return None;
        }
        let rows = std::mem::replace(&mut carry, tail);
        Some((!rows.is_empty()).then_some(rows))
    };

    let (name, first) = read_range(&template, prefix, &next()??)?;
    let (_, left) = first.start()?;
    let (mut last_row, right) = first.end()?;
    let mut result = raw_from_range(name, first).ok()?;
    let width = (right - left + 1) as usize;
    thread::scope(|scope| {
        let mut pending = VecDeque::new();
        let template = &template;
        loop {
            if pending.len() < workers {
                if let Some(rows) = next()? {
                    pending.push_back(
                        thread::Builder::new()
                            .spawn_scoped(scope, move || {
                                let (_, range) = read_range(template, prefix, &rows)?;
                                aligned_rows(range, left, right)
                            })
                            .ok()?,
                    );
                    continue;
                }
            }
            let Some(job) = pending.pop_front() else {
                break;
            };
            let (start, rows) = job.join().ok()??;
            if !rows.is_empty() {
                if start <= last_row {
                    return None;
                }
                result
                    .rows
                    .extend((last_row + 1..start).map(|_| vec![CellValue::Blank; width]));
                last_row = start.checked_add(u32::try_from(rows.len() - 1).ok()?)?;
                result.rows.extend(rows);
            }
        }
        Some(result)
    })
}

fn aligned_rows(
    mut range: Range<Data>,
    left: u32,
    right: u32,
) -> Option<(u32, Vec<Vec<CellValue>>)> {
    if range.is_empty() {
        return Some((0, Vec::new()));
    }
    let (start, col) = range.start()?;
    if col < left || range.end()?.1 > right {
        return None;
    }
    let width = (right - left + 1) as usize;
    let mut output = Vec::with_capacity(range.height());
    for i in 0..range.height() {
        // Normalization appends two derived values to each input row.
        let mut row = Vec::with_capacity(width + 2);
        row.resize((col - left) as usize, CellValue::Blank);
        row.extend(
            range[i]
                .iter_mut()
                .map(|cell| normalize_data_cell(std::mem::take(cell))),
        );
        row.resize(width, CellValue::Blank);
        output.push(row);
    }
    Some((start, output))
}

fn read_range(template: &[u8], prefix: &[u8], rows: &[u8]) -> Option<(String, Range<Data>)> {
    complete_rows(rows)?;
    let mut zip = ZipWriter::new_append(Cursor::new(template.to_vec())).ok()?;
    zip.start_file(
        SHEET,
        SimpleFileOptions::default().compression_method(CompressionMethod::Stored),
    )
    .ok()?;
    zip.write_all(prefix).ok()?;
    zip.write_all(rows).ok()?;
    zip.write_all(b"</sheetData></worksheet>").ok()?;
    let mut workbook = Xlsx::new(zip.finish().ok()?).ok()?;
    let [name] = workbook.sheet_names().try_into().ok()?;
    let range = workbook.worksheet_range(&name).ok()?;
    Some((name, range))
}

fn complete_rows(rows: &[u8]) -> Option<()> {
    // A literal </row> can occur in a comment or CDATA. Only independently
    // well-formed row elements with explicit positions may cross a block boundary.
    let mut xml = XmlReader::from_reader(rows);
    loop {
        match xml.read_event().ok()? {
            Event::Start(tag) if tag.name().as_ref() == "row" => {
                let pos = tag.try_get_attribute("r").ok()??;
                pos.value
                    .as_ref()
                    .parse::<u32>()
                    .ok()
                    .filter(|n| (1..=1_048_576).contains(n))?;
                xml.read_to_end(tag.name()).ok()?;
            }
            Event::Text(t) if t.as_ref().trim().is_empty() => {}
            Event::Eof => return Some(()),
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::read_open_workbook;
    use calamine::open_workbook_auto_from_rs;
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

    fn fixture() -> Vec<u8> {
        let mut book = Workbook::new();
        let sheet = book.add_worksheet();
        sheet.write_string(2, 2, "年期").unwrap();
        sheet.write_string(2, 4, "产品编码").unwrap();
        sheet.write_string(3, 5, "金额").unwrap();
        let date = ExcelDateTime::from_ymd(2025, 7, 1).unwrap();
        let format = Format::new().set_num_format("yyyy-mm-dd");
        for row in (4..80).filter(|r| r % 5 != 0) {
            sheet.write_string(row, 2, " 共享 & 文本 ").unwrap();
            sheet
                .write_datetime_with_format(row, 3, &date, &format)
                .unwrap();
            sheet.write_number(row, 5, 0.125).unwrap();
            sheet.write_formula(row, 4, "=1+2").unwrap();
        }
        book.save_to_buffer().unwrap()
    }

    #[test]
    fn parallel_blocks_preserve_global_bounds_gaps_types_and_order() {
        let bytes = fixture();
        let expected =
            read_open_workbook(open_workbook_auto_from_rs(Cursor::new(&bytes)).unwrap()).unwrap();
        for block in [512, 1500, 4096] {
            let actual = read_blocks(Cursor::new(&bytes), 0, block, 3).unwrap();
            assert_eq!(actual, expected, "block size {block}");
        }
    }

    #[test]
    fn fragments_require_absolute_rows_and_complete_xml() {
        assert!(complete_rows(b"<row r=\"2\"><c/></row>").is_some());
        for broken in [
            b"<row><c/></row>".as_slice(),
            b"<row r=\"2\"><!-- </row>",
            b"<row r=\"2\"><c></row>",
        ] {
            assert!(complete_rows(broken).is_none());
        }
    }

    #[test]
    fn malformed_sheet_dimension_keeps_the_original_read_error() {
        let mut archive = ZipArchive::new(Cursor::new(fixture())).unwrap();
        let mut changed = ZipWriter::new(Cursor::new(Vec::new()));
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap();
            if file.name().unwrap() == SHEET {
                let mut xml = String::new();
                file.read_to_string(&mut xml).unwrap();
                assert!(xml.contains("<dimension ref="));
                changed
                    .start_file(SHEET, SimpleFileOptions::default())
                    .unwrap();
                changed
                    .write_all(
                        xml.replace("<dimension ref=", "<dimension invalid=")
                            .as_bytes(),
                    )
                    .unwrap();
            } else {
                changed.raw_copy_file(file).unwrap();
            }
        }
        let bytes = changed.finish().unwrap().into_inner();
        assert!(read_blocks(Cursor::new(&bytes), 0, 512, 3).is_none());
        assert!(
            read_open_workbook(open_workbook_auto_from_rs(Cursor::new(bytes)).unwrap()).is_err()
        );
    }
}
