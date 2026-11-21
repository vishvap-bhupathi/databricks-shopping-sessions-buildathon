//! Strict LF-delimited CSV reader with Arrow-compatible column buffers.

use std::ffi::{c_char, CStr, OsStr};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::os::unix::ffi::OsStrExt;

const MAX_COLUMNS: usize = 7;
const MAX_ROWS: usize = 32_768;
const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;

pub struct Reader {
    source: BufReader<File>,
    line: Vec<u8>,
    width: usize,
}

#[derive(Default)]
struct Column {
    validity: Vec<u8>,
    offsets: Vec<i64>,
    data: Vec<u8>,
    nulls: usize,
}

impl Column {
    fn push(&mut self, row: usize, value: &[u8]) {
        if row % 8 == 0 {
            self.validity.push(0);
        }
        if value.is_empty() {
            self.nulls += 1;
        } else {
            self.validity[row / 8] |= 1 << (row % 8);
            self.data.extend_from_slice(value);
        }
        self.offsets.push(self.data.len() as i64);
    }
}

pub struct Batch {
    columns: Vec<Column>,
    rows: usize,
}

#[repr(C)]
pub struct ColumnView {
    validity: *const u8,
    offsets: *const i64,
    data: *const u8,
    data_bytes: usize,
    nulls: usize,
}

fn parse<'a>(line: &'a [u8], fields: &mut [&'a [u8]]) -> bool {
    let mut values = line.strip_suffix(b"\n").unwrap_or(line).split(|&b| b == b',');
    for field in fields {
        let Some(value) = values.next() else { return false };
        let value = if value.len() >= 2 && value[0] == b'"' && value[value.len() - 1] == b'"' {
            &value[1..value.len() - 1]
        } else {
            value
        };
        if value.contains(&b'"') {
            return false;
        }
        *field = value;
    }
    values.next().is_none()
}

impl Reader {
    fn open(path: &OsStr, width: usize) -> io::Result<Self> {
        let mut source = BufReader::with_capacity(256 * 1024, File::open(path)?);
        source.skip_until(b'\n')?;
        Ok(Self { source, line: Vec::new(), width })
    }

    fn next(&mut self) -> io::Result<Option<Batch>> {
        let mut batch = Batch {
            columns: (0..self.width).map(|_| Column { offsets: vec![0], ..Column::default() }).collect(),
            rows: 0,
        };
        let mut source_bytes = 0;
        while batch.rows < MAX_ROWS && source_bytes < MAX_SOURCE_BYTES {
            self.line.clear();
            let bytes = self.source.read_until(b'\n', &mut self.line)?;
            if bytes == 0 {
                break;
            }
            source_bytes += bytes;
            let mut fields = [&[][..]; MAX_COLUMNS];
            let fields = &mut fields[..self.width];
            if !parse(&self.line, fields) {
                continue;
            }
            // Arrow string buffers require valid UTF-8.
            if fields.iter().any(|field| std::str::from_utf8(field).is_err()) {
                return Err(io::ErrorKind::InvalidData.into());
            }
            for (column, value) in batch.columns.iter_mut().zip(fields) {
                column.push(batch.rows, value);
            }
            batch.rows += 1;
        }
        // A run of malformed rows can fill the byte budget without yielding rows.
        if source_bytes == 0 { Ok(None) } else { Ok(Some(batch)) }
    }
}

// Pointer ownership stays with Rust. The Python bridge holds each batch alive
// for as long as any of its Arrow buffers can be read.
#[no_mangle]
pub unsafe extern "C" fn csv_open(path: *const c_char, width: usize) -> *mut Reader {
    if !(2..=MAX_COLUMNS).contains(&width) {
        return std::ptr::null_mut();
    }
    let path = OsStr::from_bytes(CStr::from_ptr(path).to_bytes());
    Reader::open(path, width).map_or(std::ptr::null_mut(), |reader| Box::into_raw(Box::new(reader)))
}

#[no_mangle]
pub unsafe extern "C" fn csv_close(reader: *mut Reader) {
    drop(Box::from_raw(reader));
}

#[no_mangle]
pub unsafe extern "C" fn csv_next(reader: *mut Reader, result: *mut *mut Batch) -> i32 {
    match (*reader).next() {
        Ok(Some(batch)) => {
            *result = Box::into_raw(Box::new(batch));
            1
        }
        Ok(None) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn csv_batch_rows(batch: *const Batch) -> usize {
    (*batch).rows
}

#[no_mangle]
pub unsafe extern "C" fn csv_column(batch: *const Batch, index: usize) -> ColumnView {
    let column = &(&(*batch).columns)[index];
    ColumnView {
        validity: column.validity.as_ptr(),
        offsets: column.offsets.as_ptr(),
        data: column.data.as_ptr(),
        data_bytes: column.data.len(),
        nulls: column.nulls,
    }
}

#[no_mangle]
pub unsafe extern "C" fn csv_batch_free(batch: *mut Batch) {
    drop(Box::from_raw(batch));
}

mod events;
