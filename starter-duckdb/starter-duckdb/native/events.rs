//! Partitioned winner selection over read-only input mappings.
use super::{parse, Column, ColumnView, MAX_ROWS};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::{c_char, c_void, CStr, OsStr};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::slice;
use std::sync::{atomic::{AtomicUsize, Ordering}, mpsc::{sync_channel, Receiver, SyncSender}, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

const PARTITIONS: usize = 256;
const MAX_PARTITION_ROWS: usize = 1_000_000;
const MAX_INDEXED_ROWS: usize = 80_000_000;
const DAY_MS: i64 = 86_400_000;
type Bytes = Box<[u8]>;
type Dictionary = HashMap<Bytes, Bytes>;
#[derive(Default)]
pub struct Dimensions {
    aliases: Dictionary,
    products: Dictionary,
}

extern "C" {
    fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut c_void;
    fn munmap(addr: *mut c_void, len: usize) -> i32;
}

struct Mapping(*mut c_void, usize);
// The mount is read-only. The mapping outlives every scoped borrowing worker.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    fn open(path: &PathBuf) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = usize::try_from(file.metadata()?.len()).map_err(|_| io::ErrorKind::InvalidData)?;
        if len == 0 { return Ok(Self(std::ptr::null_mut(), 0)); }
        let addr = unsafe { mmap(std::ptr::null_mut(), len, 1, 2, file.as_raw_fd(), 0) };
        if addr as isize == -1 { return Err(io::Error::last_os_error()); }
        Ok(Self(addr, len))
    }

    fn bytes(&self) -> &[u8] {
        if self.1 == 0 { &[] } else { unsafe { slice::from_raw_parts(self.0.cast(), self.1) } }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        if self.1 != 0 { unsafe { munmap(self.0, self.1); } }
    }
}

fn unsigned(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() { return None; }
    bytes.iter().try_fold(0_i64, |n, &b| {
        if !b.is_ascii_digit() { return None; }
        n.checked_mul(10)?.checked_add((b - b'0') as i64)
    })
}

fn two_digits(bytes: &[u8], at: usize) -> Option<i64> {
    let a = bytes[at].wrapping_sub(b'0');
    let b = bytes[at + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 { return None; }
    Some((a * 10 + b) as i64)
}

fn datetime(bytes: &[u8]) -> Option<i64> {
    if !matches!(bytes.len(), 20 | 25)
        || [bytes[4], bytes[7], bytes[10], bytes[13], bytes[16]] != *b"--T::"
    { return None; }
    let year = two_digits(bytes, 0)? * 100 + two_digits(bytes, 2)?;
    let month = two_digits(bytes, 5)?;
    let day = two_digits(bytes, 8)?;
    let hour = two_digits(bytes, 11)?;
    let min = two_digits(bytes, 14)?;
    let sec = two_digits(bytes, 17)?;
    let leap = |y| y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let month_days = [31, if leap(year) {29} else {28}, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=12).contains(&month) || !(1..=month_days[month as usize - 1]).contains(&day)
        || hour > 23 || min > 59 || sec > 59 { return None; }
    let offset = if bytes.len() == 20 && bytes[19] == b'Z' {
        0
    } else if bytes.len() == 25 && bytes[22] == b':' && matches!(bytes[19], b'+' | b'-') {
        let h = two_digits(bytes, 20)?;
        let m = two_digits(bytes, 23)?;
        if h > 23 || m > 59 { return None; }
        (h * 60 + m) * if bytes[19] == b'+' { 1 } else { -1 }
    } else {
        return None;
    };
    let y = year - 1;
    let days_before_year = y * 365 + y.div_euclid(4) - y.div_euclid(100) + y.div_euclid(400);
    let before_month = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let days = days_before_year - 719_162 + before_month[month as usize - 1] + day - 1
        + i64::from(month > 2 && leap(year));
    Some(days * DAY_MS + ((hour * 60 + min - offset) * 60 + sec) * 1000)
}

fn timestamp(bytes: &[u8]) -> Option<i64> {
    if bytes.get(4) == Some(&b'-') { datetime(bytes) } else { unsigned(bytes) }
}

fn validate<'a>(line: &'a [u8], dimensions: &'a Dimensions, start: i64, end: i64)
    -> Option<(&'a [u8], i64, Option<i64>, &'a [u8])>
{
    let mut fields = [&[][..]; 7];
    if !parse(line, &mut fields) { return None; }
    validated_fields(fields, dimensions, start, end, datetime(fields[3])?)
}

fn validated_fields<'a>(fields: [&'a [u8];7], dimensions: &'a Dimensions,
    start:i64, end:i64, ingest_ms:i64) -> Option<(&'a [u8], i64, Option<i64>, &'a [u8])> {
    let (user, at, cents, product) = check_event(fields, start, end, ingest_ms)?;
    let person = dimensions.aliases.get(user).map_or(user, |v| v);
    let category = if cents.is_some() {
        dimensions.products.get(product).map_or(&b"unknown"[..], |v| v)
    } else { &[] };
    Some((person, at, cents, category))
}
fn check_event<'a>(fields: [&'a [u8]; 7], start: i64, end: i64, ingest_ms: i64)
    -> Option<(&'a [u8], i64, Option<i64>, &'a [u8])>
{
    let [_, user, ts, _, kind, product, price] = fields;
    if user.is_empty() || product.is_empty() { return None; }
    let cents = match kind {
        b"view" | b"cart" => None,
        b"purchase" => Some(unsigned(price).filter(|p| (1..=10_000_000).contains(p))?),
        _ => return None,
    };
    let at = timestamp(ts)?;
    if at < start || at >= end || at < ingest_ms.checked_sub(DAY_MS)? { return None; }
    Some((user, at, cents, product))
}


fn bucket(key: &[u8]) -> usize {
    let h = key_hash(key);
    ((h ^ (h >> 32)) as usize) & (PARTITIONS - 1)
}

fn key_hash(key: &[u8]) -> u64 {
    key.iter().fold(0xcbf29ce484222325u64, |h, &b| (h ^ b as u64).wrapping_mul(0x100000001b3))
}

#[derive(Default)]
pub struct EventBatch {
    person: Column,
    times: Vec<i64>,
    prices: Vec<i64>,
    price_validity: Vec<u8>,
    price_nulls: usize,
    category: Column,
}

impl EventBatch {
    fn new() -> Self {
        Self {
            person: Column { offsets: vec![0], ..Column::default() },
            category: Column { offsets: vec![0], ..Column::default() },
            ..Self::default()
        }
    }

    fn push(&mut self, person: &[u8], at: i64, cents: Option<i64>, category: &[u8]) {
        let row = self.times.len();
        self.person.push(row, person);
        self.category.push(row, category);
        self.times.push(at);
        self.prices.push(cents.unwrap_or(0));
        if row % 8 == 0 { self.price_validity.push(0); }
        if cents.is_some() { self.price_validity[row / 8] |= 1 << (row % 8); }
        else { self.price_nulls += 1; }
    }
}

type Message = Result<EventBatch, ()>;
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct Stats {
    input_bytes: u64,
    indexed_rows: u64,
    index_capacity_bytes: u64,
    largest_partition: u64,
    index_seconds: f64,
    reduction_seconds: f64,
}
pub struct EventReader(Option<Receiver<Message>>, Option<JoinHandle<()>>, Arc<Mutex<Stats>>);

type Partition<'a> = Vec<Vec<&'a [u8]>>;

fn index_files(maps: &[Mapping], workers: usize) -> Option<Vec<Partition<'_>>> {
    let next = AtomicUsize::new(0);
    let indexed = AtomicUsize::new(0);
    thread::scope(|scope| {
        let tasks: Vec<_> = (0..workers).map(|_| scope.spawn(|| {
            let mut buckets = vec![Vec::new(); PARTITIONS];
            let mut pending = 0;
            loop {
                let Some(map) = maps.get(next.fetch_add(1, Ordering::Relaxed)) else { break };
                for line in map.bytes().split(|&b| b == b'\n').skip(1) {
                    let mut fields = [&[][..]; 7];
                    if !parse(line, &mut fields) || fields[0].is_empty() { continue; }
                    // Valid F1 ingest is a dataset invariant. On any violation,
                    // defer to the SQL reference instead of changing how it is handled.
                    if fields[3].len() != 20 || datetime(fields[3]).is_none()
                        || fields.iter().any(|f| std::str::from_utf8(f).is_err()) { return None; }
                    buckets[bucket(fields[0])].push(line);
                    pending += 1;
                    if pending == MAX_ROWS {
                        if indexed.fetch_add(pending, Ordering::Relaxed) + pending > MAX_INDEXED_ROWS {
                            return None;
                        }
                        pending = 0;
                    }
                }
            }
            if indexed.fetch_add(pending, Ordering::Relaxed) + pending > MAX_INDEXED_ROWS {
                return None;
            }
            Some(buckets)
        })).collect();
        let mut out = vec![Vec::new(); PARTITIONS];
        for task in tasks {
            for (dst, rows) in out.iter_mut().zip(task.join().ok()??) {
                if !rows.is_empty() { dst.push(rows); }
            }
        }
        Some(out)
    })
}

// Treat the indexed partitions as independent tasks. Only a worker's current
// winner map and its queued output batch need to be live at any one time.
fn process(paths: Vec<PathBuf>, dims: Dimensions, start: i64, end: i64, workers: usize,
           tx: &SyncSender<Message>, stats: &Mutex<Stats>) -> Option<()> {
    let t = Instant::now();
    let maps: Vec<_> = paths.iter().map(Mapping::open).collect::<io::Result<_>>().ok()?;
    let buckets = index_files(&maps, workers)?;
    let mut report = Stats {
        input_bytes: maps.iter().map(|m| m.1 as u64).sum(),
        index_seconds: t.elapsed().as_secs_f64(),
        ..Stats::default()
    };
    for partition in &buckets {
        let count: usize = partition.iter().map(Vec::len).sum();
        if count > MAX_PARTITION_ROWS { return None; }
        report.indexed_rows += count as u64;
        report.largest_partition = report.largest_partition.max(count as u64);
        report.index_capacity_bytes += partition.iter().map(|v| {
            (v.capacity() * std::mem::size_of::<&[u8]>()) as u64
        }).sum::<u64>();
    }
    let reduction = Instant::now();
    let shards: Vec<_> = buckets.into_iter().map(Mutex::new).collect();
    let next = AtomicUsize::new(0);
    thread::scope(|scope| {
        let tasks: Vec<_> = (0..workers).map(|_| scope.spawn(|| {
            let mut batch = EventBatch::new();
            loop {
                let Some(shard) = shards.get(next.fetch_add(1, Ordering::Relaxed)) else { break };
                let chunks = std::mem::take(&mut *shard.lock().ok()?);
                let mut winners = HashMap::<&[u8], (&[u8], &[u8])>::with_capacity(
                    chunks.iter().map(Vec::len).sum()
                );
                for line in chunks.into_iter().flatten() {
                    // Width and quote placement were already checked when indexed.
                    let mut fields = line.splitn(5, |&b| b == b',');
                    let id = unquote(fields.next().unwrap());
                    let ingest = unquote(fields.nth(2).unwrap());
                    keep_winner(&mut winners, id, ingest, line);
                }
                for (_, line) in winners.into_values() {
                    if let Some((person, at, cents, category)) = validate(line, &dims, start, end) {
                        batch.push(person, at, cents, category);
                        if batch.times.len() == MAX_ROWS {
                            tx.send(Ok(std::mem::replace(&mut batch, EventBatch::new()))).ok()?;
                        }
                    }
                }
            }
            if !batch.times.is_empty() { tx.send(Ok(batch)).ok()?; }
            Some(())
        })).collect();
        for task in tasks { task.join().ok()??; }
        Some(())
    })?;
    report.reduction_seconds = reduction.elapsed().as_secs_f64();
    *stats.lock().ok()? = report;
    Some(())
}

fn unquote(value: &[u8]) -> &[u8] {
    if value.starts_with(b"\"") { &value[1..value.len()-1] } else { value }
}

fn keep_winner<'a>(winners: &mut HashMap<&'a [u8], (&'a [u8], &'a [u8])>,
                   id: &'a [u8], ingest: &'a [u8], line: &'a [u8]) {
    let current = winners.entry(id).or_insert((ingest, line));
    if ingest < current.0 { *current = (ingest, line); }
}

#[no_mangle]
pub extern "C" fn ev_dims_new() -> *mut Dimensions {
    Box::into_raw(Box::new(Dimensions::default()))
}

#[no_mangle]
pub unsafe extern "C" fn ev_dims_add(dims: *mut Dimensions, product: bool, count: usize,
    a_offsets: *const i32, a: *const u8, b_offsets: *const i32, b: *const u8)
{
    let map = if product { &mut (*dims).products } else { &mut (*dims).aliases };
    let ao = slice::from_raw_parts(a_offsets, count + 1);
    let bo = slice::from_raw_parts(b_offsets, count + 1);
    let a = slice::from_raw_parts(a, ao[count] as usize);
    let b = slice::from_raw_parts(b, bo[count] as usize);
    for i in 0..count {
        map.insert(a[ao[i] as usize..ao[i+1] as usize].into(),
                   b[bo[i] as usize..bo[i+1] as usize].into());
    }
}

#[no_mangle]
pub unsafe extern "C" fn ev_dims_free(dims: *mut Dimensions) {
    drop(Box::from_raw(dims));
}

#[no_mangle]
pub unsafe extern "C" fn ev_open(paths: *const *const c_char, count: usize, dims: *mut Dimensions,
    start: i64, end: i64, workers: usize) -> *mut EventReader
{
    let files = if count == 0 { &[] } else { slice::from_raw_parts(paths, count) };
    let paths = files.iter().map(|&p| {
        PathBuf::from(OsStr::from_bytes(CStr::from_ptr(p).to_bytes()))
    }).collect();
    let dims = *Box::from_raw(dims);
    let (tx, rx) = sync_channel(4);
    let stats = Arc::new(Mutex::new(Stats::default()));
    let report = Arc::clone(&stats);
    let task = thread::spawn(move || {
        if process(paths, dims, start, end, workers, &tx, &report).is_none() { let _ = tx.send(Err(())); }
    });
    Box::into_raw(Box::new(EventReader(Some(rx), Some(task), stats)))
}

#[no_mangle]
pub unsafe extern "C" fn ev_stats(reader: *const EventReader) -> Stats {
    *(&(*reader).2).lock().unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn ev_close(reader: *mut EventReader) {
    let mut reader = Box::from_raw(reader);
    drop(reader.0.take());
    if let Some(task) = reader.1.take() { let _ = task.join(); }
}

#[no_mangle]
pub unsafe extern "C" fn ev_next(reader: *const EventReader, out: *mut *mut EventBatch) -> i32 {
    match (&(*reader).0).as_ref().unwrap().recv() {
        Ok(Ok(batch)) => { *out = Box::into_raw(Box::new(batch)); 1 }
        Ok(Err(())) => -1,
        Err(_) => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ev_rows(batch: *const EventBatch) -> usize { (*batch).times.len() }

#[no_mangle]
pub unsafe extern "C" fn ev_column(batch: *const EventBatch, index: usize) -> ColumnView {
    let batch = &*batch;
    let strings = |v: &Column| ColumnView {
        validity: v.validity.as_ptr(), offsets: v.offsets.as_ptr(), data: v.data.as_ptr(),
        data_bytes: v.data.len(), nulls: v.nulls,
    };
    match index {
        0 => strings(&batch.person),
        1 => ColumnView { validity: std::ptr::null(), offsets: std::ptr::null(),
            data: batch.times.as_ptr().cast(), data_bytes: batch.times.len()*8, nulls: 0 },
        2 => ColumnView { validity: batch.price_validity.as_ptr(), offsets: std::ptr::null(),
            data: batch.prices.as_ptr().cast(), data_bytes: batch.prices.len()*8, nulls: batch.price_nulls },
        _ => strings(&batch.category),
    }
}
#[no_mangle]
pub unsafe extern "C" fn ev_free(batch: *mut EventBatch) { drop(Box::from_raw(batch)); }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collisions_and_revision_order() {
        let keys: Vec<_> = (0..1000).map(|i| format!("id-{i}").into_bytes()).collect();
        let a = &keys[0];
        let b = keys.iter().skip(1).find(|x| bucket(x) == bucket(a)).unwrap();
        let mut winners = HashMap::new();
        keep_winner(&mut winners, a, b"2026-10-02T00:00:01Z", b"valid later");
        keep_winner(&mut winners, b, b"2026-10-02T00:00:01Z", b"collision");
        keep_winner(&mut winners, a, b"2026-10-02T00:00:00Z", b"invalid earlier");
        keep_winner(&mut winners, a, b"2026-10-02T00:00:00Z", b"invalid earlier");
        assert_eq!(winners.len(), 2);
        assert_eq!(winners[a.as_slice()].1, b"invalid earlier");
        assert_eq!(winners[b.as_slice()].1, b"collision");
    }

    #[test]
    fn timestamp_dialect_and_overflow() {
        assert_eq!(timestamp(b"1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(timestamp(b"1969-12-31T23:59:59Z"), Some(-1000));
        assert_eq!(timestamp(b"1970-01-01T00:15:00+00:15"), Some(0));
        assert_eq!(timestamp(b"1969-12-31T23:59:00-00:01"), Some(0));
        assert_eq!(timestamp(b"00000000000000000000001"), Some(1));
        assert_eq!(timestamp(b"9223372036854775808"), None);
        assert_eq!(timestamp(b"2025-02-29T00:00:00Z"), None);
        assert_eq!(timestamp(b"2028-02-29T00:00:00Z"), Some(1835395200000));
        assert_eq!(timestamp(b"2028-02-29T24:00:00Z"), None);
        assert_eq!(timestamp(b"2028-02-29T00:00:00.001Z"), None);
        assert_eq!(timestamp(b"2028-02-29T00:00:00Z\r"), None);
    }
}

#[cfg(native_engine)]
#[path = "engine.rs"]
pub mod engine;
