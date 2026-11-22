//! Compact field offsets over read-only event chunks.
use super::*;

type Delimiters = ([u8; 6], bool);
type Scan = fn(&[u8]) -> Option<Delimiters>;

#[derive(Clone, Copy)]
struct Row {
    offset: u32,
    hash: u32,
    cuts: [u8; 6],
    length: u8,
    quoted: bool,
}

impl Row {
    fn line(self, chunk: &[u8]) -> &[u8] {
        let start = self.offset as usize;
        &chunk[start..start + usize::from(self.length)]
    }

    fn fields(self, line: &[u8]) -> [Id<'_>; 7] {
        fields(line, self.cuts, self.quoted)
    }
}

fn fields(line: &[u8], cuts: [u8; 6], quoted: bool) -> [Id<'_>; 7] {
    let [a, b, c, d, e, f] = cuts.map(usize::from);
    let mut values = [
        &line[..a],
        &line[a + 1..b],
        &line[b + 1..c],
        &line[c + 1..d],
        &line[d + 1..e],
        &line[e + 1..f],
        &line[f + 1..],
    ];
    if quoted {
        values = values.map(trim_quotes);
    }
    values
}

fn trim_quotes(field: &[u8]) -> &[u8] {
    if let Some(rest) = field.strip_prefix(b"\"") {
        rest.strip_suffix(b"\"").unwrap_or(field)
    } else {
        field
    }
}

#[derive(Default)]
struct Cuts {
    found: [u8; 6],
    count: usize,
    quoted: bool,
}
impl Cuts {
    fn comma(&mut self, offset: usize) -> Option<()> {
        *self.found.get_mut(self.count)? = offset as u8;
        self.count += 1;
        Some(())
    }

    fn mask(&mut self, mut commas: u32, offset: usize, quoted: bool) -> Option<()> {
        while commas != 0 {
            self.comma(offset + commas.trailing_zeros() as usize)?;
            commas &= commas - 1;
        }
        self.quoted |= quoted;
        Some(())
    }

    fn tail(&mut self, bytes: &[u8], offset: usize) -> Option<Delimiters> {
        for (i, &byte) in bytes.iter().enumerate() {
            if byte == b',' {
                self.comma(offset + i)?;
            }
            self.quoted |= byte == b'"';
        }
        (self.count == self.found.len()).then_some((self.found, self.quoted))
    }
}

fn scalar(line: &[u8]) -> Option<Delimiters> {
    Cuts::default().tail(line, 0)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn neon(line: &[u8]) -> Option<Delimiters> {
    use std::arch::aarch64::*;
    let mut cuts = Cuts::default();
    const POWERS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
    let weights = vld1q_u8(POWERS.as_ptr());
    let mut blocks = line.chunks_exact(16);
    for (i, block) in blocks.by_ref().enumerate() {
        // chunks_exact guarantees a readable 16-byte vector.
        let values = vld1q_u8(block.as_ptr());
        let commas = vandq_u8(vceqq_u8(values, vdupq_n_u8(b',')), weights);
        let bits = u32::from(vaddv_u8(vget_low_u8(commas)))
            | (u32::from(vaddv_u8(vget_high_u8(commas))) << 8);
        let quoted = vmaxvq_u8(vceqq_u8(values, vdupq_n_u8(b'"'))) != 0;
        cuts.mask(bits, i * 16, quoted)?;
    }
    let tail = blocks.remainder();
    cuts.tail(tail, line.len() - tail.len())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sse2(line: &[u8]) -> Option<Delimiters> {
    use std::arch::x86_64::*;
    let mut cuts = Cuts::default();
    let mut blocks = line.chunks_exact(16);
    for (i, block) in blocks.by_ref().enumerate() {
        // Unaligned loads are valid over each full-size block.
        let values = _mm_loadu_si128(block.as_ptr().cast());
        let commas = _mm_movemask_epi8(_mm_cmpeq_epi8(values, _mm_set1_epi8(b',' as i8)));
        let quoted = _mm_movemask_epi8(_mm_cmpeq_epi8(values, _mm_set1_epi8(b'"' as i8))) != 0;
        cuts.mask(commas as u32, i * 16, quoted)?;
    }
    let tail = blocks.remainder();
    cuts.tail(tail, line.len() - tail.len())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2(line: &[u8]) -> Option<Delimiters> {
    use std::arch::x86_64::*;
    let mut cuts = Cuts::default();
    let mut blocks = line.chunks_exact(32);
    for (i, block) in blocks.by_ref().enumerate() {
        // The full-size chunk permits an unaligned 32-byte load.
        let values = _mm256_loadu_si256(block.as_ptr().cast());
        let commas = _mm256_movemask_epi8(_mm256_cmpeq_epi8(values, _mm256_set1_epi8(b',' as i8)));
        let quoted =
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(values, _mm256_set1_epi8(b'"' as i8))) != 0;
        cuts.mask(commas as u32, i * 32, quoted)?;
    }
    let tail = blocks.remainder();
    cuts.tail(tail, line.len() - tail.len())
}

fn scanner(portable: bool) -> Scan {
    if !portable {
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return |line| unsafe { neon(line) };
        }
        #[cfg(target_arch = "x86_64")]
        if std::env::var("SHOPPING_SESSIONS_X86_SIMD").as_deref() == Ok("avx2")
            && std::arch::is_x86_feature_detected!("avx2")
        {
            return |line| unsafe { avx2(line) };
        }
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("sse2") {
            return |line| unsafe { sse2(line) };
        }
    }
    scalar
}

fn parse_row(line: &[u8], offset: u32, scan: Scan) -> Option<Row> {
    let (cuts, quoted) = scan(line)?;
    let mut row = Row {
        offset,
        hash: 0,
        cuts,
        length: line.len() as u8,
        quoted,
    };
    let id = if quoted {
        let fields = row.fields(line);
        if fields.iter().any(|field| field.contains(&b'"')) {
            return None;
        }
        fields[0]
    } else {
        &line[..usize::from(cuts[0])]
    };
    if id.is_empty() {
        return None;
    }
    row.hash = key_hash(id) as u32;
    Some(row)
}

pub(super) struct Index {
    rows: Option<Vec<Mutex<Vec<Row>>>>,
    repeats: Vec<u64>,
}

impl Index {
    pub(super) fn sparse(repeats: Vec<u64>) -> Self {
        Self {
            rows: None,
            repeats,
        }
    }
}

pub(super) fn prepare(
    chunks: &[&[u8]],
    workers: usize,
    portable: bool,
    profile: bool,
) -> io::Result<Index> {
    let bitmap = RepeatBitmap::new(chunks);
    let rows: Vec<Mutex<Vec<Row>>> = (0..chunks.len()).map(|_| Mutex::default()).collect();
    let physical_rows = AtomicUsize::new(0);
    let scan = scanner(portable);
    parallel(chunks.len(), workers, |_, i| {
        let bytes = chunks[i];
        if bytes.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "event chunk exceeds index bounds",
            ));
        }
        if std::str::from_utf8(bytes).is_err() {
            return Err(io::Error::other("invalid text"));
        }
        let mut found = rows[i].lock().unwrap();
        let mut scanned = 0;
        for line in lines(bytes) {
            scanned += 1;
            if scanned % ROUTE_BATCH_ROWS == 0
                && !claim_rows(&physical_rows, ROUTE_BATCH_ROWS, MAX_INDEXED_ROWS)
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "event row guard",
                ));
            }
            if line.len() > u8::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "event line exceeds index bounds",
                ));
            }
            let offset = line.as_ptr() as usize - bytes.as_ptr() as usize;
            if let Some(row) = parse_row(line, offset as u32, scan) {
                bitmap.mark(row.hash as usize);
                found.push(row);
            }
        }
        if !claim_rows(&physical_rows, scanned % ROUTE_BATCH_ROWS, MAX_INDEXED_ROWS) {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "event row guard"));
        }
        Ok(())
    })?;
    if profile {
        let mut count = 0usize;
        let mut capacity_bytes = 0usize;
        for chunk in &rows {
            let chunk = chunk.lock().unwrap();
            count += chunk.len();
            capacity_bytes += chunk.capacity() * std::mem::size_of::<Row>();
        }
        eprintln!("index_metadata rows {count} capacity_bytes {capacity_bytes}");
    }
    Ok(Index {
        rows: Some(rows),
        repeats: bitmap.finish(),
    })
}

// Retain field offsets for repeat-filter hits, including collisions. The
// winner reducer can compare exact IDs and ingest times without another parse.
#[derive(Clone, Copy)]
struct Revision<'a> {
    line: Id<'a>,
    ingest: i64,
    cuts: [u8; 6],
    quoted: bool,
}
impl<'a> Revision<'a> {
    fn new(line: Id<'a>, row: Row, ingest: i64) -> Self {
        Self {
            line,
            ingest,
            cuts: row.cuts,
            quoted: row.quoted,
        }
    }
}

pub(super) struct Revisions<'a>(Vec<Mutex<Vec<Vec<Revision<'a>>>>>);

pub(super) fn route<'a>(
    index: Index,
    chunks: &[&'a [u8]],
    dims: &'a EngineDimensions<'a>,
    users: &Routes<Event<'a>>,
    start: i64,
    end: i64,
    workers: usize,
    click_work: Option<&ClickWork<'a>>,
) -> io::Result<Revisions<'a>> {
    let mask = index.repeats.len() * 64 - 1;
    let duplicates = Revisions((0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect());
    let scan = scanner(false);
    let physical_rows = AtomicUsize::new(0);
    let mut jobs: Vec<_> = chunks
        .iter()
        .enumerate()
        .map(|(i, bytes)| (bytes.len(), i))
        .collect();
    if let Some(clicks) = click_work {
        jobs.extend(
            clicks
                .chunks
                .iter()
                .enumerate()
                .map(|(i, bytes)| (bytes.len(), chunks.len() + i)),
        );
        jobs.sort_unstable_by_key(|job| std::cmp::Reverse(job.0));
    }
    parallel(jobs.len(), workers, |_, job| {
        let i = jobs[job].1;
        if i >= chunks.len() {
            return click_work
                .expect("click job needs a scan source")
                .route_chunk(i - chunks.len());
        }
        let mut routes = vec![Vec::new(); PARTITIONS];
        let mut uncertain: Vec<Vec<Revision>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
        let mut use_row = |n: usize, row: Row, line: Id<'a>| -> io::Result<()> {
            if n % ROUTE_BATCH_ROWS == 0 {
                publish(users, &mut routes);
            }
            let fields = row.fields(line);
            let ingest = datetime(fields[3])
                .filter(|_| fields[3].len() == 20)
                .ok_or_else(|| io::Error::other("invalid ingest"))?;
            let k = row.hash as usize & mask;
            if index.repeats[k / 64] & (1u64 << (k % 64)) != 0 {
                uncertain[(row.hash >> 24) as usize].push(Revision::new(line, row, ingest));
            } else if let Some(event) = engine_event(fields, dims, start, end, ingest)? {
                routes[bucket(event.person())].push(event);
            }
            Ok(())
        };
        if let Some(chunks_rows) = &index.rows {
            let rows = std::mem::take(&mut *chunks_rows[i].lock().unwrap());
            for (n, row) in rows.into_iter().enumerate() {
                use_row(n, row, row.line(chunks[i]))?;
            }
        } else {
            let mut accepted = 0;
            let mut scanned = 0;
            for line in lines(chunks[i]) {
                scanned += 1;
                if scanned % ROUTE_BATCH_ROWS == 0
                    && !claim_rows(&physical_rows, ROUTE_BATCH_ROWS, MAX_INDEXED_ROWS)
                {
                    return Err(io::Error::other("event row guard"));
                }
                if line.len() > u8::MAX as usize {
                    return Err(io::Error::other("event line exceeds sparse index bounds"));
                }
                if let Some(row) = parse_row(line, 0, scan) {
                    use_row(accepted, row, line)?;
                    accepted += 1;
                }
            }
            if !claim_rows(&physical_rows, scanned % ROUTE_BATCH_ROWS, MAX_INDEXED_ROWS) {
                return Err(io::Error::other("event row guard"));
            }
        }
        publish(users, &mut routes);
        for (dest, records) in duplicates.0.iter().zip(uncertain) {
            if !records.is_empty() {
                dest.lock().unwrap().push(records);
            }
        }
        Ok(())
    })?;
    Ok(duplicates)
}

fn partition_winners<'a>(
    batches: Vec<Vec<Revision<'a>>>,
    unique_limit: usize,
) -> io::Result<HashMap<Id<'a>, Revision<'a>>> {
    use std::collections::hash_map::Entry;
    let count: usize = batches.iter().map(Vec::len).sum();
    let mut winners = HashMap::with_capacity(count.min(unique_limit).min(65_536));
    let mut distinct = 0;
    for candidate in batches.into_iter().flatten() {
        let raw_id = &candidate.line[..usize::from(candidate.cuts[0])];
        let id = trim_quotes(raw_id);
        match winners.entry(id) {
            Entry::Vacant(entry) => {
                if distinct == unique_limit {
                    return Err(io::Error::other("distinct event partition guard"));
                }
                entry.insert(candidate);
                distinct += 1;
            }
            Entry::Occupied(mut entry) => {
                if candidate.ingest < entry.get().ingest {
                    entry.insert(candidate);
                }
            }
        }
    }
    Ok(winners)
}

pub(super) fn resolve<'a>(
    revisions: Revisions<'a>,
    dims: &'a EngineDimensions<'a>,
    users: &Routes<Event<'a>>,
    start: i64,
    end: i64,
    workers: usize,
    profile: bool,
) -> io::Result<()> {
    if profile {
        let (mut count, mut capacity) = (0, 0);
        for partition in &revisions.0 {
            for batch in partition.lock().unwrap().iter() {
                count += batch.len();
                capacity += batch.capacity();
            }
        }
        let record_bytes = std::mem::size_of::<Revision>();
        let capacity_bytes = capacity * record_bytes;
        eprintln!(
            "repeat_metadata rows {count} record_bytes {record_bytes} capacity_bytes {capacity_bytes}"
        );
    }
    let winner_rows = AtomicUsize::new(0);
    let winner_capacity = AtomicUsize::new(0);
    parallel(PARTITIONS, workers, |_, i| {
        let batches = std::mem::take(&mut *revisions.0[i].lock().unwrap());
        let winners = partition_winners(batches, MAX_PARTITION_ROWS)?;
        if profile {
            winner_rows.fetch_add(winners.len(), Ordering::Relaxed);
            winner_capacity.fetch_add(winners.capacity(), Ordering::Relaxed);
        }
        let mut routes = vec![Vec::new(); PARTITIONS];
        for winner in winners.into_values() {
            let fields = fields(winner.line, winner.cuts, winner.quoted);
            if let Some(event) = engine_event(fields, dims, start, end, winner.ingest)? {
                routes[bucket(event.person())].push(event);
            }
        }
        publish(users, &mut routes);
        Ok(())
    })?;
    if profile {
        eprintln!(
            "winner_tables rows {} capacity {}",
            winner_rows.load(Ordering::Relaxed),
            winner_capacity.load(Ordering::Relaxed)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_matches_reference_for_boundaries_and_invalid_records() {
        #[cfg(target_arch = "x86_64")]
        eprintln!(
            "dispatch sse2={} avx2={}",
            std::arch::is_x86_feature_detected!("sse2"),
            std::arch::is_x86_feature_detected!("avx2")
        );
        let mut cases: Vec<Vec<u8>> = vec![
            b"x,\"u\",t,i,p,q,\r".to_vec(),
            b"\"x\",u,t,i,p,q,\"\"".to_vec(),
            b"x,u,\"t,i,p,q,v".to_vec(),
            b"x,u,t,i,p,q,v,w".to_vec(),
            b"x,u,t,i,p,q".to_vec(),
            b",u,t,i,p,q,v".to_vec(),
            vec![],
        ];
        for n in 0..230 {
            let prefix = "a".repeat(n);
            cases.push(format!("{prefix}z,\"猫\",t,i,p,q,r").into_bytes());
            cases.push(format!("{prefix}z,u,t,i,p,\"q\"a,r").into_bytes());
        }
        for line in cases {
            let mut expected = [&[][..]; 7];
            let expected =
                (parse(&line, &mut expected) && !expected[0].is_empty()).then_some(expected);
            let mut implementations = vec![scanner(true), scanner(false)];
            #[cfg(target_arch = "x86_64")]
            if std::arch::is_x86_feature_detected!("sse2") {
                implementations.push(|line| unsafe { sse2(line) });
            }
            #[cfg(target_arch = "x86_64")]
            if std::arch::is_x86_feature_detected!("avx2") {
                implementations.push(|line| unsafe { avx2(line) });
            }
            for scan in implementations.drain(..) {
                let actual = parse_row(&line, 0, scan).map(|row| row.fields(&line));
                assert_eq!(actual, expected, "{line:?}");
            }
        }
        assert_eq!(std::mem::size_of::<Row>(), 16);
    }

    #[test]
    fn repeated_rows_do_not_exhaust_distinct_key_guard() {
        fn rev(line: &[u8], ingest: i64) -> Revision<'_> {
            Revision::new(line, parse_row(line, 0, scalar).unwrap(), ingest)
        }
        assert_eq!(std::mem::size_of::<Revision>(), 32);
        let newer = rev(b"same,u,1,i,view,p,0", 9);
        let older_invalid = rev(b"\"same\",u,bad,i,view,p,0", 1);
        let other = rev(b"other,u,1,i,view,p,0", 8);
        let winners =
            partition_winners(vec![vec![newer; 4096], vec![older_invalid, other]], 2).unwrap();
        assert_eq!(winners.get(&b"same"[..]).unwrap().line, older_invalid.line);
        let third = rev(b"third,u,1,i,view,p,0", 7);
        assert!(partition_winners(vec![vec![newer, other, third]], 2).is_err());
    }
}
