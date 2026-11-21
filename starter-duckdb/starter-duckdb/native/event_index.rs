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
        let [a, b, c, d, e, f] = self.cuts.map(usize::from);
        let mut fields = [
            &line[..a],
            &line[a + 1..b],
            &line[b + 1..c],
            &line[c + 1..d],
            &line[d + 1..e],
            &line[e + 1..f],
            &line[f + 1..],
        ];
        if self.quoted {
            fields = fields.map(trim_quotes);
        }
        fields
    }
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

fn scanner(portable: bool) -> Scan {
    if !portable {
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return |line| unsafe { neon(line) };
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
    rows: Vec<Mutex<Vec<Row>>>,
    repeats: Vec<u64>,
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
        for (n, line) in lines(bytes).enumerate() {
            if n % ROUTE_BATCH_ROWS == 0
                && physical_rows.fetch_add(ROUTE_BATCH_ROWS, Ordering::Relaxed) >= MAX_INDEXED_ROWS
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
        rows,
        repeats: bitmap.finish(),
    })
}

pub(super) fn route<'a>(
    index: Index,
    chunks: &[&'a [u8]],
    dims: &'a EngineDimensions,
    users: &Routes<Event<'a>>,
    start: i64,
    end: i64,
    workers: usize,
) -> io::Result<Vec<Partition<'a>>> {
    let mask = index.repeats.len() * 64 - 1;
    let duplicates: Vec<Mutex<Partition>> =
        (0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect();
    parallel(chunks.len(), workers, |_, i| {
        let rows = std::mem::take(&mut *index.rows[i].lock().unwrap());
        let mut routes = vec![Vec::new(); PARTITIONS];
        let mut uncertain = vec![Vec::new(); PARTITIONS];
        for (n, row) in rows.into_iter().enumerate() {
            if n % ROUTE_BATCH_ROWS == 0 {
                publish(users, &mut routes);
            }
            let line = row.line(chunks[i]);
            let fields = row.fields(line);
            let ingest = datetime(fields[3])
                .filter(|_| fields[3].len() == 20)
                .ok_or_else(|| io::Error::other("invalid ingest"))?;
            let k = row.hash as usize & mask;
            if index.repeats[k / 64] & (1u64 << (k % 64)) != 0 {
                uncertain[(row.hash >> 24) as usize].push(line);
            } else if let Some(event) = engine_event(fields, dims, start, end, ingest)? {
                routes[bucket(event.person())].push(event);
            }
        }
        publish(users, &mut routes);
        for (dest, records) in duplicates.iter().zip(uncertain) {
            if !records.is_empty() {
                dest.lock().unwrap().push(records);
            }
        }
        Ok(())
    })?;
    Ok(duplicates
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_matches_reference_for_boundaries_and_invalid_records() {
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
            for scan in [scanner(true), scanner(false)] {
                let actual = parse_row(&line, 0, scan).map(|row| row.fields(&line));
                assert_eq!(actual, expected, "{line:?}");
            }
        }
        assert_eq!(std::mem::size_of::<Row>(), 16);
    }
}
