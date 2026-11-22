//! User-partitioned execution over immutable, read-only source mappings.
use super::*;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

#[path = "event_index.rs"]
mod event_index;

type Id<'a> = &'a [u8];
// Category ordinals follow byte order. Only the immutable user slice needs
// a pointer. The other fields are a signed timestamp and checked integers.
#[derive(Clone, Copy)]
struct Event<'a> {
    person: *const u8,
    timestamp: i64,
    packed: u64,
    source: PhantomData<&'a [u8]>,
}

// The pointer refers to immutable bytes in live input mappings or dimensions.
unsafe impl Send for Event<'_> {}

const FIELD_24_MAX: u32 = (1 << 24) - 1;

impl<'a> Event<'a> {
    fn new(person: Id<'a>, category: u32, timestamp: i64, price: i64) -> io::Result<Self> {
        let (Ok(person_len), Ok(price)) = (u16::try_from(person.len()), u32::try_from(price))
        else {
            return Err(io::Error::other("compact event bounds"));
        };
        if price > FIELD_24_MAX || category > FIELD_24_MAX {
            return Err(io::Error::other("compact event bounds"));
        }
        Ok(Self {
            person: person.as_ptr(),
            timestamp,
            packed: u64::from(person_len) | (u64::from(price) << 16) | (u64::from(category) << 40),
            source: PhantomData,
        })
    }

    fn person(&self) -> Id<'a> {
        // The original slice length and backing allocation remain valid.
        unsafe { slice::from_raw_parts(self.person, self.packed as u16 as usize) }
    }

    fn category(&self) -> u32 {
        (self.packed >> 40) as u32
    }

    fn price(&self) -> i64 {
        i64::from((self.packed >> 16) as u32 & FIELD_24_MAX)
    }
}

struct EngineDimensions<'maps> {
    aliases: HashMap<Id<'maps>, usize>,
    canonical: Vec<Id<'maps>>,
    products: HashMap<Bytes, u32>,
    categories: Vec<Bytes>,
    unknown_category: u32,
}

impl EngineDimensions<'_> {
    fn person<'a>(&'a self, user: Id<'a>) -> Id<'a> {
        self.aliases
            .get(user)
            .map_or(user, |&root| self.canonical[root])
    }
}

fn engine_event<'a>(
    fields: [Id<'a>; 7],
    dims: &'a EngineDimensions<'a>,
    start: i64,
    end: i64,
    ingest: i64,
) -> io::Result<Option<Event<'a>>> {
    let Some((user, time, price, product)) = check_event(fields, start, end, ingest) else {
        return Ok(None);
    };
    let person = dims.person(user);
    let category = if price.is_some() {
        dims.products
            .get(product)
            .copied()
            .unwrap_or(dims.unknown_category)
    } else {
        0
    };
    Event::new(person, category, time, price.unwrap_or(0)).map(Some)
}

type Click<'a> = (i64, Id<'a>, Id<'a>); // time, click ID, campaign

// Move whole batches between stages so workers can sort one user partition
// at a time using contiguous storage.
type Routed<'a, T> = (Id<'a>, T);
type Routes<T> = Vec<Mutex<Vec<Vec<T>>>>;
fn routes<T>() -> Routes<T> {
    (0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect()
}
fn publish<T>(dest: &Routes<T>, local: &mut [Vec<T>]) {
    for (shard, rows) in dest.iter().zip(local) {
        if !rows.is_empty() {
            shard.lock().unwrap().push(std::mem::take(rows));
        }
    }
}
const WEEK: i64 = 7 * DAY_MS;
const ROUTE_BATCH_ROWS: usize = 32_768;
const MAX_CLICK_ROWS: usize = 32_000_000;
const MAX_USER_PARTITION_ROWS: usize = 2_000_000;

fn files(path: &Path) -> io::Result<Vec<Mapping>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut maps = Vec::new();
    for item in path.read_dir()? {
        let file = item?.path();
        if file
            .file_name()
            .is_some_and(|name| name.as_bytes().ends_with(b".csv"))
        {
            maps.push(Mapping::open(&file)?);
        }
    }
    Ok(maps)
}

extern "C" {
    fn memchr(ptr: *const c_void, needle: i32, length: usize) -> *const u8;
}

fn newline(bytes: &[u8]) -> Option<usize> {
    // libc may dispatch to vector instructions supported by the host. The
    // returned pointer is within the slice when a newline is found.
    let found = unsafe { memchr(bytes.as_ptr().cast(), i32::from(b'\n'), bytes.len()) };
    (!found.is_null()).then(|| found as usize - bytes.as_ptr() as usize)
}

fn lines(mut bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        let len = newline(bytes).unwrap_or(bytes.len());
        let line = &bytes[..len];
        bytes = &bytes[(len + usize::from(len < bytes.len()))..];
        Some(line)
    })
}

fn rows(bytes: &[u8]) -> &[u8] {
    &bytes[newline(bytes).map_or(bytes.len(), |offset| offset + 1)..]
}

// Split only at physical line boundaries. No worker sees the header or a
// partial record, even when a row crosses several nominal chunk boundaries.
fn chunks(maps: &[Mapping]) -> Vec<&[u8]> {
    const TARGET: usize = 16 * 1024 * 1024;
    let mut chunks = Vec::new();
    for map in maps {
        let mut data = rows(map.bytes());
        while !data.is_empty() {
            let len = if data.len() <= TARGET {
                data.len()
            } else {
                newline(&data[TARGET..]).map_or(data.len(), |end| TARGET + end + 1)
            };
            let (chunk, tail) = data.split_at(len);
            chunks.push(chunk);
            data = tail;
        }
    }
    chunks
}

fn records<const N: usize>(bytes: &[u8]) -> impl Iterator<Item = [Id<'_>; N]> {
    lines(bytes).filter_map(|line| {
        let mut fields = [&[][..]; N];
        parse(line, &mut fields).then_some(fields)
    })
}

fn root(parents: &mut [usize], mut i: usize) -> usize {
    while i != parents[i] {
        parents[i] = parents[parents[i]];
        i = parents[i];
    }
    i
}

fn dimensions<'a>(data: &Path, maps: &'a [Mapping]) -> io::Result<EngineDimensions<'a>> {
    let mut ids = HashMap::new();
    let (mut parent, mut sizes, mut minimum) = (Vec::new(), Vec::new(), Vec::new());
    for map in maps {
        for [a, b] in records(rows(map.bytes())) {
            if a.is_empty() || b.is_empty() {
                continue;
            }
            let [mut x, mut y] = [a, b].map(|id| {
                let idx = *ids.entry(id).or_insert_with(|| {
                    let n = parent.len();
                    parent.push(n);
                    sizes.push(1usize);
                    minimum.push(id);
                    n
                });
                root(&mut parent, idx)
            });
            if x != y {
                if sizes[x] < sizes[y] {
                    std::mem::swap(&mut x, &mut y);
                }
                parent[y] = x;
                sizes[x] += sizes[y];
                minimum[x] = minimum[x].min(minimum[y]);
            }
        }
    }
    // Reuse the union-find's exact key table. Replacing each node with
    // its root avoids rebuilding and allocating copies of both byte strings.
    for node in ids.values_mut() {
        *node = root(&mut parent, *node);
    }
    drop(parent);
    drop(sizes);
    let catalog = Mapping::open(&data.join("products.csv"))?;
    let products: Vec<_> = records(rows(catalog.bytes()))
        .filter(|[id, cat]| !id.is_empty() && !cat.is_empty())
        .collect();
    let unknown = &b"unknown"[..];
    let mut categories: Vec<_> = products.iter().map(|[_, category]| *category).collect();
    categories.push(unknown);
    categories.sort_unstable();
    categories.dedup();
    if categories.len() > FIELD_24_MAX as usize + 1 {
        return Err(io::Error::other("compact category bounds"));
    }
    let unknown_category = categories.binary_search(&unknown).unwrap() as u32;
    let products = products
        .into_iter()
        .map(|[id, category]| {
            (
                id.into(),
                categories.binary_search(&category).unwrap() as u32,
            )
        })
        .collect();
    let categories = categories.into_iter().map(Into::into).collect();
    Ok(EngineDimensions {
        aliases: ids,
        canonical: minimum,
        products,
        categories,
        unknown_category,
    })
}

fn parallel<F>(jobs: usize, workers: usize, f: F) -> io::Result<()>
where
    F: Fn(usize, usize) -> io::Result<()> + Sync,
{
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let tasks: Vec<_> = (0..workers)
            .map(|worker| {
                let (next, f) = (&next, &f);
                s.spawn(move || loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= jobs {
                        return Ok::<(), io::Error>(());
                    }
                    f(worker, i)?;
                })
            })
            .collect();
        for task in tasks {
            task.join().unwrap()?;
        }
        Ok(())
    })
}

// Each slot has a seen bit and a repeated bit. A clear repeated bit proves
// an ID occurred at most once across all files. Hash collisions only send
// extra rows to exact winner selection. Malformed rows may set extra bits.
struct RepeatBitmap {
    slots: Vec<AtomicU64>,
}
impl RepeatBitmap {
    fn new(chunks: &[&[u8]]) -> Self {
        let bytes: usize = chunks.iter().map(|s| s.len()).sum();
        let words = (bytes / 512).clamp(1, 1 << 22).next_power_of_two();
        Self {
            slots: (0..words * 2).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn mark(&self, hash: usize) {
        let k = hash & (self.slots.len() * 32 - 1);
        let bit = 1u64 << (k % 32);
        let word = &self.slots[k / 32];
        let previous = word.fetch_or(bit, Ordering::Relaxed);
        if previous & bit != 0 && previous & (bit << 32) == 0 {
            word.fetch_or(bit << 32, Ordering::Relaxed);
        }
    }

    fn finish(self) -> Vec<u64> {
        self.slots
            .chunks_exact(2)
            .map(|words| {
                (words[0].load(Ordering::Relaxed) >> 32)
                    | (words[1].load(Ordering::Relaxed) & 0xffff_ffff_0000_0000)
            })
            .collect()
    }
}

fn repeat_filter(chunks: &[&[u8]], workers: usize) -> io::Result<Vec<u64>> {
    let bitmap = RepeatBitmap::new(chunks);
    parallel(chunks.len(), workers, |_, i| {
        let bytes = chunks[i];
        if std::str::from_utf8(bytes).is_err() {
            return Err(io::Error::other("invalid text"));
        }
        for line in lines(bytes) {
            let raw = line.split(|&b| b == b',').next().unwrap();
            let id = raw
                .strip_prefix(b"\"")
                .and_then(|s| s.strip_suffix(b"\""))
                .unwrap_or(raw);
            bitmap.mark(key_hash(id) as usize);
        }
        Ok(())
    })?;
    Ok(bitmap.finish())
}

const CLASSIFY_STAGES: [&str; 6] = [
    "csv_structure",
    "ingest_timestamp",
    "id_hash_and_bitmap",
    "validation_and_lookups",
    "user_buffer_push",
    "batch_publish",
];

fn record_time(duration: &mut Duration, start: Option<Instant>) {
    if let Some(start) = start {
        *duration += start.elapsed();
    }
}

fn classify<'a>(
    chunks: &[&'a [u8]],
    dims: &'a EngineDimensions<'a>,
    users: &Routes<Event<'a>>,
    start: i64,
    end: i64,
    workers: usize,
    repeats: &[u64],
    profile: bool,
) -> io::Result<Vec<Partition<'a>>> {
    let mask = repeats.len() * 64 - 1;
    let duplicates: Vec<Mutex<Partition>> =
        (0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect();
    let count = AtomicUsize::new(0);
    let stage_totals = Mutex::new([Duration::ZERO; CLASSIFY_STAGES.len()]);
    parallel(chunks.len(), workers, |_, i| {
        let mut subtimes = [Duration::ZERO; CLASSIFY_STAGES.len()];
        let mut routes = vec![Vec::new(); PARTITIONS];
        let mut uncertain = vec![Vec::new(); PARTITIONS];
        for (n, line) in lines(chunks[i]).enumerate() {
            if n % ROUTE_BATCH_ROWS == 0 {
                if count.fetch_add(ROUTE_BATCH_ROWS, Ordering::Relaxed) >= MAX_INDEXED_ROWS {
                    return Err(io::Error::other("event row guard"));
                }
                let clock = profile.then(Instant::now);
                publish(users, &mut routes);
                record_time(&mut subtimes[5], clock);
            }
            let sample = profile && n % 1024 == 0;
            let clock = sample.then(Instant::now);
            let mut fields = [&[][..]; 7];
            let valid = parse(line, &mut fields);
            record_time(&mut subtimes[0], clock);
            if !valid || fields[0].is_empty() {
                continue;
            }
            let clock = sample.then(Instant::now);
            let ingest = datetime(fields[3])
                .filter(|_| fields[3].len() == 20)
                .ok_or_else(|| io::Error::other("invalid ingest"))?;
            record_time(&mut subtimes[1], clock);
            let clock = sample.then(Instant::now);
            let hash = key_hash(fields[0]);
            let k = hash as usize & mask;
            let repeat = repeats[k / 64] & (1u64 << (k % 64)) != 0;
            record_time(&mut subtimes[2], clock);
            if repeat {
                uncertain[((hash ^ (hash >> 32)) as usize) & (PARTITIONS - 1)].push(line);
            } else {
                let clock = sample.then(Instant::now);
                let event = engine_event(fields, dims, start, end, ingest)?;
                record_time(&mut subtimes[3], clock);
                if let Some(event) = event {
                    let clock = sample.then(Instant::now);
                    routes[bucket(event.person())].push(event);
                    record_time(&mut subtimes[4], clock);
                }
            }
        }
        let clock = profile.then(Instant::now);
        publish(users, &mut routes);
        record_time(&mut subtimes[5], clock);
        if profile {
            for (total, duration) in stage_totals.lock().unwrap().iter_mut().zip(subtimes) {
                *total += duration;
            }
        }
        for (dst, rows) in duplicates.iter().zip(uncertain) {
            if !rows.is_empty() {
                dst.lock().unwrap().push(rows);
            }
        }
        Ok(())
    })?;
    if profile {
        for (name, elapsed) in CLASSIFY_STAGES
            .iter()
            .zip(stage_totals.into_inner().unwrap())
        {
            eprintln!("classify_sample {} {:.6}", name, elapsed.as_secs_f64());
        }
    }
    Ok(duplicates
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect())
}

fn route_events<'a>(
    index: Vec<Partition<'a>>,
    dims: &'a EngineDimensions<'a>,
    users: &Routes<Event<'a>>,
    start: i64,
    end: i64,
    workers: usize,
) -> io::Result<()> {
    let index: Vec<_> = index.into_iter().map(Mutex::new).collect();
    parallel(index.len(), workers, |_, i| {
        let chunks = std::mem::take(&mut *index[i].lock().unwrap());
        let n = chunks.iter().map(Vec::len).sum();
        if n > MAX_PARTITION_ROWS {
            return Err(io::Error::other("event partition guard"));
        }
        let mut winners = HashMap::with_capacity(n);
        for line in chunks.into_iter().flatten() {
            let mut columns = line.splitn(5, |&c| c == b',');
            let id = unquote(columns.next().unwrap());
            let ingest = unquote(columns.nth(2).unwrap());
            keep_winner(&mut winners, id, ingest, line);
        }
        let mut routes = vec![Vec::new(); PARTITIONS];
        for (_, line) in winners.into_values() {
            let mut fields = [&[][..]; 7];
            if parse(line, &mut fields) {
                if let Some(ingest) = datetime(fields[3]) {
                    if let Some(event) = engine_event(fields, dims, start, end, ingest)? {
                        routes[bucket(event.person())].push(event);
                    }
                }
            }
        }
        publish(users, &mut routes);
        Ok(())
    })
}

struct ClickWork<'a> {
    chunks: Vec<Id<'a>>,
    dims: &'a EngineDimensions<'a>,
    campaigns: &'a HashMap<Id<'a>, Id<'a>>,
    users: &'a Routes<Routed<'a, Click<'a>>>,
    start: i64,
    end: i64,
    count: AtomicUsize,
}

impl ClickWork<'_> {
    fn route_chunk(&self, i: usize) -> io::Result<()> {
        let mut routes = vec![Vec::new(); PARTITIONS];
        for (n, [click, id, campaign, ts]) in records(self.chunks[i]).enumerate() {
            if n % ROUTE_BATCH_ROWS == 0 {
                if self.count.fetch_add(ROUTE_BATCH_ROWS, Ordering::Relaxed) >= MAX_CLICK_ROWS {
                    return Err(io::Error::other("click row guard"));
                }
                publish(self.users, &mut routes);
            }
            if id.is_empty() || click.is_empty() || !self.campaigns.contains_key(campaign) {
                continue;
            }
            if let Some(t) =
                timestamp(ts).filter(|&t| t >= self.start.saturating_sub(WEEK) && t < self.end)
            {
                let person = self.dims.person(id);
                routes[bucket(person)].push((person, (t, click, campaign)));
            }
        }
        publish(self.users, &mut routes);
        Ok(())
    }

    fn route(&self, workers: usize) -> io::Result<()> {
        parallel(self.chunks.len(), workers, |_, i| self.route_chunk(i))
    }
}

// Hex encodes arbitrary UTF-8 bytes, including CR, whitespace and NUL, across
// the scratch-file boundary. An empty category is the only nullable output.
fn hex(out: &mut impl Write, value: Id) -> io::Result<()> {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut encoded = [0u8; 256];
    for bytes in value.chunks(encoded.len() / 2) {
        for (&byte, pair) in bytes.iter().zip(encoded.chunks_exact_mut(2)) {
            pair[0] = DIGITS[(byte >> 4) as usize];
            pair[1] = DIGITS[(byte & 15) as usize];
        }
        out.write_all(&encoded[..bytes.len() * 2])?;
    }
    Ok(())
}

#[derive(Default)]
struct Campaign {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assists: i64,
}
type Totals<'a> = HashMap<(Id<'a>, i64), Campaign>;

#[derive(Default)]
struct Workspace<'a> {
    starts: Vec<i64>,
    categories: HashMap<u32, i64>,
    ends: HashMap<Id<'a>, i64>,
}

const SUBSTAGES: [&str; 8] = [
    "partition_prepare",
    "event_sort",
    "click_sort_dedup",
    "user_group",
    "session_credit_and_category",
    "user_row_serialization",
    "assists",
    "campaign_serialization",
];

#[derive(Default)]
struct Timings {
    enabled: bool,
    elapsed: [Duration; SUBSTAGES.len()],
}
impl Timings {
    fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }
    fn record(&mut self, stage: usize, start: Option<Instant>) {
        if let Some(start) = start {
            self.elapsed[stage] += start.elapsed();
        }
    }
}

fn aggregate<'a>(
    id: Id,
    events: &[Event<'a>],
    clicks: &[Routed<'a, Click<'a>>],
    category_names: &[Bytes],
    workspace: &mut Workspace<'a>,
    totals: &mut Totals<'a>,
    out: &mut impl Write,
    timings: &mut Timings,
) -> io::Result<()> {
    let Workspace {
        starts,
        categories,
        ends,
    } = workspace;
    starts.clear();
    ends.clear();
    let (mut i, mut j) = (0, 0);
    let mut credit: Option<Click> = None;
    while i < events.len() {
        let session_clock = timings.start();
        let day = events[i].timestamp.div_euclid(DAY_MS);
        let (mut count, mut event_count, mut revenue) = (0i64, 0i64, 0i64);
        categories.clear();
        while i < events.len() && events[i].timestamp.div_euclid(DAY_MS) == day {
            let start = events[i].timestamp;
            starts.push(start);
            while j < clicks.len() && clicks[j].1 .0 <= start {
                let click = clicks[j].1;
                if credit.map_or(true, |c| c.0 != click.0) {
                    credit = Some(click);
                }
                j += 1;
            }
            let campaign = credit
                .filter(|c| c.0 >= start.saturating_sub(WEEK))
                .map_or(&b"organic"[..], |c| c.2);
            let mut session_revenue = 0;
            loop {
                let t = events[i].timestamp;
                let price = events[i].price();
                if price > 0 {
                    *categories.entry(events[i].category()).or_default() += price;
                    session_revenue += price;
                }
                event_count += 1;
                i += 1;
                if i == events.len() || events[i].timestamp > t.saturating_add(1_800_000) {
                    break;
                }
            }
            let c = totals.entry((campaign, day)).or_default();
            c.sessions += 1;
            c.purchasing += i64::from(session_revenue > 0);
            c.revenue += session_revenue;
            count += 1;
            revenue += session_revenue;
        }
        let best = categories
            .iter()
            .max_by(|(a, x), (b, y)| x.cmp(y).then_with(|| b.cmp(a)));
        timings.record(4, session_clock);
        let write_clock = timings.start();
        hex(out, id)?;
        write!(out, ",{},{},{},{},", day, count, event_count, revenue)?;
        if let Some((cat, _)) = best {
            hex(out, &category_names[*cat as usize])?;
        }
        writeln!(out)?;
        timings.record(5, write_clock);
    }
    let assist_clock = timings.start();
    for &(_, (t, _, campaign)) in clicks {
        let end = t.saturating_add(WEEK);
        let previous = ends.insert(campaign, end);
        let lo = previous.map_or(t, |v: i64| t.max(v.saturating_add(1)));
        let mut pos = starts.partition_point(|&s| s < lo);
        let stop = starts.partition_point(|&s| s <= end);
        while pos < stop {
            let day = starts[pos].div_euclid(DAY_MS);
            let n = starts[pos..stop].partition_point(|&s| s.div_euclid(DAY_MS) == day);
            totals.entry((campaign, day)).or_default().assists += n as i64;
            pos += n;
        }
    }
    timings.record(6, assist_clock);
    Ok(())
}

fn drain_partition<T>(routes: &Routes<T>, partition: usize) -> io::Result<Vec<T>> {
    let chunks = std::mem::take(&mut *routes[partition].lock().unwrap());
    let rows: usize = chunks.iter().map(Vec::len).sum();
    if rows > MAX_USER_PARTITION_ROWS {
        return Err(io::Error::other("user partition guard"));
    }
    let mut rows = Vec::with_capacity(rows);
    for mut chunk in chunks {
        rows.append(&mut chunk);
    }
    Ok(rows)
}

pub fn run() -> io::Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected data directory, scratch directory, start, end and worker count",
        ));
    }
    let data = Path::new(&args[1]);
    let scratch = Path::new(&args[2]);
    let start = args[3].to_str().unwrap().parse().unwrap();
    let end = args[4].to_str().unwrap().parse().unwrap();
    let workers: usize = args[5]
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap()
        .clamp(1, 8);
    let mut clock = Instant::now();
    let profile = std::env::var("SHOPPING_SESSIONS_PROFILE").as_deref() == Ok("1");
    let mut stage = |name: &str| {
        if profile {
            eprintln!("native {} {:.6}", name, clock.elapsed().as_secs_f64());
        }
        clock = Instant::now();
    };
    let event_maps = files(&data.join("events"))?;
    let event_chunks = chunks(&event_maps);
    let link_maps = files(&data.join("links"))?;
    stage("event_map_and_chunk");
    let build_dimensions = || {
        let started = Instant::now();
        let dims = dimensions(data, &link_maps)?;
        if profile {
            eprintln!(
                "native identity_and_dimensions {:.6}",
                started.elapsed().as_secs_f64()
            );
        }
        Ok::<_, io::Error>(dims)
    };
    enum EventIndex {
        Compact(event_index::Index),
        Bitmap(Vec<u64>),
    }
    let scan_mode = std::env::var("SHOPPING_SESSIONS_EVENT_SCAN").unwrap_or_default();
    let build_index = |index_workers| {
        let started = Instant::now();
        let index = if scan_mode == "two-pass" {
            EventIndex::Bitmap(repeat_filter(&event_chunks, index_workers)?)
        } else if scan_mode == "sparse" {
            EventIndex::Compact(event_index::Index::sparse(repeat_filter(
                &event_chunks,
                index_workers,
            )?))
        } else {
            match event_index::prepare(&event_chunks, index_workers, scan_mode == "scalar", profile)
            {
                Ok(index) => EventIndex::Compact(index),
                Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                    if profile {
                        eprintln!("compact index fallback: {error}");
                    }
                    EventIndex::Bitmap(repeat_filter(&event_chunks, index_workers)?)
                }
                Err(error) => return Err(error),
            }
        };
        if profile {
            eprintln!("native event_index {:.6}", started.elapsed().as_secs_f64());
        }
        Ok::<_, io::Error>(index)
    };
    // Dimension construction needs one CPU. The repeat bitmap depends only
    // on event IDs and can use the remaining CPUs within the same budget.
    let (dims, repeats) = if workers > 1 {
        thread::scope(|scope| {
            let index = scope.spawn(|| build_index(workers - 1));
            let dims = build_dimensions();
            Ok::<_, io::Error>((dims?, index.join().unwrap()?))
        })?
    } else {
        (build_dimensions()?, build_index(workers)?)
    };
    let catalog = Mapping::open(&data.join("campaigns.csv"))?;
    let campaigns: HashMap<_, _> = records(rows(catalog.bytes()))
        .filter(|[id, _]| !id.is_empty())
        .map(|[id, ch]| (id, ch))
        .collect();
    stage("dimensions_and_index_wall");
    let users = routes();
    let clicks = routes();
    let click_maps = files(&data.join("clicks"))?;
    let click_work = ClickWork {
        chunks: chunks(&click_maps),
        dims: &dims,
        campaigns: &campaigns,
        users: &clicks,
        start,
        end,
        count: AtomicUsize::new(0),
    };
    let overlap = matches!(&repeats, EventIndex::Compact(_))
        && std::env::var("SHOPPING_SESSIONS_SCHEDULER").as_deref() == Ok("shared");
    match repeats {
        EventIndex::Compact(index) => {
            let revisions = event_index::route(
                index,
                &event_chunks,
                &dims,
                &users,
                start,
                end,
                workers,
                overlap.then_some(&click_work),
            )?;
            stage(if overlap {
                "event_and_click_classification"
            } else {
                "event_classification"
            });
            event_index::resolve(revisions, &dims, &users, start, end, workers, profile)?;
        }
        EventIndex::Bitmap(repeats) => {
            let index = classify(
                &event_chunks,
                &dims,
                &users,
                start,
                end,
                workers,
                &repeats,
                profile,
            )?;
            stage("event_classification");
            route_events(index, &dims, &users, start, end, workers)?;
        }
    }
    stage("event_validate_and_partition");
    if !overlap {
        click_work.route(workers)?;
        stage("click_partition");
    }
    // Process the largest partitions first so a skewed partition cannot
    // become the final worker's entire tail.
    let mut partitions: Vec<_> = (0..PARTITIONS)
        .map(|partition| {
            let event_rows: usize = users[partition].lock().unwrap().iter().map(Vec::len).sum();
            let click_rows: usize = clicks[partition].lock().unwrap().iter().map(Vec::len).sum();
            (std::cmp::Reverse(event_rows + click_rows), partition)
        })
        .collect();
    partitions.sort_unstable();
    if profile {
        let count: usize = partitions.iter().map(|(size, _)| size.0).sum();
        let at = |i: usize| (partitions[i].0).0;
        eprintln!(
            "partition_rows total {count} min {} median {} max {}",
            at(partitions.len() - 1),
            at(partitions.len() / 2),
            at(0)
        );
    }
    let next_partition = AtomicUsize::new(0);
    let total_substages = Mutex::new([Duration::ZERO; SUBSTAGES.len()]);
    parallel(workers, workers, |_, i| {
        let worker_start = profile.then(Instant::now);
        let mut timings = Timings::default();
        let mut out = BufWriter::new(File::create(scratch.join(format!("user-{i}.csv")))?);
        let mut totals = Totals::new();
        let mut workspace = Workspace::default();
        while let Some(&(_, partition)) =
            partitions.get(next_partition.fetch_add(1, Ordering::Relaxed))
        {
            timings.enabled = profile;
            let prepare_clock = timings.start();
            let mut events = drain_partition(&users, partition)?;
            let mut history = drain_partition(&clicks, partition)?;
            timings.record(0, prepare_clock);
            let sort_clock = timings.start();
            events.sort_unstable_by(|a, b| {
                a.person()
                    .cmp(b.person())
                    .then_with(|| a.timestamp.cmp(&b.timestamp))
            });
            timings.record(1, sort_clock);
            let sort_clock = timings.start();
            history.sort_unstable();
            // All copies of a click are identical and route to the same user.
            history.dedup();
            timings.record(2, sort_clock);
            // Sample four evenly spaced groups of partitions to limit clock overhead.
            timings.enabled = profile && (partition / workers) % 8 == 0;
            let mut remaining_clicks = history.as_slice();
            for person_events in events.chunk_by(|a, b| a.person() == b.person()) {
                let group_clock = timings.start();
                let person = person_events[0].person();
                let from = remaining_clicks.partition_point(|row| row.0 < person);
                remaining_clicks = &remaining_clicks[from..];
                let through = remaining_clicks.partition_point(|row| row.0 == person);
                let (person_clicks, remaining) = remaining_clicks.split_at(through);
                remaining_clicks = remaining;
                timings.record(3, group_clock);
                aggregate(
                    person,
                    person_events,
                    person_clicks,
                    &dims.categories,
                    &mut workspace,
                    &mut totals,
                    &mut out,
                    &mut timings,
                )?;
            }
        }
        out.flush()?;
        timings.enabled = profile;
        let write_clock = timings.start();
        let mut out = BufWriter::new(File::create(scratch.join(format!("campaign-{i}.csv")))?);
        for ((id, day), v) in totals {
            hex(&mut out, id)?;
            out.write_all(b",")?;
            hex(
                &mut out,
                campaigns
                    .get(id)
                    .copied()
                    .filter(|ch| !ch.is_empty())
                    .unwrap_or(b"organic"),
            )?;
            writeln!(
                out,
                ",{},{},{},{},{}",
                day, v.sessions, v.purchasing, v.revenue, v.assists
            )?;
        }
        out.flush()?;
        timings.record(7, write_clock);
        if profile {
            for (total, duration) in total_substages
                .lock()
                .unwrap()
                .iter_mut()
                .zip(timings.elapsed)
            {
                *total += duration;
            }
        }
        if let Some(start) = worker_start {
            eprintln!("worker {} {:.6}", i, start.elapsed().as_secs_f64());
        }
        Ok(())
    })?;
    stage("session_attribution_and_aggregate_staging");
    if profile {
        for (name, elapsed) in SUBSTAGES.iter().zip(total_substages.into_inner().unwrap()) {
            eprintln!("substage {} {:.6}", name, elapsed.as_secs_f64());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_events_retain_exact_slices_and_signed_timestamps() {
        let long = vec![b'x'; usize::from(u16::MAX)];
        let too_long = vec![b'x'; long.len() + 1];
        let event = Event::new(&long, FIELD_24_MAX, i64::MIN, 10_000_000).unwrap();
        assert_eq!(event.person(), long);
        assert_eq!(event.category(), FIELD_24_MAX);
        assert_eq!(event.timestamp, i64::MIN);
        assert_eq!(event.price(), 10_000_000);
        assert!(Event::new(&too_long, 0, i64::MAX, 0).is_err());
        assert!(Event::new(b"person", FIELD_24_MAX + 1, 0, 0).is_err());
        assert!(Event::new(b"person", 1, 0, -1).is_err());
        assert_eq!(std::mem::size_of::<Event<'_>>(), 24);
    }

    #[test]
    fn physical_lines_preserve_cr_and_unterminated_tail() {
        let input = b"header\nleft,right\r\n\nlast,field";
        assert_eq!(
            lines(rows(input)).collect::<Vec<_>>(),
            [&b"left,right\r"[..], b"", b"last,field"],
        );
    }

    #[test]
    fn repeat_filter_finds_quoted_duplicates_and_routes_collisions() {
        let chunks: &[&[u8]] = &[b"id,a\n\"id\",b", b"id,c\nsolo,d"];
        let repeats = repeat_filter(chunks, 2).unwrap();
        let bit = |id: &[u8]| 1u64 << (key_hash(id) as usize % 64);
        assert_ne!(repeats[0] & bit(b"id"), 0);

        let original = b"one";
        let other = (0..)
            .map(|n| n.to_string())
            .find(|s| bit(s.as_bytes()) == bit(original))
            .unwrap();
        let data = format!("one,x\n{other},x");
        assert_ne!(
            repeat_filter(&[data.as_bytes()], 2).unwrap()[0] & bit(original),
            0
        );
        assert_eq!(repeat_filter(&[b"one,x"], 2).unwrap()[0], 0);
    }
}
