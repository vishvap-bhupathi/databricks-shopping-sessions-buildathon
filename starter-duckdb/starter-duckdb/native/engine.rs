//! User-partitioned execution over immutable, read-only source mappings.
use super::*;
use std::io::{BufWriter, Write};
use std::path::Path;

type Id<'a> = &'a [u8];
type Event<'a> = (i64, i64, Id<'a>);
type Click<'a> = (i64, Id<'a>, Id<'a>); // time, click ID, campaign

// Move whole batches between stages so workers can sort one user partition
// at a time using contiguous storage.
type Routed<'a, T> = (Id<'a>, T);
type Routes<'a, T> = Vec<Mutex<Vec<Vec<Routed<'a, T>>>>>;
fn routes<'a, T>() -> Routes<'a, T> {
    (0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect()
}
fn publish<'a, T>(dest: &Routes<'a, T>, local: &mut [Vec<(Id<'a>, T)>]) {
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

fn dimensions(data: &Path) -> io::Result<Dimensions> {
    let maps = files(&data.join("links"))?;
    let mut ids = HashMap::new();
    let (mut parent, mut sizes, mut minimum) = (Vec::new(), Vec::new(), Vec::new());
    for map in &maps {
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
    let aliases = ids
        .into_iter()
        .map(|(id, i)| (id.into(), minimum[root(&mut parent, i)].into()))
        .collect();
    let products = records(rows(Mapping::open(&data.join("products.csv"))?.bytes()))
        .filter(|[id, cat]| !id.is_empty() && !cat.is_empty())
        .map(|[id, cat]| (id.into(), cat.into()))
        .collect();
    Ok(Dimensions { aliases, products })
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
fn repeat_filter(chunks: &[&[u8]], workers: usize) -> io::Result<Vec<u64>> {
    let bytes: usize = chunks.iter().map(|s| s.len()).sum();
    let words = (bytes / 512).clamp(1, 1 << 22).next_power_of_two();
    let mask = words * 64 - 1;
    let merged = Mutex::new((vec![0u64; words], vec![0u64; words]));
    let next = AtomicUsize::new(0);
    parallel(workers, workers, |_, _| {
        let mut seen = vec![0u64; words];
        let mut repeated = vec![0u64; words];
        while let Some(&bytes) = chunks.get(next.fetch_add(1, Ordering::Relaxed)) {
            if std::str::from_utf8(bytes).is_err() {
                return Err(io::Error::other("invalid text"));
            }
            for line in lines(bytes) {
                let raw = line.split(|&b| b == b',').next().unwrap();
                let id = raw
                    .strip_prefix(b"\"")
                    .and_then(|s| s.strip_suffix(b"\""))
                    .unwrap_or(raw);
                let k = key_hash(id) as usize & mask;
                let bit = 1u64 << (k % 64);
                repeated[k / 64] |= seen[k / 64] & bit;
                seen[k / 64] |= bit;
            }
        }
        let mut shared = merged.lock().unwrap();
        let (global, duplicates) = &mut *shared;
        for i in 0..words {
            duplicates[i] |= repeated[i] | (global[i] & seen[i]);
            global[i] |= seen[i];
        }
        Ok(())
    })?;
    Ok(merged.into_inner().unwrap().1)
}

fn classify<'a>(
    chunks: &[&'a [u8]],
    dims: &'a Dimensions,
    users: &Routes<'a, Event<'a>>,
    start: i64,
    end: i64,
    workers: usize,
    repeats: &[u64],
) -> io::Result<Vec<Partition<'a>>> {
    let mask = repeats.len() * 64 - 1;
    let duplicates: Vec<Mutex<Partition>> =
        (0..PARTITIONS).map(|_| Mutex::new(Vec::new())).collect();
    let count = AtomicUsize::new(0);
    parallel(chunks.len(), workers, |_, i| {
        let mut routes = vec![Vec::new(); PARTITIONS];
        let mut uncertain = vec![Vec::new(); PARTITIONS];
        for (n, line) in lines(chunks[i]).enumerate() {
            if n % ROUTE_BATCH_ROWS == 0 {
                if count.fetch_add(ROUTE_BATCH_ROWS, Ordering::Relaxed) >= MAX_INDEXED_ROWS {
                    return Err(io::Error::other("event row guard"));
                }
                publish(users, &mut routes);
            }
            let mut fields = [&[][..]; 7];
            if !parse(line, &mut fields) || fields[0].is_empty() {
                continue;
            }
            let ingest = datetime(fields[3])
                .filter(|_| fields[3].len() == 20)
                .ok_or_else(|| io::Error::other("invalid ingest"))?;
            let hash = key_hash(fields[0]);
            let k = hash as usize & mask;
            if repeats[k / 64] & (1u64 << (k % 64)) != 0 {
                uncertain[((hash ^ (hash >> 32)) as usize) & (PARTITIONS - 1)].push(line);
            } else if let Some((id, t, price, cat)) =
                validated_fields(fields, dims, start, end, ingest)
            {
                routes[bucket(id)].push((id, (t, price.unwrap_or(0), cat)));
            }
        }
        publish(users, &mut routes);
        for (dst, rows) in duplicates.iter().zip(uncertain) {
            if !rows.is_empty() {
                dst.lock().unwrap().push(rows);
            }
        }
        Ok(())
    })?;
    Ok(duplicates
        .into_iter()
        .map(|m| m.into_inner().unwrap())
        .collect())
}

fn route_events<'a>(
    index: Vec<Partition<'a>>,
    dims: &'a Dimensions,
    users: &Routes<'a, Event<'a>>,
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
            if let Some((id, t, price, cat)) = validate(line, dims, start, end) {
                routes[bucket(id)].push((id, (t, price.unwrap_or(0), cat)));
            }
        }
        publish(users, &mut routes);
        Ok(())
    })
}

fn route_clicks<'a>(
    chunks: &[&'a [u8]],
    dims: &'a Dimensions,
    campaigns: &HashMap<Id, Id>,
    users: &Routes<'a, Click<'a>>,
    start: i64,
    end: i64,
    workers: usize,
) -> io::Result<()> {
    let count = AtomicUsize::new(0);
    parallel(chunks.len(), workers, |_, i| {
        let mut routes = vec![Vec::new(); PARTITIONS];
        for (n, [click, id, campaign, ts]) in records(chunks[i]).enumerate() {
            if n % ROUTE_BATCH_ROWS == 0 {
                if count.fetch_add(ROUTE_BATCH_ROWS, Ordering::Relaxed) >= MAX_CLICK_ROWS {
                    return Err(io::Error::other("click row guard"));
                }
                publish(users, &mut routes);
            }
            if id.is_empty() || click.is_empty() || !campaigns.contains_key(campaign) {
                continue;
            }
            if let Some(t) = timestamp(ts).filter(|&t| t >= start.saturating_sub(WEEK) && t < end) {
                let person = dims.aliases.get(id).map_or(id, |v| v);
                routes[bucket(person)].push((person, (t, click, campaign)));
            }
        }
        publish(users, &mut routes);
        Ok(())
    })
}

// Hex encodes arbitrary UTF-8 bytes, including CR, whitespace and NUL, across
// the scratch-file boundary. An empty category is the only nullable output.
fn hex(out: &mut impl Write, value: Id) -> io::Result<()> {
    const DIGITS: &[u8] = b"0123456789abcdef";
    for &b in value {
        out.write_all(&[DIGITS[(b >> 4) as usize], DIGITS[(b & 15) as usize]])?;
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
    categories: HashMap<Id<'a>, i64>,
    ends: HashMap<Id<'a>, i64>,
}

fn aggregate<'a>(
    id: Id,
    events: &[Routed<'a, Event<'a>>],
    clicks: &[Routed<'a, Click<'a>>],
    workspace: &mut Workspace<'a>,
    totals: &mut Totals<'a>,
    out: &mut impl Write,
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
        let day = events[i].1 .0.div_euclid(DAY_MS);
        let (mut count, mut event_count, mut revenue) = (0i64, 0i64, 0i64);
        categories.clear();
        while i < events.len() && events[i].1 .0.div_euclid(DAY_MS) == day {
            let start = events[i].1 .0;
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
                let (t, price, cat) = events[i].1;
                if price > 0 {
                    *categories.entry(cat).or_default() += price;
                    session_revenue += price;
                }
                event_count += 1;
                i += 1;
                if i == events.len() || events[i].1 .0 > t.saturating_add(1_800_000) {
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
        hex(out, id)?;
        write!(out, ",{},{},{},{},", day, count, event_count, revenue)?;
        if let Some((cat, _)) = best {
            hex(out, cat)?;
        }
        writeln!(out)?;
    }
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
    Ok(())
}

fn drain_partition<'a, T>(
    routes: &Routes<'a, T>,
    partition: usize,
) -> io::Result<Vec<Routed<'a, T>>> {
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
    let dims = dimensions(data)?;
    let catalog = Mapping::open(&data.join("campaigns.csv"))?;
    let campaigns: HashMap<_, _> = records(rows(catalog.bytes()))
        .filter(|[id, _]| !id.is_empty())
        .map(|[id, ch]| (id, ch))
        .collect();
    stage("identity_and_dimensions");
    let event_maps = files(&data.join("events"))?;
    let event_chunks = chunks(&event_maps);
    let repeats = repeat_filter(&event_chunks, workers)?;
    stage("event_index");
    let users = routes();
    let clicks = routes();
    let index = classify(&event_chunks, &dims, &users, start, end, workers, &repeats)?;
    drop(repeats);
    stage("event_classification");
    route_events(index, &dims, &users, start, end, workers)?;
    stage("event_validate_and_partition");
    let click_maps = files(&data.join("clicks"))?;
    route_clicks(
        &chunks(&click_maps),
        &dims,
        &campaigns,
        &clicks,
        start,
        end,
        workers,
    )?;
    stage("click_partition");
    parallel(workers, workers, |_, i| {
        let mut out = BufWriter::new(File::create(scratch.join(format!("user-{i}.csv")))?);
        let mut totals = Totals::new();
        let mut workspace = Workspace::default();
        for partition in (i..PARTITIONS).step_by(workers) {
            let mut events = drain_partition(&users, partition)?;
            let mut history = drain_partition(&clicks, partition)?;
            events.sort_unstable_by(|a, b| a.0.cmp(b.0).then_with(|| a.1 .0.cmp(&b.1 .0)));
            history.sort_unstable();
            // All copies of a click are identical and route to the same user.
            history.dedup();
            let mut remaining_clicks = history.as_slice();
            for person_events in events.chunk_by(|a, b| a.0 == b.0) {
                let person = person_events[0].0;
                let from = remaining_clicks.partition_point(|row| row.0 < person);
                remaining_clicks = &remaining_clicks[from..];
                let through = remaining_clicks.partition_point(|row| row.0 == person);
                let (person_clicks, remaining) = remaining_clicks.split_at(through);
                remaining_clicks = remaining;
                aggregate(
                    person,
                    person_events,
                    person_clicks,
                    &mut workspace,
                    &mut totals,
                    &mut out,
                )?;
            }
        }
        out.flush()?;
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
        out.flush()
    })?;
    stage("session_attribution_and_aggregate_staging");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_ne!(repeat_filter(&[data.as_bytes()], 2).unwrap()[0] & bit(original), 0);
        assert_eq!(repeat_filter(&[b"one,x"], 2).unwrap()[0], 0);
    }
}
