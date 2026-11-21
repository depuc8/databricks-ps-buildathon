//! Shopping Sessions: a single-binary pipeline.
//!
//! Phases, spread over NCPU threads:
//!   1. parse:   events are bucketed by event_id. Meanwhile one thread reads
//!               the links and builds the ID -> person map; clicks, parsed
//!               last, need it and are bucketed by person
//!   2. dedup:   per event_id bucket keep the earliest-ingested copy, then
//!               re-bucket the valid survivors by person
//!   3. persons: sort, sessionize, credit and assists, aggregate
//!   4. write:   both tables as Parquet
//!
//! IDs are u64 keys (see ids.rs). Times are u32 milliseconds since
//! `t0 = window_start - 7 days`, which covers the click window.

mod csv;
mod ids;
mod output;
mod ts;

use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use memchr::memchr;

use csv::split;
use ids::Odd;
use ts::{parse_price, parse_ts};

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const CHUNK: usize = 4 << 20;

// ---------------------------------------------------------------- hashing

#[inline]
pub fn fmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// Hasher for the small std maps (products, campaigns, odd IDs).
#[derive(Default)]
pub struct Fx(u64);

impl Hasher for Fx {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.write_u64(u64::from_le_bytes(w));
        }
    }
    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(5) ^ x).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
    #[inline]
    fn write_usize(&mut self, x: usize) {
        self.write_u64(x as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        fmix64(self.0 ^ 0x2545_f491_4f6c_dd1d)
    }
}

pub type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

/// Open addressing u64 -> u64, key 0 empty. Its hash differs from the
/// bucket hash, so keys from one bucket still spread over the table.
struct U64Map {
    slots: Vec<(u64, u64)>,
    mask: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let size = (n * 2).next_power_of_two().max(16);
        U64Map { slots: vec![(0, 0); size], mask: size - 1 }
    }
    #[inline]
    fn slot(&self, k: u64) -> usize {
        fmix64(k ^ 0x9e37_79b9_7f4a_7c15) as usize & self.mask
    }
    #[inline]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = self.slot(k);
        loop {
            let (sk, sv) = self.slots[i];
            if sk == k {
                return Some(sv);
            }
            if sk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// The value for `k`, inserting `v` first if absent.
    #[inline]
    fn get_or_insert(&mut self, k: u64, v: u64) -> u64 {
        let mut i = self.slot(k);
        loop {
            let (sk, sv) = self.slots[i];
            if sk == k {
                return sv;
            }
            if sk == 0 {
                self.slots[i] = (k, v);
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0);
        }
    }
}

// ---------------------------------------------------------------- arenas

/// NB buckets of fixed capacity in one lazily-committed anonymous mapping,
/// with a spill vector per bucket for overflow.
struct Arena<T: Copy> {
    _map: memmap2::MmapMut,
    ptr: *mut T,
    cap: usize,
    lens: Vec<u32>,
    spill: Vec<Vec<T>>,
}

unsafe impl<T: Copy + Send> Send for Arena<T> {}
unsafe impl<T: Copy + Sync> Sync for Arena<T> {}

impl<T: Copy> Arena<T> {
    fn new(nb: usize, cap: usize) -> Self {
        let bytes = (nb * cap * std::mem::size_of::<T>()).max(4096);
        let mut map = memmap2::MmapMut::map_anon(bytes).expect("map_anon");
        #[cfg(target_os = "linux")]
        let _ = map.advise(memmap2::Advice::HugePage);
        let ptr = map.as_mut_ptr() as *mut T;
        Arena { _map: map, ptr, cap, lens: vec![0; nb], spill: (0..nb).map(|_| Vec::new()).collect() }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, v: T) {
        let l = self.lens[b] as usize;
        if l < self.cap {
            // SAFETY: b < nb and l < cap, inside the mapping.
            unsafe { self.ptr.add(b * self.cap + l).write(v) };
            self.lens[b] = l as u32 + 1;
        } else {
            self.spill[b].push(v);
        }
    }
    fn bucket(&self, b: usize) -> [&[T]; 2] {
        // SAFETY: the first lens[b] slots of bucket b were written.
        let main = unsafe { std::slice::from_raw_parts(self.ptr.add(b * self.cap), self.lens[b] as usize) };
        [main, &self.spill[b]]
    }
    fn total(&self) -> usize {
        self.lens.iter().map(|&l| l as usize).sum::<usize>() + self.spill.iter().map(|s| s.len()).sum::<usize>()
    }
}

// ---------------------------------------------------------------- records

/// One parsed event copy. uid == 0 marks a copy that fails validation.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eid: u64,
    uid: u64,
    ingest: u32,
    ts: u32,
    pc: u32,
    _pad: u32,
}

const EMPTY_EV: Ev = Ev { eid: 0, uid: 0, ingest: 0, ts: 0, pc: 0, _pad: 0 };

/// A surviving event: pc = price | category << 24 (price 0: not a purchase).
#[derive(Clone, Copy)]
#[repr(C)]
struct Pe {
    person: u64,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    person: u64,
    id: u64,
    ts: u32,
    camp: u32,
}

pub struct UserRow {
    pub person: u64,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: i32,
}

#[derive(Clone, Copy, Default)]
pub struct CampStat {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

// ---------------------------------------------------------------- context

pub struct Ctx {
    ws: i64,
    we: i64,
    t0: i64,
    ingest_base: i64,
    pub day0: i64,
    pub ndays: usize,
    products: FxMap<&'static [u8], u32>,
    unknown_cat: u32,
    pub categories: Vec<&'static [u8]>,
    campaigns: FxMap<&'static [u8], u32>,
    pub campaign_ids: Vec<&'static [u8]>,
    pub channels: Vec<&'static [u8]>,
}

impl Ctx {
    pub fn organic(&self) -> usize {
        self.campaign_ids.len()
    }
    #[inline]
    fn ingest_key(&self, ms: i64) -> u32 {
        (ms.div_euclid(1000) - self.ingest_base).clamp(0, u32::MAX as i64) as u32
    }
    #[inline]
    fn day_of(&self, rel: u32) -> usize {
        ((self.t0 + rel as i64).div_euclid(DAY) - self.day0) as usize
    }
}

/// Every linked ID whose person is named by another ID -> that name.
struct Links {
    map: U64Map,
    odd: Vec<(u64, &'static [u8])>,
}

impl Links {
    #[inline]
    fn person(&self, uid: u64) -> u64 {
        self.map.get(uid).unwrap_or(uid)
    }
}

fn leak_read(path: &Path) -> &'static [u8] {
    let file = fs::File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    // SAFETY: /data is read-only and nobody changes the files while we run.
    let map = unsafe { memmap2::Mmap::map(&file) }.unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let _ = map.advise(memmap2::Advice::WillNeed);
    let map: &'static memmap2::Mmap = Box::leak(Box::new(map));
    &map[..]
}

/// The lines after the header, as one slice.
fn body(data: &'static [u8]) -> &'static [u8] {
    match memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[],
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

/// Line-aligned pieces of about CHUNK bytes.
fn chunks(data: &'static [u8], out: &mut Vec<&'static [u8]>) {
    let mut start = 0;
    while start < data.len() {
        let mut end = (start + CHUNK).min(data.len());
        if end < data.len() {
            end = match memchr(b'\n', &data[end..]) {
                Some(i) => end + i + 1,
                None => data.len(),
            };
        }
        out.push(&data[start..end]);
        start = end;
    }
}

fn for_lines(chunk: &'static [u8], mut f: impl FnMut(&'static [u8])) {
    let mut pos = 0;
    while pos < chunk.len() {
        let end = match memchr(b'\n', &chunk[pos..]) {
            Some(i) => pos + i,
            None => chunk.len(),
        };
        f(&chunk[pos..end]);
        pos = end + 1;
    }
}

fn run_par<S: Send>(states: &mut [S], ntasks: usize, f: impl Fn(&mut S, usize) + Sync) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for st in states.iter_mut() {
            let next = &next;
            let f = &f;
            sc.spawn(move || loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= ntasks {
                    break;
                }
                f(st, i);
            });
        }
    });
}

fn manifest_ts(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).expect("manifest key");
    let rest = &text[at + name.len() + 2..];
    let q1 = rest.find('"').expect("manifest value") + 1;
    let q2 = q1 + rest[q1..].find('"').expect("manifest value");
    parse_ts(rest[q1..q2].as_bytes()).expect("manifest timestamp")
}

fn bits_for(n: usize, per_bucket: usize, lo: u32, hi: u32) -> u32 {
    let want = (n / per_bucket).max(1).next_power_of_two().trailing_zeros();
    want.clamp(lo, hi)
}

#[inline]
fn bucket_of(k: u64, shift: u32) -> usize {
    (fmix64(k) >> shift) as usize
}

// ---------------------------------------------------------------- links

fn build_links(files: &[PathBuf]) -> Links {
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    let mut odd = Vec::new();
    for p in files {
        csv::for_each::<2>(body(leak_read(p)), |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                pairs.push((ids::key_keep(f[0], &mut odd), ids::key_keep(f[1], &mut odd)));
            }
        });
    }
    let odd_map: Odd = odd.iter().copied().collect();

    // Union-find over interned IDs; the root of a set is its smallest index.
    let mut index = U64Map::with_capacity(pairs.len() + 16);
    let mut keys: Vec<u64> = Vec::with_capacity(pairs.len() + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() + 16);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for &(a, b) in &pairs {
        let mut intern = |k: u64| -> u32 {
            let next = keys.len() as u64;
            let i = index.get_or_insert(k, next);
            if i == next {
                keys.push(k);
                parent.push(i as u32);
            }
            i as u32
        };
        let ia = intern(a);
        let ib = intern(b);
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let n = keys.len();
    let mut best: Vec<u64> = keys.clone();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if ids::cmp(keys[i], best[r], &odd_map).is_lt() {
            best[r] = keys[i];
        }
    }
    let mut renamed = 0;
    for i in 0..n {
        let r = parent[i] as usize; // fully compressed by the loop above
        if best[r] != keys[i] {
            renamed += 1;
        }
    }
    let mut map = U64Map::with_capacity(renamed);
    for i in 0..n {
        let name = best[parent[i] as usize];
        if name != keys[i] {
            map.get_or_insert(keys[i], name);
        }
    }
    Links { map, odd }
}

// ---------------------------------------------------------------- main

struct ParseOut {
    ev: Arena<Ev>,
    ck: Arena<Ck>,
    odd: Vec<(u64, &'static [u8])>,
    scratch: Vec<Ck>,
}

struct DedupOut {
    pe: Arena<Pe>,
    table: Vec<Ev>,
}

pub struct PersonOut {
    pub users: Vec<UserRow>,
    pub camps: Vec<CampStat>,
    sess: Vec<Session>,
    catsum: Vec<u64>,
    assist: Vec<(u32, u32)>,
    group: Group,
}

/// Scratch for grouping one bucket's events and clicks by person: a small
/// table gives each person a dense index, then a counting sort groups them.
#[derive(Default)]
struct Group {
    table: Vec<(u64, u32)>,
    persons: Vec<u64>,
    local: Vec<u32>,
    starts: Vec<u32>,
    evs: Vec<Pe>,
    ck_local: Vec<u32>,
    ck_starts: Vec<u32>,
    cks: Vec<Ck>,
}

impl Group {
    #[inline]
    fn slot(person: u64, mask: usize) -> usize {
        fmix64(person ^ 0x6a09_e667_f3bc_c909) as usize & mask
    }

    /// Fills evs/starts and cks/ck_starts: person p owns
    /// evs[starts[p]..starts[p + 1]] sorted by ts, and the same range of cks
    /// sorted by ts, then click_id descending. Clicks of a person with no
    /// events are dropped.
    fn build(&mut self, ev_parts: &[&[Pe]], ck_parts: &[&[Ck]], odd: &Odd) {
        let n: usize = ev_parts.iter().map(|p| p.len()).sum();
        let size = (n * 2).next_power_of_two().max(16);
        let mask = size - 1;
        if self.table.len() < size {
            self.table.resize(size, (0, 0));
        }
        let table = &mut self.table[..size];
        self.persons.clear();
        self.local.clear();
        for part in ev_parts {
            for e in *part {
                let mut i = Self::slot(e.person, mask);
                let id = loop {
                    let (k, v) = table[i];
                    if k == e.person {
                        break v;
                    }
                    if k == 0 {
                        let v = self.persons.len() as u32;
                        table[i] = (e.person, v);
                        self.persons.push(e.person);
                        break v;
                    }
                    i = (i + 1) & mask;
                };
                self.local.push(id);
            }
        }
        let np = self.persons.len();
        counting_place(ev_parts, &self.local, np, &mut self.starts, &mut self.evs);
        for p in 0..np {
            let (a, b) = (self.starts[p] as usize, self.starts[p + 1] as usize);
            if b - a > 1 {
                self.evs[a..b].sort_unstable_by_key(|e| e.ts);
            }
        }

        self.ck_local.clear();
        for part in ck_parts {
            for c in *part {
                let mut i = Self::slot(c.person, mask);
                let id = loop {
                    let (k, v) = table[i];
                    if k == c.person {
                        break v;
                    }
                    if k == 0 {
                        break u32::MAX;
                    }
                    i = (i + 1) & mask;
                };
                self.ck_local.push(id);
            }
        }
        counting_place(ck_parts, &self.ck_local, np, &mut self.ck_starts, &mut self.cks);
        for p in 0..np {
            let (a, b) = (self.ck_starts[p] as usize, self.ck_starts[p + 1] as usize);
            if b - a > 1 {
                self.cks[a..b].sort_unstable_by(|x, y| x.ts.cmp(&y.ts).then_with(|| ids::cmp(y.id, x.id, odd)));
            }
        }
        table.fill((0, 0));
    }
}

/// Counting sort of `parts` (in order) by `local` (u32::MAX: drop) into `out`.
fn counting_place<T: Copy>(parts: &[&[T]], local: &[u32], np: usize, starts: &mut Vec<u32>, out: &mut Vec<T>) {
    starts.clear();
    starts.resize(np + 1, 0);
    for &id in local {
        if id != u32::MAX {
            starts[id as usize + 1] += 1;
        }
    }
    for p in 0..np {
        starts[p + 1] += starts[p];
    }
    let total = starts[np] as usize;
    out.clear();
    out.reserve(total);
    let mut cursor: Vec<u32> = starts[..np].to_vec();
    let dst = out.as_mut_ptr();
    let mut k = 0;
    for part in parts {
        for v in *part {
            let id = local[k];
            k += 1;
            if id != u32::MAX {
                let c = &mut cursor[id as usize];
                // SAFETY: cursor[id] < starts[id + 1] <= total <= capacity.
                unsafe { dst.add(*c as usize).write(*v) };
                *c += 1;
            }
        }
    }
    // SAFETY: every slot in 0..total was written exactly once.
    unsafe { out.set_len(total) };
}

#[derive(Clone, Copy)]
struct Session {
    start: u32,
    day: u32,
    revenue: u32,
    purchase: bool,
}

fn main() {
    let t_start = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .max(1);

    // ---- small inputs
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    let t0 = ws - WEEK;
    assert!(we - t0 < u32::MAX as i64, "window too long for u32 times");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    let mut f2: [&'static [u8]; 2] = [&[]; 2];
    let mut product_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    for_lines(body(leak_read(&data.join("products.csv"))), |line| {
        if split(line, &mut f2) {
            product_rows.push((f2[0], f2[1]));
        }
    });
    let mut categories: Vec<&'static [u8]> = product_rows.iter().map(|r| r.1).collect();
    categories.push(b"unknown");
    categories.sort_unstable();
    categories.dedup();
    assert!(categories.len() < 256, "too many categories");
    let cat_index: FxMap<&[u8], u32> = categories.iter().enumerate().map(|(i, c)| (*c, i as u32)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let mut products: FxMap<&'static [u8], u32> = FxMap::default();
    for (p, c) in &product_rows {
        products.entry(p).or_insert(cat_index[c]);
    }

    let mut campaign_ids = Vec::new();
    let mut channels = Vec::new();
    let mut campaigns: FxMap<&'static [u8], u32> = FxMap::default();
    for_lines(body(leak_read(&data.join("campaigns.csv"))), |line| {
        if split(line, &mut f2) && !campaigns.contains_key(f2[0]) {
            campaigns.insert(f2[0], campaign_ids.len() as u32);
            campaign_ids.push(f2[0]);
            channels.push(f2[1]);
        }
    });

    let ctx = Ctx {
        ws,
        we,
        t0,
        ingest_base: ws.div_euclid(1000) - (1i64 << 31),
        day0,
        ndays,
        products,
        unknown_cat,
        categories,
        campaigns,
        campaign_ids,
        channels,
    };
    let ctx = &ctx;

    // ---- parse: links on one thread, events and then clicks on all
    let link_files = csv_files(&data.join("links"));
    let mut ev_chunks = Vec::new();
    let mut ev_bytes = 0usize;
    for p in csv_files(&data.join("events")) {
        let b = body(leak_read(&p));
        ev_bytes += b.len();
        chunks(b, &mut ev_chunks);
    }
    let mut ck_chunks = Vec::new();
    let mut ck_bytes = 0usize;
    for p in csv_files(&data.join("clicks")) {
        let b = body(leak_read(&p));
        ck_bytes += b.len();
        chunks(b, &mut ck_chunks);
    }
    let n_ev_chunks = ev_chunks.len();

    let est_ev = ev_bytes / 70 + 1024;
    let est_ck = ck_bytes / 40 + 1024;
    // Buckets small enough that their hash tables stay in L2.
    let ev_bits = bits_for(est_ev, 3000, 6, 15);
    let p_bits = bits_for(est_ev, 6000, 6, 15);
    let (ev_shift, p_shift) = (64 - ev_bits, 64 - p_bits);
    let (nb_ev, nb_p) = (1usize << ev_bits, 1usize << p_bits);
    let per = |est: usize, nb: usize| est * 13 / 10 / nb / nthreads + 64;

    let links: OnceLock<Links> = OnceLock::new();
    let mut parse_outs: Vec<ParseOut> = (0..nthreads)
        .map(|_| ParseOut {
            ev: Arena::new(nb_ev, per(est_ev, nb_ev)),
            ck: Arena::new(nb_p, per(est_ck, nb_p)),
            odd: Vec::new(),
            scratch: Vec::new(),
        })
        .collect();
    let t_parse = Instant::now();
    run_par(&mut parse_outs, 1 + n_ev_chunks + ck_chunks.len(), |out, i| {
        if i == 0 {
            let l = build_links(&link_files);
            eprintln!("links built: {:?}", t_parse.elapsed());
            let _ = links.set(l);
        } else if i <= n_ev_chunks {
            parse_events(ev_chunks[i - 1], ctx, ev_shift, out);
        } else {
            let links = links.wait();
            parse_clicks(ck_chunks[i - 1 - n_ev_chunks], ctx, links, p_shift, out);
        }
    });
    let links = links.into_inner().expect("links");
    let mut odd: Odd = links.odd.iter().copied().collect();
    for o in &mut parse_outs {
        odd.extend(o.odd.drain(..));
    }
    let n_ev: usize = parse_outs.iter().map(|o| o.ev.total()).sum();
    eprintln!("parse: {:?} ({n_ev} event lines, {} buckets)", t_start.elapsed(), nb_ev);

    // ---- dedup events, re-bucket by person
    let max_bucket = (0..nb_ev)
        .map(|b| parse_outs.iter().map(|o| o.ev.bucket(b)[0].len() + o.ev.bucket(b)[1].len()).sum::<usize>())
        .max()
        .unwrap_or(0);
    let table_size = (max_bucket * 2).next_power_of_two().max(16);
    let mut dedup_outs: Vec<DedupOut> = (0..nthreads)
        .map(|_| DedupOut { pe: Arena::new(nb_p, per(n_ev, nb_p)), table: Vec::new() })
        .collect();
    {
        let parse_outs = &parse_outs;
        let links = &links;
        run_par(&mut dedup_outs, nb_ev, |out, b| {
            if out.table.is_empty() {
                out.table = vec![EMPTY_EV; table_size];
            }
            let n: usize = parse_outs.iter().map(|o| o.ev.bucket(b)[0].len() + o.ev.bucket(b)[1].len()).sum();
            let size = (n * 2).next_power_of_two().max(16);
            let mask = size - 1;
            let table = &mut out.table[..size];
            for po in parse_outs {
                for part in po.ev.bucket(b) {
                    for e in part {
                        let mut i = fmix64(e.eid) as usize & mask;
                        loop {
                            let s = &mut table[i];
                            if s.eid == 0 {
                                *s = *e;
                                break;
                            }
                            if s.eid == e.eid {
                                if e.ingest < s.ingest {
                                    *s = *e;
                                }
                                break;
                            }
                            i = (i + 1) & mask;
                        }
                    }
                }
            }
            const AHEAD: usize = 16;
            for i in 0..size {
                if i + AHEAD < size && table[i + AHEAD].uid != 0 {
                    links.map.prefetch(table[i + AHEAD].uid);
                }
                let e = table[i];
                if e.uid != 0 {
                    let person = links.person(e.uid);
                    out.pe.push(bucket_of(person, p_shift), Pe { person, ts: e.ts, pc: e.pc });
                }
                table[i] = EMPTY_EV;
            }
        });
    }
    for o in &mut dedup_outs {
        o.table = Vec::new();
    }
    eprintln!("dedup: {:?}", t_start.elapsed());

    // ---- persons
    let ncat = ctx.categories.len();
    let mut person_outs: Vec<PersonOut> = (0..nthreads)
        .map(|_| PersonOut {
            users: Vec::new(),
            camps: vec![CampStat::default(); (ctx.organic() + 1) * ndays],
            sess: Vec::new(),
            catsum: vec![0; ncat],
            assist: Vec::new(),
            group: Group::default(),
        })
        .collect();
    {
        let dedup_outs = &dedup_outs;
        let parse_outs = &parse_outs;
        let odd = &odd;
        run_par(&mut person_outs, nb_p, |out, b| {
            let ev_parts: Vec<&[Pe]> = dedup_outs.iter().flat_map(|d| d.pe.bucket(b)).collect();
            let ck_parts: Vec<&[Ck]> = parse_outs.iter().flat_map(|p| p.ck.bucket(b)).collect();
            let mut g = std::mem::take(&mut out.group);
            g.build(&ev_parts, &ck_parts, odd);
            for p in 0..g.persons.len() {
                let evs = &g.evs[g.starts[p] as usize..g.starts[p + 1] as usize];
                let cks = &g.cks[g.ck_starts[p] as usize..g.ck_starts[p + 1] as usize];
                process_person(ctx, g.persons[p], evs, cks, out);
            }
            out.group = g;
        });
    }
    eprintln!("persons: {:?}", t_start.elapsed());

    // ---- write
    output::write_all(&out_dir, ctx, &odd, &mut person_outs);
    eprintln!("done: {:?}", t_start.elapsed());
    // Skip freeing the big allocations.
    std::process::exit(0);
}

fn parse_events(chunk: &'static [u8], ctx: &Ctx, shift: u32, out: &mut ParseOut) {
    let (ws, we, t0) = (ctx.ws, ctx.we, ctx.t0);
    let ParseOut { ev, odd, .. } = out;
    csv::for_each_line::<7>(chunk, |l| {
        let eid_s = l.get(0);
        if eid_s.is_empty() {
            return;
        }
        let eid = ids::key(eid_s);
        let mut e = Ev { eid, uid: 0, ingest: u32::MAX, ts: 0, pc: 0, _pad: 0 };
        if let Some(ingest) = parse_ts(l.get(3)) {
            e.ingest = ctx.ingest_key(ingest);
            if let Some(t) = parse_ts(l.get(2)) {
                let uid = l.get(1);
                if t >= ws && t < we && ingest - t <= DAY && !uid.is_empty() {
                    let product = l.get(5);
                    let valid = !product.is_empty()
                        && match l.get(4) {
                            b"view" | b"cart" => true,
                            b"purchase" => match parse_price(l.get(6)) {
                                Some(p) => {
                                    let cat = *ctx.products.get(product).unwrap_or(&ctx.unknown_cat);
                                    e.pc = p | cat << 24;
                                    true
                                }
                                None => false,
                            },
                            _ => false,
                        };
                    if valid {
                        e.uid = ids::key_keep(uid, odd);
                        e.ts = (t - t0) as u32;
                    }
                }
            }
        }
        ev.push(bucket_of(eid, shift), e);
    });
}

fn parse_clicks(chunk: &'static [u8], ctx: &Ctx, links: &Links, shift: u32, out: &mut ParseOut) {
    let (we, t0) = (ctx.we, ctx.t0);
    let ParseOut { ck, odd, scratch, .. } = out;
    // First the clicks with their user_id key in `person`; then the person
    // lookups, prefetched a few ahead, since the links table is large.
    scratch.clear();
    csv::for_each_line::<4>(chunk, |l| {
        let (id, user) = (l.get(0), l.get(1));
        if id.is_empty() || user.is_empty() {
            return;
        }
        let t = match parse_ts(l.get(3)) {
            Some(t) if t >= t0 && t < we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(l.get(2)) {
            Some(&c) => c,
            None => return,
        };
        scratch.push(Ck { person: ids::key(user), id: ids::key_keep(id, odd), ts: (t - t0) as u32, camp });
    });
    const AHEAD: usize = 16;
    for i in 0..scratch.len() {
        if i + AHEAD < scratch.len() {
            links.map.prefetch(scratch[i + AHEAD].person);
        }
        let mut c = scratch[i];
        c.person = links.person(c.person);
        ck.push(bucket_of(c.person, shift), c);
    }
}

fn process_person(ctx: &Ctx, person: u64, evs: &[Pe], cks: &[Ck], out: &mut PersonOut) {
    let nd = ctx.ndays;
    let organic = ctx.organic();
    out.sess.clear();
    let mut row = UserRow { person, day: -1, sessions: 0, events: 0, revenue: 0, top: -1 };
    let mut cur_day = u32::MAX;
    let mut prev = 0u32;

    let flush = |row: &UserRow, catsum: &mut [u64], users: &mut Vec<UserRow>| {
        let mut top = -1i32;
        let mut best = 0u64;
        for (c, s) in catsum.iter_mut().enumerate() {
            if *s > best {
                best = *s;
                top = c as i32;
            }
            *s = 0;
        }
        users.push(UserRow { top, ..*row });
    };

    for e in evs {
        if out.sess.is_empty() || (e.ts - prev) as i64 > GAP {
            let d = ctx.day_of(e.ts) as u32;
            if d != cur_day {
                if cur_day != u32::MAX {
                    flush(&row, &mut out.catsum, &mut out.users);
                }
                cur_day = d;
                row = UserRow {
                    person,
                    day: (ctx.day0 + d as i64) as i32,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    top: -1,
                };
            }
            out.sess.push(Session { start: e.ts, day: d, revenue: 0, purchase: false });
            row.sessions += 1;
        }
        prev = e.ts;
        row.events += 1;
        let price = e.pc & 0xff_ffff;
        if price > 0 {
            let s = out.sess.last_mut().unwrap();
            s.revenue += price;
            s.purchase = true;
            row.revenue += price as i64;
            out.catsum[(e.pc >> 24) as usize] += price as u64;
        }
    }
    flush(&row, &mut out.catsum, &mut out.users);

    // Credit: the latest click at or before the start, within 7 days. Clicks
    // are sorted by ts, then click_id descending, so the last one wins a tie.
    let mut j = 0;
    for s in &out.sess {
        while j < cks.len() && cks[j].ts <= s.start {
            j += 1;
        }
        let camp = if j > 0 && (s.start - cks[j - 1].ts) as i64 <= WEEK { cks[j - 1].camp as usize } else { organic };
        let st = &mut out.camps[camp * nd + s.day as usize];
        st.sessions += 1;
        st.purchasing += s.purchase as i64;
        st.revenue += s.revenue as i64;
    }

    // Assists: a click at t assists the sessions starting in [t, t + 7 days].
    // Per campaign, merge those intervals and count the sessions in each.
    if cks.is_empty() {
        return;
    }
    let sess = &out.sess;
    // day_end[d]: the first session starting after day d.
    let mut day_end_small = [0usize; 32];
    let mut day_end_vec = Vec::new();
    let day_end: &mut [usize] = if nd <= 32 {
        &mut day_end_small[..nd]
    } else {
        day_end_vec.resize(nd, 0);
        &mut day_end_vec[..]
    };
    let mut k = 0;
    for (d, slot) in day_end.iter_mut().enumerate() {
        while k < sess.len() && sess[k].day as usize <= d {
            k += 1;
        }
        *slot = k;
    }
    let day_end = &*day_end;
    let camps = &mut out.camps;
    let mut add = |camp: u32, a: u64, b: u64| {
        let lo = sess.partition_point(|s| (s.start as u64) < a);
        let hi = sess.partition_point(|s| (s.start as u64) <= b);
        let mut k = lo;
        while k < hi {
            let d = sess[k].day as usize;
            let next = hi.min(day_end[d]);
            camps[camp as usize * nd + d].assisted += (next - k) as i64;
            k = next;
        }
    };
    if cks.len() == 1 {
        let c = cks[0];
        add(c.camp, c.ts as u64, c.ts as u64 + WEEK as u64);
        return;
    }
    out.assist.clear();
    out.assist.extend(cks.iter().map(|c| (c.camp, c.ts)));
    out.assist.sort_unstable();
    let assist = &out.assist;
    let mut i = 0;
    while i < assist.len() {
        let camp = assist[i].0;
        let (mut a, mut b) = (assist[i].1 as u64, assist[i].1 as u64 + WEEK as u64);
        i += 1;
        while i < assist.len() && assist[i].0 == camp {
            let t = assist[i].1 as u64;
            if t > b {
                add(camp, a, b);
                a = t;
            }
            b = t + WEEK as u64;
            i += 1;
        }
        add(camp, a, b);
    }
}
