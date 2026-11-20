//! Shopping Sessions: a single-binary pipeline.
//!
//! Phases, each spread over NCPU threads:
//!   1. links:   parse, then union-find (serial) -> map user_id -> person name
//!   2. parse:   events (bucketed by event_id) and clicks (bucketed by person)
//!   3. dedup:   per event_id bucket keep the earliest-ingested copy, then
//!               re-bucket the survivors by person
//!   4. persons: sort, sessionize, credit and assists, aggregate
//!   5. write:   both tables as Parquet
//!
//! Strings are keyed as u128: up to 15 bytes are packed big-endian with the
//! length in the low byte, which is exact and sorts byte-wise; longer strings
//! get a 120-bit hash with 0xFF in the low byte and are kept in a side table.

mod csv;
mod output;
mod ts;

use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use memchr::memchr;

use csv::split;
use ts::{parse_price, parse_ts};

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const CHUNK: usize = 8 << 20;
const EV_BITS: u32 = 10;
const P_BITS: u32 = 10;

// ---------------------------------------------------------------- hashing

#[inline]
fn fmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// Hasher for the maps. Its bits are independent of `bucket`, which matters:
/// a map filled from one bucket would otherwise see keys that all share bits.
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
    fn write_u128(&mut self, x: u128) {
        self.write_u64(x as u64);
        self.write_u64((x >> 64) as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        fmix64(self.0 ^ 0x2545_f491_4f6c_dd1d)
    }
}

pub type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

#[inline]
fn bucket(k: u128, bits: u32) -> usize {
    let x = fmix64((k as u64) ^ ((k >> 64) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    (x >> (64 - bits)) as usize
}

// ---------------------------------------------------------------- string keys

pub type Longs = FxMap<u128, &'static [u8]>;

fn hash_long(s: &[u8]) -> u128 {
    let mut a: u64 = 0x243f_6a88_85a3_08d3 ^ s.len() as u64;
    let mut b: u64 = 0x1319_8a2e_0370_7344;
    for c in s.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        let x = u64::from_le_bytes(w);
        a = fmix64(a ^ x).wrapping_add(b.rotate_left(17));
        b = fmix64(b.wrapping_add(x) ^ 0x9e37_79b9_7f4a_7c15).wrapping_add(a);
    }
    ((a as u128) << 64 | b as u128) | 0xff
}

#[inline]
fn key(s: &'static [u8], longs: &mut Vec<(u128, &'static [u8])>) -> u128 {
    if s.len() <= 15 {
        let mut buf = [0u8; 16];
        buf[..s.len()].copy_from_slice(s);
        buf[15] = s.len() as u8;
        u128::from_be_bytes(buf)
    } else {
        let k = hash_long(s);
        longs.push((k, s));
        k
    }
}

/// A key for a string never shown or compared by order (event_id).
#[inline]
fn key_anon(s: &[u8]) -> u128 {
    if s.len() <= 15 {
        let mut buf = [0u8; 16];
        buf[..s.len()].copy_from_slice(s);
        buf[15] = s.len() as u8;
        u128::from_be_bytes(buf)
    } else {
        hash_long(s)
    }
}

#[inline]
fn is_long(k: u128) -> bool {
    k as u8 == 0xff
}

pub fn key_bytes<'a>(k: u128, buf: &'a mut [u8; 16], longs: &Longs) -> &'a [u8] {
    if is_long(k) {
        longs[&k]
    } else {
        *buf = k.to_be_bytes();
        let n = buf[15] as usize;
        &buf[..n]
    }
}

fn name_lt(a: u128, b: u128, longs: &Longs) -> bool {
    if !is_long(a) && !is_long(b) {
        return a < b;
    }
    let (mut ba, mut bb) = ([0u8; 16], [0u8; 16]);
    key_bytes(a, &mut ba, longs) < key_bytes(b, &mut bb, longs)
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct Ev {
    eid: u128,
    uid: u128,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u16,
    ok: bool,
}

#[derive(Clone, Copy)]
struct Pe {
    person: u128,
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct Ck {
    person: u128,
    ts: i64,
    id: &'static [u8],
    camp: u32,
}

pub struct UserRow {
    pub person: u128,
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
    pub day0: i64,
    pub ndays: usize,
    products: FxMap<&'static [u8], u16>,
    unknown_cat: u16,
    pub categories: Vec<&'static [u8]>,
    campaigns: FxMap<&'static [u8], u32>,
    pub campaign_ids: Vec<&'static [u8]>,
    pub channels: Vec<&'static [u8]>,
    link_index: FxMap<u128, u32>,
    link_names: Vec<u128>,
}

impl Ctx {
    #[inline]
    fn person(&self, uid: u128) -> u128 {
        match self.link_index.get(&uid) {
            Some(&i) => self.link_names[i as usize],
            None => uid,
        }
    }
    pub fn organic(&self) -> usize {
        self.campaign_ids.len()
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
    let _ = map.advise(memmap2::Advice::Sequential);
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

#[inline]
fn for_lines(chunk: &[u8], mut f: impl FnMut(&[u8])) {
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

// ---------------------------------------------------------------- main

struct ParseOut {
    ev: Vec<Vec<Ev>>,
    ck: Vec<Vec<Ck>>,
    longs: Vec<(u128, &'static [u8])>,
}

struct DedupOut {
    pe: Vec<Vec<Pe>>,
    map: FxMap<u128, u32>,
    buf: Vec<Ev>,
}

pub struct PersonOut {
    pub users: Vec<UserRow>,
    pub camps: Vec<CampStat>,
    evs: Vec<Pe>,
    cks: Vec<Ck>,
    sess: Vec<Session>,
    catsum: Vec<u64>,
    assist: Vec<(u32, i64)>,
}

#[derive(Clone, Copy)]
struct Session {
    start: i64,
    day: usize,
    revenue: u64,
    purchase: bool,
}

fn main() {
    let t0 = Instant::now();
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
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    let mut f2: [&'static [u8]; 2] = [&[]; 2];
    let mut product_rows: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    for_lines(body(leak_read(&data.join("products.csv"))), |line| {
        let line: &'static [u8] = unsafe { std::mem::transmute(line) };
        if split(line, &mut f2) {
            product_rows.push((f2[0], f2[1]));
        }
    });
    let mut categories: Vec<&'static [u8]> = product_rows.iter().map(|r| r.1).collect();
    categories.push(b"unknown");
    categories.sort_unstable();
    categories.dedup();
    let cat_index: FxMap<&[u8], u16> =
        categories.iter().enumerate().map(|(i, c)| (*c, i as u16)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let products: FxMap<&'static [u8], u16> =
        product_rows.iter().map(|(p, c)| (*p, cat_index[c])).collect();

    let mut campaign_ids = Vec::new();
    let mut channels = Vec::new();
    let mut campaigns: FxMap<&'static [u8], u32> = FxMap::default();
    for_lines(body(leak_read(&data.join("campaigns.csv"))), |line| {
        let line: &'static [u8] = unsafe { std::mem::transmute(line) };
        if split(line, &mut f2) && !campaigns.contains_key(f2[0]) {
            campaigns.insert(f2[0], campaign_ids.len() as u32);
            campaign_ids.push(f2[0]);
            channels.push(f2[1]);
        }
    });

    let mut ctx = Ctx {
        ws,
        we,
        day0,
        ndays,
        products,
        unknown_cat,
        categories,
        campaigns,
        campaign_ids,
        channels,
        link_index: FxMap::default(),
        link_names: Vec::new(),
    };

    // ---- links
    let mut link_chunks = Vec::new();
    for p in csv_files(&data.join("links")) {
        chunks(body(leak_read(&p)), &mut link_chunks);
    }
    let mut link_outs: Vec<(Vec<(u128, u128)>, Vec<(u128, &'static [u8])>)> =
        (0..nthreads).map(|_| (Vec::new(), Vec::new())).collect();
    run_par(&mut link_outs, link_chunks.len(), |(pairs, longs), i| {
        let mut f: [&'static [u8]; 2] = [&[]; 2];
        for_lines(link_chunks[i], |line| {
            let line: &'static [u8] = unsafe { std::mem::transmute(line) };
            if split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                pairs.push((key(f[0], longs), key(f[1], longs)));
            }
        });
    });
    let mut longs: Longs = FxMap::default();
    let mut index: FxMap<u128, u32> = FxMap::default();
    let mut keys: Vec<u128> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    let total_pairs: usize = link_outs.iter().map(|o| o.0.len()).sum();
    index.reserve(total_pairs);
    for (pairs, ls) in &link_outs {
        for &(k, s) in ls {
            longs.insert(k, s);
        }
        for &(a, b) in pairs {
            let mut intern = |k: u128| -> u32 {
                *index.entry(k).or_insert_with(|| {
                    keys.push(k);
                    parent.push(keys.len() as u32 - 1);
                    keys.len() as u32 - 1
                })
            };
            let ia = intern(a);
            let ib = intern(b);
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    drop(link_outs);
    let mut best: Vec<u128> = keys.clone();
    for i in 0..keys.len() as u32 {
        let r = find(&mut parent, i) as usize;
        if name_lt(keys[i as usize], best[r], &longs) {
            best[r] = keys[i as usize];
        }
    }
    let names: Vec<u128> = (0..keys.len() as u32).map(|i| best[find(&mut parent, i) as usize]).collect();
    ctx.link_index = index;
    ctx.link_names = names;
    eprintln!("links: {} ids, {:?}", keys.len(), t0.elapsed());
    drop((keys, best, parent));

    // ---- parse events and clicks
    let mut ev_chunks = Vec::new();
    for p in csv_files(&data.join("events")) {
        chunks(body(leak_read(&p)), &mut ev_chunks);
    }
    let mut ck_chunks = Vec::new();
    for p in csv_files(&data.join("clicks")) {
        chunks(body(leak_read(&p)), &mut ck_chunks);
    }
    let n_ev_chunks = ev_chunks.len();
    let mut parse_outs: Vec<ParseOut> = (0..nthreads)
        .map(|_| ParseOut {
            ev: (0..1 << EV_BITS).map(|_| Vec::new()).collect(),
            ck: (0..1 << P_BITS).map(|_| Vec::new()).collect(),
            longs: Vec::new(),
        })
        .collect();
    let ctx = &ctx;
    run_par(&mut parse_outs, n_ev_chunks + ck_chunks.len(), |out, i| {
        if i < n_ev_chunks {
            parse_events(ev_chunks[i], ctx, out);
        } else {
            parse_clicks(ck_chunks[i - n_ev_chunks], ctx, out);
        }
    });
    for o in &mut parse_outs {
        for &(k, s) in &o.longs {
            longs.insert(k, s);
        }
        o.longs = Vec::new();
    }
    eprintln!("parse: {:?}", t0.elapsed());

    // ---- dedup events, re-bucket by person
    let mut dedup_outs: Vec<DedupOut> = (0..nthreads)
        .map(|_| DedupOut {
            pe: (0..1 << P_BITS).map(|_| Vec::new()).collect(),
            map: FxMap::default(),
            buf: Vec::new(),
        })
        .collect();
    {
        let parse_outs = &parse_outs;
        run_par(&mut dedup_outs, 1 << EV_BITS, |out, b| {
            out.buf.clear();
            for po in parse_outs {
                out.buf.extend_from_slice(&po.ev[b]);
            }
            out.map.clear();
            for (i, e) in out.buf.iter().enumerate() {
                match out.map.entry(e.eid) {
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(i as u32);
                    }
                    std::collections::hash_map::Entry::Occupied(mut o) => {
                        if e.ingest < out.buf[*o.get() as usize].ingest {
                            o.insert(i as u32);
                        }
                    }
                }
            }
            for &i in out.map.values() {
                let e = &out.buf[i as usize];
                if e.ok {
                    let person = ctx.person(e.uid);
                    out.pe[bucket(person, P_BITS)].push(Pe { person, ts: e.ts, price: e.price, cat: e.cat });
                }
            }
        });
    }
    let parse_cks: Vec<Vec<Vec<Ck>>> = parse_outs.into_iter().map(|o| o.ck).collect();
    for o in &mut dedup_outs {
        o.map = FxMap::default();
        o.buf = Vec::new();
    }
    eprintln!("dedup: {:?}", t0.elapsed());

    // ---- persons
    let ncat = ctx.categories.len();
    let mut person_outs: Vec<PersonOut> = (0..nthreads)
        .map(|_| PersonOut {
            users: Vec::new(),
            camps: vec![CampStat::default(); (ctx.organic() + 1) * ndays],
            evs: Vec::new(),
            cks: Vec::new(),
            sess: Vec::new(),
            catsum: vec![0; ncat],
            assist: Vec::new(),
        })
        .collect();
    {
        let dedup_outs = &dedup_outs;
        let parse_cks = &parse_cks;
        run_par(&mut person_outs, 1 << P_BITS, |out, b| {
            out.evs.clear();
            for d in dedup_outs {
                out.evs.extend_from_slice(&d.pe[b]);
            }
            out.cks.clear();
            for c in parse_cks {
                out.cks.extend_from_slice(&c[b]);
            }
            out.evs.sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)));
            out.cks.sort_unstable_by(|a, b| {
                (a.person, a.ts).cmp(&(b.person, b.ts)).then_with(|| b.id.cmp(a.id))
            });
            let evs = std::mem::take(&mut out.evs);
            let cks = std::mem::take(&mut out.cks);
            let (mut i, mut ci) = (0, 0);
            while i < evs.len() {
                let person = evs[i].person;
                let mut j = i + 1;
                while j < evs.len() && evs[j].person == person {
                    j += 1;
                }
                while ci < cks.len() && cks[ci].person < person {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < cks.len() && cks[cj].person == person {
                    cj += 1;
                }
                process_person(ctx, person, &evs[i..j], &cks[ci..cj], out);
                i = j;
                ci = cj;
            }
            out.evs = evs;
            out.cks = cks;
        });
    }
    drop(dedup_outs);
    drop(parse_cks);
    eprintln!("persons: {:?}", t0.elapsed());

    // ---- write
    output::write_all(&out_dir, ctx, &longs, &mut person_outs);
    eprintln!("done: {:?}", t0.elapsed());
    // Skip freeing the big allocations.
    std::process::exit(0);
}

fn parse_events(chunk: &'static [u8], ctx: &Ctx, out: &mut ParseOut) {
    let mut f: [&'static [u8]; 7] = [&[]; 7];
    for_lines(chunk, |line| {
        let line: &'static [u8] = unsafe { std::mem::transmute(line) };
        if !split(line, &mut f) || f[0].is_empty() {
            return;
        }
        let eid = key_anon(f[0]);
        let ingest = parse_ts(f[3]);
        let ts = parse_ts(f[2]);
        let mut ok = ingest.is_some() && !f[1].is_empty() && !f[5].is_empty();
        let ts = ts.unwrap_or(i64::MIN);
        let ingest = ingest.unwrap_or(i64::MAX);
        ok = ok && ts >= ctx.ws && ts < ctx.we && ingest.saturating_sub(ts) <= DAY;
        let mut price = 0u32;
        let mut cat = 0u16;
        if ok {
            match f[4] {
                b"view" | b"cart" => {}
                b"purchase" => match parse_price(f[6]) {
                    Some(p) => {
                        price = p;
                        cat = *ctx.products.get(f[5]).unwrap_or(&ctx.unknown_cat);
                    }
                    None => ok = false,
                },
                _ => ok = false,
            }
        }
        let uid = if ok { key(f[1], &mut out.longs) } else { 0 };
        out.ev[bucket(eid, EV_BITS)].push(Ev { eid, uid, ts, ingest, price, cat, ok });
    });
}

fn parse_clicks(chunk: &'static [u8], ctx: &Ctx, out: &mut ParseOut) {
    let mut f: [&'static [u8]; 4] = [&[]; 4];
    let mut scratch = Vec::new();
    for_lines(chunk, |line| {
        let line: &'static [u8] = unsafe { std::mem::transmute(line) };
        if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let ts = match parse_ts(f[3]) {
            Some(t) if t >= ctx.ws - WEEK && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(f[2]) {
            Some(&c) => c,
            None => return,
        };
        scratch.clear();
        let person = ctx.person(key(f[1], &mut scratch));
        out.ck[bucket(person, P_BITS)].push(Ck { person, ts, id: f[0], camp });
    });
}

fn process_person(ctx: &Ctx, person: u128, evs: &[Pe], cks: &[Ck], out: &mut PersonOut) {
    let nd = ctx.ndays;
    let organic = ctx.organic();
    out.sess.clear();
    let mut row = UserRow { person, day: -1, sessions: 0, events: 0, revenue: 0, top: -1 };
    let mut cur_day = usize::MAX;
    let mut prev = i64::MIN;

    let flush = |row: &mut UserRow, catsum: &mut [u64], users: &mut Vec<UserRow>| {
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
        if out.sess.is_empty() || e.ts - prev > GAP {
            let d = (e.ts.div_euclid(DAY) - ctx.day0) as usize;
            if d != cur_day {
                if cur_day != usize::MAX {
                    flush(&mut row, &mut out.catsum, &mut out.users);
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
        if e.price > 0 {
            let s = out.sess.last_mut().unwrap();
            s.revenue += e.price as u64;
            s.purchase = true;
            row.revenue += e.price as i64;
            out.catsum[e.cat as usize] += e.price as u64;
        }
    }
    flush(&mut row, &mut out.catsum, &mut out.users);

    // Credit: the latest click at or before the start, within 7 days. Clicks
    // are sorted by ts, then click_id descending, so the last one wins a tie.
    let mut j = 0;
    for s in &out.sess {
        while j < cks.len() && cks[j].ts <= s.start {
            j += 1;
        }
        let camp = if j > 0 && s.start - cks[j - 1].ts <= WEEK { cks[j - 1].camp as usize } else { organic };
        let st = &mut out.camps[camp * nd + s.day];
        st.sessions += 1;
        st.purchasing += s.purchase as i64;
        st.revenue += s.revenue as i64;
    }

    // Assists: a click at t assists the sessions starting in [t, t + 7 days].
    // Per campaign, merge those intervals and count the sessions in each.
    if cks.is_empty() {
        return;
    }
    out.assist.clear();
    out.assist.extend(cks.iter().map(|c| (c.camp, c.ts)));
    out.assist.sort_unstable();
    let sess = &out.sess;
    let mut day_end = [0usize; 64];
    let mut day_end_vec;
    let day_end: &mut [usize] = if nd <= 64 {
        &mut day_end[..nd]
    } else {
        day_end_vec = vec![0usize; nd];
        &mut day_end_vec[..]
    };
    {
        let mut k = 0;
        for (d, slot) in day_end.iter_mut().enumerate() {
            while k < sess.len() && sess[k].day <= d {
                k += 1;
            }
            *slot = k;
        }
    }
    let camps = &mut out.camps;
    let mut add = |camp: u32, a: i64, b: i64| {
        let lo = sess.partition_point(|s| s.start < a);
        let hi = sess.partition_point(|s| s.start <= b);
        let mut k = lo;
        while k < hi {
            let d = sess[k].day;
            let next = hi.min(day_end[d]);
            camps[camp as usize * nd + d].assisted += (next - k) as i64;
            k = next;
        }
    };
    let assist = &out.assist;
    let mut i = 0;
    while i < assist.len() {
        let camp = assist[i].0;
        let (mut a, mut b) = (assist[i].1, assist[i].1 + WEEK);
        i += 1;
        while i < assist.len() && assist[i].0 == camp {
            let t = assist[i].1;
            if t > b {
                add(camp, a, b);
                a = t;
            }
            b = t + WEEK;
            i += 1;
        }
        add(camp, a, b);
    }
}
