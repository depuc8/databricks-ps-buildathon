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
use std::time::Instant;

use memchr::memchr;

use csv::split;
use ids::Odd;
use ts::{parse_price, parse_ts};

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const CHUNK: usize = 4 << 20;
/// Clicks come last and are fewer: smaller pieces even out the threads.
const CLICK_CHUNK: usize = 1 << 20;

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
    slots: Zeroed<(u64, u64)>,
    mask: usize,
}

impl U64Map {
    /// Room for n keys at a load of at most 1/2.
    fn with_capacity(n: usize) -> Self {
        Self::with_size((n * 2).next_power_of_two().max(16))
    }
    fn with_size(size: usize) -> Self {
        U64Map { slots: Zeroed::new(size), mask: size - 1 }
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
}

/// A zero-filled array in its own anonymous mapping, on huge pages where
/// the kernel allows, so random access into a big table misses the TLB less.
/// T must be valid as all-zero bytes.
struct Zeroed<T: Copy> {
    _map: memmap2::MmapMut,
    ptr: *mut T,
    len: usize,
}

unsafe impl<T: Copy + Send> Send for Zeroed<T> {}
unsafe impl<T: Copy + Sync> Sync for Zeroed<T> {}

impl<T: Copy> Zeroed<T> {
    fn new(len: usize) -> Self {
        let bytes = (len * std::mem::size_of::<T>()).max(4096);
        let mut map = memmap2::MmapMut::map_anon(bytes).expect("map_anon");
        #[cfg(target_os = "linux")]
        let _ = map.advise(memmap2::Advice::HugePage);
        let ptr = map.as_mut_ptr() as *mut T;
        Zeroed { _map: map, ptr, len }
    }
    fn as_ptr(&self) -> *const T {
        self.ptr
    }
}

impl<T: Copy> std::ops::Deref for Zeroed<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        // SAFETY: len elements, zero-initialized by the kernel.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T: Copy> std::ops::DerefMut for Zeroed<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// A register-blocked Bloom filter: each key sets 4 bits of one 64-bit
/// word, so a query is one load and one compare.
struct Bloom {
    bits: Zeroed<u64>,
    mask: usize,
}

impl Bloom {
    /// About 12 to 24 bits per key.
    fn new(n: usize) -> Self {
        let words = (n * 12 / 64 + 1).next_power_of_two();
        Bloom { bits: Zeroed::new(words), mask: words - 1 }
    }
    /// The word of k and its 4 bits.
    #[inline(always)]
    fn spot(&self, k: u64) -> (usize, u64) {
        let h = fmix64(k ^ 0x5851_f42d_4c95_7f2d);
        (h as usize & self.mask, 1 << (h >> 40 & 63) | 1 << (h >> 46 & 63) | 1 << (h >> 52 & 63) | 1 << (h >> 58))
    }
    /// Inserts k; several threads may insert at once.
    fn insert_shared(&self, k: u64) {
        let (w, pattern) = self.spot(k);
        // SAFETY: w <= mask; the word is only ever accessed atomically while
        // inserting.
        let word = unsafe { &*(self.bits.ptr.add(w) as *const std::sync::atomic::AtomicU64) };
        word.fetch_or(pattern, Ordering::Relaxed);
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
    /// The next slot of bucket b, now counted as written, or None if the
    /// bucket is full (then the value goes to its spill vector).
    #[inline(always)]
    fn reserve(&mut self, b: usize) -> Option<*mut T> {
        let l = self.lens[b] as usize;
        if l < self.cap {
            self.lens[b] = l as u32 + 1;
            // SAFETY: b < nb and l < cap, inside the mapping.
            Some(unsafe { self.ptr.add(b * self.cap + l) })
        } else {
            None
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
    /// The memory of bucket b's slots, to be reused once they are read.
    fn region(&self, b: usize) -> (*mut u8, usize) {
        // SAFETY: inside the mapping.
        (unsafe { self.ptr.add(b * self.cap) } as *mut u8, self.cap * std::mem::size_of::<T>())
    }
}

/// Values of one thread in blocks of BLOCK, per bucket a current block and
/// a list of full ones. Blocks come from memory handed over with `donate`
/// (arena regions already read, so that no fresh pages need zeroing), or
/// else from fresh mappings.
struct Blocks<T: Copy> {
    cur: Vec<(*mut T, u32)>,
    full: Vec<Vec<*mut T>>,
    free: Vec<*mut T>,
    fresh: Vec<memmap2::MmapMut>,
}

const BLOCK: usize = 64;

unsafe impl<T: Copy + Send> Send for Blocks<T> {}
unsafe impl<T: Copy + Sync> Sync for Blocks<T> {}

impl<T: Copy> Blocks<T> {
    fn new(nb: usize) -> Self {
        Blocks { cur: vec![(std::ptr::null_mut(), BLOCK as u32); nb], full: vec![Vec::new(); nb], free: Vec::new(), fresh: Vec::new() }
    }
    /// Hands over [p, p + bytes) for blocks; it must stay mapped while the
    /// values do, and not be used otherwise.
    fn donate(&mut self, p: *mut u8, bytes: usize) {
        let size = BLOCK * std::mem::size_of::<T>();
        let start = (p as usize).next_multiple_of(std::mem::align_of::<T>());
        let mut at = start;
        while at + size <= p as usize + bytes {
            self.free.push(at as *mut T);
            at += size;
        }
    }
    #[cold]
    fn refill(&mut self) {
        let size = BLOCK * std::mem::size_of::<T>();
        let mut map = memmap2::MmapMut::map_anon(512 * size).expect("map_anon");
        #[cfg(target_os = "linux")]
        let _ = map.advise(memmap2::Advice::HugePage);
        let p = map.as_mut_ptr();
        self.fresh.push(map);
        self.donate(p, 512 * size);
    }
    /// The next slot of bucket b, now counted as written.
    #[inline(always)]
    fn reserve(&mut self, b: usize) -> *mut T {
        let (p, n) = self.cur[b];
        if n as usize == BLOCK {
            if self.free.is_empty() {
                self.refill();
            }
            let blk = self.free.pop().unwrap();
            if !p.is_null() {
                self.full[b].push(p);
            }
            self.cur[b] = (blk, 1);
            return blk;
        }
        self.cur[b].1 = n + 1;
        // SAFETY: n < BLOCK slots in the current block.
        unsafe { p.add(n as usize) }
    }
    /// Bucket b's values, block by block.
    fn bucket(&self, b: usize) -> impl Iterator<Item = &[T]> + '_ {
        let (p, n) = self.cur[b];
        let cur: &[T] = if p.is_null() { &[] } else { unsafe { std::slice::from_raw_parts(p, n as usize) } };
        // SAFETY: full blocks hold BLOCK written values each.
        self.full[b].iter().map(|&q| unsafe { std::slice::from_raw_parts(q as *const T, BLOCK) }).chain(std::iter::once(cur))
    }
}

/// Arena writes made a few values late: each slot is reserved and prefetched
/// first, so that its store finds the line in cache instead of stalling on a
/// miss. `flush` must run before the arena is read.
struct Pending<T: Copy + Default> {
    ring: [(*mut T, T); PENDING],
    n: usize,
}

const PENDING: usize = 16;

impl<T: Copy + Default> Pending<T> {
    fn new() -> Self {
        Pending { ring: [(std::ptr::null_mut(), T::default()); PENDING], n: 0 }
    }
    #[inline(always)]
    fn push(&mut self, arena: &mut Arena<T>, b: usize, v: T) {
        match arena.reserve(b) {
            Some(dst) => {
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                    _mm_prefetch(dst as *const i8, _MM_HINT_T0);
                }
                let slot = &mut self.ring[self.n % PENDING];
                if self.n >= PENDING {
                    // SAFETY: a slot reserved in a live arena.
                    unsafe { slot.0.write(slot.1) };
                }
                *slot = (dst, v);
                self.n += 1;
            }
            None => arena.spill[b].push(v),
        }
    }
    /// `push` into blocks.
    #[inline(always)]
    fn push_block(&mut self, blocks: &mut Blocks<T>, b: usize, v: T) {
        let dst = blocks.reserve(b);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(dst as *const i8, _MM_HINT_T0);
        }
        let slot = &mut self.ring[self.n % PENDING];
        if self.n >= PENDING {
            // SAFETY: a slot reserved in a live block.
            unsafe { slot.0.write(slot.1) };
        }
        *slot = (dst, v);
        self.n += 1;
    }
    fn flush(&mut self) {
        for i in self.n.saturating_sub(PENDING)..self.n {
            let (dst, v) = self.ring[i % PENDING];
            // SAFETY: as in push.
            unsafe { dst.write(v) };
        }
        self.n = 0;
    }
}

// ---------------------------------------------------------------- records

/// One parsed event copy. uid == 0 marks a copy that fails validation.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ev {
    eid: u64,
    uid: u64,
    ingest: u32,
    ts: u32,
    pc: u32,
    _pad: u32,
}


/// A surviving event: pc = price | category << 24 (price 0: not a purchase).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Pe {
    person: u64,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ck {
    person: u64,
    id: u64,
    ts: u32,
    camp: u32,
}

/// The user_daily row being counted.
struct UserRow {
    person: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
}

#[derive(Clone, Copy, Default)]
pub struct CampStat {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

/// Credited sessions of one (campaign, day), per thread. Assists are
/// counted apart, in `PersonOut::assisted`, a smaller array that the many
/// assist increments keep in cache.
#[derive(Clone, Copy, Default)]
pub struct Credit {
    pub sessions: u32,
    pub purchasing: u32,
    pub revenue: u64,
}

// ---------------------------------------------------------------- context

pub struct Ctx {
    ws: i64,
    we: i64,
    t0: i64,
    ingest_base: i64,
    fast: ts::Fast,
    pub day0: i64,
    pub ndays: usize,
    products: FxMap<&'static [u8], u32>,
    /// Packed product key -> category; odd keys go to `products`.
    product_keys: U64Map,
    unknown_cat: u32,
    pub categories: Vec<&'static [u8]>,
    campaigns: FxMap<&'static [u8], u32>,
    /// Packed campaign key -> index; odd keys go to `campaigns`.
    campaign_keys: U64Map,
    pub campaign_ids: Vec<&'static [u8]>,
    pub channels: Vec<&'static [u8]>,
}

impl Ctx {
    pub fn organic(&self) -> usize {
        self.campaign_ids.len()
    }
    /// The category of a product, given whether 16 bytes from its start
    /// are readable.
    #[inline]
    fn category(&self, product: &[u8], wide: bool) -> u32 {
        let k = ids::key_w(product, wide);
        let found = if k & ids::ODD == 0 { self.product_keys.get(k).map(|c| c as u32) } else { self.products.get(product).copied() };
        found.unwrap_or(self.unknown_cat)
    }
    /// The index of a campaign, given whether 16 bytes from its start are
    /// readable.
    #[inline(always)]
    fn campaign(&self, id: &[u8], wide: bool) -> Option<u32> {
        let k = ids::key_w(id, wide);
        if k & ids::ODD == 0 {
            self.campaign_keys.get(k).map(|c| c as u32)
        } else {
            self.campaigns.get(id).copied()
        }
    }
    #[inline]
    fn ingest_key(&self, secs: i64) -> u32 {
        let d = secs - self.ingest_base;
        if d as u64 <= u32::MAX as u64 {
            d as u32
        } else if d < 0 {
            0
        } else {
            u32::MAX
        }
    }
    #[inline]
    fn day_of(&self, rel: u32) -> usize {
        ((self.t0 + rel as i64).div_euclid(DAY) - self.day0) as usize
    }
}

/// u64 -> u64 (key 0 empty), split into partitions by the top bits of the
/// key's hash so that it can be built a partition at a time, in cache. A
/// lookup probes one partition.
struct PartMap {
    slots: Zeroed<(u64, u64)>,
    /// Partition p's table is slots[base[p]..][..=mask[p]].
    base: Vec<u32>,
    mask: Vec<u32>,
    shift: u32,
}

impl PartMap {
    #[inline(always)]
    fn hash(k: u64) -> u64 {
        fmix64(k ^ 0x9e37_79b9_7f4a_7c15)
    }
    #[inline(always)]
    fn home(&self, k: u64) -> (usize, usize, usize) {
        let h = Self::hash(k);
        let p = (h >> self.shift) as usize;
        let m = self.mask[p] as usize;
        (self.base[p] as usize, m, h as usize & m)
    }
    /// The value of k, probing from its home slot `at` (= base + offset).
    #[inline]
    fn get_from(&self, k: u64, at: usize) -> Option<u64> {
        let (sk, sv) = self.slots[at];
        if sk == k {
            return Some(sv);
        }
        if sk == 0 {
            return None;
        }
        // Past the home slot: find the part to wrap around in it.
        let (b, m, mut i) = self.home(k);
        i = (i + 1) & m;
        loop {
            let (sk, sv) = self.slots[b + i];
            if sk == k {
                return Some(sv);
            }
            if sk == 0 {
                return None;
            }
            i = (i + 1) & m;
        }
    }
    /// Inserts k -> v.
    ///
    /// # Safety
    /// Threads inserting at once must insert into different partitions.
    unsafe fn insert_shared(&self, k: u64, v: u64) {
        let (b, m, mut i) = self.home(k);
        let slots = self.slots.ptr;
        while (*slots.add(b + i)).0 != 0 && (*slots.add(b + i)).0 != k {
            i = (i + 1) & m;
        }
        slots.add(b + i).write((k, v));
    }
}

/// Every linked ID whose person is named by another ID -> that name.
struct Links {
    map: PartMap,
    /// The keys of `map`: most IDs are not renamed and skip the table.
    bloom: Bloom,
    odd: Vec<(u64, &'static [u8])>,
}

impl Links {
    /// Replaces each user_id key in `keys` by its person. First the Bloom
    /// filter picks the keys that may be renamed, and their map slots are
    /// prefetched; then those are looked up. `cand` is scratch.
    fn resolve(&self, keys: &mut [u64], cand: &mut Vec<(u32, u32)>, spots: &mut Vec<(u32, u64)>) {
        // The Bloom words of all keys first, each prefetched a few keys
        // ahead of its test, with no branch on the outcome in that loop.
        const AHEAD: usize = 16;
        spots.clear();
        spots.extend(keys.iter().map(|&k| {
            let (w, pattern) = self.bloom.spot(k);
            (w as u32, pattern)
        }));
        let bits = self.bloom.bits.ptr;
        for i in 0..spots.len() {
            if let Some(&(w, _)) = spots.get(i + AHEAD) {
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                    _mm_prefetch(bits.add(w as usize) as *const i8, _MM_HINT_T0);
                }
            }
            let (w, pattern) = spots[i];
            // SAFETY: w <= mask < bits.len().
            let word = unsafe { *bits.add(w as usize) };
            spots[i].1 = (word & pattern == pattern) as u64;
        }
        cand.clear();
        for (i, &k) in keys.iter().enumerate() {
            if spots[i].1 != 0 {
                let (b, _, s) = self.map.home(k);
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                    _mm_prefetch(self.map.slots.as_ptr().add(b + s) as *const i8, _MM_HINT_T0);
                }
                cand.push((i as u32, (b + s) as u32));
            }
        }
        for &(i, at) in cand.iter() {
            let k = keys[i as usize];
            if let Some(name) = self.map.get_from(k, at as usize) {
                keys[i as usize] = name;
            }
        }
    }
}

extern "C" {
    fn madvise(addr: *mut std::ffi::c_void, len: usize, advice: i32) -> i32;
    fn mmap(addr: *mut std::ffi::c_void, len: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut std::ffi::c_void;
}

/// Drops the page mappings of the whole pages inside `chunk`, a read-only
/// file mapping. Its data stays in the page cache and would fault back in;
/// this only spreads over the workers the unmapping that exit would do.
fn release(chunk: &[u8]) {
    const PAGE: usize = 4096;
    const MADV_DONTNEED: i32 = 4;
    let start = (chunk.as_ptr() as usize).next_multiple_of(PAGE);
    let end = (chunk.as_ptr() as usize + chunk.len()) & !(PAGE - 1);
    if end > start {
        // SAFETY: the range is inside a live read-only file mapping.
        unsafe { madvise(start as *mut std::ffi::c_void, end - start, MADV_DONTNEED) };
    }
}

/// Maps the pages of `chunk` (a file mapping) in one call, instead of a
/// fault per 64 KB. Kernels before 5.14 refuse; the pages then fault in.
fn populate(chunk: &[u8]) {
    const PAGE: usize = 4096;
    const MADV_POPULATE_READ: i32 = 22;
    let start = (chunk.as_ptr() as usize) & !(PAGE - 1);
    let end = chunk.as_ptr() as usize + chunk.len();
    if end > start {
        // SAFETY: the range is inside a live read-only file mapping.
        unsafe { madvise(start as *mut std::ffi::c_void, end - start, MADV_POPULATE_READ) };
    }
}

/// A file's bytes, mapped read-only for the rest of the run, with at least
/// a page of readable zeros after them (see `csv::for_each_line_padded`):
/// the file goes over the start of a larger anonymous mapping.
fn leak_read(path: &Path) -> &'static [u8] {
    use std::os::fd::AsRawFd;
    const PAGE: usize = 4096;
    const PROT_READ: i32 = 1;
    const MAP_PRIVATE: i32 = 2;
    const MAP_FIXED: i32 = 0x10;
    const MAP_ANONYMOUS: i32 = 0x20;
    let file = fs::File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let len = file.metadata().map(|m| m.len()).unwrap_or(0) as usize;
    if len == 0 {
        return &[];
    }
    let total = len.next_multiple_of(PAGE) + PAGE;
    // SAFETY: a fresh mapping, then the file over its start; /data is
    // read-only and nobody changes the files while we run. Neither is ever
    // unmapped.
    unsafe {
        let base = mmap(std::ptr::null_mut(), total, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        assert!(base as isize != -1, "{}: mmap failed", path.display());
        let p = mmap(base, len, PROT_READ, MAP_PRIVATE | MAP_FIXED, file.as_raw_fd(), 0);
        assert!(p == base, "{}: mmap failed", path.display());
        std::slice::from_raw_parts(p as *const u8, len)
    }
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

/// Line-aligned pieces of about `size` bytes.
fn chunks(data: &'static [u8], size: usize, out: &mut Vec<&'static [u8]>) {
    let mut start = 0;
    while start < data.len() {
        let mut end = (start + size).min(data.len());
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
    run_par_then(states, ntasks, f, |_, _| {});
}

/// `run_par`, and then each thread calls `then` with its state and index
/// once the tasks have run out.
fn run_par_then<S: Send>(
    states: &mut [S],
    ntasks: usize,
    f: impl Fn(&mut S, usize) + Sync,
    then: impl Fn(&mut S, usize) + Sync,
) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for (t, st) in states.iter_mut().enumerate() {
            let next = &next;
            let (f, then) = (&f, &then);
            sc.spawn(move || {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= ntasks {
                        break;
                    }
                    f(st, i);
                }
                then(st, t);
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

/// The bucket of a key: the top bits of a multiplicative hash.
#[inline]
fn bucket_of(k: u64, shift: u32) -> usize {
    (k.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> shift) as usize
}

/// The home slot of a key in a table of 2^(64 - shift) slots for one
/// bucket's keys: the top bits of another multiplicative hash (the keys of
/// a bucket share the top bits of the first).
#[inline(always)]
fn slot_of(k: u64, shift: u32) -> usize {
    (k.wrapping_mul(0xd6e8_feb8_6659_fd93) >> shift) as usize
}

// ---------------------------------------------------------------- links

/// A raw pointer that worker threads may share, each writing its own part.
#[derive(Clone, Copy)]
struct SharedPtr<T>(*mut T);
unsafe impl<T> Send for SharedPtr<T> {}
unsafe impl<T> Sync for SharedPtr<T> {}

impl<T> SharedPtr<T> {
    /// # Safety
    /// As for `pointer::add`; no two threads may use the same place.
    unsafe fn at(&self, i: usize) -> *mut T {
        self.0.add(i)
    }
}

/// The links' IDs parsed and interned, ready for the union-find.
struct LinkPrep {
    /// Pairs of each thread; thread t's pair j is pair off[t] + j, whose
    /// sides are slots 2 i and 2 i + 1.
    pairs: Vec<Vec<(u64, u64)>>,
    off: Vec<usize>,
    odd: Vec<(u64, &'static [u8])>,
    id_of: Vec<u32>,
    keys: Vec<u64>,
    id_start: Vec<u32>,
    shift: u32,
}

/// Parses the links and interns their IDs on all threads: a partition at a
/// time (the partitions of the final map), so each partition's table stays
/// in cache. Ids come out dense and grouped by partition.
fn prepare_links(files: &[PathBuf], nthreads: usize) -> LinkPrep {
    let mut pieces = Vec::new();
    for p in files {
        chunks(body(leak_read(p)), CLICK_CHUNK / 4, &mut pieces);
    }
    let mut outs: Vec<(Vec<(u64, u64)>, Vec<(u64, &'static [u8])>)> = (0..nthreads).map(|_| (Vec::new(), Vec::new())).collect();
    run_par(&mut outs, pieces.len(), |(pairs, odd), i| {
        let mut add = |l: &csv::Line| {
            let ((a, aw), (b, bw)) = (l.get_w(0), l.get_w(1));
            if !a.is_empty() && !b.is_empty() {
                pairs.push((ids::key_keep_w(a, aw, odd), ids::key_keep_w(b, bw, odd)));
            }
        };
        populate(pieces[i]);
        // SAFETY: `leak_read` pads its mappings.
        unsafe { csv::for_each_line_padded::<2>(pieces[i], &mut add) };
    });
    let mut off = vec![0usize; nthreads + 1];
    for t in 0..nthreads {
        off[t + 1] = off[t] + outs[t].0.len();
    }
    let nslots = off[nthreads] * 2;
    let pbits = bits_for(nslots, 2048, 1, 12);
    let shift = 64 - pbits;
    let np = 1usize << pbits;
    let part = |k: u64| (PartMap::hash(k) >> shift) as usize;

    // Slots by partition, each thread its own, then each thread scatters
    // its slots to (partition, thread) places.
    let mut counts: Vec<Vec<u32>> = vec![vec![0; np]; nthreads];
    std::thread::scope(|sc| {
        for (c, o) in counts.iter_mut().zip(&outs) {
            sc.spawn(move || {
                for &(a, b) in &o.0 {
                    c[part(a)] += 1;
                    c[part(b)] += 1;
                }
            });
        }
    });
    let mut start = vec![0u32; np + 1];
    let mut at = 0u32;
    for p in 0..np {
        start[p] = at;
        for c in counts.iter_mut() {
            (c[p], at) = (at, at + c[p]);
        }
    }
    start[np] = at;
    let mut items: Vec<(u64, u32)> = Vec::with_capacity(nslots);
    let items_out = SharedPtr(items.as_mut_ptr());
    std::thread::scope(|sc| {
        for (t, (c, o)) in counts.iter_mut().zip(&outs).enumerate() {
            let base = off[t];
            sc.spawn(move || {
                let out = items_out;
                for (j, &(a, b)) in o.0.iter().enumerate() {
                    for (side, k) in [(0, a), (1, b)] {
                        let c = &mut c[part(k)];
                        // SAFETY: each place is written once, below nslots.
                        unsafe { out.at(*c as usize).write((k, (2 * (base + j) + side) as u32)) };
                        *c += 1;
                    }
                }
            });
        }
    });
    // SAFETY: all nslots places were written.
    unsafe { items.set_len(nslots) };

    // Intern each partition: local ids, then global ones.
    let mut local: Vec<u32> = Vec::with_capacity(nslots);
    let mut pkeys: Vec<Vec<u64>> = vec![Vec::new(); np];
    let local_out = SharedPtr(local.as_mut_ptr());
    let pkeys_out = SharedPtr(pkeys.as_mut_ptr());
    let mut scratch: Vec<Vec<(u64, u32)>> = vec![Vec::new(); nthreads];
    run_par(&mut scratch, np, |table, p| {
        let its = &items[start[p] as usize..start[p + 1] as usize];
        let size = (its.len() * 2).next_power_of_two().max(16);
        table.clear();
        table.resize(size, (0, 0));
        let m = size - 1;
        let mut ks = Vec::new();
        for (j, &(k, _)) in its.iter().enumerate() {
            let mut i = PartMap::hash(k) as usize & m;
            let id = loop {
                let (sk, sv) = table[i];
                if sk == k {
                    break sv;
                }
                if sk == 0 {
                    let id = ks.len() as u32;
                    table[i] = (k, id);
                    ks.push(k);
                    break id;
                }
                i = (i + 1) & m;
            };
            // SAFETY: partition p's places are its own.
            unsafe { local_out.at(start[p] as usize + j).write(id) };
        }
        // SAFETY: only this task writes pkeys[p].
        unsafe { *pkeys_out.at(p) = ks };
    });
    // SAFETY: all nslots places were written.
    unsafe { local.set_len(nslots) };
    let mut id_start = vec![0u32; np + 1];
    for p in 0..np {
        id_start[p + 1] = id_start[p] + pkeys[p].len() as u32;
    }
    let n = id_start[np] as usize;
    let mut keys: Vec<u64> = Vec::with_capacity(n);
    let mut id_of: Vec<u32> = Vec::with_capacity(nslots);
    let (keys_out, id_out) = (SharedPtr(keys.as_mut_ptr()), SharedPtr(id_of.as_mut_ptr()));
    let mut unit = vec![(); nthreads];
    run_par(&mut unit, np, |_, p| {
        let base = id_start[p] as usize;
        for (j, &k) in pkeys[p].iter().enumerate() {
            // SAFETY: partition p's ids are its own.
            unsafe { keys_out.at(base + j).write(k) };
        }
        for i in start[p] as usize..start[p + 1] as usize {
            // SAFETY: every slot is in exactly one partition.
            unsafe { id_out.at(items[i].1 as usize).write(base as u32 + local[i]) };
        }
    });
    // SAFETY: all places were written.
    unsafe {
        keys.set_len(n);
        id_of.set_len(nslots);
    }
    let odd = outs.iter_mut().flat_map(|o| o.1.drain(..)).collect();
    let pairs = outs.into_iter().map(|o| o.0).collect();
    LinkPrep { pairs, off, odd, id_of, keys, id_start, shift }
}

/// The links' sequential part: union-find and names; the map is made
/// empty, for `LinkTail::fill` to fill.
fn build_links(prep: LinkPrep) -> LinkTail {
    let LinkPrep { pairs, off, odd, id_of, keys, id_start, shift } = prep;
    let np = id_start.len() - 1;
    let odd_map: Odd = odd.iter().copied().collect();

    // Union-find; the root of a set is its smallest id.
    let n = keys.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for (t, ps) in pairs.iter().enumerate() {
        for i in off[t]..off[t] + ps.len() {
            let (ra, rb) = (find(&mut parent, id_of[2 * i]), find(&mut parent, id_of[2 * i + 1]));
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    drop(id_of);
    let mut best: Vec<u64> = keys.clone();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if ids::cmp(keys[i], best[r], &odd_map).is_lt() {
            best[r] = keys[i];
        }
    }
    // parent is fully compressed now: the name of id i is best[parent[i]],
    // looked up once (in random order) for both passes below.
    let names: Vec<u64> = parent.iter().map(|&r| best[r as usize]).collect();
    drop(best);

    // The map holds only renamed IDs, each partition sized for its own.
    let mut base = vec![0u32; np + 1];
    let mut mask = vec![0u32; np];
    let mut renamed = 0;
    for p in 0..np {
        let ids = id_start[p] as usize..id_start[p + 1] as usize;
        let r = ids.filter(|&i| names[i] != keys[i]).count();
        renamed += r;
        let size = (r * 4 / 3 + 1).next_power_of_two().max(2);
        mask[p] = size as u32 - 1;
        base[p + 1] = base[p] + size as u32;
    }
    let map = PartMap { slots: Zeroed::new(base[np] as usize), base, mask, shift };
    let bloom = Bloom::new(renamed);
    LinkTail { links: Links { map, bloom, odd }, names, keys, id_start }
}

/// The links with their map still to fill, and what fills it.
struct LinkTail {
    links: Links,
    names: Vec<u64>,
    keys: Vec<u64>,
    id_start: Vec<u32>,
}

impl LinkTail {
    /// Fills part `j` of `parts` of the map; parts may be filled at once.
    fn fill(&self, j: usize, parts: usize) {
        let np = self.id_start.len() - 1;
        for p in np * j / parts..np * (j + 1) / parts {
            for i in self.id_start[p] as usize..self.id_start[p + 1] as usize {
                if self.names[i] != self.keys[i] {
                    // SAFETY: the ids of partition p are keys of partition
                    // p, and only this call inserts into it.
                    unsafe { self.links.map.insert_shared(self.keys[i], self.names[i]) };
                    self.links.bloom.insert_shared(self.keys[i]);
                }
            }
        }
    }
}

// ---------------------------------------------------------------- main

struct ParseOut {
    ev: Arena<Ev>,
    ck: Arena<Ck>,
    odd: Vec<(u64, &'static [u8])>,
    scratch: Vec<Ck>,
    keys: Vec<u64>,
    cand: Vec<(u32, u32)>,
    spots: Vec<(u32, u64)>,
}

struct DedupOut {
    pe: Blocks<Pe>,
    table: Vec<(u64, u64)>,
    filled: Vec<u32>,
    keys: Vec<u64>,
    rest: Vec<(u32, u32)>,
    cand: Vec<(u32, u32)>,
    spots: Vec<(u32, u64)>,
}

pub struct PersonOut {
    pub users: output::UserCols,
    pub credit: Vec<Credit>,
    pub assisted: Vec<u32>,
    sess: Vec<Session>,
    catsum: Vec<u64>,
    /// Per campaign: the open assist interval and the stamp of the person
    /// it belongs to.
    camp_iv: Vec<(u64, u64, u32)>,
    stamp: u32,
    touched: Vec<u32>,
    group: Group,
}

/// An event being grouped: ts, price | category << 24, and its person's
/// index in the bucket.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Tp {
    ts: u32,
    pc: u32,
    id: u32,
}

/// Radix digits of ts: three of 11 bits cover the u32 times of the window.
const DIGIT: usize = 1 << 11;

/// Scratch for grouping one bucket's events and clicks by person: a small
/// table gives each person a dense index; events are sorted by ts with a
/// radix sort, then grouped (stably) by a counting sort.
#[derive(Default)]
struct Group {
    table: Vec<(u64, u32)>,
    filled: Vec<u32>,
    persons: Vec<u64>,
    starts: Vec<u32>,
    hist: Vec<u32>,
    tmp: Vec<Tp>,
    evs: Vec<Tp>,
    ck_tmp: Vec<Ck>,
    ck_starts: Vec<u32>,
    cks: Vec<Ck>,
}

/// Moves `src` to `dst` in the order of `key`, given each key's first
/// place in `at`; stable.
#[inline(always)]
fn radix_pass<T: Copy>(src: &[T], dst: &mut Vec<T>, at: &mut [u32], key: impl Fn(&T) -> usize) {
    dst.clear();
    dst.reserve(src.len());
    let out = dst.as_mut_ptr();
    assert_eq!(at.len(), DIGIT);
    for t in src {
        let k = key(t) & (DIGIT - 1);
        // SAFETY: k < DIGIT = at.len(); the places of all keys are below
        // src.len().
        unsafe {
            let c = at.get_unchecked_mut(k);
            out.add(*c as usize).write(*t);
            *c += 1;
        }
    }
    // SAFETY: every place in 0..len was written once.
    unsafe { dst.set_len(src.len()) };
}

impl Group {

    /// Fills evs/starts and cks/ck_starts: person p owns
    /// evs[starts[p]..starts[p + 1]] sorted by ts, and the same range of cks
    /// sorted by ts (ties in input order). Clicks of a person with no events
    /// are dropped.
    fn build(&mut self, ev_parts: &[&[Pe]], ck_parts: &[&[Ck]]) {
        let n: usize = ev_parts.iter().map(|p| p.len()).sum();
        let size = (n * 2).next_power_of_two().max(16);
        let (mask, shift) = (size - 1, 64 - size.trailing_zeros());
        if self.table.len() < size {
            self.table.resize(size, (0, 0));
        }
        let table = &mut self.table[..size];
        self.persons.clear();
        self.filled.clear();
        // Each event with its person's index; the counts of every ts digit
        // and of every person come on the way.
        self.hist.clear();
        self.hist.resize(3 * DIGIT, 0);
        self.starts.clear();
        self.starts.push(0);
        self.evs.clear();
        self.evs.reserve(n);
        let hist = &mut self.hist[..];
        for part in ev_parts {
            for e in *part {
                let mut i = slot_of(e.person, shift);
                let id = loop {
                    let (k, v) = table[i];
                    if k == e.person {
                        break v;
                    }
                    if k == 0 {
                        let v = self.persons.len() as u32;
                        table[i] = (e.person, v);
                        self.persons.push(e.person);
                        self.starts.push(0);
                        self.filled.push(i as u32);
                        break v;
                    }
                    i = (i + 1) & mask;
                };
                // SAFETY: id < np < starts.len(); ts < 2^31, so every digit
                // index is below 3 * DIGIT = hist.len().
                unsafe {
                    *self.starts.get_unchecked_mut(id as usize + 1) += 1;
                    *hist.get_unchecked_mut(e.ts as usize & (DIGIT - 1)) += 1;
                    *hist.get_unchecked_mut(DIGIT + (e.ts as usize >> 11 & (DIGIT - 1))) += 1;
                    *hist.get_unchecked_mut(2 * DIGIT + (e.ts as usize >> 22)) += 1;
                }
                self.evs.push(Tp { ts: e.ts, pc: e.pc, id });
            }
        }
        let np = self.persons.len();
        for d in hist.chunks_mut(DIGIT) {
            let mut at = 0;
            for c in d {
                (*c, at) = (at, at + *c);
            }
        }
        let (h0, rest) = hist.split_at_mut(DIGIT);
        let (h1, h2) = rest.split_at_mut(DIGIT);
        radix_pass(&self.evs, &mut self.tmp, h0, |t| t.ts as usize);
        radix_pass(&self.tmp, &mut self.evs, h1, |t| t.ts as usize >> 11);
        radix_pass(&self.evs, &mut self.tmp, h2, |t| t.ts as usize >> 22);
        // Then by person, stably: each person's events come out by ts.
        for p in 0..np {
            self.starts[p + 1] += self.starts[p];
        }
        self.evs.clear();
        self.evs.reserve(n);
        let out = self.evs.as_mut_ptr();
        let mut at: Vec<u32> = self.starts[..np].to_vec();
        for t in &self.tmp {
            let c = &mut at[t.id as usize];
            // SAFETY: person p's places are starts[p]..starts[p + 1] <= n.
            unsafe { out.add(*c as usize).write(*t) };
            *c += 1;
        }
        // SAFETY: every place in 0..n was written once.
        unsafe { self.evs.set_len(n) };

        // Clicks the same way, those of persons without events dropped; the
        // person field becomes the person's index.
        hist.fill(0);
        self.ck_starts.clear();
        self.ck_starts.resize(np + 1, 0);
        self.ck_tmp.clear();
        for part in ck_parts {
            for c in *part {
                let mut i = slot_of(c.person, shift);
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
                if id == u32::MAX {
                    continue;
                }
                // SAFETY: as for the events.
                unsafe {
                    *self.ck_starts.get_unchecked_mut(id as usize + 1) += 1;
                    *hist.get_unchecked_mut(c.ts as usize & (DIGIT - 1)) += 1;
                    *hist.get_unchecked_mut(DIGIT + (c.ts as usize >> 11 & (DIGIT - 1))) += 1;
                    *hist.get_unchecked_mut(2 * DIGIT + (c.ts as usize >> 22)) += 1;
                }
                self.ck_tmp.push(Ck { person: id as u64, ..*c });
            }
        }
        for d in hist.chunks_mut(DIGIT) {
            let mut at = 0;
            for c in d {
                (*c, at) = (at, at + *c);
            }
        }
        let (h0, rest) = hist.split_at_mut(DIGIT);
        let (h1, h2) = rest.split_at_mut(DIGIT);
        radix_pass(&self.ck_tmp, &mut self.cks, h0, |c| c.ts as usize);
        radix_pass(&self.cks, &mut self.ck_tmp, h1, |c| c.ts as usize >> 11);
        radix_pass(&self.ck_tmp, &mut self.cks, h2, |c| c.ts as usize >> 22);
        for p in 0..np {
            self.ck_starts[p + 1] += self.ck_starts[p];
        }
        let m = self.cks.len();
        self.ck_tmp.clear();
        self.ck_tmp.reserve(m);
        let out = self.ck_tmp.as_mut_ptr();
        let mut at: Vec<u32> = self.ck_starts[..np].to_vec();
        for c in &self.cks {
            let k = &mut at[c.person as usize];
            // SAFETY: person p's places are ck_starts[p]..ck_starts[p + 1] <= m.
            unsafe { out.add(*k as usize).write(*c) };
            *k += 1;
        }
        // SAFETY: every place in 0..m was written once.
        unsafe { self.ck_tmp.set_len(m) };
        std::mem::swap(&mut self.cks, &mut self.ck_tmp);
        for &i in &self.filled {
            table[i as usize] = (0, 0);
        }
    }
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
        .clamp(1, 32);

    // The input files are listed, mapped and cut into chunks meanwhile.
    let files = {
        let data = data.clone();
        std::thread::spawn(move || {
            let link_files = csv_files(&data.join("links"));
            let mut ev_chunks = Vec::new();
            let mut ev_bytes = 0usize;
            for p in csv_files(&data.join("events")) {
                let b = body(leak_read(&p));
                ev_bytes += b.len();
                chunks(b, CHUNK, &mut ev_chunks);
            }
            let mut ck_chunks = Vec::new();
            let mut ck_bytes = 0usize;
            for p in csv_files(&data.join("clicks")) {
                let b = body(leak_read(&p));
                ck_bytes += b.len();
                chunks(b, CLICK_CHUNK, &mut ck_chunks);
            }
            (link_files, ev_chunks, ev_bytes, ck_chunks, ck_bytes)
        })
    };

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
    csv::for_each::<2>(body(leak_read(&data.join("products.csv"))), |f| product_rows.push((f[0], f[1])));
    // Categories are indexed in byte-wise order, so a smaller index wins a
    // top_category tie.
    let mut cat_index: FxMap<&'static [u8], u32> = FxMap::default();
    cat_index.insert(b"unknown", 0);
    for &(_, c) in &product_rows {
        cat_index.entry(c).or_insert(0);
    }
    let mut categories: Vec<&'static [u8]> = cat_index.keys().copied().collect();
    categories.sort_unstable();
    assert!(categories.len() < 256, "too many categories");
    for (i, c) in categories.iter().enumerate() {
        cat_index.insert(c, i as u32);
    }
    let unknown_cat = cat_index[&b"unknown"[..]];
    // Packed product keys go to product_keys, odd ones (and empty) to
    // products. The first row of a product wins.
    let mut products: FxMap<&'static [u8], u32> = FxMap::default();
    let mut product_keys = U64Map::with_capacity(product_rows.len());
    for &(p, c) in &product_rows {
        let k = if p.is_empty() { ids::ODD } else { ids::key(p) };
        if k & ids::ODD == 0 {
            product_keys.get_or_insert(k, cat_index[c] as u64);
        } else {
            products.entry(p).or_insert(cat_index[c]);
        }
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

    let mut campaign_keys = U64Map::with_capacity(campaigns.len());
    for (c, &i) in &campaigns {
        if !c.is_empty() {
            campaign_keys.get_or_insert(ids::key(c), i as u64);
        }
    }
    let ctx = Ctx {
        ws,
        we,
        t0,
        ingest_base: ws.div_euclid(1000) - (1i64 << 31),
        fast: ts::Fast::new(t0 - 2 * DAY),
        day0,
        ndays,
        products,
        product_keys,
        unknown_cat,
        categories,
        campaigns,
        campaign_keys,
        campaign_ids,
        channels,
    };
    let ctx = &ctx;

    // ---- parse: links on one thread, events and then clicks on all
    let (link_files, ev_chunks, ev_bytes, ck_chunks, ck_bytes) = files.join().expect("file listing");
    let n_ev_chunks = ev_chunks.len();

    // Event lines average about 77 bytes and click lines about 47.
    let est_ev = ev_bytes / 76 + 1024;
    let est_ck = ck_bytes / 46 + 1024;
    // Buckets small enough that their hash tables stay in L2.
    let ev_bits = bits_for(est_ev, 3000, 6, 15);
    let p_bits = bits_for(est_ev, 6000, 6, 15);
    let (ev_shift, p_shift) = (64 - ev_bits, 64 - p_bits);
    let (nb_ev, nb_p) = (1usize << ev_bits, 1usize << p_bits);
    // Per thread and bucket, with slack for uneven chunks; overflow spills.
    // Every page of an arena gets touched (and zeroed by the kernel), so
    // the slack costs time. The thread that builds the links parses about a
    // third of a share of events; clicks come in fewer, larger chunks.
    let per_ev = est_ev * 108 / (100 * nthreads - 60).max(100) / nb_ev + 16;
    let per_ck = est_ck * 23 / 20 / nb_p / nthreads + 16;

    let mut parse_outs: Vec<ParseOut> = (0..nthreads)
        .map(|_| ParseOut {
            ev: Arena::new(nb_ev, per_ev),
            ck: Arena::new(nb_p, per_ck),
            odd: Vec::new(),
            scratch: Vec::new(),
            keys: Vec::new(),
            cand: Vec::new(),
            spots: Vec::new(),
        })
        .collect();
    let t_parse = Instant::now();
    eprintln!("setup: {:?}", t_start.elapsed());
    // The links' parallel part first, on all threads; the sequential part
    // runs on one thread beside the event parse; the map is filled by
    // FILLS tasks after the events, before the clicks. Built wholly beside
    // the parse, the links slow it more than they cost.
    let prep = std::sync::Mutex::new(Some(prepare_links(&link_files, nthreads)));
    eprintln!("links prepared: {:?}", t_parse.elapsed());
    let tail: std::sync::OnceLock<LinkTail> = std::sync::OnceLock::new();
    const FILLS: usize = 16;
    let filled = AtomicUsize::new(0);
    run_par(&mut parse_outs, 1 + n_ev_chunks + FILLS + ck_chunks.len(), |out, i| {
        if i == 0 {
            let _ = tail.set(build_links(prep.lock().unwrap().take().expect("links")));
            eprintln!("links built: {:?}", t_parse.elapsed());
        } else if i <= n_ev_chunks {
            populate(ev_chunks[i - 1]);
            parse_events(ev_chunks[i - 1], ctx, ev_shift, out);
            release(ev_chunks[i - 1]);
        } else if i <= n_ev_chunks + FILLS {
            tail.wait().fill(i - 1 - n_ev_chunks, FILLS);
            filled.fetch_add(1, Ordering::Release);
        } else {
            // The map's fill tasks were all taken before this one.
            while filled.load(Ordering::Acquire) < FILLS {
                std::thread::yield_now();
            }
            let c = ck_chunks[i - 1 - n_ev_chunks - FILLS];
            populate(c);
            parse_clicks(c, ctx, &tail.wait().links, p_shift, out);
            release(c);
        }
    });
    let links = tail.into_inner().expect("links").links;
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
        .map(|_| DedupOut {
            pe: Blocks::new(nb_p),
            table: Vec::new(),
            filled: Vec::new(),
            keys: Vec::new(),
            rest: Vec::new(),
            cand: Vec::new(),
            spots: Vec::new(),
        })
        .collect();
    {
        let parse_outs = &parse_outs;
        let links = &links;
        run_par(&mut dedup_outs, nb_ev, |out, b| {
            if out.table.is_empty() {
                out.table = vec![(0, 0); table_size];
            }
            // The bucket's copies from every thread, main part and spill.
            let mut parts: [&[Ev]; 64] = [&[]; 64];
            let mut nparts = 0;
            for po in parse_outs {
                for part in po.ev.bucket(b) {
                    parts[nparts] = part;
                    nparts += 1;
                }
            }
            let n: usize = parts[..nparts].iter().map(|p| p.len()).sum();
            let size = (n * 2).next_power_of_two().max(16);
            let (mask, shift) = (size - 1, 64 - size.trailing_zeros());
            let table = &mut out.table[..size];
            // A slot holds eid and tag = ingest << 32 | part << 26 | index;
            // `filled` lists the slots in use, so only they are read and reset.
            out.filled.clear();
            for (pi, part) in parts[..nparts].iter().enumerate() {
                for (j, e) in part.iter().enumerate() {
                    let tag = (e.ingest as u64) << 32 | (pi as u64) << 26 | j as u64;
                    let mut i = slot_of(e.eid, shift);
                    loop {
                        let s = &mut table[i];
                        if s.0 == 0 {
                            *s = (e.eid, tag);
                            out.filled.push(i as u32);
                            break;
                        }
                        if s.0 == e.eid {
                            if tag >> 32 < s.1 >> 32 {
                                s.1 = tag;
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            // Survivors first, then their persons in one batch, then scatter.
            out.keys.clear();
            out.rest.clear();
            for &i in &out.filled {
                let tag = table[i as usize].1;
                table[i as usize] = (0, 0);
                // SAFETY: the tag names a copy of this bucket.
                let e = unsafe { parts.get_unchecked((tag >> 26 & 63) as usize).get_unchecked((tag & ((1 << 26) - 1)) as usize) };
                if e.uid != 0 {
                    out.keys.push(e.uid);
                    out.rest.push((e.ts, e.pc));
                }
            }
            links.resolve(&mut out.keys, &mut out.cand, &mut out.spots);
            // The bucket's copies are all read: their memory takes the
            // survivors, now by person bucket, instead of fresh pages.
            for po in parse_outs {
                let (p, bytes) = po.ev.region(b);
                out.pe.donate(p, bytes);
            }
            let mut pending = Pending::new();
            for (&person, &(ts, pc)) in out.keys.iter().zip(&out.rest) {
                pending.push_block(&mut out.pe, bucket_of(person, p_shift), Pe { person, ts, pc });
            }
            pending.flush();
        });
    }
    for o in &mut dedup_outs {
        o.table = Vec::new();
    }
    // The event arenas now hold the survivors (see Blocks): they stay.
    eprintln!("dedup: {:?}", t_start.elapsed());

    // ---- persons
    let ncat = ctx.categories.len();
    let mut person_outs: Vec<PersonOut> = (0..nthreads)
        .map(|_| PersonOut {
            users: output::UserCols::default(),
            credit: vec![Credit::default(); (ctx.organic() + 1) * ndays],
            assisted: vec![0; (ctx.organic() + 1) * ndays],
            sess: Vec::new(),
            catsum: vec![0; ncat],
            camp_iv: vec![(0, 0, 0); ctx.organic() + 1],
            stamp: 0,
            touched: Vec::new(),
            group: Group::default(),
        })
        .collect();
    let (user_dir, camp_dir) = output::make_dirs(&out_dir);
    {
        let dedup_outs = &dedup_outs;
        let parse_outs = &parse_outs;
        let odd = &odd;
        let user_dir = &user_dir;
        // A thread writes its user_daily file as soon as it runs out of
        // buckets, while the others finish theirs.
        let then = |out: &mut PersonOut, t: usize| output::write_users(user_dir, t, &out.users);
        run_par_then(&mut person_outs, nb_p, |out, b| {
            let ev_parts: Vec<&[Pe]> = dedup_outs.iter().flat_map(|d| d.pe.bucket(b)).collect();
            let ck_parts: Vec<&[Ck]> = parse_outs.iter().flat_map(|p| p.ck.bucket(b)).collect();
            let mut g = std::mem::take(&mut out.group);
            g.build(&ev_parts, &ck_parts);
            for p in 0..g.persons.len() {
                let evs = &g.evs[g.starts[p] as usize..g.starts[p + 1] as usize];
                let cks = &g.cks[g.ck_starts[p] as usize..g.ck_starts[p + 1] as usize];
                process_person(ctx, g.persons[p], evs, cks, odd, out);
            }
            out.group = g;
        }, then);
    }
    eprintln!("persons: {:?}", t_start.elapsed());
    // The arenas are done with; free them in the background while writing.
    std::thread::spawn(move || drop((dedup_outs, parse_outs)));

    // ---- write
    output::write_rest(&user_dir, &camp_dir, ctx, &person_outs);
    eprintln!("done: {:?}", t_start.elapsed());
    // Skip freeing the big allocations.
    std::process::exit(0);
}

fn parse_events(chunk: &'static [u8], ctx: &Ctx, shift: u32, out: &mut ParseOut) {
    let ParseOut { ev, odd, .. } = out;
    let mut sink = EventSink { ctx, shift, ev, odd, pending: Pending::new() };
    // SAFETY: chunks come from `leak_read`, padded.
    unsafe { csv::for_each_line_padded::<7>(chunk, &mut sink) };
    sink.pending.flush();
}

struct EventSink<'a> {
    ctx: &'a Ctx,
    shift: u32,
    ev: &'a mut Arena<Ev>,
    odd: &'a mut Vec<(u64, &'static [u8])>,
    pending: Pending<Ev>,
}

impl csv::Sink for EventSink<'_> {
    #[inline(always)]
    fn line(&mut self, l: &csv::Line) {
        let ctx = self.ctx;
        let (eid_s, eid_w) = l.get_w(0);
        if eid_s.is_empty() {
            return;
        }
        let eid = ids::key_w(eid_s, eid_w);
        let mut e = Ev { eid, uid: 0, ingest: u32::MAX, ts: 0, pc: 0, _pad: 0 };
        if let Some(ingest) = ctx.fast.ingest_secs(l.get(3)) {
            e.ingest = ctx.ingest_key(ingest);
            let (ts, ts_w) = l.get_w(2);
            if let Some(t) = ctx.fast.ms(ts, ts_w) {
                let (uid, uid_w) = l.get_w(1);
                if t >= ctx.ws && t < ctx.we && ingest * 1000 - t <= DAY && !uid.is_empty() {
                    let (product, product_w) = l.get_w(5);
                    let valid = !product.is_empty()
                        && match event_type(l.get(4)) {
                            VIEW_OR_CART => true,
                            PURCHASE => match parse_price(l.get(6)) {
                                Some(p) => {
                                    let cat = ctx.category(product, product_w);
                                    e.pc = p | cat << 24;
                                    true
                                }
                                None => false,
                            },
                            _ => false,
                        };
                    if valid {
                        e.uid = ids::key_keep_w(uid, uid_w, self.odd);
                        e.ts = (t - ctx.t0) as u32;
                    }
                }
            }
        }
        self.pending.push(self.ev, bucket_of(eid, self.shift), e);
    }
}

const VIEW_OR_CART: u8 = 1;
const PURCHASE: u8 = 2;

/// VIEW_OR_CART, PURCHASE or 0 for an event_type, by one compare of its
/// bytes as a number.
#[inline(always)]
fn event_type(t: &[u8]) -> u8 {
    match t.len() {
        4 => {
            let v = u32::from_le_bytes(t.try_into().unwrap());
            (v == u32::from_le_bytes(*b"view") || v == u32::from_le_bytes(*b"cart")) as u8
        }
        8 => 2 * (u64::from_le_bytes(t.try_into().unwrap()) == u64::from_le_bytes(*b"purchase")) as u8,
        _ => 0,
    }
}

fn parse_clicks(chunk: &'static [u8], ctx: &Ctx, links: &Links, shift: u32, out: &mut ParseOut) {
    let ParseOut { ck, odd, scratch, keys, cand, spots, .. } = out;
    // First the clicks with their user_id key in `person`; then the person
    // lookups, prefetched a few ahead, since the links table is large.
    scratch.clear();
    // SAFETY: chunks come from `leak_read`, padded.
    unsafe { csv::for_each_line_padded::<4>(chunk, &mut ClickSink { ctx, scratch, odd }) };
    keys.clear();
    keys.extend(scratch.iter().map(|c| c.person));
    links.resolve(keys, cand, spots);
    let mut pending = Pending::new();
    for (c, &person) in scratch.iter().zip(keys.iter()) {
        pending.push(ck, bucket_of(person, shift), Ck { person, ..*c });
    }
    pending.flush();
}

struct ClickSink<'a> {
    ctx: &'a Ctx,
    scratch: &'a mut Vec<Ck>,
    odd: &'a mut Vec<(u64, &'static [u8])>,
}

impl csv::Sink for ClickSink<'_> {
    #[inline(always)]
    fn line(&mut self, l: &csv::Line) {
        let ctx = self.ctx;
        let ((id, id_w), (user, user_w)) = (l.get_w(0), l.get_w(1));
        if id.is_empty() || user.is_empty() {
            return;
        }
        let (ts, ts_w) = l.get_w(3);
        let t = match ctx.fast.ms(ts, ts_w) {
            Some(t) if t >= ctx.t0 && t < ctx.we => t,
            _ => return,
        };
        let (camp_id, camp_w) = l.get_w(2);
        if camp_id.is_empty() {
            return;
        }
        let camp = match ctx.campaign(camp_id, camp_w) {
            Some(c) => c,
            None => return,
        };
        let person = ids::key_w(user, user_w);
        self.scratch.push(Ck { person, id: ids::key_keep_w(id, id_w, self.odd), ts: (t - ctx.t0) as u32, camp });
    }
}

fn process_person(ctx: &Ctx, person: u64, evs: &[Tp], cks: &[Ck], odd: &Odd, out: &mut PersonOut) {
    let nd = ctx.ndays;
    let organic = ctx.organic();
    out.sess.clear();
    let mut row = UserRow { person, day: -1, sessions: 0, events: 0, revenue: 0 };
    let mut cur_day = u32::MAX;
    let mut prev = 0u32;

    let flush = |row: &UserRow, catsum: &mut [u64], users: &mut output::UserCols| {
        let mut top = None;
        if row.revenue > 0 {
            let mut best = 0u64;
            for (c, s) in catsum.iter_mut().enumerate() {
                if *s > best {
                    best = *s;
                    top = Some(ctx.categories[c]);
                }
                *s = 0;
            }
        }
        users.push(row.person, row.day, row.sessions, row.events, row.revenue, top, odd);
    };

    for e in evs {
        if out.sess.is_empty() || (e.ts - prev) as i64 > GAP {
            let d = ctx.day_of(e.ts) as u32;
            if d != cur_day {
                if cur_day != u32::MAX {
                    flush(&row, &mut out.catsum, &mut out.users);
                }
                cur_day = d;
                row = UserRow { person, day: (ctx.day0 + d as i64) as i32, sessions: 0, events: 0, revenue: 0 };
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

    // Credit: the latest click at or before the start, within 7 days; among
    // clicks at that same ts (rare), the smallest click_id.
    let mut j = 0;
    for s in &out.sess {
        while j < cks.len() && cks[j].ts <= s.start {
            j += 1;
        }
        let camp = if j > 0 && (s.start - cks[j - 1].ts) as i64 <= WEEK {
            let t = cks[j - 1].ts;
            let (mut best, mut k) = (j - 1, j - 1);
            while k > 0 && cks[k - 1].ts == t {
                k -= 1;
                if ids::cmp(cks[k].id, cks[best].id, odd).is_lt() {
                    best = k;
                }
            }
            cks[best].camp as usize
        } else {
            organic
        };
        let st = &mut out.credit[camp * nd + s.day as usize];
        st.sessions += 1;
        st.purchasing += s.purchase as u32;
        st.revenue += s.revenue as u64;
    }

    // Assists: a click at t assists the sessions starting in [t, t + 7 days].
    // Per campaign, merge those intervals (the clicks come by ts) and count
    // the sessions in each.
    if cks.is_empty() {
        return;
    }
    let PersonOut { sess, assisted, camp_iv, stamp, touched, .. } = out;
    let sess = &*sess;
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
    let mut add = |camp: u32, a: u64, b: u64| {
        let lo = sess.partition_point(|s| (s.start as u64) < a);
        let hi = sess.partition_point(|s| (s.start as u64) <= b);
        let mut k = lo;
        while k < hi {
            let d = sess[k].day as usize;
            let next = hi.min(day_end[d]);
            assisted[camp as usize * nd + d] += (next - k) as u32;
            k = next;
        }
    };
    if cks.len() == 1 {
        let c = cks[0];
        add(c.camp, c.ts as u64, c.ts as u64 + WEEK as u64);
        return;
    }
    *stamp += 1;
    touched.clear();
    for c in cks {
        let t = c.ts as u64;
        let iv = &mut camp_iv[c.camp as usize];
        if iv.2 != *stamp {
            *iv = (t, t + WEEK as u64, *stamp);
            touched.push(c.camp);
        } else if t > iv.1 {
            add(c.camp, iv.0, iv.1);
            (iv.0, iv.1) = (t, t + WEEK as u64);
        } else {
            iv.1 = t + WEEK as u64;
        }
    }
    for &camp in touched.iter() {
        let iv = camp_iv[camp as usize];
        add(camp, iv.0, iv.1);
    }
}
