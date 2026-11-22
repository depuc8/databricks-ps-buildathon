//! Writes user_daily/ and campaign_daily/ as Parquet, by hand: uncompressed
//! data pages (v1) of PLAIN values, definition levels as one bit-packed run,
//! and the footer in Thrift's compact protocol. user_daily values are kept
//! per column as the rows come (`UserCols`), and each file goes out in one
//! vectored write of page headers and column slices.

use std::fs;
use std::io::{IoSlice, Write};
use std::path::{Path, PathBuf};

use crate::ids;
use crate::{CampStat, Ctx, PersonOut};

/// Rows per data page.
const PAGE_ROWS: usize = 1 << 16;

// Parquet enums.
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const UTF8: i32 = 0;
const DATE: i32 = 6;
const PLAIN: i32 = 0;
const RLE: i32 = 3;

// Thrift compact protocol types.
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Thrift {
    out: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { out: Vec::new(), last: 0, stack: Vec::new() }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - self.last;
        if (1..=15).contains(&delta) {
            self.out.push((delta as u8) << 4 | ty);
        } else {
            self.out.push(ty);
            self.zigzag(id as i64);
        }
        self.last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn binary(&mut self, id: i16, v: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(v.len() as u64);
        self.out.extend_from_slice(v);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.out.push((n as u8) << 4 | elem);
        } else {
            self.out.push(0xf0 | elem);
            self.varint(n as u64);
        }
    }
    /// Opens a struct: as field `id`, or as a list element when `id` is None.
    fn begin(&mut self, id: Option<i16>) {
        if let Some(id) = id {
            self.field(id, T_STRUCT);
        }
        self.stack.push(self.last);
        self.last = 0;
    }
    fn end(&mut self) {
        self.out.push(0);
        self.last = self.stack.pop().unwrap_or(0);
    }
}

#[derive(Clone, Copy)]
enum Logical {
    None,
    String,
    Date,
}

struct ColumnSpec {
    name: &'static str,
    ty: i32,
    optional: bool,
    logical: Logical,
}

/// Where one column's pages sit in the file, for the footer.
struct ColumnDone {
    offset: usize,
    size: usize,
    values: usize,
}

/// A column's values, written page by page.
trait Values {
    fn len(&self) -> usize;
    /// Appends the page for rows [a, b): levels (if any), then PLAIN values.
    fn page(&self, a: usize, b: usize, out: &mut Vec<u8>);
}

fn page_header(num_values: usize, size: usize) -> Vec<u8> {
    let mut t = Thrift::new();
    t.i32(1, 0); // DATA_PAGE
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    t.begin(Some(5));
    t.i32(1, num_values as i32);
    t.i32(2, PLAIN);
    t.i32(3, RLE);
    t.i32(4, RLE);
    t.end();
    t.out.push(0);
    t.out
}

fn write_column(file: &mut Vec<u8>, values: &dyn Values, page: &mut Vec<u8>) -> ColumnDone {
    let offset = file.len();
    let n = values.len();
    let mut a = 0;
    while a < n {
        let b = (a + PAGE_ROWS).min(n);
        page.clear();
        values.page(a, b, page);
        file.extend_from_slice(&page_header(b - a, page.len()));
        file.extend_from_slice(page);
        a = b;
    }
    ColumnDone { offset, size: file.len() - offset, values: n }
}

fn footer(file: &mut Vec<u8>, specs: &[ColumnSpec], cols: &[ColumnDone], rows: usize) {
    let mut t = Thrift::new();
    t.i32(1, 1); // version
    t.list(2, T_STRUCT, specs.len() + 1);
    t.begin(None);
    t.binary(4, b"schema");
    t.i32(5, specs.len() as i32);
    t.end();
    for s in specs {
        t.begin(None);
        t.i32(1, s.ty);
        t.i32(3, if s.optional { OPTIONAL } else { REQUIRED });
        t.binary(4, s.name.as_bytes());
        match s.logical {
            Logical::None => {}
            Logical::String => {
                t.i32(6, UTF8);
                t.begin(Some(10));
                t.begin(Some(1));
                t.end();
                t.end();
            }
            Logical::Date => {
                t.i32(6, DATE);
                t.begin(Some(10));
                t.begin(Some(6));
                t.end();
                t.end();
            }
        }
        t.end();
    }
    t.i64(3, rows as i64);
    if rows == 0 {
        t.list(4, T_STRUCT, 0);
    } else {
        t.list(4, T_STRUCT, 1);
        t.begin(None);
        t.list(1, T_STRUCT, cols.len());
        for (s, c) in specs.iter().zip(cols) {
            t.begin(None);
            t.i64(2, c.offset as i64);
            t.begin(Some(3));
            t.i32(1, s.ty);
            t.list(2, T_I32, 2);
            t.zigzag(PLAIN as i64);
            t.zigzag(RLE as i64);
            t.list(3, T_BINARY, 1);
            t.varint(s.name.len() as u64);
            t.out.extend_from_slice(s.name.as_bytes());
            t.i32(4, 0); // UNCOMPRESSED
            t.i64(5, c.values as i64);
            t.i64(6, c.size as i64);
            t.i64(7, c.size as i64);
            t.i64(9, c.offset as i64);
            t.end();
            t.end();
        }
        t.i64(2, cols.iter().map(|c| c.size as i64).sum());
        t.i64(3, rows as i64);
        t.end();
    }
    t.binary(6, b"shopping-sessions");
    t.out.push(0);
    file.extend_from_slice(&t.out);
    file.extend_from_slice(&(t.out.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
}

fn write_file(path: &Path, specs: &[ColumnSpec], columns: &[&dyn Values], rows: usize) {
    let mut file = Vec::with_capacity(rows * 64 + 4096);
    file.extend_from_slice(b"PAR1");
    let mut page = Vec::new();
    let mut done = Vec::new();
    if rows > 0 {
        for c in columns {
            done.push(write_column(&mut file, *c, &mut page));
        }
    }
    footer(&mut file, specs, &done, rows);
    fs::write(path, &file).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
}

/// A required column computed from each row: `f` writes at most `max`
/// bytes for a row and returns how many it wrote.
struct Fixed<'a, R, F> {
    rows: &'a [R],
    max: usize,
    f: F,
}

impl<R, F: Fn(&R, *mut u8) -> usize> Values for Fixed<'_, R, F> {
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn page(&self, a: usize, b: usize, out: &mut Vec<u8>) {
        put_rows(out, &self.rows[a..b], self.max, &self.f);
    }
}

/// Appends each row's bytes, written by `f` through a raw pointer into
/// room for `max` bytes a row.
#[inline(always)]
fn put_rows<R>(out: &mut Vec<u8>, rows: &[R], max: usize, f: impl Fn(&R, *mut u8) -> usize) {
    out.reserve(rows.len() * max);
    let base = out.as_mut_ptr();
    let mut len = out.len();
    for r in rows {
        len += f(r, unsafe { base.add(len) });
    }
    // SAFETY: f wrote at most max bytes a row inside the reserved room.
    unsafe { out.set_len(len) };
}

/// Writes v at p unaligned.
#[inline(always)]
unsafe fn put<const N: usize>(p: *mut u8, v: [u8; N]) -> usize {
    (p as *mut [u8; N]).write_unaligned(v);
    N
}

/// Writes a BYTE_ARRAY value (length, then bytes) at p.
#[inline(always)]
unsafe fn put_bytes(p: *mut u8, v: &[u8]) -> usize {
    put(p, (v.len() as u32).to_le_bytes());
    std::ptr::copy_nonoverlapping(v.as_ptr(), p.add(4), v.len());
    4 + v.len()
}

/// Writes a packed ID as a BYTE_ARRAY value at p: at most 17 bytes.
#[inline(always)]
unsafe fn put_id(p: *mut u8, k: u64) -> usize {
    // The inverse of the packing: bytes 0..8 from bits 7..63, byte 8 from
    // bits 0..7. The bytes after the ID are zero.
    let w = ids::scatter7(k >> 7).swap_bytes();
    let n8 = 8 - (w.leading_zeros() / 8) as usize;
    let n = n8 + (n8 == 8 && k & 0x7f != 0) as usize;
    put(p, (n as u32).to_le_bytes());
    put(p.add(4), w.to_le_bytes());
    *p.add(12) = (k & 0x7f) as u8;
    4 + n
}

/// One thread's user_daily rows as each column's PLAIN values, appended as
/// the rows are made, while their data is in cache.
#[derive(Default)]
pub struct UserCols {
    rows: usize,
    id: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    top: Vec<u8>,
    /// Bit r: row r has a top_category.
    present: Vec<u64>,
    /// Where id and top stand at the start of each page.
    id_pages: Vec<usize>,
    top_pages: Vec<usize>,
}

impl UserCols {
    pub fn len(&self) -> usize {
        self.rows
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    /// `name` holds the person's bytes if the key is odd.
    pub fn push(&mut self, person: u64, name: &[u8], day: i32, sessions: i64, events: i64, revenue: i64, top: Option<&[u8]>) {
        if self.rows % PAGE_ROWS == 0 {
            self.id_pages.push(self.id.len());
            self.top_pages.push(self.top.len());
        }
        if self.rows % 64 == 0 {
            self.present.push(0);
        }
        if person & ids::ODD == 0 {
            self.id.reserve(16);
            // SAFETY: put_id writes 13 bytes for a packed ID.
            unsafe {
                let n = put_id(self.id.as_mut_ptr().add(self.id.len()), person);
                self.id.set_len(self.id.len() + n);
            }
        } else {
            self.id.extend_from_slice(&(name.len() as u32).to_le_bytes());
            self.id.extend_from_slice(name);
        }
        self.day.push(day);
        self.sessions.push(sessions as i32);
        self.events.push(events as i32);
        self.revenue.push(revenue);
        if let Some(name) = top {
            *self.present.last_mut().unwrap() |= 1 << (self.rows % 64);
            self.top.extend_from_slice(&(name.len() as u32).to_le_bytes());
            self.top.extend_from_slice(name);
        }
        self.rows += 1;
    }
}

/// The bytes of a slice of plain numbers (little-endian, as Parquet wants).
fn bytes_of<T: Copy>(v: &[T]) -> &[u8] {
    // SAFETY: i32 and i64 have no padding; x86-64 is little-endian.
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// Part of a file: bytes made for it, or a slice of a column.
enum Piece<'a> {
    Own(Vec<u8>),
    Ref(&'a [u8]),
}

impl Piece<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Piece::Own(v) => v,
            Piece::Ref(s) => s,
        }
    }
}

fn user_file(path: &Path, cols: &UserCols) {
    let specs = [
        ColumnSpec { name: "user_id", ty: BYTE_ARRAY, optional: false, logical: Logical::String },
        ColumnSpec { name: "day", ty: INT32, optional: false, logical: Logical::Date },
        ColumnSpec { name: "sessions", ty: INT32, optional: false, logical: Logical::None },
        ColumnSpec { name: "events", ty: INT32, optional: false, logical: Logical::None },
        ColumnSpec { name: "revenue_cents", ty: INT64, optional: false, logical: Logical::None },
        ColumnSpec { name: "top_category", ty: BYTE_ARRAY, optional: true, logical: Logical::String },
    ];
    let rows = cols.rows;
    let pages = rows.div_ceil(PAGE_ROWS);
    // The byte range of page p of a column given its page starts.
    let range = |starts: &[usize], end: usize, p: usize| starts[p]..if p + 1 < pages { starts[p + 1] } else { end };
    let mut pieces: Vec<Piece> = vec![Piece::Ref(b"PAR1")];
    let mut pos = 4;
    let mut done = Vec::new();
    for c in 0..if rows > 0 { specs.len() } else { 0 } {
        let offset = pos;
        for p in 0..pages {
            let (a, b) = (p * PAGE_ROWS, ((p + 1) * PAGE_ROWS).min(rows));
            let mut data: Vec<Piece> = Vec::new();
            match c {
                0 => data.push(Piece::Ref(&cols.id[range(&cols.id_pages, cols.id.len(), p)])),
                1 => data.push(Piece::Ref(bytes_of(&cols.day[a..b]))),
                2 => data.push(Piece::Ref(bytes_of(&cols.sessions[a..b]))),
                3 => data.push(Piece::Ref(bytes_of(&cols.events[a..b]))),
                4 => data.push(Piece::Ref(bytes_of(&cols.revenue[a..b]))),
                _ => {
                    // Levels: one bit-packed run of ceil(n / 8) groups, after
                    // its byte length. Pages start on a word of `present`.
                    let groups = (b - a).div_ceil(8);
                    let mut levels = Vec::with_capacity(groups + 16);
                    let mut header = ((groups as u64) << 1) | 1;
                    let mut run = Vec::new();
                    while header >= 0x80 {
                        run.push(header as u8 | 0x80);
                        header >>= 7;
                    }
                    run.push(header as u8);
                    levels.extend_from_slice(&((run.len() + groups) as u32).to_le_bytes());
                    levels.extend_from_slice(&run);
                    levels.extend_from_slice(&bytes_of(&cols.present[a / 64..b.div_ceil(64)])[..groups]);
                    data.push(Piece::Own(levels));
                    data.push(Piece::Ref(&cols.top[range(&cols.top_pages, cols.top.len(), p)]));
                }
            }
            let size: usize = data.iter().map(|d| d.bytes().len()).sum();
            let header = page_header(b - a, size);
            pos += header.len() + size;
            pieces.push(Piece::Own(header));
            pieces.extend(data);
        }
        done.push(ColumnDone { offset, size: pos - offset, values: rows });
    }
    let mut tail = Vec::new();
    footer(&mut tail, &specs, &done, rows);
    pieces.push(Piece::Own(tail));

    let mut file = fs::File::create(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut slices: Vec<IoSlice> = pieces.iter().map(|p| IoSlice::new(p.bytes())).collect();
    let mut rest = &mut slices[..];
    while !rest.is_empty() {
        let n = file.write_vectored(rest).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        assert!(n > 0, "{}: short write", path.display());
        IoSlice::advance_slices(&mut rest, n);
    }
}

struct CampRow {
    id: &'static [u8],
    channel: &'static [u8],
    day: i32,
    stat: CampStat,
}

fn campaign_file(path: &Path, rows: &[CampRow]) {
    let specs = [
        ColumnSpec { name: "campaign_id", ty: BYTE_ARRAY, optional: false, logical: Logical::String },
        ColumnSpec { name: "channel", ty: BYTE_ARRAY, optional: false, logical: Logical::String },
        ColumnSpec { name: "day", ty: INT32, optional: false, logical: Logical::Date },
        ColumnSpec { name: "sessions", ty: INT64, optional: false, logical: Logical::None },
        ColumnSpec { name: "purchasing_sessions", ty: INT64, optional: false, logical: Logical::None },
        ColumnSpec { name: "revenue_cents", ty: INT64, optional: false, logical: Logical::None },
        ColumnSpec { name: "assisted_sessions", ty: INT64, optional: false, logical: Logical::None },
    ];
    let id_max = 4 + rows.iter().map(|r| r.id.len()).max().unwrap_or(0);
    let channel_max = 4 + rows.iter().map(|r| r.channel.len()).max().unwrap_or(0);
    let id = Fixed { rows, max: id_max, f: |r: &CampRow, p: *mut u8| unsafe { put_bytes(p, r.id) } };
    let channel = Fixed { rows, max: channel_max, f: |r: &CampRow, p: *mut u8| unsafe { put_bytes(p, r.channel) } };
    let day = Fixed { rows, max: 4, f: |r: &CampRow, p: *mut u8| unsafe { put(p, r.day.to_le_bytes()) } };
    let sessions = Fixed { rows, max: 8, f: |r: &CampRow, p: *mut u8| unsafe { put(p, r.stat.sessions.to_le_bytes()) } };
    let purchasing = Fixed { rows, max: 8, f: |r: &CampRow, p: *mut u8| unsafe { put(p, r.stat.purchasing.to_le_bytes()) } };
    let revenue = Fixed { rows, max: 8, f: |r: &CampRow, p: *mut u8| unsafe { put(p, r.stat.revenue.to_le_bytes()) } };
    let assisted = Fixed { rows, max: 8, f: |r: &CampRow, p: *mut u8| unsafe { put(p, r.stat.assisted.to_le_bytes()) } };
    write_file(path, &specs, &[&id, &channel, &day, &sessions, &purchasing, &revenue, &assisted], rows.len());
}

/// Makes the two output folders: user_daily, campaign_daily.
pub fn make_dirs(out: &Path) -> (PathBuf, PathBuf) {
    let user_dir = out.join("user_daily");
    let camp_dir = out.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_dir).expect("mkdir campaign_daily");
    (user_dir, camp_dir)
}

/// Writes thread i's user_daily rows, if any, as soon as its part of the
/// persons phase is done.
pub fn write_users(user_dir: &Path, i: usize, cols: &UserCols) {
    if cols.len() > 0 {
        user_file(&user_dir.join(format!("part-{i:05}.parquet")), cols);
    }
}

/// Writes campaign_daily, and an empty user_daily file if no thread had
/// rows (the folder must hold at least one file).
pub fn write_rest(user_dir: &Path, camp_dir: &Path, ctx: &Ctx, parts: &[PersonOut]) {
    if parts.iter().all(|p| p.users.len() == 0) {
        user_file(&user_dir.join("part-00000.parquet"), &UserCols::default());
    }
    {
        // Campaign stats are summed over threads; one small file.
        let nd = ctx.ndays;
        let mut total = vec![CampStat::default(); (ctx.organic() + 1) * nd];
        for p in parts.iter() {
            for ((t, c), &a) in total.iter_mut().zip(&p.credit).zip(&p.assisted) {
                t.sessions += c.sessions as i64;
                t.purchasing += c.purchasing as i64;
                t.revenue += c.revenue as i64;
                t.assisted += a as i64;
            }
        }
        let mut rows = Vec::new();
        for c in 0..=ctx.organic() {
            let (id, channel): (&'static [u8], &'static [u8]) =
                if c == ctx.organic() { (b"organic", b"organic") } else { (ctx.campaign_ids[c], ctx.channels[c]) };
            for d in 0..nd {
                let stat = total[c * nd + d];
                if stat.sessions != 0 || stat.assisted != 0 {
                    rows.push(CampRow { id, channel, day: (ctx.day0 + d as i64) as i32, stat });
                }
            }
        }
        campaign_file(&camp_dir.join("part-00000.parquet"), &rows);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_id_matches_bytes() {
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..100_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let n = 1 + (seed % 9) as usize;
            let id: Vec<u8> = (0..n).map(|i| 1 + (seed.rotate_left(7 * i as u32) % 127) as u8).collect();
            let k = ids::key(&id);
            let (mut a, mut b) = ([0u8; 32], [0u8; 32]);
            let na = unsafe { put_id(a.as_mut_ptr(), k) };
            let nb = unsafe { put_bytes(b.as_mut_ptr(), &id) };
            assert_eq!(&a[..na], &b[..nb], "{id:?}");
        }
    }
}
