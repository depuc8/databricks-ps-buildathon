//! Writes user_daily/ and campaign_daily/ as Parquet, by hand: uncompressed
//! data pages (v1) of PLAIN values, definition levels as one bit-packed run,
//! and the footer in Thrift's compact protocol. Each file is built in memory
//! and written in one call.

use std::fs;
use std::path::Path;

use crate::ids::{self, Odd};
use crate::{CampStat, Ctx, PersonOut, UserRow};

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

/// A required column computed from each row.
struct Fixed<'a, R, F> {
    rows: &'a [R],
    f: F,
}

impl<R, F: Fn(&R, &mut Vec<u8>)> Values for Fixed<'_, R, F> {
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn page(&self, a: usize, b: usize, out: &mut Vec<u8>) {
        for r in &self.rows[a..b] {
            (self.f)(r, out);
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, v: &[u8]) {
    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
    out.extend_from_slice(v);
}

/// top_category is optional: a page holds its levels, then the present values.
struct TopCategory<'a> {
    rows: &'a [UserRow],
    names: &'a [&'static [u8]],
}

impl Values for TopCategory<'_> {
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn page(&self, a: usize, b: usize, out: &mut Vec<u8>) {
        let rows = &self.rows[a..b];
        // Levels: one bit-packed run of ceil(n / 8) groups, after its byte length.
        let groups = rows.len().div_ceil(8);
        let mut levels = Vec::with_capacity(groups + 8);
        let mut header = ((groups as u64) << 1) | 1;
        while header >= 0x80 {
            levels.push(header as u8 | 0x80);
            header >>= 7;
        }
        levels.push(header as u8);
        for g in rows.chunks(8) {
            let mut byte = 0u8;
            for (i, r) in g.iter().enumerate() {
                byte |= ((r.top >= 0) as u8) << i;
            }
            levels.push(byte);
        }
        out.extend_from_slice(&(levels.len() as u32).to_le_bytes());
        out.extend_from_slice(&levels);
        for r in rows {
            if r.top >= 0 {
                put_bytes(out, self.names[r.top as usize]);
            }
        }
    }
}

fn user_file(path: &Path, rows: &[UserRow], ctx: &Ctx, odd: &Odd) {
    let specs = [
        ColumnSpec { name: "user_id", ty: BYTE_ARRAY, optional: false, logical: Logical::String },
        ColumnSpec { name: "day", ty: INT32, optional: false, logical: Logical::Date },
        ColumnSpec { name: "sessions", ty: INT32, optional: false, logical: Logical::None },
        ColumnSpec { name: "events", ty: INT32, optional: false, logical: Logical::None },
        ColumnSpec { name: "revenue_cents", ty: INT64, optional: false, logical: Logical::None },
        ColumnSpec { name: "top_category", ty: BYTE_ARRAY, optional: true, logical: Logical::String },
    ];
    let user_id = Fixed {
        rows,
        f: |r: &UserRow, out: &mut Vec<u8>| {
            let mut buf = [0u8; 10];
            put_bytes(out, ids::bytes(r.person, &mut buf, odd));
        },
    };
    let day = Fixed { rows, f: |r: &UserRow, out: &mut Vec<u8>| out.extend_from_slice(&r.day.to_le_bytes()) };
    let sessions = Fixed { rows, f: |r: &UserRow, out: &mut Vec<u8>| out.extend_from_slice(&(r.sessions as i32).to_le_bytes()) };
    let events = Fixed { rows, f: |r: &UserRow, out: &mut Vec<u8>| out.extend_from_slice(&(r.events as i32).to_le_bytes()) };
    let revenue = Fixed { rows, f: |r: &UserRow, out: &mut Vec<u8>| out.extend_from_slice(&r.revenue.to_le_bytes()) };
    let top = TopCategory { rows, names: &ctx.categories };
    write_file(path, &specs, &[&user_id, &day, &sessions, &events, &revenue, &top], rows.len());
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
    let id = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| put_bytes(out, r.id) };
    let channel = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| put_bytes(out, r.channel) };
    let day = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| out.extend_from_slice(&r.day.to_le_bytes()) };
    let sessions = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| out.extend_from_slice(&r.stat.sessions.to_le_bytes()) };
    let purchasing = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| out.extend_from_slice(&r.stat.purchasing.to_le_bytes()) };
    let revenue = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| out.extend_from_slice(&r.stat.revenue.to_le_bytes()) };
    let assisted = Fixed { rows, f: |r: &CampRow, out: &mut Vec<u8>| out.extend_from_slice(&r.stat.assisted.to_le_bytes()) };
    write_file(path, &specs, &[&id, &channel, &day, &sessions, &purchasing, &revenue, &assisted], rows.len());
}

pub fn write_all(out: &Path, ctx: &Ctx, odd: &Odd, parts: &mut [PersonOut]) {
    let user_dir = out.join("user_daily");
    let camp_dir = out.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_dir).expect("mkdir campaign_daily");

    std::thread::scope(|sc| {
        // One file per thread's rows; at least one file even if all are empty.
        let last = parts.len().saturating_sub(1);
        let mut wrote_any = false;
        for (i, p) in parts.iter().enumerate() {
            if p.users.is_empty() && (wrote_any || i < last) {
                continue;
            }
            wrote_any = true;
            let path = user_dir.join(format!("part-{i:05}.parquet"));
            let rows = &p.users[..];
            sc.spawn(move || user_file(&path, rows, ctx, odd));
        }

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
    });
}
