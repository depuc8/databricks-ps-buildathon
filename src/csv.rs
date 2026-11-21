//! The CSV dialect from rules.md: `,` separates, a field may be wrapped in
//! quotes as a whole, any other quote makes the line malformed, and a line
//! must have exactly N fields. An empty line has one empty field, so it is
//! malformed for every N >= 2.
//!
//! No value holds a comma or newline, so quotes never change where fields
//! split. A line's quoting is valid exactly when its quote count is twice the
//! number of its fields that start and end with a quote (length >= 2): any
//! other quote is one too many. The scan finds `,` `\n` `"` 64 bytes at a time
//! as bitmaps.

/// Splits `line` into exactly N unquoted values; false if malformed.
#[inline]
pub fn split<const N: usize>(line: &'static [u8], out: &mut [&'static [u8]; N]) -> bool {
    let len = line.len();
    let mut n = 0;
    let mut start = 0;
    loop {
        let mut j = start;
        let mut quotes = 0u32;
        while j < len {
            let c = line[j];
            if c == b',' {
                break;
            }
            quotes += (c == b'"') as u32;
            j += 1;
        }
        if n == N {
            return false;
        }
        let field = &line[start..j];
        out[n] = if quotes == 0 {
            field
        } else if quotes == 2 && field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
            &field[1..field.len() - 1]
        } else {
            return false;
        };
        n += 1;
        if j == len {
            return n == N;
        }
        start = j + 1;
    }
}

/// Bitmaps of `,` or `\n`, of `\n`, and of `"` for the 64 bytes at `p`.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let comma = _mm256_set1_epi8(b',' as i8);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    let m = |v: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
    let n = m(a, nl) | m(b, nl) << 32;
    let c = m(a, comma) | m(b, comma) << 32;
    let q = m(a, quote) | m(b, quote) << 32;
    (c | n, n, q)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let (mut s, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let c = *p.add(i);
        n |= ((c == b'\n') as u64) << i;
        s |= ((c == b',' || c == b'\n') as u64) << i;
        q |= ((c == b'"') as u64) << i;
    }
    (s, n, q)
}

const NL: u32 = 1 << 31;
const POS: u32 = NL - 1;

/// One well-formed line: field i is `get(i)`, unquoted.
pub struct Line<'a> {
    chunk: &'static [u8],
    /// Each field as start | end << 32, quotes already stripped.
    bounds: &'a [u64; 16],
}

/// Bounds of the field [s, e) without its wrapping quotes, branch-free.
#[inline(always)]
fn unwrap_field(chunk: &[u8], s: usize, e: usize) -> (usize, usize, usize) {
    let ok = e >= s + 2;
    let (a, b) = if ok { (s, e - 1) } else { (0, 0) };
    // SAFETY: a, b < chunk.len() (0 when the field is shorter than 2).
    let q = (ok & (unsafe { *chunk.get_unchecked(a) } == b'"') & (unsafe { *chunk.get_unchecked(b) } == b'"')) as usize;
    (s + q, e - q, q)
}

impl Line<'_> {
    /// Field i (i < N), and whether 16 bytes from its start are readable.
    #[inline(always)]
    pub fn get_w(&self, i: usize) -> (&'static [u8], bool) {
        let f = self.get(i);
        (f, self.bounds[i] as u32 as usize + 16 <= self.chunk.len())
    }

    /// Field i (i < N).
    #[inline(always)]
    pub fn get(&self, i: usize) -> &'static [u8] {
        let v = self.bounds[i];
        // SAFETY: start <= end <= chunk.len() for the N fields of a line.
        unsafe { self.chunk.get_unchecked(v as u32 as usize..(v >> 32) as usize) }
    }
}

/// Receives each well-formed line. A trait method, unlike a closure, can be
/// forced inline into the scan loop.
pub trait Sink {
    fn line(&mut self, l: &Line);
}

impl<F: FnMut(&Line)> Sink for F {
    #[inline(always)]
    fn line(&mut self, l: &Line) {
        self(l)
    }
}

/// The end of a line with `nfields` fields, whose bounds are in `bounds`:
/// strips and checks the quotes, then passes the line on if well-formed.
#[inline(always)]
fn line_end<const N: usize>(
    chunk: &'static [u8],
    bounds: &mut [u64; 16],
    nfields: usize,
    quotes: usize,
    f: &mut impl Sink,
) {
    if nfields != N {
        return;
    }
    if quotes != 0 {
        let mut wrapped = 0;
        for b in bounds.iter_mut().take(N) {
            let (fs, fe, q) = unwrap_field(chunk, *b as u32 as usize, (*b >> 32) as usize);
            *b = fs as u64 | (fe as u64) << 32;
            wrapped += q;
        }
        if 2 * wrapped != quotes {
            return;
        }
    }
    f.line(&Line { chunk, bounds });
}

/// Calls `f` with every well-formed line of N fields in `chunk`. N <= 15.
///
/// Each field's bounds are stored as one u64 as its separator is found, so
/// the per-line code reads them back with loads of the same width (a wider
/// load of several fresh narrow stores stalls on store forwarding).
#[inline(always)]
pub fn for_each_line<const N: usize>(chunk: &'static [u8], mut f: impl Sink) {
    let len = chunk.len();
    assert!(len < POS as usize, "chunk too large");
    let base = chunk.as_ptr();
    let mut bounds = [0u64; 16];
    let mut nsep = 0usize;
    let mut field_start = 0u64;
    let mut q_before = 0usize; // quotes before the current block
    let mut q_line = 0usize; // quotes before the current line
    let mut tail = [0u8; 64];

    let mut b = 0;
    while b < len {
        let (sep, nl, q) = if b + 64 <= len {
            unsafe { masks(base.add(b)) }
        } else {
            tail.fill(0);
            tail[..len - b].copy_from_slice(&chunk[b..]);
            unsafe { masks(tail.as_ptr()) }
        };
        let mut m = sep;
        while m != 0 {
            let tz = m.trailing_zeros() as usize;
            let p = (b + tz) as u64;
            if nsep < 16 {
                bounds[nsep] = field_start | p << 32;
            }
            nsep += 1;
            field_start = p + 1;
            if (nl >> tz) & 1 != 0 {
                let q_at = q_before + (q & ((1u64 << tz) - 1)).count_ones() as usize;
                line_end::<N>(chunk, &mut bounds, nsep, q_at - q_line, &mut f);
                q_line = q_at;
                nsep = 0;
            }
            m &= m - 1;
        }
        q_before += q.count_ones() as usize;
        b += 64;
    }
    if (field_start as usize) < len || nsep > 0 {
        if nsep < 16 {
            bounds[nsep] = field_start | (len as u64) << 32;
        }
        line_end::<N>(chunk, &mut bounds, nsep + 1, q_before - q_line, &mut f);
    }
}

/// Calls `f` with the N values of every well-formed line in `chunk`. N <= 15.
#[inline(always)]
pub fn for_each<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(&[&'static [u8]; N])) {
    for_each_line::<N>(chunk, |line: &Line| f(&std::array::from_fn(|i| line.get(i))));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect() {
        let mut f: [&[u8]; 2] = [&[]; 2];
        assert!(split(b"a,b", &mut f) && f == [b"a" as &[u8], b"b"]);
        assert!(split(b"\"a\",\"\"", &mut f) && f == [b"a" as &[u8], b""]);
        assert!(split(b",", &mut f) && f == [b"" as &[u8], b""]);
        assert!(!split(b"", &mut f));
        assert!(!split(b"a", &mut f));
        assert!(!split(b"a,b,c", &mut f));
        assert!(!split(b"a,b,", &mut f));
        assert!(!split(b"a,b\"", &mut f));
        assert!(!split(b"\"a,b", &mut f));
        assert!(!split(b"\",b", &mut f));
        assert!(!split(b"\"\"\",b", &mut f));
        assert!(!split(b"a\"b\",c", &mut f));
        assert!(split(b" a , b\r", &mut f) && f == [b" a " as &[u8], b" b\r"]);
    }

    fn scan(text: &'static str) -> Vec<Vec<String>> {
        let mut rows = Vec::new();
        for_each::<3>(text.as_bytes(), |f| {
            rows.push(f.iter().map(|v| String::from_utf8(v.to_vec()).unwrap()).collect())
        });
        rows
    }

    fn by_lines(text: &'static str) -> Vec<Vec<String>> {
        let mut rows = Vec::new();
        let mut f: [&[u8]; 3] = [&[]; 3];
        let mut lines: Vec<&'static str> = text.split('\n').collect();
        if text.ends_with('\n') {
            lines.pop();
        }
        for line in lines {
            if split(line.as_bytes(), &mut f) {
                rows.push(f.iter().map(|v| String::from_utf8(v.to_vec()).unwrap()).collect());
            }
        }
        rows
    }

    fn scan_n<const N: usize>(text: &'static [u8]) -> Vec<Vec<Vec<u8>>> {
        let mut rows = Vec::new();
        for_each::<N>(text, |f| rows.push(f.iter().map(|v| v.to_vec()).collect()));
        rows
    }

    fn by_lines_n<const N: usize>(text: &'static [u8]) -> Vec<Vec<Vec<u8>>> {
        let mut rows = Vec::new();
        let mut f: [&[u8]; N] = [&[]; N];
        let mut lines: Vec<&'static [u8]> = text.split(|&c| c == b'\n').collect();
        if text.ends_with(b"\n") || text.is_empty() {
            lines.pop();
        }
        for line in lines {
            if split(line, &mut f) {
                rows.push(f.iter().map(|v| v.to_vec()).collect());
            }
        }
        rows
    }

    /// Random text in the dialect's corner cases, against the line splitter.
    #[test]
    fn scan_fuzz() {
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut rnd = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for case in 0..3000 {
            let mut t: Vec<u8> = Vec::new();
            let lines = 1 + rnd(if case % 50 == 0 { 3000 } else { 40 });
            for _ in 0..lines {
                let fields = match rnd(10) {
                    0 => rnd(12),
                    1 => 6 + rnd(3),
                    _ => if case % 2 == 0 { 7 } else { 3 },
                };
                for i in 0..fields {
                    if i > 0 {
                        t.push(b',');
                    }
                    let quote = rnd(5) == 0;
                    if quote {
                        t.push(b'"');
                    }
                    let flen = match rnd(200) { 0 => 20000 + rnd(30000), 1..=20 => 0, _ => rnd(14) };
                    for _ in 0..flen {
                        t.push(match rnd(60) { 0 => b'"', 1 => b'\r', 2 => b' ', _ => b'a' + rnd(26) as u8 });
                    }
                    if quote && rnd(30) != 0 {
                        t.push(b'"');
                    }
                }
                if rnd(40) == 0 {
                    t.push(b'\n');
                }
                t.push(b'\n');
            }
            if rnd(3) == 0 {
                t.pop();
            }
            let text: &'static [u8] = Box::leak(t.into_boxed_slice());
            assert_eq!(scan_n::<7>(text), by_lines_n::<7>(text), "case {case} N=7");
            assert_eq!(scan_n::<3>(text), by_lines_n::<3>(text), "case {case} N=3");
            assert_eq!(scan_n::<2>(text), by_lines_n::<2>(text), "case {case} N=2");
        }
    }

    #[test]
    fn scan_matches_split() {
        let cases: Vec<&'static str> = vec![
            "a,b,c\n\"a\",b,\"\"\n,,\n\n\"\"\"\",b,c\na,b\na,b,c,d\n\"a,b,c\n x ,y\r,z\na\"b,c,d\nlast,line,nolf",
            "a,b,c\n",
            "",
            "\n\n\n",
            "\",b,c\n\"\",\"\",\"\"\n",
        ];
        let mut long = String::new();
        for i in 0..500 {
            long.push_str(&format!("id{i},\"u{}\",{}\n", i % 7, if i % 3 == 0 { "\"x\"\"" } else { "t" }));
            if i % 11 == 0 {
                long.push_str("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t\n");
            }
        }
        let long: &'static str = Box::leak(long.into_boxed_str());
        for text in cases.into_iter().chain([long]) {
            assert_eq!(scan(text), by_lines(text), "{text:?}");
        }
    }
}
