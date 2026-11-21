//! The CSV dialect from rules.md: `,` separates, a field may be wrapped in
//! quotes as a whole, any other quote makes the line malformed, and a line
//! must have exactly N fields. An empty line has one empty field, so it is
//! malformed for every N >= 2.
//!
//! No value holds a comma or newline, so quotes never change where fields
//! split. The scan finds `,` `\n` `"` 64 bytes at a time as bitmaps, and
//! checks the quotes of a whole block with bit operations (see `scan`).

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

/// One well-formed line: field i is `get(i)`, unquoted.
pub struct Line<'a> {
    chunk: &'static [u8],
    /// The newline before the line, then the line's N separators.
    seps: &'a [u32],
}

impl Line<'_> {
    /// Field i (i < N), and whether 16 bytes from its start are readable.
    #[inline(always)]
    pub fn get_w(&self, i: usize) -> (&'static [u8], bool) {
        let f = self.get(i);
        (f, f.as_ptr() as usize + 16 <= self.chunk.as_ptr() as usize + self.chunk.len())
    }

    /// Field i (i < N).
    #[inline(always)]
    pub fn get(&self, i: usize) -> &'static [u8] {
        debug_assert!(i + 1 < self.seps.len());
        // SAFETY: i < N, so both entries exist; s <= e < chunk.len(), e
        // being a separator. A field that starts with a quote is wrapped in
        // quotes: the line passed the check.
        unsafe {
            let s = self.seps.get_unchecked(i).wrapping_add(1) as usize;
            let e = *self.seps.get_unchecked(i + 1) as usize;
            let q = (*self.chunk.get_unchecked(s) == b'"') as usize;
            self.chunk.get_unchecked(s + q..e - q)
        }
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

/// XOR of bits 0..=i of x, in bit i: a carry-less multiply by all ones.
#[cfg(target_feature = "pclmulqdq")]
#[inline(always)]
fn prefix_xor(x: u64) -> u64 {
    use std::arch::x86_64::*;
    unsafe { _mm_cvtsi128_si64(_mm_clmulepi64_si128::<0>(_mm_cvtsi64_si128(x as i64), _mm_set1_epi8(-1))) as u64 }
}

#[cfg(not(target_feature = "pclmulqdq"))]
#[inline(always)]
fn prefix_xor(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^ x << 32
}

/// Calls `f` with every well-formed line of N fields in `chunk`. N < 32.
#[inline(always)]
pub fn for_each_line<const N: usize>(chunk: &'static [u8], f: &mut impl Sink) {
    let main = match memchr::memrchr(b'\n', chunk) {
        Some(i) => &chunk[..=i],
        None => &chunk[..0],
    };
    scan::<N>(main, f);
    // A last line without its newline (only at the end of a file): scan a
    // copy that has one.
    let rest = &chunk[main.len()..];
    if !rest.is_empty() {
        let mut v = rest.to_vec();
        v.push(b'\n');
        scan::<N>(Box::leak(v.into_boxed_slice()), f);
    }
}

/// `for_each_line` for a chunk that ends with a newline.
///
/// Quotes are checked for all the lines of a block at once. With the quote
/// parity counted from the start of each line, a line's quoting is valid
/// exactly when every quote that opens (parity 1) starts a field, every
/// quote that closes (parity 0) is followed by a separator, and no separator
/// (the newline included) has parity 1. Then a field starting with a quote
/// is wrapped in quotes, and no other field holds one. The parity from the
/// start of the chunk, G, becomes per line by XOR with C, G at the newline
/// before: C is a fill from each newline where G is 1 up to the next
/// newline, made by one subtraction (a borrow runs up to the next newline).
/// A line is dropped if it has a bad bit or does not have N separators.
#[inline(always)]
fn scan<const N: usize>(chunk: &'static [u8], f: &mut impl Sink) {
    let len = chunk.len();
    assert!(len < u32::MAX as usize, "chunk too large");
    let base = chunk.as_ptr();
    // Separator positions in order, from the newline before the chunk. When
    // full, the entries of the line in progress move to the front.
    const SEPS: usize = 1024;
    let mut pos = [0u32; SEPS];
    pos[0] = u32::MAX; // the newline before the chunk
    let mut w = 1usize; // entries in pos
    let mut line = 1usize; // the first entry of the line in progress
    let mut par = 0u64; // all ones after an odd number of quotes
    let mut borrow = 0u64; // the line in progress started at parity 1
    let mut sep_hi = 1u64; // a field starts at the next block's byte 0
    let mut close_hi = 0u64; // a closing quote ended the last block
    let mut bad_open = false; // a bad bit in the line in progress
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
        let g = prefix_xor(q) ^ par;
        par = ((g as i64) >> 63) as u64;
        let ones = nl & g;
        let (r, o1) = nl.overflowing_sub(ones << 1);
        let (r, o2) = r.overflowing_sub(borrow);
        borrow = (o1 | o2) as u64 | ones >> 63;
        let parity = g ^ nl ^ r;
        let starts = sep << 1 | sep_hi;
        sep_hi = sep >> 63;
        let close = q & !parity;
        let mut bad = (q & parity & !starts) | ((close << 1 | close_hi) & !sep) | (sep & parity);
        close_hi = close >> 63;

        if w > SEPS - 72 {
            // A line already past N separators is dropped anyway: restart
            // it here, marked bad.
            if w - line > N {
                bad_open = true;
                line = w;
            }
            pos.copy_within(line - 1..w, 0);
            w -= line - 1;
            line = 1;
        }
        // Every separator position goes to pos, eight unconditionally.
        let cnt = sep.count_ones() as usize;
        let mut m = sep;
        let out: &mut [u32; 72] = (&mut pos[w..w + 72]).try_into().unwrap();
        for slot in out.iter_mut().take(8) {
            *slot = (b as u32).wrapping_add(m.trailing_zeros());
            m &= m.wrapping_sub(1);
        }
        let mut k = 8;
        while m != 0 {
            out[k & 63] = b as u32 + m.trailing_zeros();
            m &= m - 1;
            k += 1;
        }
        let mut n = nl;
        while n != 0 {
            let upto = (2u64 << n.trailing_zeros()).wrapping_sub(1);
            let end = w + (sep & upto).count_ones() as usize;
            if end - line == N && !bad_open && bad & upto == 0 {
                f.line(&Line { chunk, seps: &pos[line - 1..end] });
            }
            bad &= !upto;
            bad_open = false;
            line = end;
            n &= n - 1;
        }
        bad_open |= bad != 0;
        w += cnt;
        b += 64;
    }
}

/// Calls `f` with the N values of every well-formed line in `chunk`. N < 32.
#[inline(always)]
pub fn for_each<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(&[&'static [u8]; N])) {
    for_each_line::<N>(chunk, &mut |line: &Line| f(&std::array::from_fn(|i| line.get(i))));
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

    /// Random text over a tiny alphabet: odd quote counts, quotes at block
    /// edges, runs of empty lines and fields.
    #[test]
    fn scan_fuzz_dense() {
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut rnd = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for case in 0..20_000 {
            let alphabet: &[u8] = match case % 4 {
                0 => b"\",\na",
                1 => b"\"\",,,\nab\r",
                2 => b"\"a,\n",
                _ => b"\"\"\"\",,,,,,\n\naaaaaaaaaa",
            };
            let n = rnd(if case % 10 == 0 { 2000 } else { 300 }) as usize;
            let t: Vec<u8> = (0..n).map(|_| alphabet[rnd(alphabet.len() as u64) as usize]).collect();
            let text: &'static [u8] = Box::leak(t.into_boxed_slice());
            assert_eq!(scan_n::<7>(text), by_lines_n::<7>(text), "case {case} N=7");
            assert_eq!(scan_n::<4>(text), by_lines_n::<4>(text), "case {case} N=4");
            assert_eq!(scan_n::<2>(text), by_lines_n::<2>(text), "case {case} N=2");
            assert_eq!(scan_n::<1>(text), by_lines_n::<1>(text), "case {case} N=1");
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
