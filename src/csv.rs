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

/// Calls `f` with the N values of every well-formed line in `chunk`. N <= 15.
#[inline(always)]
pub fn for_each<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(&[&'static [u8]; N])) {
    let len = chunk.len();
    let base = chunk.as_ptr();
    // ends[i] is where field i ends; field i starts after ends[i-1] (or at line_start).
    let mut ends = [0usize; 16];
    let mut nsep = 0usize;
    let mut line_start = 0usize;
    let mut q_before = 0usize; // quotes before the current block
    let mut q_line = 0usize; // quotes before line_start
    let mut tail = [0u8; 64];
    let mut out: [&'static [u8]; N] = [&[]; N];

    let mut emit = |line_start: usize, nfields: usize, ends: &[usize; 16], quotes: usize| {
        if nfields != N {
            return;
        }
        let mut start = line_start;
        if quotes == 0 {
            for i in 0..N {
                // SAFETY: start <= ends[i] <= len.
                out[i] = unsafe { std::slice::from_raw_parts(base.add(start), ends[i] - start) };
                start = ends[i] + 1;
            }
        } else {
            let mut wrapped = 0;
            for i in 0..N {
                let mut s = start;
                let mut e = ends[i];
                if e - s >= 2 && chunk[s] == b'"' && chunk[e - 1] == b'"' {
                    s += 1;
                    e -= 1;
                    wrapped += 2;
                }
                out[i] = &chunk[s..e];
                start = ends[i] + 1;
            }
            if wrapped != quotes {
                return;
            }
        }
        f(&out);
    };

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
            let p = b + tz;
            if nsep < 16 {
                ends[nsep] = p;
            }
            nsep += 1;
            if (nl >> tz) & 1 != 0 {
                let q_at = q_before + (q & ((1u64 << tz) - 1)).count_ones() as usize;
                emit(line_start, nsep, &ends, q_at - q_line);
                line_start = p + 1;
                q_line = q_at;
                nsep = 0;
            }
            m &= m - 1;
        }
        q_before += q.count_ones() as usize;
        b += 64;
    }
    if line_start < len {
        if nsep < 16 {
            ends[nsep] = len;
        }
        emit(line_start, nsep + 1, &ends, q_before - q_line);
    }
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
