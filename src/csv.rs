//! The CSV dialect from rules.md: `,` separates, a field may be wrapped in
//! quotes as a whole, any other quote makes the line malformed, and a line
//! must have exactly N fields. An empty line has one empty field, so it is
//! malformed for every N >= 2.

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
}
