//! Timestamps (F1, F2, F3) and prices, ASCII digits only.

#[inline]
fn digit(c: u8) -> Option<i64> {
    let d = c.wrapping_sub(b'0');
    if d < 10 {
        Some(d as i64)
    } else {
        None
    }
}

pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

const YEAR0: i64 = 1970;
const YEARS: usize = 131;

/// Days from the epoch to January 1 of each year from 1970 to 2100.
static YEAR_DAYS: [i32; YEARS] = {
    let mut t = [0i32; YEARS];
    let mut i = 0;
    while i < YEARS {
        t[i] = days_from_civil(YEAR0 + i as i64, 1, 1) as i32;
        i += 1;
    }
    t
};

/// Days before each month (1-based), in common and leap years.
static MONTH_DAYS: [[i32; 13]; 2] = [
    [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];

/// Days from the epoch for a valid date.
#[inline(always)]
fn days(year: i64, month: i64, day: i64, leap: bool) -> i64 {
    let y = year - YEAR0;
    if (0..YEARS as i64).contains(&y) {
        YEAR_DAYS[y as usize] as i64 + MONTH_DAYS[leap as usize][month as usize] as i64 + day - 1
    } else {
        days_from_civil(year, month, day)
    }
}

#[inline(always)]
fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline(always)]
fn load8(s: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(s[i..i + 8].try_into().unwrap())
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// Whether every byte selected by `mask` (0xFF per byte) is an ASCII digit.
#[inline(always)]
fn all_digits(w: u64, mask: u64) -> bool {
    let x = w ^ ZEROS;
    let bad = ((x & 0x7f7f_7f7f_7f7f_7f7f).wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080;
    bad & mask == 0
}

/// Byte i of the result is 10 * digit(i) + digit(i + 1), for the digit
/// bytes selected by `mask`.
#[inline(always)]
fn pairs(w: u64, mask: u64) -> u64 {
    let x = (w ^ ZEROS) & mask;
    x.wrapping_mul(10).wrapping_add(x >> 8)
}

/// Milliseconds since the epoch, or None for a bad timestamp. An F3 value too
/// large for i64 is None too: it is outside every window anyway.
#[inline(always)]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s).map(|t| t * 1000);
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    if n <= 18 {
        for &c in s {
            v = v * 10 + digit(c)?;
        }
    } else {
        for &c in s {
            v = v.checked_mul(10)?.checked_add(digit(c)?)?;
        }
    }
    Some(v)
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` or F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, in seconds.
#[inline(always)]
fn parse_iso(s: &[u8]) -> Option<i64> {
    // Little-endian: byte 0 of each word is its first character.
    let a = load8(s, 0); // YYYY-MM-
    let b = load8(s, 8); // DDTHH:MM
    let c = load8(s, 12); // H:MM:SS? (byte 4 ':', bytes 5-6 the seconds, byte 7 'Z' or the sign)
    const A_DIG: u64 = 0x00ff_ff00_ffff_ffff;
    const B_DIG: u64 = 0xffff_00ff_ff00_ffff;
    const C_DIG: u64 = 0x00ff_ff00_0000_0000;
    const A_SEP: u64 = 0xff00_00ff_0000_0000;
    const B_SEP: u64 = 0x0000_ff00_00ff_0000;
    const C_SEP: u64 = 0x0000_00ff_0000_0000;
    if !all_digits(a, A_DIG) || !all_digits(b, B_DIG) || !all_digits(c, C_DIG) {
        return None;
    }
    if a & A_SEP != 0x2d00_002d_0000_0000 || b & B_SEP != 0x0000_3a00_0054_0000 || c & C_SEP != 0x0000_003a_0000_0000 {
        return None;
    }
    let pa = pairs(a, A_DIG);
    let pb = pairs(b, B_DIG);
    let pc = pairs(c, C_DIG);
    let year = ((pa & 0xff) * 100 + ((pa >> 16) & 0xff)) as i64;
    let month = ((pa >> 40) & 0xff) as i64;
    let day = (pb & 0xff) as i64;
    let hour = ((pb >> 24) & 0xff) as i64;
    let minute = ((pb >> 48) & 0xff) as i64;
    let second = ((pc >> 40) & 0xff) as i64;
    if !(1..=12).contains(&month) || day < 1 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let leap = is_leap(year);
    let month_len = match month {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day > month_len {
        return None;
    }
    let mut t = ((days(year, month, day, leap) * 24 + hour) * 60 + minute) * 60 + second;
    let zone = (c >> 56) as u8;
    if s.len() == 20 {
        if zone != b'Z' {
            return None;
        }
    } else {
        let sign = match zone {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let d = load8(s, 17); // SS+HH:MM
        const D_DIG: u64 = 0xffff_00ff_ff00_0000;
        if !all_digits(d, D_DIG) || (d >> 40) as u8 != b':' {
            return None;
        }
        let pd = pairs(d, D_DIG);
        let oh = ((pd >> 24) & 0xff) as i64;
        let om = ((pd >> 48) & 0xff) as i64;
        if oh > 23 || om > 59 {
            return None;
        }
        t -= sign * (oh * 60 + om) * 60;
    }
    Some(t)
}

/// Year, month, day of a day count from the epoch.
pub const fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + (month <= 2) as i64, month, day)
}

/// Timestamp parsing for one dataset. Nearly every date falls in the few
/// months around the window, so the table holds four of them: a date whose
/// `YYYY-MM-` equals a key has a valid year and month and both dashes, and
/// gets its day count from the table. Every other timestamp goes through the
/// full parser, so the results never depend on the table.
pub struct Fast {
    keys: [u64; 4],
    /// Seconds from the epoch to the day before the 1st of the month.
    base: [i64; 4],
    len: [u64; 4],
}

impl Fast {
    /// The table for the month of `from_ms` and the three after it.
    pub fn new(from_ms: i64) -> Fast {
        let (mut y, mut m, _) = civil_from_days(from_ms.div_euclid(86_400_000));
        let mut f = Fast { keys: [0; 4], base: [0; 4], len: [0; 4] };
        for i in 0..4 {
            if (0..=9999).contains(&y) {
                let key = format!("{y:04}-{m:02}-");
                f.keys[i] = u64::from_le_bytes(key.as_bytes().try_into().unwrap());
                f.base[i] = (days_from_civil(y, m, 1) - 1) * 86_400;
                let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
                f.len[i] = (days_from_civil(ny, nm, 1) - days_from_civil(y, m, 1)) as u64;
            }
            (y, m) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        }
        f
    }

    /// Whole seconds since the epoch (floored): `parse_ts` divided by 1000,
    /// without the multiply and divide for F1 and F2, the forms of every
    /// ingest_ts.
    #[inline(always)]
    pub fn secs(&self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if (n == 20 || n == 25) && s[4] == b'-' {
            return self.iso(s);
        }
        parse_ts(s).map(|t| t.div_euclid(1000))
    }

    /// `secs` for an ingest_ts, which rules.md guarantees to be a valid F1:
    /// one in the table's months skips the checks.
    #[inline(always)]
    pub fn ingest_secs(&self, s: &[u8]) -> Option<i64> {
        if s.len() == 20 {
            let a = load8(s, 0);
            let hit = [a == self.keys[0], a == self.keys[1], a == self.keys[2], a == self.keys[3]];
            if hit[0] | hit[1] | hit[2] | hit[3] {
                let i = hit[1] as usize | (hit[2] as usize) << 1 | (hit[3] as usize) * 3;
                let x = load8(s, 8); // DDTHH:MM
                let pd = pairs(load8(s, 11), 0xffff_00ff_ff00_ffff); // HH:MM:SS
                let day = (x & 0xf) * 10 + (x >> 8 & 0xf);
                return Some(
                    self.base[i] + (day * 86_400 + (pd & 0xff) * 3600 + (pd >> 24 & 0xff) * 60 + (pd >> 48 & 0xff)) as i64,
                );
            }
        }
        self.secs(s)
    }

    /// `parse_ts`, given whether 16 bytes from `s.as_ptr()` are readable.
    #[inline(always)]
    pub fn ms(&self, s: &[u8], wide: bool) -> Option<i64> {
        let n = s.len();
        if (n == 20 || n == 25) && s[4] == b'-' {
            return self.iso(s).map(|t| t * 1000);
        }
        if wide {
            // SAFETY: the caller vouches for 16 readable bytes.
            unsafe { digits_wide(s) }
        } else {
            parse_ts(s)
        }
    }

    /// `parse_iso` for s of length 20 or 25.
    #[inline(always)]
    fn iso(&self, s: &[u8]) -> Option<i64> {
        let a = load8(s, 0); // YYYY-MM-
        let hit = [a == self.keys[0], a == self.keys[1], a == self.keys[2], a == self.keys[3]];
        if !(hit[0] | hit[1] | hit[2] | hit[3]) {
            return parse_iso(s);
        }
        let i = hit[1] as usize | (hit[2] as usize) << 1 | (hit[3] as usize) * 3;
        let x = load8(s, 8); // DDTHH:MM
        let d = load8(s, 11); // HH:MM:SS
        const D_DIG: u64 = 0xffff_00ff_ff00_ffff;
        const D_SEP: u64 = 0x0000_ff00_00ff_0000;
        if !all_digits(x, 0xffff) || (x >> 16) as u8 != b'T' || !all_digits(d, D_DIG) || d & D_SEP != 0x0000_3a00_003a_0000 {
            return None;
        }
        let day = (x & 0xf) * 10 + (x >> 8 & 0xf);
        // Bytes 0, 3 and 6: hour, minute, second; each byte at most 99, so
        // adding 127 - limit sets its top bit exactly when over the limit.
        let pd = pairs(d, D_DIG) & 0x00ff_0000_ff00_00ff;
        if pd.wrapping_add(104 | 68 << 24 | 68 << 48) & 0x0080_0000_8000_0080 != 0 || day == 0 || day > self.len[i] {
            return None;
        }
        let t = self.base[i] + (day * 86_400 + (pd & 0xff) * 3600 + (pd >> 24 & 0xff) * 60 + (pd >> 48)) as i64;
        let zone = s[19];
        if s.len() == 20 {
            return if zone == b'Z' { Some(t) } else { None };
        }
        let sign = match zone {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let o = load8(s, 17); // SS+HH:MM
        const O_DIG: u64 = 0xffff_00ff_ff00_0000;
        if !all_digits(o, O_DIG) || (o >> 40) as u8 != b':' {
            return None;
        }
        let po = pairs(o, O_DIG);
        let (oh, om) = ((po >> 24 & 0xff) as i64, (po >> 48 & 0xff) as i64);
        if oh > 23 || om > 59 {
            return None;
        }
        Some(t - sign * (oh * 60 + om) * 60)
    }
}

/// An F3 timestamp (digits only) with SSE: `parse_ts` for anything else.
///
/// # Safety
/// 16 bytes from `s.as_ptr()` must be readable.
#[cfg(target_feature = "sse4.1")]
#[inline(always)]
unsafe fn digits_wide(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    let n = s.len();
    if n == 0 || n > 16 {
        return parse_ts(s);
    }
    let x = _mm_sub_epi8(_mm_loadu_si128(s.as_ptr() as *const __m128i), _mm_set1_epi8(b'0' as i8));
    let ok = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(x, _mm_set1_epi8(9)), x)) as u32;
    let need = (1u32 << n) - 1;
    if ok & need != need {
        return None;
    }
    // Right-align the n digits, zeros before them, then combine pairs,
    // quads and octets.
    let y = _mm_shuffle_epi8(x, _mm_loadu_si128(ALIGN[n].as_ptr() as *const __m128i));
    let t1 = _mm_maddubs_epi16(y, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
    let t2 = _mm_madd_epi16(t1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
    let t3 = _mm_packus_epi32(t2, t2);
    let t4 = _mm_madd_epi16(t3, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
    let hi = _mm_cvtsi128_si32(t4) as u32 as i64;
    let lo = _mm_extract_epi32::<1>(t4) as u32 as i64;
    Some(hi * 100_000_000 + lo)
}

#[cfg(not(target_feature = "sse4.1"))]
#[inline(always)]
unsafe fn digits_wide(s: &[u8]) -> Option<i64> {
    parse_ts(s)
}

/// ALIGN[n]: the shuffle that moves the first n of 16 bytes to the end and
/// zeroes the rest.
static ALIGN: [[u8; 16]; 17] = {
    let mut t = [[0x80u8; 16]; 17];
    let mut n = 0;
    while n <= 16 {
        let mut j = 16 - n;
        while j < 16 {
            t[n][j] = (j - (16 - n)) as u8;
            j += 1;
        }
        n += 1;
    }
    t
};

/// A purchase price: digits only, 1 to 10,000,000.
#[inline]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        v = v * 10 + digit(c)? as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(f1));
        assert_eq!(parse_ts(b"0001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-04T01:30:00+02:00"), parse_ts(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse_ts(b"2026-10-03T10:00:00-00:00"), parse_ts(b"2026-10-03T10:00:00Z"));
        assert_eq!(parse_ts(b"2028-02-29T00:00:00Z").map(|t| t % 1000), Some(0));
        for bad in [
            &b""[..],
            b"2026-10-03T23:58:10.000Z",
            b"2026-10-03T23:58:10z",
            b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10",
            b"1791071890000.0",
            b"1.79e12",
            b"-5",
            b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z",
            b"2027-02-29T23:58:10Z",
            b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:58:60Z",
            b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200",
            b"99999999999999999999999",
            "２026-10-03T23:58:10Z".as_bytes(),
            "١٧٩١٠٧١٨٩٠٠٠٠".as_bytes(),
            b"12345678901234567890",
            b"2026-10-03T23:58:10Z\r",
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
    }

    /// The table-driven parser against the full one, on timestamps around
    /// a window and corruptions of them.
    #[test]
    fn fast_matches() {
        let mut seed: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        for case in 0..200_000 {
            let ws = parse_ts(b"2028-01-25T00:00:00Z").unwrap() + rnd(800) as i64 * 86_400_000 - 400 * 86_400_000;
            let fast = Fast::new(ws - 9 * 86_400_000);
            let t = ws + rnd(40 * 86_400) as i64 * 1000 - 12 * 86_400_000;
            let (y, mo, d) = civil_from_days(t.div_euclid(86_400_000));
            let sec = t.div_euclid(1000).rem_euclid(86_400);
            let mut text = match rnd(3) {
                0 => format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}Z", sec / 3600, sec / 60 % 60, sec % 60),
                1 => format!(
                    "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}{}{:02}:{:02}",
                    sec / 3600,
                    sec / 60 % 60,
                    sec % 60,
                    if rnd(2) == 0 { '+' } else { '-' },
                    rnd(26),
                    rnd(62)
                ),
                _ => format!("{:0w$}", t + rnd(1000) as i64, w = rnd(19) as usize),
            }
            .into_bytes();
            if case % 2 == 0 {
                for _ in 0..1 + rnd(2) {
                    if text.is_empty() {
                        break;
                    }
                    let i = rnd(text.len() as u64) as usize;
                    text[i] = b"0123456789:-+TZtz .\r9"[rnd(21) as usize];
                }
            }
            if case % 7 == 0 && !text.is_empty() {
                text.truncate(rnd(text.len() as u64) as usize);
            }
            let mut buf = text.clone();
            buf.resize(text.len() + 16, b'7');
            let s = &buf[..text.len()];
            assert_eq!(fast.ms(s, true), parse_ts(s), "{:?}", std::str::from_utf8(s));
            assert_eq!(fast.ms(s, false), parse_ts(s), "{:?}", std::str::from_utf8(s));
            assert_eq!(fast.secs(s), parse_ts(s).map(|v| v.div_euclid(1000)), "{:?}", std::str::from_utf8(s));
            if fast.secs(s).is_some() && s.len() == 20 && s[19] == b'Z' {
                assert_eq!(fast.ingest_secs(s), fast.secs(s), "{:?}", std::str::from_utf8(s));
            }
        }
        assert_eq!(civil_from_days(days_from_civil(2028, 2, 29)), (2028, 2, 29));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }

    #[test]
    fn prices() {
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price(b"1"), Some(1));
        for bad in ["", "0", "00", "10000001", "19.99", "-5", "+500", "1e3", "２５０", "1000\r"] {
            assert_eq!(parse_price(bad.as_bytes()), None, "{bad}");
        }
    }
}
