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

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
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
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s);
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

/// F1 `YYYY-MM-DDTHH:MM:SSZ` or F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`.
#[inline]
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
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut t = ((days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60 + second;
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
    Some(t * 1000)
}

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
