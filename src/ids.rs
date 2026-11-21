//! IDs as u64 keys.
//!
//! An ID of 1 to 9 ASCII bytes other than NUL (every ID the generator writes,
//! campaign and product IDs too) packs exactly into 63 bits, 7 bits a byte,
//! first byte highest, so packed keys compare like the strings, byte by
//! byte. Any other ID (longer, or with NUL or non-ASCII bytes) is "odd": its
//! key is the top bit plus a 63-bit hash, and its bytes are kept in a side
//! table.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use crate::Fx;

pub const ODD: u64 = 1 << 63;

pub type Odd = HashMap<u64, &'static [u8], BuildHasherDefault<Fx>>;

#[inline]
fn pack(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 9 {
        return None;
    }
    let mut v = 0u64;
    let mut shift = 56;
    for &c in s {
        if c == 0 || c >= 0x80 {
            return None;
        }
        v |= (c as u64) << shift;
        shift -= 7;
    }
    Some(v)
}

/// `pack` with PEXT.
///
/// # Safety
/// 16 bytes from `s.as_ptr()` must be readable.
#[cfg(target_feature = "bmi2")]
#[inline(always)]
unsafe fn pack_wide(s: &[u8]) -> Option<u64> {
    use std::arch::x86_64::{_bzhi_u64, _pext_u64};
    let n = s.len();
    if n.wrapping_sub(1) > 8 {
        return None;
    }
    let p = s.as_ptr();
    // Little-endian: byte 0 is the first character. BZHI keeps the first n
    // bytes (all 8 when n >= 8).
    let raw = (p as *const u64).read_unaligned();
    let bits = 8 * n as u32;
    // Bit 7 of a byte: a non-ASCII byte, or the first NUL (a borrow can only
    // mark bytes after a real NUL).
    let bad = _bzhi_u64((raw.wrapping_sub(0x0101_0101_0101_0101) & !raw | raw) & 0x8080_8080_8080_8080, bits);
    let b = *p.add(8) as u64 & (n == 9) as u64 * 0xff;
    if bad != 0 || (n == 9 && b.wrapping_sub(1) >= 0x7f) {
        return None;
    }
    Some(_pext_u64(_bzhi_u64(raw, bits).swap_bytes(), 0x7f7f_7f7f_7f7f_7f7f) << 7 | b)
}

#[cfg(not(target_feature = "bmi2"))]
#[inline(always)]
unsafe fn pack_wide(s: &[u8]) -> Option<u64> {
    pack(s)
}

fn hash_odd(s: &[u8]) -> u64 {
    let mut a: u64 = 0x243f_6a88_85a3_08d3 ^ s.len() as u64;
    let mut b: u64 = 0x1319_8a2e_0370_7344;
    for c in s.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        let x = u64::from_le_bytes(w);
        a = crate::fmix64(a ^ x).wrapping_add(b.rotate_left(17));
        b = crate::fmix64(b.wrapping_add(x) ^ 0x9e37_79b9_7f4a_7c15).wrapping_add(a);
    }
    ODD | ((a ^ b.rotate_left(29)) >> 1)
}

/// `key`, given whether 16 bytes from `s.as_ptr()` are readable.
#[inline(always)]
pub fn key_w(s: &[u8], wide: bool) -> u64 {
    // SAFETY: the caller vouches for 16 readable bytes when `wide`.
    match if wide { unsafe { pack_wide(s) } } else { pack(s) } {
        Some(v) => v,
        None => hash_odd(s),
    }
}

/// `key_keep`, given whether 16 bytes from `s.as_ptr()` are readable.
#[inline(always)]
pub fn key_keep_w(s: &'static [u8], wide: bool, odd: &mut Vec<(u64, &'static [u8])>) -> u64 {
    match if wide { unsafe { pack_wide(s) } } else { pack(s) } {
        Some(v) => v,
        None => {
            let k = hash_odd(s);
            odd.push((k, s));
            k
        }
    }
}

/// The key of a non-empty ID whose bytes are never needed again.
#[inline]
pub fn key(s: &[u8]) -> u64 {
    match pack(s) {
        Some(v) => v,
        None => hash_odd(s),
    }
}

pub fn bytes<'a>(k: u64, buf: &'a mut [u8; 10], odd: &Odd) -> &'a [u8] {
    if k & ODD != 0 {
        return odd[&k];
    }
    let mut n = 0;
    while n < 9 {
        let c = (k >> (56 - 7 * n)) & 0x7f;
        if c == 0 {
            break;
        }
        buf[n] = c as u8;
        n += 1;
    }
    &buf[..n]
}

/// Byte-wise order of the IDs behind two keys.
#[inline]
pub fn cmp(a: u64, b: u64, odd: &Odd) -> std::cmp::Ordering {
    if (a | b) & ODD == 0 {
        return a.cmp(&b);
    }
    let (mut ba, mut bb) = ([0u8; 10], [0u8; 10]);
    bytes(a, &mut ba, odd).cmp(bytes(b, &mut bb, odd))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wide_packing_matches() {
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let pool = b"09azAZ_u1kc-. \r\t\"\x00\x7f\x80\xff";
        for _ in 0..300_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let n = (seed % 12) as usize;
            let mut buf = [0u8; 32];
            for (i, b) in buf.iter_mut().enumerate() {
                let r = seed.rotate_left(i as u32 * 5) as usize;
                *b = if r % 4 == 0 { pool[r % pool.len()] } else { b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"[r % 63] };
            }
            let s = &buf[..n];
            assert_eq!(unsafe { pack_wide(s) }, pack(s), "{s:?}");
        }
    }

    #[test]
    fn packing_keeps_order_and_bytes() {
        let odd = Odd::default();
        let words: [&[u8]; 16] = [
            b"0", b"U1", b"u", b"u1", b"u10", b"u1a", b"u2", b"zzzzzzzzz", b"A", b"c_1", b"c_10", b"cZ", b"ca", b" u1",
            b"u1 ", b"A\tB",
        ];
        for a in words {
            let ka = pack(a).unwrap();
            let mut buf = [0u8; 10];
            assert_eq!(bytes(ka, &mut buf, &odd), a);
            for b in words {
                assert_eq!(ka.cmp(&pack(b).unwrap()), a.cmp(b), "{a:?} {b:?}");
            }
        }
        for s in [&b"abcdefghij"[..], b"user_id_ab", b"u1\0", "٢".as_bytes(), "u\u{e9}".as_bytes()] {
            assert!(pack(s).is_none());
            assert_ne!(key(s) & ODD, 0);
        }
    }
}
