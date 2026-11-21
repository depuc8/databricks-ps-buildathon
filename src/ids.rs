//! IDs as u64 keys.
//!
//! An ID of 1 to 10 ASCII letters and digits (every ID the generator writes)
//! packs exactly into 60 bits, 6 bits a character, left-aligned, with codes in
//! ASCII order, so packed keys compare like the strings, byte by byte. Any
//! other ID (spaces, tabs, `\r`, other punctuation, longer) is "odd": its key
//! is the top bit plus a 63-bit hash, and its bytes are kept in a side table.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use crate::Fx;

pub const ODD: u64 = 1 << 63;

pub type Odd = HashMap<u64, &'static [u8], BuildHasherDefault<Fx>>;

const fn build_codes() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut c = 0;
    let mut code = 1;
    while c < 256 {
        let b = c as u8;
        if b.is_ascii_digit() || b.is_ascii_uppercase() || b.is_ascii_lowercase() {
            t[c] = code;
            code += 1;
        }
        c += 1;
    }
    t
}

static CODES: [u8; 256] = build_codes();

const fn build_chars() -> [u8; 64] {
    let mut t = [0u8; 64];
    let mut c = 0;
    while c < 256 {
        let code = CODES[c];
        if code != 0 {
            t[code as usize] = c as u8;
        }
        c += 1;
    }
    t
}

static CHARS: [u8; 64] = build_chars();

#[inline]
fn pack(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 10 {
        return None;
    }
    let mut v = 0u64;
    let mut shift: u32 = 54;
    for &c in s {
        let k = CODES[c as usize];
        if k == 0 {
            return None;
        }
        v |= (k as u64) << shift;
        shift = shift.wrapping_sub(6);
    }
    Some(v)
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

/// The key of a non-empty ID whose bytes are never needed again.
#[inline]
pub fn key(s: &[u8]) -> u64 {
    match pack(s) {
        Some(v) => v,
        None => hash_odd(s),
    }
}

/// The key of a non-empty ID, keeping an odd ID's bytes in `odd`.
#[inline]
pub fn key_keep(s: &'static [u8], odd: &mut Vec<(u64, &'static [u8])>) -> u64 {
    match pack(s) {
        Some(v) => v,
        None => {
            let k = hash_odd(s);
            odd.push((k, s));
            k
        }
    }
}

pub fn bytes<'a>(k: u64, buf: &'a mut [u8; 10], odd: &Odd) -> &'a [u8] {
    if k & ODD != 0 {
        return odd[&k];
    }
    let mut n = 0;
    while n < 10 {
        let code = (k >> (54 - 6 * n)) & 63;
        if code == 0 {
            break;
        }
        buf[n] = CHARS[code as usize];
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
    fn packing_keeps_order_and_bytes() {
        let odd = Odd::default();
        let words: [&[u8]; 9] = [b"0", b"U1", b"u", b"u1", b"u10", b"u1a", b"u2", b"zzzzzzzzzz", b"A"];
        for a in words {
            let ka = pack(a).unwrap();
            let mut buf = [0u8; 10];
            assert_eq!(bytes(ka, &mut buf, &odd), a);
            for b in words {
                assert_eq!(ka.cmp(&pack(b).unwrap()), a.cmp(b), "{a:?} {b:?}");
            }
        }
        for s in [&b" u1"[..], b"u1 ", b"u1\r", b"A\tB", b"user_id_a", b"abcdefghijk", "٢".as_bytes()] {
            assert!(pack(s).is_none());
            assert_ne!(key(s) & ODD, 0);
        }
    }
}
