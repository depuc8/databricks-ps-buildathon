//! IDs as u64 keys.
//!
//! An ID of 1 to 9 ASCII bytes other than NUL (every ID the generator writes,
//! campaign and product IDs too) packs exactly into 63 bits, 7 bits a byte,
//! first byte highest, so packed keys compare like the strings, byte by
//! byte. Any other ID (longer, or with NUL or non-ASCII bytes) is "odd": its
//! key is the top bit plus a 63-bit hash, and its bytes are found by key
//! where they are needed. IDs that are only ordered, never matched, can
//! instead use an order key (`order_key_w`) that holds their address.

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

/// Set by `init` on CPUs whose PEXT is microcoded and slow (AMD before
/// Zen 3): `pack_wide` then gathers the bits with shifts.
static SLOW_PEXT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Picks the bit gather for this CPU; call once before any packing.
pub fn init() {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::__cpuid;
        // SAFETY: CPUID leaves 0 and 1 exist on every x86-64 CPU (the call
        // is only unsafe on older compilers).
        #[allow(unused_unsafe)]
        let (v, f) = unsafe { (__cpuid(0), __cpuid(1)) };
        let amd = (v.ebx, v.edx, v.ecx) == (0x6874_7541, 0x6974_6e65, 0x444d_4163) // AuthenticAMD
            || (v.ebx, v.edx, v.ecx) == (0x6f67_7948, 0x6e65_476e, 0x656e_6975); // HygonGenuine
        let base = f.eax >> 8 & 0xf;
        let family = if base == 0xf { base + (f.eax >> 20 & 0xff) } else { base };
        // SS_GATHER7 forces the shifts, for testing.
        let slow = amd && family < 0x19 || std::env::var_os("SS_GATHER7").is_some();
        SLOW_PEXT.store(slow, std::sync::atomic::Ordering::Relaxed);
    }
}

/// The low 7 bits of each byte of x, packed together from the bottom:
/// PEXT with mask 0x7f7f_7f7f_7f7f_7f7f, for x whose top bits are clear.
#[inline(always)]
fn gather7(x: u64) -> u64 {
    let x = (x & 0x007f_007f_007f_007f) | (x >> 1 & 0x3f80_3f80_3f80_3f80);
    let x = (x & 0x0000_3fff_0000_3fff) | (x >> 2 & 0x0fff_c000_0fff_c000);
    (x & 0x0fff_ffff) | (x >> 4 & 0x00ff_ffff_f000_0000)
}

/// The inverse of `gather7`: PDEP with mask 0x7f7f_7f7f_7f7f_7f7f.
#[inline(always)]
pub fn scatter7(x: u64) -> u64 {
    let x = (x & 0x0fff_ffff) | (x & 0x00ff_ffff_f000_0000) << 4;
    let x = (x & 0x0000_3fff_0000_3fff) | (x & 0x0fff_c000_0fff_c000) << 2;
    (x & 0x007f_007f_007f_007f) | (x & 0x3f80_3f80_3f80_3f80) << 1
}

/// `pack` with wide reads and PEXT (or `gather7`).
///
/// # Safety
/// 16 bytes from `s.as_ptr()` must be readable.
#[cfg(target_feature = "bmi2")]
#[inline(always)]
unsafe fn pack_wide(s: &[u8]) -> Option<u64> {
    pack_wide_with(s, SLOW_PEXT.load(std::sync::atomic::Ordering::Relaxed))
}

/// `pack_wide`, with `gather7` if `slow`.
///
/// # Safety
/// As for `pack_wide`.
#[cfg(target_feature = "bmi2")]
#[inline(always)]
unsafe fn pack_wide_with(s: &[u8], slow: bool) -> Option<u64> {
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
    let x = _bzhi_u64(raw, bits).swap_bytes();
    let packed = if slow { gather7(x) } else { _pext_u64(x, 0x7f7f_7f7f_7f7f_7f7f) };
    Some(packed << 7 | b)
}

#[cfg(not(target_feature = "bmi2"))]
#[inline(always)]
unsafe fn pack_wide(s: &[u8]) -> Option<u64> {
    pack(s)
}

/// The high and low halves of a full product, folded.
#[inline(always)]
fn fold(a: u64, b: u64) -> u64 {
    let p = a as u128 * b as u128;
    (p as u64) ^ (p >> 64) as u64
}

/// The 63-bit hash of an odd ID: 16-byte blocks (the last one may overlap
/// the one before), each folded into the state with one product. The
/// constants have every byte at least 0x80, so no ASCII word cancels one.
fn hash_odd(s: &[u8]) -> u64 {
    const K1: u64 = 0xa076_1d64_78bd_a42f;
    const K2: u64 = 0xe703_7ed1_a0b4_28db;
    const K3: u64 = 0x8ebc_6af0_9c88_c6e3;
    let n = s.len();
    let w = |i: usize| u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
    let mut h = K3 ^ n as u64;
    if n >= 16 {
        let mut i = 0;
        while i + 16 < n {
            h = fold(w(i) ^ K1, w(i + 8) ^ K2 ^ h);
            i += 16;
        }
        h = fold(w(n - 16) ^ K1, w(n - 8) ^ K2 ^ h);
    } else if n >= 8 {
        h = fold(w(0) ^ K1, w(n - 8) ^ K2 ^ h);
    } else {
        let mut b = [0u8; 8];
        b[..n].copy_from_slice(s);
        h = fold(u64::from_le_bytes(b) ^ K1, K2 ^ h);
    }
    ODD | (fold(h ^ K2, (n as u64) ^ K1) >> 1)
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

/// `key_keep_w`, keeping an odd ID as its `addr_key`.
#[inline(always)]
pub fn key_note_w(s: &'static [u8], wide: bool, odd: &mut Vec<(u64, u64)>) -> u64 {
    match if wide { unsafe { pack_wide(s) } } else { pack(s) } {
        Some(v) => v,
        None => {
            let k = hash_odd(s);
            odd.push((k, addr_key(s)));
            k
        }
    }
}

/// An order key for a click ID, which is only ever compared, never
/// matched: packed if it packs, else ODD, its length (all ones if it does
/// not fit in 16 bits) and the address of its bytes. The bytes end before
/// the next `,`, `"` or newline, since no value holds one, or at the zeros
/// after the end of a file.
#[inline(always)]
pub fn order_key_w(s: &'static [u8], wide: bool) -> u64 {
    match if wide { unsafe { pack_wide(s) } } else { pack(s) } {
        Some(v) => v,
        None => addr_key(s),
    }
}

/// The odd `order_key_w` of an ID that does not pack.
#[inline]
pub fn addr_key(s: &'static [u8]) -> u64 {
    let p = s.as_ptr() as u64;
    debug_assert!(p < 1 << 47);
    ODD | (s.len().min(0xffff) as u64) << 47 | p
}

/// The bytes behind an odd `order_key_w`.
pub fn order_bytes(k: u64) -> &'static [u8] {
    let p = (k & ((1 << 47) - 1)) as *const u8;
    let mut n = (k >> 47 & 0xffff) as usize;
    // SAFETY: p points into a live input mapping, at a value ended by a
    // separator; its length is n unless that was too large to keep.
    unsafe {
        if n == 0xffff {
            while !matches!(*p.add(n), b',' | b'"' | b'\n' | 0) {
                n += 1;
            }
        }
        std::slice::from_raw_parts(p, n)
    }
}

/// Byte-wise order of the IDs behind two `order_key_w` keys.
pub fn order_cmp(a: u64, b: u64) -> std::cmp::Ordering {
    if (a | b) & ODD == 0 {
        return a.cmp(&b);
    }
    let (mut ba, mut bb) = ([0u8; 10], [0u8; 10]);
    let sa = if a & ODD != 0 { order_bytes(a) } else { unpack(a, &mut ba) };
    let sb = if b & ODD != 0 { order_bytes(b) } else { unpack(b, &mut bb) };
    sa.cmp(sb)
}

/// The bytes of a packed key.
pub fn unpack(k: u64, buf: &mut [u8; 10]) -> &[u8] {
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
    unpack(k, buf)
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
            assert_eq!(unsafe { pack_wide_with(s, false) }, pack(s), "{s:?}");
            assert_eq!(unsafe { pack_wide_with(s, true) }, pack(s), "{s:?}");
            if let Some(v) = pack(s) {
                assert_eq!(gather7(scatter7(v >> 7)), v >> 7);
            }
        }
    }

    #[test]
    fn order_keys_compare_like_bytes() {
        // Values as they sit in a line: each followed by a separator.
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut vals: Vec<&'static [u8]> = Vec::new();
        for i in 0..3000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let n = match i % 4 {
                0 => 1 + (seed % 9) as usize,
                1 => 33,
                2 => 1 + (seed % 40) as usize,
                _ => 9 + (seed % 3) as usize,
            };
            let mut v: Vec<u8> = (0..n).map(|j| b"0123456789abcdefk\x7f"[(seed.rotate_left(5 * j as u32) % 18) as usize]).collect();
            if i % 7 == 0 && n > 2 {
                v[n / 2] = 0xc3;
            }
            v.push(if i % 2 == 0 { b',' } else { b'"' });
            v.extend_from_slice(&[0; 16]);
            let leaked: &'static [u8] = Box::leak(v.into_boxed_slice());
            vals.push(&leaked[..n]);
        }
        let big: &'static [u8] = Box::leak([vec![b'a'; 70_000], b",".to_vec(), vec![0; 16]].concat().into_boxed_slice());
        let big2: &'static [u8] = Box::leak([vec![b'a'; 69_999], b"\"".to_vec(), vec![0; 16]].concat().into_boxed_slice());
        vals.push(&big[..70_000]);
        vals.push(&big2[..69_999]);
        let keys: Vec<u64> = vals.iter().enumerate().map(|(i, v)| order_key_w(v, i % 3 != 0)).collect();
        let n = vals.len();
        for i in 0..n {
            for j in (0..n).step_by(7).chain([n - 2, n - 1]) {
                assert_eq!(order_cmp(keys[i], keys[j]), vals[i].cmp(vals[j]), "{:?} {:?}", vals[i], vals[j]);
            }
        }
    }

    #[test]
    fn packing_keeps_order_and_bytes() {
        let words: [&[u8]; 16] = [
            b"0", b"U1", b"u", b"u1", b"u10", b"u1a", b"u2", b"zzzzzzzzz", b"A", b"c_1", b"c_10", b"cZ", b"ca", b" u1",
            b"u1 ", b"A\tB",
        ];
        for a in words {
            let ka = pack(a).unwrap();
            let mut buf = [0u8; 10];
            assert_eq!(unpack(ka, &mut buf), a);
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
