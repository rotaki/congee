use crate::{CongeeRaw, CongeeRawU32};

/// Newtype that widens a `u32` payload through `CongeeRaw<K, V>`'s
/// `usize: From<V>` / `V: From<usize>` bounds.
#[derive(Copy, Clone)]
struct U32Payload(u32);

impl From<usize> for U32Payload {
    fn from(v: usize) -> Self {
        U32Payload(v as u32)
    }
}

impl From<U32Payload> for usize {
    fn from(v: U32Payload) -> usize {
        v.0 as usize
    }
}

#[test]
fn basic_roundtrip() {
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    for i in 0..1000usize {
        assert!(tree.insert(i, i as u32 * 3, &guard).unwrap().is_none());
    }
    for i in 0..1000usize {
        assert_eq!(tree.get(&i, &guard), Some(i as u32 * 3));
    }

    // Update in place.
    for i in 0..1000usize {
        let old = tree.insert(i, 7, &guard).unwrap();
        assert_eq!(old, Some(i as u32 * 3));
    }
    for i in 0..1000usize {
        assert_eq!(tree.get(&i, &guard), Some(7));
    }
}

#[test]
fn full_u32_values_roundtrip() {
    // Values use the full 32-bit range (not just 2^31).
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    let values: &[u32] = &[0, 1, u32::MAX, u32::MAX - 1, 1 << 31, (1 << 31) - 1];
    for (k, v) in values.iter().enumerate() {
        tree.insert(k, *v, &guard).unwrap();
    }
    for (k, v) in values.iter().enumerate() {
        assert_eq!(tree.get(&k, &guard), Some(*v));
    }
}

#[test]
fn remove_and_reinsert() {
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    for i in 0..200usize {
        tree.insert(i, i as u32, &guard).unwrap();
    }
    for i in 0..200usize {
        assert_eq!(tree.remove(&i, &guard), Some(i as u32));
        assert!(tree.get(&i, &guard).is_none());
    }
    // All gone.
    assert!(tree.is_empty(&guard));

    // Reinsert works.
    for i in 0..200usize {
        tree.insert(i, i as u32 + 1000, &guard).unwrap();
    }
    for i in 0..200usize {
        assert_eq!(tree.get(&i, &guard), Some(i as u32 + 1000));
    }
}

#[test]
fn compute_if_present_updates() {
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    tree.insert(42, 100, &guard).unwrap();

    let (old, new) = tree
        .compute_if_present(&42, |v| Some(v * 2), &guard)
        .unwrap();
    assert_eq!(old, 100);
    assert_eq!(new, Some(200));
    assert_eq!(tree.get(&42, &guard), Some(200));

    // Absent key returns None.
    assert!(tree.compute_if_present(&99, |_| Some(0), &guard).is_none());
}

#[test]
fn get_apply_returns_value() {
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    tree.insert(7, 777, &guard).unwrap();
    let doubled = tree.get_apply(&7, |v| v * 2, &guard).unwrap();
    assert_eq!(doubled, 1554);
}

#[test]
fn forces_each_leaf_size_class() {
    // Keys share a 7-byte prefix, differing only at the terminal byte — this
    // forces upgrades through Node4Leaf → Node16Leaf → Node48Leaf → Node256Leaf.
    fn tree_with_n_terminal_keys(n: usize) -> CongeeRawU32<usize> {
        let tree = CongeeRawU32::<usize>::default();
        let guard = tree.pin();
        // Pack bytes 0..6 as zero, then distinct final byte.
        for i in 0..n {
            let key = i; // high bytes are zero, final byte varies up to 255
            tree.insert(key, i as u32 + 1, &guard).unwrap();
        }
        tree
    }

    // Small — stays in Node4Leaf.
    let t = tree_with_n_terminal_keys(4);
    let (n4, n16, n48, n256) = t.stats().leaf_node_counts();
    assert_eq!((n4, n16, n48, n256), (1, 0, 0, 0));

    // 10 — fits in Node16Leaf.
    let t = tree_with_n_terminal_keys(10);
    let (_n4, n16, _n48, _n256) = t.stats().leaf_node_counts();
    assert!(n16 >= 1);

    // 32 — fits in Node48Leaf.
    let t = tree_with_n_terminal_keys(32);
    let (_, _, n48, _) = t.stats().leaf_node_counts();
    assert!(n48 >= 1);

    // 256 — fills Node256Leaf.
    let t = tree_with_n_terminal_keys(256);
    let (_, _, _, n256) = t.stats().leaf_node_counts();
    assert_eq!(n256, 1);
}

#[test]
fn node48_leaf_freelist_reuses_after_delete() {
    // Fill a Node48Leaf (up to 48 terminal-distinct keys), delete many, reinsert,
    // verify correct state.
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    for i in 0..48usize {
        tree.insert(i, i as u32 + 1, &guard).unwrap();
    }
    // Confirm we have a Node48Leaf.
    let (_, _, n48, _) = tree.stats().leaf_node_counts();
    assert!(n48 >= 1);

    // Remove alternating.
    for i in (0..48).step_by(2) {
        assert!(tree.remove(&i, &guard).is_some());
    }
    for i in (0..48).step_by(2) {
        assert!(tree.get(&i, &guard).is_none());
    }
    for i in (1..48).step_by(2) {
        assert_eq!(tree.get(&i, &guard), Some(i as u32 + 1));
    }

    // Reinsert with different values — should exercise the bitmap freelist.
    for i in (0..48).step_by(2) {
        tree.insert(i, i as u32 + 1000, &guard).unwrap();
    }
    for i in 0..48 {
        let expected = if i % 2 == 0 {
            i as u32 + 1000
        } else {
            i as u32 + 1
        };
        assert_eq!(tree.get(&i, &guard), Some(expected));
    }
}

#[test]
fn range_scan_matches_oracle() {
    use std::collections::BTreeMap;

    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();
    let mut oracle = BTreeMap::new();

    for i in 0..500usize {
        let k = i * 3;
        let v = i as u32 + 1;
        tree.insert(k, v, &guard).unwrap();
        oracle.insert(k, v);
    }

    let mut buf = vec![(0usize, 0u32); 100];
    let start = 50;
    let end = 500;
    let n = tree.range(&start, &end, &mut buf, &guard);

    let oracle_range: Vec<(usize, u32)> = oracle
        .range(start..=end)
        .take(100)
        .map(|(k, v)| (*k, *v))
        .collect();

    assert_eq!(n, oracle_range.len());
    assert_eq!(&buf[..n], &oracle_range[..]);
}

#[test]
fn memory_vs_congee_raw_usize() {
    // Same workload on CongeeRaw<usize, u32> vs CongeeRawU32<usize>.
    // The leaf-specialized tree should allocate less at the leaf layer,
    // inspected through NodeStats::total_memory_bytes().
    let workload: Vec<usize> = (0..2000).collect();

    let tree_a: CongeeRaw<usize, U32Payload> = CongeeRaw::default();
    let guard = tree_a.pin();
    for &k in &workload {
        tree_a.insert(k, U32Payload(k as u32), &guard).unwrap();
    }
    let mem_a = tree_a.stats().total_memory_bytes();

    let tree_b: CongeeRawU32<usize> = CongeeRawU32::default();
    let guard = tree_b.pin();
    for &k in &workload {
        tree_b.insert(k, k as u32, &guard).unwrap();
    }
    let mem_b = tree_b.stats().total_memory_bytes();

    println!(
        "CongeeRaw<usize, u32>: {mem_a} bytes, CongeeRawU32<usize>: {mem_b} bytes \
         (saved {} bytes)",
        mem_a.saturating_sub(mem_b)
    );
    assert!(
        mem_b < mem_a,
        "expected leaf-specialized tree to use less memory: {mem_a} (raw) vs {mem_b} (u32)"
    );
}

#[test]
#[ignore = "perf comparison, run manually with --release --nocapture"]
fn perf_vs_usize_usize() {
    use std::time::Instant;

    const N: usize = 1_000_000;

    // Use a pseudo-random permutation so inserts/lookups aren't purely sequential.
    let mut keys: Vec<usize> = (0..N).collect();
    let mut seed = 0xdeadbeef_cafebabe_u64;
    for i in (1..N).rev() {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (seed as usize) % (i + 1);
        keys.swap(i, j);
    }

    // usize -> usize (existing CongeeRaw)
    let tree_a: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree_a.pin();
    let t = Instant::now();
    for &k in &keys {
        tree_a.insert(k, k, &guard).unwrap();
    }
    let insert_a = t.elapsed();

    // Warmup read pass (not timed).
    let mut acc_a = 0usize;
    for &k in &keys {
        acc_a = acc_a.wrapping_add(tree_a.get(&k, &guard).unwrap());
    }
    // Best-of-three timed read passes.
    let mut get_a = std::time::Duration::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        for &k in &keys {
            acc_a = acc_a.wrapping_add(tree_a.get(&k, &guard).unwrap());
        }
        let el = t.elapsed();
        if el < get_a {
            get_a = el;
        }
    }

    let mem_a = tree_a.stats().total_memory_bytes();

    // usize -> u32 (CongeeRawU32)
    let tree_b: CongeeRawU32<usize> = CongeeRawU32::default();
    let guard = tree_b.pin();
    let t = Instant::now();
    for &k in &keys {
        tree_b.insert(k, k as u32, &guard).unwrap();
    }
    let insert_b = t.elapsed();

    let mut acc_b = 0u32;
    for &k in &keys {
        acc_b = acc_b.wrapping_add(tree_b.get(&k, &guard).unwrap());
    }
    let mut get_b = std::time::Duration::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        for &k in &keys {
            acc_b = acc_b.wrapping_add(tree_b.get(&k, &guard).unwrap());
        }
        let el = t.elapsed();
        if el < get_b {
            get_b = el;
        }
    }

    let mem_b = tree_b.stats().total_memory_bytes();

    println!("\n=== Perf comparison: N = {N} random keys ===");
    println!(
        "CongeeRaw<usize, usize>: insert {:>8.2} ns/op, get {:>8.2} ns/op, mem {:>10} B",
        insert_a.as_nanos() as f64 / N as f64,
        get_a.as_nanos() as f64 / N as f64,
        mem_a
    );
    println!(
        "CongeeRawU32<usize>    : insert {:>8.2} ns/op, get {:>8.2} ns/op, mem {:>10} B",
        insert_b.as_nanos() as f64 / N as f64,
        get_b.as_nanos() as f64 / N as f64,
        mem_b
    );
    println!(
        "  get speedup: {:.2}x, memory ratio: {:.2}x",
        get_a.as_nanos() as f64 / get_b.as_nanos() as f64,
        mem_a as f64 / mem_b as f64
    );
    // Side-effect to prevent dead-code elimination of the accumulators.
    assert!(acc_a != 0 || acc_b != 0);
}

#[test]
fn many_keys_deep_and_wide() {
    // Large-ish random workload exercising all node sizes at both layers.
    let tree = CongeeRawU32::<usize>::default();
    let guard = tree.pin();

    let mut seed = 0x12345u64;
    let mut keys = Vec::new();
    for _ in 0..5000 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let k = (seed as usize) & 0x00FF_FFFF_FFFF_FFFF;
        if keys.contains(&k) {
            continue;
        }
        keys.push(k);
        tree.insert(k, (k as u32) ^ 0xdead_beef, &guard).unwrap();
    }

    for &k in &keys {
        assert_eq!(tree.get(&k, &guard), Some((k as u32) ^ 0xdead_beef));
    }
}
