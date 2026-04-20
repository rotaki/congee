#[cfg(all(feature = "shuttle", test))]
use shuttle::thread;

#[cfg(not(all(feature = "shuttle", test)))]
use std::thread;

use crate::congee_inner::CongeeInner;
use std::sync::Arc;

#[test]
fn small_insert() {
    let key_cnt = 10_000usize;
    let tree = CongeeInner::default();

    let guard = crossbeam_epoch::pin();
    for k in 0..key_cnt {
        let key: [u8; 8] = k.to_be_bytes();
        tree.insert(&key, k, &guard).unwrap();
        let v = tree.get(&key, &guard).unwrap();
        assert_eq!(v, k);
    }
}

#[test]
fn test_get_keys() {
    let key_cnt = 10_000usize;
    let mut values = vec![];
    let mut values_from_keys = vec![];
    let tree = CongeeInner::default();

    let guard = crossbeam_epoch::pin();
    for k in 0..key_cnt {
        let key: [u8; 8] = k.to_be_bytes();
        tree.insert(&key, k, &guard).unwrap();
        let v = tree.get(&key, &guard).unwrap();
        values.push(v);
    }

    let keys = tree.keys();
    assert_eq!(keys.len(), key_cnt);

    for k in keys.into_iter() {
        let v = tree.get(&k, &guard).unwrap();
        values_from_keys.push(v);
    }

    assert_eq!(values, values_from_keys);
}

#[test]
fn test_sparse_keys() {
    use crate::utils::leak_check::LeakCheckAllocator;
    let key_cnt = 100_000;
    let tree = CongeeInner::new(LeakCheckAllocator::new(), Arc::new(|_k, _v| {}));
    let mut keys = Vec::<usize>::with_capacity(key_cnt);

    let guard = crossbeam_epoch::pin();
    let mut rng = StdRng::seed_from_u64(12);
    for _i in 0..key_cnt {
        let k = rng.r#gen::<usize>() & 0x7fff_ffff_ffff_ffff;
        keys.push(k);

        let key: [u8; 8] = k.to_be_bytes();
        tree.insert(&key, k, &guard).unwrap();
    }

    let delete_cnt = key_cnt / 2;

    for i in keys.iter().take(delete_cnt) {
        let _rt = tree
            .compute_if_present(&i.to_be_bytes(), &mut |_v| None, &guard)
            .unwrap();
    }

    for i in keys.iter().take(delete_cnt) {
        let key: [u8; 8] = i.to_be_bytes();
        let v = tree.get(&key, &guard);
        assert!(v.is_none());
    }

    for i in keys.iter().skip(delete_cnt) {
        let key: [u8; 8] = i.to_be_bytes();
        let v = tree.get(&key, &guard).unwrap();
        assert_eq!(v, *i);
    }

    println!("{}", tree.stats());
}

use rand::prelude::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[test]
fn test_concurrent_insert() {
    let key_cnt_per_thread = 5_000;
    let n_thread = 3;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * n_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }
    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);

    let tree = Arc::new(CongeeInner::default());

    let mut handlers = Vec::new();
    for t in 0..n_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();

        handlers.push(thread::spawn(move || {
            let guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                let key: [u8; 8] = val.to_be_bytes();
                tree.insert(&key, val, &guard).unwrap();
            }
        }));
    }

    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let key: [u8; 8] = v.to_be_bytes();
        let val = tree.get(&key, &guard).unwrap();
        assert_eq!(val, *v);
    }

    assert_eq!(tree.value_count(&guard), key_space.len());
}

#[cfg(all(feature = "shuttle", test))]
#[test]
fn shuttle_insert_only() {
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_thread_names(false)
        .without_time()
        .with_target(false)
        .init();
    let config = shuttle::Config::default();
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(3, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(15, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(15, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(40, 2_000));

    runner.run(test_concurrent_insert);
}

#[test]
fn test_concurrent_insert_read() {
    let key_cnt_per_thread = 5_000;
    let w_thread = 2;
    let mut key_space = Vec::with_capacity(key_cnt_per_thread * w_thread);
    for i in 0..key_space.capacity() {
        key_space.push(i);
    }

    let mut r = StdRng::seed_from_u64(42);
    key_space.shuffle(&mut r);

    let key_space = Arc::new(key_space);

    let tree = Arc::new(CongeeInner::default());

    let mut handlers = Vec::new();

    let r_thread = 2;
    for t in 0..r_thread {
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut r = StdRng::seed_from_u64(10 + t);
            let mut guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                if i % 100 == 0 {
                    guard = crossbeam_epoch::pin();
                }

                let val = r.gen_range(0..(key_cnt_per_thread * w_thread));
                let key: [u8; 8] = val.to_be_bytes();
                if let Some(v) = tree.get(&key, &guard) {
                    assert_eq!(v, val);
                }
            }
        }));
    }

    for t in 0..w_thread {
        let key_space = key_space.clone();
        let tree = tree.clone();
        handlers.push(thread::spawn(move || {
            let mut guard = crossbeam_epoch::pin();
            for i in 0..key_cnt_per_thread {
                if i % 100 == 0 {
                    guard = crossbeam_epoch::pin();
                }

                let idx = t * key_cnt_per_thread + i;
                let val = key_space[idx];
                let key: [u8; 8] = val.to_be_bytes();
                tree.insert(&key, val, &guard).unwrap();
            }
        }));
    }
    for h in handlers.into_iter() {
        h.join().unwrap();
    }

    let guard = crossbeam_epoch::pin();
    for v in key_space.iter() {
        let key: [u8; 8] = v.to_be_bytes();
        let val = tree.get(&key, &guard).unwrap();
        assert_eq!(val, *v);
    }

    assert_eq!(tree.value_count(&guard), key_space.len());

    drop(guard);
    drop(tree);
}

#[cfg(all(feature = "shuttle", test))]
#[test]
fn shuttle_concurrent_insert_read() {
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_thread_names(false)
        .without_time()
        .with_target(false)
        .init();

    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    config.failure_persistence = shuttle::FailurePersistence::File(None);
    let mut runner = shuttle::PortfolioRunner::new(true, config);
    runner.add(shuttle::scheduler::PctScheduler::new(3, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(15, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(15, 2_000));
    runner.add(shuttle::scheduler::PctScheduler::new(40, 2_000));

    runner.run(test_concurrent_insert_read);
}

#[test]
fn test_compute_if_present_with_siblings_update() {
    use crate::CongeeRaw;
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    for i in 0x100..=0x103usize {
        tree.insert(i, i * 10, &guard).unwrap();
    }

    let (old, new) = tree
        .compute_if_present_with_siblings(
            &0x101,
            |v, view| {
                assert_eq!(view.target_byte(), 0x01);
                let siblings: Vec<_> = view.siblings_after().collect();
                assert_eq!(
                    siblings,
                    vec![(0x02, 0x102 * 10), (0x03, 0x103 * 10)],
                );
                Some(v + 1000)
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old, 0x101 * 10);
    assert_eq!(new, Some(0x101 * 10 + 1000));
    assert_eq!(tree.get(&0x101, &guard), Some(0x101 * 10 + 1000));
    assert_eq!(tree.get(&0x100, &guard), Some(0x100 * 10));
    assert_eq!(tree.get(&0x102, &guard), Some(0x102 * 10));
    assert_eq!(tree.get(&0x103, &guard), Some(0x103 * 10));
}

#[test]
fn test_compute_if_present_with_siblings_remove() {
    use crate::CongeeRaw;
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    for i in 0x100..=0x103usize {
        tree.insert(i, i * 10, &guard).unwrap();
    }

    let (old, new) = tree
        .compute_if_present_with_siblings(
            &0x101,
            |v, view| {
                assert_eq!(v, 0x101 * 10);
                let siblings: Vec<_> = view.siblings_after().collect();
                assert_eq!(
                    siblings,
                    vec![(0x02, 0x102 * 10), (0x03, 0x103 * 10)],
                );
                None
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old, 0x101 * 10);
    assert_eq!(new, None);
    assert_eq!(tree.get(&0x101, &guard), None);
    assert_eq!(tree.get(&0x100, &guard), Some(0x100 * 10));
    assert_eq!(tree.get(&0x102, &guard), Some(0x102 * 10));
    assert_eq!(tree.get(&0x103, &guard), Some(0x103 * 10));
}

#[test]
fn test_compute_if_present_with_siblings_nonexistent() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(0x100, 1, &guard).unwrap();

    let calls = AtomicUsize::new(0);
    let out = tree.compute_if_present_with_siblings(
        &0x200,
        |_v, _view| {
            calls.fetch_add(1, Ordering::Relaxed);
            Some(0)
        },
        &guard,
    );
    assert!(out.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn test_get_with_siblings_occupied() {
    use crate::CongeeRaw;
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    for i in 0x100..=0x103usize {
        tree.insert(i, i * 10, &guard).unwrap();
    }

    let got = tree.get_with_siblings(
        &0x101,
        |v, view| (v, view.target_byte(), view.siblings_after().collect::<Vec<_>>()),
        &guard,
    );
    assert_eq!(
        got,
        Some((
            0x101 * 10,
            0x01,
            vec![(0x02, 0x102 * 10), (0x03, 0x103 * 10)],
        )),
    );
}

#[test]
fn test_get_with_siblings_absent() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(0x100, 1, &guard).unwrap();

    let calls = AtomicUsize::new(0);
    let got = tree.get_with_siblings(
        &0x200,
        |_v, _view| {
            calls.fetch_add(1, Ordering::Relaxed);
            0usize
        },
        &guard,
    );
    assert!(got.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn test_siblings_bucket_boundary() {
    use crate::CongeeRaw;
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(0x1FDusize, 1001, &guard).unwrap();
    tree.insert(0x1FEusize, 1002, &guard).unwrap();

    // Target = 0xFD byte slot, only 0xFE is a sibling after.
    let got = tree
        .get_with_siblings(
            &0x1FDusize,
            |v, view| (v, view.siblings_after().collect::<Vec<_>>()),
            &guard,
        )
        .unwrap();
    assert_eq!(got, (1001, vec![(0xFE, 1002)]));

    // Target = 0xFE byte slot, nothing after 0xFE up to 0xFF.
    let got = tree
        .get_with_siblings(
            &0x1FEusize,
            |v, view| (v, view.siblings_after().collect::<Vec<_>>()),
            &guard,
        )
        .unwrap();
    assert_eq!(got, (1002, vec![]));
}

#[test]
fn test_siblings_target_byte_at_0xff() {
    use crate::CongeeRaw;
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(0x1FFusize, 42, &guard).unwrap();

    let got = tree
        .get_with_siblings(
            &0x1FFusize,
            |v, view| {
                assert_eq!(view.target_byte(), 0xFF);
                let siblings: Vec<_> = view.siblings_after().collect();
                (v, siblings)
            },
            &guard,
        )
        .unwrap();
    assert_eq!(got, (42, vec![]));
}

#[test]
fn test_get_with_siblings_locked_occupied() {
    use crate::CongeeRaw;
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    for i in 0x100..=0x103usize {
        tree.insert(i, i * 10, &guard).unwrap();
    }

    let got = tree.get_with_siblings_locked(
        &0x101,
        |v, view| (v, view.target_byte(), view.siblings_after().collect::<Vec<_>>()),
        &guard,
    );
    assert_eq!(
        got,
        Some((
            0x101 * 10,
            0x01,
            vec![(0x02, 0x102 * 10), (0x03, 0x103 * 10)],
        )),
    );
}

#[test]
fn test_get_with_siblings_locked_absent() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(0x100, 1, &guard).unwrap();

    let calls = AtomicUsize::new(0);
    let got = tree.get_with_siblings_locked(
        &0x200,
        |_v, _view| {
            calls.fetch_add(1, Ordering::Relaxed);
            0usize
        },
        &guard,
    );
    assert!(got.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn test_get_with_siblings_locked_exactly_once() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(42usize, 777, &guard).unwrap();

    let calls = AtomicUsize::new(0);
    let got = tree.get_with_siblings_locked(
        &42usize,
        |v, _view| {
            calls.fetch_add(1, Ordering::Relaxed);
            v
        },
        &guard,
    );
    assert_eq!(got, Some(777));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn test_compute_if_present_locked_update() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    for i in 0x100..=0x103usize {
        tree.insert(i, i * 10, &guard).unwrap();
    }

    let calls = AtomicUsize::new(0);
    let (old, new) = tree
        .compute_if_present_locked_with_siblings(
            &0x101,
            |v, view| {
                calls.fetch_add(1, Ordering::Relaxed);
                let siblings: Vec<_> = view.siblings_after().collect();
                assert_eq!(
                    siblings,
                    vec![(0x02, 0x102 * 10), (0x03, 0x103 * 10)],
                );
                Some(v + 1000)
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old, 0x101 * 10);
    assert_eq!(new, Some(0x101 * 10 + 1000));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(tree.get(&0x101, &guard), Some(0x101 * 10 + 1000));
    assert_eq!(tree.get(&0x100, &guard), Some(0x100 * 10));
}

#[test]
fn test_compute_if_present_locked_remove_not_last() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    for i in 0x100..=0x103usize {
        tree.insert(i, i * 10, &guard).unwrap();
    }

    let calls = AtomicUsize::new(0);
    let (old, new) = tree
        .compute_if_present_locked_with_siblings(
            &0x101,
            |_v, _view| {
                calls.fetch_add(1, Ordering::Relaxed);
                None
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old, 0x101 * 10);
    assert_eq!(new, None);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(tree.get(&0x101, &guard), None);
    assert_eq!(tree.get(&0x100, &guard), Some(0x100 * 10));
    assert_eq!(tree.get(&0x102, &guard), Some(0x102 * 10));
    assert_eq!(tree.get(&0x103, &guard), Some(0x103 * 10));
}

#[test]
fn test_compute_if_present_locked_remove_last() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    // Single key in its own bucket so its leaf has value_count == 1.
    tree.insert(0x100usize, 777, &guard).unwrap();

    let calls = AtomicUsize::new(0);
    let (old, new) = tree
        .compute_if_present_locked_with_siblings(
            &0x100usize,
            |v, _view| {
                calls.fetch_add(1, Ordering::Relaxed);
                assert_eq!(v, 777);
                None
            },
            &guard,
        )
        .unwrap();
    assert_eq!(old, 777);
    assert_eq!(new, None);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(tree.get(&0x100usize, &guard), None);
    assert!(tree.is_empty(&guard));
}

#[test]
fn test_compute_if_present_locked_nonexistent() {
    use crate::CongeeRaw;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let tree: CongeeRaw<usize, usize> = CongeeRaw::default();
    let guard = tree.pin();
    tree.insert(0x100, 1, &guard).unwrap();

    let calls = AtomicUsize::new(0);
    let out = tree.compute_if_present_locked_with_siblings(
        &0x200,
        |_v, _view| {
            calls.fetch_add(1, Ordering::Relaxed);
            Some(0)
        },
        &guard,
    );
    assert!(out.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn test_get_with_siblings_locked_mutual_exclusion() {
    use crate::CongeeRaw;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    let tree: Arc<CongeeRaw<usize, usize>> = Arc::new(CongeeRaw::default());
    {
        let guard = tree.pin();
        tree.insert(42usize, 1, &guard).unwrap();
    }

    // Shared log of (thread_id, phase, timestamp_ns) entries.
    type LogEntry = (u32, &'static str, std::time::Instant);
    let log: Arc<Mutex<Vec<LogEntry>>> = Arc::new(Mutex::new(Vec::new()));

    let mut handles = vec![];
    for i in 0..2u32 {
        let tree = tree.clone();
        let log = log.clone();
        handles.push(thread::spawn(move || {
            let guard = tree.pin();
            tree.get_with_siblings_locked(
                &42usize,
                |_v, _view| {
                    {
                        let mut l = log.lock().unwrap();
                        l.push((i, "enter", std::time::Instant::now()));
                    }
                    thread::sleep(Duration::from_millis(50));
                    {
                        let mut l = log.lock().unwrap();
                        l.push((i, "exit", std::time::Instant::now()));
                    }
                },
                &guard,
            );
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let l = log.lock().unwrap();
    assert_eq!(l.len(), 4, "each thread logs enter+exit: {l:?}");
    // Events must interleave as A-enter, A-exit, B-enter, B-exit (or reversed).
    // The two closure windows must not overlap.
    let first = l[0].0;
    assert_eq!(l[0].1, "enter");
    assert_eq!(l[1].0, first, "same thread's exit must follow its enter");
    assert_eq!(l[1].1, "exit");
    let second = l[2].0;
    assert_ne!(first, second, "other thread's window starts after first ends");
    assert_eq!(l[2].1, "enter");
    assert_eq!(l[3].0, second);
    assert_eq!(l[3].1, "exit");
}

#[cfg(all(feature = "shuttle", test))]
#[test]
fn shuttle_replay() {
    tracing_subscriber::fmt()
        .with_ansi(true)
        .with_thread_names(false)
        .without_time()
        .with_target(false)
        .init();

    shuttle::check_random_with_seed(test_concurrent_insert_read, 324037473359401122, 1000);
}
