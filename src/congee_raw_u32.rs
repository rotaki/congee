use std::{marker::PhantomData, sync::Arc};

use crate::{Allocator, CongeeInner, DefaultAllocator, NodeView, epoch, error::OOMError, stats};

/// A memory-efficient adaptive radix tree specialized for `usize -> u32` maps.
///
/// Unlike [`crate::CongeeRaw`], terminal nodes store values as packed `[u32; N]`
/// arrays (instead of tagged `usize` slots), roughly halving leaf-layer memory
/// usage. Internal nodes remain identical to `CongeeRaw`.
///
/// Values are full 32-bit; there is no payload-tag bit stolen.
///
/// # Examples
///
/// ```
/// use congee::CongeeRawU32;
/// let tree = CongeeRawU32::<usize>::default();
/// let guard = tree.pin();
///
/// tree.insert(1, 42u32, &guard).unwrap();
/// assert_eq!(tree.get(&1, &guard).unwrap(), 42);
/// ```
pub struct CongeeRawU32<
    K: Copy + From<usize>,
    A: Allocator + Clone + Send + 'static = DefaultAllocator,
> where
    usize: From<K>,
{
    inner: CongeeInner<8, A, true>,
    pt_key: PhantomData<K>,
}

impl<K: Copy + From<usize>> Default for CongeeRawU32<K>
where
    usize: From<K>,
{
    fn default() -> Self {
        Self::new(DefaultAllocator {})
    }
}

impl<K: Copy + From<usize>, A: Allocator + Clone + Send> CongeeRawU32<K, A>
where
    usize: From<K>,
{
    /// Create an empty tree.
    pub fn new(allocator: A) -> Self {
        Self::new_with_drainer(allocator, |_k, _v| {})
    }

    /// Create an empty tree with a drainer called on each kv pair at drop time.
    pub fn new_with_drainer(allocator: A, drainer: impl Fn(K, u32) + 'static) -> Self {
        let drainer = Arc::new(move |k: [u8; 8], v: usize| {
            drainer(K::from(usize::from_be_bytes(k)), v as u32)
        });
        CongeeRawU32 {
            inner: CongeeInner::new(allocator, drainer),
            pt_key: PhantomData,
        }
    }

    /// Enters an epoch.
    #[inline]
    pub fn pin(&self) -> epoch::Guard {
        crossbeam_epoch::pin()
    }

    /// Returns `true` iff the tree contains no key-value pairs.
    pub fn is_empty(&self, guard: &epoch::Guard) -> bool {
        self.inner.is_empty(guard)
    }

    /// Returns a copy of the value corresponding to the key.
    #[inline]
    pub fn get(&self, key: &K, guard: &epoch::Guard) -> Option<u32> {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        let v = self.inner.get(&key, guard)?;
        Some(v as u32)
    }

    /// Read-only apply under an optimistic read snapshot.
    #[inline]
    pub fn get_apply<F, R>(&self, key: &K, mut f: F, guard: &epoch::Guard) -> Option<R>
    where
        F: FnMut(u32) -> R,
    {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        self.inner.get_apply(&key, &mut |v| f(v as u32), guard)
    }

    /// Like [`CongeeRawU32::get_apply`], but also exposes sibling payloads in
    /// the same leaf node via [`NodeView`].
    ///
    /// `view.siblings_after()` yields `(u8, usize)` pairs; the `usize` values
    /// are this tree's `u32` payloads widened with `as usize`. Narrow them back
    /// with `v as u32` inside the closure.
    ///
    /// The sibling view is checked against the node version after the closure
    /// returns. Under contention, the closure may be retried.
    #[inline]
    pub fn get_apply_with_siblings<F, R>(
        &self,
        key: &K,
        mut f: F,
        guard: &epoch::Guard,
    ) -> Option<R>
    where
        F: FnMut(u32, &NodeView<'_>) -> R,
    {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        self.inner
            .get_apply_with_siblings(&key, &mut |v, view| f(v as u32, view), guard)
    }

    /// Insert a key-value pair; returns the previous value if present.
    #[inline]
    pub fn insert(&self, k: K, v: u32, guard: &epoch::Guard) -> Result<Option<u32>, OOMError> {
        let key = usize::from(k);
        let key: [u8; 8] = key.to_be_bytes();
        let val = self.inner.insert(&key, v as usize, guard);
        val.map(|inner| inner.map(|v| v as u32))
    }

    /// Removes key-value pair from the tree, returns the value if the key was found.
    #[inline]
    pub fn remove(&self, k: &K, guard: &epoch::Guard) -> Option<u32> {
        let key = usize::from(*k);
        let key: [u8; 8] = key.to_be_bytes();
        let (old, new) = self.inner.compute_if_present(&key, &mut |_v| None, guard)?;
        debug_assert!(new.is_none());
        Some(old as u32)
    }

    /// Compute and update the value if the key is present in the tree.
    #[inline]
    pub fn compute_if_present<F>(
        &self,
        key: &K,
        mut f: F,
        guard: &epoch::Guard,
    ) -> Option<(u32, Option<u32>)>
    where
        F: FnMut(u32) -> Option<u32>,
    {
        let key = usize::from(*key);
        let key: [u8; 8] = key.to_be_bytes();
        let mut g = |v: usize| f(v as u32).map(|x| x as usize);
        self.inner
            .compute_if_present(&key, &mut g, guard)
            .map(|(old, new)| (old as u32, new.map(|v| v as u32)))
    }

    /// Compute or insert the value for `key`.
    ///
    /// The closure is called with `Some(old)` if the key is present and
    /// `None` if it is not. Its return value is stored (replacing or inserting).
    ///
    /// Returns the previous value (`Some(old)`) if the key existed, or `None`
    /// if this call inserted a new entry.
    ///
    /// The closure holds an exclusive lock on the leaf node, so keep it short.
    /// It must also be safe to invoke more than once under contention.
    pub fn compute_or_insert<F>(
        &self,
        key: K,
        mut f: F,
        guard: &epoch::Guard,
    ) -> Result<Option<u32>, OOMError>
    where
        F: FnMut(Option<u32>) -> u32,
    {
        let key = usize::from(key);
        let key: [u8; 8] = key.to_be_bytes();
        let mut g = |v: Option<usize>| f(v.map(|x| x as u32)) as usize;
        let u_val = self.inner.compute_or_insert(&key, &mut g, guard)?;
        Ok(u_val.map(|v| v as u32))
    }

    /// Range scan. Writes up to `result.len()` matches into `result`; returns
    /// the number written.
    #[inline]
    pub fn range(
        &self,
        start: &K,
        end: &K,
        result: &mut [(usize, u32)],
        guard: &epoch::Guard,
    ) -> usize {
        let start = usize::from(*start);
        let end = usize::from(*end);
        let start: [u8; 8] = start.to_be_bytes();
        let end: [u8; 8] = end.to_be_bytes();

        // Reuse the inner range API by materializing into a usize buffer.
        let mut scratch: Vec<([u8; 8], usize)> = vec![([0; 8], 0); result.len()];
        let n = self.inner.range(&start, &end, &mut scratch, guard);
        for i in 0..n {
            let k = usize::from_be_bytes(scratch[i].0);
            result[i] = (k, scratch[i].1 as u32);
        }
        n
    }

    /// Retrieve all keys from the tree. Isolation level: read committed.
    pub fn keys(&self) -> Vec<K> {
        self.inner
            .keys()
            .into_iter()
            .map(|k| {
                let key = usize::from_be_bytes(k);
                K::from(key)
            })
            .collect()
    }

    /// Node statistics.
    pub fn stats(&self) -> stats::NodeStats {
        self.inner.stats()
    }

    /// Returns the allocator used by the tree.
    pub fn allocator(&self) -> &A {
        self.inner.allocator()
    }
}
