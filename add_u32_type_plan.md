# Plan: Leaf-node specialization for `usize → u32` in CongeeRaw

## Context

Congee's `NodePtr` currently occupies a full `usize` in every child slot of
every node, with the high bit distinguishing "payload" from "pointer to
subnode" ([src/nodes/node_ptr.rs:87-132](src/nodes/node_ptr.rs#L87-L132)).
For a `usize → u32` map this wastes 4 bytes per slot at the leaf layer —
the layer that dominates tree footprint.

Because keys are fixed 8 bytes ([src/congee_raw.rs:49](src/congee_raw.rs#L49)),
no key is a prefix of another, so a node's children are either *all*
subnode pointers or *all* payloads — never mixed. The leaf-layer nodes
can therefore be specialized with `[u32; N]` storage with no change to the
invariants of the tree.

**Scope of this first cut (narrowed from an earlier draft):**

1. Add four new leaf node types: `Node4Leaf`, `Node16Leaf`, `Node48Leaf`,
   `Node256Leaf` — storing `u32` values inline.
2. Keep **internal nodes untouched**: same `NodePtr` slots, same tag bit,
   same `cast_ptr!` descent. The small runtime cost of the tag-bit mask
   is much cheaper than forking `insert`/`delete`/`range_scan` logic.
3. Add `debug_assert!`s that internal-node slots never hold payloads and
   leaf-node slots never hold subnode pointers — turning the invariant
   into a checked property.
4. Add a const-generic flag on `CongeeInner` to opt into leaf
   specialization, and expose it via a new public type
   `CongeeRawU32<K>`. Existing `CongeeRaw<K, V>` is untouched.
5. **Out of scope** (can be done in a follow-up): dropping the tag bit
   entirely, compact serialization of the new leaves, widening to other
   value sizes (u16, u64).

### Expected size reduction

| Node class    | Current | New leaf variant | Saved / leaf |
|---|---|---|---|
| N4 leaf   | 56 B   | ~40 B   | 16 B |
| N16 leaf  | 160 B  | ~96 B   | 64 B |
| N48 leaf  | 664 B  | ~472 B  | 192 B |
| N256 leaf | 2096 B | ~1072 B | 1024 B |

Savings come entirely from leaf nodes. Internal-node layout is unchanged.

## Design

### New node types (`src/nodes/node_*_leaf.rs`)

All four leaf types implement the existing `Node` trait (same signatures
as today's Node4/16/48/256) — see [src/nodes/base_node.rs:70-80](src/nodes/base_node.rs#L70-L80).
Internally they store `u32` values. At the trait boundary they widen
u32 → `NodePtr::from_payload(v as usize)` on read, and narrow
NodePtr → u32 on write (with a `debug_assert!(ptr.is_payload())`).

```rust
// src/nodes/node_4_leaf.rs
#[repr(C, align(8))]
pub(crate) struct Node4Leaf {
    base: BaseNode,
    keys: [u8; 4],
    values: [u32; 4],
}
// size: 16 + 4 + 16 + 4 (padding) = 40 bytes

// src/nodes/node_16_leaf.rs
#[repr(C, align(8))]
pub(crate) struct Node16Leaf {
    base: BaseNode,
    keys: [u8; 16],
    values: [u32; 16],
}
// size: 16 + 16 + 64 = 96 bytes

// src/nodes/node_256_leaf.rs
#[repr(C, align(8))]
pub(crate) struct Node256Leaf {
    base: BaseNode,
    key_mask: [u8; 32],   // present-bit bitmap (same layout as Node256)
    values: [u32; 256],
}
// size: 16 + 32 + 1024 = 1072 bytes
```

### Node48Leaf: replace the NodePtr-encoded freelist with a bitmap

Today's Node48 at [src/nodes/node_48.rs:32-34](src/nodes/node_48.rs#L32-L34)
initializes empty child slots with `NodePtr::from_payload(i + 1)` and
chains them via `next_empty`. That trick relies on `NodePtr` being a
tagged-usize — it cannot work when slots are raw `u32`.

Replace the chained freelist with a 64-bit occupancy bitmap:

```rust
// src/nodes/node_48_leaf.rs
#[repr(C, align(8))]
pub(crate) struct Node48Leaf {
    base: BaseNode,
    child_idx: [u8; 256],   // key byte -> slot index, or EMPTY_MARKER
    occupied: u64,          // bit i = 1 iff slot i in `values` is in use
    values: [u32; 48],
}
// size: 16 + 256 + 8 + 192 = 472 bytes
```

Finding a free slot: `(!self.occupied).trailing_zeros()` — O(1). On
`insert`: set the bit and update `child_idx`. On `remove`: clear the
bit and write `EMPTY_MARKER` to `child_idx` (the stale `values[i]` is
harmless because `child_idx` is authoritative).

### NodeType enum

[src/nodes/base_node.rs:36-41](src/nodes/base_node.rs#L36-L41) gains four
variants:

```rust
pub(crate) enum NodeType {
    N4, N16, N48, N256,
    N4Leaf, N16Leaf, N48Leaf, N256Leaf,
}
```

Helpers: `is_leaf_family(&self) -> bool` on NodeType, plus `as_n4_leaf()`,
`as_n16_leaf()`, etc. on BaseNode (parallel to the existing
`as_n4`/`as_n16` downcasts).

### BaseNode dispatch macros

The `gen_method!` / `gen_method_mut!` macros at
[src/nodes/base_node.rs:141-193](src/nodes/base_node.rs#L141-L193) are
extended with four new match arms (N4Leaf → as_n4_leaf, etc.). Methods
on the `Node` trait then dispatch uniformly across eight types.

### Node creation — which three sites create leaves

Reviewing the three `make_node::<Node4, A>` sites in
[src/congee_inner.rs](src/congee_inner.rs):

| Site | Line | Purpose | Leaf?        |
|---|---|---|---|
| "missing-child helper"  | 425 | Holds the key's terminal byte as its single child. Always. | **Always leaf** |
| "single-new-node"       | 511 | Same role in the prefix-mismatch case. | **Always leaf** |
| "new-middle-node"       | 498 | Splits at `next_level`. Leaf iff `next_level == K_LEN - 1`. | Check existing branch at line 504 |

So the leaf-vs-internal decision is **per-site**, not a generic predicate —
my earlier attempt at `level + prefix_len + 1 == K_LEN` was wrong. Concrete
edits:

```rust
// congee_inner.rs:425 — ALWAYS a leaf
let mut n4 = BaseNode::make_node::<Node4Leaf, A>(remaining_prefix, &self.allocator)?;
n4.as_mut().insert(k[k.len() - 1], NodePtr::from_payload(tid_func(None)));

// congee_inner.rs:511 — ALWAYS a leaf
let mut single_new_node = BaseNode::make_node::<Node4Leaf, A>(
    &k[(next_level + 1)..k.len() - 1], &self.allocator,
)?;

// congee_inner.rs:498 — pick based on the existing check at line 504
let mut new_middle_node = if next_level == K_LEN - 1 {
    BaseNode::make_node::<Node4Leaf, A>(prefix_slice, &self.allocator)?
} else {
    BaseNode::make_node::<Node4, A>(prefix_slice, &self.allocator)?
};
```

Root creation in `CongeeInner::new()` stays as `Node4` (an empty internal
node). The first insert creates leaves below it through the sites above.

### Activation mechanism: `CongeeInner` const-generic flag + new public type

`CongeeInner<const K_LEN: usize, A>` at
[src/congee_inner.rs:20-28](src/congee_inner.rs#L20-L28) has no value-type
parameter — `V` lives only in `CongeeRaw`. Node-type dispatch alone
can't know whether to create `Node4` or `Node4Leaf` at the three creation
sites. Concrete mechanism:

```rust
// src/congee_inner.rs
pub(crate) struct CongeeInner<
    const K_LEN: usize,
    A: Allocator + Clone + Send + 'static = DefaultAllocator,
    const LEAF_IS_U32: bool = false,
> { /* same fields */ }
```

Every `impl` block for `CongeeInner` gains the `const LEAF_IS_U32: bool`
parameter. Existing `CongeeRaw<K, V>` continues to use the default
(`CongeeInner<8, A>` desugars to `CongeeInner<8, A, false>`), so
behavior and layout are unchanged.

At the three creation sites, branch on the const:

```rust
// congee_inner.rs:425 (always-leaf site) — gated by the const
let mut n4 = if LEAF_IS_U32 {
    BaseNode::make_node::<Node4Leaf, A>(remaining_prefix, &self.allocator)?
} else {
    BaseNode::make_node::<Node4, A>(remaining_prefix, &self.allocator)?
};
```

Because `LEAF_IS_U32` is a const generic, the branch monomorphizes away
per instantiation — zero runtime cost.

**New public type** `src/congee_raw_u32.rs`:

```rust
pub struct CongeeRawU32<
    K: Copy + From<usize>,
    A: Allocator + Clone + Send + 'static = DefaultAllocator,
> where usize: From<K>,
{
    inner: CongeeInner<8, A, true>,
    pt_key: PhantomData<K>,
}
```

Surfaces the same methods as `CongeeRaw<K, u32>` — `get`, `insert`,
`remove`, `compute_if_present`, `get_apply`, `range` — all of which
pass through to `CongeeInner`. Values are stored as `u32`, internally
widened to `usize` at the `NodePtr` boundary but narrowed back on read.

`src/lib.rs` adds `pub use congee_raw_u32::CongeeRawU32;`.

### Upgrade chains

[src/nodes/base_node.rs:402-423](src/nodes/base_node.rs#L402-L423) currently
dispatches `insert_grow::<CurT, BiggerT>` based on `node_type`. Extend with
four new arms for the leaf family:

```rust
NodeType::N4Leaf   => insert_grow::<Node4Leaf,   Node16Leaf,  A>(...),
NodeType::N16Leaf  => insert_grow::<Node16Leaf,  Node48Leaf,  A>(...),
NodeType::N48Leaf  => insert_grow::<Node48Leaf,  Node256Leaf, A>(...),
NodeType::N256Leaf => insert_grow::<Node256Leaf, Node256Leaf, A>(...),
```

The existing `insert_grow` generic is already parameterized over
`Node` impls, so no changes to the function itself. Leaves upgrade only
to leaves; internal nodes upgrade only to internal — guaranteed by this
dispatch.

### NodeIter extension

`Node::get_children()` returns the `NodeIter<'_>` enum at
[src/nodes/base_node.rs:82-99](src/nodes/base_node.rs#L82-L99), which
currently has four variants (`N4`, `N16`, `N48`, `N256`). It gains four
more:

```rust
pub(crate) enum NodeIter<'a> {
    N4(Node4Iter<'a>),        N16(Node16Iter<'a>),
    N48(Node48Iter<'a>),      N256(Node256Iter<'a>),
    N4Leaf(Node4LeafIter<'a>), N16Leaf(Node16LeafIter<'a>),
    N48Leaf(Node48LeafIter<'a>), N256Leaf(Node256LeafIter<'a>),
}
```

Each new iterator is structurally identical to its internal counterpart
(Node4Iter yields `(u8, NodePtr)` pairs by reading `children[i]`; the
leaf counterpart yields `(u8, NodePtr::from_payload(values[i] as usize))`).
`Iterator::next` on `NodeIter` extends with the four new arms.

### Concurrency

Confirmed by [src/nodes/base_node.rs:102-108](src/nodes/base_node.rs#L102-L108):
the per-node version lock (`AtomicU32`) serializes writers; child arrays
are plain `[T; N]` — not per-slot atomics. Readers revalidate via the
version counter at `check_version()` boundaries. Leaf variants use the
same pattern with `[u32; N]` — the slot width is irrelevant to the lock
protocol. No changes to [src/lock.rs](src/lock.rs).

### Debug invariants

Add `debug_assert!`s at the trait boundaries to catch family confusion:

- In every internal-node `insert` / `change`: assert incoming `NodePtr`
  is a SubNode (`!ptr.is_payload()`).
- In every leaf-node `insert` / `change`: assert incoming `NodePtr` is a
  Payload (`ptr.is_payload()`).
- In `copy_to<N: Node>`: the caller (`insert_grow`) guarantees the
  destination is same-family; add a `debug_assert_eq!(N::get_type().is_leaf_family(), Self::get_type().is_leaf_family())`.

These are free in release builds and make any future creation-site bug
fail loudly in tests.

### `to_compact_set` (CongeeSet path)

[src/congee_inner.rs:751-784](src/congee_inner.rs) classifies a node as
leaf by checking if any child returned `Payload` via `cast_ptr!`. After
this change, leaf-family nodes' `get_children()` still yields
`NodePtr::from_payload(...)` (widening happens at the trait boundary),
so this detection keeps working. No change required; extend the
`match (get_type(), is_leaf)` arms at line 775-784 to accept the four new
NodeType variants on the `is_leaf == true` side.

## Files to modify

**New files:**
- `src/nodes/node_4_leaf.rs`
- `src/nodes/node_16_leaf.rs`
- `src/nodes/node_48_leaf.rs`
- `src/nodes/node_256_leaf.rs`
- `src/congee_raw_u32.rs` — public wrapper type.

**Modified:**
- `src/nodes/mod.rs` — add `mod node_*_leaf;` and re-exports.
- `src/nodes/base_node.rs`:
  - `NodeType` enum: four new variants + `is_leaf_family()` helper.
  - `node_layout()`: size/align for the four new types.
  - `NodeIter` enum: four new variants + iterator dispatch arms.
  - `gen_method!` / `gen_method_mut!` macros: four new arms.
  - `make_node()`: ensure `NodeType` is recorded correctly for leaves.
  - `as_n4_leaf()` / `as_n16_leaf()` / `as_n48_leaf()` / `as_n256_leaf()` downcasts.
  - `insert_grow` dispatch at line 402-423: four new arms for the leaf chain.
- `src/congee_inner.rs`:
  - Struct definition: add `const LEAF_IS_U32: bool = false` generic param.
  - Every `impl` block for `CongeeInner`: plumb the const generic.
  - Line 425: const-gated `Node4` vs `Node4Leaf`.
  - Line 498: const-gated; if `LEAF_IS_U32` and `next_level == K_LEN - 1`, use `Node4Leaf`.
  - Line 511: const-gated `Node4` vs `Node4Leaf`.
  - Line 775-784 (compact-set serialization): accept leaf NodeType variants.
- `src/stats.rs`:
  - `NodeStats` and `LevelStats`: add counters for the four leaf types.
  - `StatsVisitor`'s match on `node.as_ref().get_type()` at
    [src/stats.rs:276](src/stats.rs#L276): add the four leaf arms.
  - `memory_by_node_type()` at [src/stats.rs:32](src/stats.rs#L32):
    extend the tuple (or return an extended struct) to include leaf sizes.
- `src/lib.rs` — `pub use congee_raw_u32::CongeeRawU32;`.

**Unchanged:** `src/lock.rs`, `src/range_scan.rs`, `src/congee_raw.rs`,
`src/congee_set.rs`, `src/congee.rs`, `src/congee_compact_set.rs`,
`src/node_view.rs`, `src/nodes/node_ptr.rs`.

## Reused patterns

- Existing `Node` trait, `make_node`, `insert_grow` — work as-is.
- Existing `gen_method!` macro — extend arms, not the macro body.
- Node256's `key_mask: [u8; 32]` bitmap pattern — reuse verbatim for Node256Leaf.
- Node48's `child_idx: [u8; 256]` key-to-slot map — reuse verbatim; only
  the freelist mechanism changes.
- `MemoryStatsAllocator` ([src/utils.rs](src/utils.rs)) — for verification.

## Verification

1. **Size asserts** — each new leaf type has a
   `const _: () = assert!(size_of::<T>() == N);` matching the table.

2. **Existing test suite passes unchanged.** If it does, the refactor is
   invisible to callers. Run `cargo test --all-features`.

3. **New targeted tests** in a `#[cfg(test)]` module inside each leaf
   file:
   - Round-trip: insert 256 keys at a shared 7-byte prefix; read all back
     through the tree.
   - Force each leaf size class by inserting increasing numbers of
     terminal-byte-distinct keys (1, 4, 16, 48, 256). Confirm the
     expected leaf type appears via the extended `NodeStats` snapshot
     (which, per the `stats.rs` changes above, now tracks leaf variants).
   - Delete + re-insert on a Node48Leaf: verify the bitmap-based
     freelist correctly reuses slots.
   - `MemoryStatsAllocator` comparison: same workload on today's tree
     vs. the new tree — measure `allocated_bytes()` delta.

4. **Concurrent smoke test** (shuttle feature): a writer + two readers
   on a small dense key range, verify no torn reads and final state
   matches an oracle.

5. **Debug-assert coverage**: a test that tries to insert a SubNode into
   a leaf (via unsafe construction of a NodePtr) must panic in debug
   builds. Conversely for internal nodes.

Commands:
```
cargo build
cargo test
cargo test --features shuttle -- --test-threads=1
cargo clippy --all-targets
```
