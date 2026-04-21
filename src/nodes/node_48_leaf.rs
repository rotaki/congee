use super::NodePtr;
use super::base_node::{BaseNode, Node, NodeIter, NodeType};
use super::node_48::EMPTY_MARKER;

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node48Leaf {
    base: BaseNode,
    pub(crate) child_idx: [u8; 256],
    /// Bit i set iff `values[i]` is currently occupied.
    occupied: u64,
    values: [u32; 48],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node48Leaf>() == 472);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node48Leaf>() == 8);

impl Node48Leaf {
    pub(crate) fn init_empty(&mut self) {
        for v in self.child_idx.iter_mut() {
            *v = EMPTY_MARKER;
        }
        self.occupied = 0;
    }

    #[inline]
    fn alloc_slot(&mut self) -> u8 {
        let free = !self.occupied;
        debug_assert!(free != 0, "Node48Leaf freelist exhausted");
        let pos = free.trailing_zeros() as u8;
        debug_assert!(pos < 48);
        self.occupied |= 1u64 << pos;
        pos
    }

    #[inline]
    fn free_slot(&mut self, pos: u8) {
        debug_assert!(pos < 48);
        self.occupied &= !(1u64 << pos);
    }
}

pub(crate) struct Node48LeafIter<'a> {
    start: u16,
    end: u16,
    node: &'a Node48Leaf,
}

impl Iterator for Node48LeafIter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.start > self.end {
                return None;
            }

            let key = self.start as usize;
            self.start += 1;

            let child_loc = self.node.child_idx[key];
            if child_loc != EMPTY_MARKER {
                let value = self.node.values[child_loc as usize];
                return Some((key as u8, NodePtr::from_payload(value as usize)));
            }
        }
    }
}

impl Node for Node48Leaf {
    fn get_type() -> NodeType {
        NodeType::N48Leaf
    }

    fn remove(&mut self, k: u8) {
        debug_assert!(self.child_idx[k as usize] != EMPTY_MARKER);
        let pos = self.child_idx[k as usize];
        self.free_slot(pos);
        self.child_idx[k as usize] = EMPTY_MARKER;
        self.base.meta.dec_count();
        debug_assert!(self.get_child(k).is_none());
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter<'_> {
        NodeIter::N48Leaf(Node48LeafIter {
            start: start as u16,
            end: end as u16,
            node: self,
        })
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        debug_assert!(
            N::get_type().is_leaf_family(),
            "leaf node copy_to non-leaf destination"
        );
        for (i, c) in self.child_idx.iter().enumerate() {
            if *c != EMPTY_MARKER {
                let value = self.values[*c as usize];
                dst.insert(i as u8, NodePtr::from_payload(value as usize));
            }
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn is_full(&self) -> bool {
        self.base.meta.count() == 48
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        debug_assert!(node.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { node.as_payload_unchecked() } as u32;

        let pos = self.alloc_slot() as usize;
        self.values[pos] = value;
        self.child_idx[key as usize] = pos as u8;
        self.base.meta.inc_count();
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        debug_assert!(val.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { val.as_payload_unchecked() } as u32;
        let pos = self.child_idx[key as usize];
        debug_assert!(pos != EMPTY_MARKER);
        let old = self.values[pos as usize];
        self.values[pos as usize] = value;
        NodePtr::from_payload(old as usize)
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        let pos = unsafe { self.child_idx.get_unchecked(key as usize) };
        if *pos == EMPTY_MARKER {
            None
        } else {
            let value = unsafe { self.values.get_unchecked(*pos as usize) };
            Some(NodePtr::from_payload(*value as usize))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node48Leaf {
        let mut node = Node48Leaf {
            base: BaseNode::new(NodeType::N48Leaf, &[]),
            child_idx: [EMPTY_MARKER; 256],
            occupied: 0,
            values: [0; 48],
        };
        node.init_empty();
        node
    }

    #[test]
    fn test_insert_and_get() {
        let mut node = create_test_node();
        for i in 0..48u8 {
            node.insert(i, NodePtr::from_payload(i as usize * 0x1000 + 1));
        }
        assert!(node.is_full());
        for i in 0..48u8 {
            let got = node.get_child(i).unwrap();
            assert_eq!(
                unsafe { got.as_payload_unchecked() },
                i as usize * 0x1000 + 1
            );
        }
    }

    #[test]
    fn test_remove_and_reinsert_reuses_slots() {
        let mut node = create_test_node();
        for i in 0..48u8 {
            node.insert(i, NodePtr::from_payload(i as usize + 1));
        }
        assert_eq!(node.occupied.count_ones(), 48);

        node.remove(5);
        node.remove(17);
        assert_eq!(node.occupied.count_ones(), 46);

        // Re-insert: should find two free slots via bitmap
        node.insert(100, NodePtr::from_payload(0xabc));
        node.insert(200, NodePtr::from_payload(0xdef));
        assert_eq!(node.occupied.count_ones(), 48);
        assert_eq!(
            unsafe { node.get_child(100).unwrap().as_payload_unchecked() },
            0xabc
        );
        assert_eq!(
            unsafe { node.get_child(200).unwrap().as_payload_unchecked() },
            0xdef
        );

        // Removed keys should no longer be present
        assert!(node.get_child(5).is_none());
        assert!(node.get_child(17).is_none());
    }
}
