use super::{
    NodePtr,
    base_node::{BaseNode, Node, NodeIter, NodeType},
};

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node256Leaf {
    base: BaseNode,
    key_mask: [u8; 32],
    values: [u32; 256],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node256Leaf>() == 1072);
#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node256Leaf>() == 8);

impl Node256Leaf {
    #[inline]
    fn set_mask(&mut self, key: usize) {
        let idx = key / 8;
        let bit = key % 8;
        self.key_mask[idx] |= 1 << bit;
    }

    #[inline]
    fn unset_mask(&mut self, key: usize) {
        let idx = key / 8;
        let bit = key % 8;
        self.key_mask[idx] &= !(1 << bit);
    }

    #[inline]
    fn get_mask(&self, key: usize) -> bool {
        let idx = key / 8;
        let bit = key % 8;
        let key_mask = unsafe { self.key_mask.get_unchecked(idx) };
        *key_mask & (1 << bit) != 0
    }
}

pub(crate) struct Node256LeafIter<'a> {
    start: u8,
    end: u8,
    idx: u16,
    node: &'a Node256Leaf,
}

impl Iterator for Node256LeafIter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cur = self.idx + self.start as u16;

            if cur > self.end as u16 {
                return None;
            }

            self.idx += 1;

            if self.node.get_mask(cur as usize) {
                let value = self.node.values[cur as usize];
                return Some((cur as u8, NodePtr::from_payload(value as usize)));
            } else {
                continue;
            }
        }
    }
}

impl Node for Node256Leaf {
    fn get_type() -> NodeType {
        NodeType::N256Leaf
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter<'_> {
        NodeIter::N256Leaf(Node256LeafIter {
            start,
            end,
            idx: 0,
            node: self,
        })
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        debug_assert!(
            N::get_type().is_leaf_family(),
            "leaf node copy_to non-leaf destination"
        );
        for (i, v) in self.values.iter().enumerate() {
            if self.get_mask(i) {
                dst.insert(i as u8, NodePtr::from_payload(*v as usize));
            }
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn is_full(&self) -> bool {
        false
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        debug_assert!(node.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { node.as_payload_unchecked() } as u32;
        self.values[key as usize] = value;
        self.set_mask(key as usize);
        self.base.meta.inc_count();
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        debug_assert!(val.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { val.as_payload_unchecked() } as u32;
        let old = self.values[key as usize];
        self.values[key as usize] = value;
        NodePtr::from_payload(old as usize)
    }

    fn remove(&mut self, k: u8) {
        self.unset_mask(k as usize);
        self.base.meta.dec_count();
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        if self.get_mask(key as usize) {
            let value = unsafe { self.values.get_unchecked(key as usize) };
            Some(NodePtr::from_payload(*value as usize))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node256Leaf {
        Node256Leaf {
            base: BaseNode::new(NodeType::N256Leaf, &[]),
            key_mask: [0; 32],
            values: [0; 256],
        }
    }

    #[test]
    fn test_insert_and_get_all_keys() {
        let mut node = create_test_node();
        for i in 0..=255u8 {
            node.insert(i, NodePtr::from_payload(i as usize * 0x11 + 1));
        }
        assert_eq!(node.base().meta.count(), 256);
        for i in 0..=255u8 {
            let got = node.get_child(i).unwrap();
            assert_eq!(unsafe { got.as_payload_unchecked() }, i as usize * 0x11 + 1);
        }
    }

    #[test]
    fn test_remove() {
        let mut node = create_test_node();
        node.insert(42, NodePtr::from_payload(0xabc));
        node.insert(200, NodePtr::from_payload(0xdef));
        assert!(node.get_child(42).is_some());
        node.remove(42);
        assert!(node.get_child(42).is_none());
        assert!(node.get_child(200).is_some());
    }
}
