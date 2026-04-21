use super::NodePtr;
use super::base_node::{BaseNode, Node, NodeIter, NodeType};

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node16Leaf {
    base: BaseNode,
    keys: [u8; 16],
    values: [u32; 16],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node16Leaf>() == 96);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node16Leaf>() == 8);

impl Node16Leaf {
    fn get_insert_pos(&self, key: u8) -> usize {
        let mut pos = 0;
        while pos < self.base.meta.count() {
            if self.keys[pos] >= key {
                return pos;
            }
            pos += 1;
        }
        pos
    }

    #[cfg(target_arch = "x86_64")]
    fn get_child_pos_simd(&self, key: u8) -> Option<usize> {
        if is_x86_feature_detected!("sse2") {
            unsafe {
                let key_vec = _mm_set1_epi8(key as i8);
                let keys_vec = _mm_loadu_si128(self.keys.as_ptr() as *const __m128i);
                let cmp = _mm_cmpeq_epi8(key_vec, keys_vec);
                let mask = _mm_movemask_epi8(cmp) as u16;

                if mask != 0 {
                    let pos = mask.trailing_zeros() as usize;
                    let count = self.base.meta.count();
                    let valid = (pos < count) as usize;
                    if valid != 0 {
                        return Some(pos);
                    }
                }
                None
            }
        } else {
            self.get_child_pos_fallback(key)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn get_child_pos_simd(&self, key: u8) -> Option<usize> {
        self.get_child_pos_fallback(key)
    }

    #[inline]
    fn get_child_pos_fallback(&self, key: u8) -> Option<usize> {
        self.keys
            .iter()
            .take(self.base.meta.count())
            .position(|k| *k == key)
    }

    #[inline]
    fn get_child_pos(&self, key: u8) -> Option<usize> {
        self.get_child_pos_simd(key)
    }
}

pub(crate) struct Node16LeafIter<'a> {
    node: &'a Node16Leaf,
    start_pos: usize,
    end_pos: usize,
}

impl Iterator for Node16LeafIter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_pos > self.end_pos {
            return None;
        }
        let key = self.node.keys[self.start_pos];
        let value = self.node.values[self.start_pos];
        self.start_pos += 1;
        Some((key, NodePtr::from_payload(value as usize)))
    }
}

impl Node for Node16Leaf {
    fn get_type() -> NodeType {
        NodeType::N16Leaf
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter<'_> {
        if self.base.meta.count() == 0 {
            return NodeIter::N16Leaf(Node16LeafIter {
                node: self,
                start_pos: 1,
                end_pos: 0,
            });
        }

        let mut start_pos = self.base.meta.count();
        for i in 0..self.base.meta.count() {
            if self.keys[i] >= start {
                start_pos = i;
                break;
            }
        }

        let mut end_pos = 0;
        let mut found_end = false;
        for i in 0..self.base.meta.count() {
            if self.keys[i] <= end {
                end_pos = i;
                found_end = true;
            } else {
                break;
            }
        }

        if start_pos >= self.base.meta.count() || !found_end || start_pos > end_pos {
            return NodeIter::N16Leaf(Node16LeafIter {
                node: self,
                start_pos: 1,
                end_pos: 0,
            });
        }

        NodeIter::N16Leaf(Node16LeafIter {
            node: self,
            start_pos,
            end_pos,
        })
    }

    fn remove(&mut self, k: u8) {
        let pos = self
            .get_child_pos(k)
            .expect("trying to delete a non-existing key");

        self.keys.copy_within(pos + 1..self.base.meta.count(), pos);
        self.values
            .copy_within(pos + 1..self.base.meta.count(), pos);

        self.base.meta.dec_count();
        debug_assert!(self.get_child(k).is_none());
    }

    fn copy_to<N: Node>(&self, dst: &mut N) {
        debug_assert!(
            N::get_type().is_leaf_family(),
            "leaf node copy_to non-leaf destination"
        );
        for i in 0..self.base.meta.count() {
            dst.insert(self.keys[i], NodePtr::from_payload(self.values[i] as usize));
        }
    }

    fn base(&self) -> &BaseNode {
        &self.base
    }

    fn is_full(&self) -> bool {
        self.base.meta.count() == 16
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        debug_assert!(node.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { node.as_payload_unchecked() } as u32;

        let pos = self.get_insert_pos(key);

        if pos < self.base.meta.count() {
            self.keys.copy_within(pos..self.base.meta.count(), pos + 1);
            self.values
                .copy_within(pos..self.base.meta.count(), pos + 1);
        }

        self.keys[pos] = key;
        self.values[pos] = value;
        self.base.meta.inc_count();

        assert!(self.base.meta.count() <= 16);
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        debug_assert!(val.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { val.as_payload_unchecked() } as u32;
        let pos = self.get_child_pos(key).unwrap();
        let old = self.values[pos];
        self.values[pos] = value;
        NodePtr::from_payload(old as usize)
    }

    fn get_child(&self, key: u8) -> Option<NodePtr> {
        let pos = self.get_child_pos(key)?;
        let value = unsafe { self.values.get_unchecked(pos) };
        Some(NodePtr::from_payload(*value as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node16Leaf {
        Node16Leaf {
            base: BaseNode::new(NodeType::N16Leaf, &[]),
            keys: [0; 16],
            values: [0; 16],
        }
    }

    #[test]
    fn test_node_operations() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);
        let ptr3 = NodePtr::from_payload(0x3000);

        assert_eq!(Node16Leaf::get_type(), NodeType::N16Leaf);
        assert!(!node.is_full());

        node.insert(20, ptr2);
        node.insert(10, ptr1);
        node.insert(30, ptr3);

        assert_eq!(node.base().meta.count(), 3);
        assert_eq!(node.keys[0], 10);
        assert_eq!(node.keys[1], 20);
        assert_eq!(node.keys[2], 30);

        assert!(node.get_child(10).is_some());
        assert!(node.get_child(20).is_some());
        assert!(node.get_child(30).is_some());
        assert!(node.get_child(15).is_none());

        node.remove(20);
        assert_eq!(node.base().meta.count(), 2);
        assert!(node.get_child(20).is_none());
    }

    #[test]
    fn test_fill_to_capacity() {
        let mut node = create_test_node();
        for i in 0..16u8 {
            node.insert(i * 2, NodePtr::from_payload((i as usize + 1) * 0x1000));
        }
        assert!(node.is_full());
        for i in 0..16u8 {
            let got = node.get_child(i * 2).unwrap();
            assert_eq!(
                unsafe { got.as_payload_unchecked() },
                (i as usize + 1) * 0x1000
            );
        }
    }
}
