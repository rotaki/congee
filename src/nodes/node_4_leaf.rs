use super::NodePtr;
use super::base_node::{BaseNode, Node, NodeIter, NodeType};

#[repr(C)]
#[repr(align(8))]
pub(crate) struct Node4Leaf {
    base: BaseNode,
    keys: [u8; 4],
    values: [u32; 4],
}

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::size_of::<Node4Leaf>() == 40);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(std::mem::align_of::<Node4Leaf>() == 8);

pub(crate) struct Node4LeafIter<'a> {
    start: u8,
    end: u8,
    idx: u8,
    cnt: u8,
    node: &'a Node4Leaf,
}

impl Iterator for Node4LeafIter<'_> {
    type Item = (u8, NodePtr);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.idx >= self.cnt {
                return None;
            }
            let cur = self.idx;
            self.idx += 1;

            let key = self.node.keys[cur as usize];
            if key >= self.start && key <= self.end {
                return Some((
                    key,
                    NodePtr::from_payload(self.node.values[cur as usize] as usize),
                ));
            }
        }
    }
}

impl Node for Node4Leaf {
    fn get_type() -> NodeType {
        NodeType::N4Leaf
    }

    fn remove(&mut self, k: u8) {
        if let Some(pos) = self.keys.iter().position(|&key| key == k) {
            self.keys.copy_within(pos + 1..self.base.meta.count(), pos);
            self.values
                .copy_within(pos + 1..self.base.meta.count(), pos);

            self.base.meta.dec_count();
        }
    }

    fn get_children(&self, start: u8, end: u8) -> NodeIter<'_> {
        NodeIter::N4Leaf(Node4LeafIter {
            start,
            end,
            idx: 0,
            cnt: self.base.meta.count() as u8,
            node: self,
        })
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
        self.base.meta.count() == 4
    }

    fn insert(&mut self, key: u8, node: NodePtr) {
        debug_assert!(node.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { node.as_payload_unchecked() } as u32;

        let mut pos: usize = 0;

        while pos < self.base.meta.count() {
            if self.keys[pos] < key {
                pos += 1;
                continue;
            } else {
                break;
            }
        }

        if pos < self.base.meta.count() {
            self.keys.copy_within(pos..self.base.meta.count(), pos + 1);
            self.values
                .copy_within(pos..self.base.meta.count(), pos + 1);
        }

        self.keys[pos] = key;
        self.values[pos] = value;
        self.base.meta.inc_count();
    }

    fn change(&mut self, key: u8, val: NodePtr) -> NodePtr {
        debug_assert!(val.is_payload(), "leaf node received a subnode pointer");
        let value = unsafe { val.as_payload_unchecked() } as u32;
        for i in 0..self.base.meta.count() {
            if self.keys[i] == key {
                let old = self.values[i];
                self.values[i] = value;
                return NodePtr::from_payload(old as usize);
            }
        }
        unreachable!("The key should always exist in the node");
    }

    #[inline]
    fn get_child(&self, key: u8) -> Option<NodePtr> {
        let count = self.base.meta.count();

        if count > 0 && self.keys[0] == key {
            return Some(NodePtr::from_payload(self.values[0] as usize));
        }
        if count > 1 && self.keys[1] == key {
            return Some(NodePtr::from_payload(self.values[1] as usize));
        }
        if count > 2 && self.keys[2] == key {
            return Some(NodePtr::from_payload(self.values[2] as usize));
        }
        if count > 3 && self.keys[3] == key {
            return Some(NodePtr::from_payload(self.values[3] as usize));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> Node4Leaf {
        Node4Leaf {
            base: BaseNode::new(NodeType::N4Leaf, &[]),
            keys: [0; 4],
            values: [0; 4],
        }
    }

    #[test]
    fn test_node_operations() {
        let mut node = create_test_node();
        let ptr1 = NodePtr::from_payload(0x1000);
        let ptr2 = NodePtr::from_payload(0x2000);
        let ptr3 = NodePtr::from_payload(0x3000);

        assert_eq!(Node4Leaf::get_type(), NodeType::N4Leaf);
        assert!(!node.is_full());
        assert_eq!(node.base().meta.count(), 0);

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

        node.insert(40, NodePtr::from_payload(0x4000));
        assert!(node.is_full());

        let new_ptr = NodePtr::from_payload(0x5000);
        let old_ptr = node.change(10, new_ptr);
        assert!(old_ptr.is_payload());
        assert_eq!(unsafe { old_ptr.as_payload_unchecked() }, 0x1000);
        assert!(node.get_child(10).is_some());

        node.remove(20);
        assert_eq!(node.base().meta.count(), 3);
        assert!(node.get_child(20).is_none());
    }
}
