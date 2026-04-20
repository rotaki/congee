use crate::{
    cast_ptr,
    nodes::{BaseNode, NodeIter},
};

/// A view over the matched leaf node that contains the matched key.
///
/// The view stays within the current node and exposes sibling payloads after the
/// matched byte. It never wraps past `0xFF` and skips sub-node children.
pub struct NodeView<'a> {
    node: &'a BaseNode,
    target_byte: u8,
}

impl<'a> NodeView<'a> {
    pub(crate) fn new(node: &'a BaseNode, target_byte: u8) -> Self {
        Self { node, target_byte }
    }

    pub fn target_byte(&self) -> u8 {
        self.target_byte
    }

    pub fn siblings_after(&self) -> impl Iterator<Item = (u8, usize)> + '_ {
        SiblingIter {
            inner: self
                .target_byte
                .checked_add(1)
                .map(|start| self.node.get_children(start, u8::MAX)),
        }
    }
}

struct SiblingIter<'a> {
    inner: Option<NodeIter<'a>>,
}

impl Iterator for SiblingIter<'_> {
    type Item = (u8, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (byte, child) = self.inner.as_mut()?.next()?;
            cast_ptr!(child => {
                Payload(payload) => return Some((byte, payload)),
                SubNode(_sub_node) => continue,
            });
        }
    }
}
