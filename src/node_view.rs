use crate::cast_ptr;
use crate::nodes::{BaseNode, NodeIter};

/// Read-only view of the ART inner node that holds the target key during a
/// [`CongeeRaw::compute_if_present_with_siblings`](crate::CongeeRaw::compute_if_present_with_siblings)
/// or [`CongeeRaw::get_with_siblings`](crate::CongeeRaw::get_with_siblings) call.
///
/// Exposes a forward iterator over sibling payloads in the same node, intended
/// for warming caches / pre-reading numerically adjacent keys without a second
/// tree traversal.
pub struct NodeView<'a> {
    node: &'a BaseNode,
    target_byte: u8,
}

impl<'a> NodeView<'a> {
    pub(crate) fn new(node: &'a BaseNode, target_byte: u8) -> Self {
        Self { node, target_byte }
    }

    /// Byte slot of the target key in this node (i.e. `k[level]` at the matched level).
    pub fn target_byte(&self) -> u8 {
        self.target_byte
    }

    /// Forward iterator over sibling payloads, starting at `target_byte + 1`
    /// and walking byte-ascending through the same node. Empty slots and
    /// sub-node slots are skipped. Does not wrap past `0xFF`; does not leave
    /// the current node.
    pub fn siblings_after(&self) -> SiblingIter<'_> {
        if self.target_byte == u8::MAX {
            SiblingIter { inner: None }
        } else {
            let start = self.target_byte + 1;
            SiblingIter {
                inner: Some(self.node.get_children(start, u8::MAX)),
            }
        }
    }
}

/// Iterator yielded by [`NodeView::siblings_after`]. Yields `(byte, payload)`
/// pairs in ascending byte order.
pub struct SiblingIter<'a> {
    inner: Option<NodeIter<'a>>,
}

impl<'a> Iterator for SiblingIter<'a> {
    type Item = (u8, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let inner = self.inner.as_mut()?;
        loop {
            let (byte, ptr) = inner.next()?;
            let tid = cast_ptr!(ptr => {
                Payload(tid) => Some(tid),
                SubNode(_) => None,
            });
            if let Some(tid) = tid {
                return Some((byte, tid));
            }
        }
    }
}
