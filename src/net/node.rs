//! Network node identification.

use std::fmt;

/// A unique identifier for a node in the network.
///
/// Node IDs are simple 32-bit integers for efficiency and ease of use.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeId(pub u32);

impl NodeId {
    /// Creates a new node ID.
    #[inline]
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Returns the raw ID value.
    #[inline]
    #[must_use]
    pub const fn as_u32(&self) -> u32 {
        self.0
    }

    /// Returns the ID as a usize for indexing.
    #[inline]
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl From<u32> for NodeId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<usize> for NodeId {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "NodeId is a 32-bit identifier by definition; simulations never \
                  approach 2^32 nodes, so a usize node count always fits in u32"
    )]
    fn from(id: usize) -> Self {
        Self(id as u32)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "N{}", self.0)
    }
}

/// A unique identifier for a message.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct MessageId(pub u64);

impl MessageId {
    /// Creates a new message ID.
    #[inline]
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw ID value.
    #[inline]
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for MessageId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Msg({})", self.0)
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "M{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_conversions() {
        let id = NodeId::new(42);
        assert_eq!(id.as_u32(), 42);
        assert_eq!(id.as_usize(), 42);

        let from_u32: NodeId = 10u32.into();
        assert_eq!(from_u32.as_u32(), 10);

        let from_usize: NodeId = 20usize.into();
        assert_eq!(from_usize.as_u32(), 20);
    }

    #[test]
    fn node_id_ordering() {
        let ids = [NodeId(3), NodeId(1), NodeId(2)];
        let mut sorted = ids;
        sorted.sort();
        assert_eq!(sorted, [NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn message_id_basics() {
        let id = MessageId::new(12345);
        assert_eq!(id.as_u64(), 12345);
        assert_eq!(format!("{id}"), "M12345");
    }
}
