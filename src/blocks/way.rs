use crate::blocks::tags::Tags;
use crate::blocks::DataBlock;
use crate::proto;
use crate::util::iter::IteratorExt;

impl DataBlock {
    /// Iterate over the block's [`Way`]s
    pub fn iter_ways(&self) -> impl Iterator<Item = Way<'_>> + '_ {
        self.0.primitivegroup.iter().flat_map(|group| {
            group.ways.iter().map(|way| Way {
                block: self,
                way,
                tags: Tags {
                    keys: &way.keys,
                    vals: &way.vals,
                },
            })
        })
    }
}

/// An OSM way
pub struct Way<'a> {
    block: &'a DataBlock,
    way: &'a proto::Way,
    tags: Tags<'a>,
}

impl<'a> Way<'a> {
    /// The way's id
    pub fn id(&self) -> i64 {
        self.way.id
    }

    /// Iterate over the way's tags as key-value pairs
    pub fn tags(&self) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.tags.iter(&self.block)
    }

    /// Iterate over the way's tags' keys
    pub fn keys(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.tags.keys(&self.block)
    }

    /// Iterate over the way's tags' keys
    pub fn values(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.tags.values(&self.block)
    }

    /// Iterate over the way's nodes' ids
    pub fn nodes(&self) -> impl Iterator<Item = i64> + 'a {
        self.way.refs.iter().copied().decode_delta()
    }
}
