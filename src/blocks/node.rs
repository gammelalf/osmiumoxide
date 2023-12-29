use std::iter::repeat;

use crate::blocks::tags::Tags;
use crate::blocks::DataBlock;
use crate::util::iter::IteratorExt;

impl DataBlock {
    /// Iterate over the block's [`Node`]s
    pub fn iter_nodes(&self) -> impl Iterator<Item = Node<'_>> + '_ {
        self.0.primitivegroup.iter().flat_map(|group| {
            let nodes = group.nodes.iter().map(|node| Node {
                block: self,
                id: node.id,
                lat: node.lat,
                lon: node.lon,
                tags: NodeTags::Normal(Tags {
                    keys: &node.keys,
                    vals: &node.vals,
                }),
            });
            let dense_nodes = group.dense.iter().flat_map(|dense_nodes| {
                const EMPTY_TAGS: &[i32] = &[];
                dense_nodes
                    .id
                    .iter()
                    .copied()
                    .decode_delta()
                    .zip(dense_nodes.lat.iter().copied().decode_delta())
                    .zip(dense_nodes.lon.iter().copied().decode_delta())
                    .zip(
                        dense_nodes
                            .keys_vals
                            .split(|x| *x == 0)
                            .chain(repeat(EMPTY_TAGS)),
                    )
                    .map(|(((id, lat), lon), keys_vals)| Node {
                        block: self,
                        id,
                        lat,
                        lon,
                        tags: NodeTags::Dense(keys_vals),
                    })
            });
            nodes.chain(dense_nodes)
        })
    }
}

/// An OSM node
pub struct Node<'a> {
    block: &'a DataBlock,

    /// The node's id
    id: i64,

    /// The node's "raw" latitude
    ///
    /// Use [`DataBlock::get_lat`] to convert it into the real latitude
    lat: i64,

    /// The node's "raw" longitude
    ///
    /// Use [`DataBlock::get_lon`] to convert it into the real longitude
    lon: i64,

    /// The node's tags
    ///
    /// The impl is dependent on the node's origin i.e. is it stored densely or not
    tags: NodeTags<'a>,
}

enum NodeTags<'a> {
    Normal(Tags<'a>),
    Dense(&'a [i32]),
}

impl<'a> Node<'a> {
    /// The node's id
    pub fn id(&self) -> i64 {
        self.id
    }

    /// The node's latitude in nanodegrees
    pub fn lat(&self) -> i64 {
        self.block.get_lat(self.lat)
    }

    /// The node's longitude in nanodegrees
    pub fn lon(&self) -> i64 {
        self.block.get_lon(self.lon)
    }

    /// Iterate over the node's tags as key-value pairs
    pub fn tags(&self) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        match &self.tags {
            NodeTags::Normal(tags) => tags.iter(&self.block).left(),
            NodeTags::Dense(tags) => tags
                .iter()
                .copied()
                .chunk_pairs()
                .flat_map(|(key, value)| {
                    self.block
                        .get_str(key as usize)
                        .zip(self.block.get_str(value as usize))
                })
                .right(),
        }
    }

    /// Iterate over the node's tags' keys
    pub fn keys(&self) -> impl Iterator<Item = &'a str> + 'a {
        match &self.tags {
            NodeTags::Normal(tags) => tags.keys(&self.block).left(),
            NodeTags::Dense(tags) => tags
                .iter()
                .copied()
                .step_by(2)
                .flat_map(|key| self.block.get_str(key as usize))
                .right(),
        }
    }

    /// Iterate over the node's tags' keys
    pub fn values(&self) -> impl Iterator<Item = &'a str> + 'a {
        match &self.tags {
            NodeTags::Normal(tags) => tags.values(&self.block).left(),
            NodeTags::Dense(tags) => tags
                .iter()
                .copied()
                .skip(1)
                .step_by(2)
                .flat_map(|value| self.block.get_str(value as usize))
                .right(),
        }
    }

    // TODO expose self.node.info
}
