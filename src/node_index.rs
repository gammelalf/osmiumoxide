use std::collections::{BTreeMap, HashMap};

use rayon::prelude::*;

use crate::osmformat::DataBlock;

pub struct NodeIndex {
    // BTree seems to have a smaller footprint than Hash
    pub map: BTreeMap<i64, LatLon>,
}

#[derive(Copy, Clone, Debug)]
pub struct LatLon {
    pub lat: i64,
    pub lon: i64,
}

impl NodeIndex {
    pub fn populate(blocks: impl Iterator<Item = DataBlock>) -> Self {
        let mut index = NodeIndex {
            map: BTreeMap::new(),
        };
        for block in blocks {
            for node in block.iter_nodes() {
                index.map.insert(
                    node.id(),
                    LatLon {
                        lat: node.lat(),
                        lon: node.lon(),
                    },
                );
            }
        }
        index
    }

    pub fn populate_par(blocks: impl ParallelIterator<Item = DataBlock>) -> Self {
        let map = BTreeMap::from_par_iter(blocks.flat_map(|block| {
            let nodes: Vec<_> = block
                .iter_nodes()
                .map(|node| {
                    (
                        node.id(),
                        LatLon {
                            lat: node.lat(),
                            lon: node.lon(),
                        },
                    )
                })
                .collect();
            nodes.into_par_iter()
        }));
        NodeIndex { map }
    }
}
