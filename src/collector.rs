use std::collections::BTreeSet;
use std::ops::Range;

use crate::blocks::{DataBlock, MemberType};
use crate::util::BSMap;

#[derive(Copy, Clone, Debug, Default)]
pub struct LatLon {
    pub lat: i64,
    pub lon: i64,
}

#[derive(Debug, Default)]
pub struct PreCollector {
    /// Set of all nodes referenced by ways
    nodes: BTreeSet<i64>,

    /// Set of all ways referenced by relations of type multipolygon
    ways: BTreeSet<i64>,
}

impl PreCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn collect_block(&mut self, block: DataBlock) {
        for way in block.iter_ways() {
            self.nodes.extend(way.nodes());
        }
        for relation in block.iter_relations() {
            if matches!(relation.tags().find(|(key, _)| *key == "type"), Some((_, value)) if value == "multipolygon")
            {
                self.ways.extend(
                    relation.members().filter_map(|member| {
                        (member.r#type == MemberType::Way).then_some(member.id)
                    }),
                );
            }
        }
    }

    pub fn finish(self) -> Collector {
        Collector {
            nodes: BSMap::from(self.nodes),
            ways: BSMap::from(self.ways),
            way_nodes: Vec::new(),
        }
    }

    pub fn mass_finish(selfs: Vec<Self>) -> Collector {
        Collector {
            nodes: BSMap::from_iter(selfs.iter().map(|pre_collector| &pre_collector.nodes)),
            ways: BSMap::from_iter(selfs.iter().map(|pre_collector| &pre_collector.ways)),
            way_nodes: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct Collector {
    /// Map from a node's id to its coordinates
    nodes: BSMap<i64, LatLon>,

    /// Map from a way's id to its member nodes' ids
    ///
    /// The actual ids are stored in `way_nodes`.
    /// This map only stores the range in `way_nodes`.
    ways: BSMap<i64, Range<usize>>,

    /// Way members' ids to be referenced by the `Range<usize>` in `ways`
    way_nodes: Vec<i64>,
}

impl Collector {
    pub fn collect_block(&mut self, block: DataBlock) {
        for node in block.iter_nodes() {
            if let Some(slot) = self.nodes.get_mut(&node.id()) {
                slot.lat = node.lat();
                slot.lon = node.lon();
            }
        }
        for way in block.iter_ways() {
            if let Some(slot) = self.ways.get_mut(&way.id()) {
                let begin = self.way_nodes.len();
                self.way_nodes.extend(way.nodes());
                let end = self.way_nodes.len();
                *slot = begin..end;
            }
        }
    }

    pub fn node(&self, id: i64) -> Option<LatLon> {
        self.nodes.get(&id).cloned()
    }

    pub fn way(&self, id: i64) -> impl Iterator<Item = LatLon> + '_ {
        let range = self.ways.get(&id).cloned().unwrap_or(0..0);
        let nodes = self.way_nodes.get(range).unwrap_or(&[]);
        nodes.into_iter().filter_map(|id| self.node(*id))
    }
}
