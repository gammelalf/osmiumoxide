//! Mid-level parsing of `.osm.pbf` files
//!
//! This modules parses [`Blob`]s while avoiding copying.
//! To achieve this, some API convenience has to be sacrificed.

mod node;
mod relation;
mod tags;
mod way;

use std::borrow::Cow;
use std::fmt;
use std::fmt::Formatter;
use std::str::from_utf8_unchecked;

use bytes::Bytes;

pub use self::node::Node;
pub use self::relation::{Member, MemberType, Relation};
pub use self::way::Way;
use crate::proto;

crate::doc_imports! {
    use crate::blobs::Blob;
}

/// A parsed `.osm.pbf` file's block
pub enum Block {
    /// A header block stores meta-information about the file
    Header(HeaderBlock),

    /// A data block stores OSM's primitives: Nodes, Relations and Ways
    Data(DataBlock),

    /// A block of unknown type
    Unknown(String, Bytes),
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Block::Header(header) => f.debug_tuple("Block::Header").field(&*header).finish(),
            Block::Data(_) => f.debug_tuple("Block::Data").finish(),
            Block::Unknown(string, _) => f.debug_tuple("Block::Unknown").field(&*string).finish(),
        }
    }
}

#[derive(Debug)]
pub struct HeaderBlock(proto::HeaderBlock);
impl HeaderBlock {
    pub fn new(block: proto::HeaderBlock) -> Self {
        Self(block)
    }

    pub fn unknown_required_features(&self) -> Option<&str> {
        for feature in self.0.required_features.iter() {
            match feature.as_str() {
                "OsmSchema-V0.6" | "DenseNodes" => continue,
                _ => return Some(feature),
            }
        }
        None
    }
}

pub struct DataBlock(proto::PrimitiveBlock);
impl DataBlock {
    /// Wrap a [`proto::PrimitiveBlock`] to provide a sane API
    ///
    /// This performs some checks:
    /// - All strings are checked (and tweaked) to be valid utf-8
    pub fn new(mut block: proto::PrimitiveBlock) -> Self {
        for bytes in block.stringtable.s.iter_mut() {
            let string = match String::from_utf8_lossy(&bytes) {
                Cow::Borrowed(_) => None,
                Cow::Owned(string) => Some(string),
            };
            if let Some(string) = string {
                *bytes = Bytes::from(string.into_bytes());
            }
        }
        Self(block)
    }

    /// Retrieve a string by its index
    fn get_str(&self, index: usize) -> Option<&str> {
        self.0.stringtable.s.get(index).map(|bytes| unsafe {
            // `stringtable` is checked to be valid utf-8 in `new` and invalid utf-8 is replaced
            from_utf8_unchecked(bytes)
        })
    }

    /// Convert the raw longitude stored in a node into nanodegrees
    fn get_lon(&self, raw_lon: i64) -> i64 {
        self.0.lon_offset.unwrap_or(0) + self.0.granularity.unwrap_or(100) as i64 * raw_lon
    }

    /// Convert the raw latitude stored in a node into nanodegrees
    fn get_lat(&self, raw_lat: i64) -> i64 {
        self.0.lat_offset.unwrap_or(0) + self.0.granularity.unwrap_or(100) as i64 * raw_lat
    }

    /// convert the raw timestamp stored in an info object into milliseconds
    fn get_time(&self, raw_time: i64) -> i64 {
        self.0.date_granularity.unwrap_or(1000) as i64 * raw_time
    }
}
