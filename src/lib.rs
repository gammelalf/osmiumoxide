use std::fs::File;
use std::io;
use std::path::Path;

use log::{debug, error, trace, warn};
use rayon::prelude::*;
use thiserror::Error;

use crate::fileformat::{ParseError, ReadError};
use crate::osmformat::{Block, DataBlock};

pub mod fileformat;
pub mod node_index;
pub mod osmformat;
pub(crate) mod util;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/osmpbf.rs"));
}

/// Read a `.osm.pbf` file and return an iterator over its blocks
///
/// This function is the simplest way to read a file but it is also offers the least amount of control:
/// - All errors during iteration are logged.
/// - File errors terminate the iterator.
/// - All other errors just skip their block.
///
/// When this function doesn't suffice (you need more error handling or control over speed),
/// use [`fileformat::read_fileformat`] to iterate over the file's [`RawBlock`](fileformat::RawBlock)s
/// and [`RawBlock::parse`] to decompress and parse them.
pub fn read(path: impl AsRef<Path>) -> Result<impl Iterator<Item = DataBlock>, Error> {
    let mut iter = fileformat::read_fileformat(File::open(path).map_err(Error::FileError)?);

    let block = iter.next().ok_or(Error::MissingHeader)??;
    let Block::Header(header) = block.parse()? else {
        return Err(Error::MissingHeader);
    };

    trace!("File header: {header:#?}");
    if let Some(feature) = header.unknown_required_features() {
        return Err(Error::UnknownFeature(feature.to_string()));
    }

    Ok(iter.take_while(Result::is_ok).filter_map(|result| {
        let raw = match result {
            Ok(raw) => raw,
            Err(err) => {
                error!("Failed to read file");
                debug!("Failed to read file: {err}");
                return None;
            }
        };
        let block = match raw.parse() {
            Ok(block) => block,
            Err(err) => {
                error!("Failed to parse block");
                debug!("Failed to parse block: {err}");
                return None;
            }
        };
        match block {
            Block::Header(_) => {
                warn!("Skipping header block");
                None
            }
            Block::Data(block) => Some(block),
            Block::Unknown(_) => {
                warn!("Skipping unknown block");
                None
            }
        }
    }))
}

/// Read a `.osm.pbf` file and return an iterator over its blocks
///
/// [`rayon`] version of [`read`]
pub fn read_par(path: impl AsRef<Path>) -> Result<impl ParallelIterator<Item = DataBlock>, Error> {
    let mut iter = fileformat::read_fileformat(File::open(path).map_err(Error::FileError)?);

    let block = iter.next().ok_or(Error::MissingHeader)??;
    let Block::Header(header) = block.parse()? else {
        return Err(Error::MissingHeader);
    };

    trace!("File header: {header:#?}");
    if let Some(feature) = header.unknown_required_features() {
        return Err(Error::UnknownFeature(feature.to_string()));
    }

    Ok(iter
        .par_bridge()
        .take_any_while(Result::is_ok)
        .filter_map(|result| {
            let raw = match result {
                Ok(raw) => raw,
                Err(err) => {
                    error!("Failed to read file");
                    debug!("Failed to read file: {err}");
                    return None;
                }
            };
            let block = match raw.parse() {
                Ok(block) => block,
                Err(err) => {
                    error!("Failed to parse block");
                    debug!("Failed to parse block: {err}");
                    return None;
                }
            };
            match block {
                Block::Header(_) => {
                    warn!("Skipping header block");
                    None
                }
                Block::Data(block) => Some(block),
                Block::Unknown(_) => {
                    warn!("Skipping unknown block");
                    None
                }
            }
        }))
}

#[derive(Error, Debug)]
pub enum Error {
    /// Failed to interact with file
    #[error("Failed to interact with file: {}", .0)]
    FileError(io::Error),

    /// Failed to decode protobuf messages
    #[error("Failed to decode protobuf messages: {}", .0)]
    ProstError(prost::DecodeError),

    /// Failed to decode compression
    #[error("Failed to decode compression: {}", .0)]
    ComprError(io::Error),

    /// The `.osm.pbf` file is missing its header block
    #[error("Missing header block")]
    MissingHeader,

    /// The `.osm.pbf` requires a feature not supported by osmiumoxide
    #[error("Unsupported feature: {}", .0)]
    UnknownFeature(String),
}
impl From<ReadError> for Error {
    fn from(value: ReadError) -> Self {
        match value {
            ReadError::Io(error) => Self::FileError(error),
            ReadError::Proto(error) => Self::ProstError(error),
        }
    }
}
impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        match value {
            ParseError::Io(error) => Self::ComprError(error),
            ParseError::Proto(error) => Self::ProstError(error),
        }
    }
}
