use std::fs::File;
use std::io;
use std::path::Path;

use log::{debug, error, trace, warn};
use rayon::prelude::*;
use thiserror::Error;

use crate::blobs::{iter_blobs, Blob, ReadError};
use crate::blocks::{Block, DataBlock};
use crate::parse::{parse_blob, ParseError};

pub mod blobs;
pub mod blocks;
pub mod collector;
pub mod node_index;
pub mod parse;
pub mod util;

/// Auto-generated protobuf messages
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
/// use [`blobs::iter_blobs`] to iterate over the file's [`Blob`]s
/// and [`parse::parse_blob`] to decompress and decode them.
pub fn read(path: impl AsRef<Path>) -> Result<impl Iterator<Item = DataBlock>, Error> {
    Ok(read_process_header(path.as_ref())?
        .take_while(Result::is_ok)
        .filter_map(read_process_block))
}

/// Read a `.osm.pbf` file and return an iterator over its blocks
///
/// [`rayon`] version of [`read`]
pub fn read_par(path: impl AsRef<Path>) -> Result<impl ParallelIterator<Item = DataBlock>, Error> {
    Ok(read_process_header(path.as_ref())?
        .par_bridge()
        .take_any_while(Result::is_ok)
        .filter_map(read_process_block))
}

/// Helper function used in `read...` to open the file and process its header
fn read_process_header(
    path: &Path,
) -> Result<impl Iterator<Item = Result<Blob, ReadError>>, Error> {
    let mut blobs = iter_blobs(File::open(path).map_err(Error::FileError)?);

    let blob = blobs.next().ok_or(Error::MissingHeader)??;
    let Block::Header(header) = parse_blob(blob)? else {
        return Err(Error::MissingHeader);
    };

    trace!("File header: {header:#?}");
    if let Some(feature) = header.unknown_required_features() {
        return Err(Error::UnknownFeature(feature.to_string()));
    }

    return Ok(blobs);
}

/// Helper function used in `read...` to process the stream of blocks
fn read_process_block(result: Result<Blob, ReadError>) -> Option<DataBlock> {
    let blob = match result {
        Ok(raw) => raw,
        Err(err) => {
            error!("Failed to read file");
            debug!("Failed to read file: {err}");
            return None;
        }
    };
    let block = match parse_blob(blob) {
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
        Block::Unknown(string, _) => {
            warn!("Skipping unknown block of type \"{string}\"");
            None
        }
    }
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
            ReadError::Decode(error) => Self::ProstError(error),
        }
    }
}
impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        match value {
            ParseError::Io(error) => Self::ComprError(error),
            ParseError::Decode(error) => Self::ProstError(error),
        }
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! doc_imports {
    ($(use $path:path;)+) => {
        $(
            #[cfg(doc)]
            use $path;
        )+
    };
}
