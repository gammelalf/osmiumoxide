//! Low-level `.osm.pbf` file details
//!
//! An `.osm.pbf` file consists of sections (called blobs) which can (in theory) be read and processed completely independently.
//!
//! ## Memory Layout
//! A file repeats the following the three parts:
//! 1. A `u32` in network endianness which contains the following part's size
//! 2. A protobuf encoded [`proto::BlobHeader`] which contains the following part's size as well as the type of data stored in this blob
//! 3. A protobuf encoded [`proto::Blob`] which contains the (possibly compressed) data
//!
//! ## Parallelism
//! In theory, you could open several file handles, seek different blobs in the file and process them completely parallel.
//!
//! Sadly the file doesn't contain an index at its beginning, so the only way to know where the next blob begins is by
//! reading all previous blobs.
//!
//! [`probe`] tries to overcome this problem but poses some new ones.
//!
//! In practice, the data stored in the blobs has some dependence on their order.
//! So, depending on your usage, the benefits from parallelization might be small.

pub mod probe;

use std::io::Read;
use std::{fmt, io};

use bytes::{Bytes, BytesMut};
use prost::Message;
use thiserror::Error;

use crate::proto;

crate::doc_imports! {
    use self::ReadError::Decode;
}

/// Iterate over a `.osm.pbf` file's raw chunks
///
/// See the [module](self) for more information.
pub fn iter_blobs<R: Read>(reader: R) -> BlobIter<R> {
    BlobIter(reader)
}

/// A raw chunk of data from an `.osm.pbf` file which can be processed independently
///
/// See the [module](self) for more information.
pub struct Blob {
    /// The blob's type indicating how to decode the `data`
    pub r#type: BlobType,

    /// The blob's data
    pub data: Bytes,
}

/// A [`Blob`]'s type indicating how to decode the `data`
#[derive(Debug)]
pub enum BlobType {
    /// The blob's `data` should be an encoded [`proto::HeaderBlock`]
    OSMHeader,

    /// The blob's `data` should be an encoded [`proto::PrimitiveBlock`]
    OSMData,

    /// The blob's `data` is of an unsupported format
    Unknown(String),
}
impl From<&str> for BlobType {
    fn from(value: &str) -> Self {
        match value {
            "OSMData" => BlobType::OSMData,
            "OSMHeader" => BlobType::OSMHeader,
            _ => BlobType::Unknown(value.to_string()),
        }
    }
}
impl fmt::Display for BlobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OSMData => f.write_str("OSMData"),
            Self::OSMHeader => f.write_str("OSMHeader"),
            Self::Unknown(string) => f.write_str(string.as_str()),
        }
    }
}

/// Iterator produced by [`iter_blobs`]
#[derive(Debug)]
pub struct BlobIter<R: Read>(R);
impl<R: Read> Iterator for BlobIter<R> {
    type Item = Result<Blob, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        fn read(reader: &mut impl Read) -> Result<Option<Blob>, ReadError> {
            let mut buffer = [0; 4];
            if let Err(err) = reader.read_exact(&mut buffer) {
                return match err.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(None),
                    _ => Err(err.into()),
                };
            }
            let header_size = u32::from_be_bytes(buffer) as usize;

            let mut buffer = vec![0; header_size as usize];
            reader.read_exact(&mut buffer)?;
            let header = proto::BlobHeader::decode(buffer.as_slice())?; // TODO: avoid String alloc
            let body_size = header.datasize as usize;

            let mut buffer = BytesMut::zeroed(body_size);
            reader.read_exact(&mut buffer)?;

            Ok(Some(Blob {
                r#type: header.r#type.as_str().into(),
                data: buffer.freeze(),
            }))
        }
        read(&mut self.0).transpose()
    }
}

fn read_u32(reader: &mut impl Read) -> io::Result<Option<u32>> {
    let mut buffer = [0; 4];
    if let Err(err) = reader.read_exact(&mut buffer) {
        return match err.kind() {
            io::ErrorKind::UnexpectedEof => Ok(None),
            _ => Err(err),
        };
    }
    Ok(Some(u32::from_be_bytes(buffer)))
}

/// An error which occurred while reading from an `.osm.pbf` file
///
/// **Note** this error is for reading not parsing.
/// The [`Decode`] variant has to be included,
/// because the [`proto::BlobHeader`] has to be decoded in order to know how many bytes to read.
#[derive(Error, Debug)]
pub enum ReadError {
    /// Failed to read `.osm.pbf` file
    #[error("Failed to read file: {}", .0)]
    Io(#[from] io::Error),

    /// Failed to decode blob header
    #[error("Failed to decode blob header: {}", .0)]
    Decode(#[from] prost::DecodeError),
}
impl From<ReadError> for io::Error {
    /// Convert the [`Decode`] variant into an [`io::ErrorKind::InvalidData`]
    fn from(value: ReadError) -> Self {
        match value {
            ReadError::Io(error) => error,
            ReadError::Decode(error) => io::Error::new(io::ErrorKind::InvalidData, error),
        }
    }
}
