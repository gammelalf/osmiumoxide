use std::io;
use std::io::Read;

use bytes::{Buf, Bytes};
use flate2::bufread;
use prost::Message;
use thiserror::Error;

use crate::blobs::{Blob, BlobType};
use crate::osmformat::{Block, DataBlock, HeaderBlock};
use crate::proto;
pub use crate::proto::blob::Data as BlockCompression;

pub fn parse_blob(blob: Blob) -> Result<Block, ParseError> {
    let Blob { r#type, data } = blob;

    // Decode outer proto
    let proto::Blob { raw_size, data } = proto::Blob::decode(data)?;

    // Decompress
    let raw = match data.unwrap_or(BlockCompression::Raw(Bytes::new())) {
        BlockCompression::Raw(raw) => raw,
        BlockCompression::ZlibData(encoded) => {
            let size_hint = raw_size
                .and_then(|x| usize::try_from(x).ok())
                .unwrap_or(encoded.len());
            let mut decoder = bufread::ZlibDecoder::new(encoded.reader());
            let mut decoded = Vec::with_capacity(size_hint);
            decoder.read_to_end(&mut decoded)?;
            decoded.into()
        }
        _ => unimplemented!("Unsupported format"),
    };

    // Decode inner proto
    let block = match r#type {
        BlobType::OSMHeader => Block::Header(HeaderBlock::new(proto::HeaderBlock::decode(raw)?)),
        BlobType::OSMData => Block::Data(DataBlock::new(proto::PrimitiveBlock::decode(raw)?)),
        BlobType::Unknown(string) => Block::Unknown(string, raw),
    };

    Ok(block)
}

#[derive(Error, Debug)]
pub enum ParseError {
    /// Failed to decompress blobs
    #[error("Failed to read file: {}", .0)]
    Io(#[from] io::Error),

    /// Failed to decode actual data
    #[error("Failed to decode data: {}", .0)]
    Decode(#[from] prost::DecodeError),
}
