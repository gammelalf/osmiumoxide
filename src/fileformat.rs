use std::io;
use std::io::Read;

use bytes::{Buf, Bytes};
use flate2::bufread;
use prost::Message;
use thiserror::Error;

use crate::osmformat::{Block, DataBlock, HeaderBlock};
use crate::proto;
pub use crate::proto::blob::Data as BlockData;
use crate::proto::{Blob, BlobHeader};

/// Read an `.osm.pbf` file by iterating over its raw (potentially compressed) blocks
pub fn read_fileformat<R: Read>(reader: R) -> FileFormatReader<R> {
    FileFormatReader(reader)
}
pub struct FileFormatReader<R: Read>(R);
impl<R: Read> Iterator for FileFormatReader<R> {
    type Item = Result<RawBlock, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        fn read(reader: &mut impl Read) -> Result<Option<RawBlock>, ReadError> {
            let mut int = [0; 4];
            if let Err(err) = reader.read_exact(&mut int) {
                return match err.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(None),
                    _ => Err(err.into()),
                };
            }
            let header_size = u32::from_be_bytes(int) as usize;

            let mut buffer = vec![0; header_size];
            reader.read_exact(&mut buffer)?;
            let header = BlobHeader::decode(buffer.as_slice())?;
            let body_size = header.datasize as usize;

            let mut buffer = vec![0; body_size];
            reader.read_exact(&mut buffer)?;
            let body = Blob::decode(buffer.as_slice())?;

            Ok(Some(RawBlock {
                r#type: header.r#type,
                raw_size: body.raw_size,
                data: body.data.unwrap_or(BlockData::Raw(Bytes::new())),
            }))
        }
        read(&mut self.0).transpose()
    }
}

pub struct RawBlock {
    pub r#type: String,
    pub raw_size: Option<i32>,
    pub data: BlockData,
}

impl RawBlock {
    /// Decompress the stored `data` and parse it based on the `r#type`
    pub fn parse(self) -> Result<Block, ParseError> {
        let data = self.data.decompress(self.raw_size.map(|x| x as usize))?;
        Ok(match self.r#type.as_str() {
            "OSMHeader" => Block::Header(HeaderBlock::new(proto::HeaderBlock::decode(data)?)),
            "OSMData" => Block::Data(DataBlock::new(proto::PrimitiveBlock::decode(data)?)),
            _ => Block::Unknown(data),
        })
    }
}

impl BlockData {
    /// Decompress the stored data
    pub fn decompress(self, size_hint: Option<usize>) -> io::Result<Bytes> {
        Ok(match self {
            Self::Raw(raw) => raw,
            Self::ZlibData(encoded) => {
                let size_hint = size_hint.unwrap_or(encoded.len());
                let mut decoder = bufread::ZlibDecoder::new(encoded.reader());
                let mut decoded = Vec::with_capacity(size_hint);
                decoder.read_to_end(&mut decoded)?;
                decoded.into()
            }
            _ => unimplemented!("Unsupported format"),
        })
    }
}

#[derive(Error, Debug)]
pub enum ReadError {
    /// Failed to read `.osm.pbf` file
    #[error("Failed to read file: {}", .0)]
    Io(#[from] io::Error),
    /// Failed to decode outer blobs
    #[error("Failed to decode blobs: {}", .0)]
    Proto(#[from] prost::DecodeError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    /// Failed to decompress blobs
    #[error("Failed to read file: {}", .0)]
    Io(#[from] io::Error),
    /// Failed to decode actual data
    #[error("Failed to decode data: {}", .0)]
    Proto(#[from] prost::DecodeError),
}
