//! Experimental functions for reading blobs truly parallel.
//!
//! These functions are experimental and could be problematic for data integrity.
//!
//! They try to find a blob's start using some simple comparisons.
//! A malicious OSM contributor might add a string which these comparisons would falsely identify as a blob's start.
//! The feasibility and impact needs further investigation.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Take};
use std::mem::size_of;
use std::path::Path;

use prost::Message;

use crate::blobs::{read_u32, ReadError};
use crate::proto;

crate::doc_imports! {
    use crate::blobs::iter_blobs;
    use crate::proto;
}

/// Open `num` file handles and position them equally spaced
///
/// Each of the returned readers start at the beginning of a blob and end before the next one starts.
pub fn mass_open(path: impl AsRef<Path>, num: usize) -> io::Result<Vec<Take<File>>> {
    let path = path.as_ref();

    let len = path.metadata()?.len();
    let chunk_size = len / num as u64;

    let mut files = VecDeque::with_capacity(num);
    let mut prev_start = len;
    for i in (0..num).rev() {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(i as u64 * chunk_size))?;
        let start = seek_next_blob(&mut file)?.unwrap_or(len);
        files.push_front(file.take(prev_start - start));
        prev_start = start;
    }

    Ok(files.into())
}

pub fn iter_blocks(
    mut reader: impl Read + Seek,
) -> impl Iterator<Item = Result<(SeekFrom, usize), ReadError>> {
    struct Iter<R> {
        reader: R,
        buffer: Vec<u8>,
    };
    impl<R: Read + Seek> Iter<R> {
        fn read(&mut self) -> Result<Option<(SeekFrom, usize)>, ReadError> {
            let Some(header_size) = read_u32(&mut self.reader)? else {
                return Ok(None);
            };
            let header_size = header_size as usize;

            if self.buffer.len() < header_size {
                self.buffer.resize(header_size, 0);
            }
            let buffer = &mut self.buffer[..header_size];

            self.reader.read_exact(buffer)?;

            let header = proto::BlobHeader::decode(&*buffer)?;

            let post_body = self
                .reader
                .seek(SeekFrom::Current(header.datasize as i64))?;
            Ok(Some((
                SeekFrom::Start(post_body - header.datasize as u64),
                header.datasize as usize,
            )))
        }
    }
    impl<R: Read + Seek> Iterator for Iter<R> {
        type Item = Result<(SeekFrom, usize), ReadError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.read().transpose()
        }
    }
    Iter {
        reader,
        buffer: Vec::new(),
    }
}

/// Find the next blob in a file
///
/// ## Arguments
/// - `reader` will probably be a [`File`] or anything which behaves similarly.
///
///     It may start in any position and will end at the beginning of a blob if `Ok(Some(_))` was returned.
///
///     "Beginning" is NOT the beginning of the [`proto::BlobHeader`] but the `u32` which precedes it.
///
///     i.e. if `Ok(Some(_))` is returned you can pass the `reader` to [`iter_blobs`].
///
/// ## Returns
/// - `Ok(None)` if end of file was reached (i.e. [`Read::read`] returned `Ok(0)`)
/// - `Ok(Some(pos))` if a blob was found,
///
///     where `pos` is its position from the beginning of the reader which could be used with [`SeekFrom::Start`].
pub fn seek_next_blob(reader: &mut (impl Read + Seek)) -> io::Result<Option<u64>> {
    const NEEDLES: &[&str] = &["OSMData", "OSMHeader"];
    const WINDOW_SIZE: usize = 7;
    const TAIL_SIZE: usize = WINDOW_SIZE - 1;

    let mut buffer = vec![0; 1024 * 1024]; // 1 MiB
    loop {
        let written = reader.read(&mut buffer[(WINDOW_SIZE - 1)..])?;
        if written == 0 {
            return Ok(None);
        }

        assert!(written > WINDOW_SIZE);
        let haystack = &buffer[0..(written + TAIL_SIZE)];

        let Some((position, needle)) =
            haystack
                .windows(WINDOW_SIZE)
                .enumerate()
                .find_map(|(i, window)| {
                    Some(i).zip(
                        NEEDLES
                            .iter()
                            .find(|needle| needle.as_bytes().starts_with(window))
                            .copied(),
                    )
                })
        else {
            // Get the last bytes which weren't at the beginning of any window yet
            let tail: [u8; TAIL_SIZE] = haystack
                .split_at(haystack.len() - WINDOW_SIZE - 1)
                .0
                .try_into()
                .unwrap();

            // Prepend the last bytes to the front of the buffer before overwriting the rest
            (&mut buffer[..TAIL_SIZE]).copy_from_slice(&tail);

            continue;
        };

        // Point the reader to the beginning of the matching reader
        #[allow(unused_parens)]
        let (
            // The needle's position in the entire reader
            needle_position
        ) =
            reader.seek(SeekFrom::Current(-((haystack.len() - position) as i64)))?;

        // Read the surrounding data where a potential header would have to be
        let (
            // The needle's position in `surroundings`
            needle_offset,
            // The bytes surrounding the needle
            surroundings,
        ) = {
            // The wiki says the max header size is 64 kiB
            // However since we can't deal with `indexdata` properly,
            // we just calculate the max length `type` and `datasize` can have
            const MAX_HEADER_SIZE: usize = 1 + 1 + 9 + 1 + size_of::<u32>();

            let mut needle_offset = MAX_HEADER_SIZE - WINDOW_SIZE;
            if needle_offset > needle_position as usize {
                needle_offset = needle_position as usize;
            }
            reader.seek(SeekFrom::Current(-(needle_offset as i64)))?;

            let mut remaining = &mut buffer[..(2 * MAX_HEADER_SIZE - WINDOW_SIZE)];
            while !remaining.is_empty() {
                let written = reader.read(remaining)?;
                if written == 0 {
                    break;
                }
                remaining = &mut remaining[written..];
            }

            (
                needle_offset,
                &buffer[..(2 * MAX_HEADER_SIZE - WINDOW_SIZE)],
            )
        };

        // 00001 010 -> ID = 1, type = 2 "length delimited"
        const TYPE_TAG: u8 = 0x0a;
        // 00011 000 -> ID = 3, type = 0 "varint"
        const DATASIZE_TAG: u8 = 0x18;

        let valid;
        let mut datasize_before_type = 0;
        if surroundings[needle_offset - 1] != needle.len() as u8 {
            valid = false;
        } else if surroundings[needle_offset - 2] != TYPE_TAG {
            valid = false;
        } else if surroundings[needle_offset + needle.len()] == DATASIZE_TAG {
            valid = true;
        } else {
            valid = loop {
                datasize_before_type += 1;
                if surroundings[needle_offset - 2 - datasize_before_type] == DATASIZE_TAG {
                    break true;
                } else if datasize_before_type == 4 {
                    break false;
                }
            };
        }

        if valid {
            return Ok(Some(reader.seek(SeekFrom::Start(
                needle_position
                    - 2
                    - if datasize_before_type > 0 {
                        datasize_before_type as u64 + 1
                    } else {
                        0
                    }
                    - 4,
            ))?));
        } else {
            reader.seek(SeekFrom::Start(needle_position + 1))?;
        }
    }
}
