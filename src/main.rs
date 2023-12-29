use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::VecDeque;
use std::error::Error;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use memmap::Mmap;
use osmiumoxide::blobs::probe::{iter_blocks, mass_open, seek_next_blob};
use osmiumoxide::blobs::{iter_blobs, Blob, BlobType};
use osmiumoxide::parse::parse_blob;
use rayon::prelude::*;

const FILE: &str = "/home/gammelalf/Downloads/bayern-latest.osm.pbf";

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let file = std::env::args().skip(1).next();
    let file = file.as_deref().unwrap_or(FILE);

    #[derive(Default)]
    struct Times<T> {
        /// Single handle, single worker
        sh_sw: T,
        /// Single handle, multiple worker
        sh_mw: T,
        /// Single seeker
        ss: T,
        /// Multiple seeker
        ms: T,
    }
    #[derive(Default)]
    struct Times2 {
        /// File descriptor
        fd: Duration,
        /// mmap
        mp: Duration,
    }
    let mut times: Times<Times2> = Default::default();

    time2(&mut times.sh_sw.fd, || {
        let file = File::open(file).unwrap();
        for blob in iter_blobs(file) {
            parse_blob(blob.unwrap()).unwrap();
        }
    });
    time2(&mut times.sh_sw.mp, || {
        let file = File::open(file).unwrap();
        let file = mmap(&file);
        let file = Cursor::new(file);
        for blob in iter_blobs(file) {
            parse_blob(blob.unwrap()).unwrap();
        }
    });
    time2(&mut times.sh_mw.fd, || {
        let file = File::open(file).unwrap();
        iter_blobs(file).par_bridge().for_each(|blob| {
            parse_blob(blob.unwrap()).unwrap();
        });
    });
    time2(&mut times.sh_mw.mp, || {
        let file = File::open(file).unwrap();
        let file = mmap(&file);
        let file = Cursor::new(file);
        iter_blobs(file).par_bridge().for_each(|blob| {
            parse_blob(blob.unwrap()).unwrap();
        });
    });
    time2(&mut times.ms.fd, || {
        mass_open(file, rayon::current_num_threads())
            .unwrap()
            .into_par_iter()
            .for_each(|file| {
                for blob in iter_blobs(file) {
                    parse_blob(blob.unwrap()).unwrap();
                }
            });
    });
    time2(&mut times.ms.mp, || {
        let num = rayon::current_num_threads();
        let path: &Path = file.as_ref();

        let len = path.metadata().unwrap().len();
        let chunk_size = len / num as u64;

        let file = File::open(file).unwrap();
        let file = mmap(&file);

        let mut files = VecDeque::with_capacity(num);
        let mut prev_start = len;
        for i in (0..num).rev() {
            let mut file = Cursor::new(&file);
            file.seek(SeekFrom::Start(i as u64 * chunk_size)).unwrap();
            let start = seek_next_blob(&mut file).unwrap().unwrap_or(len);
            files.push_front(file.take(prev_start - start));
            prev_start = start;
        }

        files.into_par_iter().for_each(|file| {
            for blob in iter_blobs(file) {
                parse_blob(blob.unwrap()).unwrap();
            }
        });
    });
    time2(&mut times.ss.fd, || {
        let seeks = iter_blocks(File::open(file).unwrap())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        seeks
            .par_chunks(seeks.len() / rayon::current_num_threads())
            .for_each(|seeks| {
                let mut file = File::open(file).unwrap();
                for (seek, len) in seeks {
                    file.seek(*seek).unwrap();
                    let mut bytes = BytesMut::zeroed(*len);
                    file.read_exact(&mut bytes).unwrap();
                    let _ = parse_blob(Blob {
                        r#type: BlobType::OSMData,
                        data: bytes.freeze(),
                    });
                }
            });
    });
    time2(&mut times.ss.mp, || {
        let file = File::open(file).unwrap();
        let file = mmap(&file);
        let seeks = iter_blocks(Cursor::new(&file))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        seeks
            .par_chunks(seeks.len() / rayon::current_num_threads())
            .for_each(|seeks| {
                let mut file = Cursor::new(&file);
                for (seek, len) in seeks {
                    file.seek(*seek).unwrap();
                    let mut bytes = BytesMut::zeroed(*len);
                    file.read_exact(&mut bytes).unwrap();
                    let _ = parse_blob(Blob {
                        r#type: BlobType::OSMData,
                        data: bytes.freeze(),
                    });
                }
            });
    });

    #[rustfmt::skip]
    let _ = {
        println!("                                Fd      Mp");
        println!("Single handle, single worker   {:5?}   {:5?}", times.sh_sw.fd, times.sh_sw.mp);
        println!("Single handle, multiple worker {:5?}   {:5?}", times.sh_mw.fd, times.sh_mw.mp);
        println!("Single seeker                  {:5?}   {:5?}", times.ss.fd, times.ss.mp);
        println!("Multiple seeker                {:5?}   {:5?}", times.ms.fd, times.ms.mp);
    };

    eprintln!(
        "Max heap={:.3}GiB",
        (ALLOC.peak.load(Ordering::Relaxed) as f64) / 1e9
    );

    Ok(())
}

fn time(name: &str, operation: impl FnOnce()) {
    let start = Instant::now();
    operation();
    let duration = Instant::now().duration_since(start);
    println!("{name} took {duration:?} to complete");
}
fn time2(duration: &mut Duration, operation: impl FnOnce()) {
    let start = Instant::now();
    operation();
    *duration = Instant::now().duration_since(start);
}

#[allow(unused)]
fn dyn_err(error: impl Error + 'static) -> Box<dyn Error> {
    Box::new(error)
}

pub struct Alloc {
    current: AtomicUsize,
    peak: AtomicUsize,
    system: System,
}
unsafe impl GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let prev = self.current.fetch_add(layout.size(), Ordering::Relaxed);
        self.peak.fetch_max(prev + layout.size(), Ordering::Relaxed);
        self.system.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.current.fetch_sub(layout.size(), Ordering::Relaxed);
        self.system.dealloc(ptr, layout)
    }
}
impl Alloc {
    /// Explicitly drop an item and return the difference in heap usage
    pub fn drop<T>(&self, item: T) -> usize {
        let before = self.current.load(Ordering::SeqCst);
        drop(item);
        let after = self.current.load(Ordering::SeqCst);
        before - after
    }
}
#[global_allocator]
pub static ALLOC: Alloc = Alloc {
    current: AtomicUsize::new(0),
    peak: AtomicUsize::new(0),
    system: System,
};

fn mmap(file: &File) -> Mmap {
    unsafe { Mmap::map(file).unwrap() }
}
