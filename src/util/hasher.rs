use std::hash::{BuildHasher, Hasher};
use std::mem;

pub struct Noop;
impl BuildHasher for Noop {
    type Hasher = NoopHasher;

    fn build_hasher(&self) -> Self::Hasher {
        NoopHasher(0)
    }
}

pub struct NoopHasher(u64);
impl Hasher for NoopHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        panic!("The NoopHasher may only be used to hash integer");
    }

    fn write_u8(&mut self, i: u8) {
        self.write_u64(i as u64)
    }

    fn write_u16(&mut self, i: u16) {
        self.write_u64(i as u64)
    }

    fn write_u32(&mut self, i: u32) {
        self.write_u64(i as u64)
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    fn write_usize(&mut self, i: usize) {
        static _TEST: () = {
            if mem::size_of::<usize>() > mem::size_of::<u64>() {
                panic!("Hasher expected usize to be convertable to u64");
            }
        };
        self.write_u64(i as u64)
    }

    fn write_i8(&mut self, i: i8) {
        self.write_i64(i as i64)
    }

    fn write_i16(&mut self, i: i16) {
        self.write_i64(i as i64)
    }

    fn write_i32(&mut self, i: i32) {
        self.write_i64(i as i64)
    }

    fn write_i64(&mut self, i: i64) {
        self.0 = u64::from_ne_bytes(i.to_ne_bytes());
    }

    fn write_isize(&mut self, i: isize) {
        static _TEST: () = {
            if mem::size_of::<isize>() > mem::size_of::<i64>() {
                panic!("Hasher expected isize to be convertable to i64");
            }
        };
        self.write_i64(i as i64);
    }
}
