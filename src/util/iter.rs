//! Helpful iterator structs

use std::ops::Add;

pub trait IteratorExt: Iterator + Sized {
    fn left<R>(self) -> Either<Self, R> {
        Either::Left(self)
    }

    fn right<L>(self) -> Either<L, Self> {
        Either::Right(self)
    }

    fn chunk_pairs(self) -> ChunkPairs<Self> {
        ChunkPairs(self)
    }

    fn decode_delta(self) -> DecodeDelta<Self>
    where
        Self::Item: Copy + Default + Add<Output = Self::Item>,
    {
        DecodeDelta {
            iter: self,
            total: Default::default(),
        }
    }
}
impl<I: Iterator + Sized> IteratorExt for I {}

/// Iterator which might be one of two different implementations
pub enum Either<L, R> {
    Left(L),
    Right(R),
}
impl<T, L: Iterator<Item = T>, R: Iterator<Item = T>> Iterator for Either<L, R> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Left(i) => i.next(),
            Self::Right(i) => i.next(),
        }
    }
}

/// Iterator which decodes delta-encoded values
pub struct DecodeDelta<I: Iterator> {
    total: I::Item,
    iter: I,
}
impl<I: Iterator> Iterator for DecodeDelta<I>
where
    I::Item: Copy + Add<Output = I::Item>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            self.total = self.total + x;
            self.total
        })
    }
}

/// Iterator which collects two `T`s to before producing a `(T, T)`
///
/// If the wrapped iterator would produce an odd amount of items, the last one will be dropped
pub struct ChunkPairs<I>(I);
impl<I: Iterator> Iterator for ChunkPairs<I> {
    type Item = (I::Item, I::Item);
    fn next(&mut self) -> Option<Self::Item> {
        let fst = self.0.next()?;
        let snd = self.0.next()?;
        Some((fst, snd))
    }
}
