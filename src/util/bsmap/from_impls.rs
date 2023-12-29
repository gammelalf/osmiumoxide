use std::collections::{BTreeMap, BTreeSet};
use std::iter::Peekable;

use crate::util::BSMap;

impl<K: Ord, V> BSMap<K, V> {
    fn merge_iters<I: ExactSizeIterator>(
        iters: impl IntoIterator<Item = I>,
        get_key: impl Fn(&I::Item) -> &K,
        get_entry: impl Fn(I::Item) -> (K, V),
    ) -> Self {
        let mut peekables: Vec<Peekable<I>> =
            iters.into_iter().map(|iter| iter.peekable()).collect();
        let mut vec = Vec::with_capacity(peekables.iter().map(|peekable| peekable.len()).sum());
        loop {
            let mut smallest: Option<(usize, &K)> = None;
            for (index, peekable) in peekables.iter_mut().enumerate() {
                match (smallest, peekable.peek().map(&get_key)) {
                    (Some((_, current)), Some(new)) if new < current => {
                        smallest = Some((index, new));
                    }
                    (None, Some(new)) => {
                        smallest = Some((index, new));
                    }
                    _ => {}
                }
            }
            if let Some((index, _)) = smallest {
                vec.push(get_entry(peekables[index].next().expect(
                    "Index should only be stored when peek yielded some item",
                )));
            } else {
                break;
            }
        }
        Self { vec }
    }
}

impl<K: Ord, V: Default> From<BTreeSet<K>> for BSMap<K, V> {
    fn from(value: BTreeSet<K>) -> Self {
        Self {
            vec: Vec::from_iter(value.into_iter().map(|k| (k, V::default()))),
        }
    }
}
impl<K: Ord, V: Default> FromIterator<BTreeSet<K>> for BSMap<K, V> {
    fn from_iter<T: IntoIterator<Item = BTreeSet<K>>>(iter: T) -> Self {
        Self::merge_iters(
            iter.into_iter().map(BTreeSet::into_iter),
            |k| k,
            |k| (k, V::default()),
        )
    }
}

impl<'a, K: Ord + Clone, V: Default> From<&'a BTreeSet<K>> for BSMap<K, V> {
    fn from(value: &'a BTreeSet<K>) -> Self {
        Self {
            vec: Vec::from_iter(value.into_iter().map(|k| (k.clone(), V::default()))),
        }
    }
}
impl<'a, K: Ord + Clone + 'a, V: Default> FromIterator<&'a BTreeSet<K>> for BSMap<K, V> {
    fn from_iter<T: IntoIterator<Item = &'a BTreeSet<K>>>(iter: T) -> Self {
        Self::merge_iters(
            iter.into_iter().map(BTreeSet::iter),
            |k| k,
            |k| (k.clone(), V::default()),
        )
    }
}

impl<K: Ord, V> From<BTreeMap<K, V>> for BSMap<K, V> {
    fn from(value: BTreeMap<K, V>) -> Self {
        Self {
            vec: Vec::from_iter(value.into_iter()),
        }
    }
}
impl<K: Ord, V> FromIterator<BTreeMap<K, V>> for BSMap<K, V> {
    fn from_iter<T: IntoIterator<Item = BTreeMap<K, V>>>(iter: T) -> Self {
        Self::merge_iters(iter.into_iter().map(BTreeMap::into_iter), |(k, _)| k, |e| e)
    }
}

impl<'a, K: Ord + Clone, V: Clone> From<&'a BTreeMap<K, V>> for BSMap<K, V> {
    fn from(value: &'a BTreeMap<K, V>) -> Self {
        Self {
            vec: Vec::from_iter(value.into_iter().map(|(k, v)| (k.clone(), v.clone()))),
        }
    }
}
impl<'a, K: Ord + Clone + 'a, V: Clone + 'a> FromIterator<&'a BTreeMap<K, V>> for BSMap<K, V> {
    fn from_iter<T: IntoIterator<Item = &'a BTreeMap<K, V>>>(iter: T) -> Self {
        Self::merge_iters(
            iter.into_iter().map(BTreeMap::iter),
            |(k, _)| k,
            |(k, v)| (k.clone(), v.clone()),
        )
    }
}
