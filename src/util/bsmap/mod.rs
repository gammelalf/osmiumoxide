//! Map based on binary search over a sorted [`Vec`]

mod from_impls;

use std::fmt;
use std::ops::{Index, IndexMut};

crate::doc_imports! {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
}

/// A map optimised for memory footprint which doesn't allow adding new elements after it has been constructed.
///
/// This map can be constructed from a [`BTreeSet`] or [`BTreeMap`].
#[derive(Clone)]
pub struct BSMap<K: Ord, V> {
    /// Sorted by `K`
    vec: Vec<(K, V)>,
}

impl<K: Ord + fmt::Debug, V: fmt::Debug> fmt::Debug for BSMap<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K: Ord, V> BSMap<K, V> {
    fn get_index(&self, key: &K) -> Option<usize> {
        self.vec.binary_search_by_key(&key, key_ref).ok()
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<'s>(&'s self, key: &'_ K) -> Option<&'s V> {
        let index = self.get_index(key)?;
        self.vec.get(index).map(value_ref)
    }

    /// Returns a mutable reference to the value corresponding to the key.
    pub fn get_mut<'s>(&'s mut self, key: &'_ K) -> Option<&'s mut V> {
        let index = self.get_index(key)?;
        self.vec.get_mut(index).map(value_mut)
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.vec.len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    /// Removes a key from the map, returning the value at the key if the key was previously in the map.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.remove_entry(key).map(value)
    }

    /// Removes a key from the map, returning the stored key and value if the key was previously in the map.
    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)> {
        self.get_index(key).map(|index| self.vec.remove(index))
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs (k, v) for which f(&k, &mut v) returns false.
    /// The elements are visited in ascending order.
    pub fn retain(&mut self, mut pred: impl FnMut(&K) -> bool) {
        self.vec.retain(|(k, _)| pred(k));
    }

    /// An iterator visiting all key-value pairs in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.vec.iter().map(|(k, v)| (k, v))
    }

    /// An iterator visiting all keys in ascending order.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.vec.iter().map(key_ref)
    }

    /// An iterator visiting all values in ascending order (ordered by their keys).
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.vec.iter().map(value_ref)
    }

    /// Creates a consuming iterator visiting all key-value pairs in ascending order.
    pub fn into_iter(self) -> impl Iterator<Item = (K, V)> {
        self.vec.into_iter()
    }

    /// Creates a consuming iterator visiting all keys in ascending order.
    pub fn into_keys(self) -> impl Iterator<Item = K> {
        self.vec.into_iter().map(key)
    }

    /// Creates a consuming iterator visiting all values in ascending order (ordered by their keys).
    pub fn into_values(self) -> impl Iterator<Item = V> {
        self.vec.into_iter().map(value)
    }
}

impl<K: Ord, V> Index<&K> for BSMap<K, V> {
    type Output = V;

    fn index(&self, index: &K) -> &Self::Output {
        self.get(index).unwrap()
    }
}
impl<K: Ord, V> IndexMut<&K> for BSMap<K, V> {
    fn index_mut(&mut self, index: &K) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

fn key<K, V>(entry: (K, V)) -> K {
    entry.0
}
fn value<K, V>(entry: (K, V)) -> V {
    entry.1
}

fn key_ref<K, V>(entry: &(K, V)) -> &K {
    &entry.0
}
fn value_ref<K, V>(entry: &(K, V)) -> &V {
    &entry.1
}

fn key_mut<K, V>(entry: &mut (K, V)) -> &mut K {
    &mut entry.0
}
fn value_mut<K, V>(entry: &mut (K, V)) -> &mut V {
    &mut entry.1
}
