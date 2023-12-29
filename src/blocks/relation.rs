use crate::blocks::tags::Tags;
use crate::blocks::DataBlock;
use crate::proto;
pub use crate::proto::relation::MemberType;
use crate::util::iter::IteratorExt;

impl DataBlock {
    /// Iterate over the block's [`Relation`]s
    pub fn iter_relations(&self) -> impl Iterator<Item = Relation<'_>> + '_ {
        self.0.primitivegroup.iter().flat_map(|group| {
            group.relations.iter().map(|relation| Relation {
                block: self,
                relation,
                tags: Tags {
                    keys: &relation.keys,
                    vals: &relation.vals,
                },
            })
        })
    }
}

/// An OSM relation
pub struct Relation<'a> {
    block: &'a DataBlock,
    relation: &'a proto::Relation,
    tags: Tags<'a>,
}

impl<'a> Relation<'a> {
    /// The relation's id
    pub fn id(&self) -> i64 {
        self.relation.id
    }

    /// Iterate over the relation's tags as key-value pairs
    pub fn tags(&self) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.tags.iter(&self.block)
    }

    /// Iterate over the relation's tags' keys
    pub fn keys(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.tags.keys(&self.block)
    }

    /// Iterate over the relation's tags' keys
    pub fn values(&self) -> impl Iterator<Item = &'a str> + 'a {
        self.tags.values(&self.block)
    }

    /// Iterate over the way's nodes' ids
    pub fn members(&self) -> impl Iterator<Item = Member<'a>> + 'a {
        self.relation
            .memids
            .iter()
            .copied()
            .decode_delta()
            .zip(self.relation.types.iter())
            .zip(self.relation.roles_sid.iter())
            .filter_map(|((id, r#type), role)| {
                Some(Member {
                    id,
                    r#type: MemberType::try_from(*r#type).ok()?,
                    role: self.block.get_str(*role as usize)?,
                })
            })
    }
}

/// A [`Relation`]'s member
pub struct Member<'a> {
    /// The member's id
    pub id: i64,

    /// The member's type i.e node, way or relation
    pub r#type: MemberType,

    /// The member's role
    pub role: &'a str,
}
