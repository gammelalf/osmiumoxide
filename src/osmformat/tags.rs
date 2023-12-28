use crate::osmformat::DataBlock;

pub(crate) struct Tags<'a> {
    pub(crate) keys: &'a Vec<u32>,
    pub(crate) vals: &'a Vec<u32>,
}

impl<'a> Tags<'a> {
    pub(crate) fn iter(
        &self,
        block: &'a DataBlock,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.keys
            .iter()
            .zip(self.vals.iter())
            .filter_map(|(key, value)| {
                block
                    .get_str(*key as usize)
                    .zip(block.get_str(*value as usize))
            })
    }

    pub(crate) fn keys(&self, block: &'a DataBlock) -> impl Iterator<Item = &'a str> + 'a {
        self.keys
            .iter()
            .filter_map(|key| block.get_str(*key as usize))
    }

    pub(crate) fn values(&self, block: &'a DataBlock) -> impl Iterator<Item = &'a str> + 'a {
        self.vals
            .iter()
            .filter_map(|value| block.get_str(*value as usize))
    }
}
