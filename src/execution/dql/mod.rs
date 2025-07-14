pub(crate) mod aggregate;
pub(crate) mod describe;
pub(crate) mod dummy;
pub(crate) mod explain;
pub(crate) mod filter;
pub(crate) mod function_scan;
pub(crate) mod index_scan;
pub(crate) mod join;
pub(crate) mod limit;
pub(crate) mod projection;
pub(crate) mod seq_scan;
pub(crate) mod show_table;
pub(crate) mod show_view;
pub(crate) mod sort;
pub(crate) mod union;
pub(crate) mod values;

#[cfg(test)]
pub(crate) mod test {
    use crate::types::value::DataValue;
    use itertools::Itertools;

    pub(crate) fn build_integers(ints: Vec<Option<i32>>) -> Vec<DataValue> {
        ints.into_iter()
            .map(|i| i.map(DataValue::Int32).unwrap_or(DataValue::Null))
            .collect_vec()
    }
}
