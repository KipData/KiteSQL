// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::optimizer::core::cm_sketch::CountMinSketch;
use crate::optimizer::core::histogram::{Histogram, HistogramMeta};
use crate::storage::{StatisticsMetaCache, Transaction};
use crate::types::index::IndexId;
use crate::types::value::DataValue;
use kite_sql_serde_macros::ReferenceSerialization;
use std::slice;

pub struct StatisticMetaLoader<'a, T: Transaction> {
    cache: &'a StatisticsMetaCache,
    tx: &'a T,
}

impl<'a, T: Transaction> StatisticMetaLoader<'a, T> {
    pub fn new(tx: &'a T, cache: &'a StatisticsMetaCache) -> StatisticMetaLoader<'a, T> {
        StatisticMetaLoader { cache, tx }
    }

    pub fn load(
        &self,
        table_name: &TableName,
        index_id: IndexId,
    ) -> Result<Option<&StatisticsMeta>, DatabaseError> {
        let key = (table_name.clone(), index_id);
        if let Some(statistics_meta) = self.cache.get(&key) {
            return Ok(Some(statistics_meta));
        }

        let Some(statistics_meta) = self.tx.statistics_meta(table_name.as_ref(), index_id)? else {
            return Ok(None);
        };
        self.cache.put(key.clone(), statistics_meta);

        Ok(self.cache.get(&key))
    }
}

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct StatisticsMetaRoot {
    index_id: IndexId,
    histogram_meta: HistogramMeta,
    cm_sketch: CountMinSketch<DataValue>,
}

impl StatisticsMetaRoot {
    pub fn new(histogram_meta: HistogramMeta, cm_sketch: CountMinSketch<DataValue>) -> Self {
        Self {
            index_id: histogram_meta.index_id(),
            histogram_meta,
            cm_sketch,
        }
    }

    pub fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub fn histogram_meta(&self) -> &HistogramMeta {
        &self.histogram_meta
    }

    pub fn cm_sketch(&self) -> &CountMinSketch<DataValue> {
        &self.cm_sketch
    }

    pub fn into_parts(self) -> (HistogramMeta, CountMinSketch<DataValue>) {
        (self.histogram_meta, self.cm_sketch)
    }
}

#[derive(Debug, Clone)]
pub struct StatisticsMeta {
    index_id: IndexId,
    histogram: Histogram,
    cm_sketch: CountMinSketch<DataValue>,
}

impl StatisticsMeta {
    pub fn new(histogram: Histogram, cm_sketch: CountMinSketch<DataValue>) -> Self {
        StatisticsMeta {
            index_id: histogram.index_id(),
            histogram,
            cm_sketch,
        }
    }

    pub fn from_parts(
        root: StatisticsMetaRoot,
        buckets: Vec<crate::optimizer::core::histogram::Bucket>,
    ) -> Result<Self, DatabaseError> {
        let (histogram_meta, cm_sketch) = root.into_parts();
        let histogram = Histogram::from_parts(histogram_meta, buckets)?;

        Ok(Self::new(histogram, cm_sketch))
    }

    pub fn into_parts(
        self,
    ) -> (
        StatisticsMetaRoot,
        Vec<crate::optimizer::core::histogram::Bucket>,
    ) {
        let (histogram_meta, buckets) = self.histogram.into_parts();
        (
            StatisticsMetaRoot::new(histogram_meta, self.cm_sketch),
            buckets,
        )
    }

    pub fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    pub fn collect_count(&self, range: &Range) -> Result<usize, DatabaseError> {
        let mut count = 0;

        let ranges = if let Range::SortedRanges(ranges) = range {
            ranges.as_slice()
        } else {
            slice::from_ref(range)
        };
        count += self.histogram.collect_count(ranges, &self.cm_sketch)?;
        Ok(count)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::errors::DatabaseError;
    use crate::optimizer::core::histogram::HistogramBuilder;
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use ulid::Ulid;

    #[test]
    fn test_into_parts_and_from_parts() -> Result<(), DatabaseError> {
        let index = IndexMeta {
            id: 0,
            column_ids: vec![Ulid::new()],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };

        let mut builder = HistogramBuilder::new(&index, Some(15));

        builder.append(&Arc::new(DataValue::Int32(14)))?;
        builder.append(&Arc::new(DataValue::Int32(13)))?;
        builder.append(&Arc::new(DataValue::Int32(12)))?;
        builder.append(&Arc::new(DataValue::Int32(11)))?;
        builder.append(&Arc::new(DataValue::Int32(10)))?;
        builder.append(&Arc::new(DataValue::Int32(4)))?;
        builder.append(&Arc::new(DataValue::Int32(3)))?;
        builder.append(&Arc::new(DataValue::Int32(2)))?;
        builder.append(&Arc::new(DataValue::Int32(1)))?;
        builder.append(&Arc::new(DataValue::Int32(0)))?;
        builder.append(&Arc::new(DataValue::Int32(9)))?;
        builder.append(&Arc::new(DataValue::Int32(8)))?;
        builder.append(&Arc::new(DataValue::Int32(7)))?;
        builder.append(&Arc::new(DataValue::Int32(6)))?;
        builder.append(&Arc::new(DataValue::Int32(5)))?;
        builder.append(&Arc::new(DataValue::Null))?;
        builder.append(&Arc::new(DataValue::Null))?;

        let (histogram, sketch) = builder.build(4)?;
        let meta = StatisticsMeta::new(histogram.clone(), sketch.clone());
        let (root, buckets) = meta.into_parts();
        let statistics_meta = StatisticsMeta::from_parts(root, buckets)?;

        assert_eq!(histogram, statistics_meta.histogram);
        assert_eq!(
            sketch.estimate(&DataValue::Null),
            statistics_meta.cm_sketch.estimate(&DataValue::Null)
        );

        Ok(())
    }
}
