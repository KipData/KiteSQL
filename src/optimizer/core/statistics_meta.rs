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
use crate::optimizer::core::histogram::{Bucket, Histogram, HistogramMeta};
use crate::optimizer::core::top_n::ColumnTopN;
use crate::storage::StatisticsMetaCache;
use crate::types::index::IndexId;
use crate::types::value::DataValue;
use kite_sql_serde_macros::ReferenceSerialization;
use std::slice;

pub struct StatisticMetaLoader<'a> {
    cache: &'a StatisticsMetaCache,
}

impl<'a> StatisticMetaLoader<'a> {
    pub fn new(cache: &'a StatisticsMetaCache) -> StatisticMetaLoader<'a> {
        StatisticMetaLoader { cache }
    }

    pub fn load(
        &self,
        table_name: &TableName,
        index_id: IndexId,
    ) -> Result<Option<&StatisticsMeta>, DatabaseError> {
        let key = (table_name.clone(), index_id);
        Ok(self.cache.get(&key))
    }

    pub fn collect_count(
        &self,
        table_name: &TableName,
        index_id: IndexId,
        range: &Range,
    ) -> Result<Option<usize>, DatabaseError> {
        let Some(entry) = self.load(table_name, index_id)? else {
            return Ok(None);
        };
        let ranges = if let Range::SortedRanges(ranges) = range {
            ranges.as_slice()
        } else {
            slice::from_ref(range)
        };

        entry
            .histogram()
            .collect_count(ranges, entry.sketch(), entry.top_n())
            .map(Some)
    }
}

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct StatisticsMetaRoot {
    index_id: IndexId,
    histogram_meta: HistogramMeta,
}

impl StatisticsMetaRoot {
    pub fn new(histogram_meta: HistogramMeta) -> Self {
        Self {
            index_id: histogram_meta.index_id(),
            histogram_meta,
        }
    }

    pub fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub fn histogram_meta(&self) -> &HistogramMeta {
        &self.histogram_meta
    }

    pub fn into_histogram_meta(self) -> HistogramMeta {
        self.histogram_meta
    }
}

#[derive(Debug, Clone)]
pub struct StatisticsMeta {
    index_id: IndexId,
    histogram: Histogram,
    sketch: CountMinSketch<DataValue>,
    top_n: ColumnTopN,
}

impl StatisticsMeta {
    pub fn new(histogram: Histogram, sketch: CountMinSketch<DataValue>, top_n: ColumnTopN) -> Self {
        StatisticsMeta {
            index_id: histogram.index_id(),
            histogram,
            sketch,
            top_n,
        }
    }

    pub fn from_parts(
        root: StatisticsMetaRoot,
        buckets: Vec<Bucket>,
        sketch: CountMinSketch<DataValue>,
        top_n: Option<ColumnTopN>,
    ) -> Result<Self, DatabaseError> {
        let histogram = Histogram::from_parts(root.into_histogram_meta(), buckets)?;

        Ok(Self::new(histogram, sketch, top_n.unwrap_or_default()))
    }

    pub fn into_parts(
        self,
    ) -> (
        StatisticsMetaRoot,
        Vec<Bucket>,
        CountMinSketch<DataValue>,
        ColumnTopN,
    ) {
        let (histogram_meta, buckets) = self.histogram.into_parts();
        (
            StatisticsMetaRoot::new(histogram_meta),
            buckets,
            self.sketch,
            self.top_n,
        )
    }

    pub fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    pub fn sketch(&self) -> &CountMinSketch<DataValue> {
        &self.sketch
    }

    pub fn top_n(&self) -> &ColumnTopN {
        &self.top_n
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::errors::DatabaseError;
    use crate::optimizer::core::histogram::{HistogramBuilder, ANALYZE_STATISTICS_RELATIVE_ERROR};
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn test_into_parts_and_from_parts() -> Result<(), DatabaseError> {
        let index = IndexMeta {
            id: 0,
            column_ids: vec![1],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };

        let mut builder = HistogramBuilder::new(&index, ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        builder.append(DataValue::Int32(14))?;
        builder.append(DataValue::Int32(13))?;
        builder.append(DataValue::Int32(12))?;
        builder.append(DataValue::Int32(11))?;
        builder.append(DataValue::Int32(10))?;
        builder.append(DataValue::Int32(4))?;
        builder.append(DataValue::Int32(3))?;
        builder.append(DataValue::Int32(2))?;
        builder.append(DataValue::Int32(1))?;
        builder.append(DataValue::Int32(0))?;
        builder.append(DataValue::Int32(9))?;
        builder.append(DataValue::Int32(8))?;
        builder.append(DataValue::Int32(7))?;
        builder.append(DataValue::Int32(6))?;
        builder.append(DataValue::Int32(5))?;
        builder.append(DataValue::Null)?;
        builder.append(DataValue::Null)?;

        let (histogram, sketch, top_n) = builder.build(4)?;
        let expected_estimate = sketch.estimate(&DataValue::Int32(7));
        let expected_top_n_count = top_n.get(&DataValue::Int32(7));
        let meta = StatisticsMeta::new(histogram.clone(), sketch, top_n);
        let (root, buckets, sketch, top_n) = meta.into_parts();
        let statistics_meta = StatisticsMeta::from_parts(root, buckets, sketch, Some(top_n))?;

        assert_eq!(histogram, statistics_meta.histogram);
        assert_eq!(
            expected_estimate,
            statistics_meta.sketch().estimate(&DataValue::Int32(7))
        );
        assert_eq!(
            expected_top_n_count,
            statistics_meta.top_n().get(&DataValue::Int32(7))
        );

        Ok(())
    }
}
