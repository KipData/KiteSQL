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

use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::expression::BinaryOperator;
use crate::optimizer::core::cm_sketch::CountMinSketch;
use crate::optimizer::core::hll::HyperLogLog;
use crate::optimizer::core::kll_sketch::KllSketchBuilder;
use crate::optimizer::core::top_n::{ColumnTopN, ANALYZE_STATISTICS_TOP_N_SIZE};
use crate::types::evaluator::{binary_create, BinaryEvaluatorRef};
use crate::types::index::{IndexId, IndexMeta};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use kite_sql_serde_macros::ReferenceSerialization;
use ordered_float::OrderedFloat;
use std::borrow::Cow;
use std::cmp;
use std::collections::Bound;
use std::mem;
use std::sync::OnceLock;

const ANALYZE_STATISTICS_CONFIDENCE: f64 = 0.95;
pub(crate) const ANALYZE_STATISTICS_RELATIVE_ERROR: f64 = 0.001;

pub struct HistogramBuilder {
    index_id: IndexId,
    null_count: usize,
    values_len: usize,
    quantile: KllSketchBuilder,
    sketch: CountMinSketch<DataValue>,
    hll: HyperLogLog<DataValue>,
    top_n: ColumnTopN,
}

#[derive(Debug)]
struct BoundComparator {
    lt: BinaryEvaluatorRef,
    lte: BinaryEvaluatorRef,
    gt: BinaryEvaluatorRef,
    gte: BinaryEvaluatorRef,
}

#[derive(Debug, Clone, PartialEq, ReferenceSerialization)]
pub struct HistogramMeta {
    index_id: IndexId,
    number_of_distinct_value: usize,
    null_count: usize,
    values_len: usize,
    buckets_len: usize,
    // TODO: How to use?
    // Correlation is the statistical correlation between physical row ordering and logical ordering of
    // the column values
    correlation: f64,
}

// Equal depth histogram
#[derive(Debug)]
pub struct Histogram {
    meta: HistogramMeta,
    buckets: Vec<Bucket>,
    comparator: OnceLock<BoundComparator>,
}

impl Clone for Histogram {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            buckets: self.buckets.clone(),
            comparator: OnceLock::new(),
        }
    }
}

impl PartialEq for Histogram {
    fn eq(&self, other: &Self) -> bool {
        self.meta == other.meta && self.buckets == other.buckets
    }
}

#[derive(Debug, Clone, PartialEq, ReferenceSerialization)]
pub struct Bucket {
    lower: DataValue,
    upper: DataValue,
    count: u64,
    // repeat: u64,
}

impl HistogramBuilder {
    pub fn new(index_meta: &IndexMeta, relative_error: f64) -> Result<Self, DatabaseError> {
        Ok(Self {
            index_id: index_meta.id,
            null_count: 0,
            values_len: 0,
            quantile: KllSketchBuilder::with_relative_error(relative_error)?,
            sketch: CountMinSketch::with_relative_error(
                ANALYZE_STATISTICS_CONFIDENCE,
                relative_error,
            )?,
            hll: HyperLogLog::with_relative_error(relative_error)?,
            top_n: ColumnTopN::default(),
        })
    }

    pub fn append(&mut self, value: DataValue) -> Result<(), DatabaseError> {
        if value.is_null() {
            self.null_count += 1;
        } else {
            self.sketch.increment(&value);
            self.hll.add(&value);
            self.top_n
                .add_with_size(ANALYZE_STATISTICS_TOP_N_SIZE, value.clone(), 1);
            self.quantile.insert(value)?;
            self.values_len += 1;
        }

        Ok(())
    }

    pub fn build(
        self,
        number_of_buckets: usize,
    ) -> Result<(Histogram, CountMinSketch<DataValue>, ColumnTopN), DatabaseError> {
        if number_of_buckets == 0 {
            return Err(DatabaseError::InvalidValue(
                "histogram bucket count must be greater than zero".to_string(),
            ));
        }

        let values_len = self.values_len;
        if number_of_buckets > values_len {
            return Err(DatabaseError::TooManyBuckets(number_of_buckets, values_len));
        }

        let HistogramBuilder {
            index_id,
            null_count,
            quantile,
            mut sketch,
            hll,
            top_n,
            ..
        } = self;
        let mut buckets = Vec::with_capacity(number_of_buckets);
        let quantile = quantile.build()?;
        let correlation = quantile.correlation();
        let ranks = (0..number_of_buckets).flat_map(|i| {
            [
                i * values_len / number_of_buckets,
                ((i + 1) * values_len / number_of_buckets) - 1,
            ]
        });
        let mut values = quantile.values_at_ranks(ranks);

        for i in 0..number_of_buckets {
            let lower_rank = i * values_len / number_of_buckets;
            let upper_rank = ((i + 1) * values_len / number_of_buckets) - 1;
            let lower = values.next().flatten().ok_or_else(|| {
                DatabaseError::InvalidValue("KLL sketch failed to produce bucket lower".to_string())
            })?;
            let upper = values.next().flatten().ok_or_else(|| {
                DatabaseError::InvalidValue("KLL sketch failed to produce bucket upper".to_string())
            })?;
            buckets.push(Bucket {
                lower,
                upper,
                count: (upper_rank - lower_rank + 1) as u64,
            });
        }
        sketch.add(&DataValue::Null, self.null_count);
        let number_of_distinct_value = hll.estimate().clamp(1, values_len);
        let top_n = top_n.finish_with_size(ANALYZE_STATISTICS_TOP_N_SIZE);

        Ok((
            Histogram {
                meta: HistogramMeta {
                    index_id,
                    number_of_distinct_value,
                    null_count,
                    values_len,
                    buckets_len: buckets.len(),
                    correlation,
                },
                buckets,
                comparator: OnceLock::new(),
            },
            sketch,
            top_n,
        ))
    }
}

impl BoundComparator {
    fn new(ty: LogicalType) -> Result<Self, DatabaseError> {
        Ok(Self {
            lt: binary_create(Cow::Borrowed(&ty), BinaryOperator::Lt)?,
            lte: binary_create(Cow::Borrowed(&ty), BinaryOperator::LtEq)?,
            gt: binary_create(Cow::Borrowed(&ty), BinaryOperator::Gt)?,
            gte: binary_create(Cow::Owned(ty), BinaryOperator::GtEq)?,
        })
    }

    fn lt(&self, value: &DataValue, target: &DataValue) -> Result<bool, DatabaseError> {
        Ok(matches!(
            self.lt.binary_eval(value, target)?,
            DataValue::Boolean(true)
        ))
    }

    fn lte(&self, value: &DataValue, target: &DataValue) -> Result<bool, DatabaseError> {
        Ok(matches!(
            self.lte.binary_eval(value, target)?,
            DataValue::Boolean(true)
        ))
    }

    fn gt(&self, value: &DataValue, target: &DataValue) -> Result<bool, DatabaseError> {
        Ok(matches!(
            self.gt.binary_eval(value, target)?,
            DataValue::Boolean(true)
        ))
    }

    fn gte(&self, value: &DataValue, target: &DataValue) -> Result<bool, DatabaseError> {
        Ok(matches!(
            self.gte.binary_eval(value, target)?,
            DataValue::Boolean(true)
        ))
    }
}

fn is_under(
    comparator: &BoundComparator,
    value: &DataValue,
    target: &Bound<DataValue>,
    is_min: bool,
) -> Result<bool, DatabaseError> {
    let _is_under = |value: &DataValue, target: &DataValue, is_min: bool| {
        if is_min {
            comparator.lt(value, target)
        } else {
            comparator.lte(value, target)
        }
    };

    Ok(match target {
        Bound::Included(target) => _is_under(value, target, is_min)?,
        Bound::Excluded(target) => _is_under(value, target, !is_min)?,
        Bound::Unbounded => !is_min,
    })
}

fn is_above(
    comparator: &BoundComparator,
    value: &DataValue,
    target: &Bound<DataValue>,
    is_min: bool,
) -> Result<bool, DatabaseError> {
    let _is_above = |value: &DataValue, target: &DataValue, is_min: bool| {
        if is_min {
            comparator.gte(value, target)
        } else {
            comparator.gt(value, target)
        }
    };
    Ok(match target {
        Bound::Included(target) => _is_above(value, target, is_min)?,
        Bound::Excluded(target) => _is_above(value, target, !is_min)?,
        Bound::Unbounded => is_min,
    })
}

impl Histogram {
    pub fn from_parts(meta: HistogramMeta, buckets: Vec<Bucket>) -> Result<Self, DatabaseError> {
        if meta.buckets_len != buckets.len() {
            return Err(DatabaseError::InvalidValue(format!(
                "histogram bucket count mismatch: meta={}, actual={}",
                meta.buckets_len,
                buckets.len()
            )));
        }

        Ok(Self {
            meta,
            buckets,
            comparator: OnceLock::new(),
        })
    }

    pub fn into_parts(self) -> (HistogramMeta, Vec<Bucket>) {
        (self.meta, self.buckets)
    }

    fn comparator(&self) -> Result<&BoundComparator, DatabaseError> {
        if let Some(comparator) = self.comparator.get() {
            return Ok(comparator);
        }

        let comparator = BoundComparator::new(self.buckets[0].upper.logical_type())?;
        let _ = self.comparator.set(comparator);
        self.comparator
            .get()
            .ok_or(DatabaseError::EvaluatorNotFound)
    }

    pub fn meta(&self) -> &HistogramMeta {
        &self.meta
    }

    pub fn buckets(&self) -> &[Bucket] {
        &self.buckets
    }

    pub fn null_count(&self) -> usize {
        self.meta.null_count
    }

    pub fn correlation(&self) -> f64 {
        self.meta.correlation
    }

    pub fn index_id(&self) -> IndexId {
        self.meta.index_id
    }

    pub fn values_len(&self) -> usize {
        self.meta.values_len
    }

    pub fn distinct_values_len(&self) -> usize {
        self.meta.number_of_distinct_value
    }

    pub fn buckets_len(&self) -> usize {
        self.meta.buckets_len
    }

    fn average_count(&self) -> usize {
        let distinct_values = self.meta.number_of_distinct_value;
        if distinct_values == 0 || self.meta.values_len == 0 {
            return 0;
        }

        cmp::max(
            1,
            (self.meta.values_len as f64 / distinct_values as f64).ceil() as usize,
        )
    }

    fn equal_count(&self, value: &DataValue, sketch: &CountMinSketch<DataValue>) -> usize {
        let average_count = self.average_count();
        if average_count == 0 {
            return 0;
        }

        let estimated_count = sketch.estimate(value);
        let error_bound = sketch.error_bound(self.meta.values_len);
        if error_bound < average_count
            || estimated_count.saturating_sub(error_bound) > average_count
        {
            estimated_count
        } else {
            average_count
        }
    }

    fn top_n_count_or_fallback(
        &self,
        value: &DataValue,
        sketch: &CountMinSketch<DataValue>,
        top_n: &ColumnTopN,
    ) -> usize {
        let Some(entry) = top_n.get_entry(value) else {
            return self.equal_count(value, sketch);
        };
        if entry.error() == 0 || entry.error() < self.average_count() {
            return entry.count();
        }

        let lower = entry.count().saturating_sub(entry.error());
        self.equal_count(value, sketch).clamp(lower, entry.count())
    }

    fn estimate_range_selectivity(&self, range: &Range) -> f64 {
        let Some(bucket) = self.buckets.first() else {
            return 0.0;
        };
        let key_type = bucket.upper.logical_type();
        let distinct = self.distinct_values_len().max(1) as f64;
        match range {
            Range::Dummy => 0.0,
            Range::Eq(value) => DataValue::bound_selectivity(
                Bound::Included(value),
                Bound::Included(value),
                &key_type,
                distinct,
            ),
            Range::Scope { min, max } => {
                DataValue::bound_selectivity(min.as_ref(), max.as_ref(), &key_type, distinct)
            }
            Range::SortedRanges(ranges) => ranges
                .iter()
                .map(|range| self.estimate_range_selectivity(range))
                .sum::<f64>()
                .min(1.0),
        }
    }

    pub fn collect_count(
        &self,
        ranges: &[Range],
        sketch: &CountMinSketch<DataValue>,
        top_n: &ColumnTopN,
    ) -> Result<usize, DatabaseError> {
        if self.buckets.is_empty() || ranges.is_empty() {
            return Ok(0);
        }
        if ranges.iter().any(Range::has_parameter) {
            let selectivity = ranges
                .iter()
                .map(|range| self.estimate_range_selectivity(range))
                .sum::<f64>()
                .min(1.0);
            let count = (self.values_len() as f64 * selectivity).ceil() as usize;
            return Ok(if selectivity > 0.0 && self.values_len() > 0 {
                count.max(1)
            } else {
                0
            });
        }
        let comparator = self.comparator()?;

        let mut count = 0;
        let mut binary_i = 0;
        let mut bucket_i = 0;
        let mut bucket_idxs = Vec::new();
        let mut buf = Vec::new();

        while bucket_i < self.buckets.len() && binary_i < ranges.len() {
            let is_dummy = self._collect_count(
                ranges,
                &mut binary_i,
                &mut bucket_i,
                &mut bucket_idxs,
                &mut count,
                sketch,
                top_n,
                comparator,
                &mut buf,
            )?;
            if is_dummy {
                return Ok(0);
            }
        }

        Ok(bucket_idxs
            .iter()
            .map(|idx| self.buckets[*idx].count as usize)
            .sum::<usize>()
            + count)
    }

    #[allow(clippy::too_many_arguments)]
    fn _collect_count(
        &self,
        ranges: &[Range],
        binary_i: &mut usize,
        bucket_i: &mut usize,
        bucket_idxs: &mut Vec<usize>,
        count: &mut usize,
        sketch: &CountMinSketch<DataValue>,
        top_n: &ColumnTopN,
        comparator: &BoundComparator,
        buf: &mut Vec<u8>,
    ) -> Result<bool, DatabaseError> {
        let distinct_1 = OrderedFloat(1.0 / self.meta.number_of_distinct_value as f64);

        match &ranges[*binary_i] {
            Range::Scope { min, max } => {
                let bucket = &self.buckets[*bucket_i];
                let mut bucket_count = bucket.count as usize;
                if *bucket_i == 0 && scope_lower_includes_null(min) {
                    bucket_count += self.meta.null_count;
                }

                let mut temp_count = 0;

                let is_eq = |value: &DataValue, target: &Bound<DataValue>| match target {
                    Bound::Included(target) => target.eq(value),
                    _ => false,
                };

                if (is_above(comparator, &bucket.lower, min, true)? || is_eq(&bucket.lower, min))
                    && (is_under(comparator, &bucket.upper, max, false)?
                        || is_eq(&bucket.upper, max))
                {
                    bucket_idxs.push(mem::replace(bucket_i, *bucket_i + 1));
                } else if is_above(comparator, &bucket.lower, max, false)? {
                    *binary_i += 1;
                } else if is_under(comparator, &bucket.upper, min, true)? {
                    *bucket_i += 1;
                } else if is_above(comparator, &bucket.lower, min, true)? {
                    let (temp_ratio, option) = match max {
                        Bound::Included(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            None,
                        ),
                        Bound::Excluded(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            endpoint_count(val, bucket, sketch),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1.max(OrderedFloat(temp_ratio).min(OrderedFloat(1.0)));
                    temp_count += (bucket_count as f64 * ratio).ceil() as usize;
                    if let Some(count) = option {
                        temp_count = subtract_endpoint_count(temp_count, count);
                    }
                    *bucket_i += 1;
                } else if is_under(comparator, &bucket.upper, max, false)? {
                    let (temp_ratio, option) = match min {
                        Bound::Included(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            None,
                        ),
                        Bound::Excluded(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            endpoint_count(val, bucket, sketch),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1.max(OrderedFloat(temp_ratio).min(OrderedFloat(1.0)));
                    temp_count += (bucket_count as f64 * (1.0 - ratio)).ceil() as usize;
                    if let Some(count) = option {
                        temp_count = subtract_endpoint_count(temp_count, count);
                    }
                    *bucket_i += 1;
                } else {
                    let (temp_ratio_max, option_max) = match max {
                        Bound::Included(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            None,
                        ),
                        Bound::Excluded(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            endpoint_count(val, bucket, sketch),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let (temp_ratio_min, option_min) = match min {
                        Bound::Included(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            None,
                        ),
                        Bound::Excluded(val) => (
                            encoded_fraction(&bucket.lower, &bucket.upper, val, buf)?,
                            endpoint_count(val, bucket, sketch),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1
                        .max(OrderedFloat(temp_ratio_max - temp_ratio_min).min(OrderedFloat(1.0)));
                    temp_count += (bucket_count as f64 * ratio).ceil() as usize;
                    if let Some(count) = option_max {
                        temp_count = subtract_endpoint_count(temp_count, count);
                    }
                    if let Some(count) = option_min {
                        temp_count = subtract_endpoint_count(temp_count, count);
                    }
                    *binary_i += 1;
                }
                *count += cmp::max(temp_count, 0);
            }
            Range::Eq(value) => {
                *count += if value.is_null() {
                    self.meta.null_count
                } else {
                    self.top_n_count_or_fallback(value, sketch, top_n)
                };
                *binary_i += 1
            }
            Range::Dummy => return Ok(true),
            Range::SortedRanges(_) => unreachable!(),
        }

        Ok(false)
    }
}

fn encoded_fraction(
    start: &DataValue,
    end: &DataValue,
    value: &DataValue,
    buf: &mut Vec<u8>,
) -> Result<f64, DatabaseError> {
    buf.clear();
    start.memcomparable_encode(buf)?;
    let lower_end = buf.len();
    end.memcomparable_encode(buf)?;
    let upper_end = buf.len();
    value.memcomparable_encode(buf)?;
    let lower = &buf[..lower_end];
    let upper = &buf[lower_end..upper_end];
    let key = &buf[upper_end..];
    if key <= lower {
        return Ok(0.0);
    }
    if key >= upper {
        return Ok(1.0);
    }
    let prefix = lower.iter().zip(upper).take_while(|(a, b)| a == b).count();
    // Subtract eight-byte integer coordinates before converting to f64, so
    // nearby large keys retain their distance. Encoded distance is approximate.
    let coordinate = |bytes: &[u8]| {
        (0..8).fold(0u64, |value, i| {
            (value << 8) | u64::from(bytes.get(prefix + i).copied().unwrap_or(0))
        })
    };
    let lower = coordinate(lower);
    let upper = coordinate(upper);
    Ok((coordinate(key) - lower) as f64 / (upper - lower) as f64)
}

fn subtract_endpoint_count(count: usize, endpoint_count: usize) -> usize {
    if endpoint_count < count {
        count.saturating_sub(endpoint_count)
    } else if endpoint_count == count && count > 1 {
        count - 1
    } else {
        count
    }
}

fn endpoint_count(
    value: &DataValue,
    bucket: &Bucket,
    sketch: &CountMinSketch<DataValue>,
) -> Option<usize> {
    let bucket_key_type = bucket.lower.logical_type();
    debug_assert_eq!(bucket_key_type, bucket.upper.logical_type());

    if value.logical_type() == bucket_key_type {
        match value {
            DataValue::Tuple(values, true) => {
                Some(sketch.estimate(&DataValue::Tuple(values.clone(), false)))
            }
            _ => Some(sketch.estimate(value)),
        }
    } else {
        None
    }
}

fn scope_lower_includes_null(min: &Bound<DataValue>) -> bool {
    match min {
        Bound::Unbounded => true,
        Bound::Included(value) => value.is_null(),
        Bound::Excluded(_) => false,
    }
}

impl HistogramMeta {
    pub fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub fn values_len(&self) -> usize {
        self.values_len
    }

    pub fn distinct_values_len(&self) -> usize {
        self.number_of_distinct_value
    }

    pub fn buckets_len(&self) -> usize {
        self.buckets_len
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::optimizer::core::cm_sketch::CountMinSketch;
    use crate::optimizer::core::histogram::{
        Bucket, HistogramBuilder, ANALYZE_STATISTICS_RELATIVE_ERROR,
    };
    use crate::optimizer::core::top_n::ColumnTopN;
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;

    fn index_meta() -> IndexMeta {
        IndexMeta {
            id: 0,
            column_ids: vec![1],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        }
    }

    #[test]
    fn test_sort_tuples_on_histogram() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        builder.append(DataValue::Int32(0))?;
        builder.append(DataValue::Int32(1))?;
        builder.append(DataValue::Int32(2))?;
        builder.append(DataValue::Int32(3))?;
        builder.append(DataValue::Int32(4))?;

        builder.append(DataValue::Int32(5))?;
        builder.append(DataValue::Int32(6))?;
        builder.append(DataValue::Int32(7))?;
        builder.append(DataValue::Int32(8))?;
        builder.append(DataValue::Int32(9))?;

        builder.append(DataValue::Int32(10))?;
        builder.append(DataValue::Int32(11))?;
        builder.append(DataValue::Int32(12))?;
        builder.append(DataValue::Int32(13))?;
        builder.append(DataValue::Int32(14))?;

        builder.append(DataValue::Null)?;
        builder.append(DataValue::Null)?;

        // assert!(matches!(builder.build(10), Err(DataBaseError::TooManyBuckets)));

        let (histogram, _, _) = builder.build(5)?;

        assert_eq!(histogram.correlation(), 1.0);
        assert_eq!(histogram.null_count(), 2);
        assert_eq!(histogram.buckets().to_vec().len(), 5);
        assert_eq!(
            histogram.buckets().to_vec(),
            vec![
                Bucket {
                    lower: DataValue::Int32(0),
                    upper: DataValue::Int32(2),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(3),
                    upper: DataValue::Int32(5),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(6),
                    upper: DataValue::Int32(8),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(9),
                    upper: DataValue::Int32(11),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(12),
                    upper: DataValue::Int32(14),
                    count: 3,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_rev_sort_tuples_on_histogram() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        builder.append(DataValue::Int32(14))?;
        builder.append(DataValue::Int32(13))?;
        builder.append(DataValue::Int32(12))?;
        builder.append(DataValue::Int32(11))?;
        builder.append(DataValue::Int32(10))?;

        builder.append(DataValue::Int32(9))?;
        builder.append(DataValue::Int32(8))?;
        builder.append(DataValue::Int32(7))?;
        builder.append(DataValue::Int32(6))?;
        builder.append(DataValue::Int32(5))?;

        builder.append(DataValue::Int32(4))?;
        builder.append(DataValue::Int32(3))?;
        builder.append(DataValue::Int32(2))?;
        builder.append(DataValue::Int32(1))?;
        builder.append(DataValue::Int32(0))?;

        builder.append(DataValue::Null)?;
        builder.append(DataValue::Null)?;

        let (histogram, _, _) = builder.build(5)?;

        assert_eq!(histogram.correlation(), -1.0);
        assert_eq!(histogram.null_count(), 2);
        assert_eq!(histogram.buckets().to_vec().len(), 5);
        assert_eq!(
            histogram.buckets().to_vec(),
            vec![
                Bucket {
                    lower: DataValue::Int32(0),
                    upper: DataValue::Int32(2),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(3),
                    upper: DataValue::Int32(5),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(6),
                    upper: DataValue::Int32(8),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(9),
                    upper: DataValue::Int32(11),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(12),
                    upper: DataValue::Int32(14),
                    count: 3,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_non_average_on_histogram() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

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

        let (histogram, _, _) = builder.build(4)?;

        assert!(histogram.correlation() < 0.0);
        assert_eq!(histogram.null_count(), 2);
        assert_eq!(histogram.buckets().to_vec().len(), 4);
        assert_eq!(
            histogram.buckets().to_vec(),
            vec![
                Bucket {
                    lower: DataValue::Int32(0),
                    upper: DataValue::Int32(2),
                    count: 3,
                },
                Bucket {
                    lower: DataValue::Int32(3),
                    upper: DataValue::Int32(6),
                    count: 4,
                },
                Bucket {
                    lower: DataValue::Int32(7),
                    upper: DataValue::Int32(10),
                    count: 4,
                },
                Bucket {
                    lower: DataValue::Int32(11),
                    upper: DataValue::Int32(14),
                    count: 4,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn parameterized_tuple_ranges_accumulate_prefix_selectivity() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;
        for value in 0..100 {
            builder.append(DataValue::Tuple(vec![DataValue::Int32(value); 4], false))?;
        }
        let (mut histogram, sketch, top_n) = builder.build(10)?;
        // Fixed metadata isolates the heuristic from HLL estimation error.
        histogram.meta.values_len = 10_000;
        histogram.meta.number_of_distinct_value = 10_000;
        let parameter = |id| DataValue::Parameter {
            id,
            ty: crate::types::LogicalType::Integer,
        };
        let prefix = vec![parameter(1), parameter(2)];
        let scope = |lower, upper| Range::Scope {
            min: Bound::Excluded(DataValue::Tuple(lower, false)),
            max: Bound::Excluded(DataValue::Tuple(upper, true)),
        };
        assert_eq!(
            histogram.collect_count(&[scope(prefix.clone(), prefix.clone())], &sketch, &top_n)?,
            100
        );
        let longer = vec![parameter(1), parameter(2), parameter(3)];
        assert_eq!(
            histogram.collect_count(&[scope(longer.clone(), longer)], &sketch, &top_n)?,
            10
        );
        let mut lower = prefix.clone();
        lower.push(parameter(3));
        let mut upper = prefix.clone();
        upper.push(parameter(4));
        assert_eq!(
            histogram.collect_count(&[scope(lower.clone(), prefix)], &sketch, &top_n)?,
            25
        );
        assert_eq!(
            histogram.collect_count(&[scope(lower.clone(), upper.clone())], &sketch, &top_n)?,
            2
        );
        let full = DataValue::Tuple(
            vec![parameter(1), parameter(2), parameter(3), parameter(4)],
            false,
        );
        assert_eq!(
            histogram.collect_count(&[Range::Eq(full.clone())], &sketch, &top_n)?,
            1
        );
        assert_eq!(
            histogram.collect_count(
                &[Range::Scope {
                    min: Bound::Excluded(full.clone()),
                    max: Bound::Included(full),
                }],
                &sketch,
                &top_n
            )?,
            0
        );
        // Equal trailing elements after the first differing element are not a prefix.
        lower.push(parameter(5));
        upper.push(parameter(5));
        assert_eq!(
            histogram.collect_count(&[scope(lower, upper)], &sketch, &top_n)?,
            2
        );
        histogram.buckets[0].upper = DataValue::Tuple(
            vec![
                DataValue::Tuple(vec![DataValue::Int32(0); 2], false),
                DataValue::Tuple(vec![DataValue::Int32(0); 2], false),
            ],
            false,
        );
        let nested = vec![DataValue::Tuple(vec![parameter(1), parameter(2)], false)];
        assert_eq!(
            histogram.collect_count(&[scope(nested.clone(), nested)], &sketch, &top_n)?,
            100
        );
        Ok(())
    }

    #[test]
    fn encoded_fraction_expected_ratios_by_type() -> Result<(), DatabaseError> {
        use crate::types::value::Utf8Type;
        use crate::types::CharLengthUnits;

        #[allow(unused_mut)]
        let mut cases: Vec<(&str, Vec<DataValue>)> = vec![
            ("null", vec![DataValue::Null]),
            (
                "boolean",
                vec![DataValue::Boolean(false), DataValue::Boolean(true)],
            ),
            ("i8", [-100, -50, 0, 50, 100].map(DataValue::Int8).to_vec()),
            (
                "i16",
                [-100, -50, 0, 50, 100].map(DataValue::Int16).to_vec(),
            ),
            (
                "i32",
                [-100, -50, 0, 50, 100].map(DataValue::Int32).to_vec(),
            ),
            (
                "i64",
                [-100, -50, 0, 50, 100].map(DataValue::Int64).to_vec(),
            ),
            ("u8", [0, 50, 100, 150, 200].map(DataValue::UInt8).to_vec()),
            (
                "u16",
                [0, 50, 100, 150, 200].map(DataValue::UInt16).to_vec(),
            ),
            (
                "u32",
                [0, 50, 100, 150, 200].map(DataValue::UInt32).to_vec(),
            ),
            (
                "u64",
                [0, 50, 100, 150, 200].map(DataValue::UInt64).to_vec(),
            ),
            (
                "f32",
                [1.0, 1.25, 1.5, 1.75, 2.0]
                    .map(|v| DataValue::Float32(v.into()))
                    .to_vec(),
            ),
            (
                "f64",
                [1.0, 1.25, 1.5, 1.75, 2.0]
                    .map(|v| DataValue::Float64(v.into()))
                    .to_vec(),
            ),
            (
                "date",
                [0, 50, 100, 150, 200].map(DataValue::Date32).to_vec(),
            ),
            (
                "datetime",
                [0, 50, 100, 150, 200].map(DataValue::Date64).to_vec(),
            ),
            (
                "time32",
                [0, 50, 100, 150, 200]
                    .map(|v| DataValue::Time32(v, 0))
                    .to_vec(),
            ),
            (
                "time64",
                [0, 50, 100, 150, 200]
                    .map(|v| DataValue::Time64(v, 6, false))
                    .to_vec(),
            ),
            (
                "timestamp",
                [0, 50, 100, 150, 200]
                    .map(|v| DataValue::Time64(v, 6, true))
                    .to_vec(),
            ),
            (
                "varchar",
                ["a", "b", "c", "d", "e"]
                    .map(|v| DataValue::from(v.to_string()))
                    .to_vec(),
            ),
            (
                "char",
                ["a", "b", "c", "d", "e"]
                    .map(|v| DataValue::Utf8 {
                        value: v.into(),
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Characters,
                    })
                    .to_vec(),
            ),
            (
                "unicode",
                ["一", "丁", "丂", "七", "丄"]
                    .map(|v| DataValue::from(v.to_string()))
                    .to_vec(),
            ),
            (
                "long string",
                ["a", "b", "c", "d", "e"]
                    .map(|v| DataValue::from(format!("{}{}", "prefix".repeat(20), v)))
                    .to_vec(),
            ),
            (
                "tuple",
                [0, 50, 100, 150, 200]
                    .map(|v| {
                        DataValue::Tuple(vec![DataValue::Int32(1), DataValue::Int32(v)], false)
                    })
                    .to_vec(),
            ),
            (
                "nested tuple",
                [0, 50, 100, 150, 200]
                    .map(|v| {
                        DataValue::Tuple(
                            vec![DataValue::Tuple(vec![DataValue::Int32(v)], false)],
                            false,
                        )
                    })
                    .to_vec(),
            ),
            (
                "i64 max",
                (0..5).map(|v| DataValue::Int64(i64::MAX - 4 + v)).collect(),
            ),
            (
                "u64 max",
                (0..5)
                    .map(|v| DataValue::UInt64(u64::MAX - 4 + v))
                    .collect(),
            ),
        ];
        #[cfg(feature = "decimal")]
        cases.push((
            "decimal",
            [100, 125, 150, 175, 200]
                .map(|v| DataValue::Decimal(rust_decimal::Decimal::new(v, 2)))
                .to_vec(),
        ));
        let mut buf = Vec::new();
        for (name, values) in &cases {
            let expected: &[f64] = match *name {
                "null" => &[0.0],
                "boolean" => &[0.0, 1.0],
                // Accepted encoded-space ratios, not numeric-space quartiles.
                "decimal" => &[0.0, 0.59765625, 0.6953125, 0.79296875, 1.0],
                _ => &[0.0, 0.25, 0.5, 0.75, 1.0],
            };
            let lower = values.first().unwrap();
            let upper = values.last().unwrap();
            let fractions = values
                .iter()
                .map(|value| super::encoded_fraction(lower, upper, value, &mut buf))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(fractions.len(), expected.len(), "{name}");
            for (i, (actual, expected)) in fractions.iter().zip(expected).enumerate() {
                assert!(
                    (actual - expected).abs() <= 1e-12,
                    "{name}[{i}]: expected {expected}, got {actual}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn tuple_interpolation_uses_encoded_order_and_prefix_markers() -> Result<(), DatabaseError> {
        let tuple = |a, b| DataValue::Tuple(vec![DataValue::Int32(a), DataValue::Int32(b)], false);
        let mut buf = Vec::new();
        let fraction =
            super::encoded_fraction(&tuple(1, 0), &tuple(2, 0), &tuple(1, 100), &mut buf)?;
        assert!((fraction - 100.0 / 256.0_f64.powi(5)).abs() <= 1e-20);
        let lower = DataValue::Tuple(vec![DataValue::Int32(1)], false);
        let upper = DataValue::Tuple(vec![DataValue::Int32(1)], true);
        assert_eq!(
            super::encoded_fraction(&tuple(1, 0), &tuple(2, 0), &lower, &mut buf)?,
            0.0
        );
        assert_eq!(
            super::encoded_fraction(&tuple(1, 0), &tuple(2, 0), &upper, &mut buf)?,
            0.994140625
        );
        let string_tuple = |s: &str| {
            DataValue::Tuple(
                vec![DataValue::Int32(1), DataValue::from(s.to_string())],
                false,
            )
        };
        let fraction = super::encoded_fraction(
            &string_tuple("Alice"),
            &string_tuple("Zoe"),
            &string_tuple("Bob"),
            &mut buf,
        )?;
        assert!((fraction - 0.04044539011559242).abs() <= 1e-12);
        Ok(())
    }

    #[test]
    fn parameterized_range_count_uses_shape_and_ndv() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;
        for value in 0..1_000 {
            builder.append(DataValue::Int32(value % 100))?;
        }
        let (histogram, sketch, top_n) = builder.build(10)?;
        let parameter = |id| DataValue::Parameter {
            id,
            ty: crate::types::LogicalType::Integer,
        };
        for (range, expected) in [
            (
                Range::Eq(parameter(1)),
                1_000usize.div_ceil(histogram.distinct_values_len()),
            ),
            (
                Range::Scope {
                    min: Bound::Included(parameter(1)),
                    max: Bound::Unbounded,
                },
                250,
            ),
            (
                Range::Scope {
                    min: Bound::Included(parameter(1)),
                    max: Bound::Excluded(parameter(2)),
                },
                16,
            ),
            (
                Range::SortedRanges(vec![Range::Eq(parameter(1)), Range::Eq(parameter(2))]),
                2 * 1_000usize.div_ceil(histogram.distinct_values_len()),
            ),
        ] {
            assert_eq!(
                histogram.collect_count(&[range], &sketch, &top_n)?,
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn test_collect_count() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

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

        let (histogram, sketch, _) = builder.build(4)?;
        let top_n = ColumnTopN::default();

        let count_1 = histogram.collect_count(
            &[
                Range::Eq(DataValue::Int32(2)),
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(4)),
                    max: Bound::Excluded(DataValue::Int32(12)),
                },
            ],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_1, 9);

        let count_2 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Included(DataValue::Int32(4)),
                max: Bound::Unbounded,
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_2, 11);

        let count_3 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Excluded(DataValue::Int32(7)),
                max: Bound::Unbounded,
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_3, 7);

        let count_4 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(DataValue::Int32(11)),
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_4, 12);

        let count_5 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(8)),
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_5, 8);

        let count_6 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Included(DataValue::Int32(2)),
                max: Bound::Unbounded,
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_6, 12);

        let count_7 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Excluded(DataValue::Int32(1)),
                max: Bound::Unbounded,
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_7, 13);

        let count_8 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(DataValue::Int32(12)),
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_8, 13);

        let count_9 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(13)),
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_9, 13);

        let count_10 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Excluded(DataValue::Int32(3)),
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_10, 2);

        let count_11 = histogram.collect_count(
            &[Range::Scope {
                min: Bound::Included(DataValue::Int32(1)),
                max: Bound::Included(DataValue::Int32(2)),
            }],
            &sketch,
            &top_n,
        )?;

        assert_eq!(count_11, 2);

        Ok(())
    }

    #[test]
    fn test_builder_uses_hll_for_distinct_values() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for _ in 0..50 {
            builder.append(DataValue::Int32(1))?;
            builder.append(DataValue::Int32(2))?;
        }

        let (histogram, _, top_n) = builder.build(10)?;

        assert_eq!(histogram.values_len(), 100);
        assert_eq!(histogram.distinct_values_len(), 2);
        assert_eq!(top_n.get(&DataValue::Int32(1)), Some(50));
        assert_eq!(top_n.get(&DataValue::Int32(2)), Some(50));

        Ok(())
    }

    #[test]
    fn test_eq_count_uses_cm_sketch_when_error_is_small() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for _ in 0..70 {
            builder.append(DataValue::Int32(1))?;
        }
        for _ in 0..30 {
            builder.append(DataValue::Int32(2))?;
        }

        let (histogram, sketch, _) = builder.build(10)?;
        let top_n = ColumnTopN::default();

        assert_eq!(
            histogram.collect_count(&[Range::Eq(DataValue::Int32(1))], &sketch, &top_n)?,
            70
        );

        Ok(())
    }

    #[test]
    fn test_eq_count_uses_top_n_when_value_is_cached() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for _ in 0..70 {
            builder.append(DataValue::Int32(1))?;
        }
        for _ in 0..30 {
            builder.append(DataValue::Int32(2))?;
        }

        let (histogram, mut sketch, top_n) = builder.build(10)?;
        sketch.add(&DataValue::Int32(1), 1000);

        assert_eq!(
            histogram.collect_count(&[Range::Eq(DataValue::Int32(1))], &sketch, &top_n)?,
            70
        );

        Ok(())
    }

    #[test]
    fn test_eq_count_falls_back_to_average_when_cm_error_is_large() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for value in 0..10_000 {
            builder.append(DataValue::Int32(value))?;
        }

        let (histogram, sketch, _) = builder.build(100)?;
        let top_n = ColumnTopN::default();
        let average_count = histogram.average_count();
        let error_bound = sketch.error_bound(histogram.values_len());
        let estimated_count = sketch.estimate(&DataValue::Int32(7));
        assert!(error_bound >= average_count);
        assert!(estimated_count.saturating_sub(error_bound) <= average_count);

        assert_eq!(
            histogram.collect_count(&[Range::Eq(DataValue::Int32(7))], &sketch, &top_n)?,
            average_count
        );

        Ok(())
    }

    #[test]
    fn test_eq_count_uses_cm_sketch_for_clear_heavy_hitter() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for _ in 0..1_000 {
            builder.append(DataValue::Int32(7))?;
        }
        for value in 0..9_000 {
            builder.append(DataValue::Int32(10_000 + value))?;
        }

        let (histogram, sketch, _) = builder.build(100)?;
        let top_n = ColumnTopN::default();
        let average_count = histogram.average_count();
        let error_bound = sketch.error_bound(histogram.values_len());
        let estimated_count = sketch.estimate(&DataValue::Int32(7));
        assert!(error_bound >= average_count);
        assert!(estimated_count.saturating_sub(error_bound) > average_count);

        assert_eq!(
            histogram.collect_count(&[Range::Eq(DataValue::Int32(7))], &sketch, &top_n)?,
            estimated_count
        );

        Ok(())
    }

    #[test]
    fn test_eq_count_falls_back_when_top_n_error_is_large() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for value in 0..10_000 {
            builder.append(DataValue::Int32(value))?;
        }

        let (histogram, sketch, _) = builder.build(100)?;

        let mut top_n = ColumnTopN::default();
        top_n.add_with_size(1, DataValue::Int32(1), 10);
        top_n.add_with_size(1, DataValue::Int32(7), 1);
        let entry = top_n.get_entry(&DataValue::Int32(7)).unwrap();
        assert_eq!(entry.count(), 11);
        assert_eq!(entry.error(), 10);

        let fallback = histogram.equal_count(&DataValue::Int32(7), &sketch);
        let lower = entry.count().saturating_sub(entry.error());
        let expected = fallback.clamp(lower, entry.count());
        assert!(expected < entry.count());

        assert_eq!(
            histogram.collect_count(&[Range::Eq(DataValue::Int32(7))], &sketch, &top_n)?,
            expected
        );

        Ok(())
    }

    #[test]
    fn test_collect_count_ignores_tuple_prefix_endpoint_count() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for value in 0..15 {
            builder.append(DataValue::Tuple(
                vec![DataValue::Int32(value), DataValue::Int32(value)],
                false,
            ))?;
        }

        let (histogram, mut sketch, top_n) = builder.build(5)?;
        let ranges = [Range::Scope {
            min: Bound::Excluded(DataValue::Tuple(vec![DataValue::Int32(0)], false)),
            max: Bound::Excluded(DataValue::Tuple(vec![DataValue::Int32(8)], true)),
        }];
        let clean_count = histogram.collect_count(&ranges, &sketch, &top_n)?;

        sketch.increment(&DataValue::Tuple(vec![DataValue::Int32(0)], false));
        sketch.increment(&DataValue::Tuple(vec![DataValue::Int32(8)], true));

        assert_eq!(
            histogram.collect_count(&ranges, &sketch, &top_n)?,
            clean_count
        );

        Ok(())
    }

    #[test]
    fn test_endpoint_count_uses_only_full_histogram_keys() -> Result<(), DatabaseError> {
        let bucket = Bucket {
            lower: DataValue::Tuple(vec![DataValue::Int32(0), DataValue::Int32(0)], false),
            upper: DataValue::Tuple(vec![DataValue::Int32(10), DataValue::Int32(10)], false),
            count: 11,
        };
        let mut sketch = CountMinSketch::with_relative_error(
            super::ANALYZE_STATISTICS_CONFIDENCE,
            ANALYZE_STATISTICS_RELATIVE_ERROR,
        )?;

        let real_key = DataValue::Tuple(vec![DataValue::Int32(8), DataValue::Int32(8)], false);
        let upper_bound = DataValue::Tuple(vec![DataValue::Int32(8), DataValue::Int32(8)], true);
        let prefix_bound = DataValue::Tuple(vec![DataValue::Int32(8)], true);

        sketch.increment(&real_key);
        sketch.increment(&prefix_bound);

        assert_eq!(
            super::endpoint_count(&upper_bound, &bucket, &sketch),
            Some(1)
        );
        assert_eq!(super::endpoint_count(&prefix_bound, &bucket, &sketch), None);

        Ok(())
    }
}
