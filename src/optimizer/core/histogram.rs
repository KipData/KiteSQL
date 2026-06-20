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
        if sketch.error_bound(self.meta.values_len) >= average_count {
            average_count
        } else {
            sketch.estimate(value)
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
        let comparator = self.comparator()?;

        let mut count = 0;
        let mut binary_i = 0;
        let mut bucket_i = 0;
        let mut bucket_idxs = Vec::new();

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
    ) -> Result<bool, DatabaseError> {
        let float_value = |value: &DataValue, prefix_len: usize| {
            let value = match value.logical_type() {
                LogicalType::Varchar(..) | LogicalType::Char(..) => match value {
                    DataValue::Utf8 { value, .. } => {
                        if prefix_len > value.len() {
                            return Ok(0.0);
                        }

                        let mut val = 0u64;
                        for (i, char) in value
                            .get(prefix_len..prefix_len + 8)
                            .unwrap()
                            .chars()
                            .enumerate()
                        {
                            if value.len() - prefix_len > i {
                                val += (val << 8) + char as u64;
                            } else {
                                val += val << 8;
                            }
                        }

                        Some(val as f64)
                    }
                    _ => unreachable!(),
                },
                LogicalType::Date
                | LogicalType::DateTime
                | LogicalType::Time(_)
                | LogicalType::TimeStamp(_, _) => match value {
                    DataValue::Date32(value) => DataValue::Int32(*value)
                        .cast(&LogicalType::Double)?
                        .double(),
                    DataValue::Date64(value) => DataValue::Int64(*value)
                        .cast(&LogicalType::Double)?
                        .double(),
                    DataValue::Time32(value, ..) => DataValue::UInt32(*value)
                        .cast(&LogicalType::Double)?
                        .double(),
                    DataValue::Time64(value, ..) => DataValue::Int64(*value)
                        .cast(&LogicalType::Double)?
                        .double(),
                    _ => unreachable!(),
                },

                LogicalType::SqlNull
                | LogicalType::Boolean
                | LogicalType::Tinyint
                | LogicalType::UTinyint
                | LogicalType::Smallint
                | LogicalType::USmallint
                | LogicalType::Integer
                | LogicalType::UInteger
                | LogicalType::Bigint
                | LogicalType::UBigint
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal(_, _) => value.clone().cast(&LogicalType::Double)?.double(),
                LogicalType::Tuple(_) => match value {
                    DataValue::Tuple(values, _) => {
                        let mut float = 0.0;

                        for (i, value) in values.iter().enumerate() {
                            if !value.logical_type().is_numeric() {
                                continue;
                            }
                            if let Some(f) =
                                DataValue::clone(value).cast(&LogicalType::Double)?.double()
                            {
                                float += f / (10_i32.pow(i as u32) as f64);
                            }
                        }
                        Some(float)
                    }
                    DataValue::Null => None,
                    _ => unreachable!(),
                },
            }
            .unwrap_or(0.0);
            Ok::<f64, DatabaseError>(value)
        };
        let calc_fraction = |start: &DataValue, end: &DataValue, value: &DataValue| {
            let prefix_len = start.common_prefix_length(end).unwrap_or(0);
            Ok::<f64, DatabaseError>(
                (float_value(value, prefix_len)? - float_value(start, prefix_len)?)
                    / (float_value(end, prefix_len)? - float_value(start, prefix_len)?),
            )
        };

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
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
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
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
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
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
                            endpoint_count(val, bucket, sketch),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let (temp_ratio_min, option_min) = match min {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
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
                } else if let Some(count) = top_n.get(value) {
                    count
                } else {
                    self.equal_count(value, sketch)
                };
                *binary_i += 1
            }
            Range::Dummy => return Ok(true),
            Range::SortedRanges(_) => unreachable!(),
        }

        Ok(false)
    }
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
    fn test_eq_count_falls_back_to_hll_when_cm_error_is_large() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for value in 0..10_000 {
            builder.append(DataValue::Int32(value))?;
        }

        let (histogram, mut sketch, _) = builder.build(100)?;
        let top_n = ColumnTopN::default();
        let average_count = histogram.average_count();
        assert!(sketch.error_bound(histogram.values_len()) >= average_count);

        sketch.add(&DataValue::Int32(7), 1000);

        assert_eq!(
            histogram.collect_count(&[Range::Eq(DataValue::Int32(7))], &sketch, &top_n)?,
            average_count
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
