use crate::errors::DatabaseError;
use crate::execution::dql::sort::{radix_sort, BumpVec, NullableVec};
use crate::expression::range_detacher::Range;
use crate::expression::BinaryOperator;
use crate::optimizer::core::cm_sketch::CountMinSketch;
use crate::storage::table_codec::BumpBytes;
use crate::types::evaluator::EvaluatorFactory;
use crate::types::index::{IndexId, IndexMeta};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use bumpalo::Bump;
use kite_sql_serde_macros::ReferenceSerialization;
use ordered_float::OrderedFloat;
use std::collections::Bound;
use std::{cmp, mem};

pub struct HistogramBuilder {
    arena: Bump,
    index_id: IndexId,
    capacity: Option<usize>,
    is_init: bool,

    null_count: usize,
    values: Option<NullableVec<'static, (usize, DataValue)>>,
    sort_keys: Option<BumpVec<'static, (usize, BumpBytes<'static>)>>,

    value_index: usize,
}

// Equal depth histogram
#[derive(Debug, Clone, PartialEq, ReferenceSerialization)]
pub struct Histogram {
    index_id: IndexId,

    number_of_distinct_value: usize,
    null_count: usize,
    values_len: usize,

    buckets: Vec<Bucket>,
    // TODO: How to use?
    // Correlation is the statistical correlation between physical row ordering and logical ordering of
    // the column values
    correlation: f64,
}

#[derive(Debug, Clone, PartialEq, ReferenceSerialization)]
struct Bucket {
    lower: DataValue,
    upper: DataValue,
    count: u64,
    // repeat: u64,
}

impl HistogramBuilder {
    #[allow(clippy::missing_transmute_annotations)]
    pub(crate) fn init(&mut self) {
        if self.is_init {
            return;
        }
        let (values, sort_keys) = self
            .capacity
            .map(|capacity| {
                (
                    NullableVec::<(usize, DataValue)>::with_capacity(capacity, &self.arena),
                    BumpVec::<(usize, BumpBytes<'static>)>::with_capacity_in(capacity, &self.arena),
                )
            })
            .unwrap_or_else(|| (NullableVec::new(&self.arena), BumpVec::new_in(&self.arena)));

        self.values = Some(unsafe { mem::transmute::<_, _>(values) });
        self.sort_keys = Some(unsafe { mem::transmute::<_, _>(sort_keys) });
        self.is_init = true;
    }

    pub fn new(index_meta: &IndexMeta, capacity: Option<usize>) -> Self {
        Self {
            arena: Default::default(),
            index_id: index_meta.id,
            capacity,
            is_init: false,
            null_count: 0,
            values: None,
            sort_keys: None,
            value_index: 0,
        }
    }

    #[allow(clippy::missing_transmute_annotations)]
    pub fn append(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        self.init();
        if value.is_null() {
            self.null_count += 1;
        } else {
            let mut bytes = BumpBytes::new_in(&self.arena);

            value.memcomparable_encode(&mut bytes)?;
            self.values
                .as_mut()
                .unwrap()
                .put((self.value_index, value.clone()));
            self.sort_keys
                .as_mut()
                .unwrap()
                .push((self.value_index, unsafe { mem::transmute::<_, _>(bytes) }))
        }

        self.value_index += 1;

        Ok(())
    }

    pub fn build(
        mut self,
        number_of_buckets: usize,
    ) -> Result<(Histogram, CountMinSketch<DataValue>), DatabaseError> {
        self.init();
        let values_len = self.values.as_ref().unwrap().len();
        if number_of_buckets > values_len {
            return Err(DatabaseError::TooManyBuckets(number_of_buckets, values_len));
        }

        let mut sketch = CountMinSketch::new(values_len, 0.95, 1.0);
        let HistogramBuilder {
            arena,
            index_id,
            null_count,
            values,
            sort_keys,
            ..
        } = self;
        let mut values = values.unwrap();
        let sort_keys = sort_keys.unwrap();
        let mut buckets = Vec::with_capacity(number_of_buckets);
        let bucket_len = if values_len % number_of_buckets == 0 {
            values_len / number_of_buckets
        } else {
            (values_len + number_of_buckets) / number_of_buckets
        };
        let sorted_indices = radix_sort(sort_keys, &arena);

        for i in 0..number_of_buckets {
            let mut bucket = Bucket::empty();
            let j = (i + 1) * bucket_len;

            bucket.upper = values
                .get(sorted_indices[cmp::min(j, values_len) - 1])
                .1
                .clone();
            buckets.push(bucket);
        }
        let mut corr_xy_sum = 0.0;
        let mut number_of_distinct_value = 0;
        let mut last_value: Option<DataValue> = None;

        for (i, index) in sorted_indices.into_iter().enumerate() {
            let (ordinal, value) = values.take(index);
            sketch.increment(&value);

            if let None | Some(true) = last_value.as_ref().map(|last_value| last_value != &value) {
                last_value = Some(value.clone());
                number_of_distinct_value += 1;
            }

            let bucket = &mut buckets[i / bucket_len];

            if bucket.lower.is_null() {
                bucket.lower = value;
            }
            bucket.count += 1;

            corr_xy_sum += i as f64 * ordinal as f64;
        }
        sketch.add(&DataValue::Null, self.null_count);

        drop(values);
        drop(arena);

        Ok((
            Histogram {
                index_id,
                number_of_distinct_value,
                null_count,
                values_len,
                buckets,
                correlation: Self::calc_correlation(corr_xy_sum, values_len),
            },
            sketch,
        ))
    }

    // https://github.com/pingcap/tidb/blob/6957170f1147e96958e63db48148445a7670328e/pkg/statistics/builder.go#L210
    fn calc_correlation(corr_xy_sum: f64, values_len: usize) -> f64 {
        if values_len == 1 {
            return 1.0;
        }
        let item_count = values_len as f64;
        let corr_x_sum = (item_count - 1.0) * item_count / 2.0;
        let corr_x2_sum = (item_count - 1.0) * item_count * (2.0 * item_count - 1.0) / 6.0;
        (item_count * corr_xy_sum - corr_x_sum * corr_x_sum)
            / (item_count * corr_x2_sum - corr_x_sum * corr_x_sum)
    }
}

fn is_under(
    value: &DataValue,
    target: &Bound<DataValue>,
    is_min: bool,
) -> Result<bool, DatabaseError> {
    let _is_under = |value: &DataValue, target: &DataValue, is_min: bool| {
        let evaluator = EvaluatorFactory::binary_create(
            value.logical_type(),
            if is_min {
                BinaryOperator::Lt
            } else {
                BinaryOperator::LtEq
            },
        )?;
        let value = evaluator.0.binary_eval(value, target)?;
        Ok::<bool, DatabaseError>(matches!(value, DataValue::Boolean(true)))
    };

    Ok(match target {
        Bound::Included(target) => _is_under(value, target, is_min)?,
        Bound::Excluded(target) => _is_under(value, target, !is_min)?,
        Bound::Unbounded => !is_min,
    })
}

fn is_above(
    value: &DataValue,
    target: &Bound<DataValue>,
    is_min: bool,
) -> Result<bool, DatabaseError> {
    let _is_above = |value: &DataValue, target: &DataValue, is_min: bool| {
        let evaluator = EvaluatorFactory::binary_create(
            value.logical_type(),
            if is_min {
                BinaryOperator::GtEq
            } else {
                BinaryOperator::Gt
            },
        )?;
        let value = evaluator.0.binary_eval(value, target)?;
        Ok::<bool, DatabaseError>(matches!(value, DataValue::Boolean(true)))
    };
    Ok(match target {
        Bound::Included(target) => _is_above(value, target, is_min)?,
        Bound::Excluded(target) => _is_above(value, target, !is_min)?,
        Bound::Unbounded => is_min,
    })
}

impl Histogram {
    pub fn index_id(&self) -> IndexId {
        self.index_id
    }

    pub fn values_len(&self) -> usize {
        self.values_len
    }

    pub fn collect_count(
        &self,
        ranges: &[Range],
        sketch: &CountMinSketch<DataValue>,
    ) -> Result<usize, DatabaseError> {
        if self.buckets.is_empty() || ranges.is_empty() {
            return Ok(0);
        }

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

    fn _collect_count(
        &self,
        ranges: &[Range],
        binary_i: &mut usize,
        bucket_i: &mut usize,
        bucket_idxs: &mut Vec<usize>,
        count: &mut usize,
        sketch: &CountMinSketch<DataValue>,
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

        let distinct_1 = OrderedFloat(1.0 / self.number_of_distinct_value as f64);

        match &ranges[*binary_i] {
            Range::Scope { min, max } => {
                let bucket = &self.buckets[*bucket_i];
                let mut bucket_count = bucket.count as usize;
                if *bucket_i == 0 {
                    bucket_count += self.null_count;
                }

                let mut temp_count = 0;

                let is_eq = |value: &DataValue, target: &Bound<DataValue>| match target {
                    Bound::Included(target) => target.eq(value),
                    _ => false,
                };

                if (is_above(&bucket.lower, min, true)? || is_eq(&bucket.lower, min))
                    && (is_under(&bucket.upper, max, false)? || is_eq(&bucket.upper, max))
                {
                    bucket_idxs.push(mem::replace(bucket_i, *bucket_i + 1));
                } else if is_above(&bucket.lower, max, false)? {
                    *binary_i += 1;
                } else if is_under(&bucket.upper, min, true)? {
                    *bucket_i += 1;
                } else if is_above(&bucket.lower, min, true)? {
                    let (temp_ratio, option) = match max {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1.max(OrderedFloat(temp_ratio).min(OrderedFloat(1.0)));
                    temp_count += (bucket_count as f64 * ratio).ceil() as usize;
                    if let Some(count) = option {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    *bucket_i += 1;
                } else if is_under(&bucket.upper, max, false)? {
                    let (temp_ratio, option) = match min {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1.max(OrderedFloat(temp_ratio).min(OrderedFloat(1.0)));
                    temp_count += (bucket_count as f64 * (1.0 - ratio)).ceil() as usize;
                    if let Some(count) = option {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    *bucket_i += 1;
                } else {
                    let (temp_ratio_max, option_max) = match max {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let (temp_ratio_min, option_min) = match min {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val)?, None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val)?,
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1
                        .max(OrderedFloat(temp_ratio_max - temp_ratio_min).min(OrderedFloat(1.0)));
                    temp_count += (bucket_count as f64 * ratio).ceil() as usize;
                    if let Some(count) = option_max {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    if let Some(count) = option_min {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    *binary_i += 1;
                }
                *count += cmp::max(temp_count, 0);
            }
            Range::Eq(value) => {
                *count += sketch.estimate(value);
                *binary_i += 1
            }
            Range::Dummy => return Ok(true),
            Range::SortedRanges(_) => unreachable!(),
        }

        Ok(false)
    }
}

impl Bucket {
    fn empty() -> Self {
        let empty_value = DataValue::Null;

        Bucket {
            lower: empty_value.clone(),
            upper: empty_value,
            count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::optimizer::core::histogram::{Bucket, HistogramBuilder};
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;
    use std::sync::Arc;
    use ulid::Ulid;

    fn index_meta() -> IndexMeta {
        IndexMeta {
            id: 0,
            column_ids: vec![Ulid::new()],
            table_name: Arc::new("t1".to_string()),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        }
    }

    #[test]
    fn test_sort_tuples_on_histogram() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), Some(15));

        builder.append(&DataValue::Int32(0))?;
        builder.append(&DataValue::Int32(1))?;
        builder.append(&DataValue::Int32(2))?;
        builder.append(&DataValue::Int32(3))?;
        builder.append(&DataValue::Int32(4))?;

        builder.append(&DataValue::Int32(5))?;
        builder.append(&DataValue::Int32(6))?;
        builder.append(&DataValue::Int32(7))?;
        builder.append(&DataValue::Int32(8))?;
        builder.append(&DataValue::Int32(9))?;

        builder.append(&DataValue::Int32(10))?;
        builder.append(&DataValue::Int32(11))?;
        builder.append(&DataValue::Int32(12))?;
        builder.append(&DataValue::Int32(13))?;
        builder.append(&DataValue::Int32(14))?;

        builder.append(&DataValue::Null)?;
        builder.append(&DataValue::Null)?;

        // assert!(matches!(builder.build(10), Err(DataBaseError::TooManyBuckets)));

        let (histogram, _) = builder.build(5)?;

        assert_eq!(histogram.correlation, 1.0);
        assert_eq!(histogram.null_count, 2);
        assert_eq!(histogram.buckets.len(), 5);
        assert_eq!(
            histogram.buckets,
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
        let mut builder = HistogramBuilder::new(&index_meta(), Some(15));

        builder.append(&DataValue::Int32(14))?;
        builder.append(&DataValue::Int32(13))?;
        builder.append(&DataValue::Int32(12))?;
        builder.append(&DataValue::Int32(11))?;
        builder.append(&DataValue::Int32(10))?;

        builder.append(&DataValue::Int32(9))?;
        builder.append(&DataValue::Int32(8))?;
        builder.append(&DataValue::Int32(7))?;
        builder.append(&DataValue::Int32(6))?;
        builder.append(&DataValue::Int32(5))?;

        builder.append(&DataValue::Int32(4))?;
        builder.append(&DataValue::Int32(3))?;
        builder.append(&DataValue::Int32(2))?;
        builder.append(&DataValue::Int32(1))?;
        builder.append(&DataValue::Int32(0))?;

        builder.append(&DataValue::Null)?;
        builder.append(&DataValue::Null)?;

        let (histogram, _) = builder.build(5)?;

        assert_eq!(histogram.correlation, -1.0);
        assert_eq!(histogram.null_count, 2);
        assert_eq!(histogram.buckets.len(), 5);
        assert_eq!(
            histogram.buckets,
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
        let mut builder = HistogramBuilder::new(&index_meta(), Some(15));

        builder.append(&DataValue::Int32(14))?;
        builder.append(&DataValue::Int32(13))?;
        builder.append(&DataValue::Int32(12))?;
        builder.append(&DataValue::Int32(11))?;
        builder.append(&DataValue::Int32(10))?;

        builder.append(&DataValue::Int32(4))?;
        builder.append(&DataValue::Int32(3))?;
        builder.append(&DataValue::Int32(2))?;
        builder.append(&DataValue::Int32(1))?;
        builder.append(&DataValue::Int32(0))?;

        builder.append(&DataValue::Int32(9))?;
        builder.append(&DataValue::Int32(8))?;
        builder.append(&DataValue::Int32(7))?;
        builder.append(&DataValue::Int32(6))?;
        builder.append(&DataValue::Int32(5))?;

        builder.append(&DataValue::Null)?;
        builder.append(&DataValue::Null)?;

        let (histogram, _) = builder.build(4)?;

        assert!(histogram.correlation < 0.0);
        assert_eq!(histogram.null_count, 2);
        assert_eq!(histogram.buckets.len(), 4);
        assert_eq!(
            histogram.buckets,
            vec![
                Bucket {
                    lower: DataValue::Int32(0),
                    upper: DataValue::Int32(3),
                    count: 4,
                },
                Bucket {
                    lower: DataValue::Int32(4),
                    upper: DataValue::Int32(7),
                    count: 4,
                },
                Bucket {
                    lower: DataValue::Int32(8),
                    upper: DataValue::Int32(11),
                    count: 4,
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
    fn test_collect_count() -> Result<(), DatabaseError> {
        let mut builder = HistogramBuilder::new(&index_meta(), Some(15));

        builder.append(&DataValue::Int32(14))?;
        builder.append(&DataValue::Int32(13))?;
        builder.append(&DataValue::Int32(12))?;
        builder.append(&DataValue::Int32(11))?;
        builder.append(&DataValue::Int32(10))?;

        builder.append(&DataValue::Int32(4))?;
        builder.append(&DataValue::Int32(3))?;
        builder.append(&DataValue::Int32(2))?;
        builder.append(&DataValue::Int32(1))?;
        builder.append(&DataValue::Int32(0))?;

        builder.append(&DataValue::Int32(9))?;
        builder.append(&DataValue::Int32(8))?;
        builder.append(&DataValue::Int32(7))?;
        builder.append(&DataValue::Int32(6))?;
        builder.append(&DataValue::Int32(5))?;

        builder.append(&DataValue::Null)?;

        let (histogram, sketch) = builder.build(4)?;

        let count_1 = histogram.collect_count(
            &vec![
                Range::Eq(DataValue::Int32(2)),
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(4)),
                    max: Bound::Excluded(DataValue::Int32(12)),
                },
            ],
            &sketch,
        )?;

        assert_eq!(count_1, 9);

        let count_2 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Included(DataValue::Int32(4)),
                max: Bound::Unbounded,
            }],
            &sketch,
        )?;

        assert_eq!(count_2, 11);

        let count_3 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(7)),
                max: Bound::Unbounded,
            }],
            &sketch,
        )?;

        assert_eq!(count_3, 7);

        let count_4 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(DataValue::Int32(11)),
            }],
            &sketch,
        )?;

        assert_eq!(count_4, 12);

        let count_5 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(8)),
            }],
            &sketch,
        )?;

        assert_eq!(count_5, 8);

        let count_6 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Included(DataValue::Int32(2)),
                max: Bound::Unbounded,
            }],
            &sketch,
        )?;

        assert_eq!(count_6, 13);

        let count_7 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(1)),
                max: Bound::Unbounded,
            }],
            &sketch,
        )?;

        assert_eq!(count_7, 13);

        let count_8 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(DataValue::Int32(12)),
            }],
            &sketch,
        )?;

        assert_eq!(count_8, 13);

        let count_9 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(13)),
            }],
            &sketch,
        )?;

        assert_eq!(count_9, 13);

        let count_10 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Excluded(DataValue::Int32(3)),
            }],
            &sketch,
        )?;

        assert_eq!(count_10, 3);

        let count_11 = histogram.collect_count(
            &vec![Range::Scope {
                min: Bound::Included(DataValue::Int32(1)),
                max: Bound::Included(DataValue::Int32(2)),
            }],
            &sketch,
        )?;

        assert_eq!(count_11, 2);

        Ok(())
    }
}
