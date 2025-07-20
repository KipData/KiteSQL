use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::types::value::{DataValue, NULL_VALUE};
use crate::types::ColumnId;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Formatter;
use std::{fmt, mem};

/// Used to represent binary relationships between fields and constants
/// Tips: The NotEq case is ignored because it makes expression composition very complex
/// - [`Range::Scope`]:
#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Range {
    Scope {
        min: Bound<DataValue>,
        max: Bound<DataValue>,
    },
    Eq(DataValue),
    Dummy,
    SortedRanges(Vec<Range>),
}

struct TreeNode<T> {
    value: Option<T>,
    children: Vec<TreeNode<T>>,
}

impl<T> TreeNode<T> {
    fn new(value: Option<T>) -> Self {
        TreeNode {
            value,
            children: Vec::new(),
        }
    }

    fn add_child(&mut self, child: TreeNode<T>) {
        self.children.push(child);
    }
}

impl<T: Clone> TreeNode<T> {
    fn enumeration(self, path: &mut Vec<T>, combinations: &mut Vec<Vec<T>>) {
        if self.value.is_none() && self.children.is_empty() {
            combinations.push(path.clone());
        }
        for mut child in self.children {
            if let Some(val) = child.value.take() {
                path.push(val);
                Self::enumeration(child, path, combinations);
                let _ = path.pop();
            } else {
                Self::enumeration(child, path, combinations);
            }
        }
    }
}

fn build_tree(ranges: &[Range], current_level: usize) -> Option<TreeNode<&DataValue>> {
    fn build_subtree<'a>(
        ranges: &'a [Range],
        range: &'a Range,
        current_level: usize,
    ) -> Option<TreeNode<&'a DataValue>> {
        let value = match range {
            Range::Eq(value) => value,
            _ => return None,
        };
        let mut child = TreeNode::new(Some(value));
        let subtree = build_tree(ranges, current_level + 1)?;

        if !subtree.children.is_empty() || current_level == ranges.len() - 1 {
            child.add_child(subtree);
        }
        Some(child)
    }

    let mut root = TreeNode::new(None);

    if current_level < ranges.len() {
        match &ranges[current_level] {
            Range::SortedRanges(child_ranges) => {
                for range in child_ranges.iter() {
                    root.children
                        .push(build_subtree(ranges, range, current_level)?);
                }
            }
            range => {
                root.children
                    .push(build_subtree(ranges, range, current_level)?);
            }
        }
    }
    Some(root)
}

impl Range {
    pub(crate) fn only_eq(&self) -> bool {
        match self {
            Range::Eq(_) => true,
            Range::SortedRanges(ranges) => ranges.iter().all(|range| range.only_eq()),
            _ => false,
        }
    }

    pub(crate) fn combining_eqs(&self, eqs: &[Range]) -> Option<Range> {
        #[allow(clippy::map_clone)]
        fn merge_value(tuple: &[&DataValue], is_upper: bool, value: DataValue) -> DataValue {
            let mut merge_tuple = Vec::with_capacity(tuple.len() + 1);
            for value in tuple {
                merge_tuple.push((*value).clone());
            }
            merge_tuple.push(value);

            DataValue::Tuple(merge_tuple, is_upper)
        }
        fn collect_tuple_range(result_ranges: &mut Vec<Range>, tuple: &[&DataValue], range: Range) {
            fn merge_value_on_bound(
                tuple: &[&DataValue],
                is_upper: bool,
                bound: Bound<DataValue>,
            ) -> Bound<DataValue> {
                match bound {
                    Bound::Included(v) => Bound::Included(merge_value(tuple, is_upper, v)),
                    Bound::Excluded(v) => Bound::Excluded(merge_value(tuple, is_upper, v)),
                    Bound::Unbounded => {
                        if tuple.is_empty() {
                            return Bound::Unbounded;
                        }
                        let values = tuple.iter().map(|v| (*v).clone()).collect_vec();
                        Bound::Excluded(DataValue::Tuple(values, is_upper))
                    }
                }
            }

            match range {
                Range::Scope { min, max } => result_ranges.push(Range::Scope {
                    min: merge_value_on_bound(tuple, false, min),
                    max: merge_value_on_bound(tuple, true, max),
                }),
                Range::Eq(v) => result_ranges.push(Range::Eq(merge_value(tuple, false, v))),
                Range::Dummy => result_ranges.push(Range::Dummy),
                Range::SortedRanges(mut ranges) => {
                    for range in &mut ranges {
                        collect_tuple_range(result_ranges, tuple, mem::replace(range, Range::Dummy))
                    }
                }
            }
        }

        let node = build_tree(eqs, 0)?;
        let mut combinations = Vec::new();

        node.enumeration(&mut Vec::new(), &mut combinations);

        let mut ranges = Vec::new();

        for tuple in combinations {
            collect_tuple_range(&mut ranges, &tuple, self.clone())
        }
        Some(RangeDetacher::ranges2range(ranges))
    }
}

pub struct RangeDetacher<'a> {
    table_name: &'a str,
    column_id: &'a ColumnId,
}

impl<'a> RangeDetacher<'a> {
    pub(crate) fn new(table_name: &'a str, column_id: &'a ColumnId) -> Self {
        Self {
            table_name,
            column_id,
        }
    }

    pub(crate) fn detach(
        &mut self,
        expr: &ScalarExpression,
    ) -> Result<Option<Range>, DatabaseError> {
        Ok(match expr {
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => match (self.detach(left_expr)?, self.detach(right_expr)?) {
                (Some(left_binary), Some(right_binary)) => {
                    Self::merge_binary(*op, left_binary, right_binary)
                }
                (None, None) => {
                    if let (Some(col), Some(val)) =
                        (left_expr.unpack_col(false), right_expr.unpack_val())
                    {
                        return self.new_range(*op, col, val, false);
                    } else if let (Some(val), Some(col)) =
                        (left_expr.unpack_val(), right_expr.unpack_col(false))
                    {
                        return self.new_range(*op, col, val, true);
                    }

                    None
                }
                (Some(binary), None) | (None, Some(binary)) => self.check_or(op, binary),
            },
            ScalarExpression::Alias { expr, .. }
            | ScalarExpression::TypeCast { expr, .. }
            | ScalarExpression::Unary { expr, .. }
            | ScalarExpression::In { expr, .. }
            | ScalarExpression::Between { expr, .. }
            | ScalarExpression::SubString { expr, .. } => self.detach(expr)?,
            ScalarExpression::Position { expr, .. } => self.detach(expr)?,
            ScalarExpression::Trim { expr, .. } => self.detach(expr)?,
            ScalarExpression::IsNull { expr, negated, .. } => match expr.as_ref() {
                ScalarExpression::ColumnRef(column) => {
                    if let (Some(col_id), Some(col_table)) = (column.id(), column.table_name()) {
                        if &col_id == self.column_id && col_table.as_str() == self.table_name {
                            return if *negated {
                                // Range::NotEq(NULL_VALUE.clone())
                                Ok(None)
                            } else {
                                Ok(Some(Range::Eq(NULL_VALUE.clone())))
                            };
                        }
                    }

                    None
                }
                ScalarExpression::Constant(_)
                | ScalarExpression::Alias { .. }
                | ScalarExpression::TypeCast { .. }
                | ScalarExpression::IsNull { .. }
                | ScalarExpression::Unary { .. }
                | ScalarExpression::Binary { .. }
                | ScalarExpression::AggCall { .. }
                | ScalarExpression::In { .. }
                | ScalarExpression::Between { .. }
                | ScalarExpression::SubString { .. }
                | ScalarExpression::Position { .. }
                | ScalarExpression::Trim { .. }
                | ScalarExpression::ScalaFunction(_)
                | ScalarExpression::If { .. }
                | ScalarExpression::IfNull { .. }
                | ScalarExpression::NullIf { .. }
                | ScalarExpression::Coalesce { .. }
                | ScalarExpression::CaseWhen { .. } => self.detach(expr)?,
                ScalarExpression::Tuple(_)
                | ScalarExpression::TableFunction(_)
                | ScalarExpression::Reference { .. }
                | ScalarExpression::Empty => unreachable!(),
            },
            ScalarExpression::Constant(_) | ScalarExpression::ColumnRef(_) => None,
            // FIXME: support [RangeDetacher::_detach]
            ScalarExpression::Tuple(_)
            | ScalarExpression::AggCall { .. }
            | ScalarExpression::ScalaFunction(_)
            | ScalarExpression::If { .. }
            | ScalarExpression::IfNull { .. }
            | ScalarExpression::NullIf { .. }
            | ScalarExpression::Coalesce { .. }
            | ScalarExpression::CaseWhen { .. } => None,
            ScalarExpression::TableFunction(_)
            | ScalarExpression::Reference { .. }
            | ScalarExpression::Empty => unreachable!(),
        })
    }

    fn merge_binary(op: BinaryOperator, left_binary: Range, right_binary: Range) -> Option<Range> {
        fn process_exclude_bound_with_eq(
            bound: Bound<DataValue>,
            eq: &DataValue,
            op: BinaryOperator,
        ) -> Bound<DataValue> {
            match bound {
                Bound::Excluded(bound_val) => {
                    if &bound_val == eq && op == BinaryOperator::Or {
                        Bound::Included(bound_val)
                    } else {
                        Bound::Excluded(bound_val)
                    }
                }
                bound => bound,
            }
        }
        match (left_binary, right_binary) {
            (Range::Dummy, binary) | (binary, Range::Dummy) => match op {
                BinaryOperator::And => Some(Range::Dummy),
                BinaryOperator::Or => Some(binary),
                _ => None,
            },
            // e.g. c1 > 1 ? c1 < 2
            (
                Range::Scope {
                    min: left_min,
                    max: left_max,
                },
                Range::Scope {
                    min: right_min,
                    max: right_max,
                },
            ) => match op {
                BinaryOperator::And => Some(Self::and_scope_merge(
                    left_min, left_max, right_min, right_max,
                )),
                BinaryOperator::Or => Some(Self::or_scope_merge(
                    left_min, left_max, right_min, right_max,
                )),
                _ => None,
            },
            // e.g. c1 > 1 ? c1 = 1
            (Range::Scope { min, max }, Range::Eq(eq))
            | (Range::Eq(eq), Range::Scope { min, max }) => {
                let unpack_bound = |bound_eq: Bound<DataValue>| match bound_eq {
                    Bound::Included(val) | Bound::Excluded(val) => val,
                    _ => unreachable!(),
                };
                match op {
                    BinaryOperator::And => {
                        let bound_eq = Bound::Included(eq);
                        let is_less = matches!(
                            Self::bound_compared(&bound_eq, &min, true).unwrap_or({
                                if matches!(min, Bound::Unbounded) {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }),
                            Ordering::Less
                        );

                        if is_less
                            || matches!(
                                Self::bound_compared(&bound_eq, &max, false),
                                Some(Ordering::Greater)
                            )
                        {
                            return Some(Range::Dummy);
                        }
                        Some(Range::Eq(unpack_bound(bound_eq)))
                    }
                    BinaryOperator::Or => {
                        if eq.is_null() {
                            return Some(if matches!(min, Bound::Excluded(_)) {
                                Range::SortedRanges(vec![Range::Eq(eq), Range::Scope { min, max }])
                            } else {
                                Range::Scope { min, max }
                            });
                        }
                        let bound_eq = Bound::Excluded(eq);
                        let range = match Self::bound_compared(&bound_eq, &min, true) {
                            Some(Ordering::Less) => Range::SortedRanges(vec![
                                Range::Eq(unpack_bound(bound_eq)),
                                Range::Scope { min, max },
                            ]),
                            Some(Ordering::Equal) => Range::Scope {
                                min: process_exclude_bound_with_eq(
                                    min,
                                    &unpack_bound(bound_eq),
                                    op,
                                ),
                                max,
                            },
                            _ => match Self::bound_compared(&bound_eq, &max, false) {
                                Some(Ordering::Greater) => Range::SortedRanges(vec![
                                    Range::Scope { min, max },
                                    Range::Eq(unpack_bound(bound_eq)),
                                ]),
                                Some(Ordering::Equal) => Range::Scope {
                                    min,
                                    max: process_exclude_bound_with_eq(
                                        max,
                                        &unpack_bound(bound_eq),
                                        op,
                                    ),
                                },
                                _ => Range::Scope { min, max },
                            },
                        };
                        Some(range)
                    }
                    _ => None,
                }
            }
            // e.g. c1 > 1 ? (c1 = 1 or c1 = 2)
            (Range::Scope { min, max }, Range::SortedRanges(ranges))
            | (Range::SortedRanges(ranges), Range::Scope { min, max }) => {
                let merged_ranges =
                    Self::extract_merge_ranges(op, Some(Range::Scope { min, max }), ranges, &mut 0);

                Some(Self::ranges2range(merged_ranges))
            }
            // e.g. c1 = 1 ? c1 = 2
            (Range::Eq(left_val), Range::Eq(right_val)) => {
                if left_val.eq(&right_val) && matches!(op, BinaryOperator::And | BinaryOperator::Or)
                {
                    return Some(Range::Eq(left_val));
                }
                match op {
                    BinaryOperator::And => Some(Range::Dummy),
                    BinaryOperator::Or => {
                        let mut ranges = Vec::new();

                        let (val_1, val_2) = if let Some(true) =
                            left_val.partial_cmp(&right_val).map(Ordering::is_gt)
                        {
                            (right_val, left_val)
                        } else {
                            (left_val, right_val)
                        };
                        ranges.push(Range::Eq(val_1));
                        ranges.push(Range::Eq(val_2));
                        Some(Range::SortedRanges(ranges))
                    }
                    _ => None,
                }
            }
            // e.g. c1 = 1 ? (c1 = 1 or c1 = 2)
            (Range::Eq(eq), Range::SortedRanges(ranges))
            | (Range::SortedRanges(ranges), Range::Eq(eq)) => {
                let merged_ranges =
                    Self::extract_merge_ranges(op, Some(Range::Eq(eq)), ranges, &mut 0);

                Some(Self::ranges2range(merged_ranges))
            }
            // e.g. (c1 = 1 or c1 = 2) ? (c1 = 1 or c1 = 2)
            (Range::SortedRanges(left_ranges), Range::SortedRanges(mut right_ranges)) => {
                let mut idx = 0;

                for left_range in left_ranges {
                    right_ranges =
                        Self::extract_merge_ranges(op, Some(left_range), right_ranges, &mut idx)
                }

                Some(Self::ranges2range(right_ranges))
            }
        }
    }

    fn ranges2range(mut merged_ranges: Vec<Range>) -> Range {
        if merged_ranges.is_empty() {
            Range::Dummy
        } else if merged_ranges.len() == 1 {
            merged_ranges.pop().unwrap()
        } else {
            Range::SortedRanges(merged_ranges)
        }
    }

    #[allow(unreachable_code)]
    fn extract_merge_ranges(
        op: BinaryOperator,
        mut binary: Option<Range>,
        mut ranges: Vec<Range>,
        idx: &mut usize,
    ) -> Vec<Range> {
        // FIXME: Lots of duplicate code
        while *idx < ranges.len() {
            match (&binary, &ranges[*idx]) {
                (
                    Some(Range::Scope {
                        min: l_min,
                        max: l_max,
                    }),
                    Range::Scope {
                        min: r_min,
                        max: r_max,
                    },
                ) => {
                    if let Some(true) =
                        Self::bound_compared(l_max, r_min, false).map(Ordering::is_lt)
                    {
                        ranges.insert(*idx, binary.unwrap());
                        return ranges;
                    } else if let Some(true) =
                        Self::bound_compared(l_min, r_max, true).map(Ordering::is_gt)
                    {
                        *idx += 1;
                        continue;
                    } else {
                        binary = Self::merge_binary(op, binary.unwrap(), ranges.remove(*idx));
                    }
                }
                (
                    Some(Range::Scope {
                        min: l_min,
                        max: l_max,
                    }),
                    Range::Eq(r_val),
                ) => {
                    let r_bound = Bound::Included(r_val.clone());

                    if let Some(true) =
                        Self::bound_compared(l_max, &r_bound, false).map(Ordering::is_lt)
                    {
                        ranges.insert(*idx, binary.unwrap());
                        return ranges;
                    } else if Self::bound_compared(l_min, &r_bound, true)
                        .map(Ordering::is_gt)
                        .unwrap_or_else(|| op == BinaryOperator::Or)
                    {
                        *idx += 1;
                        continue;
                    } else if r_val.is_null() {
                        let _ = ranges.remove(*idx);
                    } else {
                        binary = Self::merge_binary(op, binary.unwrap(), ranges.remove(*idx));
                    }
                }
                (Some(Range::Eq(l_val)), Range::Eq(r_val)) => {
                    if let Some(true) = l_val.partial_cmp(r_val).map(Ordering::is_lt) {
                        ranges.insert(*idx, binary.unwrap());
                        return ranges;
                    } else if let Some(true) = l_val.partial_cmp(r_val).map(Ordering::is_gt) {
                        *idx += 1;
                        continue;
                    } else {
                        binary = Self::merge_binary(op, binary.unwrap(), ranges.remove(*idx));
                    }
                }
                (
                    Some(Range::Eq(l_val)),
                    Range::Scope {
                        min: r_min,
                        max: r_max,
                    },
                ) => {
                    let l_bound = Bound::Included(l_val.clone());

                    if Self::bound_compared(&l_bound, r_min, false)
                        .map(Ordering::is_lt)
                        .unwrap_or_else(|| op == BinaryOperator::Or)
                    {
                        ranges.insert(*idx, binary.unwrap());
                        return ranges;
                    } else if let Some(true) =
                        Self::bound_compared(&l_bound, r_max, true).map(Ordering::is_gt)
                    {
                        *idx += 1;
                        continue;
                    } else if l_val.is_null() {
                        binary = Some(ranges.remove(*idx));
                    } else {
                        binary = Self::merge_binary(op, binary.unwrap(), ranges.remove(*idx));
                    }
                }
                (Some(Range::Dummy), _) => {
                    binary = match op {
                        BinaryOperator::And => return vec![],
                        BinaryOperator::Or => Some(ranges.remove(*idx)),
                        _ => None,
                    };
                }
                (Some(Range::SortedRanges(l_ranges)), r_range) => {
                    return Self::extract_merge_ranges(
                        op,
                        Some(r_range.clone()),
                        l_ranges.clone(),
                        &mut 0,
                    );
                }
                (None, _) => break,
                _ => {
                    #[cfg(debug_assertions)]
                    {
                        unreachable!();
                    }
                    return vec![];
                }
            }
        }
        if let Some(range) = binary {
            ranges.push(range);
        }
        ranges
    }

    fn or_scope_merge(
        left_min: Bound<DataValue>,
        left_max: Bound<DataValue>,
        right_min: Bound<DataValue>,
        right_max: Bound<DataValue>,
    ) -> Range {
        if matches!(
            Self::bound_compared(&left_max, &right_min, false),
            Some(Ordering::Less)
        ) || matches!(
            Self::bound_compared(&right_max, &left_min, false),
            Some(Ordering::Less)
        ) {
            let (min_1, max_1, min_2, max_2) = if let Some(true) =
                Self::bound_compared(&left_min, &right_min, true).map(Ordering::is_lt)
            {
                (left_min, left_max, right_min, right_max)
            } else {
                (right_min, right_max, left_min, left_max)
            };
            return Range::SortedRanges(vec![
                Range::Scope {
                    min: min_1,
                    max: max_1,
                },
                Range::Scope {
                    min: min_2,
                    max: max_2,
                },
            ]);
        }
        let min = if let Some(true) =
            Self::bound_compared(&left_min, &right_min, true).map(Ordering::is_lt)
        {
            left_min
        } else {
            right_min
        };
        let max = if let Some(true) =
            Self::bound_compared(&left_max, &right_max, false).map(Ordering::is_gt)
        {
            left_max
        } else {
            right_max
        };
        match Self::bound_compared(&min, &max, matches!(min, Bound::Unbounded)) {
            Some(Ordering::Equal) => match min {
                Bound::Included(val) => Range::Eq(val),
                Bound::Excluded(_) => Range::Dummy,
                Bound::Unbounded => Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                },
            },
            _ => Range::Scope { min, max },
        }
    }

    fn and_scope_merge(
        left_min: Bound<DataValue>,
        left_max: Bound<DataValue>,
        right_min: Bound<DataValue>,
        right_max: Bound<DataValue>,
    ) -> Range {
        let min = if let Some(true) =
            Self::bound_compared(&left_min, &right_min, true).map(Ordering::is_gt)
        {
            left_min
        } else {
            right_min
        };
        let max = if let Some(true) =
            Self::bound_compared(&left_max, &right_max, false).map(Ordering::is_lt)
        {
            left_max
        } else {
            right_max
        };
        match Self::bound_compared(&min, &max, matches!(min, Bound::Unbounded)) {
            Some(Ordering::Greater) => Range::Dummy,
            Some(Ordering::Equal) => match min {
                Bound::Included(val) => Range::Eq(val),
                Bound::Excluded(_) => Range::Dummy,
                Bound::Unbounded => Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                },
            },
            _ => Range::Scope { min, max },
        }
    }

    fn _is_belong(table_name: &str, col: &ColumnRef) -> bool {
        matches!(
            col.table_name().map(|name| table_name == name.as_str()),
            Some(true)
        )
    }

    fn bound_compared(
        left_bound: &Bound<DataValue>,
        right_bound: &Bound<DataValue>,
        is_min: bool,
    ) -> Option<Ordering> {
        fn is_min_then_reverse(is_min: bool, order: Ordering) -> Ordering {
            if is_min {
                order
            } else {
                order.reverse()
            }
        }
        match (left_bound, right_bound) {
            (Bound::Unbounded, Bound::Unbounded) => Some(Ordering::Equal),
            (Bound::Unbounded, _) => Some(is_min_then_reverse(is_min, Ordering::Less)),
            (_, Bound::Unbounded) => Some(is_min_then_reverse(is_min, Ordering::Greater)),
            (Bound::Included(left), Bound::Included(right)) => left.partial_cmp(right),
            (Bound::Included(left), Bound::Excluded(right)) => left
                .partial_cmp(right)
                .map(|order| order.then(is_min_then_reverse(is_min, Ordering::Less))),
            (Bound::Excluded(left), Bound::Excluded(right)) => left.partial_cmp(right),
            (Bound::Excluded(left), Bound::Included(right)) => left
                .partial_cmp(right)
                .map(|order| order.then(is_min_then_reverse(is_min, Ordering::Greater))),
        }
    }

    fn new_range(
        &mut self,
        mut op: BinaryOperator,
        col: ColumnRef,
        mut val: DataValue,
        is_flip: bool,
    ) -> Result<Option<Range>, DatabaseError> {
        if !Self::_is_belong(self.table_name, &col) || col.id() != Some(*self.column_id) {
            return Ok(None);
        }
        if &val.logical_type() != col.datatype() {
            val = val.cast(col.datatype())?
        }
        if is_flip {
            op = match op {
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                source_op => source_op,
            };
        }
        Ok(match op {
            BinaryOperator::Gt => Some(Range::Scope {
                min: Bound::Excluded(val.clone()),
                max: Bound::Unbounded,
            }),
            BinaryOperator::Lt => Some(Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(val.clone()),
            }),
            BinaryOperator::GtEq => Some(Range::Scope {
                min: Bound::Included(val.clone()),
                max: Bound::Unbounded,
            }),
            BinaryOperator::LtEq => Some(Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(val.clone()),
            }),
            BinaryOperator::Eq | BinaryOperator::Spaceship => Some(Range::Eq(val.clone())),
            _ => None,
        })
    }

    /// check if: `c1 > c2 or c1 > 1` or `c2 > 1 or c1 > 1`
    /// this case it makes no sense to just extract c1 > 1
    fn check_or(&mut self, op: &BinaryOperator, binary: Range) -> Option<Range> {
        if matches!(op, BinaryOperator::Or) {
            return None;
        }

        Some(binary)
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Range::Scope { min, max } => {
                match min {
                    Bound::Unbounded => write!(f, "(-inf")?,
                    Bound::Included(value) => write!(f, "[{}", value)?,
                    Bound::Excluded(value) => write!(f, "({}", value)?,
                }

                write!(f, ", ")?;

                match max {
                    Bound::Unbounded => write!(f, "+inf)")?,
                    Bound::Included(value) => write!(f, "{}]", value)?,
                    Bound::Excluded(value) => write!(f, "{})", value)?,
                }

                Ok(())
            }
            Range::Eq(value) => write!(f, "{}", value),
            Range::Dummy => write!(f, "Dummy"),
            Range::SortedRanges(ranges) => {
                let ranges_str = ranges.iter().map(|range| format!("{}", range)).join(", ");
                write!(f, "{}", ranges_str)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::{Range, RangeDetacher};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::Operator;
    use crate::planner::LogicalPlan;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::tuple::TupleLtBinaryEvaluator;
    use crate::types::evaluator::BinaryEvaluator;
    use crate::types::value::DataValue;
    use std::ops::Bound;

    fn plan_filter(plan: LogicalPlan) -> Result<Option<FilterOperator>, DatabaseError> {
        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .find_best::<RocksTransaction>(None)?;
        if let Operator::Filter(filter_op) = best_plan.childrens.pop_only().operator {
            Ok(Some(filter_op))
        } else {
            Ok(None)
        }
    }

    #[test]
    fn test_detach_ideal_cases() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        {
            let plan = table_state.plan("select * from t1 where c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = 1 => {}", range);
            assert_eq!(range, Range::Eq(DataValue::Int32(1)))
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = 1.0")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = 1.0 => {}", range);
            assert_eq!(range, Range::Eq(DataValue::Int32(1)))
        }
        {
            let plan = table_state.plan("select * from t1 where c1 != 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 != 1 => {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 > 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 > 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Excluded(DataValue::Int32(1)),
                    max: Bound::Unbounded,
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 >= 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 >= 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(1)),
                    max: Bound::Unbounded,
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 < 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 < 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Excluded(DataValue::Int32(1)),
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 <= 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 <= 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Included(DataValue::Int32(1)),
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 < 1 and c1 >= 0")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 < 1 and c1 >= 0 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(0)),
                    max: Bound::Excluded(DataValue::Int32(1)),
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 < 1 or c1 >= 0")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 < 1 or c1 >= 0 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                }
            )
        }
        // and & or
        {
            let plan = table_state.plan("select * from t1 where c1 = 1 and c1 = 0")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = 1 and c1 = 0 => c1: {}", range);
            assert_eq!(range, Range::Dummy)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = 1 or c1 = 0")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = 1 or c1 = 0 => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Eq(DataValue::Int32(0)),
                    Range::Eq(DataValue::Int32(1)),
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = 1 and c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = 1 and c1 = 1 => c1: {}", range);
            assert_eq!(range, Range::Eq(DataValue::Int32(1)))
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = 1 or c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = 1 or c1 = 1 => c1: {}", range);
            assert_eq!(range, Range::Eq(DataValue::Int32(1)))
        }

        {
            let plan = table_state.plan("select * from t1 where c1 > 1 and c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 > 1 and c1 = 1 => c1: {}", range);
            assert_eq!(range, Range::Dummy)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 >= 1 and c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 >= 1 and c1 = 1 => c1: {}", range);
            assert_eq!(range, Range::Eq(DataValue::Int32(1)))
        }
        {
            let plan = table_state.plan("select * from t1 where c1 > 1 or c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 > 1 or c1 = 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(1)),
                    max: Bound::Unbounded,
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 >= 1 or c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 >= 1 or c1 = 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(1)),
                    max: Bound::Unbounded,
                }
            )
        }
        // scope
        {
            let plan = table_state
                .plan("select * from t1 where (c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "(c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4) => c1: {}",
                range
            );
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Excluded(DataValue::Int32(1)),
                    max: Bound::Excluded(DataValue::Int32(3)),
                }
            )
        }
        {
            let plan = table_state
                .plan("select * from t1 where (c1 > 0 and c1 < 3) or (c1 > 1 and c1 < 4)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "(c1 > 0 and c1 < 3) or (c1 > 1 and c1 < 4) => c1: {}",
                range
            );
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Excluded(DataValue::Int32(0)),
                    max: Bound::Excluded(DataValue::Int32(4)),
                }
            )
        }

        {
            let plan = table_state.plan(
                "select * from t1 where ((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) and c1 = 0",
            )?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) and c1 = 0 => c1: {}",
                range
            );
            assert_eq!(range, Range::Dummy)
        }
        {
            let plan = table_state.plan(
                "select * from t1 where ((c1 > 0 and c1 < 3) or (c1 > 1 and c1 < 4)) and c1 = 0",
            )?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "((c1 > 0 and c1 < 3) or (c1 > 1 and c1 < 4)) and c1 = 0 => c1: {}",
                range
            );
            assert_eq!(range, Range::Dummy)
        }
        {
            let plan = table_state.plan(
                "select * from t1 where ((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) or c1 = 0",
            )?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) or c1 = 0 => c1: {}",
                range
            );
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Eq(DataValue::Int32(0)),
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(1)),
                        max: Bound::Excluded(DataValue::Int32(3)),
                    }
                ])
            )
        }
        {
            let plan = table_state.plan(
                "select * from t1 where ((c1 > 0 and c1 < 3) or (c1 > 1 and c1 < 4)) or c1 = 0",
            )?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "((c1 > 0 and c1 < 3) or (c1 > 1 and c1 < 4)) or c1 = 0 => c1: {}",
                range
            );
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(0)),
                    max: Bound::Excluded(DataValue::Int32(4)),
                }
            )
        }

        {
            let plan = table_state.plan("select * from t1 where (((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) and c1 = 0) and (c1 >= 0 and c1 <= 2)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("(((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) and c1 = 0) and (c1 >= 0 and c1 <= 2) => c1: {}", range);
            assert_eq!(range, Range::Dummy)
        }
        {
            let plan = table_state.plan("select * from t1 where (((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) and c1 = 0) or (c1 >= 0 and c1 <= 2)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("(((c1 > 0 and c1 < 3) and (c1 > 1 and c1 < 4)) and c1 = 0) or (c1 >= 0 and c1 <= 2) => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(0)),
                    max: Bound::Included(DataValue::Int32(2)),
                }
            )
        }
        // ranges and ranges
        {
            let plan = table_state.plan("select * from t1 where ((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) and ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5))")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) and ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5)) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(1)),
                        max: Bound::Excluded(DataValue::Int32(2)),
                    },
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(5)),
                        max: Bound::Excluded(DataValue::Int32(6)),
                    },
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where ((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) or ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5))")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) or ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5)) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(0)),
                        max: Bound::Excluded(DataValue::Int32(3)),
                    },
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(4)),
                        max: Bound::Excluded(DataValue::Int32(7)),
                    },
                ])
            )
        }
        // empty
        {
            let plan = table_state.plan("select * from t1 where true")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("empty => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        // other column
        {
            let plan = table_state.plan("select * from t1 where c2 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c2 = 1 => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 > 1 or c2 > 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 > 1 or c2 > 1 => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 > c2 or c2 > 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 > c2 or c2 > 1 => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        // case 1
        {
            let plan = table_state.plan(
                "select * from t1 where c1 = 5 or (c1 > 5 and (c1 > 6 or c1 < 8) and c1 < 12)",
            )?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "c1 = 5 or (c1 > 5 and (c1 > 6 or c1 < 8) and c1 < 12) => c1: {}",
                range
            );
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(5)),
                    max: Bound::Excluded(DataValue::Int32(12)),
                }
            )
        }
        // case 2
        {
            let plan = table_state.plan(
                "select * from t1 where ((c2 >= -8 and -4 >= c1) or (c1 >= 0 and 5 > c2)) and ((c2 > 0 and c1 <= 1) or (c1 > -8 and c2 < -6))",
            )?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!(
                "((c2 >= -8 and -4 >= c1) or (c1 >= 0 and 5 > c2)) and ((c2 > 0 and c1 <= 1) or (c1 > -8 and c2 < -6)) => c1: {}",
                range
            );
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Scope {
                        min: Bound::Unbounded,
                        max: Bound::Included(DataValue::Int32(-4)),
                    },
                    Range::Scope {
                        min: Bound::Included(DataValue::Int32(0)),
                        max: Bound::Unbounded,
                    }
                ])
            )
        }

        Ok(())
    }

    // Tips: `null` should be First
    #[test]
    fn test_detach_null_cases() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        // eq
        {
            let plan = table_state.plan("select * from t1 where c1 = null")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = null => c1: {}", range);
            assert_eq!(range, Range::Eq(DataValue::Null))
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = null or c1 = 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = null or c1 = 1 => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Eq(DataValue::Null),
                    Range::Eq(DataValue::Int32(1))
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = null or c1 < 5")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = null or c1 < 5 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Excluded(DataValue::Int32(5)),
                }
            )
        }
        {
            let plan =
                table_state.plan("select * from t1 where c1 = null or (c1 > 1 and c1 < 5)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = null or (c1 > 1 and c1 < 5) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Eq(DataValue::Null),
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(1)),
                        max: Bound::Excluded(DataValue::Int32(5)),
                    },
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = null and c1 < 5")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = null and c1 < 5 => c1: {}", range);
            assert_eq!(range, Range::Eq(DataValue::Null))
        }
        {
            let plan =
                table_state.plan("select * from t1 where c1 = null and (c1 > 1 and c1 < 5)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 = null and (c1 > 1 and c1 < 5) => c1: {}", range);
            assert_eq!(range, Range::Dummy)
        }
        // noteq
        {
            let plan = table_state.plan("select * from t1 where c1 != null")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 != null => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 = null or c1 != 1")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 = null or c1 != 1 => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 != null or c1 < 5")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 != null or c1 < 5 => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan =
                table_state.plan("select * from t1 where c1 != null or (c1 > 1 and c1 < 5)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?;
            println!("c1 != null or (c1 > 1 and c1 < 5) => c1: {:#?}", range);
            assert_eq!(range, None)
        }
        {
            let plan = table_state.plan("select * from t1 where c1 != null and c1 < 5")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 != null and c1 < 5 => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Excluded(DataValue::Int32(5)),
                }
            )
        }
        {
            let plan =
                table_state.plan("select * from t1 where c1 != null and (c1 > 1 and c1 < 5)")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("c1 != null and (c1 > 1 and c1 < 5) => c1: {}", range);
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Excluded(DataValue::Int32(1)),
                    max: Bound::Excluded(DataValue::Int32(5)),
                }
            )
        }
        {
            let plan = table_state.plan("select * from t1 where (c1 = null or (c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) or ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5))")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("(c1 = null or (c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) or ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5)) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Eq(DataValue::Null),
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(0)),
                        max: Bound::Excluded(DataValue::Int32(3)),
                    },
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(4)),
                        max: Bound::Excluded(DataValue::Int32(7)),
                    }
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where ((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) or (c1 = null or (c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5))")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) or (c1 = null or (c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5)) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Eq(DataValue::Null),
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(0)),
                        max: Bound::Excluded(DataValue::Int32(3)),
                    },
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(4)),
                        max: Bound::Excluded(DataValue::Int32(7)),
                    }
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where (c1 = null or (c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) and ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5))")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("(c1 = null or (c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) and ((c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5)) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(1)),
                        max: Bound::Excluded(DataValue::Int32(2)),
                    },
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(5)),
                        max: Bound::Excluded(DataValue::Int32(6)),
                    }
                ])
            )
        }
        {
            let plan = table_state.plan("select * from t1 where ((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) and (c1 = null or (c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5))")?;
            let op = plan_filter(plan)?.unwrap();
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"))
                .detach(&op.predicate)?
                .unwrap();
            println!("((c1 < 2 and c1 > 0) or (c1 < 6 and c1 > 4)) and (c1 = null or (c1 < 3 and c1 > 1) or (c1 < 7 and c1 > 5)) => c1: {}", range);
            assert_eq!(
                range,
                Range::SortedRanges(vec![
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(1)),
                        max: Bound::Excluded(DataValue::Int32(2)),
                    },
                    Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(5)),
                        max: Bound::Excluded(DataValue::Int32(6)),
                    }
                ])
            )
        }

        Ok(())
    }

    #[test]
    fn test_to_tuple_range_some() {
        let eqs_ranges = vec![
            Range::Eq(DataValue::Int32(1)),
            Range::SortedRanges(vec![
                Range::Eq(DataValue::Null),
                Range::Eq(DataValue::Int32(1)),
                Range::Eq(DataValue::Int32(2)),
            ]),
            Range::SortedRanges(vec![
                Range::Eq(DataValue::Int32(1)),
                Range::Eq(DataValue::Int32(2)),
            ]),
        ];

        let range = Range::Scope {
            min: Bound::Included(DataValue::Int32(1)),
            max: Bound::Unbounded,
        }
        .combining_eqs(&eqs_ranges);

        assert_eq!(
            range,
            Some(Range::SortedRanges(vec![
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Excluded(DataValue::Tuple(
                        vec![DataValue::Int32(1), DataValue::Null, DataValue::Int32(1),],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Excluded(DataValue::Tuple(
                        vec![DataValue::Int32(1), DataValue::Null, DataValue::Int32(2),],
                        true
                    ))
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
            ]))
        );

        let range = Range::Scope {
            min: Bound::Unbounded,
            max: Bound::Included(DataValue::Int32(1)),
        }
        .combining_eqs(&eqs_ranges);

        assert_eq!(
            range,
            Some(Range::SortedRanges(vec![
                Range::Scope {
                    min: Bound::Excluded(DataValue::Tuple(
                        vec![DataValue::Int32(1), DataValue::Null, DataValue::Int32(1),],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Excluded(DataValue::Tuple(
                        vec![DataValue::Int32(1), DataValue::Null, DataValue::Int32(2),],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Excluded(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        true
                    )),
                },
            ]))
        );

        let range = Range::Scope {
            min: Bound::Included(DataValue::Int32(1)),
            max: Bound::Included(DataValue::Int32(2)),
        }
        .combining_eqs(&eqs_ranges);

        assert_eq!(
            range,
            Some(Range::SortedRanges(vec![
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Null,
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
                Range::Scope {
                    min: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                            DataValue::Int32(1),
                        ],
                        false
                    )),
                    max: Bound::Included(DataValue::Tuple(
                        vec![
                            DataValue::Int32(1),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                            DataValue::Int32(2),
                        ],
                        true
                    )),
                },
            ]))
        )
    }

    #[test]
    fn test_to_tuple_range_none() {
        let range = Range::Scope {
            min: Bound::Included(DataValue::Int32(2)),
            max: Bound::Unbounded,
        }
        .combining_eqs(&[
            Range::Eq(DataValue::Int32(7)),
            Range::Eq(DataValue::Int32(10)),
        ]);
        assert_eq!(
            range,
            Some(Range::Scope {
                min: Bound::Included(DataValue::Tuple(
                    vec![
                        DataValue::Int32(7),
                        DataValue::Int32(10),
                        DataValue::Int32(2)
                    ],
                    false
                )),
                max: Bound::Excluded(DataValue::Tuple(
                    vec![DataValue::Int32(7), DataValue::Int32(10)],
                    true
                )),
            })
        );
        let Range::Scope {
            min: Bound::Included(min),
            max: Bound::Excluded(max),
        } = range.unwrap()
        else {
            unreachable!()
        };
        assert_eq!(
            TupleLtBinaryEvaluator.binary_eval(&min, &max).unwrap(),
            DataValue::Boolean(true)
        )
    }
}
