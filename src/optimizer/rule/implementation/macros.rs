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

#[macro_export]
macro_rules! single_mapping {
    ($ty:ty, $pattern:expr, $option:expr) => {
        impl MatchPattern for $ty {
            fn pattern(&self) -> &Pattern {
                &$pattern
            }
        }

        impl<T: Transaction> ImplementationRule<T> for $ty {
            fn to_expression(
                &self,
                _: &Operator,
                _: &StatisticMetaLoader<'_, T>,
                group_expr: &mut GroupExpression,
            ) -> Result<(), DatabaseError> {
                //TODO: CostModel
                group_expr.append_expr(Expression {
                    op: $option,
                    cost: None,
                });

                Ok(())
            }
        }
    };
}
