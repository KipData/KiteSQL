use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use super::*;
use crate::errors::DatabaseError;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::Childrens;
use kite_sql_serde_macros::ReferenceSerialization;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{CopyOption, CopySource, CopyTarget};

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, ReferenceSerialization)]
pub struct ExtSource {
    pub path: PathBuf,
    pub format: FileFormat,
}

/// File format.
#[derive(
    Debug,
    PartialEq,
    PartialOrd,
    Ord,
    Hash,
    Eq,
    Clone,
    Serialize,
    Deserialize,
    ReferenceSerialization,
)]
pub enum FileFormat {
    Csv {
        /// Delimiter to parse.
        delimiter: char,
        /// Quote to use.
        quote: char,
        /// Escape character to use.
        escape: Option<char>,
        /// Whether or not the file has a header line.
        header: bool,
    },
}

impl std::fmt::Display for ExtSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for ExtSource {
    type Err = ();
    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(())
    }
}

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(super) fn bind_copy(
        &mut self,
        source: CopySource,
        to: bool,
        target: CopyTarget,
        options: &[CopyOption],
    ) -> Result<LogicalPlan, DatabaseError> {
        let (table_name, ..) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => (table_name, columns),
            CopySource::Query(_) => {
                return Err(DatabaseError::UnsupportedStmt("'COPY SOURCE'".to_string()));
            }
        };
        let table_name = Arc::new(lower_case_name(&table_name)?);

        if let Some(table) = self.context.table(table_name.clone())? {
            let schema_ref = table.schema_ref().clone();
            let ext_source = ExtSource {
                path: match target {
                    CopyTarget::File { filename } => filename.into(),
                    t => {
                        return Err(DatabaseError::UnsupportedStmt(format!(
                            "copy target: {:?}",
                            t
                        )))
                    }
                },
                format: FileFormat::from_options(options),
            };

            if to {
                // COPY <source_table> TO <dest_file>
                Ok(LogicalPlan::new(
                    Operator::CopyToFile(CopyToFileOperator {
                        target: ext_source,
                        schema_ref,
                    }),
                    Childrens::Only(TableScanOperator::build(table_name, table)),
                ))
            } else {
                // COPY <dest_table> FROM <source_file>
                Ok(LogicalPlan::new(
                    Operator::CopyFromFile(CopyFromFileOperator {
                        source: ext_source,
                        schema_ref,
                        table: table_name,
                    }),
                    Childrens::None,
                ))
            }
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }
}

impl FileFormat {
    /// Create from copy options.
    pub fn from_options(options: &[CopyOption]) -> Self {
        let mut delimiter = ',';
        let mut quote = '"';
        let mut escape = None;
        let mut header = false;
        for opt in options {
            match opt {
                CopyOption::Format(fmt) => {
                    debug_assert_eq!(fmt.value.to_lowercase(), "csv", "only support CSV format")
                }
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(b) => header = *b,
                CopyOption::Quote(c) => quote = *c,
                CopyOption::Escape(c) => escape = Some(*c),
                o => panic!("unsupported copy option: {:?}", o),
            }
        }
        FileFormat::Csv {
            delimiter,
            quote,
            escape,
            header,
        }
    }
}
