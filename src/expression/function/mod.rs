use crate::types::LogicalType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub mod scala;
pub mod table;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct FunctionSummary {
    pub name: Arc<str>,
    pub arg_types: Vec<LogicalType>,
}
