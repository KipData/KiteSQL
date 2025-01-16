use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum AggKind {
    Avg,
    Max,
    Min,
    Sum,
    Count,
}

impl AggKind {
    pub fn allow_distinct(&self) -> bool {
        match self {
            AggKind::Avg => false,
            AggKind::Max => false,
            AggKind::Min => false,
            AggKind::Sum => true,
            AggKind::Count => true,
        }
    }
}
