use serde::{Deserialize, Serialize};

/// Distance metric for vector search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceType {
    /// Euclidean distance (L2)
    L2,
    /// Cosine similarity
    Cosine,
    /// Dot product
    Dot,
}

impl From<DistanceType> for lancedb::DistanceType {
    fn from(dt: DistanceType) -> Self {
        match dt {
            DistanceType::L2 => lancedb::DistanceType::L2,
            DistanceType::Cosine => lancedb::DistanceType::Cosine,
            DistanceType::Dot => lancedb::DistanceType::Dot,
        }
    }
}

/// A single column value that can be serialized to/from Dart
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    FloatArray(Vec<f64>),
    IntArray(Vec<i64>),
    StringArray(Vec<String>),
}

/// A single row of data as key-value pairs
pub type RowData = std::collections::HashMap<String, ColumnValue>;

/// Schema field information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Table schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub fields: Vec<FieldInfo>,
}

/// Query result containing rows of data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<RowData>,
    pub num_rows: usize,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            num_rows: 0,
        }
    }
}

/// Index type for creating indexes on tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    /// Automatically choose the best index type
    Auto,
    /// BTree index for scalar columns - good for high cardinality columns
    BTree,
    /// Bitmap index for low cardinality columns
    Bitmap,
    /// LabelList index for List<T> columns (array_contains queries)
    LabelList,
    /// Full text search index using BM25
    Fts,
    /// IVF-Flat index for vector columns (no quantization)
    IvfFlat {
        num_partitions: Option<u32>,
    },
    /// IVF-PQ index for vector columns (product quantization)
    IvfPq {
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
    },
    /// IVF-SQ index for vector columns (scalar quantization)
    IvfSq {
        num_partitions: Option<u32>,
    },
    /// IVF-RQ index for vector columns (RabitQ quantization)
    IvfRq {
        num_partitions: Option<u32>,
    },
    /// IVF-HNSW-PQ index - HNSW with product quantization
    IvfHnswPq {
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
    },
    /// IVF-HNSW-SQ index - HNSW with scalar quantization
    IvfHnswSq {
        num_partitions: Option<u32>,
    },
}

/// Error type for LanceDB operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceError {
    pub message: String,
    pub code: String,
}

impl From<lancedb::Error> for LanceError {
    fn from(e: lancedb::Error) -> Self {
        Self {
            message: e.to_string(),
            code: "LANCE_ERROR".to_string(),
        }
    }
}

impl std::fmt::Display for LanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for LanceError {}
