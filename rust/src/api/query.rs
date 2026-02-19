use crate::api::table::record_batches_to_query_result;
use crate::api::types::{DistanceType, LanceError, QueryResult};
use crate::internal::VectorQueryHolder;
use arrow_array::RecordBatch;
use lancedb::query::{ExecutableQuery, QueryBase};

/// Opaque wrapper for LanceDB vector query
#[flutter_rust_bridge::frb(opaque)]
pub struct LanceVectorQuery {
    holder: VectorQueryHolder,
}

impl LanceVectorQuery {
    #[flutter_rust_bridge::frb(ignore)]
    pub fn new(query: lancedb::query::VectorQuery) -> Self {
        Self {
            holder: VectorQueryHolder::new(query),
        }
    }

    #[flutter_rust_bridge::frb(ignore)]
    pub(crate) fn with_column(self, column: String) -> Self {
        let inner = self.holder.clone_inner().column(&column);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }
}

impl LanceVectorQuery {
    /// Set the vector column name for the search
    #[flutter_rust_bridge::frb(sync)]
    pub fn column(&self, column: String) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().column(&column);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Set the distance metric type
    #[flutter_rust_bridge::frb(sync)]
    pub fn distance_type(&self, distance_type: DistanceType) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().distance_type(distance_type.into());
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Set the number of probes for IVF index (higher = more accurate, slower)
    #[flutter_rust_bridge::frb(sync)]
    pub fn nprobes(&self, nprobes: i32) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().nprobes(nprobes as usize);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Set the refinement factor for more accurate results
    #[flutter_rust_bridge::frb(sync)]
    pub fn refine_factor(&self, factor: i32) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().refine_factor(factor as u32);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Set the maximum number of results to return
    #[flutter_rust_bridge::frb(sync)]
    pub fn limit(&self, limit: i64) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().limit(limit as usize);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Set the offset for pagination
    #[flutter_rust_bridge::frb(sync)]
    pub fn offset(&self, offset: i64) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().offset(offset as usize);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Add a filter predicate (SQL WHERE clause)
    #[flutter_rust_bridge::frb(sync)]
    pub fn only_if(&self, predicate: String) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().only_if(predicate);
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Enable post-filtering (filter after vector search instead of before)
    #[flutter_rust_bridge::frb(sync)]
    pub fn postfilter(&self) -> LanceVectorQuery {
        let inner = self.holder.clone_inner().postfilter();
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Select specific columns to return
    #[flutter_rust_bridge::frb(sync)]
    pub fn select(&self, columns: Vec<String>) -> LanceVectorQuery {
        let inner = self
            .holder
            .clone_inner()
            .select(lancedb::query::Select::Columns(columns));
        LanceVectorQuery {
            holder: VectorQueryHolder::new(inner),
        }
    }

    /// Execute the vector search and return results
    pub async fn execute(&self) -> Result<QueryResult, LanceError> {
        use futures::TryStreamExt;

        let stream = self
            .holder
            .clone_inner()
            .execute()
            .await
            .map_err(LanceError::from)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| LanceError {
            message: format!("Failed to collect search results: {}", e),
            code: "SEARCH_COLLECT_ERROR".to_string(),
        })?;

        record_batches_to_query_result(&batches)
    }
}
