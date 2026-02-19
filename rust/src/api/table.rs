use crate::api::query::LanceVectorQuery;
use crate::api::types::{FieldInfo, LanceError, QueryResult, SchemaInfo};
use crate::internal::TableHolder;
use arrow_array::RecordBatch;
use lancedb::query::{ExecutableQuery, QueryBase};
use std::sync::Arc;

/// Opaque wrapper for LanceDB table
#[flutter_rust_bridge::frb(opaque)]
pub struct LanceTable {
    holder: TableHolder,
}

impl LanceTable {
    #[flutter_rust_bridge::frb(ignore)]
    pub fn new(table: lancedb::Table) -> Self {
        Self {
            holder: TableHolder::new(table),
        }
    }

    #[flutter_rust_bridge::frb(ignore)]
    pub(crate) fn inner(&self) -> &lancedb::Table {
        self.holder.inner()
    }
}

impl LanceTable {
    /// Get the table name
    #[flutter_rust_bridge::frb(sync)]
    pub fn name(&self) -> String {
        self.inner().name().to_string()
    }

    /// Get the table schema
    pub async fn schema(&self) -> Result<SchemaInfo, LanceError> {
        let schema = self.inner().schema().await.map_err(LanceError::from)?;
        let fields = schema
            .fields()
            .iter()
            .map(|f| FieldInfo {
                name: f.name().to_string(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();
        Ok(SchemaInfo { fields })
    }

    /// Count the number of rows in the table
    pub async fn count_rows(&self, filter: Option<String>) -> Result<i64, LanceError> {
        let count = self
            .inner()
            .count_rows(filter)
            .await
            .map_err(LanceError::from)?;
        Ok(count as i64)
    }

    /// Add data to the table
    ///
    /// # Arguments
    /// * `data_json` - JSON array of row objects
    ///
    /// Data JSON format:
    /// ```json
    /// [
    ///   {"id": 1, "text": "hello", "vector": [0.1, 0.2, ...]},
    ///   {"id": 2, "text": "world", "vector": [0.3, 0.4, ...]}
    /// ]
    /// ```
    pub async fn add(&self, data_json: String) -> Result<(), LanceError> {
        let batches = json_to_record_batches(&data_json)?;
        self.inner().add(batches).execute().await.map_err(LanceError::from)?;
        Ok(())
    }

    /// Delete rows matching the predicate
    ///
    /// # Arguments
    /// * `predicate` - SQL WHERE clause predicate (e.g., "id > 10")
    pub async fn delete(&self, predicate: String) -> Result<(), LanceError> {
        self.inner()
            .delete(&predicate)
            .await
            .map_err(LanceError::from)?;
        Ok(())
    }

    /// Update rows matching the predicate
    ///
    /// # Arguments
    /// * `updates_json` - JSON object with column names and new values
    /// * `predicate` - Optional SQL WHERE clause predicate
    ///
    /// Updates JSON format:
    /// ```json
    /// {"column1": "new_value", "column2": 123}
    /// ```
    pub async fn update(
        &self,
        updates_json: String,
        predicate: Option<String>,
    ) -> Result<i64, LanceError> {
        let updates: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&updates_json).map_err(|e| LanceError {
                message: format!("Failed to parse updates JSON: {}", e),
                code: "UPDATE_PARSE_ERROR".to_string(),
            })?;

        let mut builder = self.inner().update();

        if let Some(pred) = predicate {
            builder = builder.only_if(pred);
        }

        for (column, value) in updates {
            let value_str = match value {
                serde_json::Value::String(s) => format!("'{}'", s),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => "NULL".to_string(),
                _ => value.to_string(),
            };
            builder = builder.column(&column, &value_str);
        }

        builder.execute().await.map_err(LanceError::from)?;
        Ok(0) // Update returns () in newer lancedb versions
    }

    /// Execute a simple query and return all results
    ///
    /// # Arguments
    /// * `columns` - Optional list of columns to select (None = all columns)
    /// * `filter` - Optional SQL WHERE clause predicate
    /// * `limit` - Optional maximum number of rows to return
    pub async fn query(
        &self,
        columns: Option<Vec<String>>,
        filter: Option<String>,
        limit: Option<i64>,
    ) -> Result<QueryResult, LanceError> {
        use futures::TryStreamExt;

        let mut query = self.inner().query();

        if let Some(cols) = columns {
            query = query.select(lancedb::query::Select::Columns(cols));
        }

        if let Some(f) = filter {
            query = query.only_if(f);
        }

        if let Some(l) = limit {
            query = query.limit(l as usize);
        }

        let stream = query.execute().await.map_err(LanceError::from)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| LanceError {
            message: format!("Failed to collect query results: {}", e),
            code: "QUERY_COLLECT_ERROR".to_string(),
        })?;

        record_batches_to_query_result(&batches)
    }

    /// Perform a vector similarity search
    ///
    /// # Arguments
    /// * `vector` - Query vector as a list of floats
    /// * `column` - Optional name of the vector column (default: auto-detect)
    ///
    /// Returns a VectorQuery builder for further configuration
    pub fn vector_search(
        &self,
        vector: Vec<f32>,
        column: Option<String>,
    ) -> Result<LanceVectorQuery, LanceError> {
        let query = self
            .inner()
            .vector_search(vector)
            .map_err(LanceError::from)?;
        
        let mut vq = LanceVectorQuery::new(query);
        if let Some(col) = column {
            vq = vq.with_column(col);
        }
        Ok(vq)
    }

    /// Create an index on the table
    ///
    /// # Arguments
    /// * `column` - Column name to create index on
    /// * `index_type` - Type of index (e.g., "ivf_pq", "btree", "auto")
    /// * `replace` - Whether to replace existing index
    pub async fn create_index(
        &self,
        column: String,
        index_type: Option<String>,
        replace: Option<bool>,
    ) -> Result<(), LanceError> {
        use lancedb::index::Index;

        let index = match index_type.as_deref() {
            Some("btree") => Index::BTree(Default::default()),
            Some("bitmap") => Index::Bitmap(Default::default()),
            Some("label_list") => Index::LabelList(Default::default()),
            Some("fts") => Index::FTS(Default::default()),
            Some("ivf_flat") => Index::IvfFlat(Default::default()),
            Some("ivf_pq") => Index::IvfPq(Default::default()),
            Some("ivf_sq") => Index::IvfSq(Default::default()),
            Some("ivf_rq") => Index::IvfRq(Default::default()),
            Some("ivf_hnsw_pq") => Index::IvfHnswPq(Default::default()),
            Some("ivf_hnsw_sq") => Index::IvfHnswSq(Default::default()),
            _ => Index::Auto,
        };

        let mut builder = self.inner().create_index(&[column], index);
        
        if replace.unwrap_or(false) {
            builder = builder.replace(true);
        }

        builder.execute().await.map_err(LanceError::from)?;
        Ok(())
    }

    /// List all indexes on the table
    pub async fn list_indices(&self) -> Result<Vec<String>, LanceError> {
        let indices = self.inner().list_indices().await.map_err(LanceError::from)?;
        Ok(indices
            .iter()
            .map(|i| format!("{}:{}", i.name, i.index_type))
            .collect())
    }

    /// Optimize the table (compact files, etc.)
    pub async fn optimize(&self) -> Result<String, LanceError> {
        let stats = self
            .inner()
            .optimize(lancedb::table::OptimizeAction::All)
            .await
            .map_err(LanceError::from)?;
        Ok(format!(
            "Compaction: {:?}, Prune: {:?}",
            stats.compaction, stats.prune
        ))
    }

    /// Get the current version of the table
    pub async fn version(&self) -> Result<i64, LanceError> {
        let version = self.inner().version().await.map_err(LanceError::from)?;
        Ok(version as i64)
    }
}

/// Convert JSON data to Arrow RecordBatch (reuse from connection.rs logic)
fn json_to_record_batches(
    json: &str,
) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, LanceError> {
    use arrow_array::builder::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};

    let rows: Vec<serde_json::Value> = serde_json::from_str(json).map_err(|e| LanceError {
        message: format!("Failed to parse data JSON: {}", e),
        code: "DATA_PARSE_ERROR".to_string(),
    })?;

    if rows.is_empty() {
        return Err(LanceError {
            message: "Data array is empty".to_string(),
            code: "EMPTY_DATA_ERROR".to_string(),
        });
    }

    // Infer schema from first row
    let first_row = rows[0].as_object().ok_or_else(|| LanceError {
        message: "Data must be an array of objects".to_string(),
        code: "DATA_FORMAT_ERROR".to_string(),
    })?;

    let mut fields = Vec::new();
    let mut column_names = Vec::new();

    for (key, value) in first_row.iter() {
        column_names.push(key.clone());
        let data_type = infer_data_type(value);
        fields.push(Field::new(key, data_type, true));
    }

    let schema = Arc::new(Schema::new(fields.clone()));

    // Build arrays for each column
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (idx, field) in fields.iter().enumerate() {
        let col_name = &column_names[idx];
        match field.data_type() {
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in &rows {
                    if let Some(v) = row.get(col_name).and_then(|v| v.as_i64()) {
                        builder.append_value(v);
                    } else {
                        builder.append_null();
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in &rows {
                    if let Some(v) = row.get(col_name).and_then(|v| v.as_f64()) {
                        builder.append_value(v);
                    } else {
                        builder.append_null();
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in &rows {
                    if let Some(v) = row.get(col_name).and_then(|v| v.as_str()) {
                        builder.append_value(v);
                    } else {
                        builder.append_null();
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in &rows {
                    if let Some(v) = row.get(col_name).and_then(|v| v.as_bool()) {
                        builder.append_value(v);
                    } else {
                        builder.append_null();
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
            DataType::FixedSizeList(inner_field, size) => {
                match inner_field.data_type() {
                    DataType::Float32 => {
                        let values_builder = Float32Builder::new();
                        let mut list_builder =
                            FixedSizeListBuilder::new(values_builder, *size);
                        for row in &rows {
                            if let Some(arr) = row.get(col_name).and_then(|v| v.as_array()) {
                                let values: Vec<f32> = arr
                                    .iter()
                                    .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                                    .collect();
                                list_builder.values().append_slice(&values);
                                list_builder.append(true);
                            } else {
                                for _ in 0..*size {
                                    list_builder.values().append_value(0.0);
                                }
                                list_builder.append(false);
                            }
                        }
                        arrays.push(Arc::new(list_builder.finish()));
                    }
                    _ => {
                        let values_builder = Float64Builder::new();
                        let mut list_builder =
                            FixedSizeListBuilder::new(values_builder, *size);
                        for row in &rows {
                            if let Some(arr) = row.get(col_name).and_then(|v| v.as_array()) {
                                let values: Vec<f64> = arr
                                    .iter()
                                    .map(|v| v.as_f64().unwrap_or(0.0))
                                    .collect();
                                list_builder.values().append_slice(&values);
                                list_builder.append(true);
                            } else {
                                for _ in 0..*size {
                                    list_builder.values().append_value(0.0);
                                }
                                list_builder.append(false);
                            }
                        }
                        arrays.push(Arc::new(list_builder.finish()));
                    }
                }
            }
            _ => {
                // Default to string
                let mut builder = StringBuilder::new();
                for row in &rows {
                    if let Some(v) = row.get(col_name) {
                        builder.append_value(v.to_string());
                    } else {
                        builder.append_null();
                    }
                }
                arrays.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| LanceError {
        message: format!("Failed to create RecordBatch: {}", e),
        code: "BATCH_CREATE_ERROR".to_string(),
    })?;

    Ok(Box::new(RecordBatchIterator::new(
        vec![Ok(batch)],
        schema,
    )))
}

/// Infer Arrow DataType from JSON value
fn infer_data_type(value: &serde_json::Value) -> arrow_schema::DataType {
    use arrow_schema::{DataType, Field};

    match value {
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    128,
                )
            } else {
                let size = arr.len() as i32;
                let item_type = if arr[0].is_f64() || arr[0].is_i64() {
                    DataType::Float32
                } else {
                    DataType::Utf8
                };
                DataType::FixedSizeList(Arc::new(Field::new("item", item_type, true)), size)
            }
        }
        _ => DataType::Utf8,
    }
}

/// Convert Arrow RecordBatches to QueryResult
pub(crate) fn record_batches_to_query_result(
    batches: &[RecordBatch],
) -> Result<QueryResult, LanceError> {
    use crate::api::types::RowData;

    if batches.is_empty() {
        return Ok(QueryResult::empty());
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_data = RowData::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col_name = field.name();
                let array = batch.column(col_idx);

                let value = extract_value(array, row_idx);
                row_data.insert(col_name.clone(), value);
            }

            rows.push(row_data);
        }
    }

    let num_rows = rows.len();
    Ok(QueryResult {
        columns,
        rows,
        num_rows,
    })
}

/// Extract a value from an Arrow array at the given index
fn extract_value(array: &dyn arrow_array::Array, idx: usize) -> crate::api::types::ColumnValue {
    use crate::api::types::ColumnValue;
    use arrow_array::*;

    if array.is_null(idx) {
        return ColumnValue::Null;
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return ColumnValue::Int(arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return ColumnValue::Int(arr.value(idx) as i64);
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return ColumnValue::Float(arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return ColumnValue::Float(arr.value(idx) as f64);
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return ColumnValue::String(arr.value(idx).to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return ColumnValue::Bool(arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let inner = arr.value(idx);
        if let Some(float_arr) = inner.as_any().downcast_ref::<Float32Array>() {
            let values: Vec<f64> = (0..float_arr.len())
                .map(|i| float_arr.value(i) as f64)
                .collect();
            return ColumnValue::FloatArray(values);
        }
        if let Some(float_arr) = inner.as_any().downcast_ref::<Float64Array>() {
            let values: Vec<f64> = (0..float_arr.len()).map(|i| float_arr.value(i)).collect();
            return ColumnValue::FloatArray(values);
        }
    }

    // Fallback to string representation
    ColumnValue::String(format!("{:?}", array))
}
