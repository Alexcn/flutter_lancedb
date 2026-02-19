use crate::api::table::LanceTable;
use crate::api::types::LanceError;
use crate::internal::ConnectionHolder;

/// Opaque wrapper for LanceDB connection
#[flutter_rust_bridge::frb(opaque)]
pub struct LanceConnection {
    holder: ConnectionHolder,
}

impl LanceConnection {
    #[flutter_rust_bridge::frb(ignore)]
    pub fn new(conn: lancedb::Connection) -> Self {
        Self {
            holder: ConnectionHolder::new(conn),
        }
    }

    #[flutter_rust_bridge::frb(ignore)]
    pub(crate) fn inner(&self) -> &lancedb::Connection {
        self.holder.inner()
    }
}

/// Connect to a LanceDB database at the given URI
///
/// # Arguments
/// * `uri` - Path to the database directory (local) or cloud URI
///
/// # Example
/// ```dart
/// final db = await connect("/path/to/db");
/// final db = await connect("s3://bucket/path");
/// ```
pub async fn connect(uri: String) -> Result<LanceConnection, LanceError> {
    let conn = lancedb::connect(&uri).execute().await.map_err(LanceError::from)?;
    Ok(LanceConnection::new(conn))
}

impl LanceConnection {
    /// List all table names in the database
    pub async fn table_names(&self) -> Result<Vec<String>, LanceError> {
        self.inner()
            .table_names()
            .execute()
            .await
            .map_err(LanceError::from)
    }

    /// Open an existing table by name
    pub async fn open_table(&self, name: String) -> Result<LanceTable, LanceError> {
        let table = self
            .inner()
            .open_table(&name)
            .execute()
            .await
            .map_err(LanceError::from)?;
        Ok(LanceTable::new(table))
    }

    /// Create a new empty table with the given schema
    ///
    /// # Arguments
    /// * `name` - Name of the table to create
    /// * `schema_json` - JSON representation of the schema
    ///
    /// Schema JSON format:
    /// ```json
    /// {
    ///   "fields": [
    ///     {"name": "id", "type": "int64", "nullable": false},
    ///     {"name": "text", "type": "string", "nullable": true},
    ///     {"name": "vector", "type": "fixed_size_list", "list_size": 128, "item_type": "float32", "nullable": false}
    ///   ]
    /// }
    /// ```
    pub async fn create_empty_table(
        &self,
        name: String,
        schema_json: String,
    ) -> Result<LanceTable, LanceError> {
        let schema = parse_schema_json(&schema_json)?;
        let table = self
            .inner()
            .create_empty_table(&name, schema)
            .execute()
            .await
            .map_err(LanceError::from)?;
        Ok(LanceTable::new(table))
    }

    /// Create a new table with initial data
    ///
    /// # Arguments
    /// * `name` - Name of the table to create
    /// * `data_json` - JSON array of row objects
    ///
    /// Data JSON format:
    /// ```json
    /// [
    ///   {"id": 1, "text": "hello", "vector": [0.1, 0.2, ...]},
    ///   {"id": 2, "text": "world", "vector": [0.3, 0.4, ...]}
    /// ]
    /// ```
    pub async fn create_table(
        &self,
        name: String,
        data_json: String,
    ) -> Result<LanceTable, LanceError> {
        let batches = json_to_record_batches(&data_json)?;
        let table = self
            .inner()
            .create_table(&name, batches)
            .execute()
            .await
            .map_err(LanceError::from)?;
        Ok(LanceTable::new(table))
    }

    /// Drop (delete) a table from the database
    pub async fn drop_table(&self, name: String) -> Result<(), LanceError> {
        self.inner()
            .drop_table(&name, &[])
            .await
            .map_err(LanceError::from)
    }
}

/// Parse schema from JSON representation
fn parse_schema_json(json: &str) -> Result<arrow_schema::SchemaRef, LanceError> {
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[derive(serde::Deserialize)]
    struct SchemaJson {
        fields: Vec<FieldJson>,
    }

    #[derive(serde::Deserialize)]
    struct FieldJson {
        name: String,
        #[serde(rename = "type")]
        data_type: String,
        #[serde(default)]
        nullable: bool,
        #[serde(default)]
        list_size: Option<i32>,
        #[serde(default)]
        item_type: Option<String>,
    }

    let schema_json: SchemaJson = serde_json::from_str(json).map_err(|e| LanceError {
        message: format!("Failed to parse schema JSON: {}", e),
        code: "SCHEMA_PARSE_ERROR".to_string(),
    })?;

    let fields: Vec<Field> = schema_json
        .fields
        .into_iter()
        .map(|f| {
            let data_type = match f.data_type.as_str() {
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                "float32" => DataType::Float32,
                "float64" => DataType::Float64,
                "string" | "utf8" => DataType::Utf8,
                "bool" | "boolean" => DataType::Boolean,
                "fixed_size_list" => {
                    let size = f.list_size.unwrap_or(128);
                    let item_type = match f.item_type.as_deref().unwrap_or("float32") {
                        "float32" => DataType::Float32,
                        "float64" => DataType::Float64,
                        _ => DataType::Float32,
                    };
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", item_type, true)),
                        size,
                    )
                }
                _ => DataType::Utf8, // Default to string
            };
            Field::new(&f.name, data_type, f.nullable)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

/// Convert JSON data to Arrow RecordBatch
pub(crate) fn json_to_record_batches(
    json: &str,
) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, LanceError> {
    use arrow_array::builder::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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
    use std::sync::Arc;

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
